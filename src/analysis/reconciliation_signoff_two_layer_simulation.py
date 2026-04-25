"""Two-layer anchor simulation helpers for reconciliation/signoff decision gates."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.analysis.reconciliation_signoff_identity import share


def evaluate_label_subset(
    frame: pd.DataFrame,
    *,
    label_column: str,
    positive_label: str,
    hard_negative_label: str,
    parent_label: str,
    ambiguous_label: str,
    persona_column: str,
    target_persona_id: str,
    parent_persona_id: str = "persona_01",
) -> dict[str, Any]:
    """Evaluate one labeled anchor subset against one persona assignment column."""
    positives = frame[frame[label_column].astype(str) == positive_label]
    hard_negatives = frame[frame[label_column].astype(str) == hard_negative_label]
    parents = frame[frame[label_column].astype(str) == parent_label]
    ambiguous = frame[frame[label_column].astype(str) == ambiguous_label]

    positive_hits = int(positives[persona_column].astype(str).eq(target_persona_id).sum())
    hard_negative_hits = int(hard_negatives[persona_column].astype(str).eq(target_persona_id).sum())
    parent_hits = int(parents[persona_column].astype(str).eq(parent_persona_id).sum())
    ambiguous_hits = int(ambiguous[persona_column].astype(str).eq(target_persona_id).sum())

    return {
        "positive_capture_rate": share(positive_hits, len(positives)),
        "hard_negative_false_positive_rate": share(hard_negative_hits, len(hard_negatives)),
        "parent_retention_rate": share(parent_hits, len(parents)),
        "ambiguous_movement_rate": share(ambiguous_hits, len(ambiguous)),
        "positive_hits": positive_hits,
        "hard_negative_hits": hard_negative_hits,
        "parent_hits": parent_hits,
        "ambiguous_hits": ambiguous_hits,
        "parent_examples_wrongly_pulled_out_of_persona_01": int((~parents[persona_column].astype(str).eq(parent_persona_id)).sum()),
    }


def top_3_cluster_share(assignments: pd.Series) -> float:
    """Compute top-3 persona share from one assignment series."""
    counts = assignments.astype(str).value_counts()
    return share(int(counts.head(3).sum()), int(len(assignments)))


def estimated_final_usable_persona_count(top_3_share_pct: float, persona_05_drift_risk: bool) -> int:
    """Estimate final usable persona count under the current workbook concentration intuition."""
    if top_3_share_pct < 80.0 and not persona_05_drift_risk:
        return 4
    return 3


def two_layer_variant_decision(
    *,
    identity_overlap: float,
    selected_example_overlap_pct: float,
    identity_positive_capture_rate: float,
    identity_hard_negative_fp_rate: float,
    identity_parent_retention_rate: float,
    expansion_positive_capture_rate: float,
    baseline_expansion_positive_capture_rate: float,
    expansion_hard_negative_fp_rate: float,
    expansion_parent_retention_rate: float,
    expansion_ambiguous_movement_rate: float,
    persona_01_leakage_pct: float,
    persona_05_drift_risk: bool,
    top_3_share_pct: float,
) -> dict[str, Any]:
    """Return pass/fail results for the final bounded two-layer gate."""
    identity_pass = (
        identity_overlap >= 0.85
        and selected_example_overlap_pct >= 80.0
        and identity_positive_capture_rate >= 95.0
        and identity_hard_negative_fp_rate <= 16.7
        and identity_parent_retention_rate >= 99.0
        and persona_01_leakage_pct <= 5.0
        and not persona_05_drift_risk
    )

    expansion_pass = (
        expansion_positive_capture_rate >= (baseline_expansion_positive_capture_rate + 20.0)
        and expansion_parent_retention_rate >= 99.0
        and expansion_hard_negative_fp_rate <= 16.7
        and expansion_ambiguous_movement_rate <= 25.0
    )

    fail_reasons: list[str] = []
    if not identity_pass:
        fail_reasons.append("identity_anchor_gate")
    if not expansion_pass:
        fail_reasons.append("expansion_anchor_gate")
    if top_3_share_pct > 80.5:
        fail_reasons.append("top_3_share_not_materially_improved")
    if persona_05_drift_risk:
        fail_reasons.append("persona_05_promotion_drift_risk_present")

    return {
        "identity_anchor_pass": identity_pass,
        "expansion_anchor_pass": expansion_pass,
        "future_production_patch_candidate": not fail_reasons,
        "fail_reasons": fail_reasons,
    }
