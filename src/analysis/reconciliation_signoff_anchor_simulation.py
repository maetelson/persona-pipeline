"""Anchor-level simulation helpers for reconciliation/signoff diagnostics."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.analysis.reconciliation_signoff_identity import share

ANCHOR_TARGET_ID = "persona_04"
ANCHOR_PARENT_ID = "persona_01"


def evaluate_anchor_subset(anchor_df: pd.DataFrame, persona_column: str, target_id: str = ANCHOR_TARGET_ID) -> dict[str, Any]:
    """Evaluate one anchor-labeled subset against one persona assignment column."""
    positives = anchor_df[anchor_df["anchor_label"].astype(str) == "anchor_positive_reconciliation_signoff"]
    hard_negatives = anchor_df[anchor_df["anchor_label"].astype(str) == "anchor_hard_negative"]
    parents = anchor_df[anchor_df["anchor_label"].astype(str) == "anchor_parent_reporting_packager"]
    ambiguous = anchor_df[anchor_df["anchor_label"].astype(str) == "non_anchor_ambiguous"]

    positive_hits = int(positives[persona_column].astype(str).eq(target_id).sum())
    hard_negative_hits = int(hard_negatives[persona_column].astype(str).eq(target_id).sum())
    parent_hits = int(parents[persona_column].astype(str).eq(ANCHOR_PARENT_ID).sum())
    ambiguous_hits = int(ambiguous[persona_column].astype(str).eq(target_id).sum())

    return {
        "positive_anchor_capture_rate": share(positive_hits, len(positives)),
        "hard_negative_anchor_false_positive_rate": share(hard_negative_hits, len(hard_negatives)),
        "parent_anchor_retention_rate": share(parent_hits, len(parents)),
        "ambiguous_non_anchor_movement_rate": share(ambiguous_hits, len(ambiguous)),
        "positive_anchor_hits": positive_hits,
        "hard_negative_anchor_hits": hard_negative_hits,
        "parent_anchor_hits": parent_hits,
        "ambiguous_non_anchor_hits": ambiguous_hits,
        "parent_examples_wrongly_pulled_out_of_persona_01": int((~parents[persona_column].astype(str).eq(ANCHOR_PARENT_ID)).sum()),
    }


def top_3_cluster_share(assignments: pd.Series) -> float:
    """Compute top-3 persona share from one assignment series."""
    counts = assignments.astype(str).value_counts()
    return share(int(counts.head(3).sum()), int(len(assignments)))


def persona_statuses(top_3_share_pct: float, persona_05_drift_risk: bool) -> dict[str, str]:
    """Return one compact workbook-style status readout for simulation-only reports."""
    persona_04_status = "likely_unblocked_by_concentration" if top_3_share_pct < 80.0 else "blocked_by_concentration"
    if persona_05_drift_risk:
        persona_05_status = "promotion_drift_risk"
    else:
        persona_05_status = "still_likely_blocked"
    return {
        "persona_04_status_simulation": persona_04_status,
        "persona_05_status_simulation": persona_05_status,
    }


def estimate_final_usable_persona_count(top_3_share_pct: float, persona_05_drift_risk: bool) -> int:
    """Estimate final usable persona count under current workbook-concentration intuition."""
    if top_3_share_pct < 80.0 and not persona_05_drift_risk:
        return 4
    return 3


def anchor_variant_eligibility(
    *,
    identity_gate: dict[str, Any],
    positive_anchor_capture_rate: float,
    baseline_positive_anchor_capture_rate: float,
    hard_negative_anchor_false_positive_rate: float,
    parent_anchor_retention_rate: float,
    ambiguous_non_anchor_movement_rate: float,
    baseline_ambiguous_non_anchor_movement_rate: float,
    top_3_share_pct: float,
    persona_05_drift_risk: bool,
) -> dict[str, Any]:
    """Evaluate whether one anchor-level simulation is eligible for future implementation."""
    fail_reasons: list[str] = []
    if not bool(identity_gate.get("eligible_for_future_implementation", False)):
        fail_reasons.append("identity_continuity_gate")
    if positive_anchor_capture_rate < (baseline_positive_anchor_capture_rate + 10.0):
        fail_reasons.append("positive_anchor_capture_not_meaningfully_improved")
    if hard_negative_anchor_false_positive_rate > 16.7:
        fail_reasons.append("hard_negative_fp_above_ceiling")
    if parent_anchor_retention_rate < 99.0:
        fail_reasons.append("parent_anchor_retention_below_floor")
    if ambiguous_non_anchor_movement_rate > max(25.0, baseline_ambiguous_non_anchor_movement_rate):
        fail_reasons.append("ambiguous_non_anchor_movement_not_controlled")
    if top_3_share_pct > 80.5:
        fail_reasons.append("top_3_share_not_materially_improved")
    if persona_05_drift_risk:
        fail_reasons.append("persona_05_promotion_drift_risk_present")

    return {
        "eligible_for_future_implementation": not fail_reasons,
        "fail_reasons": fail_reasons,
    }
