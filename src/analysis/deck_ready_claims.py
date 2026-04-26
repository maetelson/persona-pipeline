"""Deck-ready claim eligibility annotations for persona-facing analysis outputs."""

from __future__ import annotations

from typing import Any

import pandas as pd


POLICY_COLUMNS = [
    "deck_ready_claim_eligible_persona",
    "deck_ready_claim_evidence_status",
    "deck_ready_claim_reason",
    "core_anchor_policy_status",
    "supporting_validation_policy_status",
    "exploratory_dependency_policy_status",
    "excluded_source_dependency_policy_status",
]


def build_deck_ready_claim_outputs(
    persona_summary_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_promotion_path_debug_df: pd.DataFrame,
) -> dict[str, Any]:
    """Apply one centralized Phase 3A claim-eligibility layer to persona outputs."""
    annotated_cluster_stats_df = _annotate_claim_policy_frame(cluster_stats_df)
    annotated_persona_summary_df = _merge_claim_policy_fields(persona_summary_df, annotated_cluster_stats_df)
    annotated_promotion_path_debug_df = _merge_claim_policy_fields(persona_promotion_path_debug_df, annotated_cluster_stats_df)
    claim_eligible_count = (
        int(
            annotated_cluster_stats_df.get("deck_ready_claim_eligible_persona", pd.Series(dtype=bool))
            .fillna(False)
            .astype(bool)
            .sum()
        )
        if not annotated_cluster_stats_df.empty
        else 0
    )
    return {
        "persona_summary_df": annotated_persona_summary_df,
        "cluster_stats_df": annotated_cluster_stats_df,
        "persona_promotion_path_debug_df": annotated_promotion_path_debug_df,
        "counts": {
            "deck_ready_claim_eligible_persona_count": claim_eligible_count,
        },
    }


def _annotate_claim_policy_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Add one deterministic claim-eligibility policy layer to a persona frame."""
    if frame.empty:
        annotated = frame.copy()
        for column in POLICY_COLUMNS:
            annotated[column] = pd.Series(dtype=object)
        return annotated
    annotated = frame.copy()
    annotated["core_anchor_policy_status"] = annotated.apply(_core_anchor_policy_status, axis=1)
    annotated["supporting_validation_policy_status"] = annotated.apply(_supporting_validation_policy_status, axis=1)
    annotated["exploratory_dependency_policy_status"] = annotated.apply(_exploratory_dependency_policy_status, axis=1)
    annotated["excluded_source_dependency_policy_status"] = annotated.apply(_excluded_source_dependency_policy_status, axis=1)
    annotated["deck_ready_claim_eligible_persona"] = annotated.apply(_deck_ready_claim_eligible, axis=1)
    annotated["deck_ready_claim_reason"] = annotated.apply(_deck_ready_claim_reason, axis=1)
    return annotated


def _merge_claim_policy_fields(target_df: pd.DataFrame, source_df: pd.DataFrame) -> pd.DataFrame:
    """Merge centralized claim-policy fields into another persona-facing output."""
    if target_df.empty:
        return target_df.copy()
    merge_columns = ["persona_id", *POLICY_COLUMNS]
    cleaned = target_df.drop(columns=[column for column in POLICY_COLUMNS if column in target_df.columns], errors="ignore")
    return cleaned.merge(source_df[merge_columns], on="persona_id", how="left")


def _core_anchor_policy_status(row: pd.Series) -> str:
    """Return pass/block status for the core representative anchor rule."""
    if not bool(row.get("has_core_representative_anchor", False)):
        return "blocked_missing_core_anchor"
    strength = str(row.get("core_anchor_strength", "none") or "none")
    if strength in {"strong", "moderate"}:
        return "pass"
    if strength == "weak":
        return "blocked_weak_core_anchor"
    return "blocked_missing_core_anchor"


def _supporting_validation_policy_status(row: pd.Series) -> str:
    """Return how supporting-validation evidence contributes to claim interpretation."""
    strength = str(row.get("supporting_validation_strength", "none") or "none")
    if strength in {"strong", "moderate"}:
        return "strengthens_claim"
    if strength == "weak":
        return "limited_support_only"
    return "no_supporting_validation"


def _exploratory_dependency_policy_status(row: pd.Series) -> str:
    """Return pass/block status for exploratory dependency risk."""
    risk = str(row.get("exploratory_dependency_risk", "high") or "high")
    if risk == "low":
        return "pass"
    return f"blocked_{risk}_exploratory_dependency"


def _excluded_source_dependency_policy_status(row: pd.Series) -> str:
    """Return pass/block status for excluded-source dependency risk."""
    risk = str(row.get("excluded_source_dependency_risk", "high") or "high")
    if risk == "low":
        return "pass"
    return f"blocked_{risk}_excluded_dependency"


def _deck_ready_claim_eligible(row: pd.Series) -> bool:
    """Return whether one persona is eligible for deck-ready claim wording."""
    if not _is_production_or_approved_review_ready(row):
        return False
    if str(row.get("readiness_tier", "") or "") == "blocked_or_constrained_candidate":
        return False
    if str(row.get("core_anchor_policy_status", "") or "") != "pass":
        return False
    if str(row.get("exploratory_dependency_policy_status", "") or "") != "pass":
        return False
    if str(row.get("excluded_source_dependency_policy_status", "") or "") != "pass":
        return False
    if _has_thin_evidence_blocker(row):
        return False
    return True


def _deck_ready_claim_reason(row: pd.Series) -> str:
    """Explain one persona's Phase 3A claim-eligibility outcome in reviewer-facing language."""
    persona_id = str(row.get("persona_id", "") or "")
    readiness_tier = str(row.get("readiness_tier", "") or "")
    if readiness_tier == "blocked_or_constrained_candidate":
        return "Blocked or constrained persona remains ineligible for deck-ready claim wording."
    if not _is_production_or_approved_review_ready(row):
        return "Claim eligibility is limited to production-ready personas and explicitly approved review-ready personas."
    if str(row.get("core_anchor_policy_status", "") or "") != "pass":
        return "Core representative anchor is missing or too weak for deck-ready claim wording."
    if str(row.get("exploratory_dependency_policy_status", "") or "") != "pass":
        return "Exploratory dependency risk is above the low-risk policy floor."
    if str(row.get("excluded_source_dependency_policy_status", "") or "") != "pass":
        return "Excluded-source dependency risk is above the low-risk policy floor."
    if _has_thin_evidence_blocker(row):
        return "Claim wording stays blocked because the persona still has thin-evidence or structural caution."
    if bool(row.get("production_ready_persona", False)):
        return "Production-ready persona is core-anchored with low exploratory and excluded dependency risk, so deck-ready claim wording is allowed."
    if bool(row.get("review_ready_persona", False)):
        return (
            f"{persona_id} is a core-anchored approved review-ready persona for analyst/deck discussion only; "
            "it remains non-production and outside the final usable count."
        )
    return "Claim eligibility is not granted under the current Phase 3A policy."


def _is_production_or_approved_review_ready(row: pd.Series) -> bool:
    """Return whether the persona is eligible to be considered for claim wording."""
    if bool(row.get("production_ready_persona", False)):
        return True
    return bool(row.get("review_ready_persona", False))


def _has_thin_evidence_blocker(row: pd.Series) -> bool:
    """Return whether existing evidence fields still signal a caution that blocks strong claim wording."""
    if str(row.get("cluster_evidence_status", "") or "") != "sufficient":
        return True
    if str(row.get("structural_support_status", "") or "") not in {"structurally_supported", "structurally_supported_broad_parent"}:
        return True
    selected_example_count = int(pd.to_numeric(pd.Series([row.get("selected_example_count", 0)]), errors="coerce").fillna(0).iloc[0])
    return selected_example_count <= 0
