"""Release visibility annotations for reviewable claim releases."""

from __future__ import annotations

from typing import Any

import pandas as pd


RELEASE_VISIBILITY_COLUMNS = [
    "release_visibility_tier",
    "headline_inclusion",
    "workbook_section",
    "tail_status",
    "tail_reason",
    "diagnostics_only_persona",
    "included_in_final_narrative",
]


def build_release_visibility_outputs(
    persona_summary_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_promotion_path_debug_df: pd.DataFrame,
) -> dict[str, Any]:
    """Annotate persona-facing outputs with release visibility tiers and counts."""
    annotated_persona_summary_df = _annotate_release_visibility_frame(persona_summary_df)
    annotated_cluster_stats_df = _merge_release_visibility_fields(cluster_stats_df, annotated_persona_summary_df)
    annotated_promotion_path_debug_df = _merge_release_visibility_fields(
        persona_promotion_path_debug_df,
        annotated_persona_summary_df,
    )
    tier_series = annotated_persona_summary_df.get("release_visibility_tier", pd.Series(dtype=str)).astype(str)
    headline_series = (
        annotated_persona_summary_df.get("headline_inclusion", pd.Series(dtype=bool)).fillna(False).astype(bool)
    )
    return {
        "persona_summary_df": annotated_persona_summary_df,
        "cluster_stats_df": annotated_cluster_stats_df,
        "persona_promotion_path_debug_df": annotated_promotion_path_debug_df,
        "counts": {
            "final_usable_release_persona_count": int(tier_series.eq("final_usable_persona").sum()),
            "review_ready_claim_persona_count": int(tier_series.eq("review_ready_claim_persona").sum()),
            "future_candidate_subtheme_count": int(tier_series.eq("future_candidate_subtheme").sum()),
            "exploratory_tail_persona_count": int(tier_series.eq("exploratory_tail_diagnostics_only").sum()),
            "release_headline_persona_count": int(headline_series.sum()),
        },
    }


def _annotate_release_visibility_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Return one persona frame with release visibility columns added."""
    if frame.empty:
        annotated = frame.copy()
        for column in RELEASE_VISIBILITY_COLUMNS:
            annotated[column] = pd.Series(dtype=object)
        return annotated
    annotated = frame.copy()
    annotated["release_visibility_tier"] = annotated.apply(_release_visibility_tier, axis=1)
    annotated["headline_inclusion"] = annotated["release_visibility_tier"].isin(
        {"final_usable_persona", "review_ready_claim_persona"}
    )
    annotated["workbook_section"] = annotated["release_visibility_tier"].map(
        {
            "final_usable_persona": "headline_personas",
            "review_ready_claim_persona": "constrained_review_personas",
            "future_candidate_subtheme": "future_candidate_subthemes",
            "exploratory_tail_diagnostics_only": "exploratory_tail_diagnostics",
        }
    ).fillna("exploratory_tail_diagnostics")
    annotated["tail_status"] = annotated["release_visibility_tier"].map(
        {
            "final_usable_persona": "not_tail",
            "review_ready_claim_persona": "not_tail",
            "future_candidate_subtheme": "future_candidate_subtheme",
            "exploratory_tail_diagnostics_only": "exploratory_tail_diagnostics_only",
        }
    ).fillna("exploratory_tail_diagnostics_only")
    annotated["tail_reason"] = annotated.apply(_tail_reason, axis=1)
    annotated["diagnostics_only_persona"] = annotated["release_visibility_tier"].eq("exploratory_tail_diagnostics_only")
    annotated["included_in_final_narrative"] = annotated["release_visibility_tier"].isin(
        {"final_usable_persona", "review_ready_claim_persona"}
    )
    return annotated


def _merge_release_visibility_fields(target_df: pd.DataFrame, source_df: pd.DataFrame) -> pd.DataFrame:
    """Merge centralized release-visibility fields into another persona output."""
    if target_df.empty:
        return target_df.copy()
    merge_columns = ["persona_id", *RELEASE_VISIBILITY_COLUMNS]
    cleaned = target_df.drop(columns=[column for column in RELEASE_VISIBILITY_COLUMNS if column in target_df.columns], errors="ignore")
    return cleaned.merge(source_df[merge_columns], on="persona_id", how="left")


def _release_visibility_tier(row: pd.Series) -> str:
    """Classify one persona into a release-visibility tier."""
    if bool(row.get("final_usable_persona", False)):
        return "final_usable_persona"
    if bool(row.get("review_ready_persona", False)):
        return "review_ready_claim_persona"
    if _is_future_candidate_subtheme(row):
        return "future_candidate_subtheme"
    return "exploratory_tail_diagnostics_only"


def _tail_reason(row: pd.Series) -> str:
    """Build a concise reviewer-facing reason for non-headline personas."""
    tier = str(row.get("release_visibility_tier", "") or "")
    if tier == "final_usable_persona":
        return ""
    if tier == "review_ready_claim_persona":
        return str(row.get("review_ready_reason", "") or row.get("deck_ready_claim_reason", "") or "").strip()
    if tier == "future_candidate_subtheme":
        return str(
            row.get("subtheme_reason", "")
            or row.get("blocked_reason", "")
            or row.get("persona05_boundary_rule_status", "")
            or "Preserved as a future candidate subtheme with constrained workbook visibility."
        ).strip()
    return str(
        row.get("blocked_reason", "")
        or row.get("review_ready_reason", "")
        or row.get("promotion_reason", "")
        or row.get("structural_support_reason", "")
        or "Exploratory residual persona kept for diagnostics only."
    ).strip()


def _is_future_candidate_subtheme(row: pd.Series) -> bool:
    """Return true when a persona should be preserved as a future subtheme."""
    if bool(row.get("future_candidate_subtheme", False)):
        return True
    if str(row.get("subtheme_status", "") or "").strip() == "future_candidate_subtheme":
        return True
    persona_id = str(row.get("persona_id", "") or "").strip()
    if persona_id == "persona_05":
        return True
    if persona_id != "persona_05":
        return False
    boundary_status = str(row.get("persona05_boundary_rule_status", "") or "").strip()
    boundary_readiness = str(row.get("persona05_boundary_readiness", "") or "").strip()
    clean_evidence = int(row.get("persona05_clean_evidence_count", 0) or 0)
    return bool(boundary_status) and boundary_status != "not_applicable" and (
        boundary_readiness in {"fail", "blocked", "future_candidate"} or clean_evidence > 0
    )
