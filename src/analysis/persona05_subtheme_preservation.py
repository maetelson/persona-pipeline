"""Subtheme-preservation annotations for persona_05 in analysis outputs only."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


PERSONA_05_ID = "persona_05"
PERSONA_03_ID = "persona_03"

ROOT_SUBTHEME_IMPLEMENTATION_JSON = "artifacts/readiness/persona05_subtheme_preservation_implementation.json"
ROOT_SUBTHEME_IMPLEMENTATION_MD = "docs/operational/PERSONA05_SUBTHEME_PRESERVATION_IMPLEMENTATION.md"

SUBTHEME_COLUMNS = [
    "subtheme_status",
    "parent_persona_id",
    "parent_persona_relation",
    "future_candidate_subtheme",
    "subtheme_reason",
    "standalone_persona_recommended",
    "claim_eligible_recommended",
    "related_subtheme_ids",
]


def build_persona05_subtheme_outputs(
    persona_summary_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_promotion_path_debug_df: pd.DataFrame,
) -> dict[str, Any]:
    """Annotate persona-facing analysis outputs with persona_05 subtheme-preservation fields."""
    annotated_cluster_stats_df = _annotate_subtheme_frame(cluster_stats_df)
    annotated_persona_summary_df = _merge_subtheme_fields(persona_summary_df, annotated_cluster_stats_df)
    annotated_promotion_path_debug_df = _merge_subtheme_fields(
        persona_promotion_path_debug_df,
        annotated_cluster_stats_df,
    )
    report = _build_report(annotated_cluster_stats_df)
    return {
        "persona_summary_df": annotated_persona_summary_df,
        "cluster_stats_df": annotated_cluster_stats_df,
        "persona_promotion_path_debug_df": annotated_promotion_path_debug_df,
        "report": report,
    }


def write_persona05_subtheme_artifacts(root_dir: Path, report: dict[str, Any]) -> dict[str, Path]:
    """Write one small implementation report for persona_05 subtheme preservation."""
    json_path = root_dir / ROOT_SUBTHEME_IMPLEMENTATION_JSON
    md_path = root_dir / ROOT_SUBTHEME_IMPLEMENTATION_MD
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(_report_markdown(report), encoding="utf-8")
    return {
        "persona05_subtheme_preservation_implementation_json": json_path,
        "persona05_subtheme_preservation_implementation_md": md_path,
    }


def _annotate_subtheme_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Add deterministic subtheme-preservation fields to one persona-facing frame."""
    if frame.empty:
        annotated = frame.copy()
        for column in SUBTHEME_COLUMNS:
            annotated[column] = pd.Series(dtype=object)
        return annotated
    annotated = frame.copy()
    for column in SUBTHEME_COLUMNS:
        if column in {"future_candidate_subtheme", "standalone_persona_recommended", "claim_eligible_recommended"}:
            annotated[column] = False
        else:
            annotated[column] = ""
    annotated["subtheme_status"] = "not_applicable"
    annotated["standalone_persona_recommended"] = True
    annotated["claim_eligible_recommended"] = annotated.get(
        "deck_ready_claim_eligible_persona",
        pd.Series(False, index=annotated.index),
    ).fillna(False).astype(bool)

    persona03_mask = annotated["persona_id"].astype(str).eq(PERSONA_03_ID)
    annotated.loc[persona03_mask, "related_subtheme_ids"] = PERSONA_05_ID

    persona05_mask = annotated["persona_id"].astype(str).eq(PERSONA_05_ID)
    annotated.loc[persona05_mask, "subtheme_status"] = "future_candidate_subtheme"
    annotated.loc[persona05_mask, "parent_persona_id"] = PERSONA_03_ID
    annotated.loc[persona05_mask, "parent_persona_relation"] = "delivery_specific_subtheme"
    annotated.loc[persona05_mask, "future_candidate_subtheme"] = True
    annotated.loc[
        persona05_mask,
        "subtheme_reason",
    ] = (
        "Last-mile reporting output construction blocked by tool limitations is real, "
        "but current evidence is too overlap-heavy and too thin for standalone persona treatment."
    )
    annotated.loc[persona05_mask, "standalone_persona_recommended"] = False
    annotated.loc[persona05_mask, "claim_eligible_recommended"] = False
    annotated.loc[persona05_mask, "related_subtheme_ids"] = ""
    return annotated


def _merge_subtheme_fields(target_df: pd.DataFrame, source_df: pd.DataFrame) -> pd.DataFrame:
    """Merge centralized subtheme-preservation fields into another persona-facing frame."""
    if target_df.empty:
        return target_df.copy()
    merge_columns = ["persona_id", *SUBTHEME_COLUMNS]
    cleaned = target_df.drop(columns=[column for column in SUBTHEME_COLUMNS if column in target_df.columns], errors="ignore")
    return cleaned.merge(source_df[merge_columns], on="persona_id", how="left")


def _build_report(cluster_stats_df: pd.DataFrame) -> dict[str, Any]:
    """Build one small report describing the persona_05 subtheme-preservation implementation state."""
    row = (
        cluster_stats_df[cluster_stats_df["persona_id"].astype(str).eq(PERSONA_05_ID)].iloc[0].to_dict()
        if not cluster_stats_df.empty and cluster_stats_df["persona_id"].astype(str).eq(PERSONA_05_ID).any()
        else {}
    )
    persona03 = (
        cluster_stats_df[cluster_stats_df["persona_id"].astype(str).eq(PERSONA_03_ID)].iloc[0].to_dict()
        if not cluster_stats_df.empty and cluster_stats_df["persona_id"].astype(str).eq(PERSONA_03_ID).any()
        else {}
    )
    return {
        "persona_id": PERSONA_05_ID,
        "subtheme_fields_added": SUBTHEME_COLUMNS,
        "persona_05_field_values": {
            "subtheme_status": row.get("subtheme_status", ""),
            "parent_persona_id": row.get("parent_persona_id", ""),
            "parent_persona_relation": row.get("parent_persona_relation", ""),
            "future_candidate_subtheme": bool(row.get("future_candidate_subtheme", False)),
            "subtheme_reason": row.get("subtheme_reason", ""),
            "standalone_persona_recommended": bool(row.get("standalone_persona_recommended", False)),
            "claim_eligible_recommended": bool(row.get("claim_eligible_recommended", False)),
            "production_ready_persona": bool(row.get("production_ready_persona", False)),
            "review_ready_persona": bool(row.get("review_ready_persona", False)),
            "final_usable_persona": bool(row.get("final_usable_persona", False)),
            "deck_ready_claim_eligible_persona": bool(row.get("deck_ready_claim_eligible_persona", False)),
            "readiness_tier": row.get("readiness_tier", ""),
        },
        "persona_03_related_subtheme_ids": persona03.get("related_subtheme_ids", ""),
    }


def _report_markdown(report: dict[str, Any]) -> str:
    """Render one concise Markdown implementation note."""
    fields = report.get("persona_05_field_values", {})
    return "\n".join(
        [
            "## Persona 05 Subtheme Preservation Implementation",
            "",
            "This pass adds subtheme-preservation fields to analysis outputs only.",
            "",
            f"- `subtheme_status = {fields.get('subtheme_status', '')}`",
            f"- `parent_persona_id = {fields.get('parent_persona_id', '')}`",
            f"- `parent_persona_relation = {fields.get('parent_persona_relation', '')}`",
            f"- `future_candidate_subtheme = {fields.get('future_candidate_subtheme', False)}`",
            f"- `standalone_persona_recommended = {fields.get('standalone_persona_recommended', False)}`",
            f"- `claim_eligible_recommended = {fields.get('claim_eligible_recommended', False)}`",
            "",
            "No readiness, promotion, claim-eligibility, or final-usable semantics were changed.",
        ]
    )
