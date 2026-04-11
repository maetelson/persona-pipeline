"""Unknown-cause classification helpers for label quality analysis."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.utils.pipeline_schema import CORE_LABEL_COLUMNS, is_unknown_like
from src.utils.record_access import get_record_text

TEXT_FIELDS = [
    "normalized_episode",
    "evidence_snippet",
    "business_question",
    "bottleneck_text",
    "workaround_text",
    "desired_output",
]

ANNOUNCEMENT_TERMS = [
    "launched:",
    "guided help article",
    "welcome to the community",
    "new guided troubleshooter",
    "resources and videos",
]

PRODUCT_SUPPORT_TERMS = [
    "upgrade",
    "migration",
    "connector",
    "connection",
    "permissions",
    "permission",
    "query timeout",
    "native sql",
    "stored procedure",
    "uuid field filter",
    "dropdown filter",
    "dashboard filter",
    "field filter",
    "database fails",
    "not syncing",
    "syncing all views",
    "metadata sync",
    "sql server",
    "vertica",
    "snowflake",
    "postgresql",
    "mongodb",
    "mysql",
    "databricks",
    "driver",
    "login",
    "session parameters",
    "cache",
    "drilldown",
    "drill down",
]

ROLE_TERMS = ["analyst", "stakeholder", "leadership", "manager", "marketer", "merchant", "client"]
QUESTION_TERMS = [
    "why",
    "root cause",
    "diagnose",
    "report",
    "reporting",
    "export",
    "automation",
    "automate",
    "not showing",
    "disapproved",
    "review",
]
PAIN_TERMS = [
    "manual",
    "mismatch",
    "not working",
    "broken",
    "issue",
    "problem",
    "limitation",
    "handoff",
]
OUTPUT_TERMS = [
    "dashboard",
    "report",
    "excel",
    "export",
    "sheet",
    "monitor",
    "tracking",
    "automation",
    "workflow",
]

REMEDIATION_BY_REASON = {
    "labelability_failure_product_support": "Tighten prefilter or labelability rules for product-support and admin troubleshooting threads.",
    "labelability_failure_announcement": "Strengthen source-specific announcement suppression before labeling.",
    "too_generic_or_noisy": "Improve low-signal gating and episode segmentation so weak rows do not enter persona labeling.",
    "workflow_stage_missing": "Extend deterministic output-stage repairs and output taxonomy hints.",
    "analysis_goal_missing": "Extend question-goal repair rules and goal taxonomy coverage.",
    "bottleneck_axis_missing": "Extend pain-axis repair rules and bottleneck taxonomy coverage.",
    "role_missing": "Extend role inference heuristics for report owners, operators, and marketers.",
    "taxonomy_missing_multi_axis": "Expand taxonomy coverage or add source-aware repair rules for multi-axis gaps.",
    "parser_schema_mismatch": "Inspect parser/prompt contract for rows with strong evidence but unresolved labels.",
    "multi_axis_conflict": "Add disambiguation rules or allow broader multi-axis handling for conflicting evidence.",
}


def infer_axis_unknown_reason(row: pd.Series, family: str, text: str | None = None) -> str:
    """Return an actionable unknown reason for one unresolved label family."""
    lowered = (text or get_record_text(row, fields=TEXT_FIELDS)).lower()
    labelability_status = str(row.get("labelability_status", "") or "")
    confidence = float(row.get("label_confidence", 0.0) or 0.0)
    fit_code = str(row.get("fit_code", "unknown") or "unknown")

    if any(term in lowered for term in ANNOUNCEMENT_TERMS):
        return "labelability_failure_announcement"
    if labelability_status == "low_signal":
        if any(term in lowered for term in PRODUCT_SUPPORT_TERMS):
            return "labelability_failure_product_support"
        return "too_generic_or_noisy"
    if family == "role_codes" and any(term in lowered for term in ROLE_TERMS):
        return "role_missing"
    if family == "question_codes" and any(term in lowered for term in QUESTION_TERMS):
        return "analysis_goal_missing"
    if family == "pain_codes" and any(term in lowered for term in PAIN_TERMS):
        return "bottleneck_axis_missing"
    if family == "output_codes" and any(term in lowered for term in OUTPUT_TERMS):
        return "workflow_stage_missing"
    if family == "role_codes" and any(term in lowered for term in ["campaign", "stakeholder", "analyst", "leadership"]):
        return "multi_axis_conflict"
    if confidence >= 0.75 and fit_code != "unknown":
        return "parser_schema_mismatch"
    if len(lowered.strip()) < 80:
        return "too_generic_or_noisy"
    return "taxonomy_missing_multi_axis"


def infer_row_unknown_reason(row: pd.Series, failed_axes: list[str], failed_reasons: list[str], text: str | None = None) -> str:
    """Return the primary row-level unknown cause from failed axes and row context."""
    lowered = (text or get_record_text(row, fields=TEXT_FIELDS)).lower()
    labelability_status = str(row.get("labelability_status", "") or "")
    failed_axis_set = set(failed_axes)
    failed_reason_set = {reason for reason in failed_reasons if reason}

    if any(term in lowered for term in ANNOUNCEMENT_TERMS):
        return "labelability_failure_announcement"
    if labelability_status == "low_signal":
        if any(term in lowered for term in PRODUCT_SUPPORT_TERMS):
            return "labelability_failure_product_support"
        return "too_generic_or_noisy"
    if "multi_axis_conflict" in failed_reason_set:
        return "multi_axis_conflict"
    if failed_axis_set == {"output_codes"}:
        return "workflow_stage_missing"
    if failed_axis_set == {"question_codes"}:
        return "analysis_goal_missing"
    if failed_axis_set == {"pain_codes"}:
        return "bottleneck_axis_missing"
    if failed_axis_set == {"role_codes"}:
        return "role_missing"
    if "parser_schema_mismatch" in failed_reason_set:
        return "parser_schema_mismatch"
    return "taxonomy_missing_multi_axis"


def build_unknown_reason_breakdown(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    details_df: pd.DataFrame,
    sample_limit: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build row-level unknown cause rows and aggregated breakdown for unresolved core labels."""
    if labeled_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    episode_columns = [column for column in ["episode_id", "source", *TEXT_FIELDS] if column in episodes_df.columns]
    merged = labeled_df.merge(episodes_df[episode_columns], on="episode_id", how="left", suffixes=("", "_episode"))
    core_columns = [column for column in CORE_LABEL_COLUMNS if column in merged.columns]
    unknown_mask = merged[core_columns].fillna("unknown").apply(lambda row: any(is_unknown_like(value) for value in row.tolist()), axis=1)
    unknown_rows = merged.loc[unknown_mask].copy()
    if unknown_rows.empty:
        return pd.DataFrame(), pd.DataFrame()

    unknown_details = details_df[
        details_df["predicted_label"].fillna("").astype(str).eq("unknown")
        & details_df["axis_name"].isin(CORE_LABEL_COLUMNS)
    ].copy()
    grouped_details = unknown_details.groupby("episode_id", dropna=False).agg(
        failed_axes=("axis_name", lambda values: list(dict.fromkeys(str(value) for value in values if str(value)))),
        failed_reasons=("unknown_reason", lambda values: list(dict.fromkeys(str(value) for value in values if str(value)))),
    )
    unknown_rows = unknown_rows.merge(grouped_details, on="episode_id", how="left")
    unknown_rows["failed_axes"] = unknown_rows["failed_axes"].apply(lambda value: value if isinstance(value, list) else [])
    unknown_rows["failed_reasons"] = unknown_rows["failed_reasons"].apply(lambda value: value if isinstance(value, list) else [])
    unknown_rows["text_excerpt"] = unknown_rows.apply(lambda row: get_record_text(row, fields=TEXT_FIELDS)[:220], axis=1)
    unknown_rows["unknown_reason"] = unknown_rows.apply(
        lambda row: infer_row_unknown_reason(
            row,
            failed_axes=list(row.get("failed_axes", []) or []),
            failed_reasons=list(row.get("failed_reasons", []) or []),
            text=str(row.get("text_excerpt", "") or ""),
        ),
        axis=1,
    )
    unknown_rows["likely_remediation_type"] = unknown_rows["unknown_reason"].map(REMEDIATION_BY_REASON).fillna("Inspect row-level evidence and expand the nearest repair rule.")
    unknown_rows["missing_core_axes"] = unknown_rows[core_columns].fillna("unknown").apply(
        lambda row: " | ".join([column for column, value in row.items() if is_unknown_like(value)]),
        axis=1,
    )

    total_unknown = int(len(unknown_rows))
    grouped = unknown_rows.groupby("unknown_reason", dropna=False)
    breakdown_rows: list[dict[str, Any]] = []
    for unknown_reason, group in grouped:
        sample_rows = []
        for _, sample in group.head(sample_limit).iterrows():
            sample_rows.append(f"{sample['episode_id']} :: {str(sample.get('text_excerpt', '') or '')}")
        breakdown_rows.append(
            {
                "unknown_reason": str(unknown_reason or "unknown"),
                "count": int(len(group)),
                "share_of_unknown": round(float(len(group)) / float(total_unknown), 6),
                "sample_rows": " || ".join(sample_rows),
                "likely_remediation_type": str(group["likely_remediation_type"].iloc[0]),
            }
        )
    breakdown_df = pd.DataFrame(breakdown_rows).sort_values(["count", "unknown_reason"], ascending=[False, True]).reset_index(drop=True)

    preferred = [
        "episode_id",
        "source",
        "labelability_status",
        "label_confidence",
        "fit_code",
        "unknown_reason",
        "likely_remediation_type",
        "missing_core_axes",
        "failed_axes",
        "failed_reasons",
        "text_excerpt",
    ]
    available = [column for column in preferred if column in unknown_rows.columns]
    return breakdown_df, unknown_rows[available].reset_index(drop=True)