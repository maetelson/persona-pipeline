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

WORKFLOW_TERMS = [
    "report",
    "reporting",
    "dashboard",
    "export",
    "download",
    "monitor",
    "triage",
    "validate",
    "reconcile",
    "diagnose",
    "review",
    "filter",
    "sync",
    "automation",
]

BOTTLENECK_SIGNAL_TERMS = [
    "broken",
    "failed",
    "fails",
    "error",
    "issue",
    "problem",
    "not working",
    "not showing",
    "not syncing",
    "mismatch",
    "incorrect",
    "manual",
    "limitation",
    "blocked",
]

GENERIC_CHATTER_NOT_PERSONA_USABLE = "generic_chatter_not_persona_usable"
MISSING_TAXONOMY_VALUE = "missing_taxonomy_value"
OVERLY_STRICT_AXIS_REQUIREMENT = "overly_strict_axis_requirement"
WEAK_WORKFLOW_CONTEXT = "weak_workflow_context"
INSUFFICIENT_BOTTLENECK_SIGNAL = "insufficient_bottleneck_signal"
OUTPUT_EXPECTATION_NOT_CAPTURED = "output_expectation_not_captured"
PARSER_SCHEMA_MISMATCH = "parser_schema_mismatch"
MULTI_AXIS_CONFLICT = "multi_axis_conflict"

SUPPORTED_LOW_SIGNAL_UNKNOWN_CATEGORIES = {
    MISSING_TAXONOMY_VALUE,
    OVERLY_STRICT_AXIS_REQUIREMENT,
    WEAK_WORKFLOW_CONTEXT,
    INSUFFICIENT_BOTTLENECK_SIGNAL,
    OUTPUT_EXPECTATION_NOT_CAPTURED,
}

REMEDIATION_BY_CATEGORY = {
    GENERIC_CHATTER_NOT_PERSONA_USABLE: "Keep these rows out of persona-core and improve low-signal suppression upstream.",
    MISSING_TAXONOMY_VALUE: "Expand deterministic taxonomy coverage or add source-aware repair rules for stable multi-axis gaps.",
    OVERLY_STRICT_AXIS_REQUIREMENT: "Preserve and repair low-signal rows that already express a stable persona signature instead of blanking them wholesale.",
    WEAK_WORKFLOW_CONTEXT: "Extend workflow-stage inference from reporting, triage, validation, and automation cues.",
    INSUFFICIENT_BOTTLENECK_SIGNAL: "Strengthen bottleneck inference for product friction, data issues, and operational blockers.",
    OUTPUT_EXPECTATION_NOT_CAPTURED: "Broaden output and delivery heuristics for dashboard, export, validation, and automation expectations.",
    PARSER_SCHEMA_MISMATCH: "Inspect parser or prompt contract for rows with strong evidence but unresolved labels.",
    MULTI_AXIS_CONFLICT: "Add explicit disambiguation rules or allow broader multi-axis handling for conflicting evidence.",
}

CATEGORY_BY_REASON = {
    "labelability_failure_product_support": OVERLY_STRICT_AXIS_REQUIREMENT,
    "labelability_failure_announcement": GENERIC_CHATTER_NOT_PERSONA_USABLE,
    "too_generic_or_noisy": GENERIC_CHATTER_NOT_PERSONA_USABLE,
    MISSING_TAXONOMY_VALUE: MISSING_TAXONOMY_VALUE,
    OVERLY_STRICT_AXIS_REQUIREMENT: OVERLY_STRICT_AXIS_REQUIREMENT,
    WEAK_WORKFLOW_CONTEXT: WEAK_WORKFLOW_CONTEXT,
    INSUFFICIENT_BOTTLENECK_SIGNAL: INSUFFICIENT_BOTTLENECK_SIGNAL,
    OUTPUT_EXPECTATION_NOT_CAPTURED: OUTPUT_EXPECTATION_NOT_CAPTURED,
    PARSER_SCHEMA_MISMATCH: PARSER_SCHEMA_MISMATCH,
    MULTI_AXIS_CONFLICT: MULTI_AXIS_CONFLICT,
    "workflow_stage_missing": WEAK_WORKFLOW_CONTEXT,
    "analysis_goal_missing": OVERLY_STRICT_AXIS_REQUIREMENT,
    "bottleneck_axis_missing": INSUFFICIENT_BOTTLENECK_SIGNAL,
    "role_missing": MISSING_TAXONOMY_VALUE,
    "taxonomy_missing_multi_axis": MISSING_TAXONOMY_VALUE,
    "parser_schema_mismatch": PARSER_SCHEMA_MISMATCH,
    "multi_axis_conflict": MULTI_AXIS_CONFLICT,
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
    if "multi_axis_conflict" in failed_reason_set:
        return MULTI_AXIS_CONFLICT
    if "parser_schema_mismatch" in failed_reason_set:
        return PARSER_SCHEMA_MISMATCH
    category = infer_unknown_category(
        row,
        failed_axes=failed_axes,
        failed_reasons=failed_reasons,
        text=lowered,
    )
    if labelability_status == "low_signal":
        if category == GENERIC_CHATTER_NOT_PERSONA_USABLE:
            return "too_generic_or_noisy"
        return category
    if failed_axis_set == {"output_codes"}:
        return "workflow_stage_missing"
    if failed_axis_set == {"question_codes"}:
        return "analysis_goal_missing"
    if failed_axis_set == {"pain_codes"}:
        return "bottleneck_axis_missing"
    if failed_axis_set == {"role_codes"}:
        return "role_missing"
    if category in CATEGORY_BY_REASON:
        return category
    return "taxonomy_missing_multi_axis"


def infer_unknown_category(row: pd.Series, failed_axes: list[str], failed_reasons: list[str], text: str | None = None) -> str:
    """Map row-level unknown evidence into explicit persona-core coverage categories."""
    lowered = (text or get_record_text(row, fields=TEXT_FIELDS)).lower()
    labelability_status = str(row.get("labelability_status", "") or "")
    failed_axis_set = set(failed_axes)
    failed_reason_set = {reason for reason in failed_reasons if reason}

    if any(term in lowered for term in ANNOUNCEMENT_TERMS):
        return GENERIC_CHATTER_NOT_PERSONA_USABLE
    if "multi_axis_conflict" in failed_reason_set:
        return MULTI_AXIS_CONFLICT
    if "parser_schema_mismatch" in failed_reason_set:
        return PARSER_SCHEMA_MISMATCH
    if labelability_status == "low_signal" and any(term in lowered for term in PRODUCT_SUPPORT_TERMS):
        return OVERLY_STRICT_AXIS_REQUIREMENT
    if failed_axis_set == {"output_codes"}:
        return OUTPUT_EXPECTATION_NOT_CAPTURED
    if "workflow_stage_missing" in failed_reason_set or ("output_codes" in failed_axis_set and _has_workflow_signal(lowered)):
        return WEAK_WORKFLOW_CONTEXT
    if failed_axis_set == {"pain_codes"} or "bottleneck_axis_missing" in failed_reason_set:
        return INSUFFICIENT_BOTTLENECK_SIGNAL
    if failed_axis_set == {"question_codes"} or "analysis_goal_missing" in failed_reason_set:
        return OVERLY_STRICT_AXIS_REQUIREMENT
    if failed_axis_set == {"role_codes"} or "role_missing" in failed_reason_set:
        return MISSING_TAXONOMY_VALUE
    if "taxonomy_missing_multi_axis" in failed_reason_set:
        if _has_output_signal(lowered) and "output_codes" in failed_axis_set:
            return OUTPUT_EXPECTATION_NOT_CAPTURED
        if _has_bottleneck_signal(lowered) and "pain_codes" in failed_axis_set:
            return INSUFFICIENT_BOTTLENECK_SIGNAL
        if _has_workflow_signal(lowered) and "output_codes" in failed_axis_set:
            return WEAK_WORKFLOW_CONTEXT
        return MISSING_TAXONOMY_VALUE
    if labelability_status == "low_signal":
        if _has_output_signal(lowered):
            return OUTPUT_EXPECTATION_NOT_CAPTURED
        if _has_bottleneck_signal(lowered):
            return INSUFFICIENT_BOTTLENECK_SIGNAL
        if _has_workflow_signal(lowered):
            return WEAK_WORKFLOW_CONTEXT
        return GENERIC_CHATTER_NOT_PERSONA_USABLE
    if len(lowered.strip()) < 80:
        return GENERIC_CHATTER_NOT_PERSONA_USABLE
    return MISSING_TAXONOMY_VALUE


def category_from_unknown_reason(reason: str) -> str:
    """Return the explicit coverage category for a detailed unknown reason."""
    return CATEGORY_BY_REASON.get(str(reason or "").strip(), MISSING_TAXONOMY_VALUE)


def is_supportable_low_signal_category(category: str) -> bool:
    """Return whether a low-signal row can still contribute to persona-core after repair."""
    return str(category or "").strip() in SUPPORTED_LOW_SIGNAL_UNKNOWN_CATEGORIES


def persona_core_policy_for_category(category: str) -> str:
    """Expose whether the category is supportable for persona-core coverage repair."""
    return "supportable_low_signal" if is_supportable_low_signal_category(category) else "exclude_low_signal"


def _has_output_signal(text: str) -> bool:
    return any(term in text for term in OUTPUT_TERMS + ["xlsx", "csv", "download", "sheet", "board report"])


def _has_workflow_signal(text: str) -> bool:
    return any(term in text for term in WORKFLOW_TERMS)


def _has_bottleneck_signal(text: str) -> bool:
    return any(term in text for term in PAIN_TERMS + BOTTLENECK_SIGNAL_TERMS)


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
    unknown_rows["root_cause_category"] = unknown_rows.apply(
        lambda row: infer_unknown_category(
            row,
            failed_axes=list(row.get("failed_axes", []) or []),
            failed_reasons=list(row.get("failed_reasons", []) or []),
            text=str(row.get("text_excerpt", "") or ""),
        ),
        axis=1,
    )
    unknown_rows["persona_core_policy"] = unknown_rows["root_cause_category"].map(persona_core_policy_for_category)
    unknown_rows["likely_remediation_type"] = unknown_rows["root_cause_category"].map(REMEDIATION_BY_CATEGORY).fillna(
        "Inspect row-level evidence and expand the nearest repair rule."
    )
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
                "root_cause_category": str(group["root_cause_category"].iloc[0] or ""),
                "persona_core_policy": str(group["persona_core_policy"].iloc[0] or ""),
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
        "root_cause_category",
        "persona_core_policy",
        "likely_remediation_type",
        "missing_core_axes",
        "failed_axes",
        "failed_reasons",
        "text_excerpt",
    ]
    available = [column for column in preferred if column in unknown_rows.columns]
    return breakdown_df, unknown_rows[available].reset_index(drop=True)