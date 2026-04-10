"""Shared pipeline schema constants and small normalization helpers."""

from __future__ import annotations

import json
from typing import Iterable

UNKNOWN_VALUES = {"", "unknown", "null", "none", "nan", "other", "unspecified", "unspecified_output", "unassigned"}

RECORD_ID_FIELDS = ["episode_id", "raw_id", "raw_source_id", "id"]
RECORD_TEXT_FIELDS = [
    "normalized_episode",
    "evidence_snippet",
    "business_question",
    "bottleneck_text",
    "workaround_text",
    "desired_output",
    "title",
    "body",
    "body_text",
    "comments_text",
    "raw_text",
    "thread_title",
    "parent_context",
]
RECORD_SOURCE_TEXT_FIELDS = ["body", "body_text", "comments_text", "raw_text", "thread_title", "parent_context"]
SOURCE_META_JSON_KEY = "json"
ROLE_HEAVY_NAME_TERMS = ["analyst", "manager", "marketer", "user", "persona"]
TOOL_HEAVY_NAME_TERMS = ["power bi", "tableau", "excel", "looker", "sigma", "google sheets", "sheets"]
GENERIC_PERSONA_NAMES = {"mixed workflow friction", "workflow friction", "mixed persona", "persona"}

LABEL_CODE_COLUMNS = [
    "role_codes",
    "moment_codes",
    "question_codes",
    "pain_codes",
    "env_codes",
    "workaround_codes",
    "output_codes",
    "fit_code",
]

CORE_LABEL_COLUMNS = [
    "role_codes",
    "question_codes",
    "pain_codes",
    "output_codes",
]

THEME_COLUMNS = [
    "question_codes",
    "pain_codes",
    "output_codes",
    "workaround_codes",
    "env_codes",
]

QUALITY_FLAG_OK = "OK"
QUALITY_FLAG_LOW = "LOW QUALITY"
QUALITY_FLAG_EXPLORATORY = "EXPLORATORY"
QUALITY_FLAG_UNSTABLE = "UNSTABLE"
QUALITY_UNKNOWN_RATIO_THRESHOLD = 0.30

FINAL_WORKBOOK_SHEET_NAMES = [
    "overview",
    "counts",
    "source_distribution",
    "taxonomy_summary",
    "cluster_stats",
    "persona_summary",
    "persona_axes",
    "persona_needs",
    "persona_cooccurrence",
    "persona_examples",
    "quality_checks",
    "source_diagnostics",
    "quality_failures",
    "metric_glossary",
]

WORKBOOK_SHEET_NAMES = FINAL_WORKBOOK_SHEET_NAMES

WORKBOOK_COLUMN_ORDERS = {
    "overview": ["metric", "value"],
    "counts": ["metric", "count", "denominator_type", "denominator_value", "definition"],
    "source_distribution": [
        "source",
        "raw_count",
        "normalized_count",
        "valid_count",
        "prefiltered_valid_count",
        "episode_count",
        "labeled_count",
        "share_of_labeled",
        "denominator_type",
        "denominator_value",
    ],
    "taxonomy_summary": ["axis_name", "why_it_matters", "allowed_values_or_logic", "evidence_fields"],
    "cluster_stats": [
        "persona_id",
        "persona_size",
        "share_of_total",
        "denominator_type",
        "denominator_value",
        "min_cluster_size",
        "promotion_status",
        "promotion_reason",
        "dominant_signature",
        "dominant_bottleneck",
        "dominant_analysis_goal",
    ],
    "persona_summary": [
        "persona_id",
        "persona_name",
        "persona_size",
        "share_of_total",
        "denominator_type",
        "denominator_value",
        "min_cluster_size",
        "promotion_status",
        "one_line_summary",
        "dominant_bottleneck",
        "main_workflow_context",
        "analysis_behavior",
        "trust_explanation_need",
        "current_tool_dependency",
        "primary_output_expectation",
        "top_pain_points",
        "representative_examples",
        "why_this_persona_matters",
    ],
    "persona_axes": ["persona_id", "axis_name", "axis_value", "count", "pct_of_persona"],
    "persona_needs": ["persona_id", "pain_or_need", "count", "pct_of_persona", "rank"],
    "persona_cooccurrence": ["persona_id", "theme_a", "theme_b", "pair_count", "pct_of_persona", "rank"],
    "persona_examples": ["persona_id", "example_rank", "grounded_text", "why_selected", "matched_axes", "reason_selected"],
    "quality_checks": ["metric", "value", "threshold", "status", "level", "denominator_type", "denominator_value", "notes"],
    "source_diagnostics": [
        "source",
        "raw_count",
        "normalized_count",
        "valid_count",
        "prefiltered_valid_count",
        "episode_count",
        "labelable_count",
        "labeled_count",
        "promoted_to_persona_count",
        "failure_reason_top",
        "failure_level",
        "recommended_seed_set",
    ],
    "quality_failures": ["metric", "level", "value", "threshold", "passed"],
    "metric_glossary": ["metric", "denominator_type", "definition"],
}

WORKBOOK_RATIO_COLUMNS = {
    "source_distribution": {"share_of_labeled": 1},
    "cluster_stats": {"share_of_total": 1},
    "persona_summary": {"share_of_total": 1},
    "persona_axes": {"pct_of_persona": 1},
    "persona_needs": {"pct_of_persona": 1},
    "persona_cooccurrence": {"pct_of_persona": 1},
}


def is_unknown_like(value: object) -> bool:
    """Return whether a scalar value should be treated as unresolved."""
    return str(value or "").strip().lower() in UNKNOWN_VALUES


def split_pipe_codes(value: object) -> list[str]:
    """Split a pipe-delimited code string and remove unknown markers."""
    text = str(value or "").strip()
    if not text:
        return []
    results: list[str] = []
    for raw in text.split("|"):
        item = str(raw or "").strip()
        if not item or is_unknown_like(item):
            continue
        results.append(item)
    return results


def round_ratio(value: float, digits: int = 6) -> float:
    """Round a ratio deterministically."""
    return round(float(value), digits)


def round_pct(count: int | float, total: int | float, digits: int = 1) -> float:
    """Convert count / total to a rounded percentage."""
    denominator = max(float(total), 1.0)
    return round((float(count) / denominator) * 100, digits)


def compute_quality_flag(unknown_ratio: float) -> str:
    """Return the workbook-level quality flag from deterministic thresholds."""
    return QUALITY_FLAG_LOW if float(unknown_ratio) > QUALITY_UNKNOWN_RATIO_THRESHOLD else QUALITY_FLAG_OK


def reorder_frame_columns(sheet_name: str, df):
    """Reorder workbook columns deterministically when a sheet contract is defined."""
    import pandas as pd

    if df is None:
        return pd.DataFrame()
    frame = df.copy()
    preferred = WORKBOOK_COLUMN_ORDERS.get(sheet_name, [])
    if not preferred:
        return frame
    ordered = [column for column in preferred if column in frame.columns]
    extras = [column for column in frame.columns if column not in ordered]
    return frame[ordered + extras]


def round_frame_ratios(sheet_name: str, df):
    """Round workbook ratio columns deterministically when configured."""
    import pandas as pd

    if df is None:
        return pd.DataFrame()
    frame = df.copy()
    ratio_columns = WORKBOOK_RATIO_COLUMNS.get(sheet_name, {})
    for column, digits in ratio_columns.items():
        if column not in frame.columns:
            continue
        frame[column] = pd.to_numeric(frame[column], errors="coerce").round(int(digits))
    return frame


def row_has_unknown_labels(values: Iterable[object]) -> bool:
    """Return whether any label-family value remains unresolved."""
    return any(is_unknown_like(value) for value in values)


def contains_any_term(value: object, terms: Iterable[object]) -> bool:
    """Return whether a string contains any candidate substring."""
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    return any(str(term or "").strip().lower() in lowered for term in terms if str(term or "").strip())


def parse_json_dict(value: object, nested_json_key: str = SOURCE_META_JSON_KEY) -> dict:
    """Parse a dict-or-JSON payload into a plain dictionary."""
    if isinstance(value, dict):
        nested = value.get(nested_json_key)
        if isinstance(nested, str):
            try:
                parsed = json.loads(nested)
                return parsed if isinstance(parsed, dict) else {key: item for key, item in value.items() if key != nested_json_key}
            except json.JSONDecodeError:
                return {key: item for key, item in value.items() if key != nested_json_key}
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}
