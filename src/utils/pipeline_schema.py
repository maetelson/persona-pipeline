"""Shared pipeline schema constants and small normalization helpers."""

from __future__ import annotations

import json
from collections import OrderedDict
import math
from typing import Iterable

UNKNOWN_VALUES = {"", "unknown", "null", "none", "nan", "other", "unspecified", "unspecified_output", "unassigned"}

EPISODE_ID_FIELD = "episode_id"
RAW_ID_FIELD = "raw_id"
SOURCE_FIELD = "source"
REDDIT_AGGREGATE_SOURCE = "reddit"
REDDIT_VARIANT_PREFIX = "reddit_"
DENOMINATOR_LABELED_EPISODE_ROWS = "labeled_episode_rows"
DENOMINATOR_PERSONA_CORE_LABELED_ROWS = "persona_core_labeled_rows"
DENOMINATOR_PROMOTED_PERSONA_ROWS = "promoted_persona_rows"
DENOMINATOR_RAW_RECORD_ROWS = "raw_record_rows"
DENOMINATOR_NORMALIZED_POST_ROWS = "normalized_post_rows"
DENOMINATOR_VALID_CANDIDATE_ROWS = "valid_candidate_rows"
DENOMINATOR_PREFILTERED_VALID_ROWS = "prefiltered_valid_rows"
DENOMINATOR_EPISODE_ROWS = "episode_rows"

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
TOOL_HEAVY_NAME_TERMS = ["power bi", "tableau", "excel", "sigma", "google sheets", "sheets"]
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
STATUS_OK = "OK"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"
QUALITY_UNKNOWN_RATIO_THRESHOLD = 0.30
CLUSTER_DOMINANCE_SHARE_PCT = 70.0
MIN_CLUSTER_SIZE_ABSOLUTE = 5
MIN_CLUSTER_SIZE_RATIO = 0.05

LABELABLE_STATUSES = {"labelable", "borderline"}
RAW_WITHOUT_LABEL_FAILURE_SOURCES = {
    "github_discussions",
    "hubspot_community",
    "klaviyo_community",
    "mixpanel_community",
    "qlik_community",
    "shopify_community",
}

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
        "workbook_readiness_state",
        "workbook_readiness_gate_status",
        "workbook_usage_restriction",
        "persona_size",
        "share_of_core_labeled",
        "share_of_all_labeled",
        "denominator_type",
        "denominator_value",
        "min_cluster_size",
        "base_promotion_status",
        "promoted_candidate_persona",
        "workbook_review_visible",
        "visibility_state",
        "final_usable_persona",
        "production_ready_persona",
        "review_ready_persona",
        "readiness_tier",
        "deck_ready_claim_eligible_persona",
        "deck_ready_claim_evidence_status",
        "deck_ready_claim_reason",
        "core_anchor_policy_status",
        "supporting_validation_policy_status",
        "exploratory_dependency_policy_status",
        "excluded_source_dependency_policy_status",
        "review_ready_reason",
        "blocked_reason",
        "workbook_policy_constraint",
        "review_visibility_status",
        "usability_state",
        "deck_ready_persona",
        "deck_readiness_state",
        "reporting_readiness_status",
        "promotion_action",
        "promotion_status",
        "grounding_status",
        "promotion_grounding_status",
        "promotion_reason",
        "grounding_reason",
        "grounded_candidate_count",
        "weak_candidate_count",
        "context_evidence_count",
        "workaround_evidence_count",
        "trust_validation_evidence_count",
        "bundle_episode_count",
        "bundle_dimension_hits",
        "total_bundle_strength",
        "bundle_grounding_status",
        "bundle_grounding_reason",
        "selected_example_count",
        "fallback_selected_count",
        "dominant_signature",
        "dominant_bottleneck",
        "dominant_analysis_goal",
    ],
    "persona_summary": [
        "persona_id",
        "persona_schema_version",
        "workbook_readiness_state",
        "workbook_readiness_gate_status",
        "workbook_usage_restriction",
        "persona_name",
        "persona_profile_name",
        "legacy_persona_name",
        "persona_size",
        "share_of_core_labeled",
        "share_of_all_labeled",
        "denominator_type",
        "denominator_value",
        "min_cluster_size",
        "base_promotion_status",
        "promoted_candidate_persona",
        "workbook_review_visible",
        "visibility_state",
        "final_usable_persona",
        "production_ready_persona",
        "review_ready_persona",
        "readiness_tier",
        "deck_ready_claim_eligible_persona",
        "deck_ready_claim_evidence_status",
        "deck_ready_claim_reason",
        "core_anchor_policy_status",
        "supporting_validation_policy_status",
        "exploratory_dependency_policy_status",
        "excluded_source_dependency_policy_status",
        "review_ready_reason",
        "blocked_reason",
        "workbook_policy_constraint",
        "review_visibility_status",
        "usability_state",
        "deck_ready_persona",
        "deck_readiness_state",
        "reporting_readiness_status",
        "promotion_action",
        "promotion_status",
        "grounding_status",
        "promotion_grounding_status",
        "promotion_reason",
        "grounding_reason",
        "grounded_candidate_count",
        "weak_candidate_count",
        "context_evidence_count",
        "workaround_evidence_count",
        "trust_validation_evidence_count",
        "bundle_episode_count",
        "bundle_dimension_hits",
        "total_bundle_strength",
        "bundle_grounding_status",
        "bundle_grounding_reason",
        "bundle_support_examples",
        "selected_example_count",
        "fallback_selected_count",
        "one_line_summary",
        "user_role_family",
        "functional_context",
        "stakeholder_exposure",
        "decision_responsibility",
        "recurring_job_to_be_done",
        "typical_trigger_event",
        "expected_output_artifact",
        "frequency_of_need",
        "primary_bottleneck",
        "secondary_bottleneck",
        "trust_failure_mode",
        "workaround_pattern",
        "why_current_tools_fail",
        "why_this_persona_would_use_our_product",
        "activation_moment",
        "success_signal",
        "role_context_json",
        "work_loop_json",
        "bottleneck_pattern_json",
        "product_relevance_json",
        "derivation_basis",
        "derivation_evidence_summary",
        "dominant_bottleneck",
        "main_workflow_context",
        "analysis_behavior",
        "trust_explanation_need",
        "current_tool_dependency",
        "primary_output_expectation",
        "top_pain_points",
        "representative_examples",
        "why_this_persona_matters",
        "legacy_cluster_name",
    ],
    "persona_axes": ["persona_id", "axis_name", "axis_value", "count", "pct_of_persona"],
    "persona_needs": ["persona_id", "pain_or_need", "count", "pct_of_persona", "rank"],
    "persona_cooccurrence": ["persona_id", "theme_a", "theme_b", "pair_count", "pct_of_persona", "rank"],
    "persona_examples": [
        "persona_id",
        "example_rank",
        "grounded_text",
        "selection_strength",
        "grounding_strength",
        "fallback_selected",
        "coverage_selection_reason",
        "grounding_reason",
        "why_selected",
        "matched_axes",
        "reason_selected",
        "quote_quality",
        "grounding_fit_score",
        "mismatch_count",
        "critical_mismatch_count",
        "matched_axis_count",
        "final_example_score",
    ],
    "quality_checks": ["metric", "value", "threshold", "status", "level", "denominator_type", "denominator_value", "notes"],
    "source_diagnostics": [
        "source",
        "section",
        "row_kind",
        "grain",
        "metric_name",
        "metric_value",
        "metric_type",
        "denominator_metric",
        "denominator_grain",
        "denominator_value",
        "bounded_range",
        "is_same_grain_funnel",
        "diagnostic_level",
        "metric_definition",
    ],
    "quality_failures": ["metric", "level", "value", "threshold", "passed"],
    "metric_glossary": ["metric", "denominator_type", "definition"],
}

WORKBOOK_RATIO_COLUMNS = {
    "source_distribution": {"share_of_labeled": 1},
    "cluster_stats": {"share_of_core_labeled": 1, "share_of_all_labeled": 1},
    "persona_summary": {"share_of_core_labeled": 1, "share_of_all_labeled": 1},
    "persona_axes": {"pct_of_persona": 1},
    "persona_needs": {"pct_of_persona": 1},
    "persona_cooccurrence": {"pct_of_persona": 1},
    "source_diagnostics": {},
}

PIPELINE_STAGE_DEFINITIONS = OrderedDict(
    [
        (
            DENOMINATOR_RAW_RECORD_ROWS,
            {
                "sheet_label": "Raw Record Rows",
                "definition": "Non-empty JSONL rows under data/raw/{source}/*.jsonl.",
                "artifact": "data/raw/{source}/*.jsonl",
            },
        ),
        (
            DENOMINATOR_NORMALIZED_POST_ROWS,
            {
                "sheet_label": "Normalized Post Rows",
                "definition": "Rows in data/normalized/normalized_posts.parquet after source normalizers.",
                "artifact": "data/normalized/normalized_posts.parquet",
            },
        ),
        (
            DENOMINATOR_VALID_CANDIDATE_ROWS,
            {
                "sheet_label": "Valid Candidate Rows",
                "definition": "Rows in data/valid/valid_candidates.parquet after invalid filtering and before relevance prefiltering.",
                "artifact": "data/valid/valid_candidates.parquet",
            },
        ),
        (
            DENOMINATOR_PREFILTERED_VALID_ROWS,
            {
                "sheet_label": "Prefiltered Valid Rows",
                "definition": "Rows in data/valid/valid_candidates_prefiltered.parquet passed into episode building when present.",
                "artifact": "data/valid/valid_candidates_prefiltered.parquet",
            },
        ),
        (
            DENOMINATOR_EPISODE_ROWS,
            {
                "sheet_label": "Episode Rows",
                "definition": "Rows in data/episodes/episode_table.parquet.",
                "artifact": "data/episodes/episode_table.parquet",
            },
        ),
        (
            DENOMINATOR_LABELED_EPISODE_ROWS,
            {
                "sheet_label": "Labeled Episode Rows",
                "definition": "Rows in data/labeled/labeled_episodes.parquet.",
                "artifact": "data/labeled/labeled_episodes.parquet",
            },
        ),
    ]
)

PIPELINE_STAGE_METRIC_NAMES = tuple(PIPELINE_STAGE_DEFINITIONS.keys())

LEGACY_STAGE_METRIC_ALIASES = {
    "raw_records": DENOMINATOR_RAW_RECORD_ROWS,
    "normalized_records": DENOMINATOR_NORMALIZED_POST_ROWS,
    "valid_records": DENOMINATOR_VALID_CANDIDATE_ROWS,
    "prefiltered_valid_records": DENOMINATOR_PREFILTERED_VALID_ROWS,
    "episodes": DENOMINATOR_EPISODE_ROWS,
    "labeled_records": DENOMINATOR_LABELED_EPISODE_ROWS,
    "total_raw_count": DENOMINATOR_RAW_RECORD_ROWS,
    "cleaned_count": DENOMINATOR_VALID_CANDIDATE_ROWS,
    "labeled_count": DENOMINATOR_LABELED_EPISODE_ROWS,
    "total_labeled_records": DENOMINATOR_LABELED_EPISODE_ROWS,
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


def persona_min_cluster_size(labeled_count: int | float) -> int:
    """Return the default floor for promoting a cluster into a persona."""
    return max(MIN_CLUSTER_SIZE_ABSOLUTE, int(math.ceil(float(labeled_count) * MIN_CLUSTER_SIZE_RATIO)))


def is_single_cluster_dominant(largest_share: int | float) -> bool:
    """Return whether one cluster exceeds the dominance gate."""
    return float(largest_share) > CLUSTER_DOMINANCE_SHARE_PCT


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


def canonical_source_name(source: object) -> str:
    """Return the workbook/reporting source key used for aggregation."""
    value = str(source or "").strip()
    if value.startswith(REDDIT_VARIANT_PREFIX):
        return REDDIT_AGGREGATE_SOURCE
    return value


def pipeline_stage_definition(metric: object) -> dict[str, str]:
    """Return metadata for one canonical pipeline-stage metric."""
    return dict(PIPELINE_STAGE_DEFINITIONS.get(str(metric or "").strip(), {}))


def is_pipeline_stage_metric(metric: object) -> bool:
    """Return whether a metric name is one canonical pipeline-stage count."""
    return str(metric or "").strip() in PIPELINE_STAGE_DEFINITIONS


def canonical_stage_metric_name(metric: object) -> str:
    """Normalize legacy workbook aliases to the canonical stage metric name."""
    value = str(metric or "").strip()
    if value in PIPELINE_STAGE_DEFINITIONS:
        return value
    return LEGACY_STAGE_METRIC_ALIASES.get(value, value)


def share_column_for_denominator(denominator_type: object) -> str:
    """Return the canonical share column name for a denominator."""
    mapping = {
        DENOMINATOR_LABELED_EPISODE_ROWS: "share_of_all_labeled",
        DENOMINATOR_PERSONA_CORE_LABELED_ROWS: "share_of_core_labeled",
        DENOMINATOR_PROMOTED_PERSONA_ROWS: "share_of_promoted_persona_rows",
    }
    return mapping.get(str(denominator_type or "").strip(), "")


def source_row_count(df, source: str, source_column: str = SOURCE_FIELD) -> int:
    """Return row count for one source from a dataframe-like table."""
    import pandas as pd

    if df is None or df.empty or source_column not in df.columns:
        return 0
    canonical = canonical_source_name(source)
    return int(df[source_column].astype(str).map(canonical_source_name).eq(canonical).sum())


def aggregated_source_count(df, source: str, count_column: str, source_column: str = SOURCE_FIELD) -> int:
    """Return a numeric count for one source in a pre-aggregated dataframe."""
    import pandas as pd

    if df is None or df.empty or source_column not in df.columns or count_column not in df.columns:
        return 0
    canonical = canonical_source_name(source)
    match = df[df[source_column].astype(str).map(canonical_source_name).eq(canonical)]
    return int(pd.to_numeric(match[count_column], errors="coerce").fillna(0).sum()) if not match.empty else 0


def unique_record_count(df, column: str = EPISODE_ID_FIELD) -> int:
    """Return unique non-empty values for an identifier column."""
    if df is None or df.empty or column not in df.columns:
        return 0
    values = df[column].fillna("").astype(str).str.strip()
    return int(values[values != ""].nunique())


def top_non_unknown_value(df, column: str, fallback: str = "unassigned") -> str:
    """Return the dominant non-unknown value for one dataframe column."""
    if df is None or df.empty or column not in df.columns:
        return fallback
    series = df[column].astype(str).str.strip()
    series = series[~series.map(is_unknown_like)]
    if series.empty:
        return fallback
    return str(series.value_counts().idxmax())


def collect_pipe_codes_from_frame(df, columns: Iterable[str]) -> list[str]:
    """Collect split pipe-delimited codes across selected dataframe columns."""
    import pandas as pd

    if df is None or df.empty:
        return []
    values: list[str] = []
    for column in columns:
        for raw_value in df.get(column, pd.Series(dtype=str)):
            values.extend(split_pipe_codes(raw_value))
    return values


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
