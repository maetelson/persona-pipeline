"""Audit helpers for labeling output."""

from __future__ import annotations

from typing import Any

import pandas as pd
from src.utils.pipeline_schema import CORE_LABEL_COLUMNS, LABEL_CODE_COLUMNS, row_has_unknown_labels


def build_label_audit(labeled_df: pd.DataFrame, llm_audit_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Summarize label coverage, LLM routing, call counts, and token usage."""
    if labeled_df.empty:
        return pd.DataFrame(columns=["metric", "value"])

    metrics: dict[str, Any] = {
        "total_episodes": len(labeled_df),
        "fit_code_known": int((labeled_df["fit_code"].fillna("") != "unknown").sum()),
        "role_code_known": int((labeled_df["role_codes"].fillna("") != "unknown").sum()),
        "question_code_unknown": int((labeled_df["question_codes"].fillna("") == "unknown").sum()),
        "pain_code_unknown": int((labeled_df["pain_codes"].fillna("") == "unknown").sum()),
        "env_code_unknown": int((labeled_df["env_codes"].fillna("") == "unknown").sum()),
        "unknown_remaining_count": int(_row_has_unknown(labeled_df).sum()),
        "missing_core_family_count": int(_row_has_missing_core(labeled_df).sum()),
        "role_coverage_ratio": round(_coverage_ratio(labeled_df, "role_codes"), 4),
        "question_coverage_ratio": round(_coverage_ratio(labeled_df, "question_codes"), 4),
        "pain_coverage_ratio": round(_coverage_ratio(labeled_df, "pain_codes"), 4),
        "output_coverage_ratio": round(_coverage_ratio(labeled_df, "output_codes"), 4),
        "role_dominant_code_share": round(_dominant_share(labeled_df, "role_codes"), 4),
        "question_dominant_code_share": round(_dominant_share(labeled_df, "question_codes"), 4),
        "pain_dominant_code_share": round(_dominant_share(labeled_df, "pain_codes"), 4),
    }
    if "labelability_status" in labeled_df.columns:
        metrics["labelable_count"] = int((labeled_df["labelability_status"] == "labelable").sum())
        metrics["borderline_count"] = int((labeled_df["labelability_status"] == "borderline").sum())
        metrics["low_signal_count"] = int((labeled_df["labelability_status"] == "low_signal").sum())
    if "persona_core_eligible" in labeled_df.columns:
        metrics["persona_core_eligible_count"] = int(labeled_df["persona_core_eligible"].fillna(False).sum())

    if llm_audit_df is not None and not llm_audit_df.empty:
        metrics["rule_labeled_only_count"] = int((llm_audit_df["llm_status"] == "not_targeted").sum())
        metrics["skipped_by_targeting_count"] = int((llm_audit_df["llm_status"] == "not_targeted").sum())
        metrics["llm_targeted_count"] = int(llm_audit_df["was_llm_targeted"].fillna(False).sum())
        metrics["uncached_targeted_count"] = int(
            llm_audit_df["was_llm_targeted"].fillna(False).sum()
            - (llm_audit_df["llm_status"] == "cache_hit").sum()
            - (llm_audit_df["llm_status"] == "run_reuse").sum()
        )
        metrics["llm_called_count"] = int(llm_audit_df["was_llm_called"].fillna(False).sum())
        metrics["live_call_attempt_count"] = int(llm_audit_df["was_llm_called"].fillna(False).sum())
        metrics["llm_cache_hit_count"] = int((llm_audit_df["llm_status"] == "cache_hit").sum())
        metrics["llm_run_reuse_count"] = int((llm_audit_df["llm_status"] == "run_reuse").sum())
        metrics["served_from_cache_count"] = int(
            (llm_audit_df["llm_status"] == "cache_hit").sum() + (llm_audit_df["llm_status"] == "run_reuse").sum()
        )
        metrics["only_uncached_filtered_count"] = int((llm_audit_df["llm_status"] == "only_uncached_filtered").sum())
        metrics["llm_batch_count"] = int(
            ((llm_audit_df["llm_mode"] == "batch") & llm_audit_df["was_llm_targeted"].fillna(False)).sum()
        )
        metrics["llm_success_count"] = int(llm_audit_df["llm_status"].isin(["applied", "no_change"]).sum())
        metrics["live_call_success_count"] = int(llm_audit_df["llm_status"].isin(["applied", "no_change"]).sum())
        metrics["llm_failed_count"] = int((llm_audit_df["llm_status"] == "failed").sum())
        metrics["live_call_failure_count"] = int((llm_audit_df["llm_status"] == "failed").sum())
        metrics["llm_usage_present_count"] = int(llm_audit_df.get("usage_present", pd.Series(dtype=bool)).fillna(False).sum())
        metrics["llm_retry_count_total"] = int(llm_audit_df.get("retry_count", pd.Series(dtype=int)).fillna(0).sum())
        metrics["fallback_count"] = int(llm_audit_df.get("fallback_used", pd.Series(dtype=bool)).fillna(False).sum())
        metrics["llm_disabled_count"] = int(
            llm_audit_df["llm_reason"].fillna("").astype(str).str.startswith("llm:disabled:").sum()
        )
        metrics["usage_input_tokens_total"] = int(llm_audit_df["usage_input_tokens"].fillna(0).sum())
        metrics["usage_output_tokens_total"] = int(llm_audit_df["usage_output_tokens"].fillna(0).sum())
        metrics["usage_total_tokens_total"] = int(llm_audit_df["usage_total_tokens"].fillna(0).sum())
        called_mask = llm_audit_df["was_llm_called"].fillna(False)
        if int(called_mask.sum()) > 0:
            metrics["avg_input_tokens"] = round(float(llm_audit_df.loc[called_mask, "usage_input_tokens"].fillna(0).mean()), 2)
            metrics["avg_output_tokens"] = round(float(llm_audit_df.loc[called_mask, "usage_output_tokens"].fillna(0).mean()), 2)
        else:
            metrics["avg_input_tokens"] = 0.0
            metrics["avg_output_tokens"] = 0.0

    return pd.DataFrame({"metric": list(metrics.keys()), "value": list(metrics.values())})


def build_labeling_audit(labeled_df: pd.DataFrame, llm_audit_df: pd.DataFrame) -> pd.DataFrame:
    """Create the row-level audit table required for LLM call diagnostics."""
    if labeled_df.empty:
        return pd.DataFrame(
            columns=[
                "episode_id",
                "was_rule_labeled",
                "was_llm_targeted",
                "was_llm_called",
                "llm_status",
                "llm_reason",
                "usage_input_tokens",
                "usage_output_tokens",
                "usage_total_tokens",
                "label_confidence",
                "label_reason",
                "unknown_remaining",
                "missing_core_family",
            ]
        )

    detail = labeled_df[["episode_id", "label_confidence", "label_reason"] + CORE_LABEL_COLUMNS].copy()
    for column in ["labelability_status", "labelability_score", "labelability_reason", "persona_core_eligible"]:
        if column in labeled_df.columns:
            detail[column] = labeled_df[column]
    detail["unknown_remaining"] = _row_has_unknown(detail)
    detail["missing_core_family"] = _row_has_missing_core(detail)
    if llm_audit_df is not None and not llm_audit_df.empty:
        detail = detail.merge(llm_audit_df, on="episode_id", how="left")

    preferred = [
        "episode_id",
        "audit_tag",
        "was_rule_labeled",
        "was_llm_targeted",
        "was_llm_called",
        "llm_mode",
        "llm_target_reason",
        "llm_status",
        "llm_reason",
        "model_used",
        "llm_job_id",
        "endpoint_used",
        "api_base_url",
        "openai_organization",
        "openai_project",
        "api_key_masked",
        "call_correlation_id",
        "response_id",
        "request_id",
        "http_status",
        "transport_error_class",
        "retry_count",
        "usage_present",
        "usage_input_tokens",
        "usage_output_tokens",
        "usage_total_tokens",
        "cost_estimate_optional",
        "parse_success",
        "fallback_used",
        "skip_category",
        "cache_source",
        "cache_key",
        "label_confidence",
        "label_reason",
        "unknown_remaining",
        "missing_core_family",
    ]
    remainder = [column for column in detail.columns if column not in preferred]
    return detail[preferred + remainder]


def build_llm_experiment_summary(llm_audit_df: pd.DataFrame, audit_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Create a compact experiment-oriented summary table for one labeled run."""
    if llm_audit_df is None or llm_audit_df.empty:
        return pd.DataFrame()

    metric_lookup: dict[str, Any] = {}
    if audit_df is not None and not audit_df.empty:
        metric_lookup = {
            str(row["metric"]): row["value"]
            for _, row in audit_df.iterrows()
            if "metric" in audit_df.columns and "value" in audit_df.columns
        }

    targeted_rows = int(metric_lookup.get("llm_targeted_count", int(llm_audit_df["was_llm_targeted"].fillna(False).sum())))
    served_from_cache = int(metric_lookup.get("served_from_cache_count", 0))
    live_calls = int(metric_lookup.get("live_call_attempt_count", int(llm_audit_df["was_llm_called"].fillna(False).sum())))
    audit_tag = ""
    if "audit_tag" in llm_audit_df.columns:
        non_empty_tags = llm_audit_df["audit_tag"].fillna("").astype(str)
        non_empty_tags = non_empty_tags[non_empty_tags != ""]
        if not non_empty_tags.empty:
            audit_tag = str(non_empty_tags.iloc[0])
    summary = {
        "audit_tag": audit_tag,
        "total_rows": int(metric_lookup.get("total_episodes", len(llm_audit_df))),
        "targeted_rows": targeted_rows,
        "skipped_by_targeting_count": int(metric_lookup.get("skipped_by_targeting_count", 0)),
        "cache_hit_count": int(metric_lookup.get("llm_cache_hit_count", 0)),
        "run_reuse_count": int(metric_lookup.get("llm_run_reuse_count", 0)),
        "served_from_cache_count": served_from_cache,
        "uncached_targeted_count": int(metric_lookup.get("uncached_targeted_count", max(targeted_rows - served_from_cache, 0))),
        "live_call_attempt_count": live_calls,
        "live_call_success_count": int(metric_lookup.get("live_call_success_count", 0)),
        "live_call_failure_count": int(metric_lookup.get("live_call_failure_count", 0)),
        "retry_count_total": int(metric_lookup.get("llm_retry_count_total", 0)),
        "fallback_count": int(metric_lookup.get("fallback_count", 0)),
        "usage_present_count": int(metric_lookup.get("llm_usage_present_count", 0)),
        "usage_input_tokens_total": int(metric_lookup.get("usage_input_tokens_total", 0)),
        "usage_output_tokens_total": int(metric_lookup.get("usage_output_tokens_total", 0)),
        "usage_total_tokens_total": int(metric_lookup.get("usage_total_tokens_total", 0)),
        "request_id_count": int(llm_audit_df.get("request_id", pd.Series(dtype=str)).fillna("").astype(str).ne("").sum()),
        "response_id_count": int(llm_audit_df.get("response_id", pd.Series(dtype=str)).fillna("").astype(str).ne("").sum()),
    }
    if targeted_rows > 0:
        summary["percent_targeted_from_cache"] = round((served_from_cache / targeted_rows) * 100.0, 2)
        summary["percent_targeted_live"] = round((live_calls / targeted_rows) * 100.0, 2)
    else:
        summary["percent_targeted_from_cache"] = 0.0
        summary["percent_targeted_live"] = 0.0

    return pd.DataFrame([summary])


def _row_has_unknown(df: pd.DataFrame) -> pd.Series:
    """Mark rows that still have any unresolved label family."""
    label_columns = [column for column in LABEL_CODE_COLUMNS if column in df.columns]
    return df[label_columns].apply(lambda row: row_has_unknown_labels(row.tolist()), axis=1)


def _row_has_missing_core(df: pd.DataFrame) -> pd.Series:
    """Mark rows missing one of the core persona families."""
    core = [column for column in CORE_LABEL_COLUMNS if column in df.columns]
    return df[core].fillna("unknown").eq("unknown").any(axis=1)


def _coverage_ratio(df: pd.DataFrame, column: str) -> float:
    """Return the ratio of rows with known labels in one family."""
    if df.empty or column not in df.columns:
        return 0.0
    return float((~df[column].fillna("").astype(str).eq("unknown")).mean())


def _dominant_share(df: pd.DataFrame, column: str) -> float:
    """Return the dominant exact-code share among known rows."""
    if df.empty or column not in df.columns:
        return 0.0
    known = df[column].fillna("").astype(str)
    known = known[known != "unknown"]
    if known.empty:
        return 0.0
    return float(known.value_counts(normalize=True).iloc[0])
