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
        metrics["llm_targeted_count"] = int(llm_audit_df["was_llm_targeted"].fillna(False).sum())
        metrics["llm_called_count"] = int(llm_audit_df["was_llm_called"].fillna(False).sum())
        metrics["llm_batch_count"] = int(
            ((llm_audit_df["llm_mode"] == "batch") & llm_audit_df["was_llm_targeted"].fillna(False)).sum()
        )
        metrics["llm_success_count"] = int(llm_audit_df["llm_status"].isin(["applied", "no_change"]).sum())
        metrics["llm_failed_count"] = int((llm_audit_df["llm_status"] == "failed").sum())
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
        "was_rule_labeled",
        "was_llm_targeted",
        "was_llm_called",
        "llm_mode",
        "llm_target_reason",
        "llm_status",
        "llm_reason",
        "model_used",
        "usage_input_tokens",
        "usage_output_tokens",
        "usage_total_tokens",
        "cost_estimate_optional",
        "parse_success",
        "fallback_used",
        "label_confidence",
        "label_reason",
        "unknown_remaining",
        "missing_core_family",
    ]
    remainder = [column for column in detail.columns if column not in preferred]
    return detail[preferred + remainder]


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
