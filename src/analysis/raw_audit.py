"""Helpers for collection and downstream loss audit tables."""

from __future__ import annotations

import pandas as pd

RAW_AUDIT_COLUMNS = [
    "source",
    "raw_record_count",
    "raw_path",
    "collector_mode",
    "status",
    "error_message",
    "page_error_count",
]

PAGE_AUDIT_COLUMNS = [
    "source",
    "query_id",
    "query_text",
    "window_id",
    "window_start",
    "window_end",
    "page_no",
    "page_raw_count",
    "page_raw_count_before_dedupe",
    "duplicate_count",
    "duplicate_ratio",
    "stop_reason",
]

ERROR_AUDIT_COLUMNS = [
    "source",
    "query_id",
    "query_text",
    "window_id",
    "window_start",
    "window_end",
    "page_no",
    "error_stage",
    "error_type",
    "error_code",
    "error_message",
    "is_retryable",
]

ERROR_SUMMARY_COLUMNS = [
    "source",
    "query_id",
    "query_text",
    "window_id",
    "error_code",
    "error_type",
    "error_count",
    "retryable_error_count",
]

SUMMARY_COLUMNS = [
    "source",
    "query_id",
    "query_text",
    "window_id",
    "window_start",
    "window_end",
    "raw_count",
]


def build_raw_audit_df(source_rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build the source-level raw audit table with a stable column order."""
    return pd.DataFrame(source_rows, columns=RAW_AUDIT_COLUMNS)


def build_error_summary_df(error_audit_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate page-level errors into a compact per-query summary table."""
    if error_audit_df.empty:
        return pd.DataFrame(columns=ERROR_SUMMARY_COLUMNS)

    return (
        error_audit_df.groupby(
            ["source", "query_id", "query_text", "window_id", "error_code", "error_type"],
            dropna=False,
        )
        .agg(
            error_count=("error_message", "count"),
            retryable_error_count=("is_retryable", "sum"),
        )
        .reset_index()
        .sort_values(
            ["source", "error_count", "query_id", "window_id"],
            ascending=[True, False, True, True],
        )
        .reset_index(drop=True)
    )


def build_summary_df(page_audit_df: pd.DataFrame) -> pd.DataFrame:
    """Build one raw-count row per source/query/window combination."""
    if page_audit_df.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    return (
        page_audit_df.groupby(
            ["source", "query_id", "query_text", "window_id", "window_start", "window_end"],
            dropna=False,
        )["page_raw_count"]
        .sum()
        .reset_index(name="raw_count")
        .sort_values(["source", "raw_count", "query_id", "window_id"], ascending=[True, False, True, True])
        .reset_index(drop=True)
    )


def build_raw_query_window_matrix(page_audit_df: pd.DataFrame) -> pd.DataFrame:
    """Build a source × query × window matrix with page and duplicate stats."""
    if page_audit_df.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "query_id",
                "query_text",
                "window_id",
                "window_start",
                "window_end",
                "pages_seen",
                "raw_count",
                "duplicate_count",
                "avg_duplicate_ratio",
                "final_stop_reason",
            ]
        )

    grouped = (
        page_audit_df.groupby(
            ["source", "query_id", "query_text", "window_id", "window_start", "window_end"],
            dropna=False,
        )
        .agg(
            pages_seen=("page_no", "max"),
            raw_count=("page_raw_count", "sum"),
            duplicate_count=("duplicate_count", "sum"),
            avg_duplicate_ratio=("duplicate_ratio", "mean"),
        )
        .reset_index()
    )
    last_stop = (
        page_audit_df.sort_values(["source", "query_id", "window_id", "page_no"])
        .groupby(["source", "query_id", "window_id"], dropna=False)
        .tail(1)[["source", "query_id", "window_id", "stop_reason"]]
        .rename(columns={"stop_reason": "final_stop_reason"})
    )
    return grouped.merge(last_stop, on=["source", "query_id", "window_id"], how="left")


def build_low_yield_query_audit(query_window_df: pd.DataFrame, low_yield_threshold: int = 1) -> pd.DataFrame:
    """Highlight query windows that produced zero or near-zero raw rows."""
    if query_window_df.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "query_id",
                "query_text",
                "window_id",
                "raw_count",
                "pages_seen",
                "low_yield_flag",
            ]
        )

    result = query_window_df.copy()
    result["low_yield_flag"] = result["raw_count"].fillna(0).astype(int) <= int(low_yield_threshold)
    return result[result["low_yield_flag"]].reset_index(drop=True)


def build_downstream_loss_audit(
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    invalid_df: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize source-level loss after normalization and validity filtering."""
    sources = sorted(
        set(normalized_df.get("source", pd.Series(dtype=str)).dropna().astype(str).tolist())
        | set(valid_df.get("source", pd.Series(dtype=str)).dropna().astype(str).tolist())
        | set(invalid_df.get("source", pd.Series(dtype=str)).dropna().astype(str).tolist())
    )
    rows: list[dict[str, object]] = []
    for source in sources:
        normalized_count = int((normalized_df["source"] == source).sum()) if "source" in normalized_df.columns else 0
        valid_count = int((valid_df["source"] == source).sum()) if "source" in valid_df.columns else 0
        invalid_count = int((invalid_df["source"] == source).sum()) if "source" in invalid_df.columns else 0
        denominator = max(normalized_count, 1)
        rows.append(
            {
                "source": source,
                "normalized_count": normalized_count,
                "valid_count": valid_count,
                "invalid_count": invalid_count,
                "valid_ratio": round(valid_count / denominator, 4),
                "invalid_ratio": round(invalid_count / denominator, 4),
            }
        )
    return pd.DataFrame(rows)
