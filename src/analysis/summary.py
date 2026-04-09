"""High-level source and run summaries."""

from __future__ import annotations

import pandas as pd
from src.utils.pipeline_schema import (
    CORE_LABEL_COLUMNS,
    LABEL_CODE_COLUMNS,
    compute_quality_flag,
    row_has_unknown_labels,
    round_pct,
)


def build_source_summary(normalized_df: pd.DataFrame, valid_df: pd.DataFrame, episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize counts by source across core stages."""
    sources = sorted(set(normalized_df.get("source", pd.Series(dtype=str)).tolist()))
    rows: list[dict[str, int | str]] = []
    for source in sources:
        rows.append(
            {
                "source": source,
                "normalized_count": int((normalized_df["source"] == source).sum()) if not normalized_df.empty else 0,
                "valid_count": int((valid_df["source"] == source).sum()) if not valid_df.empty else 0,
                "episode_count": int((episodes_df["source"] == source).sum()) if not episodes_df.empty else 0,
            }
        )
    return pd.DataFrame(rows, columns=["source", "normalized_count", "valid_count", "episode_count"])


def build_counts_table(
    raw_audit_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build deterministic top-line pipeline counts for the final report."""
    total_raw_count = int(raw_audit_df.get("raw_record_count", pd.Series(dtype=int)).fillna(0).sum()) if not raw_audit_df.empty else 0
    rows = [
        {"metric": "raw_records", "count": total_raw_count},
        {"metric": "normalized_records", "count": int(len(normalized_df))},
        {"metric": "valid_records", "count": int(len(valid_df))},
        {"metric": "episodes", "count": int(len(episodes_df))},
        {"metric": "labeled_records", "count": int(len(labeled_df))},
    ]
    return pd.DataFrame(rows)


def build_final_source_distribution(
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build final source distribution with labeled-share percentages."""
    source_df = build_source_summary(normalized_df, valid_df, episodes_df)
    labeled_with_source = (
        labeled_df[["episode_id"]].merge(episodes_df[["episode_id", "source"]], on="episode_id", how="left")
        if not labeled_df.empty and "episode_id" in labeled_df.columns and "episode_id" in episodes_df.columns
        else pd.DataFrame(columns=["source"])
    )
    sources = sorted(set(source_df.get("source", pd.Series(dtype=str)).tolist()) | set(labeled_with_source.get("source", pd.Series(dtype=str)).dropna().tolist()))
    total_labeled = int(len(labeled_with_source))
    rows: list[dict[str, object]] = []
    for source in sources:
        rows.append(
            {
                "source": source,
                "normalized_count": int((normalized_df["source"] == source).sum()) if not normalized_df.empty else 0,
                "valid_count": int((valid_df["source"] == source).sum()) if not valid_df.empty else 0,
                "episode_count": int((episodes_df["source"] == source).sum()) if not episodes_df.empty else 0,
                "labeled_count": int((labeled_with_source["source"] == source).sum()) if not labeled_with_source.empty else 0,
                "share_of_labeled": round_pct(int((labeled_with_source["source"] == source).sum()) if not labeled_with_source.empty else 0, total_labeled),
            }
        )
    return pd.DataFrame(rows)


def build_taxonomy_summary(final_axis_schema: list[dict[str, object]]) -> pd.DataFrame:
    """Convert final axis schema into a workbook-friendly taxonomy summary."""
    rows: list[dict[str, object]] = []
    for row in final_axis_schema:
        rows.append(
            {
                "axis_name": str(row.get("axis_name", "")).strip(),
                "why_it_matters": str(row.get("why_it_matters", "")).strip(),
                "allowed_values_or_logic": " | ".join(str(value) for value in list(row.get("allowed_values_or_logic", []) or row.get("allowed_values", []) or []))
                or str(row.get("clustering_logic", "")).strip(),
                "evidence_fields": " | ".join(str(value) for value in list(row.get("evidence_fields_used", []) or row.get("evidence_fields", []) or [])),
                "axis_role": str(row.get("axis_role", "core")).strip(),
                "reduction_decision": str(row.get("reduction_decision", "")).strip(),
            }
        )
    return pd.DataFrame(rows)


def build_quality_checks_df(quality_checks: dict[str, object]) -> pd.DataFrame:
    """Convert deterministic quality metrics into workbook rows."""
    thresholds = {
        "unknown_ratio": 0.30,
        "cluster_count": 2,
    }
    rows: list[dict[str, object]] = []
    for metric, value in quality_checks.items():
        if metric == "cluster_distribution":
            rows.append(
                {
                    "metric": metric,
                    "value": len(value) if isinstance(value, list) else 0,
                    "threshold": "",
                    "status": "info",
                    "notes": str(value)[:1000],
                }
            )
            continue
        threshold = thresholds.get(metric, "")
        status = "pass"
        notes = ""
        if metric == "unknown_ratio" and float(value) > 0.30:
            status = "warn"
            notes = "unknown ratio above recommended threshold"
        elif metric == "cluster_count" and int(value) < 2:
            status = "warn"
            notes = "too few clusters for robust persona comparison"
        elif metric == "quality_flag":
            status = "pass" if str(value) == "OK" else "warn"
            notes = "derived from unknown_ratio threshold"
        rows.append(
            {
                "metric": metric,
                "value": value,
                "threshold": threshold,
                "status": status,
                "notes": notes,
            }
        )
    return pd.DataFrame(rows)


def build_quality_checks(
    raw_audit_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    cluster_profiles: list[dict[str, object]],
) -> dict[str, object]:
    """Build analysis quality checks for persona-report readiness."""
    total_raw_count = int(raw_audit_df.get("raw_record_count", pd.Series(dtype=int)).fillna(0).sum()) if not raw_audit_df.empty else 0
    cleaned_count = int(len(valid_df))
    labeled_count = int(len(labeled_df))
    core_labeled_df = _persona_core_subset(labeled_df)
    unknown_ratio = round(_row_unknown_ratio(core_labeled_df), 6)
    overall_unknown_ratio = round(_row_unknown_ratio(labeled_df), 6)
    cluster_count = int(len(cluster_profiles))
    cluster_distribution = [
        {
            "cluster_id": str(row.get("cluster_id", "")),
            "size": int(row.get("size", 0)),
            "share_of_total": float(row.get("share_of_total", 0.0)),
        }
        for row in cluster_profiles
    ]
    return {
        "total_raw_count": total_raw_count,
        "cleaned_count": cleaned_count,
        "labeled_count": labeled_count,
        "persona_core_labeled_count": int(len(core_labeled_df)),
        "unknown_ratio": unknown_ratio,
        "overall_unknown_ratio": overall_unknown_ratio,
        "cluster_count": cluster_count,
        "cluster_distribution": cluster_distribution,
        "quality_flag": compute_quality_flag(unknown_ratio),
    }


def _persona_core_subset(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Use persona-core-eligible rows when available for quality scoring."""
    if labeled_df.empty or "persona_core_eligible" not in labeled_df.columns:
        return labeled_df
    return labeled_df[labeled_df["persona_core_eligible"].fillna(True)]
def _row_unknown_ratio(labeled_df: pd.DataFrame) -> float:
    """Return ratio of rows that still have any unresolved label family."""
    if labeled_df.empty:
        return 1.0
    label_columns = [column for column in CORE_LABEL_COLUMNS if column in labeled_df.columns]
    unknown_mask = labeled_df[label_columns].apply(lambda row: row_has_unknown_labels(row.tolist()), axis=1)
    return float(unknown_mask.mean())
