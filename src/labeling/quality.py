"""Label-quality audits, unknown analysis, and QA sample builders."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir, write_parquet
from src.utils.pipeline_schema import LABEL_CODE_COLUMNS, is_unknown_like
from src.utils.record_access import get_record_text

TEXT_FIELDS = [
    "normalized_episode",
    "evidence_snippet",
    "business_question",
    "bottleneck_text",
    "workaround_text",
    "desired_output",
]


def build_label_quality_audit(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    details_df: pd.DataFrame,
    labelability_df: pd.DataFrame,
    axis_wide_df: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Build axis- and source-level quality audits plus QA samples."""
    merged = episodes_df.merge(labeled_df, on="episode_id", how="left")
    if "labelability_status" not in merged.columns:
        merge_columns = ["episode_id", "source", "labelability_status", "labelability_score", "labelability_reason", "persona_core_eligible"]
        available = [column for column in merge_columns if column in labelability_df.columns]
        merged = merged.merge(labelability_df[available], on=["episode_id", "source"], how="left")
    axis_rows: list[dict[str, Any]] = []
    for axis_name in LABEL_CODE_COLUMNS:
        values = labeled_df.get(axis_name, pd.Series(dtype=str)).fillna("unknown").astype(str)
        unknown_mask = values.map(is_unknown_like)
        known = values[~unknown_mask]
        distribution = known.value_counts(normalize=True).head(8)
        dominant_share = float(distribution.iloc[0]) if not distribution.empty else 0.0
        axis_rows.append(
            {
                "axis_name": axis_name,
                "total_labeled_rows": len(labeled_df),
                "known_coverage": round(float((~unknown_mask).mean()), 4) if len(values) else 0.0,
                "unknown_rate": round(float(unknown_mask.mean()), 4) if len(values) else 0.0,
                "distinct_class_count": int(known.nunique()),
                "class_imbalance": round(dominant_share, 4),
                "label_distribution": " | ".join(f"{label}:{share:.3f}" for label, share in distribution.items()),
                "low_information_rows": int(
                    details_df[(details_df["axis_name"] == axis_name) & (details_df["unknown_reason"] == "low_relevance_input")].shape[0]
                ),
                "worst_unknown_reason": _top_value(
                    details_df[(details_df["axis_name"] == axis_name) & (details_df["predicted_label"] == "unknown")]["unknown_reason"]
                ),
                "cluster_usefulness_hint": "high" if axis_name in {"question_codes", "pain_codes"} else "medium",
            }
        )
    axis_df = pd.DataFrame(axis_rows).sort_values(["unknown_rate", "class_imbalance"], ascending=[False, False]).reset_index(drop=True)

    source_rows: list[dict[str, Any]] = []
    for source, group in merged.groupby("source", dropna=False):
        row: dict[str, Any] = {"source": source, "row_count": len(group)}
        for axis_name in LABEL_CODE_COLUMNS:
            values = group[axis_name].fillna("unknown").astype(str) if axis_name in group.columns else pd.Series(dtype=str)
            row[f"{axis_name}_unknown_rate"] = round(float(values.map(is_unknown_like).mean()), 4) if len(values) else 0.0
        row["low_signal_rate"] = round(float((group["labelability_status"] == "low_signal").mean()), 4) if "labelability_status" in group.columns else 0.0
        source_rows.append(row)
    source_df = pd.DataFrame(source_rows).sort_values("low_signal_rate", ascending=False).reset_index(drop=True)

    low_signal_df = merged[merged["labelability_status"] == "low_signal"].copy()
    borderline_df = merged[merged["labelability_status"] == "borderline"].copy()
    return {
        "unknown_by_axis_df": axis_df,
        "unknown_by_source_df": source_df,
        "low_signal_rows_df": _sample_rows(low_signal_df, 200),
        "borderline_rows_df": _sample_rows(borderline_df, 200),
        "top_unknown_examples_df": build_top_unknown_examples(merged, details_df, limit=50),
    }


def build_top_unknown_examples(merged_df: pd.DataFrame, details_df: pd.DataFrame, limit: int = 50) -> pd.DataFrame:
    """Return the most unknown-heavy examples with a short failure explanation."""
    if merged_df.empty or details_df.empty:
        return pd.DataFrame()
    unknown_counts = (
        details_df.assign(is_unknown=details_df["predicted_label"].eq("unknown"))
        .groupby("episode_id", dropna=False)["is_unknown"]
        .sum()
        .reset_index(name="unknown_axis_count")
    )
    failed_axes = (
        details_df[details_df["predicted_label"] == "unknown"]
        .groupby("episode_id", dropna=False)
        .agg(
            failed_axes=("axis_name", lambda values: " | ".join(sorted(dict.fromkeys(values)))),
            failed_reasons=("unknown_reason", lambda values: " | ".join(sorted(dict.fromkeys(values)))),
        )
        .reset_index()
    )
    merged = merged_df.merge(unknown_counts, on="episode_id", how="left").merge(failed_axes, on="episode_id", how="left")
    merged["unknown_axis_count"] = merged["unknown_axis_count"].fillna(0)
    merged["text_length"] = merged.apply(lambda row: len(get_record_text(row, fields=TEXT_FIELDS)), axis=1)
    merged["failure_explanation"] = merged.apply(_failure_explanation, axis=1)
    columns = [
        "episode_id",
        "source",
        "labelability_status",
        "unknown_axis_count",
        "failed_axes",
        "failed_reasons",
        "failure_explanation",
        "normalized_episode",
    ]
    return merged.sort_values(["unknown_axis_count", "text_length"], ascending=[False, False]).head(limit)[columns].reset_index(drop=True)


def write_label_quality_outputs(root_dir: Path, outputs: dict[str, pd.DataFrame], repaired_df: pd.DataFrame, details_df: pd.DataFrame) -> dict[str, Path]:
    """Persist label-quality artifacts for QA and before/after comparison."""
    labeled_dir = ensure_dir(root_dir / "data" / "labeled")
    analysis_dir = ensure_dir(root_dir / "data" / "analysis")
    paths = {
        "unknown_by_axis_csv": analysis_dir / "unknown_by_axis.csv",
        "unknown_by_source_csv": analysis_dir / "unknown_by_source.csv",
        "low_signal_rows_csv": analysis_dir / "low_signal_rows.csv",
        "borderline_rows_csv": analysis_dir / "borderline_rows_for_review.csv",
        "repaired_labels_csv": analysis_dir / "repaired_labels.csv",
        "top_unknown_examples_csv": analysis_dir / "top_unknown_examples.csv",
        "label_quality_audit_md": analysis_dir / "label_quality_audit.md",
        "label_details_parquet": labeled_dir / "label_details.parquet",
    }
    outputs["unknown_by_axis_df"].to_csv(paths["unknown_by_axis_csv"], index=False)
    outputs["unknown_by_source_df"].to_csv(paths["unknown_by_source_csv"], index=False)
    outputs["low_signal_rows_df"].to_csv(paths["low_signal_rows_csv"], index=False)
    outputs["borderline_rows_df"].to_csv(paths["borderline_rows_csv"], index=False)
    repaired_df.to_csv(paths["repaired_labels_csv"], index=False)
    outputs["top_unknown_examples_df"].to_csv(paths["top_unknown_examples_csv"], index=False)
    write_parquet(details_df, paths["label_details_parquet"])
    paths["label_quality_audit_md"].write_text(_audit_markdown(outputs), encoding="utf-8")
    return paths


def _sample_rows(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if df.empty:
        return df
    frame = df.copy()
    frame["text_excerpt"] = frame.apply(lambda row: get_record_text(row, fields=TEXT_FIELDS)[:300], axis=1)
    preferred = ["episode_id", "source", "labelability_status", "labelability_score", "labelability_reason", "text_excerpt"]
    available = [column for column in preferred if column in frame.columns]
    return frame[available].head(limit).reset_index(drop=True)


def _top_value(series: pd.Series) -> str:
    values = series.fillna("").astype(str)
    values = values[values != ""]
    if values.empty:
        return ""
    return str(values.value_counts().index[0])


def _failure_explanation(row: pd.Series) -> str:
    reasons = str(row.get("failed_reasons", "") or "")
    labelability = str(row.get("labelability_status", "") or "")
    text = str(row.get("normalized_episode", "") or "")
    if labelability == "low_signal":
        return "Row is too weak or too noisy for persona labeling and should not drive clustering."
    if "conflicting_evidence" in reasons:
        return "The row contains mixed directional signals, so broad axes conflict and the current taxonomy cannot safely choose one."
    if "taxonomy_gap" in reasons:
        return "The row is relevant but the current label set does not cleanly capture the expressed problem."
    if len(text) < 80:
        return "The row has too little grounded evidence to support a reliable label."
    return "The row is relevant, but the current evidence does not strongly support the failed axes."


def _audit_markdown(outputs: dict[str, pd.DataFrame]) -> str:
    axis_df = outputs["unknown_by_axis_df"]
    source_df = outputs["unknown_by_source_df"]
    lines = ["# Label Quality Audit", "", "## Worst Axes", ""]
    for _, row in axis_df.head(8).iterrows():
        lines.append(
            f"- `{row['axis_name']}`: unknown_rate={row['unknown_rate']}, known_coverage={row['known_coverage']}, "
            f"worst_unknown_reason={row['worst_unknown_reason']}, distribution={row['label_distribution']}"
        )
    lines.extend(["", "## Source Unknown Rates", ""])
    for _, row in source_df.head(8).iterrows():
        lines.append(
            f"- `{row['source']}`: low_signal_rate={row['low_signal_rate']}, "
            f"role_unknown={row.get('role_codes_unknown_rate', 0)}, output_unknown={row.get('output_codes_unknown_rate', 0)}"
        )
    lines.append("")
    return "\n".join(lines)
