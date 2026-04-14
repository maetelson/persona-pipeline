"""Scale-up snapshot metrics for before/after pipeline comparisons."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.diagnostics import count_raw_jsonl_by_source
from src.utils.io import read_parquet


ANALYSIS_DIRNAME = "analysis"
BASELINE_PATHNAME = "pipeline_scale_metrics_baseline.json"
CURRENT_PATHNAME = "pipeline_scale_metrics_current.json"
OVERALL_CSV_PATHNAME = "pipeline_scale_metrics_overall.csv"
SOURCE_CSV_PATHNAME = "pipeline_scale_metrics_by_source.csv"
MARKDOWN_PATHNAME = "data/analysis/pipeline_metrics_before_after.md"


@dataclass(slots=True)
class ScaleMetricsSnapshot:
    """Serializable pipeline scale snapshot."""

    generated_at: str
    overall: list[dict[str, Any]]
    by_source: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-serializable payload."""
        return {
            "generated_at": self.generated_at,
            "overall": self.overall,
            "by_source": self.by_source,
        }


def build_scale_metrics_snapshot(root_dir: Path) -> ScaleMetricsSnapshot:
    """Build the current scale snapshot from pipeline artifacts."""
    raw_counts_df = count_raw_jsonl_by_source(root_dir)
    valid_df = _read_stage_parquet(root_dir / "data" / "valid" / "valid_candidates.parquet")
    prefiltered_df = _read_stage_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    episodes_df = _read_stage_parquet(root_dir / "data" / "episodes" / "episode_table.parquet")
    labeled_df = _attach_episode_source(
        _read_stage_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet"),
        episodes_df,
    )
    source_balance_df = _read_csv(root_dir / "data" / ANALYSIS_DIRNAME / "source_balance_audit.csv")
    overview_df = _read_csv(root_dir / "data" / ANALYSIS_DIRNAME / "overview.csv")

    source_rows = _build_source_rows(
        raw_counts_df=raw_counts_df,
        valid_df=valid_df,
        prefiltered_df=prefiltered_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        source_balance_df=source_balance_df,
    )
    overall_rows = _build_overall_rows(source_rows=source_rows, labeled_df=labeled_df, overview_df=overview_df)
    return ScaleMetricsSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        overall=overall_rows,
        by_source=source_rows.to_dict(orient="records"),
    )


def write_scale_metrics_outputs(root_dir: Path, set_baseline: bool = False) -> dict[str, Path]:
    """Write current scale metrics artifacts and a before/after markdown report."""
    analysis_dir = root_dir / "data" / ANALYSIS_DIRNAME
    analysis_dir.mkdir(parents=True, exist_ok=True)

    snapshot = build_scale_metrics_snapshot(root_dir)
    current_path = analysis_dir / CURRENT_PATHNAME
    baseline_path = analysis_dir / BASELINE_PATHNAME
    overall_csv_path = analysis_dir / OVERALL_CSV_PATHNAME
    source_csv_path = analysis_dir / SOURCE_CSV_PATHNAME
    markdown_path = root_dir / MARKDOWN_PATHNAME

    current_path.write_text(json.dumps(snapshot.to_dict(), indent=2), encoding="utf-8")
    pd.DataFrame(snapshot.overall).to_csv(overall_csv_path, index=False)
    pd.DataFrame(snapshot.by_source).to_csv(source_csv_path, index=False)

    if set_baseline or not baseline_path.exists():
        baseline_path.write_text(json.dumps(snapshot.to_dict(), indent=2), encoding="utf-8")

    baseline_snapshot = _load_snapshot(baseline_path)
    markdown_path.write_text(_render_markdown(snapshot, baseline_snapshot), encoding="utf-8")
    return {
        "current_json": current_path,
        "baseline_json": baseline_path,
        "overall_csv": overall_csv_path,
        "source_csv": source_csv_path,
        "markdown": markdown_path,
    }


def _read_stage_parquet(path: Path) -> pd.DataFrame:
    """Read a stage parquet file or return an empty frame."""
    if not path.exists():
        return pd.DataFrame()
    return read_parquet(path)


def _read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV file or return an empty frame."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _attach_episode_source(labeled_df: pd.DataFrame, episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Attach source to labeled rows using the episode table when needed."""
    if labeled_df.empty or "source" in labeled_df.columns:
        return labeled_df
    if episodes_df.empty or not {"episode_id", "source"}.issubset(episodes_df.columns):
        return labeled_df
    source_lookup = episodes_df[["episode_id", "source"]].drop_duplicates(subset=["episode_id"])
    return labeled_df.merge(source_lookup, on="episode_id", how="left")


def _build_source_rows(
    raw_counts_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    prefiltered_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    source_balance_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build the source-level scale table."""
    raw_lookup = _count_lookup(raw_counts_df, "source", "raw_count")
    valid_lookup = _value_counts(valid_df, "source")
    prefiltered_lookup = _value_counts(prefiltered_df, "source")
    episodes_lookup = _value_counts(episodes_df, "source")
    labeled_lookup = _value_counts(labeled_df, "source")
    persona_core_lookup = _persona_core_counts(labeled_df)
    labelable_lookup = _labelable_counts(labeled_df)
    blended_lookup = _count_lookup(source_balance_df, "source", "blended_influence_share_pct")
    labeled_share_lookup = _count_lookup(source_balance_df, "source", "labeled_share_pct")
    status_lookup = _string_lookup(source_balance_df, "source", "source_balance_status")
    failure_lookup = _string_lookup(source_balance_df, "source", "failure_reason_top")

    sources = sorted(
        set(raw_lookup)
        | set(valid_lookup)
        | set(prefiltered_lookup)
        | set(episodes_lookup)
        | set(labeled_lookup)
        | set(persona_core_lookup)
        | set(labelable_lookup)
    )
    rows: list[dict[str, Any]] = []
    for source in sources:
        rows.append(
            {
                "source": source,
                "raw": int(raw_lookup.get(source, 0) or 0),
                "valid": int(valid_lookup.get(source, 0) or 0),
                "prefiltered_valid": int(prefiltered_lookup.get(source, 0) or 0),
                "episodes": int(episodes_lookup.get(source, 0) or 0),
                "labeled": int(labeled_lookup.get(source, 0) or 0),
                "labelable": int(labelable_lookup.get(source, 0) or 0),
                "persona_core": int(persona_core_lookup.get(source, 0) or 0),
                "labeled_share_pct": round(float(labeled_share_lookup.get(source, 0.0) or 0.0), 1),
                "blended_influence_share_pct": round(float(blended_lookup.get(source, 0.0) or 0.0), 1),
                "source_balance_status": status_lookup.get(source, ""),
                "top_failure_reason": failure_lookup.get(source, ""),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(["persona_core", "labeled", "prefiltered_valid", "raw"], ascending=False).reset_index(drop=True)


def _build_overall_rows(source_rows: pd.DataFrame, labeled_df: pd.DataFrame, overview_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Build the overall metrics rows used for before/after comparison."""
    raw_total = int(source_rows.get("raw", pd.Series(dtype=int)).sum())
    valid_total = int(source_rows.get("valid", pd.Series(dtype=int)).sum())
    prefiltered_total = int(source_rows.get("prefiltered_valid", pd.Series(dtype=int)).sum())
    episodes_total = int(source_rows.get("episodes", pd.Series(dtype=int)).sum())
    labeled_total = int(source_rows.get("labeled", pd.Series(dtype=int)).sum())
    labelable_total = int(source_rows.get("labelable", pd.Series(dtype=int)).sum())
    persona_core_total = int(source_rows.get("persona_core", pd.Series(dtype=int)).sum())
    largest_raw_source_share_pct = _largest_share(source_rows, "raw")
    largest_labeled_source_share_pct = _largest_share(source_rows, "labeled")
    largest_blended_influence_share_pct = round(
        float(source_rows.get("blended_influence_share_pct", pd.Series(dtype=float)).max() or 0.0),
        1,
    ) if not source_rows.empty else 0.0
    effective_balanced_source_count = _overview_metric(overview_df, "effective_balanced_source_count")
    source_count_with_labelable = int((source_rows.get("labelable", pd.Series(dtype=int)) > 0).sum()) if not source_rows.empty else 0
    dominant_labeled_source = _top_source(source_rows, "labeled")
    dominant_influence_source = _top_source(source_rows, "blended_influence_share_pct")
    readiness_state = _overview_metric(overview_df, "persona_readiness_state", default="")
    quality_flag = _overview_metric(overview_df, "quality_flag", default="")

    return [
        _overall_row("raw", raw_total, "rows"),
        _overall_row("valid", valid_total, "rows"),
        _overall_row("prefiltered_valid", prefiltered_total, "rows"),
        _overall_row("episodes", episodes_total, "rows"),
        _overall_row("labeled", labeled_total, "rows"),
        _overall_row("labelable", labelable_total, "rows"),
        _overall_row("persona_core", persona_core_total, "rows"),
        _overall_row("largest_raw_source_share_pct", largest_raw_source_share_pct, "pct"),
        _overall_row("largest_labeled_source_share_pct", largest_labeled_source_share_pct, "pct"),
        _overall_row("largest_blended_influence_share_pct", largest_blended_influence_share_pct, "pct"),
        _overall_row("effective_balanced_source_count", effective_balanced_source_count, "count"),
        _overall_row("source_count_with_labelable", source_count_with_labelable, "count"),
        _overall_row("dominant_labeled_source", dominant_labeled_source, "label"),
        _overall_row("dominant_influence_source", dominant_influence_source, "label"),
        _overall_row("persona_readiness_state", readiness_state, "label"),
        _overall_row("quality_flag", quality_flag, "label"),
    ]


def _load_snapshot(path: Path) -> ScaleMetricsSnapshot:
    """Load a previously written snapshot."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ScaleMetricsSnapshot(
        generated_at=str(payload.get("generated_at", "")),
        overall=list(payload.get("overall", []) or []),
        by_source=list(payload.get("by_source", []) or []),
    )


def _render_markdown(current: ScaleMetricsSnapshot, baseline: ScaleMetricsSnapshot) -> str:
    """Render the root-level before/after markdown report."""
    current_overall = _overall_lookup(current.overall)
    baseline_overall = _overall_lookup(baseline.overall)
    current_source_df = pd.DataFrame(current.by_source)
    baseline_time = baseline.generated_at or "unknown"
    current_time = current.generated_at or "unknown"
    is_same_snapshot = baseline.generated_at == current.generated_at

    lines = [
        "# Pipeline Metrics Before vs After",
        "",
        f"- Baseline snapshot: {baseline_time}",
        f"- Current snapshot: {current_time}",
        "- Scope: raw, valid, prefiltered valid, episodes, labeled, persona-core, and source-balance risk indicators.",
    ]
    if is_same_snapshot:
        lines.append("- Current snapshot still equals the baseline. Rerun collection and downstream stages, then rerun `python run/diagnostics/20_capture_scale_metrics.py` to populate the after delta.")
    lines.extend(
        [
            "",
            "## Overall Funnel",
            "",
            "| Metric | Before | Current | Delta |",
            "|---|---:|---:|---:|",
        ]
    )
    for metric in ["raw", "valid", "prefiltered_valid", "episodes", "labeled", "labelable", "persona_core"]:
        before_value = baseline_overall.get(metric, {}).get("value", 0)
        current_value = current_overall.get(metric, {}).get("value", 0)
        lines.append(f"| {metric} | {before_value} | {current_value} | {_delta_str(before_value, current_value)} |")

    lines.extend(
        [
            "",
            "## Source Balance",
            "",
            "| Metric | Before | Current | Delta |",
            "|---|---:|---:|---:|",
        ]
    )
    for metric in [
        "largest_raw_source_share_pct",
        "largest_labeled_source_share_pct",
        "largest_blended_influence_share_pct",
        "effective_balanced_source_count",
        "source_count_with_labelable",
    ]:
        before_value = baseline_overall.get(metric, {}).get("value", 0)
        current_value = current_overall.get(metric, {}).get("value", 0)
        lines.append(f"| {metric} | {before_value} | {current_value} | {_delta_str(before_value, current_value)} |")

    lines.extend(
        [
            "",
            "## Current Source Snapshot",
            "",
            "| Source | Raw | Valid | Prefiltered valid | Episodes | Labeled | Persona-core | Labeled share % | Blended influence % | Balance status | Top failure reason |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    if current_source_df.empty:
        lines.append("| none | 0 | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 | n/a | n/a |")
    else:
        for _, row in current_source_df.iterrows():
            lines.append(
                "| {source} | {raw} | {valid} | {prefiltered_valid} | {episodes} | {labeled} | {persona_core} | {labeled_share_pct} | {blended_influence_share_pct} | {source_balance_status} | {top_failure_reason} |".format(
                    source=row.get("source", ""),
                    raw=int(row.get("raw", 0) or 0),
                    valid=int(row.get("valid", 0) or 0),
                    prefiltered_valid=int(row.get("prefiltered_valid", 0) or 0),
                    episodes=int(row.get("episodes", 0) or 0),
                    labeled=int(row.get("labeled", 0) or 0),
                    persona_core=int(row.get("persona_core", 0) or 0),
                    labeled_share_pct=round(float(row.get("labeled_share_pct", 0.0) or 0.0), 1),
                    blended_influence_share_pct=round(float(row.get("blended_influence_share_pct", 0.0) or 0.0), 1),
                    source_balance_status=row.get("source_balance_status", ""),
                    top_failure_reason=row.get("top_failure_reason", ""),
                )
            )

    lines.extend(
        [
            "",
            "## Readiness Context",
            "",
            f"- Dominant labeled source: {current_overall.get('dominant_labeled_source', {}).get('value', '')}",
            f"- Dominant influence source: {current_overall.get('dominant_influence_source', {}).get('value', '')}",
            f"- Persona readiness state: {current_overall.get('persona_readiness_state', {}).get('value', '')}",
            f"- Quality flag: {current_overall.get('quality_flag', {}).get('value', '')}",
        ]
    )
    return "\n".join(lines) + "\n"


def _value_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    """Return per-source row counts for a dataframe."""
    if df.empty or column not in df.columns:
        return {}
    counts = df[column].fillna("").astype(str).value_counts()
    return {str(index): int(value) for index, value in counts.items() if str(index)}


def _persona_core_counts(df: pd.DataFrame) -> dict[str, int]:
    """Return per-source persona-core counts from labeled rows."""
    if df.empty or "source" not in df.columns or "persona_core_eligible" not in df.columns:
        return {}
    filtered = df[df["persona_core_eligible"].fillna(False).astype(bool)]
    return _value_counts(filtered, "source")


def _labelable_counts(df: pd.DataFrame) -> dict[str, int]:
    """Return per-source labelable counts from labeled rows."""
    if df.empty or "source" not in df.columns or "labelability_status" not in df.columns:
        return {}
    filtered = df[df["labelability_status"].astype(str).isin(["labelable", "borderline"])]
    return _value_counts(filtered, "source")


def _count_lookup(df: pd.DataFrame, key_col: str, value_col: str) -> dict[str, Any]:
    """Build a keyed lookup from one dataframe column pair."""
    if df.empty or key_col not in df.columns or value_col not in df.columns:
        return {}
    result: dict[str, Any] = {}
    for _, row in df.iterrows():
        key = str(row.get(key_col, "") or "")
        if key:
            result[key] = row.get(value_col)
    return result


def _string_lookup(df: pd.DataFrame, key_col: str, value_col: str) -> dict[str, str]:
    """Build a keyed string lookup."""
    raw_lookup = _count_lookup(df, key_col, value_col)
    return {key: str(value or "") for key, value in raw_lookup.items()}


def _largest_share(source_rows: pd.DataFrame, value_col: str) -> float:
    """Return the dominant source share for one volume column."""
    if source_rows.empty or value_col not in source_rows.columns:
        return 0.0
    total = float(source_rows[value_col].sum() or 0.0)
    if total <= 0:
        return 0.0
    return round(float(source_rows[value_col].max() or 0.0) / total * 100.0, 1)


def _top_source(source_rows: pd.DataFrame, value_col: str) -> str:
    """Return the source with the highest value in one column."""
    if source_rows.empty or value_col not in source_rows.columns:
        return ""
    sorted_rows = source_rows.sort_values(value_col, ascending=False)
    if sorted_rows.empty:
        return ""
    return str(sorted_rows.iloc[0].get("source", "") or "")


def _overview_metric(overview_df: pd.DataFrame, metric: str, default: Any = 0.0) -> Any:
    """Look up one overview metric value."""
    if overview_df.empty or not {"metric", "value"}.issubset(overview_df.columns):
        return default
    matches = overview_df[overview_df["metric"].astype(str) == metric]
    if matches.empty:
        return default
    value = matches.iloc[0]["value"]
    if isinstance(default, str):
        return str(value or "")
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return default


def _overall_row(metric: str, value: Any, unit: str) -> dict[str, Any]:
    """Build one overall metric row."""
    return {"metric": metric, "value": value, "unit": unit}


def _overall_lookup(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Build a lookup from overall rows."""
    return {str(row.get("metric", "")): row for row in rows if str(row.get("metric", ""))}


def _delta_str(before_value: Any, current_value: Any) -> str:
    """Render a markdown-friendly delta value."""
    try:
        before_number = float(before_value)
        current_number = float(current_value)
    except (TypeError, ValueError):
        return "n/a"
    delta = current_number - before_number
    if float(before_number).is_integer() and float(current_number).is_integer() and float(delta).is_integer():
        return str(int(delta))
    return f"{delta:.1f}"
