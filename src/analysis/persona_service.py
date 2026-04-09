"""Axis-based persona clustering and report-ready persona tables."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.persona_axes import build_axis_assignments
from src.analysis.summary import build_quality_checks_df
from src.utils.pipeline_schema import (
    THEME_COLUMNS,
    is_unknown_like,
    round_pct,
    split_pipe_codes,
)
from src.utils.record_access import get_record_text
from src.utils.io import ensure_dir


def build_persona_outputs(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    final_axis_schema: list[dict[str, Any]],
    quality_checks: dict[str, Any],
) -> dict[str, Any]:
    """Build stable persona clusters and report-ready persona outputs."""
    axis_names = [str(row.get("axis_name", "")).strip() for row in final_axis_schema if str(row.get("axis_name", "")).strip()]
    merged = episodes_df.merge(labeled_df, on="episode_id", how="inner").fillna("")
    axis_wide_df, axis_long_df = build_axis_assignments(episodes_df, labeled_df, axis_names=axis_names)
    if merged.empty or axis_wide_df.empty or not axis_names:
        return _empty_outputs()

    persona_assignments_df = _assign_personas(axis_wide_df, axis_names)
    persona_source_df = merged.merge(persona_assignments_df, on="episode_id", how="inner")
    total_labeled_records = int(quality_checks.get("labeled_count", len(labeled_df)))

    overview_df = _build_overview_df(persona_source_df, axis_names, quality_checks, total_labeled_records)
    persona_summary_df = _build_persona_summary_df(persona_source_df, axis_names, total_labeled_records)
    persona_axes_df = _build_persona_axes_df(persona_assignments_df, axis_long_df)
    persona_pains_df = _build_persona_pains_df(persona_source_df)
    persona_cooccurrence_df = _build_persona_cooccurrence_df(persona_source_df)
    persona_examples_df = _build_persona_examples_df(persona_source_df, axis_names)
    cluster_stats_df = _build_cluster_stats_df(persona_source_df, axis_names, total_labeled_records)
    quality_checks_df = build_quality_checks_df(quality_checks)

    outputs = {
        "overview_df": overview_df,
        "persona_summary_df": persona_summary_df,
        "persona_axes_df": persona_axes_df,
        "persona_pains_df": persona_pains_df,
        "persona_cooccurrence_df": persona_cooccurrence_df,
        "persona_examples_df": persona_examples_df,
        "cluster_stats_df": cluster_stats_df,
        "quality_checks_df": quality_checks_df,
        "persona_assignments_df": persona_assignments_df,
        "axis_wide_df": axis_wide_df,
        "axis_long_df": axis_long_df,
    }
    return outputs


def write_persona_outputs(root_dir: Path, outputs: dict[str, Any]) -> dict[str, Path]:
    """Write persona-analysis tables as optional debug artifacts."""
    output_dir = ensure_dir(root_dir / "data" / "analysis")
    paths = {
        "persona_summary_csv": output_dir / "persona_summary.csv",
        "persona_axes_csv": output_dir / "persona_axes.csv",
        "persona_pains_csv": output_dir / "persona_pains.csv",
        "persona_cooccurrence_csv": output_dir / "persona_cooccurrence.csv",
        "persona_examples_csv": output_dir / "persona_examples.csv",
        "cluster_stats_csv": output_dir / "cluster_stats.csv",
        "quality_checks_csv": output_dir / "quality_checks.csv",
        "overview_csv": output_dir / "overview.csv",
        "persona_assignments_parquet": output_dir / "persona_assignments.parquet",
        "persona_axis_assignments_parquet": output_dir / "persona_axis_assignments.parquet",
        "persona_axis_values_parquet": output_dir / "persona_axis_values.parquet",
        "persona_summary_json": output_dir / "persona_summary.json",
    }
    outputs["persona_summary_df"].to_csv(paths["persona_summary_csv"], index=False)
    outputs["persona_axes_df"].to_csv(paths["persona_axes_csv"], index=False)
    outputs["persona_pains_df"].to_csv(paths["persona_pains_csv"], index=False)
    outputs["persona_cooccurrence_df"].to_csv(paths["persona_cooccurrence_csv"], index=False)
    outputs["persona_examples_df"].to_csv(paths["persona_examples_csv"], index=False)
    outputs["cluster_stats_df"].to_csv(paths["cluster_stats_csv"], index=False)
    outputs["quality_checks_df"].to_csv(paths["quality_checks_csv"], index=False)
    outputs["overview_df"].to_csv(paths["overview_csv"], index=False)
    outputs["persona_assignments_df"].to_parquet(paths["persona_assignments_parquet"], index=False)
    outputs["axis_wide_df"].to_parquet(paths["persona_axis_assignments_parquet"], index=False)
    outputs["axis_long_df"].to_parquet(paths["persona_axis_values_parquet"], index=False)
    paths["persona_summary_json"].write_text(
        json.dumps(outputs["persona_summary_df"].to_dict(orient="records"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return paths


def _assign_personas(axis_wide_df: pd.DataFrame, axis_names: list[str]) -> pd.DataFrame:
    """Create stable persona signatures and merge sparse signatures into larger personas."""
    working = axis_wide_df.copy()
    working["signature"] = working.apply(lambda row: _signature(row, axis_names), axis=1)
    total_rows = len(working)
    min_persona_size = max(25, int(total_rows * 0.03))
    signature_counts = working["signature"].value_counts()
    anchor_signatures = [signature for signature, count in signature_counts.items() if count >= min_persona_size]
    if not anchor_signatures:
        anchor_signatures = list(signature_counts.head(min(5, len(signature_counts))).index)

    signature_to_anchor = {signature: signature for signature in anchor_signatures}
    for signature in signature_counts.index:
        if signature in signature_to_anchor:
            continue
        signature_to_anchor[signature] = _nearest_anchor(signature, anchor_signatures, axis_names)

    anchor_to_id = {signature: f"persona_{index:02d}" for index, signature in enumerate(anchor_signatures, start=1)}
    working["persona_signature"] = working["signature"].map(signature_to_anchor)
    working["persona_id"] = working["persona_signature"].map(anchor_to_id)
    for axis_name in axis_names:
        working[f"{axis_name}__persona"] = working["persona_signature"].map(lambda signature: _signature_map(signature).get(axis_name, "unassigned"))
    return working


def _build_overview_df(
    persona_source_df: pd.DataFrame,
    axis_names: list[str],
    quality_checks: dict[str, Any],
    total_labeled_records: int,
) -> pd.DataFrame:
    """Build workbook overview sheet."""
    overview_rows = [
        {"metric": "total_labeled_records", "value": total_labeled_records},
        {"metric": "persona_count", "value": int(persona_source_df["persona_id"].nunique())},
        {"metric": "selected_axes", "value": " | ".join(axis_names)},
        {"metric": "quality_flag", "value": quality_checks.get("quality_flag", "unknown")},
        {"metric": "unknown_ratio", "value": quality_checks.get("unknown_ratio", 0.0)},
    ]
    return pd.DataFrame(overview_rows)


def _build_persona_summary_df(persona_source_df: pd.DataFrame, axis_names: list[str], total_labeled_records: int) -> pd.DataFrame:
    """Build top-level persona summary sheet."""
    rows: list[dict[str, Any]] = []
    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        persona_size = int(len(group))
        role = _top_value(group, "user_role__persona")
        workflow = _top_value(group, "workflow_stage__persona")
        goal = _top_value(group, "analysis_goal__persona")
        bottleneck = _top_value(group, "bottleneck_type__persona")
        trust = _top_value(group, "trust_validation_need__persona")
        tool_mode = _top_value(group, "tool_dependency_mode__persona")
        output_mode = _top_value(group, "output_expectation__persona")
        rows.append(
            {
                "persona_id": persona_id,
                "persona_name": _persona_name(role, workflow, goal),
                "persona_size": persona_size,
                "share_of_total": round_pct(persona_size, total_labeled_records),
                "one_line_summary": _one_line_summary(role, workflow, bottleneck, goal),
                "main_workflow_context": workflow,
                "dominant_bottleneck": bottleneck,
                "analysis_behavior": goal,
                "trust_explanation_need": trust,
                "current_tool_dependency": tool_mode,
                "primary_output_expectation": output_mode,
                "top_pain_points": " | ".join(_top_themes(group, ["pain_codes", "question_codes"], limit=4)),
                "representative_examples": " | ".join(_select_examples(group, axis_names, max_items=2)),
                "why_this_persona_matters": _why_persona_matters(group, bottleneck, goal, output_mode),
            }
        )
    return pd.DataFrame(rows).sort_values(["persona_size", "persona_id"], ascending=[False, True]).reset_index(drop=True)


def _build_persona_axes_df(persona_assignments_df: pd.DataFrame, axis_long_df: pd.DataFrame) -> pd.DataFrame:
    """Build persona axis value counts."""
    merged = persona_assignments_df[["episode_id", "persona_id"]].merge(axis_long_df, on="episode_id", how="inner")
    persona_sizes = persona_assignments_df.groupby("persona_id").size().to_dict()
    grouped = (
        merged.groupby(["persona_id", "axis_name", "axis_value"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    grouped["pct_of_persona"] = grouped.apply(
        lambda row: round_pct(row["count"], persona_sizes.get(row["persona_id"], 1)),
        axis=1,
    )
    return grouped.sort_values(["persona_id", "axis_name", "count", "axis_value"], ascending=[True, True, False, True]).reset_index(drop=True)


def _build_persona_pains_df(persona_source_df: pd.DataFrame) -> pd.DataFrame:
    """Build top pain and need patterns per persona."""
    rows: list[dict[str, Any]] = []
    persona_sizes = persona_source_df.groupby("persona_id").size().to_dict()
    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        counts = Counter(_theme_values(group, ["pain_codes", "question_codes", "output_codes", "workaround_codes"]))
        for rank, (theme, count) in enumerate(counts.most_common(12), start=1):
            rows.append(
                {
                    "persona_id": persona_id,
                    "pain_or_need": theme,
                    "count": int(count),
                    "pct_of_persona": round_pct(count, persona_sizes.get(persona_id, 1)),
                    "rank": rank,
                }
            )
    return pd.DataFrame(rows).sort_values(["persona_id", "rank"]).reset_index(drop=True)


def _build_persona_cooccurrence_df(persona_source_df: pd.DataFrame) -> pd.DataFrame:
    """Build within-persona theme co-occurrence table."""
    rows: list[dict[str, Any]] = []
    persona_sizes = persona_source_df.groupby("persona_id").size().to_dict()
    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        pair_counts: Counter[tuple[str, str]] = Counter()
        for _, row in group.iterrows():
            themes = sorted(set(_row_theme_values(row)))
            for index, theme_a in enumerate(themes):
                for theme_b in themes[index + 1 :]:
                    pair_counts[(theme_a, theme_b)] += 1
        for theme_rank, ((theme_a, theme_b), count) in enumerate(pair_counts.most_common(12), start=1):
            rows.append(
                {
                    "persona_id": persona_id,
                    "theme_a": theme_a,
                    "theme_b": theme_b,
                    "pair_count": int(count),
                    "pct_of_persona": round_pct(count, persona_sizes.get(persona_id, 1)),
                    "rank": theme_rank,
                }
            )
    return pd.DataFrame(rows).sort_values(["persona_id", "rank"]).reset_index(drop=True)


def _build_persona_examples_df(persona_source_df: pd.DataFrame, axis_names: list[str]) -> pd.DataFrame:
    """Build grounded representative examples per persona."""
    rows: list[dict[str, Any]] = []
    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        examples = _select_examples(group, axis_names, max_items=8, with_reason=True)
        for rank, (text, reason) in enumerate(examples, start=1):
            rows.append(
                {
                    "persona_id": persona_id,
                    "example_rank": rank,
                    "grounded_text": text,
                    "reason_selected": reason,
                }
            )
    return pd.DataFrame(rows).sort_values(["persona_id", "example_rank"]).reset_index(drop=True)


def _build_cluster_stats_df(persona_source_df: pd.DataFrame, axis_names: list[str], total_labeled_records: int) -> pd.DataFrame:
    """Build stable persona-cluster stats."""
    rows: list[dict[str, Any]] = []
    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        persona_size = int(len(group))
        axis_signature = " | ".join(
            f"{axis}={_top_value(group, f'{axis}__persona')}"
            for axis in axis_names
        )
        rows.append(
            {
                "persona_id": persona_id,
                "persona_size": persona_size,
                "share_of_total": round_pct(persona_size, total_labeled_records),
                "dominant_signature": axis_signature,
                "dominant_bottleneck": _top_value(group, "bottleneck_type__persona"),
                "dominant_analysis_goal": _top_value(group, "analysis_goal__persona"),
            }
        )
    return pd.DataFrame(rows).sort_values(["persona_size", "persona_id"], ascending=[False, True]).reset_index(drop=True)


def _empty_outputs() -> dict[str, Any]:
    """Return empty dataframes when persona generation has no inputs."""
    empty = pd.DataFrame()
    return {
        "overview_df": empty,
        "persona_summary_df": empty,
        "persona_axes_df": empty,
        "persona_pains_df": empty,
        "persona_cooccurrence_df": empty,
        "persona_examples_df": empty,
        "cluster_stats_df": empty,
        "quality_checks_df": empty,
        "persona_assignments_df": empty,
        "axis_wide_df": empty,
        "axis_long_df": empty,
    }


def _signature(row: pd.Series, axis_names: list[str]) -> str:
    """Build a deterministic persona signature from selected axis values."""
    return "||".join(f"{axis}={row.get(axis, 'unassigned')}" for axis in axis_names)


def _signature_map(signature: str) -> dict[str, str]:
    """Parse a signature string into axis-value mapping."""
    mapping: dict[str, str] = {}
    for item in str(signature or "").split("||"):
        if "=" not in item:
            continue
        axis_name, axis_value = item.split("=", 1)
        mapping[axis_name] = axis_value
    return mapping


def _nearest_anchor(signature: str, anchors: list[str], axis_names: list[str]) -> str:
    """Attach a sparse signature to the nearest anchor persona."""
    signature_values = _signature_map(signature)
    best_anchor = anchors[0]
    best_score = -1
    for anchor in anchors:
        anchor_values = _signature_map(anchor)
        score = sum(
            1
            for axis in axis_names
            if signature_values.get(axis, "unassigned") == anchor_values.get(axis, "unassigned")
        )
        if score > best_score:
            best_score = score
            best_anchor = anchor
    return best_anchor


def _top_value(group: pd.DataFrame, column: str) -> str:
    """Return the dominant non-unknown value for one column."""
    series = group[column].astype(str).str.strip()
    series = series[~series.map(is_unknown_like)]
    if series.empty:
        return "unassigned"
    return str(series.value_counts().idxmax())


def _theme_values(group: pd.DataFrame, columns: list[str]) -> list[str]:
    """Collect repeated theme values from labeled columns."""
    values: list[str] = []
    for column in columns:
        for raw_value in group.get(column, pd.Series(dtype=str)):
            values.extend(split_pipe_codes(raw_value))
    return values


def _row_theme_values(row: pd.Series) -> list[str]:
    """Collect theme values from one row for co-occurrence counting."""
    values: list[str] = []
    for column in THEME_COLUMNS:
        values.extend(split_pipe_codes(row.get(column, "")))
    return values


def _top_themes(group: pd.DataFrame, columns: list[str], limit: int) -> list[str]:
    """Return the most common label themes across selected columns."""
    counts = Counter(_theme_values(group, columns))
    return [theme for theme, _ in counts.most_common(limit)]


def _select_examples(
    group: pd.DataFrame,
    axis_names: list[str],
    max_items: int,
    with_reason: bool = False,
) -> list[Any]:
    """Select grounded representative examples with deterministic reasons."""
    ranked = group.sort_values(
        ["label_confidence", "episode_id"],
        ascending=[False, True],
    )
    seen: set[str] = set()
    examples: list[Any] = []
    for _, row in ranked.iterrows():
        text = get_record_text(row, fields=["normalized_episode"])
        if not text or text in seen:
            continue
        seen.add(text)
        reason = "high confidence + dominant axis match"
        value = (text[:500], reason) if with_reason else text[:220]
        examples.append(value)
        if len(examples) >= max_items:
            break
    return examples


def _persona_name(role: str, workflow: str, goal: str) -> str:
    """Create a grounded persona name from dominant axis values."""
    parts = [_titleize(role), _titleize(workflow), _titleize(goal)]
    return " ".join(part for part in parts if part and part != "Unassigned").strip() or "Mixed Persona"


def _one_line_summary(role: str, workflow: str, bottleneck: str, goal: str) -> str:
    """Create a grounded one-line persona summary."""
    return (
        f"{_titleize(role, 'Users')} working in {_titleize(workflow, 'mixed workflow').lower()} "
        f"who primarily need {_titleize(goal, 'better analysis support').lower()} "
        f"while blocked by {_titleize(bottleneck, 'general friction').lower()}."
    )


def _why_persona_matters(group: pd.DataFrame, bottleneck: str, goal: str, output_mode: str) -> str:
    """Summarize why this persona matters using computed stats only."""
    size = int(len(group))
    return (
        f"{size} labeled records repeatedly combine {_titleize(goal).lower()}, "
        f"{_titleize(bottleneck).lower()}, and {_titleize(output_mode).lower()} expectations."
    )


def _titleize(value: str, fallback: str = "unassigned") -> str:
    """Humanize snake-style axis values."""
    text = str(value or "").strip()
    if not text or is_unknown_like(text):
        text = fallback
    return text.replace("_", " ").title()
