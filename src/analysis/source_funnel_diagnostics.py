"""Build reusable source-collapse diagnostics from current pipeline artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir, write_parquet

TARGET_SOURCES = ["merchant_center_community", "reddit"]
FUNNEL_STAGES = [
    "raw_collected_rows",
    "parsed_rows",
    "normalized_rows",
    "valid_rows",
    "relevance_prefilter_passed_rows",
    "episode_built_rows",
    "labeled_rows",
    "persona_relevant_promoted_rows",
]
STAGE_LINKS: dict[str, dict[str, str]] = {
    "raw_collected_rows": {
        "script": "run/01_collect_all.py",
        "function": "collector.collect()",
        "config": "config/sources/{source}.yaml",
    },
    "parsed_rows": {
        "script": "run/01_collect_all.py",
        "function": "collector.collect() / parser.parse_thread_page()",
        "config": "config/sources/{source}.yaml",
    },
    "normalized_rows": {
        "script": "run/02_normalize_all.py",
        "function": "normalizer.normalize_row()",
        "config": "config/sources/{source}.yaml",
    },
    "valid_rows": {
        "script": "run/03_filter_valid.py",
        "function": "apply_invalid_filter() / split_duplicate_posts()",
        "config": "config/invalid_rules.yaml",
    },
    "relevance_prefilter_passed_rows": {
        "script": "run/03_5_prefilter_relevance.py",
        "function": "apply_relevance_prefilter() / _evaluate_row_from_context()",
        "config": "config/relevance_rules.yaml",
    },
    "episode_built_rows": {
        "script": "run/04_build_episodes.py",
        "function": "build_episode_outputs() / _assess_episode_quality()",
        "config": "config/segmentation_rules.yaml",
    },
    "labeled_rows": {
        "script": "run/05_label_episodes.py",
        "function": "build_labelability_table() / prelabel_episodes()",
        "config": "config/labeling_policy.yaml",
    },
    "persona_relevant_promoted_rows": {
        "script": "run/06_cluster_and_score.py",
        "function": "cluster + promotion outputs",
        "config": "docs/source_balance_policy.md",
    },
}
SOURCE_STAGE_OVERRIDES: dict[str, dict[str, dict[str, str]]] = {
    "merchant_center_community": {
        "parsed_rows": {
            "function": "BusinessCommunityCollector.collect() / parse_thread_page()",
        },
        "relevance_prefilter_passed_rows": {
            "function": "apply_relevance_prefilter() / _derive_dropped_reason()",
            "config": "config/relevance_rules.yaml (merchant_center_community whitelist terms)",
        },
    },
    "reddit": {
        "parsed_rows": {
            "function": "RedditCollector.collect() / collect_with_pagination()",
        },
        "relevance_prefilter_passed_rows": {
            "function": "apply_relevance_prefilter() / _evaluate_row_from_context() [reddit branch]",
            "config": "config/relevance_rules.yaml (reddit subreddit boosts + thresholds)",
        },
    },
}


def build_source_collapse_diagnostics(
    root_dir: Path,
    sources: list[str] | None = None,
    output_dir_name: str = "source_collapse_diagnostics",
) -> dict[str, Path]:
    """Build and persist source-collapse diagnostics for the requested sources."""
    selected_sources = sources or TARGET_SOURCES
    output_dir = ensure_dir(root_dir / "data" / "analysis" / output_dir_name)
    artifacts = _load_artifacts(root_dir)
    summary_rows: list[dict[str, Any]] = []
    long_rows: list[dict[str, Any]] = []
    discrepancy_rows: list[dict[str, Any]] = []

    for source in selected_sources:
        counts, provenance = _collect_counts_for_source(source, root_dir, artifacts)
        first_major_drop_stage = _first_major_drop_stage(counts)
        collapse_link = _stage_link(source, first_major_drop_stage)
        summary_rows.append(
            {
                "source": source,
                **{stage: counts.get(stage, 0) for stage in FUNNEL_STAGES},
                "first_major_drop_stage": first_major_drop_stage,
                "drop_shape": _drop_shape(counts),
                "first_severe_drop_flagged": first_major_drop_stage != "none",
                "collapse_script": collapse_link["script"],
                "collapse_function": collapse_link["function"],
                "collapse_config": collapse_link["config"],
            }
        )
        long_rows.extend(_build_long_rows(source, counts, provenance))
        discrepancy_rows.extend(_build_discrepancy_rows(source, counts, artifacts))

    summary_df = pd.DataFrame(summary_rows)
    long_df = pd.DataFrame(long_rows)
    discrepancy_df = pd.DataFrame(discrepancy_rows)

    summary_csv = output_dir / "source_funnel_summary.csv"
    summary_md = output_dir / "source_funnel_diagnosis.md"
    long_csv = output_dir / "source_funnel_long.csv"
    discrepancy_csv = output_dir / "source_funnel_discrepancies.csv"

    summary_df.to_csv(summary_csv, index=False)
    long_df.to_csv(long_csv, index=False)
    discrepancy_df.to_csv(discrepancy_csv, index=False)
    write_parquet(summary_df, output_dir / "source_funnel_summary.parquet")
    write_parquet(long_df, output_dir / "source_funnel_long.parquet")
    write_parquet(discrepancy_df, output_dir / "source_funnel_discrepancies.parquet")
    summary_md.write_text(_build_markdown_diagnosis(summary_df, long_df, discrepancy_df), encoding="utf-8")

    return {
        "summary_csv": summary_csv,
        "summary_parquet": output_dir / "source_funnel_summary.parquet",
        "long_csv": long_csv,
        "long_parquet": output_dir / "source_funnel_long.parquet",
        "discrepancy_csv": discrepancy_csv,
        "discrepancy_parquet": output_dir / "source_funnel_discrepancies.parquet",
        "diagnosis_md": summary_md,
    }


def build_source_funnel_diagnostics(root_dir: Path, sources: list[str] | None = None) -> dict[str, Path]:
    """Backward-compatible wrapper for older funnel-diagnostics callers."""
    return build_source_collapse_diagnostics(
        root_dir=root_dir,
        sources=sources,
        output_dir_name="source_funnel_diagnostics",
    )


def _load_artifacts(root_dir: Path) -> dict[str, pd.DataFrame]:
    """Load reusable stage artifacts once."""
    return {
        "normalized_all": _read_parquet(root_dir / "data" / "normalized" / "normalized_posts.parquet"),
        "valid": _read_parquet(root_dir / "data" / "valid" / "valid_candidates.parquet"),
        "prefiltered": _read_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet"),
        "episodes": _read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet"),
        "labelability": _read_parquet(root_dir / "data" / "labeled" / "labelability_audit.parquet"),
        "labeled": _read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet"),
        "persona_assignments": _read_parquet(root_dir / "data" / "analysis" / "persona_assignments.parquet"),
        "cluster_stats": _read_csv(root_dir / "data" / "analysis" / "cluster_stats.csv"),
        "business_health": _read_csv(root_dir / "data" / "analysis" / "business_community_source_health.csv"),
        "source_diagnostics": _read_csv(root_dir / "data" / "analysis" / "source_diagnostics.csv"),
    }


def _collect_counts_for_source(source: str, root_dir: Path, artifacts: dict[str, pd.DataFrame]) -> tuple[dict[str, int], dict[str, str]]:
    """Collect stage counts and provenance for one source."""
    counts: dict[str, int] = {}
    provenance: dict[str, str] = {}

    raw_count = _count_jsonl_rows(root_dir / "data" / "raw" / source / "raw.jsonl")
    counts["raw_collected_rows"] = raw_count
    provenance["raw_collected_rows"] = f"data/raw/{source}/raw.jsonl"

    if source == "merchant_center_community":
        parsed_count = _lookup_business_parse_success(artifacts["business_health"], source)
        if parsed_count <= 0:
            parsed_count = raw_count
            provenance["parsed_rows"] = "fallback: raw jsonl rows because parse-success audit was missing"
        else:
            provenance["parsed_rows"] = "data/analysis/business_community_source_health.csv parse_success_count"
    else:
        parsed_count = raw_count
        provenance["parsed_rows"] = "recovered from current raw jsonl because reddit raw rows are collector-emitted parsed RawRecord rows"
    counts["parsed_rows"] = parsed_count

    normalized_count = _count_source_rows(artifacts["normalized_all"], source)
    counts["normalized_rows"] = normalized_count
    provenance["normalized_rows"] = "data/normalized/normalized_posts.parquet"

    valid_count = _count_source_rows(artifacts["valid"], source)
    counts["valid_rows"] = valid_count
    provenance["valid_rows"] = "data/valid/valid_candidates.parquet"

    prefiltered_count = _count_source_rows(artifacts["prefiltered"], source)
    counts["relevance_prefilter_passed_rows"] = prefiltered_count
    provenance["relevance_prefilter_passed_rows"] = "data/valid/valid_candidates_prefiltered.parquet"

    episode_count = _count_source_rows(artifacts["episodes"], source)
    counts["episode_built_rows"] = episode_count
    provenance["episode_built_rows"] = "data/episodes/episode_table.parquet"

    labeled_count = _count_labelable_source_rows(artifacts["labelability"], source)
    counts["labeled_rows"] = labeled_count
    provenance["labeled_rows"] = "data/labeled/labelability_audit.parquet joined to labeled_episodes boundary"

    promoted_count = _count_promoted_source_rows(
        source=source,
        episodes_df=artifacts["episodes"],
        persona_assignments_df=artifacts["persona_assignments"],
        cluster_stats_df=artifacts["cluster_stats"],
    )
    counts["persona_relevant_promoted_rows"] = promoted_count
    provenance["persona_relevant_promoted_rows"] = "data/analysis/persona_assignments.parquet + data/analysis/cluster_stats.csv + episode source join"
    return counts, provenance


def _build_long_rows(source: str, counts: dict[str, int], provenance: dict[str, str]) -> list[dict[str, Any]]:
    """Build one long-format funnel table for one source."""
    rows: list[dict[str, Any]] = []
    previous_count: int | None = None
    for stage in FUNNEL_STAGES:
        count = int(counts.get(stage, 0))
        stage_link = _stage_link(source, stage)
        if previous_count is None:
            kept_pct = 100.0
            drop_count = 0
            drop_pct = 0.0
        else:
            kept_pct = round((count / previous_count) * 100.0, 1) if previous_count > 0 else 0.0
            drop_count = max(previous_count - count, 0)
            drop_pct = round((drop_count / previous_count) * 100.0, 1) if previous_count > 0 else 0.0
        rows.append(
            {
                "source": source,
                "stage_name": stage,
                "stage_count": count,
                "previous_stage_count": previous_count if previous_count is not None else count,
                "drop_from_previous_count": drop_count,
                "drop_from_previous_pct": drop_pct,
                "kept_from_previous_pct": kept_pct,
                "is_first_severe_drop": stage == _first_major_drop_stage(counts),
                "count_provenance": provenance.get(stage, ""),
                "stage_script": stage_link["script"],
                "stage_function": stage_link["function"],
                "stage_config": stage_link["config"],
            }
        )
        previous_count = count
    return rows


def _build_discrepancy_rows(source: str, counts: dict[str, int], artifacts: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    """Record stale secondary-summary discrepancies against primary stage outputs."""
    rows: list[dict[str, Any]] = []
    diagnostics_df = artifacts["source_diagnostics"]
    if diagnostics_df.empty:
        return rows
    source_diag = diagnostics_df[diagnostics_df["source"].astype(str).eq(source)].copy()
    expected_metric_map = {
        "raw_collected_rows": "raw_record_count",
        "normalized_rows": "normalized_post_count",
        "valid_rows": "valid_post_count",
        "relevance_prefilter_passed_rows": "prefiltered_valid_post_count",
        "episode_built_rows": "episode_count",
        "labeled_rows": "labeled_episode_count",
        "persona_relevant_promoted_rows": "promoted_persona_episode_count",
    }
    for stage_name, metric_name in expected_metric_map.items():
        metric_rows = source_diag[source_diag["metric_name"].astype(str).eq(metric_name)]
        if metric_rows.empty:
            continue
        metric_value = _safe_int(metric_rows.iloc[0]["metric_value"])
        primary_value = int(counts.get(stage_name, 0))
        if metric_value != primary_value:
            rows.append(
                {
                    "source": source,
                    "stage_name": stage_name,
                    "primary_count": primary_value,
                    "secondary_metric_name": metric_name,
                    "secondary_count": metric_value,
                    "secondary_source": "data/analysis/source_diagnostics.csv",
                }
            )
    return rows


def _first_major_drop_stage(counts: dict[str, int]) -> str:
    """Return the first stage whose drop exceeds 50 percent from the prior stage."""
    previous_count: int | None = None
    for stage in FUNNEL_STAGES:
        count = int(counts.get(stage, 0))
        if previous_count is None:
            previous_count = count
            continue
        if previous_count > 0 and (previous_count - count) / previous_count >= 0.5:
            return stage
        previous_count = count
    return "none"


def _drop_shape(counts: dict[str, int]) -> str:
    """Classify whether source loss is gradual or concentrated."""
    drops: list[float] = []
    previous_count: int | None = None
    for stage in FUNNEL_STAGES:
        count = int(counts.get(stage, 0))
        if previous_count is not None and previous_count > 0:
            drops.append((previous_count - count) / previous_count)
        previous_count = count
    if not drops:
        return "none"
    max_drop = max(drops)
    large_drop_count = sum(drop >= 0.5 for drop in drops)
    if max_drop >= 0.8 and large_drop_count <= 2:
        return "concentrated"
    return "gradual"


def _build_markdown_diagnosis(summary_df: pd.DataFrame, long_df: pd.DataFrame, discrepancy_df: pd.DataFrame) -> str:
    """Render a source-specific markdown diagnosis from the funnel tables."""
    lines = ["# Source Funnel Diagnosis", ""]
    for _, row in summary_df.iterrows():
        source = str(row["source"])
        lines.extend(
            [
                f"## {source}",
                "",
                f"- first major drop stage: `{row['first_major_drop_stage']}`",
                f"- drop shape: `{row['drop_shape']}`",
                f"- collapse script: `{row['collapse_script']}`",
                f"- collapse function: `{row['collapse_function']}`",
                f"- collapse config: `{row['collapse_config']}`",
                "",
                "| stage | count | drop_from_previous | drop_pct | severe_drop | script | function | config |",
                "|---|---:|---:|---:|---|---|---|---|",
            ]
        )
        source_long = long_df[long_df["source"].astype(str).eq(source)]
        for _, stage_row in source_long.iterrows():
            lines.append(
                f"| {stage_row['stage_name']} | {int(stage_row['stage_count'])} | {int(stage_row['drop_from_previous_count'])} | {float(stage_row['drop_from_previous_pct']):.1f}% | {bool(stage_row['is_first_severe_drop'])} | {stage_row['stage_script']} | {stage_row['stage_function']} | {stage_row['stage_config']} |"
            )
        lines.append("")
    if not discrepancy_df.empty:
        lines.extend(
            [
                "## Secondary Summary Mismatches",
                "",
                "These rows indicate stale secondary summaries that do not match the current primary stage artifacts.",
                "",
                "| source | stage | primary | secondary_metric | secondary |",
                "|---|---|---:|---|---:|",
            ]
        )
        for _, row in discrepancy_df.iterrows():
            lines.append(
                f"| {row['source']} | {row['stage_name']} | {int(row['primary_count'])} | {row['secondary_metric_name']} | {int(row['secondary_count'])} |"
            )
        lines.append("")
    return "\n".join(lines)


def _count_jsonl_rows(path: Path) -> int:
    """Count non-empty JSONL lines for one source."""
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _count_source_rows(df: pd.DataFrame, source: str) -> int:
    """Count rows for one source in a dataframe."""
    if df.empty or "source" not in df.columns:
        return 0
    return int(df["source"].astype(str).eq(source).sum())


def _count_labelable_source_rows(labelability_df: pd.DataFrame, source: str) -> int:
    """Count labeled rows for one source using the labelability audit grain."""
    if labelability_df.empty or "source" not in labelability_df.columns:
        return 0
    return int(labelability_df["source"].astype(str).eq(source).sum())


def _count_promoted_source_rows(
    source: str,
    episodes_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
) -> int:
    """Count source episodes assigned to promoted or review-visible personas."""
    if episodes_df.empty or persona_assignments_df.empty or cluster_stats_df.empty:
        return 0
    if "workbook_review_visible" in cluster_stats_df.columns:
        promoted_ids = set(
            cluster_stats_df[cluster_stats_df["workbook_review_visible"].fillna(False).astype(bool)]["persona_id"].astype(str).tolist()
        )
    else:
        promoted_ids = set(
            cluster_stats_df[cluster_stats_df["promotion_status"].astype(str).isin({"promoted_persona", "review_visible_persona"})]["persona_id"].astype(str).tolist()
        )
    merged = persona_assignments_df.merge(episodes_df[["episode_id", "source"]], on="episode_id", how="left")
    filtered = merged[
        merged["source"].astype(str).eq(source)
        & merged["persona_id"].astype(str).isin(promoted_ids)
    ]
    return int(len(filtered))


def _lookup_business_parse_success(df: pd.DataFrame, source: str) -> int:
    """Return parse_success_count for one business community source when available."""
    if df.empty or "source_id" not in df.columns or "parse_success_count" not in df.columns:
        return 0
    match = df[df["source_id"].astype(str).eq(source)]
    if match.empty:
        return 0
    return _safe_int(match.iloc[0]["parse_success_count"])


def _safe_int(value: Any) -> int:
    """Parse a numeric-like value into int with safe fallback."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _stage_link(source: str, stage: str) -> dict[str, str]:
    """Return the most relevant script/function/config pointers for one stage."""
    base = dict(STAGE_LINKS.get(stage, {}))
    if not base:
        return {"script": "", "function": "", "config": ""}
    if "{source}" in base.get("config", ""):
        base["config"] = base["config"].format(source=source)
    overrides = SOURCE_STAGE_OVERRIDES.get(source, {}).get(stage, {})
    return {
        "script": overrides.get("script", base.get("script", "")),
        "function": overrides.get("function", base.get("function", "")),
        "config": overrides.get("config", base.get("config", "")),
    }


def _read_parquet(path: Path) -> pd.DataFrame:
    """Read parquet when present or return an empty dataframe."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _read_csv(path: Path) -> pd.DataFrame:
    """Read csv when present or return an empty dataframe."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)
