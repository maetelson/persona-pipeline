"""Source-level diagnostics and workbook quality gates."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import load_yaml, read_parquet
from src.utils.pipeline_schema import (
    CLUSTER_DOMINANCE_SHARE_PCT,
    LABELABLE_STATUSES,
    MIN_LABELED_SOURCE_COUNT,
    QUALITY_FLAG_EXPLORATORY,
    QUALITY_FLAG_OK,
    QUALITY_FLAG_UNSTABLE,
    RAW_WITHOUT_LABEL_FAILURE_SOURCES,
    SOURCE_FIELD,
    aggregated_source_count,
    is_single_cluster_dominant,
    persona_min_cluster_size,
    round_pct,
    source_row_count,
)


def count_raw_jsonl_by_source(root_dir: Path) -> pd.DataFrame:
    """Count raw JSONL records directly from data/raw source folders."""
    raw_root = root_dir / "data" / "raw"
    rows: list[dict[str, Any]] = []
    if not raw_root.exists():
        return pd.DataFrame(columns=["source", "raw_count", "raw_file_count"])
    for source_dir in sorted(path for path in raw_root.iterdir() if path.is_dir()):
        files = sorted(source_dir.glob("*.jsonl"))
        rows.append(
            {
                "source": source_dir.name,
                "raw_count": sum(_count_jsonl_lines(path) for path in files),
                "raw_file_count": len(files),
            }
        )
    return pd.DataFrame(rows)


def build_metric_glossary() -> pd.DataFrame:
    """Build explicit metric definitions for workbook readers."""
    rows = [
        ("raw_records", "raw_jsonl_rows", "Non-empty JSONL rows under data/raw/{source}/*.jsonl."),
        ("normalized_records", "normalized_posts_rows", "Rows in data/normalized/normalized_posts.parquet after source normalizers."),
        ("valid_records", "valid_candidate_rows", "Rows in data/valid/valid_candidates.parquet before relevance prefiltering."),
        ("prefiltered_valid_records", "prefiltered_valid_rows", "Rows in data/valid/valid_candidates_prefiltered.parquet used by episode building when present."),
        ("episodes", "episode_rows", "Rows in data/episodes/episode_table.parquet; one post can yield zero or more episodes."),
        ("labeled_records", "labeled_episode_rows", "Rows in data/labeled/labeled_episodes.parquet joined to episodes by episode_id."),
        ("labelable_count", "labelability_rows", "Episode rows with labelability_status in labelable or borderline."),
        ("core_labeled", "persona_core_eligible_rows", "Labeled rows with persona_core_eligible=true, used for persona clustering."),
        ("promoted_to_persona_count", "promoted_cluster_rows", "Source rows assigned to clusters that pass persona promotion gates."),
    ]
    return pd.DataFrame(rows, columns=["metric", "denominator_type", "definition"])


def build_source_diagnostics(
    root_dir: Path,
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build source-level stage drop-off diagnostics."""
    raw_counts_df = count_raw_jsonl_by_source(root_dir)
    prefiltered_df = read_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    labelability_df = read_parquet(root_dir / "data" / "labeled" / "labelability_audit.parquet")
    relevance_drop_df = read_parquet(root_dir / "data" / "prefilter" / "relevance_drop.parquet")
    invalid_with_prefilter_df = read_parquet(root_dir / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")

    episode_source = _episode_source_lookup(episodes_df)
    labeled_with_source = _with_episode_source(labeled_df, episode_source)
    labelable_df = labelability_df[labelability_df.get("labelability_status", pd.Series(dtype=str)).astype(str).isin(LABELABLE_STATUSES)]
    promoted_ids = set(
        cluster_stats_df[
            cluster_stats_df.get("promotion_status", pd.Series(dtype=str)).astype(str).eq("promoted_persona")
        ]
        .get("persona_id", pd.Series(dtype=str))
        .astype(str)
        .tolist()
    )
    promoted_assignments = persona_assignments_df[persona_assignments_df.get("persona_id", pd.Series(dtype=str)).astype(str).isin(promoted_ids)]
    promoted_with_source = _with_episode_source(promoted_assignments, episode_source)

    sources = sorted(
        set(raw_counts_df.get("source", pd.Series(dtype=str)).astype(str))
        | set(normalized_df.get("source", pd.Series(dtype=str)).astype(str))
        | set(valid_df.get("source", pd.Series(dtype=str)).astype(str))
        | set(episodes_df.get("source", pd.Series(dtype=str)).astype(str))
    )
    rows: list[dict[str, Any]] = []
    for source in sources:
        raw_count = _source_count(raw_counts_df, source, "raw_count")
        normalized_count = source_row_count(normalized_df, source)
        valid_count = source_row_count(valid_df, source)
        prefiltered_count = source_row_count(prefiltered_df, source)
        episode_count = source_row_count(episodes_df, source)
        labelable_count = source_row_count(labelable_df, source)
        labeled_count = source_row_count(labeled_with_source, source)
        promoted_count = source_row_count(promoted_with_source, source)
        prefilter_survival_rate = round_pct(prefiltered_count, valid_count) if valid_count else 0.0
        episode_survival_rate = round_pct(episode_count, prefiltered_count) if prefiltered_count else 0.0
        labeling_survival_rate = round_pct(labeled_count, episode_count) if episode_count else 0.0
        reason = _source_failure_reason(
            source=source,
            raw_count=raw_count,
            normalized_count=normalized_count,
            valid_count=valid_count,
            prefiltered_count=prefiltered_count,
            episode_count=episode_count,
            labelable_count=labelable_count,
            labeled_count=labeled_count,
            relevance_drop_df=relevance_drop_df,
            invalid_with_prefilter_df=invalid_with_prefilter_df,
        )
        rows.append(
            {
                "source": source,
                "raw_count": raw_count,
                "normalized_count": normalized_count,
                "valid_count": valid_count,
                "prefiltered_valid_count": prefiltered_count,
                "prefilter_survival_rate": prefilter_survival_rate,
                "episode_count": episode_count,
                "episode_survival_rate": episode_survival_rate,
                "labelable_count": labelable_count,
                "labeled_count": labeled_count,
                "labeling_survival_rate": labeling_survival_rate,
                "effective_diversity_contribution": _effective_source_contribution(labeled_count),
                "promoted_to_persona_count": promoted_count,
                "failure_reason_top": reason,
                "failure_level": _failure_level(source, raw_count, labeled_count),
                "recommended_seed_set": _recommended_seed_set(root_dir, source, reason),
            }
        )
    return pd.DataFrame(rows)


def build_quality_failures(
    quality_checks: dict[str, Any],
    source_diagnostics_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build quality failures with hard/soft/warning levels."""
    labeled_sources = int((source_diagnostics_df.get("labeled_count", pd.Series(dtype=int)) > 0).sum()) if not source_diagnostics_df.empty else 0
    effective_labeled_sources = _effective_labeled_source_count(source_diagnostics_df)
    raw_sources = int((source_diagnostics_df.get("raw_count", pd.Series(dtype=int)) > 0).sum()) if not source_diagnostics_df.empty else 0
    largest_share = _largest_cluster_share(cluster_stats_df)
    min_cluster_size = int(quality_checks.get("min_cluster_size", 0))
    small_promoted = _small_promoted_count(cluster_stats_df, min_cluster_size)
    example_failures = _example_failure_count(persona_examples_df)

    rows = [
        _gate_row(
            "source_diversity_gate",
            "soft_fail" if effective_labeled_sources < MIN_LABELED_SOURCE_COUNT else "pass",
            round(float(effective_labeled_sources), 2),
            ">= 4 effective labeled sources; sources with labeled_count < 5 contribute fractionally",
        ),
        _gate_row("cluster_dominance_gate", "hard_fail" if largest_share > CLUSTER_DOMINANCE_SHARE_PCT else "pass", largest_share, "<= 70% largest promoted/core cluster share"),
        _gate_row("persona_promotion_gate", "hard_fail" if small_promoted else "pass", small_promoted, f"0 promoted personas below min_cluster_size={min_cluster_size}"),
        _gate_row("raw_to_labeled_source_gate", "soft_fail" if raw_sources >= 3 and labeled_sources <= 2 else "pass", f"raw={raw_sources}, labeled={labeled_sources}", "avoid raw coverage collapsing to <=2 labeled sources"),
        _gate_row("example_grounding_gate", "soft_fail" if example_failures else "pass", example_failures, "0 selected examples with grounding failures"),
        _gate_row("denominator_consistency_check", "pass", quality_checks.get("denominator_consistency", "explicit"), "all summary rows expose denominator_type/value"),
    ]
    for _, row in source_diagnostics_df.iterrows():
        raw_count = int(row.get("raw_count", 0) or 0)
        labeled_count = int(row.get("labeled_count", 0) or 0)
        source = str(row.get("source", "") or "")
        if raw_count > 0 and labeled_count == 0:
            rows.append(
                _gate_row(
                    f"source_failure:{source}",
                    "soft_fail",
                    labeled_count,
                    "raw_count > 0 should produce labeled_count > 0",
                )
            )
    return pd.DataFrame(rows)


def finalize_quality_checks(
    base_checks: dict[str, Any],
    source_diagnostics_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
) -> dict[str, Any]:
    """Apply workbook-level quality gates."""
    checks = dict(base_checks)
    labeled_sources = int((source_diagnostics_df.get("labeled_count", pd.Series(dtype=int)) > 0).sum()) if not source_diagnostics_df.empty else 0
    effective_labeled_sources = _effective_labeled_source_count(source_diagnostics_df)
    raw_sources = int((source_diagnostics_df.get("raw_count", pd.Series(dtype=int)) > 0).sum()) if not source_diagnostics_df.empty else 0
    min_cluster_size = persona_min_cluster_size(int(checks.get("labeled_count", 0)))
    largest_share = _largest_cluster_share(cluster_stats_df)
    failed_sources = (
        source_diagnostics_df[
            (pd.to_numeric(source_diagnostics_df.get("raw_count", pd.Series(dtype=int)), errors="coerce").fillna(0) > 0)
            & (pd.to_numeric(source_diagnostics_df.get("labeled_count", pd.Series(dtype=int)), errors="coerce").fillna(0) <= 0)
        ]["source"].astype(str).tolist()
        if not source_diagnostics_df.empty
        else []
    )
    checks.update(
        {
            "labeled_source_count": labeled_sources,
            "effective_labeled_source_count": round(float(effective_labeled_sources), 2),
            "raw_source_count": raw_sources,
            "min_cluster_size": min_cluster_size,
            "largest_cluster_share": largest_share,
            "single_cluster_dominance": is_single_cluster_dominant(largest_share),
            "small_promoted_persona_count": _small_promoted_count(cluster_stats_df, min_cluster_size),
            "example_grounding_failure_count": _example_failure_count(persona_examples_df),
            "denominator_consistency": "explicit",
            "source_failures": " | ".join(failed_sources),
        }
    )
    hard_fail = (
        is_single_cluster_dominant(largest_share)
        or checks["small_promoted_persona_count"] > 0
    )
    exploratory_fail = (
        effective_labeled_sources < MIN_LABELED_SOURCE_COUNT
        or checks["example_grounding_failure_count"] > 0
        or (raw_sources >= 3 and labeled_sources <= 2)
        or bool(failed_sources)
    )
    checks["quality_flag"] = QUALITY_FLAG_UNSTABLE if hard_fail else QUALITY_FLAG_EXPLORATORY if exploratory_fail else QUALITY_FLAG_OK
    return checks


def build_survival_funnel_by_source(source_diagnostics_df: pd.DataFrame) -> pd.DataFrame:
    """Build a compact by-source funnel table from source diagnostics."""
    if source_diagnostics_df.empty:
        return pd.DataFrame()
    return source_diagnostics_df[
        [
            "source",
            "valid_count",
            "prefiltered_valid_count",
            "prefilter_survival_rate",
            "episode_count",
            "episode_survival_rate",
            "labeled_count",
            "labeling_survival_rate",
            "effective_diversity_contribution",
            "failure_reason_top",
            "failure_level",
        ]
    ].copy()


def _count_jsonl_lines(path: Path) -> int:
    """Count non-empty JSONL lines."""
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def _episode_source_lookup(episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Return unique episode-to-source mapping."""
    if episodes_df.empty or not {"episode_id", "source"}.issubset(episodes_df.columns):
        return pd.DataFrame(columns=["episode_id", "source"])
    return episodes_df[["episode_id", "source"]].drop_duplicates("episode_id")


def _with_episode_source(df: pd.DataFrame, episode_source: pd.DataFrame) -> pd.DataFrame:
    """Attach source from episode_id when needed."""
    if df.empty:
        return pd.DataFrame(columns=["source"])
    if "source" in df.columns:
        return df
    if "episode_id" not in df.columns:
        return pd.DataFrame(columns=["source"])
    return df.merge(episode_source, on="episode_id", how="left")


def _source_count(df: pd.DataFrame, source: str, column: str) -> int:
    """Return a numeric count for one source in a pre-aggregated table."""
    return aggregated_source_count(df, source, column)


def _source_failure_reason(
    source: str,
    raw_count: int,
    normalized_count: int,
    valid_count: int,
    prefiltered_count: int,
    episode_count: int,
    labelable_count: int,
    labeled_count: int,
    relevance_drop_df: pd.DataFrame,
    invalid_with_prefilter_df: pd.DataFrame,
) -> str:
    """Explain the dominant drop-off point for a source."""
    if raw_count <= 0:
        return "no_raw_records"
    if normalized_count <= 0:
        return "raw_not_normalized"
    if valid_count <= 0:
        return "invalid_filter_removed_all: " + _top_reason(invalid_with_prefilter_df, source, "invalid_reason")
    if prefiltered_count <= 0:
        return "relevance_prefilter_removed_all: " + _top_reason(relevance_drop_df, source, "prefilter_reason")
    if episode_count <= 0:
        return "episode_extraction_zero_from_prefiltered_candidates"
    if labelable_count <= 0:
        return "labelability_gate_removed_all"
    if labeled_count <= 0:
        return "label_output_missing_after_labelability"
    return "labeled_output_present"


def _top_reason(df: pd.DataFrame, source: str, column: str) -> str:
    """Return top reason text for one source."""
    if df.empty or column not in df.columns or SOURCE_FIELD not in df.columns:
        return "reason_unavailable"
    subset = df[df[SOURCE_FIELD].astype(str) == source]
    if subset.empty:
        return "reason_unavailable"
    return str(subset[column].fillna("unknown").astype(str).value_counts().idxmax())


def _failure_level(source: str, raw_count: int, labeled_count: int) -> str:
    """Treat raw-without-labeled source coverage as a failure."""
    if raw_count > 0 and labeled_count <= 0:
        return "failure"
    if source in RAW_WITHOUT_LABEL_FAILURE_SOURCES and raw_count <= 0:
        return "warning"
    return "pass"


def _effective_source_contribution(labeled_count: int) -> float:
    """Count low-volume labeled sources as fractional diversity contributors."""
    if labeled_count <= 0:
        return 0.0
    return min(1.0, float(labeled_count) / 5.0)


def _effective_labeled_source_count(source_diagnostics_df: pd.DataFrame) -> float:
    """Return effective labeled source count using weak contributions below 5 labels."""
    if source_diagnostics_df.empty or "labeled_count" not in source_diagnostics_df.columns:
        return 0.0
    counts = pd.to_numeric(source_diagnostics_df["labeled_count"], errors="coerce").fillna(0).astype(int)
    return float(sum(_effective_source_contribution(int(count)) for count in counts.tolist()))


def _recommended_seed_set(root_dir: Path, source: str, reason: str) -> str:
    """Return active source-friendly seeds when relevance loss suggests seed mismatch."""
    if "relevance_prefilter_removed_all" not in reason:
        return ""
    seed_path = _seed_path(root_dir, source)
    if seed_path is None:
        return "review source-specific BI/reporting seed bank"
    data = load_yaml(seed_path)
    seeds = data.get("active_core_seeds") or data.get("core_seeds") or []
    values: list[str] = []
    for item in seeds:
        if isinstance(item, dict):
            values.append(str(item.get("seed", "")).strip())
        else:
            values.append(str(item).strip())
    return " | ".join(value for value in values if value)


def _seed_path(root_dir: Path, source: str) -> Path | None:
    """Find the local seed file for a source."""
    candidates = [
        root_dir / "config" / "seeds" / "business_communities" / f"{source}.yaml",
        root_dir / "config" / "seeds" / "existing_forums" / f"{source}.yaml",
        root_dir / "config" / "seeds" / "reddit" / f"{source}.yaml",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _largest_cluster_share(cluster_stats_df: pd.DataFrame) -> float:
    """Return largest cluster share percentage."""
    if cluster_stats_df.empty or "share_of_total" not in cluster_stats_df.columns:
        return 0.0
    values = pd.to_numeric(cluster_stats_df["share_of_total"], errors="coerce").fillna(0)
    return round(float(values.max()), 1) if not values.empty else 0.0


def _small_promoted_count(cluster_stats_df: pd.DataFrame, min_cluster_size: int) -> int:
    """Count promoted personas below the size floor."""
    if cluster_stats_df.empty or "promotion_status" not in cluster_stats_df.columns:
        return 0
    promoted = cluster_stats_df[cluster_stats_df["promotion_status"].astype(str).eq("promoted_persona")]
    sizes = pd.to_numeric(promoted.get("persona_size", pd.Series(dtype=int)), errors="coerce").fillna(0)
    return int((sizes < min_cluster_size).sum())


def _example_failure_count(persona_examples_df: pd.DataFrame) -> int:
    """Count selected examples with weak grounding evidence."""
    if persona_examples_df.empty:
        return 0
    quality = persona_examples_df.get("quote_quality", pd.Series(dtype=str)).astype(str)
    text_len = pd.to_numeric(persona_examples_df.get("source_text_length", pd.Series(dtype=int)), errors="coerce").fillna(0)
    reasons = persona_examples_df.get("rejection_reason", pd.Series(dtype=str)).fillna("").astype(str)
    return int((quality.isin({"reject", "borderline"}) | (text_len < 80) | reasons.ne("")).sum())


def _gate_row(metric: str, level: str, value: Any, threshold: str) -> dict[str, Any]:
    """Build one quality gate row."""
    return {
        "metric": metric,
        "level": level,
        "value": value,
        "threshold": threshold,
        "passed": level == "pass",
    }
