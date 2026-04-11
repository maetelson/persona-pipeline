"""Source-level diagnostics and workbook quality gates."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.quality_status import flatten_quality_status_result
from src.utils.io import load_yaml, read_parquet
from src.utils.pipeline_schema import (
    CLUSTER_DOMINANCE_SHARE_PCT,
    DENOMINATOR_PERSONA_CORE_LABELED_ROWS,
    LABELABLE_STATUSES,
    QUALITY_FLAG_EXPLORATORY,
    QUALITY_FLAG_OK,
    QUALITY_FLAG_UNSTABLE,
    RAW_WITHOUT_LABEL_FAILURE_SOURCES,
    SOURCE_FIELD,
    aggregated_source_count,
    canonical_source_name,
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
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["source", "raw_count", "raw_file_count"])
    frame["source"] = frame["source"].map(canonical_source_name)
    grouped = (
        frame.groupby("source", dropna=False, as_index=False)[["raw_count", "raw_file_count"]]
        .sum()
        .sort_values("source")
        .reset_index(drop=True)
    )
    return grouped


def build_metric_glossary() -> pd.DataFrame:
    """Build explicit metric definitions for workbook readers."""
    rows = [
        ("total_labeled_records", "labeled_episode_rows", "All labeled episode rows in the final labeled stage, including rows outside persona-core clustering."),
        ("persona_core_labeled_records", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Labeled episode rows with persona_core_eligible=true; this is the clustering denominator for persona shares."),
        ("persona_count", "promoted_persona_rows", "Count of personas whose final promotion_status remains promoted_persona in the workbook."),
        ("exploratory_bucket_count", "exploratory_cluster_rows", "Count of non-promoted exploratory clusters shown for context."),
        ("persona_core_unknown_ratio", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Ratio of persona-core labeled rows that still contain unresolved core label families."),
        ("overall_unknown_ratio", "labeled_episode_rows", "Ratio of all labeled rows with unresolved core label families, including rows outside persona-core clustering."),
        ("persona_core_coverage_of_all_labeled_pct", "labeled_episode_rows", "Percentage of all labeled rows that remain inside the persona-core subset used for clustering."),
        ("largest_cluster_share_of_core_labeled", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Largest promoted or exploratory persona cluster share over persona-core labeled rows."),
        ("largest_labeled_source_share_pct", "labeled_episode_rows", "Largest source contribution share over all labeled episode rows."),
        ("promoted_persona_example_coverage_pct", "promoted_persona_rows", "Percentage of promoted personas that have any accepted grounding state, including weakly grounded personas."),
        ("promoted_persona_grounded_count", "promoted_persona_rows", "Count of promoted personas with at least one grounded representative example."),
        ("promoted_persona_weakly_grounded_count", "promoted_persona_rows", "Count of promoted personas whose evidence only meets weak fallback policy, not normal grounded selection."),
        ("promoted_persona_ungrounded_count", "promoted_persona_rows", "Count of promoted personas with no acceptable grounding evidence under policy."),
        ("promoted_personas_missing_examples", "promoted_persona_rows", "Pipe-delimited promoted persona ids with no accepted selected example rows."),
        ("promoted_personas_weakly_grounded", "promoted_persona_rows", "Pipe-delimited promoted persona ids that remain visible only with weak grounding coverage."),
        ("promotion_status", "persona_cluster_row", "Final workbook promotion label after applying grounding policy merge. This can stay promoted while grounding_status is weak or ungrounded."),
        ("base_promotion_status", "persona_cluster_row", "Size and dominance based promotion label before grounding policy is applied."),
        ("grounding_status", "persona_cluster_row", "Reviewer-facing grounding state for a persona: grounded, weakly_grounded, ungrounded, or not_applicable."),
        ("promotion_grounding_status", "persona_cluster_row", "Combined promotion-grounding state such as promoted_and_grounded or promoted_but_ungrounded."),
        ("selection_strength", "persona_example_row", "Workbook-facing selection label for an example row, distinguishing grounded selections from weak_grounding_fallback rows."),
        ("grounding_strength", "persona_example_row", "Policy bucket for one example candidate: strong, grounded, weak, or unacceptable."),
        ("coverage_selection_reason", "persona_example_row", "Why a row was selected: normal score_plus_diversity_policy or explicit minimum coverage policy."),
        ("grounding_fit_score", "persona_example_row", "Selection-time score component that rewards axis fit and penalizes mismatches for grounding quality."),
        ("row_grain", "workbook_column_label", "Display-only export label for grain. In source_diagnostics this tells the reviewer whether a row is post-, episode-, or mixed-grain."),
        ("raw_records", "raw_jsonl_rows", "Non-empty JSONL rows under data/raw/{source}/*.jsonl."),
        ("normalized_records", "normalized_posts_rows", "Rows in data/normalized/normalized_posts.parquet after source normalizers."),
        ("valid_records", "valid_candidate_rows", "Rows in data/valid/valid_candidates.parquet before relevance prefiltering."),
        ("prefiltered_valid_records", "prefiltered_valid_rows", "Rows in data/valid/valid_candidates_prefiltered.parquet used by episode building when present."),
        ("episodes", "episode_rows", "Rows in data/episodes/episode_table.parquet; one post can yield zero or more episodes."),
        ("labeled_records", "labeled_episode_rows", "Rows in data/labeled/labeled_episodes.parquet joined to episodes by episode_id."),
        ("labelable_count", "labelability_rows", "Episode rows with labelability_status in labelable or borderline."),
        ("core_labeled", "persona_core_eligible_rows", "Labeled rows with persona_core_eligible=true, used for persona clustering."),
        ("promoted_to_persona_count", "promoted_cluster_rows", "Source rows assigned to clusters that pass persona promotion gates."),
        ("raw_record_count", "raw_jsonl_rows", "Source-level raw JSONL record count in source_diagnostics; grain is raw record rows, not normalized post rows."),
        ("normalized_post_count", "normalized_posts_rows", "Source-level normalized post count after source normalizers; grain is normalized post rows."),
        ("valid_post_count", "normalized_posts_rows", "Source-level valid post count after invalid filtering; numerator grain and denominator grain are normalized post rows."),
        ("prefiltered_valid_post_count", "valid_candidate_rows", "Source-level post count retained by the relevance prefilter; numerator grain and denominator grain are valid post rows."),
        ("valid_posts_per_normalized_post_pct", "normalized_posts_rows", "Same-grain post funnel percentage: valid_post_count / normalized_post_count * 100."),
        ("prefiltered_valid_posts_per_valid_post_pct", "valid_candidate_rows", "Same-grain post funnel percentage: prefiltered_valid_post_count / valid_post_count * 100."),
        ("episode_count", "episode_rows", "Source-level episode count in episode_table.parquet; one prefiltered post can yield zero or more episodes."),
        ("labeled_episode_count", "episode_rows", "Source-level episode count that appears in labeled_episodes.parquet; same grain as episode_count."),
        ("labelable_episode_count", "labeled_episode_rows", "Source-level labeled episode count whose labelability_status is labelable or borderline; same grain as labeled_episode_count."),
        ("labeled_episodes_per_episode_pct", "episode_rows", "Same-grain episode funnel percentage: labeled_episode_count / episode_count * 100."),
        ("labelable_episodes_per_labeled_episode_pct", "labeled_episode_rows", "Same-grain episode funnel percentage: labelable_episode_count / labeled_episode_count * 100."),
        ("episodes_per_prefiltered_valid_post", "prefiltered_valid_rows", "Cross-grain bridge metric: episode_count / prefiltered_valid_post_count. This can exceed 1.0 and is not a funnel rate."),
        ("labeled_episodes_per_prefiltered_valid_post", "prefiltered_valid_rows", "Cross-grain bridge metric: labeled_episode_count / prefiltered_valid_post_count. This can exceed 1.0 and is not a funnel rate."),
        ("labelable_episodes_per_prefiltered_valid_post", "prefiltered_valid_rows", "Cross-grain bridge metric: labelable_episode_count / prefiltered_valid_post_count. This can exceed 1.0 and is not a funnel rate."),
        ("effective_diversity_contribution", "source_quality_score", "Weak source-diversity contribution capped at 1.0 from labeled episode volume; this is a source-quality score, not a funnel metric."),
        ("promoted_persona_episode_count", "promoted_cluster_rows", "Source-level episode count assigned to promoted personas; grain is promoted persona assignment rows."),
    ]
    return pd.DataFrame(rows, columns=["metric", "denominator_type", "definition"])


def build_source_stage_counts(
    root_dir: Path,
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build source-level counts before workbook formatting."""
    raw_counts_df = count_raw_jsonl_by_source(root_dir)
    prefiltered_df = read_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    labelability_df = read_parquet(root_dir / "data" / "labeled" / "labelability_audit.parquet")
    relevance_drop_df = read_parquet(root_dir / "data" / "prefilter" / "relevance_drop.parquet")
    invalid_with_prefilter_df = read_parquet(root_dir / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")

    episode_source = _episode_source_lookup(episodes_df)
    labeled_with_source = _with_episode_source(labeled_df, episode_source)
    labelable_df = labelability_df[labelability_df.get("labelability_status", pd.Series(dtype=str)).astype(str).isin(LABELABLE_STATUSES)]
    labelable_with_source = _with_episode_source(labelable_df, episode_source)
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
        set(raw_counts_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name))
        | set(normalized_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name))
        | set(valid_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name))
        | set(episodes_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name))
    )
    rows: list[dict[str, Any]] = []
    for source in sources:
        raw_count = _source_count(raw_counts_df, source, "raw_count")
        normalized_count = source_row_count(normalized_df, source)
        valid_count = source_row_count(valid_df, source)
        prefiltered_count = source_row_count(prefiltered_df, source)
        episode_count = source_row_count(episodes_df, source)
        labelable_count = source_row_count(labelable_with_source, source)
        labeled_count = source_row_count(labeled_with_source, source)
        promoted_count = source_row_count(promoted_with_source, source)
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
                "raw_record_count": raw_count,
                "normalized_post_count": normalized_count,
                "valid_post_count": valid_count,
                "prefiltered_valid_post_count": prefiltered_count,
                "episode_count": episode_count,
                "labeled_episode_count": labeled_count,
                "labelable_episode_count": labelable_count,
                "effective_diversity_contribution": _effective_source_contribution(labeled_count),
                "promoted_persona_episode_count": promoted_count,
                "failure_reason_top": reason,
                "failure_level": _failure_level(source, raw_count, labeled_count),
                "recommended_seed_set": _recommended_seed_set(root_dir, source, reason),
                "metric_glossary_note": "See metric_glossary for source_diagnostics metric definitions and row grains.",
            }
        )
    result = pd.DataFrame(rows)
    _validate_source_stage_counts(result)
    return result


def build_source_diagnostics(source_stage_counts_df: pd.DataFrame) -> pd.DataFrame:
    """Build workbook-friendly source diagnostics with explicit grain and section typing."""
    if source_stage_counts_df.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "section",
                "grain",
                "metric_name",
                "metric_value",
                "metric_type",
                "denominator_metric",
                "denominator_grain",
                "denominator_value",
                "bounded_range",
                "is_same_grain_funnel",
                "metric_definition",
                "failure_reason_top",
                "failure_level",
                "recommended_seed_set",
            ]
        )
    definitions = {
        row["metric"]: row["definition"]
        for _, row in build_metric_glossary().iterrows()
        if str(row.get("metric", "")).strip()
    }
    rows: list[dict[str, Any]] = []
    for _, source_row in source_stage_counts_df.iterrows():
        source = str(source_row.get("source", "") or "")
        raw_record_count = int(source_row.get("raw_record_count", 0) or 0)
        normalized_post_count = int(source_row.get("normalized_post_count", 0) or 0)
        valid_post_count = int(source_row.get("valid_post_count", 0) or 0)
        prefiltered_valid_post_count = int(source_row.get("prefiltered_valid_post_count", 0) or 0)
        episode_count = int(source_row.get("episode_count", 0) or 0)
        labeled_episode_count = int(source_row.get("labeled_episode_count", 0) or 0)
        labelable_episode_count = int(source_row.get("labelable_episode_count", 0) or 0)
        failure_reason_top = str(source_row.get("failure_reason_top", "") or "")
        failure_level = str(source_row.get("failure_level", "") or "")
        recommended_seed_set = str(source_row.get("recommended_seed_set", "") or "")

        metric_specs = [
            ("raw_ingest", "other", "raw_record_count", raw_record_count, "count", "", "", "", "", False),
            ("post_funnel", "post", "normalized_post_count", normalized_post_count, "count", "", "", "", "", True),
            ("post_funnel", "post", "valid_post_count", valid_post_count, "count", "", "", "", "", True),
            ("post_funnel", "post", "prefiltered_valid_post_count", prefiltered_valid_post_count, "count", "", "", "", "", True),
            (
                "post_funnel",
                "post",
                "valid_posts_per_normalized_post_pct",
                round_pct(valid_post_count, normalized_post_count) if normalized_post_count else 0.0,
                "percentage",
                "normalized_post_count",
                "post",
                normalized_post_count,
                "0-100_pct",
                True,
            ),
            (
                "post_funnel",
                "post",
                "prefiltered_valid_posts_per_valid_post_pct",
                round_pct(prefiltered_valid_post_count, valid_post_count) if valid_post_count else 0.0,
                "percentage",
                "valid_post_count",
                "post",
                valid_post_count,
                "0-100_pct",
                True,
            ),
            ("episode_funnel", "episode", "episode_count", episode_count, "count", "", "", "", "", True),
            ("episode_funnel", "episode", "labeled_episode_count", labeled_episode_count, "count", "", "", "", "", True),
            ("episode_funnel", "episode", "labelable_episode_count", labelable_episode_count, "count", "", "", "", "", True),
            (
                "episode_funnel",
                "episode",
                "labeled_episodes_per_episode_pct",
                round_pct(labeled_episode_count, episode_count) if episode_count else 0.0,
                "percentage",
                "episode_count",
                "episode",
                episode_count,
                "0-100_pct",
                True,
            ),
            (
                "episode_funnel",
                "episode",
                "labelable_episodes_per_labeled_episode_pct",
                round_pct(labelable_episode_count, labeled_episode_count) if labeled_episode_count else 0.0,
                "percentage",
                "labeled_episode_count",
                "episode",
                labeled_episode_count,
                "0-100_pct",
                True,
            ),
            (
                "cross_grain_bridge",
                "mixed_grain_bridge",
                "episodes_per_prefiltered_valid_post",
                round(float(episode_count) / float(prefiltered_valid_post_count), 2) if prefiltered_valid_post_count else 0.0,
                "ratio",
                "prefiltered_valid_post_count",
                "post",
                prefiltered_valid_post_count,
                "unbounded_ratio",
                False,
            ),
            (
                "cross_grain_bridge",
                "mixed_grain_bridge",
                "labeled_episodes_per_prefiltered_valid_post",
                round(float(labeled_episode_count) / float(prefiltered_valid_post_count), 2) if prefiltered_valid_post_count else 0.0,
                "ratio",
                "prefiltered_valid_post_count",
                "post",
                prefiltered_valid_post_count,
                "unbounded_ratio",
                False,
            ),
            (
                "cross_grain_bridge",
                "mixed_grain_bridge",
                "labelable_episodes_per_prefiltered_valid_post",
                round(float(labelable_episode_count) / float(prefiltered_valid_post_count), 2) if prefiltered_valid_post_count else 0.0,
                "ratio",
                "prefiltered_valid_post_count",
                "post",
                prefiltered_valid_post_count,
                "unbounded_ratio",
                False,
            ),
            (
                "source_quality",
                "other",
                "effective_diversity_contribution",
                round(float(source_row.get("effective_diversity_contribution", 0.0) or 0.0), 2),
                "ratio",
                "",
                "",
                "",
                "0-1_ratio",
                False,
            ),
            (
                "source_quality",
                "episode",
                "promoted_persona_episode_count",
                int(source_row.get("promoted_persona_episode_count", 0) or 0),
                "count",
                "",
                "",
                "",
                "",
                False,
            ),
        ]
        for section, grain, metric_name, metric_value, metric_type, denominator_metric, denominator_grain, denominator_value, bounded_range, is_same_grain_funnel in metric_specs:
            rows.append(
                {
                    "source": source,
                    "section": section,
                    "grain": grain,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "metric_type": metric_type,
                    "denominator_metric": denominator_metric,
                    "denominator_grain": denominator_grain,
                    "denominator_value": denominator_value,
                    "bounded_range": bounded_range,
                    "is_same_grain_funnel": is_same_grain_funnel,
                    "metric_definition": definitions.get(metric_name, ""),
                    "failure_reason_top": failure_reason_top,
                    "failure_level": failure_level,
                    "recommended_seed_set": recommended_seed_set,
                }
            )
    result = pd.DataFrame(rows)
    _validate_source_diagnostics_frame(result)
    return result


def build_quality_failures(
    quality_checks: dict[str, Any],
    source_stage_counts_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build quality failures from the evaluated quality result plus source failure rows."""
    labeled_sources = int((source_stage_counts_df.get("labeled_episode_count", pd.Series(dtype=int)) > 0).sum()) if not source_stage_counts_df.empty else 0
    effective_labeled_sources = _effective_labeled_source_count(source_stage_counts_df)
    raw_sources = int((source_stage_counts_df.get("raw_record_count", pd.Series(dtype=int)) > 0).sum()) if not source_stage_counts_df.empty else 0
    largest_share = _largest_cluster_share(cluster_stats_df)
    min_cluster_size = int(quality_checks.get("min_cluster_size", 0))
    small_promoted = _small_promoted_count(cluster_stats_df, min_cluster_size)

    rows = [
        _gate_row("overall_uncertainty_gate", _gate_level_from_status(str(quality_checks.get("overall_unknown_status", "OK"))), round(float(quality_checks.get("overall_unknown_ratio", 0.0) or 0.0), 6), str(quality_checks.get("overall_unknown_reason_keys", "") or "")),
        _gate_row("core_coverage_gate", _gate_level_from_status(str(quality_checks.get("core_coverage_status", "OK"))), round(float(quality_checks.get("persona_core_coverage_of_all_labeled_pct", 0.0) or 0.0), 1), str(quality_checks.get("core_coverage_reason_keys", "") or "")),
        _gate_row(
            "source_diversity_gate",
            _gate_level_from_status(str(quality_checks.get("effective_source_diversity_status", "OK"))),
            round(float(effective_labeled_sources), 2),
            str(quality_checks.get("effective_source_diversity_reason_keys", "") or ""),
        ),
        _gate_row("source_concentration_gate", _gate_level_from_status(str(quality_checks.get("source_concentration_status", "OK"))), float(quality_checks.get("largest_labeled_source_share_pct", 0.0) or 0.0), str(quality_checks.get("source_concentration_reason_keys", "") or "")),
        _gate_row("cluster_dominance_gate", _gate_level_from_status(str(quality_checks.get("largest_cluster_dominance_status", "OK"))), largest_share, str(quality_checks.get("largest_cluster_dominance_reason_keys", "") or "")),
        _gate_row("persona_promotion_gate", "hard_fail" if small_promoted else "pass", small_promoted, f"0 promoted personas below min_cluster_size={min_cluster_size}"),
        _gate_row("raw_to_labeled_source_gate", "soft_fail" if raw_sources >= 3 and labeled_sources <= 2 else "pass", f"raw={raw_sources}, labeled={labeled_sources}", "avoid raw coverage collapsing to <=2 labeled sources"),
        _gate_row("example_grounding_gate", _gate_level_from_status(str(quality_checks.get("grounding_coverage_status", "OK"))), int(quality_checks.get("example_grounding_failure_count", 0) or 0), str(quality_checks.get("grounding_coverage_reason_keys", "") or "")),
        _gate_row("promoted_example_coverage_gate", _gate_level_from_status(str(quality_checks.get("grounding_coverage_status", "OK"))), float(quality_checks.get("promoted_persona_example_coverage_pct", 0.0) or 0.0), str(quality_checks.get("grounding_coverage_reason_keys", "") or "")),
        _gate_row("denominator_consistency_check", "pass", quality_checks.get("denominator_consistency", "explicit"), "all summary rows expose denominator_type/value"),
    ]
    for _, row in source_stage_counts_df.iterrows():
        raw_count = int(row.get("raw_record_count", 0) or 0)
        labeled_count = int(row.get("labeled_episode_count", 0) or 0)
        source = str(row.get("source", "") or "")
        if raw_count > 0 and labeled_count == 0:
            rows.append(
                _gate_row(
                    f"source_failure:{source}",
                    "soft_fail",
                    labeled_count,
                    "raw_record_count > 0 should produce labeled_episode_count > 0",
                )
            )
    return pd.DataFrame(rows)


def finalize_quality_checks(evaluated_quality_result: dict[str, Any]) -> dict[str, Any]:
    """Flatten the evaluated quality policy result for workbook rendering only."""
    return flatten_quality_status_result(evaluated_quality_result)


def build_survival_funnel_by_source(source_stage_counts_df: pd.DataFrame) -> pd.DataFrame:
    """Build a compact by-source diagnostics table from validated source stage counts."""
    if source_stage_counts_df.empty:
        return pd.DataFrame()
    frame = source_stage_counts_df.copy()
    frame["valid_posts_per_normalized_post_pct"] = frame.apply(
        lambda row: round_pct(row.get("valid_post_count", 0), row.get("normalized_post_count", 0)) if int(row.get("normalized_post_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["prefiltered_valid_posts_per_valid_post_pct"] = frame.apply(
        lambda row: round_pct(row.get("prefiltered_valid_post_count", 0), row.get("valid_post_count", 0)) if int(row.get("valid_post_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["labeled_episodes_per_episode_pct"] = frame.apply(
        lambda row: round_pct(row.get("labeled_episode_count", 0), row.get("episode_count", 0)) if int(row.get("episode_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["labelable_episodes_per_labeled_episode_pct"] = frame.apply(
        lambda row: round_pct(row.get("labelable_episode_count", 0), row.get("labeled_episode_count", 0)) if int(row.get("labeled_episode_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["episodes_per_prefiltered_valid_post"] = frame.apply(
        lambda row: round(float(row.get("episode_count", 0) or 0) / float(row.get("prefiltered_valid_post_count", 0) or 1), 2)
        if int(row.get("prefiltered_valid_post_count", 0) or 0)
        else 0.0,
        axis=1,
    )
    frame["labeled_episodes_per_prefiltered_valid_post"] = frame.apply(
        lambda row: round(float(row.get("labeled_episode_count", 0) or 0) / float(row.get("prefiltered_valid_post_count", 0) or 1), 2)
        if int(row.get("prefiltered_valid_post_count", 0) or 0)
        else 0.0,
        axis=1,
    )
    frame["labelable_episodes_per_prefiltered_valid_post"] = frame.apply(
        lambda row: round(float(row.get("labelable_episode_count", 0) or 0) / float(row.get("prefiltered_valid_post_count", 0) or 1), 2)
        if int(row.get("prefiltered_valid_post_count", 0) or 0)
        else 0.0,
        axis=1,
    )
    return frame[
        [
            "source",
            "normalized_post_count",
            "valid_post_count",
            "prefiltered_valid_post_count",
            "valid_posts_per_normalized_post_pct",
            "prefiltered_valid_posts_per_valid_post_pct",
            "episode_count",
            "labeled_episode_count",
            "labelable_episode_count",
            "labeled_episodes_per_episode_pct",
            "labelable_episodes_per_labeled_episode_pct",
            "episodes_per_prefiltered_valid_post",
            "labeled_episodes_per_prefiltered_valid_post",
            "labelable_episodes_per_prefiltered_valid_post",
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


def _effective_labeled_source_count(source_stage_counts_df: pd.DataFrame) -> float:
    """Return effective labeled source count using weak contributions below 5 labels."""
    if source_stage_counts_df.empty or "labeled_episode_count" not in source_stage_counts_df.columns:
        return 0.0
    counts = pd.to_numeric(source_stage_counts_df["labeled_episode_count"], errors="coerce").fillna(0).astype(int)
    return float(sum(_effective_source_contribution(int(count)) for count in counts.tolist()))


def _validate_source_stage_counts(df: pd.DataFrame) -> None:
    """Validate source stage counts before workbook formatting."""
    if df.empty:
        return
    raw = pd.to_numeric(df.get("raw_record_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    normalized = pd.to_numeric(df.get("normalized_post_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    valid = pd.to_numeric(df.get("valid_post_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    prefiltered = pd.to_numeric(df.get("prefiltered_valid_post_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    episodes = pd.to_numeric(df.get("episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    labeled = pd.to_numeric(df.get("labeled_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    labelable = pd.to_numeric(df.get("labelable_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    if (normalized > raw).any():
        raise ValueError("source_diagnostics invariant failed: normalized_post_count cannot exceed raw_record_count.")
    if (valid > normalized).any():
        raise ValueError("source_diagnostics invariant failed: valid_post_count cannot exceed normalized_post_count.")
    if (prefiltered > valid).any():
        raise ValueError("source_diagnostics invariant failed: prefiltered_valid_post_count cannot exceed valid_post_count.")
    if (labeled > episodes).any():
        raise ValueError("source_diagnostics invariant failed: labeled_episode_count cannot exceed episode_count.")
    if (labelable > labeled).any():
        raise ValueError("source_diagnostics invariant failed: labelable_episode_count cannot exceed labeled_episode_count.")


def _validate_source_diagnostics_frame(df: pd.DataFrame) -> None:
    """Validate workbook-facing source diagnostics rows and naming contracts."""
    if df.empty:
        return
    forbidden_columns = {
        "raw_count",
        "normalized_count",
        "valid_count",
        "prefiltered_valid_count",
        "prefilter_survival_rate",
        "episode_survival_rate",
        "labelable_count",
        "labeled_count",
        "labeling_survival_rate",
        "promoted_to_persona_count",
    }
    present_forbidden = sorted(column for column in forbidden_columns if column in df.columns)
    if present_forbidden:
        raise ValueError(
            "source_diagnostics invariant failed: legacy ambiguous columns present: "
            + ", ".join(present_forbidden)
        )
    required_columns = {
        "source",
        "section",
        "grain",
        "metric_name",
        "metric_value",
        "metric_type",
        "denominator_metric",
        "denominator_grain",
        "denominator_value",
        "bounded_range",
        "is_same_grain_funnel",
        "metric_definition",
    }
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError("source_diagnostics invariant failed: missing required columns: " + ", ".join(missing))
    mixed_bridge = df[df["grain"].astype(str).eq("mixed_grain_bridge")]
    if not mixed_bridge.empty:
        invalid_names = mixed_bridge[
            mixed_bridge["metric_name"].astype(str).str.contains("rate|share|survival", case=False, regex=True)
        ]
        if not invalid_names.empty:
            bad = ", ".join(sorted(invalid_names["metric_name"].astype(str).unique().tolist()))
            raise ValueError(f"source_diagnostics invariant failed: mixed-grain bridge metrics cannot use rate/share/survival names: {bad}")
    percentage_rows = df[df["metric_type"].astype(str).eq("percentage")]
    if not percentage_rows.empty:
        values = pd.to_numeric(percentage_rows["metric_value"], errors="coerce").fillna(0.0)
        if ((values < 0.0) | (values > 100.0)).any():
            raise ValueError("source_diagnostics invariant failed: same-grain percentage metrics must be within 0-100.")


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
    if cluster_stats_df.empty:
        return 0.0
    share_column = "share_of_core_labeled" if "share_of_core_labeled" in cluster_stats_df.columns else "share_of_total"
    if share_column not in cluster_stats_df.columns:
        return 0.0
    values = pd.to_numeric(cluster_stats_df[share_column], errors="coerce").fillna(0)
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


def _gate_level_from_status(status: str) -> str:
    """Map centralized status strings into quality failure gate levels."""
    if status == "FAIL":
        return "hard_fail"
    if status == "WARN":
        return "soft_fail"
    return "pass"
