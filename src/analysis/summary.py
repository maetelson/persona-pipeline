"""High-level source and run summaries."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from src.analysis.diagnostics import count_raw_jsonl_by_source
from src.analysis.stage_counts import build_pipeline_stage_counts, build_pipeline_stage_rows
from src.analysis.quality_status import build_quality_metrics, quality_display_thresholds
from src.utils.pipeline_schema import (
    DENOMINATOR_EPISODE_ROWS,
    DENOMINATOR_LABELED_EPISODE_ROWS,
    DENOMINATOR_NORMALIZED_POST_ROWS,
    DENOMINATOR_PERSONA_CORE_LABELED_ROWS,
    DENOMINATOR_PREFILTERED_VALID_ROWS,
    DENOMINATOR_RAW_RECORD_ROWS,
    DENOMINATOR_VALID_CANDIDATE_ROWS,
    PIPELINE_STAGE_METRIC_NAMES,
    QUALITY_FLAG_OK,
    QUALITY_FLAG_UNSTABLE,
    STATUS_FAIL,
    SOURCE_FIELD,
    aggregated_source_count,
    canonical_source_name,
    round_pct,
    source_row_count,
)


def build_source_summary(normalized_df: pd.DataFrame, valid_df: pd.DataFrame, episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize counts by source across core stages."""
    sources = sorted(set(normalized_df.get(SOURCE_FIELD, pd.Series(dtype=str)).astype(str).map(canonical_source_name).tolist()))
    rows: list[dict[str, int | str]] = []
    for source in sources:
        rows.append(
            {
                "source": source,
                "normalized_count": source_row_count(normalized_df, source),
                "valid_count": source_row_count(valid_df, source),
                "episode_count": source_row_count(episodes_df, source),
            }
        )
    return pd.DataFrame(rows, columns=["source", "normalized_count", "valid_count", "episode_count"])


def build_counts_table(
    raw_audit_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    root_dir: Path | None = None,
) -> pd.DataFrame:
    """Build deterministic top-line pipeline counts for the final report."""
    stage_counts = build_pipeline_stage_counts(
        raw_audit_df=raw_audit_df,
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        root_dir=root_dir,
    )
    rows = [
        _count_row(
            metric=str(row["metric"]),
            count=int(row["count"]),
            denominator_type=str(row["denominator_type"]),
            denominator_value=int(row["denominator_value"]),
            definition=str(row["definition"]),
        )
        for row in build_pipeline_stage_rows(stage_counts)
    ]
    return pd.DataFrame(rows)


def build_final_source_distribution(
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    root_dir: Path | None = None,
) -> pd.DataFrame:
    """Build final source distribution with labeled-share percentages."""
    source_df = build_source_summary(normalized_df, valid_df, episodes_df)
    raw_counts_df = count_raw_jsonl_by_source(root_dir) if root_dir is not None else pd.DataFrame()
    prefiltered_df = pd.DataFrame()
    if root_dir is not None:
        from src.utils.io import read_parquet

        prefiltered_df = read_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    episode_source_df = (
        episodes_df[["episode_id", "source"]].drop_duplicates(subset=["episode_id"], keep="first")
        if not episodes_df.empty and "episode_id" in episodes_df.columns and "source" in episodes_df.columns
        else pd.DataFrame(columns=["episode_id", "source"])
    )
    labeled_with_source = (
        labeled_df[["episode_id"]].merge(episode_source_df, on="episode_id", how="left")
        if not labeled_df.empty and "episode_id" in labeled_df.columns
        else pd.DataFrame(columns=["source"])
    )
    sources = sorted(
        set(raw_counts_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name).tolist())
        | set(source_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name).tolist())
        | set(labeled_with_source.get("source", pd.Series(dtype=str)).dropna().astype(str).map(canonical_source_name).tolist())
    )
    total_labeled = int(len(labeled_with_source))
    rows: list[dict[str, object]] = []
    for source in sources:
        rows.append(
            {
                "source": source,
                "raw_count": aggregated_source_count(raw_counts_df, source, "raw_count"),
                "normalized_count": source_row_count(normalized_df, source),
                "valid_count": source_row_count(valid_df, source),
                "prefiltered_valid_count": source_row_count(prefiltered_df, source),
                "episode_count": source_row_count(episodes_df, source),
                "labeled_count": source_row_count(labeled_with_source, source),
                "share_of_labeled": round_pct(source_row_count(labeled_with_source, source), total_labeled),
                "denominator_type": "labeled_episode_rows",
                "denominator_value": total_labeled,
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
                "metric_glossary_note": "See metric_glossary sheet for raw/normalized/valid/episode/labeled/core_labeled definitions.",
            }
        )
    return pd.DataFrame(rows)


def build_quality_checks_df(quality_checks: dict[str, object]) -> pd.DataFrame:
    """Convert evaluated quality metrics/statuses into workbook rows without re-evaluating policy."""
    thresholds = quality_display_thresholds()
    metric_status_map = {
        "persona_core_unknown_ratio": ("core_unknown_status", "core_unknown_reason_keys"),
        "overall_unknown_ratio": ("overall_unknown_status", "overall_unknown_reason_keys"),
        "persona_core_coverage_of_all_labeled_pct": ("core_coverage_status", "core_coverage_reason_keys"),
        "effective_balanced_source_count": ("effective_source_diversity_status", "effective_source_diversity_reason_keys"),
        "largest_labeled_source_share_pct": ("source_concentration_status", "source_concentration_reason_keys"),
        "largest_source_influence_share_pct": ("source_influence_concentration_status", "source_influence_concentration_reason_keys"),
        "weak_source_cost_center_count": ("weak_source_yield_status", "weak_source_yield_reason_keys"),
        "largest_cluster_share_of_core_labeled": ("largest_cluster_dominance_status", "largest_cluster_dominance_reason_keys"),
        "promoted_persona_example_coverage_pct": ("grounding_coverage_status", "grounding_coverage_reason_keys"),
        "promoted_persona_grounding_failure_count": ("grounding_coverage_status", "grounding_coverage_reason_keys"),
        "persona_readiness_state": ("persona_readiness_gate_status", "persona_readiness_blockers"),
        "persona_readiness_gate_status": ("persona_readiness_gate_status", "persona_readiness_blockers"),
        "overall_status": ("overall_status", "composite_reason_keys"),
        "core_clustering_status": ("core_clustering_status", "core_clustering_reason_keys"),
        "source_diversity_status": ("source_diversity_status", "source_diversity_reason_keys"),
        "source_influence_concentration_status": ("source_influence_concentration_status", "source_influence_concentration_reason_keys"),
        "weak_source_yield_status": ("weak_source_yield_status", "weak_source_yield_reason_keys"),
        "example_grounding_status": ("example_grounding_status", "example_grounding_reason_keys"),
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
                    "level": "info",
                    "denominator_type": "persona_core_labeled_rows",
                    "denominator_value": quality_checks.get("persona_core_labeled_rows", ""),
                    "notes": str(value)[:1000],
                }
            )
            continue
        threshold = thresholds.get(metric, "")
        status = "pass"
        level = "pass"
        notes = ""
        if metric in metric_status_map:
            status_key, reason_key = metric_status_map[metric]
            status, level = _quality_row_style(str(quality_checks.get(status_key, QUALITY_FLAG_OK) or QUALITY_FLAG_OK))
            notes = str(quality_checks.get(reason_key, "") or "")
        elif metric == "quality_flag":
            status, level = _quality_row_style(str(quality_checks.get("overall_status", QUALITY_FLAG_OK) or QUALITY_FLAG_OK))
            notes = str(quality_checks.get("quality_flag_rule", "") or "")
        elif metric == "source_failures" and str(value).strip():
            status, level = "fail", "soft_fail"
            notes = str(quality_checks.get("source_diversity_reason_keys", "") or "")
        elif metric == "selected_example_grounding_issue_count":
            issue_count = int(value or 0)
            if issue_count > 0:
                status, level = "warn", "warning"
                notes = "example-level grounding issues among selected representative examples"
            else:
                status, level = "pass", "pass"
                notes = "no selected representative examples were flagged with weak or degraded grounding evidence"
        elif metric == "denominator_consistency":
            status = "pass"
            level = "pass"
            notes = "rendered from centralized evaluated status result"
        elif metric == "persona_readiness_label":
            status, level = _quality_row_style(str(quality_checks.get("persona_readiness_gate_status", QUALITY_FLAG_OK) or QUALITY_FLAG_OK))
            notes = str(quality_checks.get("persona_usage_restriction", "") or "")
        elif metric in {"persona_asset_class", "persona_usage_restriction", "persona_readiness_summary", "persona_readiness_blockers", "persona_readiness_rule"}:
            status = "info"
            level = "info"
        elif metric.endswith("_threshold_rule"):
            status = "info"
            level = "info"
        rows.append(
            {
                "metric": metric,
                "value": value,
                "threshold": threshold,
                "status": status,
                "level": level,
                "denominator_type": _quality_denominator_type(metric, quality_checks),
                "denominator_value": _quality_denominator_value(metric, quality_checks),
                "notes": notes,
            }
        )
    return pd.DataFrame(rows)


def _quality_row_style(status_value: str) -> tuple[str, str]:
    """Map evaluated status strings into workbook row status/level."""
    if status_value == STATUS_FAIL or status_value == QUALITY_FLAG_UNSTABLE:
        return "fail", "hard_fail"
    if status_value != QUALITY_FLAG_OK:
        return "warn", "warning"
    return "pass", "pass"


def append_source_survival_rows(quality_checks_df: pd.DataFrame, source_stage_counts_df: pd.DataFrame) -> pd.DataFrame:
    """Append source-level post/episode funnel checks and mixed-grain bridge metrics."""
    if source_stage_counts_df.empty:
        return quality_checks_df
    rows: list[dict[str, object]] = []
    for _, row in source_stage_counts_df.iterrows():
        source = str(row.get("source", "") or "")
        normalized_count = int(row.get("normalized_post_count", 0) or 0)
        valid_count = int(row.get("valid_post_count", 0) or 0)
        prefiltered_count = int(row.get("prefiltered_valid_post_count", 0) or 0)
        episode_count = int(row.get("episode_count", 0) or 0)
        labeled_count = int(row.get("labeled_episode_count", 0) or 0)
        labelable_count = int(row.get("labelable_episode_count", 0) or 0)
        rows.extend(
            [
                {
                    "metric": f"valid_posts_per_normalized_post_pct:{source}",
                    "value": round_pct(valid_count, normalized_count) if normalized_count else 0.0,
                    "threshold": "",
                    "status": "info",
                    "level": "info",
                    "denominator_type": "normalized_posts_rows",
                    "denominator_value": normalized_count,
                    "notes": f"valid_post_count={valid_count}",
                },
                {
                    "metric": f"prefiltered_valid_posts_per_valid_post_pct:{source}",
                    "value": round_pct(prefiltered_count, valid_count) if valid_count else 0.0,
                    "threshold": "",
                    "status": "info",
                    "level": "info",
                    "denominator_type": "valid_candidate_rows",
                    "denominator_value": valid_count,
                    "notes": f"prefiltered_valid_post_count={prefiltered_count}",
                },
                {
                    "metric": f"labeled_episodes_per_episode_pct:{source}",
                    "value": round_pct(labeled_count, episode_count) if episode_count else 0.0,
                    "threshold": "",
                    "status": "info",
                    "level": "info",
                    "denominator_type": "episode_rows",
                    "denominator_value": episode_count,
                    "notes": f"labeled_episode_count={labeled_count}",
                },
                {
                    "metric": f"labelable_episodes_per_labeled_episode_pct:{source}",
                    "value": round_pct(labelable_count, labeled_count) if labeled_count else 0.0,
                    "threshold": "",
                    "status": "info",
                    "level": "info",
                    "denominator_type": "labeled_episode_rows",
                    "denominator_value": labeled_count,
                    "notes": f"labelable_episode_count={labelable_count}",
                },
                {
                    "metric": f"episodes_per_prefiltered_valid_post:{source}",
                    "value": round(float(episode_count) / float(prefiltered_count), 2) if prefiltered_count else 0.0,
                    "threshold": "",
                    "status": "info",
                    "level": "info",
                    "denominator_type": "prefiltered_valid_rows",
                    "denominator_value": prefiltered_count,
                    "notes": f"episode_count={episode_count}; cross-grain bridge metric can exceed 1.0",
                },
                {
                    "metric": f"labeled_episodes_per_prefiltered_valid_post:{source}",
                    "value": round(float(labeled_count) / float(prefiltered_count), 2) if prefiltered_count else 0.0,
                    "threshold": "",
                    "status": "info",
                    "level": "info",
                    "denominator_type": "prefiltered_valid_rows",
                    "denominator_value": prefiltered_count,
                    "notes": f"labeled_episode_count={labeled_count}; cross-grain bridge metric can exceed 1.0",
                },
                {
                    "metric": f"labelable_episodes_per_prefiltered_valid_post:{source}",
                    "value": round(float(labelable_count) / float(prefiltered_count), 2) if prefiltered_count else 0.0,
                    "threshold": "",
                    "status": "info",
                    "level": "info",
                    "denominator_type": "prefiltered_valid_rows",
                    "denominator_value": prefiltered_count,
                    "notes": f"labelable_episode_count={labelable_count}; cross-grain bridge metric can exceed 1.0",
                },
            ]
        )
    survival_df = pd.DataFrame(rows)
    if quality_checks_df.empty:
        return survival_df
    return pd.concat([quality_checks_df, survival_df], ignore_index=True)


def build_quality_checks(
    raw_audit_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    cluster_profiles: list[dict[str, object]],
    root_dir: Path | None = None,
) -> dict[str, object]:
    """Build analysis quality checks for persona-report readiness."""
    stage_counts = build_pipeline_stage_counts(
        raw_audit_df=raw_audit_df,
        normalized_df=pd.DataFrame(),
        valid_df=valid_df,
        episodes_df=pd.DataFrame(),
        labeled_df=labeled_df,
        root_dir=root_dir,
    )
    return build_quality_metrics(
        stage_counts=stage_counts,
        labeled_df=labeled_df,
        source_stage_counts_df=pd.DataFrame(),
        cluster_stats_df=pd.DataFrame(),
        persona_examples_df=pd.DataFrame(),
        cluster_profiles=cluster_profiles,
    )


def _count_row(metric: str, count: int, denominator_type: str, denominator_value: int, definition: str) -> dict[str, object]:
    """Build one count row with its denominator definition."""
    return {
        "metric": metric,
        "count": count,
        "denominator_type": denominator_type,
        "denominator_value": denominator_value,
        "definition": definition,
    }


def _quality_denominator_type(metric: str, quality_checks: dict[str, object]) -> str:
    """Return denominator type for a quality metric."""
    if metric in PIPELINE_STAGE_METRIC_NAMES:
        return metric
    if metric in {
        "persona_core_unknown_ratio",
        "cluster_distribution",
        "cluster_count",
        "robust_cluster_count",
        "stable_cluster_count",
        "fragile_cluster_count",
        "micro_cluster_count",
        "thin_evidence_cluster_count",
        "structurally_supported_cluster_count",
        "weak_separation_cluster_count",
        "fragile_tail_cluster_count",
        "largest_cluster_share_of_core_labeled",
        "top_3_cluster_share_of_core_labeled",
        "avg_cluster_separation",
        "min_cluster_separation",
        "fragile_tail_share_of_core_labeled",
    }:
        return DENOMINATOR_PERSONA_CORE_LABELED_ROWS
    if metric in {
        "overall_unknown_ratio",
        DENOMINATOR_LABELED_EPISODE_ROWS,
        "persona_core_coverage_of_all_labeled_pct",
        "largest_labeled_source_share_pct",
    }:
        return DENOMINATOR_LABELED_EPISODE_ROWS
    if metric == "largest_promoted_source_share_pct":
        return "promoted_cluster_rows"
    if metric == "largest_grounded_source_share_pct":
        return "grounded_persona_rows"
    if metric == "promoted_persona_episode_rows":
        return "promoted_cluster_rows"
    if metric == "grounded_promoted_persona_episode_rows":
        return "grounded_persona_rows"
    if metric in {"effective_labeled_source_count", "effective_balanced_source_count", "weak_source_cost_center_count", "weak_source_cost_centers", "largest_source_influence_share_pct"}:
        return "source_count"
    if metric in {"promoted_persona_example_coverage_pct"}:
        return "promoted_persona_rows"
    if metric in {"promoted_candidate_persona_count", "promotion_visibility_persona_count", "headline_persona_count", "final_usable_persona_count", "deck_ready_persona_count", "promoted_persona_count", "promoted_persona_grounded_count", "promoted_persona_weakly_grounded_count", "promoted_persona_ungrounded_count", "promoted_persona_grounding_failure_count"}:
        return "persona_cluster_rows"
    if metric in {"selected_example_grounding_issue_count"}:
        return "persona_example_rows"
    if metric in {"raw_source_count"}:
        return DENOMINATOR_RAW_RECORD_ROWS
    if metric in {"labeled_source_count"}:
        return "source_count"
    if metric == "source_failures":
        return "raw_covered_source_count"
    if metric in {"persona_readiness_state", "persona_readiness_label", "persona_asset_class", "persona_readiness_gate_status", "persona_readiness_rule", "persona_readiness_blockers", "persona_readiness_summary", "persona_usage_restriction", "persona_completion_claim_allowed"}:
        return "explicit_metric_value"
    return str(quality_checks.get("denominator_type", "explicit_metric_value"))


def _quality_denominator_value(metric: str, quality_checks: dict[str, object]) -> object:
    """Return denominator value for a quality metric."""
    if metric in PIPELINE_STAGE_METRIC_NAMES:
        return quality_checks.get(metric, "")
    if metric in {"persona_core_unknown_ratio", "cluster_distribution", "cluster_count", "largest_cluster_share_of_core_labeled", "robust_cluster_count", "stable_cluster_count", "fragile_cluster_count", "micro_cluster_count", "thin_evidence_cluster_count", "structurally_supported_cluster_count", "weak_separation_cluster_count", "fragile_tail_cluster_count", "top_3_cluster_share_of_core_labeled", "avg_cluster_separation", "min_cluster_separation", "fragile_tail_share_of_core_labeled"}:
        return quality_checks.get("persona_core_labeled_rows", quality_checks.get("persona_core_labeled_count", ""))
    if metric in {
        "overall_unknown_ratio",
        DENOMINATOR_LABELED_EPISODE_ROWS,
        "persona_core_coverage_of_all_labeled_pct",
        "largest_labeled_source_share_pct",
    }:
        return quality_checks.get(DENOMINATOR_LABELED_EPISODE_ROWS, quality_checks.get("labeled_count", ""))
    if metric == "largest_promoted_source_share_pct":
        return quality_checks.get("promoted_persona_episode_rows", "")
    if metric == "largest_grounded_source_share_pct":
        return quality_checks.get("grounded_promoted_persona_episode_rows", "")
    if metric == "promoted_persona_episode_rows":
        return quality_checks.get("promoted_persona_episode_rows", "")
    if metric == "grounded_promoted_persona_episode_rows":
        return quality_checks.get("grounded_promoted_persona_episode_rows", "")
    if metric in {"effective_labeled_source_count", "effective_balanced_source_count", "weak_source_cost_center_count", "weak_source_cost_centers", "largest_source_influence_share_pct"}:
        return quality_checks.get("raw_source_count", "")
    if metric == "promoted_persona_example_coverage_pct":
        return quality_checks.get("promoted_persona_count", "")
    if metric == "promoted_candidate_persona_count":
        return quality_checks.get("cluster_count", "")
    if metric in {"promotion_visibility_persona_count", "headline_persona_count", "promoted_persona_count", "promoted_persona_grounded_count", "promoted_persona_weakly_grounded_count", "promoted_persona_ungrounded_count", "promoted_persona_grounding_failure_count", "final_usable_persona_count", "deck_ready_persona_count"}:
        return quality_checks.get("promoted_candidate_persona_count", quality_checks.get("cluster_count", ""))
    if metric == "selected_example_grounding_issue_count":
        return quality_checks.get("promoted_personas_with_examples", "")
    if metric == "raw_source_count":
        return quality_checks.get(DENOMINATOR_RAW_RECORD_ROWS, "")
    if metric in {"labeled_source_count"}:
        return quality_checks.get("raw_source_count", "")
    if metric == "source_failures":
        return quality_checks.get("raw_source_count", "")
    if metric in {"persona_readiness_state", "persona_readiness_label", "persona_asset_class", "persona_readiness_gate_status", "persona_readiness_rule", "persona_readiness_blockers", "persona_readiness_summary", "persona_usage_restriction", "persona_completion_claim_allowed"}:
        return ""
    return ""
