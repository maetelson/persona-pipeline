"""Shared analytics-stage orchestration with clear deterministic/export boundaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.axis_reduction import (
    apply_axis_reduction,
    build_axis_quality_audit,
    recommend_axis_reduction,
    write_axis_reduction_outputs,
)
from src.analysis.cluster import build_cluster_summary
from src.analysis.cooccurrence import build_code_edges, build_code_frequency_table
from src.analysis.clustering import build_code_clusters, cluster_summary_json
from src.analysis.persona import build_persona_candidates
from src.analysis.persona_axes import discover_persona_axes, write_persona_axis_outputs
from src.analysis.persona_gen import generate_personas
from src.analysis.persona_messaging import build_persona_messaging_outputs, write_persona_messaging_outputs
from src.analysis.persona_service import build_persona_outputs, write_persona_outputs
from src.analysis.quality_status import build_quality_metrics, evaluate_quality_status
from src.analysis.diagnostics import (
    build_metric_glossary,
    build_quality_failures,
    build_source_diagnostics,
    build_source_stage_counts,
    build_survival_funnel_by_source,
    finalize_quality_checks,
)
from src.analysis.pipeline_thresholds import evaluate_cluster_thresholds, load_threshold_profile, upsert_threshold_audit
from src.analysis.profiling import build_cluster_profiles
from src.analysis.report_export import export_persona_reports
from src.analysis.score import build_priority_scores
from src.analysis.stage_counts import build_pipeline_stage_counts
from src.analysis.summary import (
    build_counts_table,
    build_final_source_distribution,
    append_source_survival_rows,
    build_quality_checks_df,
    build_taxonomy_summary,
)
from src.analysis.workbook_bundle import assemble_workbook_frames, validate_workbook_frames, write_workbook_bundle
from src.exporters.xlsx_exporter import export_workbook_from_frames
from src.utils.io import ensure_dir, load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("analysis.stage_service")


def load_analysis_inputs(root_dir: Path) -> dict[str, Any]:
    """Load the analysis-stage inputs once for downstream deterministic processing."""
    scoring = load_yaml(root_dir / "config" / "scoring.yaml")
    profile, profile_cfg = load_threshold_profile(root_dir / "config" / "pipeline_thresholds.yaml")
    reduction_config = load_yaml(root_dir / "config" / "axis_reduction.yaml")
    return {
        "labeled_df": read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet"),
        "episodes_df": read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet"),
        "valid_df": read_parquet(root_dir / "data" / "valid" / "valid_candidates.parquet"),
        "normalized_df": read_parquet(root_dir / "data" / "normalized" / "normalized_posts.parquet"),
        "raw_audit_df": read_parquet(root_dir / "data" / "analysis" / "raw_audit.parquet"),
        "scoring": scoring,
        "threshold_profile": profile,
        "threshold_profile_cfg": profile_cfg,
        "axis_reduction_config": reduction_config,
    }


def build_deterministic_analysis_outputs(root_dir: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    """Build deterministic analytics outputs without final export side effects."""
    labeled_df = inputs["labeled_df"]
    episodes_df = inputs["episodes_df"]
    valid_df = inputs["valid_df"]
    normalized_df = inputs["normalized_df"]
    raw_audit_df = inputs["raw_audit_df"]
    clustering_labeled_df = _persona_core_subset(labeled_df)
    clustering_episode_ids = set(clustering_labeled_df.get("episode_id", pd.Series(dtype=str)).astype(str).tolist())
    clustering_episodes_df = episodes_df[episodes_df["episode_id"].astype(str).isin(clustering_episode_ids)].reset_index(drop=True)

    cluster_threshold_df, cluster_meta = evaluate_cluster_thresholds(
        clustering_labeled_df,
        inputs["threshold_profile"],
        inputs["threshold_profile_cfg"],
    )
    combined_threshold_df = upsert_threshold_audit(root_dir, cluster_threshold_df)

    priority_scores_df = build_priority_scores(labeled_df, inputs["scoring"])
    if cluster_meta["cluster_allowed"]:
        cluster_summary_df = build_cluster_summary(clustering_labeled_df)
        persona_candidates_df = build_persona_candidates(clustering_labeled_df, priority_scores_df[priority_scores_df["episode_id"].astype(str).isin(clustering_episode_ids)].reset_index(drop=True))
    else:
        cluster_summary_df = build_cluster_summary(clustering_labeled_df.iloc[0:0].copy())
        persona_candidates_df = build_persona_candidates(
            clustering_labeled_df.iloc[0:0].copy(),
            priority_scores_df.iloc[0:0].copy(),
        )
    cluster_summary_df = _annotate_analysis_df(cluster_summary_df, cluster_meta)
    persona_candidates_df = _annotate_analysis_df(persona_candidates_df, cluster_meta)
    priority_scores_df = _annotate_analysis_df(priority_scores_df, cluster_meta)

    code_freq_df = build_code_frequency_table(clustering_labeled_df)
    code_edges_df = build_code_edges(clustering_labeled_df, code_freq_df, min_pair_count=5, normalization="count")
    clusters_df, cluster_summary_rows = build_code_clusters(code_freq_df, code_edges_df)

    cluster_profiles = build_cluster_profiles(
        episodes_df=clustering_episodes_df,
        labeled_df=clustering_labeled_df,
        clusters_df=clusters_df,
        cluster_summary_rows=cluster_summary_rows,
        priority_df=priority_scores_df,
    )

    axis_candidates_df, final_axis_schema, implementation_note = discover_persona_axes(
        episodes_df=episodes_df,
        labeled_df=clustering_labeled_df,
    )
    audit_outputs = build_axis_quality_audit(
        episodes_df=clustering_episodes_df,
        labeled_df=clustering_labeled_df,
        candidate_df=axis_candidates_df,
        current_axis_schema=final_axis_schema,
        config=inputs["axis_reduction_config"],
    )
    recommendations_df = recommend_axis_reduction(audit_outputs["audit_df"], inputs["axis_reduction_config"])
    reduced_outputs = apply_axis_reduction(
        axis_wide_df=audit_outputs["axis_wide_df"],
        axis_long_df=audit_outputs["axis_long_df"],
        audit_df=audit_outputs["audit_df"],
        recommendations_df=recommendations_df,
        candidate_df=axis_candidates_df,
        current_axis_schema=final_axis_schema,
        config=inputs["axis_reduction_config"],
    )
    axis_paths = write_persona_axis_outputs(
        root_dir=root_dir,
        axis_candidates_df=axis_candidates_df,
        final_axis_schema=reduced_outputs["reduced_axis_schema"],
        implementation_note=implementation_note,
    )
    reduction_paths = write_axis_reduction_outputs(
        root_dir=root_dir,
        audit_df=audit_outputs["audit_df"],
        recommendations_df=recommendations_df,
        reduced_outputs=reduced_outputs,
        apply_changes=True,
    )

    persona_service_outputs = build_persona_outputs(
        episodes_df=clustering_episodes_df,
        labeled_df=clustering_labeled_df,
        final_axis_schema=reduced_outputs["reduced_axis_schema"],
        quality_checks={
            "labeled_episode_rows": int(len(labeled_df)),
            "persona_core_labeled_rows": int(len(clustering_labeled_df)),
        },
    )
    bottleneck_cluster_profiles = persona_service_outputs.get("cluster_profiles", [])
    stage_counts = build_pipeline_stage_counts(
        raw_audit_df=raw_audit_df,
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        root_dir=root_dir,
    )
    source_stage_counts_df = build_source_stage_counts(
        root_dir=root_dir,
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        persona_assignments_df=persona_service_outputs["persona_assignments_df"],
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
    )
    source_diagnostics_df = build_source_diagnostics(source_stage_counts_df)
    quality_metrics = build_quality_metrics(
        stage_counts=stage_counts,
        labeled_df=labeled_df,
        source_stage_counts_df=source_stage_counts_df,
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
        persona_examples_df=persona_service_outputs["persona_examples_df"],
        cluster_profiles=bottleneck_cluster_profiles,
    )
    evaluated_quality = evaluate_quality_status(quality_metrics)
    quality_checks = finalize_quality_checks(evaluated_quality)
    persona_service_outputs["overview_df"] = _build_final_overview_df(
        axis_names=reduced_outputs["reduced_axis_schema"],
        quality_checks=quality_checks,
        stage_counts=stage_counts,
        persona_core_labeled_rows=int(len(clustering_labeled_df)),
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
    )
    survival_funnel_df = build_survival_funnel_by_source(source_stage_counts_df)
    persona_service_outputs["quality_checks_df"] = append_source_survival_rows(
        build_quality_checks_df(quality_checks),
        source_stage_counts_df,
    )
    quality_failures_df = build_quality_failures(
        quality_checks=quality_checks,
        source_stage_counts_df=source_stage_counts_df,
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
        persona_examples_df=persona_service_outputs["persona_examples_df"],
    )
    metric_glossary_df = build_metric_glossary()

    counts_df = build_counts_table(
        raw_audit_df=raw_audit_df,
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        root_dir=root_dir,
    )
    source_distribution_df = build_final_source_distribution(
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        root_dir=root_dir,
    )
    taxonomy_summary_df = build_taxonomy_summary(reduced_outputs["reduced_axis_schema"])
    workbook_frames = assemble_workbook_frames(
        overview_df=persona_service_outputs["overview_df"],
        counts_df=counts_df,
        source_distribution_df=source_distribution_df,
        taxonomy_summary_df=taxonomy_summary_df,
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
        persona_summary_df=persona_service_outputs["persona_summary_df"],
        persona_axes_df=persona_service_outputs["persona_axes_df"],
        persona_needs_df=persona_service_outputs["persona_pains_df"],
        persona_cooccurrence_df=persona_service_outputs["persona_cooccurrence_df"],
        persona_examples_df=persona_service_outputs["persona_examples_df"],
        quality_checks_df=persona_service_outputs["quality_checks_df"],
        source_diagnostics_df=source_diagnostics_df,
        quality_failures_df=quality_failures_df,
        metric_glossary_df=metric_glossary_df,
    )
    return {
        "cluster_meta": cluster_meta,
        "combined_threshold_df": combined_threshold_df,
        "cluster_summary_df": cluster_summary_df,
        "persona_candidates_df": persona_candidates_df,
        "priority_scores_df": priority_scores_df,
        "code_freq_df": code_freq_df,
        "code_edges_df": code_edges_df,
        "clusters_df": clusters_df,
        "cluster_summary_rows": cluster_summary_rows,
        "cluster_profiles": cluster_profiles,
        "axis_candidates_df": axis_candidates_df,
        "reduced_outputs": reduced_outputs,
        "axis_paths": axis_paths,
        "reduction_paths": reduction_paths,
        "quality_checks": quality_checks,
        "evaluated_quality": evaluated_quality,
        "persona_service_outputs": persona_service_outputs,
        "bottleneck_cluster_profiles": bottleneck_cluster_profiles,
        "counts_df": counts_df,
        "source_distribution_df": source_distribution_df,
        "taxonomy_summary_df": taxonomy_summary_df,
        "source_stage_counts_df": source_stage_counts_df,
        "source_diagnostics_df": source_diagnostics_df,
        "survival_funnel_df": survival_funnel_df,
        "quality_failures_df": quality_failures_df,
        "metric_glossary_df": metric_glossary_df,
        "workbook_frames": workbook_frames,
        "clustering_labeled_df": clustering_labeled_df,
        "clustering_episodes_df": clustering_episodes_df,
    }


def build_optional_persona_outputs(deterministic_outputs: dict[str, Any]) -> dict[str, Any]:
    """Build optional persona messaging and report outputs on top of deterministic analysis."""
    personas, persona_audit = generate_personas(deterministic_outputs["bottleneck_cluster_profiles"])
    messaging_outputs = build_persona_messaging_outputs(
        cluster_audit_df=deterministic_outputs["persona_service_outputs"]["cluster_meaning_audit_df"],
        naming_df=deterministic_outputs["persona_service_outputs"]["cluster_naming_recommendations_df"],
        persona_summary_df=deterministic_outputs["persona_service_outputs"]["persona_summary_df"],
        examples_df=deterministic_outputs["persona_service_outputs"]["representative_examples_v2_df"],
        personas=personas,
    )
    return {
        "personas": personas,
        "persona_audit": persona_audit,
        "messaging_outputs": messaging_outputs,
    }


def persist_analysis_outputs(
    root_dir: Path,
    deterministic_outputs: dict[str, Any],
    optional_outputs: dict[str, Any] | None = None,
    write_debug_artifacts: bool = True,
) -> dict[str, Any]:
    """Persist deterministic analysis outputs and optional report/debug artifacts."""
    analysis_dir = ensure_dir(root_dir / "data" / "analysis")
    cluster_meta = deterministic_outputs["cluster_meta"]
    persona_service_outputs = deterministic_outputs["persona_service_outputs"]

    write_parquet(deterministic_outputs["cluster_summary_df"], analysis_dir / "cluster_summary.parquet")
    write_parquet(deterministic_outputs["persona_candidates_df"], analysis_dir / "persona_candidates.parquet")
    write_parquet(deterministic_outputs["priority_scores_df"], analysis_dir / "priority_scores.parquet")
    write_parquet(deterministic_outputs["priority_scores_df"], analysis_dir / "priority_matrix.parquet")

    deterministic_outputs["code_freq_df"].to_csv(analysis_dir / "code_freq.csv", index=False)
    deterministic_outputs["code_edges_df"].to_csv(analysis_dir / "code_edges.csv", index=False)
    deterministic_outputs["clusters_df"].to_csv(analysis_dir / "clusters.csv", index=False)
    (analysis_dir / "cluster_summary.json").write_text(
        cluster_summary_json(deterministic_outputs["cluster_summary_rows"]),
        encoding="utf-8",
    )
    (analysis_dir / "cluster_profiles.json").write_text(
        json.dumps(deterministic_outputs["cluster_profiles"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (analysis_dir / "cluster_profiles_bottleneck.json").write_text(
        json.dumps(deterministic_outputs["bottleneck_cluster_profiles"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    for message in validate_workbook_frames(deterministic_outputs["workbook_frames"]):
        LOGGER.warning("Workbook bundle validation: %s", message)
    bundle_paths = write_workbook_bundle(root_dir, deterministic_outputs["workbook_frames"])

    debug_paths: dict[str, Path] = {}
    export_paths: dict[str, Path] = {}
    messaging_paths: dict[str, Path] = {}

    if optional_outputs:
        (analysis_dir / "persona_generation_audit.json").write_text(
            json.dumps(optional_outputs["persona_audit"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if write_debug_artifacts:
        debug_paths = write_persona_outputs(root_dir, persona_service_outputs)
        deterministic_outputs["counts_df"].to_csv(analysis_dir / "counts.csv", index=False)
        deterministic_outputs["source_distribution_df"].to_csv(analysis_dir / "source_distribution.csv", index=False)
        deterministic_outputs["taxonomy_summary_df"].to_csv(analysis_dir / "taxonomy_summary.csv", index=False)
        deterministic_outputs["source_diagnostics_df"].to_csv(analysis_dir / "source_diagnostics.csv", index=False)
        deterministic_outputs["survival_funnel_df"].to_csv(analysis_dir / "survival_funnel_by_source.csv", index=False)
        deterministic_outputs["quality_failures_df"].to_csv(analysis_dir / "quality_failures.csv", index=False)
        deterministic_outputs["metric_glossary_df"].to_csv(analysis_dir / "metric_glossary.csv", index=False)
        write_parquet(deterministic_outputs["survival_funnel_df"], analysis_dir / "survival_funnel_by_source.parquet")
        debug_paths.update(
            {
                "counts_csv": analysis_dir / "counts.csv",
                "source_distribution_csv": analysis_dir / "source_distribution.csv",
                "taxonomy_summary_csv": analysis_dir / "taxonomy_summary.csv",
                "source_diagnostics_csv": analysis_dir / "source_diagnostics.csv",
                "survival_funnel_csv": analysis_dir / "survival_funnel_by_source.csv",
                "survival_funnel_parquet": analysis_dir / "survival_funnel_by_source.parquet",
                "quality_failures_csv": analysis_dir / "quality_failures.csv",
                "metric_glossary_csv": analysis_dir / "metric_glossary.csv",
            }
        )
        if optional_outputs:
            export_paths = export_persona_reports(
                root_dir=root_dir,
                personas=optional_outputs["personas"],
                cluster_profiles=deterministic_outputs["bottleneck_cluster_profiles"],
                cluster_summary_rows=deterministic_outputs["bottleneck_cluster_profiles"],
                quality_checks=deterministic_outputs["quality_checks"],
            )
            messaging_paths = write_persona_messaging_outputs(root_dir, optional_outputs["messaging_outputs"])

    return {
        "bundle_paths": bundle_paths,
        "debug_paths": debug_paths,
        "export_paths": export_paths,
        "messaging_paths": messaging_paths,
        "cluster_meta": cluster_meta,
        "axis_paths": deterministic_outputs["axis_paths"],
        "reduction_paths": deterministic_outputs["reduction_paths"],
        "quality_flag": deterministic_outputs["quality_checks"]["quality_flag"],
        "promotion_visibility_persona_count": int(deterministic_outputs["quality_checks"].get("promotion_visibility_persona_count", 0) or 0),
        "final_usable_persona_count": int(deterministic_outputs["quality_checks"].get("final_usable_persona_count", 0) or 0),
        "code_cluster_count": len(deterministic_outputs["cluster_summary_rows"]),
        "service_axis_count": len(deterministic_outputs["reduced_outputs"]["reduced_axis_schema"]),
        "generated_persona_count": len(optional_outputs["personas"]) if optional_outputs else 0,
    }


def run_analysis_stage(root_dir: Path, write_debug_artifacts: bool = True) -> dict[str, Any]:
    """Run analysis stage with one shared in-memory handoff from analytics to persistence."""
    inputs = load_analysis_inputs(root_dir)
    deterministic_outputs = build_deterministic_analysis_outputs(root_dir, inputs)
    optional_outputs = build_optional_persona_outputs(deterministic_outputs) if write_debug_artifacts else None
    persisted = persist_analysis_outputs(
        root_dir=root_dir,
        deterministic_outputs=deterministic_outputs,
        optional_outputs=optional_outputs,
        write_debug_artifacts=write_debug_artifacts,
    )
    return {
        "inputs": inputs,
        "deterministic_outputs": deterministic_outputs,
        "optional_outputs": optional_outputs,
        "persisted": persisted,
    }


def run_final_report_stage(root_dir: Path, write_debug_artifacts: bool = True) -> dict[str, Any]:
    """Run analytics and write the final workbook through the single export path."""
    outputs = run_analysis_stage(root_dir, write_debug_artifacts=write_debug_artifacts)
    workbook_path = export_workbook_from_frames(
        root_dir=root_dir,
        frames=outputs["deterministic_outputs"]["workbook_frames"],
    )
    outputs["final_workbook_path"] = workbook_path
    return outputs


def _annotate_analysis_df(df: pd.DataFrame, cluster_meta: dict[str, Any]) -> pd.DataFrame:
    """Attach threshold interpretation flags to exploratory analysis outputs."""
    result = df.copy()
    for column, value in {
        "threshold_profile": cluster_meta["profile"],
        "exploratory_only": cluster_meta["exploratory_only"],
        "cluster_reliability": cluster_meta["cluster_reliability"],
        "threshold_reason": cluster_meta["reason"],
    }.items():
        result[column] = value
    if result.empty:
        return pd.DataFrame(result)
    return result


def _persona_core_subset(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Exclude low-signal rows from persona-core clustering when flagged."""
    if labeled_df.empty or "persona_core_eligible" not in labeled_df.columns:
        return labeled_df.copy()
    return labeled_df[labeled_df["persona_core_eligible"].fillna(True)].reset_index(drop=True)


def _build_final_overview_df(
    axis_names: list[dict[str, Any]],
    quality_checks: dict[str, Any],
    stage_counts: dict[str, int],
    persona_core_labeled_rows: int,
    cluster_stats_df: pd.DataFrame,
) -> pd.DataFrame:
    """Render overview directly from the evaluated quality result and stable report counts."""
    promoted_mask = cluster_stats_df.get("promotion_status", pd.Series(dtype=str)).astype(str).eq("promoted_persona") if not cluster_stats_df.empty else pd.Series(dtype=bool)
    promotion_grounding_status = cluster_stats_df.get("promotion_grounding_status", pd.Series(dtype=str)).astype(str) if not cluster_stats_df.empty else pd.Series(dtype=str)
    promotion_visibility_persona_count = int(quality_checks.get("promotion_visibility_persona_count", int(promoted_mask.sum()) if not cluster_stats_df.empty else 0) or 0)
    final_usable_persona_count = int(quality_checks.get("final_usable_persona_count", int(promotion_grounding_status.eq("promoted_and_grounded").sum()) if not cluster_stats_df.empty else 0) or 0)
    exploratory_bucket_count = int((~promoted_mask).sum()) if not cluster_stats_df.empty else 0
    selected_axes = " | ".join(
        str(row.get("axis_name", "")).strip()
        for row in axis_names
        if str(row.get("axis_name", "")).strip()
    )
    rows = [
        {"metric": "overall_status", "value": quality_checks.get("overall_status", "")},
        {"metric": "quality_flag", "value": quality_checks.get("quality_flag", "")},
        {"metric": "quality_flag_rule", "value": quality_checks.get("quality_flag_rule", "")},
        {"metric": "composite_reason_keys", "value": quality_checks.get("composite_reason_keys", "")},
        {"metric": "core_clustering_status", "value": quality_checks.get("core_clustering_status", "")},
        {"metric": "source_diversity_status", "value": quality_checks.get("source_diversity_status", "")},
        {"metric": "example_grounding_status", "value": quality_checks.get("example_grounding_status", "")},
        {"metric": "overall_unknown_status", "value": quality_checks.get("overall_unknown_status", "")},
        {"metric": "core_unknown_status", "value": quality_checks.get("core_unknown_status", "")},
        {"metric": "core_coverage_status", "value": quality_checks.get("core_coverage_status", "")},
        {"metric": "effective_source_diversity_status", "value": quality_checks.get("effective_source_diversity_status", "")},
        {"metric": "source_concentration_status", "value": quality_checks.get("source_concentration_status", "")},
        {"metric": "largest_cluster_dominance_status", "value": quality_checks.get("largest_cluster_dominance_status", "")},
        {"metric": "grounding_coverage_status", "value": quality_checks.get("grounding_coverage_status", "")},
        *[{"metric": metric, "value": int(stage_counts.get(metric, 0) or 0)} for metric in stage_counts],
        {"metric": "persona_core_labeled_rows", "value": persona_core_labeled_rows},
        {"metric": "persona_core_coverage_of_all_labeled_pct", "value": quality_checks.get("persona_core_coverage_of_all_labeled_pct", 0.0)},
        {"metric": "persona_core_unknown_ratio", "value": quality_checks.get("persona_core_unknown_ratio", 0.0)},
        {"metric": "overall_unknown_ratio", "value": quality_checks.get("overall_unknown_ratio", 0.0)},
        {"metric": "effective_labeled_source_count", "value": quality_checks.get("effective_labeled_source_count", 0.0)},
        {"metric": "largest_cluster_share_of_core_labeled", "value": quality_checks.get("largest_cluster_share_of_core_labeled", 0.0)},
        {"metric": "largest_labeled_source_share_pct", "value": quality_checks.get("largest_labeled_source_share_pct", 0.0)},
        {"metric": "promoted_candidate_persona_count", "value": quality_checks.get("promoted_candidate_persona_count", promotion_visibility_persona_count)},
        {"metric": "promotion_visibility_persona_count", "value": promotion_visibility_persona_count},
        {"metric": "final_usable_persona_count", "value": final_usable_persona_count},
        {"metric": "deck_ready_persona_count", "value": quality_checks.get("deck_ready_persona_count", final_usable_persona_count)},
        {"metric": "promoted_persona_example_coverage_pct", "value": quality_checks.get("promoted_persona_example_coverage_pct", 0.0)},
        {"metric": "promoted_persona_grounded_count", "value": quality_checks.get("promoted_persona_grounded_count", 0)},
        {"metric": "promoted_persona_weakly_grounded_count", "value": quality_checks.get("promoted_persona_weakly_grounded_count", 0)},
        {"metric": "promoted_persona_ungrounded_count", "value": quality_checks.get("promoted_persona_ungrounded_count", 0)},
        {"metric": "promoted_personas_weakly_grounded", "value": quality_checks.get("promoted_personas_weakly_grounded", "")},
        {"metric": "promoted_personas_missing_examples", "value": quality_checks.get("promoted_personas_missing_examples", "")},
        {"metric": "exploratory_bucket_count", "value": exploratory_bucket_count},
        {"metric": "min_cluster_size", "value": quality_checks.get("min_cluster_size", 0)},
        {"metric": "selected_axes", "value": selected_axes},
        {"metric": "clustering_mode", "value": "bottleneck_first"},
    ]
    return pd.DataFrame(rows)
