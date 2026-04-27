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
from src.analysis.persona_axes import build_persona_core_flags, discover_persona_axes, write_persona_axis_outputs
from src.analysis.persona_gen import generate_personas
from src.analysis.persona_messaging import build_persona_messaging_outputs, write_persona_messaging_outputs
from src.analysis.persona05_boundary_diagnostics import (
    build_persona05_boundary_outputs,
    write_persona05_boundary_artifacts,
)
from src.analysis.persona05_subtheme_preservation import (
    build_persona05_subtheme_outputs,
    write_persona05_subtheme_artifacts,
)
from src.analysis.persona_service import _review_ready_fields, build_persona_outputs, write_persona_outputs
from src.analysis.quality_status import build_quality_metrics, evaluate_quality_status
from src.analysis.reddit_retention import analyze_reddit_retention
from src.analysis.diagnostics import (
    build_metric_glossary,
    build_quality_failures,
    build_source_balance_audit,
    build_source_diagnostics,
    build_source_stage_counts,
    build_survival_funnel_by_source,
    build_weak_source_triage,
    finalize_quality_checks,
)
from src.analysis.deck_ready_claims import build_deck_ready_claim_outputs
from src.analysis.release_visibility import build_release_visibility_outputs
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
from src.analysis.source_tiers import source_tier_counts
from src.analysis.source_tier_evidence import (
    build_source_tier_evidence_outputs,
    write_source_tier_evidence_artifacts,
)
from src.analysis.workbook_bundle import assemble_workbook_frames, validate_workbook_frames, write_workbook_bundle
from src.exporters.xlsx_exporter import export_workbook_from_frames
from src.labeling.unknown_reasons import build_unknown_reason_breakdown
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
        "label_details_df": read_parquet(root_dir / "data" / "labeled" / "label_details.parquet")
        if (root_dir / "data" / "labeled" / "label_details.parquet").exists()
        else pd.DataFrame(),
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
    labeled_df = inputs["labeled_df"].copy()
    label_details_df = inputs.get("label_details_df", pd.DataFrame())
    episodes_df = inputs["episodes_df"]
    valid_df = inputs["valid_df"]
    normalized_df = inputs["normalized_df"]
    raw_audit_df = inputs["raw_audit_df"]

    priority_scores_df = build_priority_scores(labeled_df, inputs["scoring"])

    axis_candidates_df, final_axis_schema, implementation_note = discover_persona_axes(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
    )
    audit_outputs = build_axis_quality_audit(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
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

    unknown_rows_df = pd.DataFrame()
    if not label_details_df.empty:
        _, unknown_rows_df = build_unknown_reason_breakdown(episodes_df, labeled_df, label_details_df)
    labeled_df, persona_core_policy_df = build_persona_core_flags(
        labeled_df=labeled_df,
        axis_wide_df=reduced_outputs["reduced_axis_wide_df"],
        final_axis_schema=reduced_outputs["reduced_axis_schema"],
        unknown_rows_df=unknown_rows_df,
    )
    clustering_labeled_df = _persona_core_subset(labeled_df)
    clustering_episode_ids = set(clustering_labeled_df.get("episode_id", pd.Series(dtype=str)).astype(str).tolist())
    clustering_episodes_df = episodes_df[episodes_df["episode_id"].astype(str).isin(clustering_episode_ids)].reset_index(drop=True)

    cluster_threshold_df, cluster_meta = evaluate_cluster_thresholds(
        clustering_labeled_df,
        inputs["threshold_profile"],
        inputs["threshold_profile_cfg"],
    )
    combined_threshold_df = upsert_threshold_audit(root_dir, cluster_threshold_df)

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
    initial_source_stage_counts_df = build_source_stage_counts(
        root_dir=root_dir,
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        persona_assignments_df=persona_service_outputs["persona_assignments_df"],
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
    )
    initial_source_balance_audit_df = build_source_balance_audit(initial_source_stage_counts_df)
    persona_service_outputs, promotion_constraint_summary = _apply_workbook_promotion_constraints(
        persona_service_outputs=persona_service_outputs,
        clustering_episodes_df=clustering_episodes_df,
        source_balance_audit_df=initial_source_balance_audit_df,
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
    source_balance_audit_df = build_source_balance_audit(source_stage_counts_df)
    weak_source_triage_df = build_weak_source_triage(source_balance_audit_df)
    quality_metrics = build_quality_metrics(
        stage_counts=stage_counts,
        labeled_df=labeled_df,
        source_stage_counts_df=source_stage_counts_df,
        source_balance_audit_df=source_balance_audit_df,
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
        persona_examples_df=persona_service_outputs["persona_examples_df"],
        cluster_profiles=bottleneck_cluster_profiles,
        cluster_robustness_summary_df=persona_service_outputs.get("cluster_robustness_summary_df"),
    )
    evaluated_quality = evaluate_quality_status(quality_metrics)
    quality_checks = finalize_quality_checks(evaluated_quality)
    quality_checks |= promotion_constraint_summary
    quality_checks["source_action_priority_summary"] = _source_action_priority_summary(source_balance_audit_df)
    quality_checks["fix_now_source_count"] = int(source_balance_audit_df.get("priority_tier", pd.Series(dtype=str)).astype(str).eq("fix_now").sum()) if "priority_tier" in source_balance_audit_df.columns else int((source_balance_audit_df.get("failure_level", pd.Series(dtype=str)).astype(str) == "failure").sum())
    quality_checks["tune_soon_source_count"] = int(source_balance_audit_df.get("priority_tier", pd.Series(dtype=str)).astype(str).eq("tune_soon").sum()) if "priority_tier" in source_balance_audit_df.columns else int((source_balance_audit_df.get("failure_level", pd.Series(dtype=str)).astype(str) == "warning").sum())
    source_tier_evidence = build_source_tier_evidence_outputs(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        persona_assignments_df=persona_service_outputs["persona_assignments_df"],
        persona_summary_df=persona_service_outputs["persona_summary_df"],
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
    )
    quality_checks.update(source_tier_evidence["global_counts"])
    persona_service_outputs["persona_summary_df"] = source_tier_evidence["persona_summary_df"]
    persona_service_outputs["cluster_stats_df"] = source_tier_evidence["cluster_stats_df"]
    persona05_boundary_outputs = build_persona05_boundary_outputs(
        persona_assignments_df=persona_service_outputs["persona_assignments_df"],
        episodes_df=episodes_df,
        persona_summary_df=persona_service_outputs["persona_summary_df"],
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
    )
    persona_service_outputs["persona_summary_df"] = persona05_boundary_outputs["persona_summary_df"]
    persona_service_outputs["cluster_stats_df"] = persona05_boundary_outputs["cluster_stats_df"]
    deck_ready_claim_outputs = build_deck_ready_claim_outputs(
        persona_summary_df=persona_service_outputs["persona_summary_df"],
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
        persona_promotion_path_debug_df=persona_service_outputs["persona_promotion_path_debug_df"],
    )
    quality_checks.update(deck_ready_claim_outputs["counts"])
    persona_service_outputs["persona_summary_df"] = deck_ready_claim_outputs["persona_summary_df"]
    persona_service_outputs["cluster_stats_df"] = deck_ready_claim_outputs["cluster_stats_df"]
    persona_service_outputs["persona_promotion_path_debug_df"] = deck_ready_claim_outputs["persona_promotion_path_debug_df"]
    persona_service_outputs["cluster_stats_df"] = _annotate_persona_readiness_frame(
        persona_service_outputs["cluster_stats_df"],
        quality_checks,
    )
    persona_service_outputs["persona_summary_df"] = _annotate_persona_readiness_frame(
        persona_service_outputs["persona_summary_df"],
        quality_checks,
    )
    persona_service_outputs["persona_promotion_grounding_audit_df"] = _annotate_persona_readiness_frame(
        persona_service_outputs.get("persona_promotion_grounding_audit_df", pd.DataFrame()),
        quality_checks,
    )
    release_visibility_outputs = build_release_visibility_outputs(
        persona_summary_df=persona_service_outputs["persona_summary_df"],
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
        persona_promotion_path_debug_df=persona_service_outputs["persona_promotion_path_debug_df"],
    )
    quality_checks.update(release_visibility_outputs["counts"])
    persona_service_outputs["persona_summary_df"] = release_visibility_outputs["persona_summary_df"]
    persona_service_outputs["cluster_stats_df"] = release_visibility_outputs["cluster_stats_df"]
    persona_service_outputs["persona_promotion_path_debug_df"] = release_visibility_outputs["persona_promotion_path_debug_df"]
    persona_service_outputs["overview_df"] = _build_final_overview_df(
        axis_names=reduced_outputs["reduced_axis_schema"],
        quality_checks=quality_checks,
        stage_counts=stage_counts,
        cluster_stats_df=persona_service_outputs["cluster_stats_df"],
        source_balance_audit_df=source_balance_audit_df,
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
        "source_balance_audit_df": source_balance_audit_df,
        "weak_source_triage_df": weak_source_triage_df,
        "survival_funnel_df": survival_funnel_df,
        "quality_failures_df": quality_failures_df,
        "metric_glossary_df": metric_glossary_df,
        "workbook_frames": workbook_frames,
        "persona_core_policy_df": persona_core_policy_df,
        "clustering_labeled_df": clustering_labeled_df,
        "clustering_episodes_df": clustering_episodes_df,
        "source_tier_evidence_report": source_tier_evidence["report"],
        "persona_evidence_tier_breakdown_df": source_tier_evidence["persona_breakdown_df"],
        "persona05_boundary_outputs": persona05_boundary_outputs,
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
    write_debug_artifacts: bool = False,
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

    for message in validate_workbook_frames(
        deterministic_outputs["workbook_frames"],
        context_frames={
            "persona_promotion_path_debug": persona_service_outputs.get("persona_promotion_path_debug_df", pd.DataFrame()),
        },
    ):
        LOGGER.warning("Workbook bundle validation: %s", message)
    bundle_paths = write_workbook_bundle(root_dir, deterministic_outputs["workbook_frames"])
    persona_service_outputs["overview_df"].to_csv(analysis_dir / "overview.csv", index=False)
    persona_service_outputs["quality_checks_df"].to_csv(analysis_dir / "quality_checks.csv", index=False)
    persona_service_outputs["cluster_stats_df"].to_csv(analysis_dir / "cluster_stats.csv", index=False)
    persona_service_outputs["persona_summary_df"].to_csv(analysis_dir / "persona_summary.csv", index=False)
    persona_service_outputs["persona_promotion_path_debug_df"].to_csv(
        analysis_dir / "persona_promotion_path_debug.csv",
        index=False,
    )

    debug_paths: dict[str, Path] = {}
    export_paths: dict[str, Path] = {}
    messaging_paths: dict[str, Path] = {}
    debug_persona_outputs = dict(persona_service_outputs)

    if optional_outputs:
        (analysis_dir / "persona_generation_audit.json").write_text(
            json.dumps(optional_outputs["persona_audit"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if write_debug_artifacts:
        subtheme_outputs = build_persona05_subtheme_outputs(
            persona_summary_df=persona_service_outputs["persona_summary_df"],
            cluster_stats_df=persona_service_outputs["cluster_stats_df"],
            persona_promotion_path_debug_df=persona_service_outputs["persona_promotion_path_debug_df"],
        )
        debug_persona_outputs["persona_summary_df"] = subtheme_outputs["persona_summary_df"]
        debug_persona_outputs["cluster_stats_df"] = subtheme_outputs["cluster_stats_df"]
        debug_persona_outputs["persona_promotion_path_debug_df"] = subtheme_outputs["persona_promotion_path_debug_df"]
        debug_paths = write_persona_outputs(root_dir, debug_persona_outputs)
        debug_paths.update(
            write_source_tier_evidence_artifacts(
                root_dir,
                deterministic_outputs["source_tier_evidence_report"],
                deterministic_outputs["persona_evidence_tier_breakdown_df"],
            )
        )
        debug_paths.update(
            write_persona05_boundary_artifacts(
                root_dir,
                deterministic_outputs["persona05_boundary_outputs"]["diagnostic_df"],
                deterministic_outputs["persona05_boundary_outputs"]["summary"],
                deterministic_outputs["persona05_boundary_outputs"]["report"],
            )
        )
        debug_paths.update(write_persona05_subtheme_artifacts(root_dir, subtheme_outputs["report"]))
        deterministic_outputs["persona_core_policy_df"].to_csv(analysis_dir / "persona_core_policy_audit.csv", index=False)
        deterministic_outputs["counts_df"].to_csv(analysis_dir / "counts.csv", index=False)
        deterministic_outputs["source_distribution_df"].to_csv(analysis_dir / "source_distribution.csv", index=False)
        deterministic_outputs["taxonomy_summary_df"].to_csv(analysis_dir / "taxonomy_summary.csv", index=False)
        deterministic_outputs["source_diagnostics_df"].to_csv(analysis_dir / "source_diagnostics.csv", index=False)
        deterministic_outputs["source_balance_audit_df"].to_csv(analysis_dir / "source_balance_audit.csv", index=False)
        deterministic_outputs["weak_source_triage_df"].to_csv(analysis_dir / "weak_source_triage.csv", index=False)
        deterministic_outputs["survival_funnel_df"].to_csv(analysis_dir / "survival_funnel_by_source.csv", index=False)
        deterministic_outputs["quality_failures_df"].to_csv(analysis_dir / "quality_failures.csv", index=False)
        deterministic_outputs["metric_glossary_df"].to_csv(analysis_dir / "metric_glossary.csv", index=False)
        write_parquet(deterministic_outputs["survival_funnel_df"], analysis_dir / "survival_funnel_by_source.parquet")
        write_parquet(deterministic_outputs["weak_source_triage_df"], analysis_dir / "weak_source_triage.parquet")
        reddit_retention_paths = analyze_reddit_retention(root_dir)
        debug_paths.update(
            {
                "counts_csv": analysis_dir / "counts.csv",
                "persona_core_policy_audit_csv": analysis_dir / "persona_core_policy_audit.csv",
                "source_distribution_csv": analysis_dir / "source_distribution.csv",
                "taxonomy_summary_csv": analysis_dir / "taxonomy_summary.csv",
                "source_diagnostics_csv": analysis_dir / "source_diagnostics.csv",
                "source_balance_audit_csv": analysis_dir / "source_balance_audit.csv",
                "weak_source_triage_csv": analysis_dir / "weak_source_triage.csv",
                "weak_source_triage_parquet": analysis_dir / "weak_source_triage.parquet",
                "survival_funnel_csv": analysis_dir / "survival_funnel_by_source.csv",
                "survival_funnel_parquet": analysis_dir / "survival_funnel_by_source.parquet",
                "quality_failures_csv": analysis_dir / "quality_failures.csv",
                "metric_glossary_csv": analysis_dir / "metric_glossary.csv",
                **{f"reddit_retention_{name}": path for name, path in reddit_retention_paths.items()},
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


def run_analysis_stage(root_dir: Path, write_debug_artifacts: bool = False) -> dict[str, Any]:
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


def run_final_report_stage(root_dir: Path, write_debug_artifacts: bool = False) -> dict[str, Any]:
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


def _annotate_persona_readiness_frame(df: pd.DataFrame, quality_checks: dict[str, Any]) -> pd.DataFrame:
    """Stamp workbook-level readiness fields onto persona-facing sheets."""
    if df is None:
        return pd.DataFrame()
    result = df.copy()
    readiness_state = str(quality_checks.get("persona_readiness_state", "exploratory_only") or "exploratory_only")
    result["workbook_readiness_state"] = readiness_state
    result["workbook_readiness_gate_status"] = quality_checks.get("persona_readiness_gate_status", "FAIL")
    result["workbook_usage_restriction"] = quality_checks.get("persona_usage_restriction", "")
    if readiness_state not in {"deck_ready", "production_persona_ready"}:
        if "deck_ready_persona" in result.columns:
            result["deck_ready_persona"] = False
        if "deck_readiness_state" in result.columns:
            result["deck_readiness_state"] = readiness_state
        if "reporting_readiness_status" in result.columns:
            result["reporting_readiness_status"] = result["reporting_readiness_status"].astype(str).replace(
                {"deck_ready_persona": "reviewable_but_not_deck_ready"}
            )
    return result


def _build_final_overview_df(
    axis_names: list[dict[str, Any]],
    quality_checks: dict[str, Any],
    stage_counts: dict[str, int],
    cluster_stats_df: pd.DataFrame,
    source_balance_audit_df: pd.DataFrame | None = None,
    persona_core_labeled_rows: int | None = None,
) -> pd.DataFrame:
    """Render overview directly from the evaluated quality result and stable report counts."""
    if not cluster_stats_df.empty:
        workbook_review_visible = cluster_stats_df.get("workbook_review_visible", pd.Series(dtype=bool))
        if workbook_review_visible.empty:
            promoted_mask = cluster_stats_df.get("promotion_status", pd.Series(dtype=str)).astype(str).isin({"promoted_persona", "review_visible_persona"})
        else:
            promoted_mask = workbook_review_visible.fillna(False).astype(bool)
    else:
        promoted_mask = pd.Series(dtype=bool)
    promotion_grounding_status = cluster_stats_df.get("promotion_grounding_status", pd.Series(dtype=str)).astype(str) if not cluster_stats_df.empty else pd.Series(dtype=str)
    promotion_visibility_persona_count = int(quality_checks.get("promotion_visibility_persona_count", int(promoted_mask.sum()) if not cluster_stats_df.empty else 0) or 0)
    final_usable_persona_count = int(quality_checks.get("final_usable_persona_count", int(promotion_grounding_status.eq("promoted_and_grounded").sum()) if not cluster_stats_df.empty else 0) or 0)
    production_ready_persona = cluster_stats_df.get("production_ready_persona", pd.Series(dtype=bool)).fillna(False).astype(bool) if not cluster_stats_df.empty else pd.Series(dtype=bool)
    review_ready_persona = cluster_stats_df.get("review_ready_persona", pd.Series(dtype=bool)).fillna(False).astype(bool) if not cluster_stats_df.empty else pd.Series(dtype=bool)
    readiness_tier = cluster_stats_df.get("readiness_tier", pd.Series(dtype=str)).astype(str) if not cluster_stats_df.empty else pd.Series(dtype=str)
    production_ready_persona_count = int(production_ready_persona.sum()) if not production_ready_persona.empty else final_usable_persona_count
    review_ready_persona_count = int(review_ready_persona.sum()) if not review_ready_persona.empty else 0
    exploratory_bucket_count = int(readiness_tier.eq("exploratory_bucket").sum()) if not readiness_tier.empty else int((~promoted_mask).sum()) if not cluster_stats_df.empty else 0
    selected_axes = " | ".join(
        str(row.get("axis_name", "")).strip()
        for row in axis_names
        if str(row.get("axis_name", "")).strip()
    )
    tier_counts = source_tier_counts(source_balance_audit_df) if source_balance_audit_df is not None and not source_balance_audit_df.empty else {
        "core_representative_source_count": 0,
        "supporting_validation_source_count": 0,
        "exploratory_edge_source_count": 0,
        "excluded_from_deck_ready_core_source_count": 0,
    }
    rows = [
        {"metric": "persona_readiness_state", "value": quality_checks.get("persona_readiness_state", "exploratory_only")},
        {"metric": "persona_readiness_label", "value": quality_checks.get("persona_readiness_label", "Hypothesis Material")},
        {"metric": "persona_asset_class", "value": quality_checks.get("persona_asset_class", "hypothesis_material")},
        {"metric": "persona_readiness_gate_status", "value": quality_checks.get("persona_readiness_gate_status", "FAIL")},
        {"metric": "persona_completion_claim_allowed", "value": quality_checks.get("persona_completion_claim_allowed", False)},
        {"metric": "persona_usage_restriction", "value": quality_checks.get("persona_usage_restriction", "")},
        {"metric": "persona_readiness_summary", "value": quality_checks.get("persona_readiness_summary", "")},
        {"metric": "persona_readiness_blockers", "value": quality_checks.get("persona_readiness_blockers", "")},
        {"metric": "persona_readiness_rule", "value": quality_checks.get("persona_readiness_rule", "")},
        {"metric": "promotion_constraint_status", "value": quality_checks.get("promotion_constraint_status", "")},
        {"metric": "promotion_constraint_summary", "value": quality_checks.get("promotion_constraint_summary", "")},
        {"metric": "source_action_priority_summary", "value": quality_checks.get("source_action_priority_summary", "")},
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
        {"metric": "source_influence_concentration_status", "value": quality_checks.get("source_influence_concentration_status", "")},
        {"metric": "weak_source_yield_status", "value": quality_checks.get("weak_source_yield_status", "")},
        {"metric": "largest_cluster_dominance_status", "value": quality_checks.get("largest_cluster_dominance_status", "")},
        {"metric": "cluster_concentration_tail_status", "value": quality_checks.get("cluster_concentration_tail_status", "")},
        {"metric": "cluster_fragility_status", "value": quality_checks.get("cluster_fragility_status", "")},
        {"metric": "cluster_evidence_status", "value": quality_checks.get("cluster_evidence_status", "")},
        {"metric": "cluster_separation_status", "value": quality_checks.get("cluster_separation_status", "")},
        {"metric": "grounding_coverage_status", "value": quality_checks.get("grounding_coverage_status", "")},
        *[{"metric": metric, "value": int(stage_counts.get(metric, 0) or 0)} for metric in stage_counts],
        {"metric": "persona_core_labeled_rows", "value": int(quality_checks.get("persona_core_labeled_rows", 0) or 0)},
        {"metric": "persona_core_coverage_of_all_labeled_pct", "value": quality_checks.get("persona_core_coverage_of_all_labeled_pct", 0.0)},
        {"metric": "persona_core_unknown_ratio", "value": quality_checks.get("persona_core_unknown_ratio", 0.0)},
        {"metric": "overall_unknown_ratio", "value": quality_checks.get("overall_unknown_ratio", 0.0)},
        {"metric": "effective_labeled_source_count", "value": quality_checks.get("effective_labeled_source_count", 0.0)},
        {"metric": "effective_balanced_source_count", "value": quality_checks.get("effective_balanced_source_count", 0.0)},
        {"metric": "largest_cluster_share_of_core_labeled", "value": quality_checks.get("largest_cluster_share_of_core_labeled", 0.0)},
        {"metric": "top_3_cluster_share_of_core_labeled", "value": quality_checks.get("top_3_cluster_share_of_core_labeled", 0.0)},
        {"metric": "robust_cluster_count", "value": quality_checks.get("robust_cluster_count", 0)},
        {"metric": "stable_cluster_count", "value": quality_checks.get("stable_cluster_count", 0)},
        {"metric": "fragile_cluster_count", "value": quality_checks.get("fragile_cluster_count", 0)},
        {"metric": "micro_cluster_count", "value": quality_checks.get("micro_cluster_count", 0)},
        {"metric": "thin_evidence_cluster_count", "value": quality_checks.get("thin_evidence_cluster_count", 0)},
        {"metric": "structurally_supported_cluster_count", "value": quality_checks.get("structurally_supported_cluster_count", 0)},
        {"metric": "weak_separation_cluster_count", "value": quality_checks.get("weak_separation_cluster_count", 0)},
        {"metric": "fragile_tail_cluster_count", "value": quality_checks.get("fragile_tail_cluster_count", 0)},
        {"metric": "fragile_tail_share_of_core_labeled", "value": quality_checks.get("fragile_tail_share_of_core_labeled", 0.0)},
        {"metric": "avg_cluster_separation", "value": quality_checks.get("avg_cluster_separation", 0.0)},
        {"metric": "min_cluster_separation", "value": quality_checks.get("min_cluster_separation", 0.0)},
        {"metric": "largest_labeled_source_share_pct", "value": quality_checks.get("largest_labeled_source_share_pct", 0.0)},
        {"metric": "largest_promoted_source_share_pct", "value": quality_checks.get("largest_promoted_source_share_pct", 0.0)},
        {"metric": "largest_grounded_source_share_pct", "value": quality_checks.get("largest_grounded_source_share_pct", 0.0)},
        {"metric": "largest_source_influence_share_pct", "value": quality_checks.get("largest_source_influence_share_pct", 0.0)},
        {"metric": "weak_source_cost_center_count", "value": quality_checks.get("weak_source_cost_center_count", 0)},
        {"metric": "weak_source_cost_centers", "value": quality_checks.get("weak_source_cost_centers", "")},
        {"metric": "core_representative_source_count", "value": tier_counts["core_representative_source_count"]},
        {"metric": "supporting_validation_source_count", "value": tier_counts["supporting_validation_source_count"]},
        {"metric": "exploratory_edge_source_count", "value": tier_counts["exploratory_edge_source_count"]},
        {"metric": "excluded_from_deck_ready_core_source_count", "value": tier_counts["excluded_from_deck_ready_core_source_count"]},
        {"metric": "deck_ready_core_labeled_row_count", "value": quality_checks.get("deck_ready_core_labeled_row_count", 0)},
        {"metric": "deck_ready_core_persona_core_row_count", "value": quality_checks.get("deck_ready_core_persona_core_row_count", 0)},
        {"metric": "supporting_validation_labeled_row_count", "value": quality_checks.get("supporting_validation_labeled_row_count", 0)},
        {"metric": "supporting_validation_persona_core_row_count", "value": quality_checks.get("supporting_validation_persona_core_row_count", 0)},
        {"metric": "exploratory_edge_labeled_row_count", "value": quality_checks.get("exploratory_edge_labeled_row_count", 0)},
        {"metric": "exploratory_edge_persona_core_row_count", "value": quality_checks.get("exploratory_edge_persona_core_row_count", 0)},
        {"metric": "excluded_from_deck_ready_core_labeled_row_count", "value": quality_checks.get("excluded_from_deck_ready_core_labeled_row_count", 0)},
        {"metric": "excluded_from_deck_ready_core_persona_core_row_count", "value": quality_checks.get("excluded_from_deck_ready_core_persona_core_row_count", 0)},
        {"metric": "deck_ready_claim_eligible_persona_count", "value": quality_checks.get("deck_ready_claim_eligible_persona_count", 0)},
        {"metric": "fix_now_source_count", "value": quality_checks.get("fix_now_source_count", 0)},
        {"metric": "tune_soon_source_count", "value": quality_checks.get("tune_soon_source_count", 0)},
        {"metric": "promoted_candidate_persona_count", "value": quality_checks.get("promoted_candidate_persona_count", promotion_visibility_persona_count)},
        {"metric": "promotion_visibility_persona_count", "value": promotion_visibility_persona_count},
        {"metric": "headline_persona_count", "value": quality_checks.get("headline_persona_count", final_usable_persona_count)},
        {"metric": "final_usable_release_persona_count", "value": quality_checks.get("final_usable_release_persona_count", final_usable_persona_count)},
        {"metric": "review_ready_claim_persona_count", "value": quality_checks.get("review_ready_claim_persona_count", review_ready_persona_count)},
        {"metric": "future_candidate_subtheme_count", "value": quality_checks.get("future_candidate_subtheme_count", 0)},
        {"metric": "exploratory_tail_persona_count", "value": quality_checks.get("exploratory_tail_persona_count", exploratory_bucket_count)},
        {"metric": "release_headline_persona_count", "value": quality_checks.get("release_headline_persona_count", final_usable_persona_count + review_ready_persona_count)},
        {"metric": "production_ready_persona_count", "value": production_ready_persona_count},
        {"metric": "review_ready_persona_count", "value": review_ready_persona_count},
        {"metric": "final_usable_persona_count", "value": final_usable_persona_count},
        {"metric": "deck_ready_persona_count", "value": quality_checks.get("deck_ready_persona_count", final_usable_persona_count)},
        {"metric": "promoted_persona_example_coverage_pct", "value": quality_checks.get("promoted_persona_example_coverage_pct", 0.0)},
        {"metric": "promoted_persona_grounded_count", "value": quality_checks.get("promoted_persona_grounded_count", 0)},
        {"metric": "promoted_persona_weakly_grounded_count", "value": quality_checks.get("promoted_persona_weakly_grounded_count", 0)},
        {"metric": "promoted_persona_ungrounded_count", "value": quality_checks.get("promoted_persona_ungrounded_count", 0)},
        {"metric": "promoted_personas_weakly_grounded", "value": quality_checks.get("promoted_personas_weakly_grounded", "")},
        {"metric": "promoted_personas_missing_examples", "value": quality_checks.get("promoted_personas_missing_examples", "")},
        {"metric": "exploratory_bucket_count", "value": exploratory_bucket_count},
        {"metric": "blocked_or_constrained_persona_count", "value": int(readiness_tier.eq("blocked_or_constrained_candidate").sum()) if not readiness_tier.empty else 0},
        {"metric": "min_cluster_size", "value": quality_checks.get("min_cluster_size", 0)},
        {"metric": "selected_axes", "value": selected_axes},
        {"metric": "clustering_mode", "value": "bottleneck_first_robust"},
    ]
    return pd.DataFrame(rows)


def _apply_workbook_promotion_constraints(
    persona_service_outputs: dict[str, Any],
    clustering_episodes_df: pd.DataFrame,
    source_balance_audit_df: pd.DataFrame,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Downgrade borderline promoted personas when concentration and source-balance risks stay high."""
    outputs = dict(persona_service_outputs)
    cluster_stats_df = outputs.get("cluster_stats_df", pd.DataFrame()).copy()
    persona_summary_df = outputs.get("persona_summary_df", pd.DataFrame()).copy()
    audit_df = outputs.get("persona_promotion_grounding_audit_df", pd.DataFrame()).copy()
    promotion_path_debug_df = outputs.get("persona_promotion_path_debug_df", pd.DataFrame()).copy()
    if cluster_stats_df.empty or persona_summary_df.empty:
        return outputs, {
            "promotion_constraint_status": "not_applicable",
            "promotion_constraint_summary": "",
        }

    weak_sources = set(
        source_balance_audit_df.loc[
            source_balance_audit_df.get("weak_source_cost_center", pd.Series(dtype=bool)).fillna(False).astype(bool),
            "source",
        ].astype(str).tolist()
    ) if not source_balance_audit_df.empty and "source" in source_balance_audit_df.columns else set()
    top_3_share = round(float(pd.to_numeric(cluster_stats_df.get("share_of_core_labeled", pd.Series(dtype=float)), errors="coerce").fillna(0.0).nlargest(3).sum()), 1)
    largest_source_influence_share = float(pd.to_numeric(source_balance_audit_df.get("blended_influence_share_pct", pd.Series(dtype=float)), errors="coerce").fillna(0.0).max()) if not source_balance_audit_df.empty else 0.0
    guard_failures: list[str] = []
    if top_3_share >= 80.0:
        guard_failures.append(f"top_3_cluster_share_of_core_labeled={top_3_share}")
    if largest_source_influence_share >= 33.0:
        guard_failures.append(f"largest_source_influence_share_pct={round(largest_source_influence_share, 1)}")
    if weak_sources:
        guard_failures.append("weak_source_cost_centers_present")
    if not guard_failures:
        return outputs, {
            "promotion_constraint_status": "clear",
            "promotion_constraint_summary": "Promotion set clears concentration and source-balance guards.",
        }

    episode_source = clustering_episodes_df[["episode_id", "source"]].drop_duplicates("episode_id") if {"episode_id", "source"}.issubset(clustering_episodes_df.columns) else pd.DataFrame(columns=["episode_id", "source"])
    assignments = outputs.get("persona_assignments_df", pd.DataFrame()).copy()
    assignments_with_source = assignments.merge(episode_source, on="episode_id", how="left") if not assignments.empty else pd.DataFrame(columns=["persona_id", "source"])
    persona_source_rank = (
        assignments_with_source.groupby(["persona_id", "source"], dropna=False).size().reset_index(name="count")
        if not assignments_with_source.empty
        else pd.DataFrame(columns=["persona_id", "source", "count"])
    )
    primary_source_lookup = (
        persona_source_rank.sort_values(["persona_id", "count", "source"], ascending=[True, False, True]).drop_duplicates("persona_id").set_index("persona_id")["source"].astype(str).to_dict()
        if not persona_source_rank.empty
        else {}
    )

    promoted = cluster_stats_df[cluster_stats_df.get("promotion_status", pd.Series(dtype=str)).astype(str).eq("promoted_persona")].copy()
    if promoted.empty:
        return outputs, {
            "promotion_constraint_status": "constrained_without_promoted_personas",
            "promotion_constraint_summary": "Promotion guards failed, but no promoted personas were available for constraint handling.",
        }
    promoted["primary_source"] = promoted["persona_id"].astype(str).map(primary_source_lookup)
    promoted["share_rank"] = promoted["share_of_core_labeled"].rank(method="first", ascending=False)
    promoted["borderline_candidate"] = (
        (pd.to_numeric(promoted.get("promotion_score", pd.Series(dtype=float)), errors="coerce").fillna(0.0) < 0.82)
        | (pd.to_numeric(promoted.get("cross_source_robustness_score", pd.Series(dtype=float)), errors="coerce").fillna(0.0) < 0.55)
        | (pd.to_numeric(promoted.get("selected_example_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int) < 2)
        | (pd.to_numeric(promoted.get("bundle_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int) < 3)
    )
    promoted["weak_source_link"] = promoted["primary_source"].astype(str).isin(weak_sources)
    promoted["protected_distinct_candidate"] = (
        promoted.get("structural_support_status", pd.Series(dtype=str)).astype(str).eq("structurally_supported")
        & promoted.get("grounding_status", pd.Series(dtype=str)).astype(str).isin({"grounded_single", "grounded_bundle"})
        & pd.to_numeric(promoted.get("cross_source_robustness_score", pd.Series(dtype=float)), errors="coerce").fillna(0.0).ge(0.75)
        & pd.to_numeric(promoted.get("share_of_core_labeled", pd.Series(dtype=float)), errors="coerce").fillna(0.0).ge(8.0)
        & pd.to_numeric(promoted.get("selected_example_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int).ge(1)
        & ~promoted["weak_source_link"]
    )
    promoted["constraint_priority"] = (
        promoted["weak_source_link"].astype(int) * 100
        + (promoted["share_rank"] > 2).astype(int) * 10
        + promoted["borderline_candidate"].astype(int)
    )
    candidates = promoted[
        (promoted["borderline_candidate"] | promoted["weak_source_link"] | (promoted["share_rank"] > 2))
        & ~promoted["protected_distinct_candidate"]
    ].sort_values(
        ["constraint_priority", "promotion_score", "persona_size"],
        ascending=[False, True, True],
    )
    if candidates.empty:
        return outputs, {
            "promotion_constraint_status": "constrained_no_borderline_match",
            "promotion_constraint_summary": "Promotion guards failed, but no borderline promoted persona met the downgrade rules.",
        }

    downgrade_target = 0
    if top_3_share >= 80.0:
        downgrade_target = max(downgrade_target, min(len(candidates), max(int(len(promoted) - 3), 1)))
    if largest_source_influence_share >= 33.0:
        downgrade_target = max(downgrade_target, 1)
    if weak_sources:
        downgrade_target = max(downgrade_target, 1)
    selected_ids = candidates.head(downgrade_target)["persona_id"].astype(str).tolist()
    if not selected_ids:
        return outputs, {
            "promotion_constraint_status": "constrained_no_selection",
            "promotion_constraint_summary": "Promotion guards failed, but no promoted persona was selected for downgrade.",
        }

    for persona_id in selected_ids:
        primary_source = str(primary_source_lookup.get(persona_id, "") or "")
        reason_parts = [
            "promotion constrained by workbook concentration and source-balance policy",
            *guard_failures,
        ]
        if primary_source and primary_source in weak_sources:
            reason_parts.append(f"primary_source={primary_source}")
        reason = "; ".join(reason_parts)
        for frame in [cluster_stats_df, persona_summary_df, audit_df, promotion_path_debug_df]:
            if frame.empty or "persona_id" not in frame.columns:
                continue
            mask = frame["persona_id"].astype(str).eq(persona_id)
            if not mask.any():
                continue
            if "promotion_status" in frame.columns:
                frame.loc[mask, "promotion_status"] = "exploratory_bucket"
            if "promotion_grounding_status" in frame.columns:
                frame.loc[mask, "promotion_grounding_status"] = "promotion_constrained_by_workbook_policy"
            if "promotion_action" in frame.columns:
                frame.loc[mask, "promotion_action"] = "downgraded_to_exploratory"
            if "visibility_state" in frame.columns:
                frame.loc[mask, "visibility_state"] = "exploratory_bucket"
            if "usability_state" in frame.columns:
                frame.loc[mask, "usability_state"] = "not_final_usable"
            if "reporting_readiness_status" in frame.columns:
                frame.loc[mask, "reporting_readiness_status"] = "promotion_constrained_by_workbook_policy"
            if "workbook_review_visible" in frame.columns:
                frame.loc[mask, "workbook_review_visible"] = False
            if "final_usable_persona" in frame.columns:
                frame.loc[mask, "final_usable_persona"] = False
            if "deck_ready_persona" in frame.columns:
                frame.loc[mask, "deck_ready_persona"] = False
            if "deck_readiness_state" in frame.columns:
                frame.loc[mask, "deck_readiness_state"] = "exploratory_only"
            if "promotion_reason" in frame.columns:
                existing = frame.loc[mask, "promotion_reason"].astype(str).str.strip()
                frame.loc[mask, "promotion_reason"] = existing.map(lambda value: f"{value}; {reason}".strip("; "))
            if "fail_reason" in frame.columns:
                frame.loc[mask, "fail_reason"] = reason
            if "one_line_summary" in frame.columns:
                existing = frame.loc[mask, "one_line_summary"].astype(str).str.strip()
                frame.loc[mask, "one_line_summary"] = existing.map(lambda value: f"Exploratory bucket retained for caution: {value}" if value else "Exploratory bucket retained for caution.")
            if "evidence_caution" in frame.columns:
                existing = frame.loc[mask, "evidence_caution"].astype(str).str.strip()
                frame.loc[mask, "evidence_caution"] = existing.map(lambda value: f"{value} Promotion is additionally constrained by concentration and source-balance risk.".strip())

    cluster_stats_df = _refresh_review_ready_overlay(cluster_stats_df)
    persona_summary_df = _refresh_review_ready_overlay(persona_summary_df)
    promotion_path_debug_df = _refresh_review_ready_overlay(promotion_path_debug_df)
    outputs["cluster_stats_df"] = cluster_stats_df
    outputs["persona_summary_df"] = persona_summary_df
    outputs["persona_promotion_grounding_audit_df"] = audit_df
    outputs["persona_promotion_path_debug_df"] = promotion_path_debug_df
    return outputs, {
        "promotion_constraint_status": "constrained",
        "promotion_constraint_summary": (
            f"Promotion set constrained by concentration/source-balance policy; downgraded {len(selected_ids)} borderline promoted personas: "
            + " | ".join(selected_ids)
        ),
    }


def _refresh_review_ready_overlay(frame: pd.DataFrame) -> pd.DataFrame:
    """Recompute review-ready output fields after workbook policy overlays mutate persona rows."""
    if frame.empty or "persona_id" not in frame.columns:
        return frame
    refreshed = frame.copy()
    rows: list[dict[str, Any]] = []
    for _, row in refreshed.iterrows():
        payload = row.to_dict()
        review_fields = _review_ready_fields(
            payload,
            selected_example_count=int(row.get("selected_example_count", 0) or 0),
            evidence_confidence_tier=str(row.get("evidence_confidence_tier", "") or ""),
        )
        rows.append(review_fields)
    overlay = pd.DataFrame(rows, index=refreshed.index)
    for column in overlay.columns:
        refreshed[column] = overlay[column]
    return refreshed


def _source_action_priority_summary(source_balance_audit_df: pd.DataFrame) -> str:
    """Return a short workbook-facing action summary for the highest-priority sources."""
    if source_balance_audit_df.empty:
        return ""
    frame = source_balance_audit_df.copy()
    frame["priority_tier"] = frame.apply(
        lambda row: "fix_now"
        if bool(row.get("weak_source_cost_center", False))
        or str(row.get("source_balance_status", "") or "") == "overdominant_source_risk"
        or str(row.get("failure_level", "") or "") == "failure"
        else "tune_soon"
        if str(row.get("failure_level", "") or "") == "warning"
        else "monitor",
        axis=1,
    )
    ranked = frame[frame["priority_tier"].isin({"fix_now", "tune_soon"})].sort_values(
        ["priority_tier", "blended_influence_share_pct", "raw_record_count", "source"],
        ascending=[True, False, False, True],
    )
    if ranked.empty:
        return "No immediate source action required beyond monitoring."
    summaries = [
        f"{row.source}:{row.priority_tier}:{row.collapse_stage}:{row.policy_action}"
        for row in ranked.head(5).itertuples(index=False)
    ]
    return " | ".join(summaries)
