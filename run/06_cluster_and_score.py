"""Run exploratory clustering, persona generation, and scoring."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.cluster import build_cluster_summary
from src.analysis.cooccurrence import build_code_edges, build_code_frequency_table
from src.analysis.clustering import build_code_clusters, cluster_summary_json
from src.analysis.pipeline_thresholds import (
    evaluate_cluster_thresholds,
    load_threshold_profile,
    upsert_threshold_audit,
)
from src.analysis.persona import build_persona_candidates
from src.analysis.persona_axes import discover_persona_axes, write_persona_axis_outputs
from src.analysis.persona_gen import generate_personas
from src.analysis.persona_service import build_persona_outputs, write_persona_outputs
from src.analysis.profiling import build_cluster_profiles
from src.analysis.report_export import export_persona_reports
from src.analysis.score import build_priority_scores
from src.analysis.summary import (
    build_counts_table,
    build_final_source_distribution,
    build_quality_checks,
    build_taxonomy_summary,
)
from src.analysis.workbook_bundle import assemble_workbook_frames, validate_workbook_frames, write_workbook_bundle
from src.utils.io import ensure_dir, load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.cluster_and_score")


def main() -> None:
    """Generate exploratory analysis parquet artifacts from labeled episodes."""
    write_debug_artifacts = os.getenv("WRITE_ANALYSIS_DEBUG_ARTIFACTS", "true").strip().lower() == "true"
    labeled_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    episodes_df = read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet")
    valid_df = read_parquet(ROOT / "data" / "valid" / "valid_candidates.parquet")
    normalized_df = read_parquet(ROOT / "data" / "normalized" / "normalized_posts.parquet")
    raw_audit_df = read_parquet(ROOT / "data" / "analysis" / "raw_audit.parquet")
    scoring = load_yaml(ROOT / "config" / "scoring.yaml")
    profile, profile_cfg = load_threshold_profile(ROOT / "config" / "pipeline_thresholds.yaml")
    cluster_threshold_df, cluster_meta = evaluate_cluster_thresholds(labeled_df, profile, profile_cfg)
    combined_threshold_df = upsert_threshold_audit(ROOT, cluster_threshold_df)

    priority_scores_df = build_priority_scores(labeled_df, scoring)
    if cluster_meta["cluster_allowed"]:
        cluster_summary_df = build_cluster_summary(labeled_df)
        persona_candidates_df = build_persona_candidates(labeled_df, priority_scores_df)
    else:
        cluster_summary_df = build_cluster_summary(labeled_df.iloc[0:0].copy())
        persona_candidates_df = build_persona_candidates(labeled_df.iloc[0:0].copy(), priority_scores_df.iloc[0:0].copy())
    cluster_summary_df = _annotate_analysis_df(cluster_summary_df, cluster_meta)
    persona_candidates_df = _annotate_analysis_df(persona_candidates_df, cluster_meta)
    priority_scores_df = _annotate_analysis_df(priority_scores_df, cluster_meta)

    write_parquet(cluster_summary_df, ROOT / "data" / "analysis" / "cluster_summary.parquet")
    write_parquet(persona_candidates_df, ROOT / "data" / "analysis" / "persona_candidates.parquet")
    write_parquet(priority_scores_df, ROOT / "data" / "analysis" / "priority_scores.parquet")
    write_parquet(priority_scores_df, ROOT / "data" / "analysis" / "priority_matrix.parquet")

    code_freq_df = build_code_frequency_table(labeled_df)
    code_edges_df = build_code_edges(labeled_df, code_freq_df, min_pair_count=5, normalization="count")
    code_freq_df.to_csv(ROOT / "data" / "analysis" / "code_freq.csv", index=False)
    code_edges_df.to_csv(ROOT / "data" / "analysis" / "code_edges.csv", index=False)

    clusters_df, cluster_summary_rows = build_code_clusters(code_freq_df, code_edges_df)
    clusters_df.to_csv(ROOT / "data" / "analysis" / "clusters.csv", index=False)
    (ensure_dir(ROOT / "data" / "analysis") / "cluster_summary.json").write_text(
        cluster_summary_json(cluster_summary_rows),
        encoding="utf-8",
    )

    cluster_profiles = build_cluster_profiles(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        clusters_df=clusters_df,
        cluster_summary_rows=cluster_summary_rows,
        priority_df=priority_scores_df,
    )
    (ROOT / "data" / "analysis" / "cluster_profiles.json").write_text(
        json.dumps(cluster_profiles, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    axis_candidates_df, final_axis_schema, implementation_note = discover_persona_axes(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
    )
    axis_paths = write_persona_axis_outputs(
        root_dir=ROOT,
        axis_candidates_df=axis_candidates_df,
        final_axis_schema=final_axis_schema,
        implementation_note=implementation_note,
    )

    personas, persona_audit = generate_personas(cluster_profiles)
    (ROOT / "data" / "analysis" / "persona_generation_audit.json").write_text(
        json.dumps(persona_audit, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    quality_checks = build_quality_checks(
        raw_audit_df=raw_audit_df,
        valid_df=valid_df,
        labeled_df=labeled_df,
        cluster_profiles=cluster_profiles,
    )
    counts_df = build_counts_table(
        raw_audit_df=raw_audit_df,
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
    )
    source_distribution_df = build_final_source_distribution(
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
    )
    taxonomy_summary_df = build_taxonomy_summary(final_axis_schema)
    persona_service_outputs = build_persona_outputs(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        final_axis_schema=final_axis_schema,
        quality_checks=quality_checks,
    )
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
    )
    for message in validate_workbook_frames(workbook_frames):
        LOGGER.warning("Workbook bundle validation: %s", message)
    bundle_paths = write_workbook_bundle(ROOT, workbook_frames)
    export_paths: dict[str, Path] = {}
    debug_paths: dict[str, Path] = {}
    if write_debug_artifacts:
        debug_paths = write_persona_outputs(ROOT, persona_service_outputs)
        counts_df.to_csv(ROOT / "data" / "analysis" / "counts.csv", index=False)
        source_distribution_df.to_csv(ROOT / "data" / "analysis" / "source_distribution.csv", index=False)
        taxonomy_summary_df.to_csv(ROOT / "data" / "analysis" / "taxonomy_summary.csv", index=False)
        debug_paths.update(
            {
                "counts_csv": ROOT / "data" / "analysis" / "counts.csv",
                "source_distribution_csv": ROOT / "data" / "analysis" / "source_distribution.csv",
                "taxonomy_summary_csv": ROOT / "data" / "analysis" / "taxonomy_summary.csv",
            }
        )
        export_paths = export_persona_reports(
            root_dir=ROOT,
            personas=personas,
            cluster_profiles=cluster_profiles,
            cluster_summary_rows=cluster_summary_rows,
            quality_checks=quality_checks,
        )
    LOGGER.info(
        "Wrote analysis artifacts (profile=%s, clusters=%s, personas=%s, priority_scores=%s, cluster_allowed=%s, exploratory_only=%s, cluster_reliability=%s, code_clusters=%s, persona_reports=%s, service_personas=%s, quality_flag=%s, persona_axes=%s, workbook_bundle=%s, debug_artifacts=%s)",
        profile,
        len(cluster_summary_df),
        len(persona_candidates_df),
        len(priority_scores_df),
        cluster_meta["cluster_allowed"],
        cluster_meta["exploratory_only"],
        cluster_meta["cluster_reliability"],
        len(cluster_summary_rows),
        len(personas),
        len(persona_service_outputs["persona_summary_df"]),
        quality_checks["quality_flag"],
        len(final_axis_schema),
        bundle_paths["manifest"],
        write_debug_artifacts,
    )
    LOGGER.info("Canonical workbook bundle: %s", ", ".join(str(path) for path in bundle_paths.values()))
    if write_debug_artifacts:
        LOGGER.info("Persona report exports: %s", ", ".join(str(path) for path in export_paths.values()))
        LOGGER.info("Persona debug outputs: %s", ", ".join(str(path) for path in debug_paths.values()))
    LOGGER.info("Persona axis discovery outputs: %s", ", ".join(str(path) for path in axis_paths.values()))
    if not cluster_meta["cluster_allowed"]:
        LOGGER.warning("Strict cluster gate skipped cluster/persona generation: %s", cluster_meta["reason"])


def _annotate_analysis_df(df, cluster_meta):
    """Attach threshold interpretation flags to exploratory analysis outputs."""
    import pandas as pd

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


if __name__ == "__main__":
    main()
