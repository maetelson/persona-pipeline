"""Axis-based persona clustering and report-ready persona tables."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.analysis.bottleneck_clustering import (
    build_bottleneck_cluster_outputs,
    render_cluster_examples_markdown,
)
from src.analysis.example_selection import apply_promotion_grounding_policy, load_example_selection_config
from src.analysis.persona_axes import build_axis_assignments
from src.utils.pipeline_schema import (
    DENOMINATOR_PERSONA_CORE_LABELED_ROWS,
    THEME_COLUMNS,
    collect_pipe_codes_from_frame,
    is_single_cluster_dominant,
    is_unknown_like,
    persona_min_cluster_size,
    round_pct,
    split_pipe_codes,
    top_non_unknown_value,
    unique_record_count,
)
from src.utils.io import ensure_dir, load_yaml


def build_persona_outputs(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    final_axis_schema: list[dict[str, Any]],
    quality_checks: dict[str, Any],
) -> dict[str, Any]:
    """Build bottleneck-first persona clusters and report-ready persona outputs."""
    axis_names = [str(row.get("axis_name", "")).strip() for row in final_axis_schema if str(row.get("axis_name", "")).strip()]
    core_axis_names = [
        str(row.get("axis_name", "")).strip()
        for row in final_axis_schema
        if str(row.get("axis_name", "")).strip() and str(row.get("axis_role", "core")).strip() == "core"
    ]
    merged = episodes_df.merge(labeled_df, on="episode_id", how="inner").fillna("")
    axis_wide_df, axis_long_df = build_axis_assignments(episodes_df, labeled_df, axis_names=axis_names)
    if merged.empty or axis_wide_df.empty or not core_axis_names:
        return _empty_outputs()

    cluster_outputs = build_bottleneck_cluster_outputs(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        axis_wide_df=axis_wide_df,
        final_axis_schema=final_axis_schema,
    )
    persona_assignments_df = cluster_outputs["persona_assignments_df"]
    persona_source_df = (
        merged.merge(axis_wide_df, on="episode_id", how="left")
        .merge(persona_assignments_df, on="episode_id", how="inner")
        .merge(cluster_outputs["feature_df"], on="episode_id", how="left")
        .fillna("")
        .drop_duplicates(subset=["episode_id"])
    )
    total_labeled_records = int(quality_checks.get("labeled_episode_rows", len(labeled_df)))
    persona_core_labeled_records = int(quality_checks.get("persona_core_labeled_rows", len(labeled_df)))
    bottleneck_config = load_yaml(Path(__file__).resolve().parents[2] / "config" / "bottleneck_clustering.yaml")
    example_config = load_example_selection_config(Path(__file__).resolve().parents[2])
    cluster_policy = _cluster_promotion_policy(
        persona_source_df,
        total_labeled_records,
        cluster_outputs.get("cluster_robustness_audit_df"),
        promotion_config=dict(bottleneck_config.get("promotion_scoring", {}) or {}),
    )
    grounding_outputs = apply_promotion_grounding_policy(
        selected_df=cluster_outputs["selected_examples_df"],
        audit_df=cluster_outputs["example_audit_df"],
        promoted_persona_ids=[
            persona_id
            for persona_id, payload in cluster_policy["status_by_persona"].items()
            if _is_promoted_candidate_persona(payload)
        ],
        config=example_config,
        max_items_per_persona=int(example_config.get("policy", {}).get("fallback", {}).get("max_examples_per_persona", 1)),
    )
    cluster_outputs["selected_examples_df"] = grounding_outputs["selected_df"]
    cluster_outputs["example_audit_df"] = grounding_outputs["audit_df"]
    cluster_policy = _merge_grounding_policy(
        cluster_policy=cluster_policy,
        persona_grounding_df=grounding_outputs["persona_grounding_df"],
        config=example_config,
        promotion_config=dict(bottleneck_config.get("promotion_scoring", {}) or {}),
    )

    persona_summary_df = _build_persona_summary_df(
        persona_source_df,
        core_axis_names,
        total_labeled_records,
        persona_core_labeled_records,
        cluster_policy,
        summary_examples_lookup=_summary_examples_lookup(cluster_outputs["selected_examples_df"]),
        naming_lookup=_naming_lookup(cluster_outputs["cluster_naming_recommendations_df"]),
    )
    persona_axes_df = _build_persona_axes_df(persona_assignments_df, axis_long_df)
    persona_pains_df = _build_persona_pains_df(persona_source_df)
    persona_cooccurrence_df = _build_persona_cooccurrence_df(persona_source_df)
    persona_examples_df = _build_persona_examples_df(cluster_outputs["selected_examples_df"])
    cluster_stats_df = _build_cluster_stats_df(
        persona_source_df,
        core_axis_names,
        total_labeled_records,
        persona_core_labeled_records,
        cluster_policy,
        cluster_outputs["cluster_meaning_audit_df"],
    )
    persona_promotion_score_df = _build_persona_promotion_score_df(persona_source_df, cluster_policy)
    persona_promotion_path_debug_df = _build_persona_promotion_path_debug_df(cluster_policy)
    structural_support_debug_df = _build_structural_support_debug_df(
        persona_source_df=persona_source_df,
        cluster_policy=cluster_policy,
        config=bottleneck_config,
    )
    structural_support_distribution_df = _build_structural_support_distribution_df(cluster_policy)

    outputs = {
        "overview_df": pd.DataFrame(columns=["metric", "value"]),
        "persona_summary_df": persona_summary_df,
        "persona_axes_df": persona_axes_df,
        "persona_pains_df": persona_pains_df,
        "persona_cooccurrence_df": persona_cooccurrence_df,
        "persona_examples_df": persona_examples_df,
        "cluster_stats_df": cluster_stats_df,
        "persona_promotion_grounding_audit_df": _build_persona_promotion_grounding_audit_df(persona_summary_df),
        "persona_promotion_score_df": persona_promotion_score_df,
        "persona_promotion_path_debug_df": persona_promotion_path_debug_df,
        "structural_support_debug_df": structural_support_debug_df,
        "structural_support_distribution_df": structural_support_distribution_df,
        "quality_checks_df": pd.DataFrame(columns=["metric", "value", "threshold", "status", "level", "denominator_type", "denominator_value", "notes"]),
        "persona_assignments_df": persona_assignments_df,
        "persona_grounding_df": grounding_outputs["persona_grounding_df"],
        "grounding_debug_df": grounding_outputs.get("grounding_debug_df", pd.DataFrame()),
        "axis_wide_df": axis_wide_df,
        "axis_long_df": axis_long_df,
        "representative_examples_v2_df": cluster_outputs["selected_examples_df"],
        "representative_examples_borderline_df": cluster_outputs["borderline_examples_df"],
        "representative_examples_rejected_df": cluster_outputs["rejected_examples_df"],
        "example_selection_audit_df": cluster_outputs["example_audit_df"],
        "representative_examples_markdown": cluster_outputs["representative_examples_markdown"],
        "representative_examples_by_new_cluster_md": render_cluster_examples_markdown(
            cluster_outputs["selected_examples_df"],
            cluster_outputs["cluster_naming_recommendations_df"],
        ),
        "cluster_meaning_audit_df": cluster_outputs["cluster_meaning_audit_df"],
        "cluster_robustness_audit_df": cluster_outputs["cluster_robustness_audit_df"],
        "cluster_robustness_summary_df": cluster_outputs["cluster_robustness_summary_df"],
        "cluster_naming_recommendations_df": cluster_outputs["cluster_naming_recommendations_df"],
        "old_vs_new_cluster_summary_df": cluster_outputs["old_vs_new_cluster_summary_df"],
        "bottleneck_feature_importance_df": cluster_outputs["bottleneck_feature_importance_df"],
        "role_feature_importance_before_after_df": cluster_outputs["role_feature_importance_before_after_df"],
        "cluster_comparison_before_after_df": cluster_outputs["cluster_comparison_before_after_df"],
        "cluster_comparison_before_after_md": cluster_outputs["cluster_comparison_before_after_md"],
        "persona_overlap_merge_audit_df": cluster_outputs["persona_overlap_merge_audit_df"],
        "persona_overlap_merge_summary_df": cluster_outputs["persona_overlap_merge_summary_df"],
        "cluster_profiles": cluster_outputs["cluster_profiles"],
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
        "persona_grounding_csv": output_dir / "persona_grounding.csv",
        "grounding_debug_csv": output_dir / "grounding_debug.csv",
        "cluster_stats_csv": output_dir / "cluster_stats.csv",
        "persona_promotion_grounding_audit_csv": output_dir / "persona_promotion_grounding_audit.csv",
        "persona_promotion_score_csv": output_dir / "persona_promotion_score.csv",
        "persona_promotion_path_debug_csv": output_dir / "persona_promotion_path_debug.csv",
        "structural_support_debug_csv": output_dir / "structural_support_debug.csv",
        "structural_support_distribution_csv": output_dir / "structural_support_distribution.csv",
        "quality_checks_csv": output_dir / "quality_checks.csv",
        "overview_csv": output_dir / "overview.csv",
        "persona_assignments_parquet": output_dir / "persona_assignments.parquet",
        "persona_axis_assignments_parquet": output_dir / "persona_axis_assignments.parquet",
        "persona_axis_values_parquet": output_dir / "persona_axis_values.parquet",
        "persona_summary_json": output_dir / "persona_summary.json",
        "representative_examples_v2_csv": output_dir / "representative_examples_v2.csv",
        "representative_examples_by_persona_md": output_dir / "representative_examples_by_persona.md",
        "representative_examples_by_new_cluster_md": output_dir / "representative_examples_by_new_cluster.md",
        "rejected_example_samples_csv": output_dir / "rejected_example_samples.csv",
        "borderline_example_samples_csv": output_dir / "borderline_example_samples.csv",
        "example_selection_audit_csv": output_dir / "example_selection_audit.csv",
        "cluster_meaning_audit_csv": output_dir / "cluster_meaning_audit.csv",
        "cluster_robustness_audit_csv": output_dir / "cluster_robustness_audit.csv",
        "cluster_robustness_summary_csv": output_dir / "cluster_robustness_summary.csv",
        "cluster_naming_recommendations_csv": output_dir / "cluster_naming_recommendations.csv",
        "old_vs_new_cluster_summary_csv": output_dir / "old_vs_new_cluster_summary.csv",
        "bottleneck_feature_importance_csv": output_dir / "bottleneck_feature_importance.csv",
        "role_feature_importance_before_after_csv": output_dir / "role_feature_importance_before_after.csv",
        "cluster_comparison_before_after_csv": output_dir / "cluster_comparison_before_after.csv",
        "cluster_comparison_before_after_md": output_dir / "cluster_comparison_before_after.md",
        "persona_overlap_merge_audit_csv": output_dir / "persona_overlap_merge_audit.csv",
        "persona_overlap_merge_summary_csv": output_dir / "persona_overlap_merge_summary.csv",
    }
    outputs["persona_summary_df"].to_csv(paths["persona_summary_csv"], index=False)
    outputs["persona_axes_df"].to_csv(paths["persona_axes_csv"], index=False)
    outputs["persona_pains_df"].to_csv(paths["persona_pains_csv"], index=False)
    outputs["persona_cooccurrence_df"].to_csv(paths["persona_cooccurrence_csv"], index=False)
    outputs["persona_examples_df"].to_csv(paths["persona_examples_csv"], index=False)
    outputs["persona_grounding_df"].to_csv(paths["persona_grounding_csv"], index=False)
    outputs["grounding_debug_df"].to_csv(paths["grounding_debug_csv"], index=False)
    outputs["cluster_stats_df"].to_csv(paths["cluster_stats_csv"], index=False)
    outputs["persona_promotion_grounding_audit_df"].to_csv(paths["persona_promotion_grounding_audit_csv"], index=False)
    outputs["persona_promotion_score_df"].to_csv(paths["persona_promotion_score_csv"], index=False)
    outputs["persona_promotion_path_debug_df"].to_csv(paths["persona_promotion_path_debug_csv"], index=False)
    outputs["structural_support_debug_df"].to_csv(paths["structural_support_debug_csv"], index=False)
    outputs["structural_support_distribution_df"].to_csv(paths["structural_support_distribution_csv"], index=False)
    outputs["quality_checks_df"].to_csv(paths["quality_checks_csv"], index=False)
    outputs["overview_df"].to_csv(paths["overview_csv"], index=False)
    outputs["persona_assignments_df"].to_parquet(paths["persona_assignments_parquet"], index=False)
    outputs["axis_wide_df"].to_parquet(paths["persona_axis_assignments_parquet"], index=False)
    outputs["axis_long_df"].to_parquet(paths["persona_axis_values_parquet"], index=False)
    outputs["representative_examples_v2_df"].to_csv(paths["representative_examples_v2_csv"], index=False)
    outputs["representative_examples_rejected_df"].head(200).to_csv(paths["rejected_example_samples_csv"], index=False)
    outputs["representative_examples_borderline_df"].head(200).to_csv(paths["borderline_example_samples_csv"], index=False)
    outputs["example_selection_audit_df"].to_csv(paths["example_selection_audit_csv"], index=False)
    outputs["cluster_meaning_audit_df"].to_csv(paths["cluster_meaning_audit_csv"], index=False)
    outputs["cluster_robustness_audit_df"].to_csv(paths["cluster_robustness_audit_csv"], index=False)
    outputs["cluster_robustness_summary_df"].to_csv(paths["cluster_robustness_summary_csv"], index=False)
    outputs["cluster_naming_recommendations_df"].to_csv(paths["cluster_naming_recommendations_csv"], index=False)
    outputs["old_vs_new_cluster_summary_df"].to_csv(paths["old_vs_new_cluster_summary_csv"], index=False)
    outputs["bottleneck_feature_importance_df"].to_csv(paths["bottleneck_feature_importance_csv"], index=False)
    outputs["role_feature_importance_before_after_df"].to_csv(paths["role_feature_importance_before_after_csv"], index=False)
    outputs["cluster_comparison_before_after_df"].to_csv(paths["cluster_comparison_before_after_csv"], index=False)
    outputs["persona_overlap_merge_audit_df"].to_csv(paths["persona_overlap_merge_audit_csv"], index=False)
    outputs["persona_overlap_merge_summary_df"].to_csv(paths["persona_overlap_merge_summary_csv"], index=False)
    paths["representative_examples_by_persona_md"].write_text(outputs["representative_examples_markdown"], encoding="utf-8")
    paths["representative_examples_by_new_cluster_md"].write_text(outputs["representative_examples_by_new_cluster_md"], encoding="utf-8")
    paths["cluster_comparison_before_after_md"].write_text(outputs["cluster_comparison_before_after_md"], encoding="utf-8")
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


def _build_persona_summary_df(
    persona_source_df: pd.DataFrame,
    axis_names: list[str],
    total_labeled_records: int,
    persona_core_labeled_records: int,
    cluster_policy: dict[str, Any],
    summary_examples_lookup: dict[str, list[str]],
    naming_lookup: dict[str, str],
) -> pd.DataFrame:
    """Build top-level persona summary sheet."""
    rows: list[dict[str, Any]] = []
    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        persona_size = unique_record_count(group)
        role = top_non_unknown_value(group, "user_role")
        workflow = top_non_unknown_value(group, "workflow_stage")
        goal = top_non_unknown_value(group, "analysis_goal")
        bottleneck = top_non_unknown_value(group, "bottleneck_type")
        trust = top_non_unknown_value(group, "trust_validation_need")
        tool_mode = top_non_unknown_value(group, "tool_dependency_mode")
        output_mode = top_non_unknown_value(group, "output_expectation")
        top_pain_points = _top_themes(group, ["pain_codes", "question_codes"], limit=4)
        representative_examples = summary_examples_lookup.get(str(persona_id), [])
        cluster_name = naming_lookup.get(str(persona_id), str(group.get("cluster_name", pd.Series([persona_id])).iloc[0]))
        promotion = cluster_policy["status_by_persona"].get(str(persona_id), {})
        legacy_persona_name = _archetype_name(role, workflow, bottleneck, goal, output_mode, promotion.get("status", "exploratory_bucket"))
        schema_fields = _persona_schema_fields(
            group=group,
            role=role,
            workflow=workflow,
            goal=goal,
            bottleneck=bottleneck,
            trust=trust,
            tool_mode=tool_mode,
            output_mode=output_mode,
            top_pain_points=top_pain_points,
            representative_examples=representative_examples,
        )
        persona_name = str(schema_fields.get("persona_profile_name", "") or legacy_persona_name)
        rows.append(
            {
                "persona_id": persona_id,
                "persona_schema_version": "b2b_workflow_persona_v2",
                "persona_name": persona_name,
                "legacy_persona_name": legacy_persona_name,
                "persona_size": persona_size,
                "share_of_core_labeled": round_pct(persona_size, persona_core_labeled_records),
                "share_of_all_labeled": round_pct(persona_size, total_labeled_records),
                "denominator_type": DENOMINATOR_PERSONA_CORE_LABELED_ROWS,
                "denominator_value": persona_core_labeled_records,
                "min_cluster_size": int(cluster_policy["min_cluster_size"]),
                "base_promotion_status": promotion.get("base_promotion_status", promotion.get("status", "exploratory_bucket")),
                "structural_support_status": promotion.get("structural_support_status", "not_evaluated"),
                "structural_support_reason": promotion.get("structural_support_reason", ""),
                "visibility_state": _visibility_state(promotion),
                "usability_state": _usability_state(promotion),
                "deck_readiness_state": _deck_readiness_state(promotion),
                "promotion_action": _promotion_action(promotion),
                "promoted_candidate_persona": _is_promoted_candidate_persona(promotion),
                "workbook_review_visible": _is_workbook_review_visible(promotion),
                "final_usable_persona": _is_final_usable_persona(promotion),
                "deck_ready_persona": _is_final_usable_persona(promotion),
                "reporting_readiness_status": _reporting_readiness_status(promotion),
                "promotion_status": promotion.get("status", "exploratory_bucket"),
                "grounding_status": promotion.get("grounding_status", "not_applicable"),
                "promotion_grounding_status": promotion.get("promotion_grounding_status", "exploratory_bucket"),
                "promotion_reason": promotion.get("reason", ""),
                "grounding_reason": promotion.get("grounding_reason", ""),
                "grounded_candidate_count": int(promotion.get("grounded_candidate_count", 0) or 0),
                "weak_candidate_count": int(promotion.get("weak_candidate_count", 0) or 0),
                "structural_stability_score": float(promotion.get("structural_stability_score", 0.0) or 0.0),
                "grounding_quality_score": float(promotion.get("grounding_quality_score", 0.0) or 0.0),
                "distinctiveness_score": float(promotion.get("distinctiveness_score", 0.0) or 0.0),
                "actionability_score": float(promotion.get("actionability_score", 0.0) or 0.0),
                "output_consistency_score": float(promotion.get("output_consistency_score", 0.0) or 0.0),
                "cross_source_robustness_score": float(promotion.get("cross_source_robustness_score", 0.0) or 0.0),
                "promotion_score": float(promotion.get("promotion_score", 0.0) or 0.0),
                "product_value_proposition": promotion.get("product_value_proposition", ""),
                "activation_moment": promotion.get("activation_moment", ""),
                "ux_feature_need": promotion.get("ux_feature_need", ""),
                "nearest_persona_id": promotion.get("nearest_persona_id", ""),
                "strategic_redundancy_status": promotion.get("strategic_redundancy_status", ""),
                "strategic_redundancy_reason": promotion.get("strategic_redundancy_reason", ""),
                "context_evidence_count": int(promotion.get("context_evidence_count", 0) or 0),
                "workaround_evidence_count": int(promotion.get("workaround_evidence_count", 0) or 0),
                "trust_validation_evidence_count": int(promotion.get("trust_validation_evidence_count", 0) or 0),
                "bundle_episode_count": int(promotion.get("bundle_episode_count", 0) or 0),
                "bundle_dimension_hits": int(promotion.get("bundle_dimension_hits", 0) or 0),
                "total_bundle_strength": int(promotion.get("total_bundle_strength", 0) or 0),
                "bundle_grounding_status": promotion.get("bundle_grounding_status", ""),
                "bundle_grounding_reason": promotion.get("bundle_grounding_reason", ""),
                "bundle_support_examples": promotion.get("bundle_support_examples", ""),
                "selected_example_count": int(promotion.get("selected_example_count", 0) or 0),
                "fallback_selected_count": int(promotion.get("fallback_selected_count", 0) or 0),
                "cluster_stability_status": promotion.get("cluster_stability_status", ""),
                "cluster_evidence_status": promotion.get("cluster_evidence_status", ""),
                "cluster_concentration_status": promotion.get("cluster_concentration_status", ""),
                "tail_fragility_status": promotion.get("tail_fragility_status", ""),
                "cluster_separation": float(promotion.get("cluster_separation", 0.0) or 0.0),
                "nearest_neighbor_similarity": float(promotion.get("nearest_neighbor_similarity", 0.0) or 0.0),
                "pre_merge_anchor_count": int(promotion.get("pre_merge_anchor_count", 0) or 0),
                "robustness_action_summary": promotion.get("robustness_action_summary", ""),
                "one_line_summary": _one_line_summary(
                    persona_name,
                    workflow,
                    bottleneck,
                    goal,
                    output_mode,
                    promotion.get("status", ""),
                    promotion.get("promotion_grounding_status", ""),
                ),
                "main_workflow_context": workflow,
                "dominant_bottleneck": bottleneck,
                "analysis_behavior": goal,
                "trust_explanation_need": trust,
                "current_tool_dependency": tool_mode,
                "primary_output_expectation": output_mode,
                "top_pain_points": " | ".join(top_pain_points),
                "representative_examples": " | ".join(representative_examples),
                "why_this_persona_matters": _why_persona_matters(group, bottleneck, goal, output_mode),
                "legacy_cluster_name": cluster_name,
                **schema_fields,
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
    persona_sizes = persona_source_df.groupby("persona_id")["episode_id"].nunique().to_dict()
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
    persona_sizes = persona_source_df.groupby("persona_id")["episode_id"].nunique().to_dict()
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


def _build_persona_examples_df(selected_examples_df: pd.DataFrame) -> pd.DataFrame:
    """Build grounded representative examples per persona."""
    if selected_examples_df is None or selected_examples_df.empty:
        return pd.DataFrame(columns=["persona_id", "example_rank", "grounded_text", "why_selected", "matched_axes", "reason_selected"])
    frame = selected_examples_df.copy()
    if "why_selected" not in frame.columns:
        frame["why_selected"] = frame.get("reason_selected", "")
    if "matched_axes" not in frame.columns:
        frame["matched_axes"] = frame.get("cluster_fit_reason", "")
    preferred = [
        "persona_id",
        "example_rank",
        "grounded_text",
        "selection_strength",
        "grounding_strength",
        "fallback_selected",
        "coverage_selection_reason",
        "grounding_reason",
        "why_selected",
        "matched_axes",
        "reason_selected",
        "quote_quality",
        "grounding_fit_score",
        "mismatch_count",
        "critical_mismatch_count",
        "matched_axis_count",
        "final_example_score",
    ]
    remainder = [column for column in frame.columns if column not in preferred]
    return frame[preferred + remainder].sort_values(["persona_id", "example_rank"]).reset_index(drop=True)


def _build_persona_promotion_grounding_audit_df(persona_summary_df: pd.DataFrame) -> pd.DataFrame:
    """Build one audit row per promoted candidate persona with explicit disposition fields."""
    if persona_summary_df is None or persona_summary_df.empty:
        return pd.DataFrame()
    promoted = persona_summary_df[persona_summary_df.get("promoted_candidate_persona", pd.Series(dtype=bool)).fillna(False).astype(bool)].copy()
    if promoted.empty:
        return pd.DataFrame()
    preferred = [
        "persona_id",
        "persona_name",
        "persona_size",
        "base_promotion_status",
        "structural_support_status",
        "structural_support_reason",
        "visibility_state",
        "promotion_status",
        "promotion_action",
        "promotion_reason",
        "grounding_status",
        "promotion_grounding_status",
        "grounding_reason",
        "structural_stability_score",
        "grounding_quality_score",
        "distinctiveness_score",
        "actionability_score",
        "output_consistency_score",
        "cross_source_robustness_score",
        "promotion_score",
        "product_value_proposition",
        "activation_moment",
        "ux_feature_need",
        "nearest_persona_id",
        "strategic_redundancy_status",
        "strategic_redundancy_reason",
        "grounded_candidate_count",
        "weak_candidate_count",
        "context_evidence_count",
        "workaround_evidence_count",
        "trust_validation_evidence_count",
        "bundle_episode_count",
        "bundle_dimension_hits",
        "total_bundle_strength",
        "bundle_grounding_status",
        "bundle_grounding_reason",
        "bundle_support_examples",
        "selected_example_count",
        "fallback_selected_count",
        "cluster_stability_status",
        "cluster_evidence_status",
        "cluster_concentration_status",
        "tail_fragility_status",
        "cluster_separation",
        "nearest_neighbor_similarity",
        "pre_merge_anchor_count",
        "robustness_action_summary",
        "usability_state",
        "deck_readiness_state",
        "final_usable_persona",
        "deck_ready_persona",
    ]
    available = [column for column in preferred if column in promoted.columns]
    return promoted[available].sort_values(["final_usable_persona", "persona_size", "persona_id"], ascending=[False, False, True]).reset_index(drop=True)


def _build_cluster_stats_df(
    persona_source_df: pd.DataFrame,
    axis_names: list[str],
    total_labeled_records: int,
    persona_core_labeled_records: int,
    cluster_policy: dict[str, Any],
    cluster_audit_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build stable persona-cluster stats."""
    rows: list[dict[str, Any]] = []
    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        persona_size = unique_record_count(group)
        axis_signature = " | ".join(
            f"{axis}={top_non_unknown_value(group, axis)}"
            for axis in axis_names
        )
        rows.append(
            {
                "persona_id": persona_id,
                "persona_size": persona_size,
                "share_of_core_labeled": round_pct(persona_size, persona_core_labeled_records),
                "share_of_all_labeled": round_pct(persona_size, total_labeled_records),
                "denominator_type": DENOMINATOR_PERSONA_CORE_LABELED_ROWS,
                "denominator_value": persona_core_labeled_records,
                "min_cluster_size": int(cluster_policy["min_cluster_size"]),
                "base_promotion_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("base_promotion_status", "exploratory_bucket"),
                "structural_support_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("structural_support_status", "not_evaluated"),
                "structural_support_reason": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("structural_support_reason", ""),
                "visibility_state": _visibility_state(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "usability_state": _usability_state(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "deck_readiness_state": _deck_readiness_state(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "promotion_action": _promotion_action(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "promoted_candidate_persona": _is_promoted_candidate_persona(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "workbook_review_visible": _is_workbook_review_visible(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "final_usable_persona": _is_final_usable_persona(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "deck_ready_persona": _is_final_usable_persona(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "reporting_readiness_status": _reporting_readiness_status(cluster_policy["status_by_persona"].get(str(persona_id), {})),
                "promotion_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("status", "exploratory_bucket"),
                "promotion_reason": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("reason", ""),
                "grounding_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("grounding_status", "not_applicable"),
                "promotion_grounding_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("promotion_grounding_status", "exploratory_bucket"),
                "grounding_reason": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("grounding_reason", ""),
                "grounded_candidate_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("grounded_candidate_count", 0) or 0),
                "weak_candidate_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("weak_candidate_count", 0) or 0),
                "structural_stability_score": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("structural_stability_score", 0.0) or 0.0),
                "grounding_quality_score": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("grounding_quality_score", 0.0) or 0.0),
                "distinctiveness_score": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("distinctiveness_score", 0.0) or 0.0),
                "actionability_score": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("actionability_score", 0.0) or 0.0),
                "output_consistency_score": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("output_consistency_score", 0.0) or 0.0),
                "cross_source_robustness_score": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("cross_source_robustness_score", 0.0) or 0.0),
                "promotion_score": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("promotion_score", 0.0) or 0.0),
                "product_value_proposition": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("product_value_proposition", ""),
                "activation_moment": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("activation_moment", ""),
                "ux_feature_need": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("ux_feature_need", ""),
                "nearest_persona_id": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("nearest_persona_id", ""),
                "strategic_redundancy_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("strategic_redundancy_status", ""),
                "strategic_redundancy_reason": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("strategic_redundancy_reason", ""),
                "context_evidence_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("context_evidence_count", 0) or 0),
                "workaround_evidence_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("workaround_evidence_count", 0) or 0),
                "trust_validation_evidence_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("trust_validation_evidence_count", 0) or 0),
                "bundle_episode_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("bundle_episode_count", 0) or 0),
                "bundle_dimension_hits": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("bundle_dimension_hits", 0) or 0),
                "total_bundle_strength": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("total_bundle_strength", 0) or 0),
                "bundle_grounding_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("bundle_grounding_status", ""),
                "bundle_grounding_reason": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("bundle_grounding_reason", ""),
                "selected_example_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("selected_example_count", 0) or 0),
                "fallback_selected_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("fallback_selected_count", 0) or 0),
                "cluster_stability_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("cluster_stability_status", ""),
                "cluster_evidence_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("cluster_evidence_status", ""),
                "cluster_concentration_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("cluster_concentration_status", ""),
                "tail_fragility_status": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("tail_fragility_status", ""),
                "cluster_separation": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("cluster_separation", 0.0) or 0.0),
                "nearest_neighbor_similarity": float(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("nearest_neighbor_similarity", 0.0) or 0.0),
                "pre_merge_anchor_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("pre_merge_anchor_count", 0) or 0),
                "robustness_action_summary": cluster_policy["status_by_persona"].get(str(persona_id), {}).get("robustness_action_summary", ""),
                "dominant_signature": axis_signature,
                "dominant_bottleneck": top_non_unknown_value(group, "bottleneck_type"),
                "dominant_analysis_goal": top_non_unknown_value(group, "analysis_goal"),
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
        "persona_grounding_df": empty,
        "grounding_debug_df": empty,
        "cluster_stats_df": empty,
        "persona_promotion_grounding_audit_df": empty,
        "persona_promotion_score_df": empty,
        "persona_promotion_path_debug_df": empty,
        "structural_support_debug_df": empty,
        "structural_support_distribution_df": empty,
        "quality_checks_df": empty,
        "persona_assignments_df": empty,
        "axis_wide_df": empty,
        "axis_long_df": empty,
        "representative_examples_v2_df": empty,
        "representative_examples_borderline_df": empty,
        "representative_examples_rejected_df": empty,
        "example_selection_audit_df": empty,
        "representative_examples_markdown": "",
        "representative_examples_by_new_cluster_md": "",
        "cluster_meaning_audit_df": empty,
        "cluster_robustness_audit_df": empty,
        "cluster_robustness_summary_df": empty,
        "cluster_naming_recommendations_df": empty,
        "old_vs_new_cluster_summary_df": empty,
        "bottleneck_feature_importance_df": empty,
        "role_feature_importance_before_after_df": empty,
        "cluster_comparison_before_after_df": empty,
        "cluster_comparison_before_after_md": "",
        "persona_overlap_merge_audit_df": empty,
        "persona_overlap_merge_summary_df": empty,
        "cluster_profiles": [],
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


def _theme_values(group: pd.DataFrame, columns: list[str]) -> list[str]:
    """Collect repeated theme values from labeled columns."""
    return collect_pipe_codes_from_frame(group, columns)


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


def _persona_name(role: str, workflow: str, goal: str) -> str:
    """Create a grounded persona name from dominant axis values."""
    parts = [_titleize(role), _titleize(workflow), _titleize(goal)]
    return " ".join(part for part in parts if part and part != "Unassigned").strip() or "Mixed Persona"


def _one_line_summary(
    cluster_name: str,
    workflow: str,
    bottleneck: str,
    goal: str,
    output_mode: str = "",
    promotion_status: str = "",
    promotion_grounding_status: str = "",
) -> str:
    """Create a grounded one-line persona summary."""
    if promotion_grounding_status == "grounded_but_structurally_weak":
        prefix = "Review-visible structurally weak grounded persona"
    elif promotion_grounding_status == "promoted_but_weakly_grounded":
        prefix = "Review-visible weakly grounded persona"
    elif promotion_grounding_status == "promoted_but_ungrounded":
        prefix = "Review-visible ungrounded persona"
    elif promotion_grounding_status == "downgraded_due_to_no_grounding":
        prefix = "Downgraded exploratory cluster"
    elif promotion_status == "review_visible_persona":
        prefix = "Review-visible persona"
    else:
        prefix = "Promoted persona" if promotion_status == "promoted_persona" else "Exploratory residual group"
    return (
        f"{prefix}: {cluster_name} repeatedly works in {_titleize(workflow, 'mixed workflow').lower()} "
        f"to {_titleize(goal, 'move analysis forward').lower()}, but {_titleize(bottleneck, 'general friction').lower()} "
        f"keeps blocking {_titleize(output_mode, 'usable output').lower()}."
    )


def _why_persona_matters(group: pd.DataFrame, bottleneck: str, goal: str, output_mode: str) -> str:
    """Summarize why this persona matters using computed stats only."""
    size = unique_record_count(group) or int(len(group))
    return (
        f"{size} labeled records repeatedly combine {_titleize(goal).lower()}, "
        f"{_titleize(bottleneck).lower()}, and {_titleize(output_mode).lower()} expectations."
    )


def _persona_schema_fields(
    group: pd.DataFrame,
    role: str,
    workflow: str,
    goal: str,
    bottleneck: str,
    trust: str,
    tool_mode: str,
    output_mode: str,
    top_pain_points: list[str],
    representative_examples: list[str],
) -> dict[str, str]:
    """Derive layered B2B workflow-persona fields from current labels and evidence."""
    secondary_bottleneck = _secondary_bottleneck(group, primary=bottleneck)
    trust_failure_mode = _trust_failure_mode(group, bottleneck=bottleneck, trust=trust)
    workaround_pattern = _workaround_pattern(group, bottleneck=bottleneck, tool_mode=tool_mode, output_mode=output_mode)
    user_role_family = _user_role_family(role)
    functional_context = _functional_context(workflow, goal, output_mode)
    stakeholder_exposure = _stakeholder_exposure(group, workflow, goal, output_mode, trust)
    decision_responsibility = _decision_responsibility(goal, workflow, trust)
    recurring_job_to_be_done = _recurring_job_to_be_done(goal, workflow, output_mode)
    typical_trigger_event = _typical_trigger_event(group, goal, workflow, bottleneck, trust)
    expected_output_artifact = _expected_output_artifact(output_mode, workflow)
    frequency_of_need = _frequency_of_need(goal, workflow, bottleneck)
    why_current_tools_fail = _why_current_tools_fail(bottleneck, trust_failure_mode, workaround_pattern, tool_mode)
    why_this_persona_would_use_our_product = _why_this_persona_would_use_our_product(
        recurring_job_to_be_done,
        primary_bottleneck=bottleneck,
        expected_output_artifact=expected_output_artifact,
        trust_failure_mode=trust_failure_mode,
    )
    activation_moment = _activation_moment(typical_trigger_event, primary_bottleneck=bottleneck, trust_failure_mode=trust_failure_mode)
    success_signal = _success_signal(recurring_job_to_be_done, expected_output_artifact, primary_bottleneck=bottleneck)
    role_context = {
        "user_role_family": user_role_family,
        "functional_context": functional_context,
        "stakeholder_exposure": stakeholder_exposure,
        "decision_responsibility": decision_responsibility,
    }
    work_loop = {
        "recurring_job_to_be_done": recurring_job_to_be_done,
        "typical_trigger_event": typical_trigger_event,
        "expected_output_artifact": expected_output_artifact,
        "frequency_of_need": frequency_of_need,
    }
    bottleneck_pattern = {
        "primary_bottleneck": bottleneck,
        "secondary_bottleneck": secondary_bottleneck,
        "trust_failure_mode": trust_failure_mode,
        "workaround_pattern": workaround_pattern,
    }
    product_relevance = {
        "why_current_tools_fail": why_current_tools_fail,
        "why_this_persona_would_use_our_product": why_this_persona_would_use_our_product,
        "activation_moment": activation_moment,
        "success_signal": success_signal,
    }
    return {
        "persona_profile_name": _persona_profile_name(user_role_family, recurring_job_to_be_done, functional_context),
        "user_role_family": user_role_family,
        "functional_context": functional_context,
        "stakeholder_exposure": stakeholder_exposure,
        "decision_responsibility": decision_responsibility,
        "recurring_job_to_be_done": recurring_job_to_be_done,
        "typical_trigger_event": typical_trigger_event,
        "expected_output_artifact": expected_output_artifact,
        "frequency_of_need": frequency_of_need,
        "primary_bottleneck": bottleneck,
        "secondary_bottleneck": secondary_bottleneck,
        "trust_failure_mode": trust_failure_mode,
        "workaround_pattern": workaround_pattern,
        "why_current_tools_fail": why_current_tools_fail,
        "why_this_persona_would_use_our_product": why_this_persona_would_use_our_product,
        "activation_moment": activation_moment,
        "success_signal": success_signal,
        "role_context_json": json.dumps(role_context, ensure_ascii=False, sort_keys=True),
        "work_loop_json": json.dumps(work_loop, ensure_ascii=False, sort_keys=True),
        "bottleneck_pattern_json": json.dumps(bottleneck_pattern, ensure_ascii=False, sort_keys=True),
        "product_relevance_json": json.dumps(product_relevance, ensure_ascii=False, sort_keys=True),
        "derivation_basis": "labels_plus_episode_text",
        "derivation_evidence_summary": " | ".join((top_pain_points + representative_examples)[:4]),
    }


def _top_non_unknown_values(group: pd.DataFrame, column: str, limit: int) -> list[str]:
    """Return the most common non-unknown values for one grouped column."""
    if column not in group.columns:
        return []
    counts = Counter(
        str(value).strip()
        for value in group[column].tolist()
        if str(value).strip() and not is_unknown_like(value)
    )
    return [value for value, _ in counts.most_common(limit)]


def _secondary_bottleneck(group: pd.DataFrame, primary: str) -> str:
    """Return the next strongest bottleneck label when available."""
    for value in _top_non_unknown_values(group, "bottleneck_type", 4):
        if value != primary:
            return value
    theme_map = {
        "manual_reporting": "tool_limitation",
        "tool_limitation": "handoff_dependency",
        "data_quality": "manual_reporting",
        "handoff_dependency": "data_quality",
        "general_friction": "tool_limitation",
    }
    return theme_map.get(str(primary or "").strip(), "unassigned")


def _user_role_family(role: str) -> str:
    """Normalize dominant role into a reusable workflow persona family."""
    mapping = {
        "analyst": "analyst_operator",
        "manager": "manager_operator",
        "marketer": "marketing_operator",
        "business_user": "business_operator",
    }
    return mapping.get(str(role).strip().lower(), "workflow_operator")


def _functional_context(workflow: str, goal: str, output_mode: str) -> str:
    """Describe the stable functional context around the recurring work."""
    workflow_key = str(workflow).strip().lower()
    goal_key = str(goal).strip().lower()
    output_key = str(output_mode).strip().lower()
    if workflow_key == "reporting" or goal_key == "report_speed" or output_key == "excel_ready_output":
        return "reporting_and_performance_management"
    if workflow_key == "validation" or goal_key == "validate_numbers":
        return "metric_governance_and_validation"
    if workflow_key == "automation" or goal_key == "automate_workflow":
        return "analytics_operations_and_automation"
    if workflow_key == "triage" or goal_key == "diagnose_change":
        return "performance_monitoring_and_issue_triage"
    return "analytics_workflow_execution"


def _stakeholder_exposure(group: pd.DataFrame, workflow: str, goal: str, output_mode: str, trust: str) -> str:
    """Estimate how visible the work is to external stakeholders inside the business."""
    themes = set(_top_themes(group, ["pain_codes", "question_codes", "output_codes"], limit=8))
    if str(output_mode).strip().lower() == "excel_ready_output" or str(goal).strip().lower() == "report_speed":
        return "cross_functional_reporting_delivery"
    if str(trust).strip().lower() == "high" or "Q_VALIDATE_NUMBERS" in themes:
        return "decision_support_with_signoff_pressure"
    if str(workflow).strip().lower() == "triage":
        return "operational_visibility_and_explanation"
    return "team_internal_workflow"


def _decision_responsibility(goal: str, workflow: str, trust: str) -> str:
    """Describe the persona's recurring decision burden."""
    goal_key = str(goal).strip().lower()
    workflow_key = str(workflow).strip().lower()
    trust_key = str(trust).strip().lower()
    if goal_key == "validate_numbers" or workflow_key == "validation" or trust_key == "high":
        return "metric_signoff_and_pre_share_validation"
    if goal_key == "diagnose_change" or workflow_key == "triage":
        return "investigate_and_explain_variance"
    if goal_key == "automate_workflow" or workflow_key == "automation":
        return "operationalize_repeatable_analysis_work"
    if goal_key == "report_speed" or workflow_key == "reporting":
        return "package_and_deliver_recurring_reporting"
    return "support_operational_analysis_execution"


def _recurring_job_to_be_done(goal: str, workflow: str, output_mode: str) -> str:
    """Express the main recurring work loop in action terms."""
    goal_key = str(goal).strip().lower()
    workflow_key = str(workflow).strip().lower()
    output_key = str(output_mode).strip().lower()
    if goal_key == "report_speed" or workflow_key == "reporting":
        return "deliver_recurring_reporting_without_manual_repackaging"
    if goal_key == "diagnose_change" or workflow_key == "triage":
        return "explain_metric_or_dashboard_changes_fast"
    if goal_key == "validate_numbers" or workflow_key == "validation":
        return "validate_numbers_before_sharing_or_acting"
    if goal_key == "automate_workflow" or workflow_key == "automation" or output_key == "automation_output":
        return "turn_repeated_analysis_work_into_repeatable_ops"
    return "move_analysis_work_to_a_shareable_output"


def _typical_trigger_event(group: pd.DataFrame, goal: str, workflow: str, bottleneck: str, trust: str) -> str:
    """Infer the event that typically starts this persona's work loop."""
    themes = set(_top_themes(group, ["question_codes", "pain_codes"], limit=8))
    if str(goal).strip().lower() == "report_speed" or str(workflow).strip().lower() == "reporting":
        return "scheduled_reporting_cycle_or_stakeholder_request"
    if str(goal).strip().lower() == "diagnose_change" or str(workflow).strip().lower() == "triage":
        return "unexpected_metric_shift_or_dashboard_question"
    if str(goal).strip().lower() == "validate_numbers" or str(trust).strip().lower() == "high":
        return "numbers_do_not_reconcile_before_distribution"
    if str(goal).strip().lower() == "automate_workflow" or str(workflow).strip().lower() == "automation":
        return "repeated_manual_work_becomes_operationally_unacceptable"
    if str(bottleneck).strip().lower() == "handoff_dependency" or "P_HANDOFF" in themes:
        return "cross_team_follow_up_blocks_delivery"
    return "analysis_delivery_deadline_or_escalation"


def _expected_output_artifact(output_mode: str, workflow: str) -> str:
    """Normalize the end artifact this persona is trying to produce."""
    mapping = {
        "excel_ready_output": "stakeholder_ready_export_or_packaged_report",
        "dashboard_update": "updated_dashboard_or_explanation_for_existing_dashboard",
        "automation_output": "repeatable_workflow_or_scheduled_delivery",
    }
    output_key = str(output_mode).strip().lower()
    if output_key in mapping:
        return mapping[output_key]
    if str(workflow).strip().lower() == "validation":
        return "validated_metric_pack_before_distribution"
    return "shareable_analysis_output"


def _frequency_of_need(goal: str, workflow: str, bottleneck: str) -> str:
    """Estimate whether the need is cyclical, event-driven, or continuous."""
    goal_key = str(goal).strip().lower()
    workflow_key = str(workflow).strip().lower()
    bottleneck_key = str(bottleneck).strip().lower()
    if goal_key == "report_speed" or workflow_key == "reporting":
        return "scheduled_recurring"
    if goal_key == "diagnose_change" or workflow_key == "triage":
        return "event_driven"
    if goal_key == "validate_numbers" or workflow_key == "validation":
        return "pre_share_or_pre_decision"
    if goal_key == "automate_workflow" or workflow_key == "automation" or bottleneck_key == "general_friction":
        return "continuous_operational"
    return "recurring_operational"


def _trust_failure_mode(group: pd.DataFrame, bottleneck: str, trust: str) -> str:
    """Describe how trust breaks in the current workflow."""
    themes = set(_top_themes(group, ["pain_codes", "question_codes"], limit=10))
    bottleneck_key = str(bottleneck).strip().lower()
    trust_key = str(trust).strip().lower()
    if bottleneck_key == "data_quality" or trust_key == "high" or "Q_VALIDATE_NUMBERS" in themes:
        return "numbers_do_not_reconcile_or_feel_safe_to_share"
    if bottleneck_key == "handoff_dependency" or "P_HANDOFF" in themes:
        return "context_is_not_explainable_without_manual_follow_up"
    if bottleneck_key == "tool_limitation":
        return "tool_outputs_do_not_support_confident_explanation"
    if bottleneck_key == "manual_reporting":
        return "delivery_depends_on_manual_rework_and_can_drift"
    return "workflow_breaks_before_a_trusted_output_exists"


def _workaround_pattern(group: pd.DataFrame, bottleneck: str, tool_mode: str, output_mode: str) -> str:
    """Describe the repeatable workaround pattern implied by current evidence."""
    workaround_counts = Counter(_theme_values(group, ["workaround_codes", "env_codes", "output_codes"]))
    dominant_workarounds = [value for value, _ in workaround_counts.most_common(4)]
    tool_key = str(tool_mode).strip().lower()
    bottleneck_key = str(bottleneck).strip().lower()
    output_key = str(output_mode).strip().lower()
    if tool_key == "spreadsheet_heavy" or bottleneck_key == "manual_reporting":
        return "export_then_patch_in_spreadsheet"
    if "W_SCRIPT" in dominant_workarounds or tool_key == "script_assisted":
        return "fill_gaps_with_custom_scripts_or_manual_queries"
    if bottleneck_key == "handoff_dependency":
        return "escalate_to_other_teams_for_context_and_validation"
    if bottleneck_key == "tool_limitation" or output_key == "dashboard_update":
        return "rebuild_or_explain_results_outside_the_primary_tool"
    return "manual_reconciliation_and_tool_hopping"


def _why_current_tools_fail(bottleneck: str, trust_failure_mode: str, workaround_pattern: str, tool_mode: str) -> str:
    """Summarize product failure as an operational gap rather than a demographic story."""
    bottleneck_label = _titleize(bottleneck, "workflow friction").lower()
    tool_label = _titleize(tool_mode, "current tools").lower()
    return (
        f"Current {tool_label} workflows do not remove {bottleneck_label}; "
        f"the team still relies on {workaround_pattern.replace('_', ' ')} because {trust_failure_mode.replace('_', ' ')}."
    )


def _why_this_persona_would_use_our_product(
    recurring_job_to_be_done: str,
    primary_bottleneck: str,
    expected_output_artifact: str,
    trust_failure_mode: str,
) -> str:
    """State the product reason in workflow terms."""
    return (
        f"They would adopt a product that helps them {recurring_job_to_be_done.replace('_', ' ')}, "
        f"eliminates {str(primary_bottleneck).replace('_', ' ')}, and reliably produces {expected_output_artifact.replace('_', ' ')} "
        f"without {trust_failure_mode.replace('_', ' ')}."
    )


def _activation_moment(typical_trigger_event: str, primary_bottleneck: str, trust_failure_mode: str) -> str:
    """Describe when product pull is strongest for this persona."""
    return (
        f"Activation happens when {typical_trigger_event.replace('_', ' ')} and "
        f"{str(primary_bottleneck).replace('_', ' ')} turns into {trust_failure_mode.replace('_', ' ')}."
    )


def _success_signal(recurring_job_to_be_done: str, expected_output_artifact: str, primary_bottleneck: str) -> str:
    """Describe success in operational terms."""
    return (
        f"Success means the team can {recurring_job_to_be_done.replace('_', ' ')}, produce {expected_output_artifact.replace('_', ' ')}, "
        f"and no longer depend on {str(primary_bottleneck).replace('_', ' ')} workarounds."
    )


def _persona_profile_name(user_role_family: str, recurring_job_to_be_done: str, functional_context: str) -> str:
    """Name the persona as a reusable operator profile rather than a pain fragment."""
    role_label = {
        "analyst_operator": "Analyst",
        "manager_operator": "Manager",
        "marketing_operator": "Marketing Operator",
        "business_operator": "Business Operator",
        "workflow_operator": "Workflow Operator",
    }.get(str(user_role_family).strip().lower(), "Workflow Operator")
    job_label = {
        "deliver_recurring_reporting_without_manual_repackaging": "Reporting Operator",
        "explain_metric_or_dashboard_changes_fast": "Performance Investigator",
        "validate_numbers_before_sharing_or_acting": "Metric Steward",
        "turn_repeated_analysis_work_into_repeatable_ops": "Workflow Automator",
        "move_analysis_work_to_a_shareable_output": "Analysis Operator",
    }.get(str(recurring_job_to_be_done).strip().lower(), _titleize(functional_context, "analysis operations"))
    return f"{role_label} {job_label}".strip()


def _summary_examples_lookup(selected_examples_df: pd.DataFrame) -> dict[str, list[str]]:
    """Build short representative-example lists keyed by persona id."""
    if selected_examples_df is None or selected_examples_df.empty:
        return {}
    frame = selected_examples_df.copy()
    frame["summary_text"] = frame.apply(
        lambda row: (
            f"[weak grounding fallback] {row.get('grounded_text', '')}"
            if str(row.get("selection_strength", "") or "") == "weak_grounding_fallback"
            else str(row.get("grounded_text", "") or "")
        ),
        axis=1,
    )
    return frame.groupby("persona_id")["summary_text"].apply(lambda values: list(values[:3])).to_dict()


def _naming_lookup(naming_df: pd.DataFrame) -> dict[str, str]:
    """Build persona-id to recommended cluster-name lookup."""
    if naming_df is None or naming_df.empty:
        return {}
    return naming_df.set_index("persona_id")["recommended_cluster_name"].to_dict()


def _titleize(value: str, fallback: str = "unassigned") -> str:
    """Humanize snake-style axis values."""
    text = str(value or "").strip()
    if not text or is_unknown_like(text):
        text = fallback
    return text.replace("_", " ").title()


def _is_promoted_candidate_persona(payload: dict[str, Any]) -> bool:
    """Return whether a cluster passed the base promotion gate before grounding review."""
    base_status = str(payload.get("base_promotion_status", payload.get("status", "")) or "")
    return base_status in {"promoted_candidate_persona", "promoted_persona"}


def _is_workbook_review_visible(payload: dict[str, Any]) -> bool:
    """Return whether a persona remains visible in the workbook's promoted set."""
    return str(payload.get("status", "") or "") in {"promoted_persona", "review_visible_persona"}


def _has_structural_support(payload: dict[str, Any]) -> bool:
    """Return whether a persona cleared structural support for final use."""
    return str(payload.get("structural_support_status", "structurally_supported") or "structurally_supported") in {
        "structurally_supported",
        "structurally_supported_broad_parent",
    }


def _is_final_usable_persona(payload: dict[str, Any]) -> bool:
    """Return whether a persona is usable for downstream reporting."""
    return _has_structural_support(payload) and str(payload.get("promotion_grounding_status", "") or "") == "promoted_and_grounded"


def _visibility_state(payload: dict[str, Any]) -> str:
    """Return the explicit review visibility state for one persona."""
    if str(payload.get("status", "") or "") == "review_visible_persona":
        return "review_visible_persona"
    if _is_promoted_candidate_persona(payload):
        return "promoted_candidate_persona"
    return str(payload.get("status", "exploratory_bucket") or "exploratory_bucket")


def _usability_state(payload: dict[str, Any]) -> str:
    """Return whether the persona is final usable under the current grounding policy."""
    return "final_usable_persona" if _is_final_usable_persona(payload) else "not_final_usable"


def _deck_readiness_state(payload: dict[str, Any]) -> str:
    """Return whether the persona is deck-ready under the current reporting policy."""
    return "deck_ready_persona" if _is_final_usable_persona(payload) else "not_deck_ready"


def _promotion_action(payload: dict[str, Any]) -> str:
    """Return the policy action implied by the current promotion-grounding outcome."""
    if _is_final_usable_persona(payload):
        return "remain_promoted"
    if str(payload.get("status", "") or "") == "review_visible_persona":
        return "remain_review_visible"
    if _is_promoted_candidate_persona(payload):
        return "promotion_candidate_pending_review"
    return "downgraded_to_exploratory"


def _reporting_readiness_status(payload: dict[str, Any]) -> str:
    """Return the downstream reporting readiness class for one persona."""
    if _is_final_usable_persona(payload):
        return "deck_ready_persona" if bool(payload.get("deck_ready_persona", True)) else "final_usable_persona"
    if _is_workbook_review_visible(payload):
        combined_status = str(payload.get("promotion_grounding_status", "") or "")
        if combined_status == "grounded_but_structurally_weak":
            return "grounded_but_structurally_weak"
        if combined_status == "promoted_but_weakly_grounded":
            return "promoted_but_weakly_grounded"
        if combined_status == "promoted_but_ungrounded":
            return "promoted_but_ungrounded"
        return "review_visible_persona"
    if _is_promoted_candidate_persona(payload):
        return "promoted_candidate_persona"
    return "not_final_usable"


def _build_persona_promotion_score_df(persona_source_df: pd.DataFrame, cluster_policy: dict[str, Any]) -> pd.DataFrame:
    """Build explicit promotion-score breakdown rows for every persona."""
    rows: list[dict[str, Any]] = []
    size_lookup = persona_source_df.groupby("persona_id")["episode_id"].nunique().to_dict() if not persona_source_df.empty else {}
    for persona_id, payload in dict(cluster_policy.get("status_by_persona", {})).items():
        rows.append(
            {
                "persona_id": str(persona_id),
                "persona_size": int(size_lookup.get(persona_id, 0) or 0),
                "base_promotion_status": payload.get("base_promotion_status", ""),
                "promotion_status": payload.get("status", ""),
                "promotion_action": _promotion_action(payload),
                "structural_stability_score": float(payload.get("structural_stability_score", 0.0) or 0.0),
                "grounding_quality_score": float(payload.get("grounding_quality_score", 0.0) or 0.0),
                "distinctiveness_score": float(payload.get("distinctiveness_score", 0.0) or 0.0),
                "actionability_score": float(payload.get("actionability_score", 0.0) or 0.0),
                "output_consistency_score": float(payload.get("output_consistency_score", 0.0) or 0.0),
                "cross_source_robustness_score": float(payload.get("cross_source_robustness_score", 0.0) or 0.0),
                "promotion_score": float(payload.get("promotion_score", 0.0) or 0.0),
                "product_value_proposition": payload.get("product_value_proposition", ""),
                "activation_moment": payload.get("activation_moment", ""),
                "ux_feature_need": payload.get("ux_feature_need", ""),
                "nearest_persona_id": payload.get("nearest_persona_id", ""),
                "strategic_redundancy_status": payload.get("strategic_redundancy_status", ""),
                "strategic_redundancy_reason": payload.get("strategic_redundancy_reason", ""),
                "promotion_reason": payload.get("reason", ""),
                "grounding_status": payload.get("grounding_status", ""),
                "promotion_grounding_status": payload.get("promotion_grounding_status", ""),
                "grounding_reason": payload.get("grounding_reason", ""),
            }
        )
    return pd.DataFrame(rows).sort_values(["promotion_score", "persona_id"], ascending=[False, True]).reset_index(drop=True) if rows else pd.DataFrame()


def _build_persona_promotion_path_debug_df(cluster_policy: dict[str, Any]) -> pd.DataFrame:
    """Build a compact debug table for the promotion-to-grounding failure path."""
    rows: list[dict[str, Any]] = []
    for persona_id, payload in dict(cluster_policy.get("status_by_persona", {})).items():
        base_promotion_status = str(payload.get("base_promotion_status", payload.get("status", "")) or "")
        promotion_status = str(payload.get("status", "") or "")
        if base_promotion_status not in {"promoted_candidate_persona", "promoted_persona"} and promotion_status not in {"promoted_persona", "review_visible_persona"}:
            continue
        rows.append(
            {
                "persona_id": str(persona_id),
                "base_promotion_status": base_promotion_status,
                "structural_support_status": str(payload.get("structural_support_status", "") or ""),
                "structural_support_fail_reason": _structural_support_fail_reason(payload),
                "grounding_status": str(payload.get("grounding_status", "") or ""),
                "promotion_grounding_status": str(payload.get("promotion_grounding_status", "") or ""),
                "grounding_fail_reason": _grounding_fail_reason(payload),
                "grounding_penalty_counted_separately": bool(_grounding_penalty_counted_separately(payload)),
                "structural_grounding_overlap": _structural_grounding_overlap(payload),
                "promotion_status": promotion_status,
                "final_usable_persona": bool(_is_final_usable_persona(payload)),
                "deck_ready_persona": bool(_is_final_usable_persona(payload)),
                "fail_reason": _promotion_fail_reason(payload),
            }
        )
    return pd.DataFrame(rows).sort_values(["base_promotion_status", "persona_id"], ascending=[False, True]).reset_index(drop=True) if rows else pd.DataFrame(
        columns=[
            "persona_id",
            "base_promotion_status",
            "structural_support_status",
            "structural_support_fail_reason",
            "grounding_status",
            "promotion_grounding_status",
            "grounding_fail_reason",
            "grounding_penalty_counted_separately",
            "structural_grounding_overlap",
            "promotion_status",
            "final_usable_persona",
            "deck_ready_persona",
            "fail_reason",
        ]
    )


def _build_structural_support_debug_df(
    persona_source_df: pd.DataFrame,
    cluster_policy: dict[str, Any],
    config: dict[str, Any],
) -> pd.DataFrame:
    """Export the exact structural support inputs, thresholds, and failed conditions for each persona."""
    robustness_config = dict(config.get("robustness", {}) or {})
    size_threshold = int(robustness_config.get("min_stable_cluster_size", config.get("clustering", {}).get("min_anchor_size", 24)))
    share_threshold = float(robustness_config.get("min_stable_cluster_share", 0.08))
    cohesion_threshold = float(robustness_config.get("sufficient_cohesion_floor", 0.9))
    separation_threshold = float(robustness_config.get("sufficient_separation_floor", 0.12))
    broad_parent_cohesion_threshold = float(robustness_config.get("merge_broadened_cohesion_floor", 0.8))
    broad_parent_separation_threshold = float(robustness_config.get("merge_broadened_separation_floor", max(separation_threshold, 0.2)))
    broad_parent_anchor_threshold = int(robustness_config.get("merge_broadened_min_anchor_count", 3))
    size_lookup = persona_source_df.groupby("persona_id")["episode_id"].nunique().to_dict() if not persona_source_df.empty and "episode_id" in persona_source_df.columns else {}
    source_count_lookup = persona_source_df.groupby("persona_id")["source"].nunique().to_dict() if not persona_source_df.empty and "source" in persona_source_df.columns else {}
    rows: list[dict[str, Any]] = []
    for persona_id, payload in dict(cluster_policy.get("status_by_persona", {})).items():
        cluster_size = int(size_lookup.get(persona_id, 0) or 0)
        share_of_core_labeled = float(str(payload.get("share", "0") or "0") or 0.0) / 100.0
        stability_status = str(payload.get("cluster_stability_status", "") or "")
        evidence_status = str(payload.get("cluster_evidence_status", "") or "")
        structural_support_status = str(payload.get("structural_support_status", "") or "")
        cluster_cohesion = float(payload.get("cluster_cohesion", 0.0) or 0.0)
        cluster_separation = float(payload.get("cluster_separation", 0.0) or 0.0)
        pre_merge_anchor_count = int(payload.get("pre_merge_anchor_count", 0) or 0)
        action_summary = str(payload.get("robustness_action_summary", "") or "")
        broadened_parent = any(token in action_summary for token in ["merged_overlap_persona", "merged_low_separation_cluster"])
        raw_structural_support_status = _raw_structural_support_status(
            stability_status=stability_status,
            cluster_cohesion=cluster_cohesion,
            cluster_separation=cluster_separation,
            pre_merge_anchor_count=pre_merge_anchor_count,
            broadened_parent=broadened_parent,
            cohesion_threshold=cohesion_threshold,
            separation_threshold=separation_threshold,
            broad_parent_cohesion_threshold=broad_parent_cohesion_threshold,
            broad_parent_separation_threshold=broad_parent_separation_threshold,
            broad_parent_anchor_threshold=broad_parent_anchor_threshold,
        )
        failed_conditions = _structural_failed_conditions(
            cluster_size=cluster_size,
            share_of_core_labeled=share_of_core_labeled,
            stability_status=stability_status,
            cluster_cohesion=cluster_cohesion,
            cluster_separation=cluster_separation,
            pre_merge_anchor_count=pre_merge_anchor_count,
            broadened_parent=broadened_parent,
            size_threshold=size_threshold,
            share_threshold=share_threshold,
            cohesion_threshold=cohesion_threshold,
            separation_threshold=separation_threshold,
            broad_parent_cohesion_threshold=broad_parent_cohesion_threshold,
            broad_parent_separation_threshold=broad_parent_separation_threshold,
            broad_parent_anchor_threshold=broad_parent_anchor_threshold,
        )
        rows.append(
            {
                "persona_id": str(persona_id),
                "raw_structural_support_status": raw_structural_support_status,
                "promotion_structural_support_status": structural_support_status,
                "promotion_gate_overrode_structural_status": bool(structural_support_status == "not_applicable" and raw_structural_support_status != "not_applicable"),
                "structural_support_reason": str(payload.get("structural_support_reason", "") or ""),
                "cluster_size": cluster_size,
                "min_stable_cluster_size_threshold": size_threshold,
                "cluster_size_pass": bool(cluster_size >= size_threshold),
                "share_of_core_labeled": round(share_of_core_labeled, 4),
                "min_stable_cluster_share_threshold": share_threshold,
                "cluster_share_pass": bool(share_of_core_labeled >= share_threshold),
                "stability_status": stability_status,
                "internal_coherence": cluster_cohesion,
                "sufficient_cohesion_threshold": cohesion_threshold,
                "cohesion_pass": bool(cluster_cohesion >= cohesion_threshold),
                "separation": cluster_separation,
                "sufficient_separation_threshold": separation_threshold,
                "separation_pass": bool(cluster_separation >= separation_threshold),
                "evidence_status": evidence_status,
                "pre_merge_anchor_count": pre_merge_anchor_count,
                "merge_broadened_min_anchor_count_threshold": broad_parent_anchor_threshold,
                "merge_anchor_pass": bool(pre_merge_anchor_count >= broad_parent_anchor_threshold),
                "merge_broadened_parent_detected": bool(broadened_parent),
                "merge_broadened_cohesion_threshold": broad_parent_cohesion_threshold,
                "merge_broadened_separation_threshold": broad_parent_separation_threshold,
                "source_diversity_count": int(source_count_lookup.get(persona_id, 0) or 0),
                "source_diversity_used": False,
                "representative_example_count": int(payload.get("selected_example_count", 0) or 0),
                "representative_examples_used": False,
                "grounded_candidate_count": int(payload.get("grounded_candidate_count", 0) or 0),
                "grounding_used": False,
                "merge_stability_used": True,
                "failed_conditions": " | ".join(failed_conditions),
                "failed_condition_count": int(len(failed_conditions)),
                "grounding_status": str(payload.get("grounding_status", "") or ""),
                "promotion_grounding_status": str(payload.get("promotion_grounding_status", "") or ""),
                "structural_grounding_overlap": _structural_grounding_overlap(payload),
                "grounding_penalty_counted_separately": bool(_grounding_penalty_counted_separately(payload)),
            }
        )
    return pd.DataFrame(rows).sort_values(["raw_structural_support_status", "persona_id"], ascending=[True, True]).reset_index(drop=True) if rows else pd.DataFrame()


def _build_structural_support_distribution_df(cluster_policy: dict[str, Any]) -> pd.DataFrame:
    """Export the current structural support status distribution."""
    status_values = [
        _raw_structural_support_status(
            stability_status=str(payload.get("cluster_stability_status", "") or ""),
            cluster_cohesion=float(payload.get("cluster_cohesion", 0.0) or 0.0),
            cluster_separation=float(payload.get("cluster_separation", 0.0) or 0.0),
            pre_merge_anchor_count=int(payload.get("pre_merge_anchor_count", 0) or 0),
            broadened_parent=any(token in str(payload.get("robustness_action_summary", "") or "") for token in ["merged_overlap_persona", "merged_low_separation_cluster"]),
            cohesion_threshold=0.9,
            separation_threshold=0.12,
            broad_parent_cohesion_threshold=0.8,
            broad_parent_separation_threshold=0.2,
            broad_parent_anchor_threshold=3,
        )
        for payload in dict(cluster_policy.get("status_by_persona", {})).values()
    ]
    if not status_values:
        return pd.DataFrame(columns=["structural_support_status", "persona_count"])
    distribution = pd.Series(status_values).value_counts().rename_axis("structural_support_status").reset_index(name="persona_count")
    return distribution.sort_values(["persona_count", "structural_support_status"], ascending=[False, True]).reset_index(drop=True)


def _promotion_fail_reason(payload: dict[str, Any]) -> str:
    """Return the exact condition that blocks final usability for one persona."""
    if _is_final_usable_persona(payload):
        return "final_usable"
    promotion_grounding_status = str(payload.get("promotion_grounding_status", "") or "")
    grounding_status = str(payload.get("grounding_status", "") or "")
    promotion_status = str(payload.get("status", "") or "")
    reasons: list[str] = []
    structural_fail_reason = _structural_support_fail_reason(payload)
    if structural_fail_reason:
        reasons.append(f"missing structural support: {structural_fail_reason}")
    grounding_fail_reason = _grounding_fail_reason(payload)
    if _grounding_penalty_counted_separately(payload) and (
        promotion_grounding_status in {"promoted_but_ungrounded", "promoted_but_weakly_grounded", "downgraded_due_to_no_grounding"}
        or grounding_status in {"ungrounded", "weak_bundle"}
    ):
        reasons.append(f"missing grounding: {grounding_fail_reason or 'no acceptable grounding evidence met policy'}")
    if promotion_status == "review_visible_persona" and promotion_grounding_status in {
        "promoted_but_ungrounded",
        "promoted_but_weakly_grounded",
        "grounded_but_structurally_weak",
        "review_visible_persona",
        "downgraded_due_to_no_grounding",
    }:
        reasons.append("downgraded by merge grounding policy")
    if not reasons:
        reasons.append("blocked by deck-ready rule")
    return " | ".join(reasons)


def _cluster_promotion_policy(
    persona_source_df: pd.DataFrame,
    total_labeled_records: int,
    cluster_robustness_df: pd.DataFrame | None = None,
    promotion_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify clusters as promoted personas or exploratory residual buckets."""
    sizes = persona_source_df.groupby("persona_id")["episode_id"].nunique().sort_values(ascending=False)
    min_cluster_size = persona_min_cluster_size(total_labeled_records)
    largest_share = round_pct(int(sizes.iloc[0]) if not sizes.empty else 0, total_labeled_records)
    single_cluster_dominance = is_single_cluster_dominant(largest_share)
    dominant_persona = str(sizes.index[0]) if not sizes.empty else ""
    config = dict(promotion_config or {})
    robustness_lookup = (
        cluster_robustness_df.set_index("persona_id").to_dict(orient="index")
        if cluster_robustness_df is not None and not cluster_robustness_df.empty and "persona_id" in cluster_robustness_df.columns
        else {}
    )
    profiles = _promotion_profiles(persona_source_df, robustness_lookup)
    status_by_persona: dict[str, dict[str, str]] = {}
    for persona_id, size in sizes.items():
        persona_key = str(persona_id)
        share = round_pct(int(size), total_labeled_records)
        robustness = dict(robustness_lookup.get(persona_key, {}) or {})
        stability_status = str(robustness.get("stability_status", "not_evaluated") or "not_evaluated")
        evidence_status = str(robustness.get("evidence_status", "not_evaluated") or "not_evaluated")
        structural_support_status = str(robustness.get("structural_support_status", "structurally_supported") or "structurally_supported")
        structural_support_reason = _structural_support_reason(robustness)
        structurally_supported = _has_structural_support({"structural_support_status": structural_support_status})
        scorecard = _promotion_scorecard(
            profile=profiles.get(persona_key, {}),
            size=int(size),
            min_cluster_size=min_cluster_size,
            total_labeled_records=total_labeled_records,
            config=config,
        )
        if int(size) < min_cluster_size:
            status = "exploratory_bucket"
            reason = f"sample size {int(size)} below min_cluster_size {min_cluster_size}"
        elif single_cluster_dominance and persona_key != dominant_persona:
            status = "exploratory_bucket"
            reason = f"residual cluster under single_cluster_dominance; largest cluster share {largest_share}%"
        elif not structurally_supported:
            status = "review_visible_persona"
            reason = (
                f"sample size {int(size)} meets min_cluster_size {min_cluster_size}; "
                f"review-visible because {structural_support_reason}"
            )
        elif str(scorecard.get("strategic_redundancy_status", "")) == "strategically_redundant":
            status = "review_visible_persona"
            reason = str(scorecard.get("strategic_redundancy_reason", "strategically redundant with a stronger neighboring persona") or "strategically redundant with a stronger neighboring persona")
        elif float(scorecard.get("pre_grounding_promotion_score", 0.0) or 0.0) < float(config.get("candidate_score_min", 0.58)):
            status = "exploratory_bucket"
            reason = (
                f"promotion score {float(scorecard.get('pre_grounding_promotion_score', 0.0) or 0.0):.3f} below candidate threshold "
                f"{float(config.get('candidate_score_min', 0.58)):.2f}"
            )
        elif float(scorecard.get("actionability_score", 0.0) or 0.0) < float(config.get("minimum_actionability_score", 0.55)):
            status = "review_visible_persona"
            reason = "review-visible because the persona does not yet imply a clear product strategy, activation moment, and UX need"
        elif float(scorecard.get("distinctiveness_score", 0.0) or 0.0) < float(config.get("minimum_distinctiveness_score", 0.18)):
            status = "review_visible_persona"
            reason = "review-visible because the persona is too close to a neighboring persona to justify separate promotion"
        elif float(scorecard.get("output_consistency_score", 0.0) or 0.0) < float(config.get("minimum_output_consistency_score", 0.55)):
            status = "review_visible_persona"
            reason = "review-visible because output expectations remain too mixed to support a stable product-facing persona"
        elif float(scorecard.get("cross_source_robustness_score", 0.0) or 0.0) < float(config.get("minimum_cross_source_robustness_score", 0.35)):
            status = "review_visible_persona"
            reason = "review-visible because evidence is too source-concentrated for a promoted product persona"
        elif float(scorecard.get("pre_grounding_promotion_score", 0.0) or 0.0) >= float(config.get("promote_score_min", 0.72)):
            status = "promoted_persona"
            reason = (
                f"promotion score {float(scorecard.get('pre_grounding_promotion_score', 0.0) or 0.0):.3f} clears promotion threshold "
                f"{float(config.get('promote_score_min', 0.72)):.2f} with structurally supported and product-actionable evidence"
            )
        else:
            status = "review_visible_persona"
            reason = "review-visible because the persona is structurally sound but not yet strong enough on the combined promotion score"
        base_status = "promoted_candidate_persona" if status in {"promoted_persona", "review_visible_persona"} else status
        status_by_persona[persona_key] = {
            "status": status,
            "reason": reason,
            "share": str(share),
            "base_promotion_status": base_status,
            "structural_support_status": structural_support_status if status != "exploratory_bucket" else "not_applicable",
            "structural_support_reason": structural_support_reason if status != "exploratory_bucket" else "cluster did not clear the promotion size gate",
            "cluster_stability_status": stability_status,
            "cluster_evidence_status": evidence_status,
            "cluster_concentration_status": str(robustness.get("concentration_status", "") or ""),
            "tail_fragility_status": str(robustness.get("tail_fragility_status", "") or ""),
            "cluster_cohesion": float(robustness.get("cohesion", 0.0) or 0.0),
            "cluster_separation": float(robustness.get("separation", 0.0) or 0.0),
            "nearest_neighbor_similarity": float(robustness.get("nearest_neighbor_similarity", 0.0) or 0.0),
            "pre_merge_anchor_count": int(robustness.get("pre_merge_anchor_count", 0) or 0),
            "robustness_action_summary": str(robustness.get("robustness_action_summary", "") or ""),
            "structural_stability_score": float(scorecard.get("structural_stability_score", 0.0) or 0.0),
            "grounding_quality_score": 0.0,
            "distinctiveness_score": float(scorecard.get("distinctiveness_score", 0.0) or 0.0),
            "actionability_score": float(scorecard.get("actionability_score", 0.0) or 0.0),
            "output_consistency_score": float(scorecard.get("output_consistency_score", 0.0) or 0.0),
            "cross_source_robustness_score": float(scorecard.get("cross_source_robustness_score", 0.0) or 0.0),
            "pre_grounding_promotion_score": float(scorecard.get("pre_grounding_promotion_score", 0.0) or 0.0),
            "promotion_score": float(scorecard.get("pre_grounding_promotion_score", 0.0) or 0.0),
            "product_value_proposition": str(scorecard.get("product_value_proposition", "") or ""),
            "activation_moment": str(scorecard.get("activation_moment", "") or ""),
            "ux_feature_need": str(scorecard.get("ux_feature_need", "") or ""),
            "nearest_persona_id": str(scorecard.get("nearest_persona_id", "") or ""),
            "strategic_redundancy_status": str(scorecard.get("strategic_redundancy_status", "") or ""),
            "strategic_redundancy_reason": str(scorecard.get("strategic_redundancy_reason", "") or ""),
            "grounding_status": "not_evaluated" if base_status == "promoted_candidate_persona" else "not_applicable",
            "promotion_grounding_status": "promotion_pending_grounding_review" if base_status == "promoted_candidate_persona" else status,
            "grounding_reason": "",
        }
    return {
        "min_cluster_size": min_cluster_size,
        "largest_share": largest_share,
        "single_cluster_dominance": single_cluster_dominance,
        "status_by_persona": status_by_persona,
        "promoted_count": sum(1 for value in status_by_persona.values() if value["status"] == "promoted_persona"),
        "exploratory_count": sum(1 for value in status_by_persona.values() if value["status"] != "promoted_persona"),
    }


def _promotion_profiles(
    persona_source_df: pd.DataFrame,
    robustness_lookup: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    if persona_source_df.empty:
        return profiles
    for persona_id, group in persona_source_df.groupby("persona_id"):
        persona_key = str(persona_id)
        source_counts = group.groupby("source")["episode_id"].nunique().sort_values(ascending=False) if "source" in group.columns else pd.Series(dtype=float)
        robustness = dict(robustness_lookup.get(persona_key, {}) or {})
        profiles[persona_key] = {
            "persona_id": persona_key,
            "size": int(group["episode_id"].nunique()),
            "role_terms": _top_terms(_series_from_candidates(group, ["role_context", "user_role", "role_clue"])),
            "work_terms": _top_terms(_series_from_candidates(group, ["work_loop", "workflow_stage", "analysis_goal", "work_moment"])),
            "output_terms": _top_terms(_series_from_candidates(group, ["expected_output", "desired_output", "output_mode", "expected_output_artifact"])),
            "bottleneck_terms": _top_terms(_series_from_candidates(group, ["bottleneck_pattern", "bottleneck_type", "bottleneck_text"])),
            "workaround_terms": _top_terms(_series_from_candidates(group, ["current_workaround", "workaround_pattern", "workaround_text"])),
            "trust_terms": _top_terms(_series_from_candidates(group, ["trust_failure", "trust_failure_mode", "business_question"])),
            "product_terms": _top_terms(_series_from_candidates(group, ["product_relevance", "solution_type", "tool_env", "desired_output"])),
            "solution_terms": _top_terms(_series_from_candidates(group, ["solution_type", "tool_env"])),
            "source_count": int(source_counts.size),
            "primary_source_share": float(source_counts.iloc[0] / max(1, int(source_counts.sum()))) if not source_counts.empty else 1.0,
            "nearest_persona_id": str(robustness.get("nearest_neighbor_id", "") or ""),
            "nearest_neighbor_similarity": float(robustness.get("nearest_neighbor_similarity", 0.0) or 0.0),
            "separation": float(robustness.get("separation", 0.0) or 0.0),
            "robustness": robustness,
        }
    return profiles


def _series_from_candidates(frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """Return the first populated series from a list of candidate column names."""
    for column in candidates:
        if column not in frame.columns:
            continue
        series = frame[column]
        if series.dropna().astype(str).str.strip().ne("").any():
            return series
    return pd.Series(dtype=str)


def _top_terms(series: pd.Series, limit: int = 4) -> list[str]:
    terms: list[str] = []
    if series is None:
        return terms
    for raw_value in series.dropna().astype(str):
        parts = [part.strip().lower() for part in re.split(r"[|;/,]", raw_value) if part.strip()]
        terms.extend(parts)
    if not terms:
        return []
    counts = pd.Series(terms).value_counts()
    return [str(idx) for idx in counts.index[:limit]]


def _promotion_scorecard(
    profile: dict[str, Any],
    size: int,
    min_cluster_size: int,
    total_labeled_records: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    structural_stability = _structural_stability_score(profile, size, min_cluster_size, total_labeled_records)
    distinctiveness = _distinctiveness_score(profile)
    actionability, product_value_proposition, activation_moment, ux_feature_need = _actionability_scorecard(profile)
    output_consistency = _output_consistency_score(profile)
    cross_source_robustness = _cross_source_robustness_score(profile)
    weights = dict(config.get("weights", {}) or {})
    component_scores = {
        "structural_stability": structural_stability,
        "distinctiveness": distinctiveness,
        "actionability": actionability,
        "output_consistency": output_consistency,
        "cross_source_robustness": cross_source_robustness,
    }
    weight_total = sum(float(weights.get(key, 0.0) or 0.0) for key in component_scores)
    if weight_total <= 0.0:
        weight_total = float(len(component_scores))
        weights = {key: 1.0 for key in component_scores}
    pre_grounding_promotion_score = sum(
        component_scores[key] * float(weights.get(key, 0.0) or 0.0)
        for key in component_scores
    ) / weight_total
    strategic_redundancy_status, strategic_redundancy_reason = _strategic_redundancy_status(profile, config)
    return {
        "structural_stability_score": structural_stability,
        "distinctiveness_score": distinctiveness,
        "actionability_score": actionability,
        "output_consistency_score": output_consistency,
        "cross_source_robustness_score": cross_source_robustness,
        "pre_grounding_promotion_score": float(pre_grounding_promotion_score),
        "product_value_proposition": product_value_proposition,
        "activation_moment": activation_moment,
        "ux_feature_need": ux_feature_need,
        "nearest_persona_id": str(profile.get("nearest_persona_id", "") or ""),
        "strategic_redundancy_status": strategic_redundancy_status,
        "strategic_redundancy_reason": strategic_redundancy_reason,
    }


def _structural_stability_score(
    profile: dict[str, Any],
    size: int,
    min_cluster_size: int,
    total_labeled_records: int,
) -> float:
    robustness = dict(profile.get("robustness", {}) or {})
    size_ratio = min(1.0, float(size) / max(1.0, float(min_cluster_size)))
    share_ratio = min(1.0, float(size) / max(1.0, float(total_labeled_records) * 0.2))
    anchor_ratio = min(1.0, float(robustness.get("pre_merge_anchor_count", 0) or 0) / 4.0)
    robustness_ratio = min(1.0, max(0.0, float(robustness.get("robustness_score", 0.0) or 0.0)))
    separation_ratio = min(1.0, max(0.0, float(profile.get("separation", 0.0) or 0.0) / 0.35))
    return float(np.mean([size_ratio, share_ratio, anchor_ratio, robustness_ratio, separation_ratio]))


def _distinctiveness_score(profile: dict[str, Any]) -> float:
    nearest_similarity = float(profile.get("nearest_neighbor_similarity", 0.0) or 0.0)
    role_density = min(1.0, len(profile.get("role_terms", [])) / 3.0)
    work_density = min(1.0, len(profile.get("work_terms", [])) / 3.0)
    bottleneck_density = min(1.0, len(profile.get("bottleneck_terms", [])) / 3.0)
    return float(np.mean([1.0 - min(1.0, nearest_similarity), role_density, work_density, bottleneck_density]))


def _actionability_scorecard(profile: dict[str, Any]) -> tuple[float, str, str, str]:
    role_terms = list(profile.get("role_terms", []))
    work_terms = list(profile.get("work_terms", []))
    output_terms = list(profile.get("output_terms", []))
    bottleneck_terms = list(profile.get("bottleneck_terms", []))
    product_terms = list(profile.get("product_terms", []))
    workaround_terms = list(profile.get("workaround_terms", []))
    trust_terms = list(profile.get("trust_terms", []))
    if role_terms and output_terms and bottleneck_terms:
        value_prop = f"Help {role_terms[0]} deliver {output_terms[0]} without {bottleneck_terms[0]}"
    elif role_terms and work_terms:
        value_prop = f"Support {role_terms[0]} through the {work_terms[0]} workflow"
    else:
        value_prop = ""
    if work_terms and bottleneck_terms:
        activation_moment = f"When {work_terms[0]} is blocked by {bottleneck_terms[0]}"
    elif bottleneck_terms:
        activation_moment = f"When {bottleneck_terms[0]} becomes the limiting step"
    else:
        activation_moment = ""
    ux_signals = [term for term in [product_terms[0] if product_terms else "", workaround_terms[0] if workaround_terms else "", trust_terms[0] if trust_terms else ""] if term]
    ux_feature_need = "; ".join(ux_signals[:2]) if ux_signals else ""
    return (
        float(np.mean([
            1.0 if value_prop else 0.0,
            1.0 if activation_moment else 0.0,
            1.0 if ux_feature_need else 0.0,
            min(1.0, len(product_terms) / 2.0),
        ])),
        value_prop,
        activation_moment,
        ux_feature_need,
    )


def _output_consistency_score(profile: dict[str, Any]) -> float:
    scores = [
        min(1.0, len(profile.get("output_terms", [])) / 2.0),
        min(1.0, len(profile.get("work_terms", [])) / 2.0),
        1.0 if profile.get("workaround_terms") else 0.0,
        1.0 if profile.get("trust_terms") else 0.0,
    ]
    return float(np.mean(scores))


def _cross_source_robustness_score(profile: dict[str, Any]) -> float:
    source_count = int(profile.get("source_count", 0) or 0)
    primary_source_share = float(profile.get("primary_source_share", 1.0) or 1.0)
    return float(np.mean([
        min(1.0, source_count / 3.0),
        1.0 - min(1.0, primary_source_share),
    ]))


def _strategic_redundancy_status(profile: dict[str, Any], config: dict[str, Any]) -> tuple[str, str]:
    nearest_persona_id = str(profile.get("nearest_persona_id", "") or "")
    nearest_similarity = float(profile.get("nearest_neighbor_similarity", 0.0) or 0.0)
    redundancy_similarity_ceiling = float(config.get("redundancy_similarity_ceiling", 0.82))
    if nearest_persona_id and nearest_similarity >= redundancy_similarity_ceiling:
        return "strategically_redundant", f"too similar to {nearest_persona_id} for separate promotion"
    if nearest_persona_id:
        return "distinct_enough", f"nearest persona {nearest_persona_id} remains sufficiently differentiated"
    return "not_evaluated", "nearest persona not available"


def _grounding_quality_score(payload: dict[str, Any]) -> float:
    grounding_status = str(payload.get("grounding_status", "") or "")
    promotion_grounding_status = str(payload.get("promotion_grounding_status", "") or "")
    bundle_episode_count = int(payload.get("bundle_episode_count", 0) or 0)
    selected_example_count = int(payload.get("selected_example_count", 0) or 0)
    status_floor = {
        "grounded_bundle": 1.0,
        "grounded_quote": 0.8,
        "grounded_candidate": 0.8,
        "weak_bundle": 0.6,
        "weak_quote": 0.45,
        "promoted_but_weakly_grounded": 0.45,
        "promoted_but_ungrounded": 0.15,
        "promotion_pending_grounding_review": 0.15,
        "not_evaluated": 0.0,
        "not_applicable": 0.0,
    }
    status_score = max(status_floor.get(grounding_status, 0.0), status_floor.get(promotion_grounding_status, 0.0))
    return float(np.mean([
        status_score,
        min(1.0, bundle_episode_count / 3.0),
        min(1.0, selected_example_count / 3.0),
    ]))


def _merge_grounding_policy(
    cluster_policy: dict[str, Any],
    persona_grounding_df: pd.DataFrame,
    config: dict[str, Any],
    promotion_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge explicit grounding outcomes into the promotion policy map."""
    merged = dict(cluster_policy)
    status_by_persona = {key: dict(value) for key, value in dict(cluster_policy.get("status_by_persona", {})).items()}
    grounding_lookup = (
        persona_grounding_df.set_index("persona_id").to_dict(orient="index")
        if persona_grounding_df is not None and not persona_grounding_df.empty and "persona_id" in persona_grounding_df.columns
        else {}
    )
    score_config = dict(promotion_config or {})
    weights = dict(score_config.get("weights", {}) or {})
    exploratory_status = str(config.get("policy", {}).get("promotion_grounding", {}).get("exploratory_status", "exploratory_bucket"))
    downgraded_status = str(config.get("policy", {}).get("promotion_grounding", {}).get("downgraded_due_to_no_grounding_status", "downgraded_due_to_no_grounding"))
    for persona_id, payload in status_by_persona.items():
        base_status = str(payload.get("base_promotion_status", payload.get("status", exploratory_status)) or exploratory_status)
        grounding = dict(grounding_lookup.get(str(persona_id), {}) or {})
        if base_status not in {"promoted_persona", "promoted_candidate_persona"}:
            payload["grounding_status"] = "not_applicable"
            payload["promotion_grounding_status"] = exploratory_status
            payload["grounding_reason"] = "grounding coverage is only enforced for promoted personas"
            continue
        structurally_supported = _has_structural_support(payload)
        combined_status = str(grounding.get("promotion_grounding_status", "promoted_but_ungrounded") or "promoted_but_ungrounded")
        payload["grounding_status"] = str(grounding.get("grounding_status", "ungrounded") or "ungrounded")
        payload["promotion_grounding_status"] = combined_status
        payload["grounding_reason"] = str(grounding.get("grounding_reason", "") or "")
        payload["grounded_candidate_count"] = int(grounding.get("grounded_candidate_count", 0) or 0)
        payload["weak_candidate_count"] = int(grounding.get("weak_candidate_count", 0) or 0)
        payload["context_evidence_count"] = int(grounding.get("context_evidence_count", 0) or 0)
        payload["workaround_evidence_count"] = int(grounding.get("workaround_evidence_count", 0) or 0)
        payload["trust_validation_evidence_count"] = int(grounding.get("trust_validation_evidence_count", 0) or 0)
        payload["bundle_episode_count"] = int(grounding.get("bundle_episode_count", 0) or 0)
        payload["bundle_dimension_hits"] = int(grounding.get("bundle_dimension_hits", 0) or 0)
        payload["total_bundle_strength"] = int(grounding.get("total_bundle_strength", 0) or 0)
        payload["bundle_grounding_status"] = str(grounding.get("bundle_grounding_status", "") or "")
        payload["bundle_grounding_reason"] = str(grounding.get("bundle_grounding_reason", "") or "")
        payload["bundle_support_examples"] = str(grounding.get("bundle_support_examples", "") or "")
        payload["selected_example_count"] = int(grounding.get("selected_example_count", 0) or 0)
        payload["fallback_selected_count"] = int(grounding.get("fallback_selected_count", 0) or 0)
        payload["grounding_quality_score"] = _grounding_quality_score(payload)
        weighted_sum = (
            float(payload.get("structural_stability_score", 0.0) or 0.0) * float(weights.get("structural_stability", 1.0) or 1.0)
            + float(payload.get("grounding_quality_score", 0.0) or 0.0) * float(weights.get("grounding_quality", 1.0) or 1.0)
            + float(payload.get("distinctiveness_score", 0.0) or 0.0) * float(weights.get("distinctiveness", 1.0) or 1.0)
            + float(payload.get("actionability_score", 0.0) or 0.0) * float(weights.get("actionability", 1.0) or 1.0)
            + float(payload.get("output_consistency_score", 0.0) or 0.0) * float(weights.get("output_consistency", 1.0) or 1.0)
            + float(payload.get("cross_source_robustness_score", 0.0) or 0.0) * float(weights.get("cross_source_robustness", 1.0) or 1.0)
        )
        weight_total = sum(
            float(weights.get(key, 1.0) or 1.0)
            for key in [
                "structural_stability",
                "grounding_quality",
                "distinctiveness",
                "actionability",
                "output_consistency",
                "cross_source_robustness",
            ]
        )
        payload["promotion_score"] = float(weighted_sum / max(weight_total, 1e-9))
        if _grounding_is_derivative_of_structural_weakness(payload, combined_status):
            payload["status"] = "review_visible_persona"
            payload["promotion_grounding_status"] = "structurally_weak_primary_blocker"
            payload["reason"] = f"{payload.get('reason', '')}; review-visible because structural fragility is the primary blocker and weak grounding appears to come from the same low-volume scarcity".strip("; ")
        elif combined_status == downgraded_status:
            payload["status"] = exploratory_status
            payload["reason"] = f"{payload.get('reason', '')}; downgraded because no acceptable grounding evidence met policy".strip("; ")
        elif str(payload.get("strategic_redundancy_status", "")) == "strategically_redundant":
            payload["status"] = "review_visible_persona"
            payload["promotion_grounding_status"] = "review_visible_persona"
            payload["reason"] = str(payload.get("strategic_redundancy_reason", "strategically redundant with a stronger neighboring persona") or "strategically redundant with a stronger neighboring persona")
        elif not structurally_supported and combined_status == "promoted_and_grounded":
            payload["status"] = "review_visible_persona"
            payload["promotion_grounding_status"] = "grounded_but_structurally_weak"
            payload["reason"] = f"{payload.get('reason', '')}; review-visible because cluster robustness remains structurally weak".strip("; ")
        elif combined_status == "promoted_but_ungrounded":
            payload["status"] = "review_visible_persona"
            payload["reason"] = f"{payload.get('reason', '')}; review-visible because no acceptable grounding evidence met policy".strip("; ")
        elif combined_status == "promoted_but_weakly_grounded":
            payload["status"] = "review_visible_persona"
            payload["reason"] = f"{payload.get('reason', '')}; review-visible because only weak fallback evidence met policy".strip("; ")
        elif not structurally_supported:
            payload["status"] = "review_visible_persona"
            payload["reason"] = f"{payload.get('reason', '')}; review-visible because cluster robustness remains structurally weak".strip("; ")
        elif float(payload.get("promotion_score", 0.0) or 0.0) < float(score_config.get("promote_score_min", 0.72)):
            payload["status"] = "review_visible_persona"
            payload["promotion_grounding_status"] = "review_visible_persona"
            payload["reason"] = (
                f"{payload.get('reason', '')}; review-visible because final promotion score {float(payload.get('promotion_score', 0.0) or 0.0):.3f} "
                f"is below {float(score_config.get('promote_score_min', 0.72)):.2f} after grounding quality is applied"
            ).strip("; ")
        else:
            payload["status"] = "promoted_persona"
    merged["status_by_persona"] = status_by_persona
    merged["promoted_count"] = sum(1 for value in status_by_persona.values() if value.get("status") == "promoted_persona")
    merged["exploratory_count"] = sum(1 for value in status_by_persona.values() if value.get("status") != "promoted_persona")
    return merged


def _structural_support_reason(robustness: dict[str, Any]) -> str:
    """Explain why a cluster is or is not structurally supported for persona promotion."""
    structural_support_status = str(robustness.get("structural_support_status", "structurally_supported") or "structurally_supported")
    if structural_support_status == "structurally_supported":
        return "cluster robustness clears stability, evidence, and separation checks"
    if structural_support_status == "structurally_supported_broad_parent":
        return "stable merge-broadened parent clears separation and anchor checks despite a slightly lower cohesion score"
    stability_status = str(robustness.get("stability_status", "") or "")
    evidence_status = str(robustness.get("evidence_status", "") or "")
    cohesion = float(robustness.get("cohesion", 0.0) or 0.0)
    separation = float(robustness.get("separation", 0.0) or 0.0)
    action_summary = str(robustness.get("robustness_action_summary", "") or "")
    if stability_status in {"fragile", "micro"}:
        return f"cluster remains {stability_status} after robustness merging"
    if evidence_status == "thin":
        if separation < 0.12:
            return f"cluster separation {separation:.4f} remains below the standalone support floor"
        if "merged_" in action_summary:
            return f"stable merged parent keeps strong separation {separation:.4f} but cohesion {cohesion:.4f} remains below the standalone support floor"
        return f"cluster cohesion {cohesion:.4f} remains below the standalone support floor"
    return "cluster robustness remains below the standalone support threshold"


def _structural_support_fail_reason(payload: dict[str, Any]) -> str:
    """Return the structural blocker only when the persona lacks final structural support."""
    return "" if _has_structural_support(payload) else str(payload.get("structural_support_reason", "") or "")


def _grounding_fail_reason(payload: dict[str, Any]) -> str:
    """Return the grounding blocker only when grounding still blocks promotion."""
    promotion_grounding_status = str(payload.get("promotion_grounding_status", "") or "")
    grounding_status = str(payload.get("grounding_status", "") or "")
    if promotion_grounding_status == "promoted_and_grounded":
        return ""
    if grounding_status == "not_evaluated":
        return "grounding review was skipped before representative evidence could be evaluated"
    return str(payload.get("grounding_reason", "") or "")


def _grounding_is_derivative_of_structural_weakness(payload: dict[str, Any], combined_status: str) -> bool:
    """Return whether weak grounding is likely caused by the same low-volume fragility that already failed structure."""
    if _has_structural_support(payload):
        return False
    if str(payload.get("cluster_stability_status", "") or "") not in {"fragile", "micro"}:
        return False
    return combined_status in {"promoted_but_ungrounded", "promoted_but_weakly_grounded", "downgraded_due_to_no_grounding"}


def _grounding_penalty_counted_separately(payload: dict[str, Any]) -> bool:
    """Return whether grounding should remain an independent blocker next to structure."""
    combined_status = str(payload.get("promotion_grounding_status", "") or "")
    return combined_status != "structurally_weak_primary_blocker"


def _structural_grounding_overlap(payload: dict[str, Any]) -> str:
    """Classify whether structural and grounding weakness are independent or likely the same scarcity signal."""
    if _has_structural_support(payload):
        return "no_structural_overlap"
    if _grounding_is_derivative_of_structural_weakness(payload, str(payload.get("promotion_grounding_status", "") or "")):
        return "shared_low_volume_scarcity"
    grounding_status = str(payload.get("grounding_status", "") or "")
    if grounding_status in {"ungrounded", "weak_bundle", "grounded_single", "grounded_bundle"}:
        return "largely_independent_from_grounding"
    return "not_evaluated"


def _structural_failed_conditions(
    cluster_size: int,
    share_of_core_labeled: float,
    stability_status: str,
    cluster_cohesion: float,
    cluster_separation: float,
    pre_merge_anchor_count: int,
    broadened_parent: bool,
    size_threshold: int,
    share_threshold: float,
    cohesion_threshold: float,
    separation_threshold: float,
    broad_parent_cohesion_threshold: float,
    broad_parent_separation_threshold: float,
    broad_parent_anchor_threshold: int,
) -> list[str]:
    """Return the exact structural conditions that failed for a persona."""
    failures: list[str] = []
    stable_by_size_or_share = cluster_size >= size_threshold or share_of_core_labeled >= share_threshold
    if not stable_by_size_or_share:
        failures.append("stability gate: cluster size/share below stable floor")
    if cluster_cohesion < cohesion_threshold:
        failures.append("evidence gate: cohesion below sufficient floor")
    if cluster_separation < separation_threshold:
        failures.append("evidence gate: separation below sufficient floor")
    if stable_by_size_or_share and cluster_cohesion < cohesion_threshold and cluster_separation >= separation_threshold:
        if not broadened_parent:
            failures.append("broad-parent relief unavailable: persona was not merge-broadened")
        if pre_merge_anchor_count < broad_parent_anchor_threshold:
            failures.append("broad-parent relief unavailable: pre-merge anchor count below floor")
        if cluster_cohesion < broad_parent_cohesion_threshold:
            failures.append("broad-parent relief unavailable: cohesion below broadened-parent floor")
        if cluster_separation < broad_parent_separation_threshold:
            failures.append("broad-parent relief unavailable: separation below broadened-parent floor")
    if stability_status in {"fragile", "micro"}:
        failures.append(f"cluster classified as {stability_status}")
    return list(dict.fromkeys(failures))


def _raw_structural_support_status(
    stability_status: str,
    cluster_cohesion: float,
    cluster_separation: float,
    pre_merge_anchor_count: int,
    broadened_parent: bool,
    cohesion_threshold: float,
    separation_threshold: float,
    broad_parent_cohesion_threshold: float,
    broad_parent_separation_threshold: float,
    broad_parent_anchor_threshold: int,
) -> str:
    """Recompute structural support independent of later promotion gating."""
    if stability_status == "stable" and cluster_cohesion >= cohesion_threshold and cluster_separation >= separation_threshold:
        return "structurally_supported"
    if (
        stability_status == "stable"
        and broadened_parent
        and pre_merge_anchor_count >= broad_parent_anchor_threshold
        and cluster_separation >= broad_parent_separation_threshold
        and cluster_cohesion >= broad_parent_cohesion_threshold
    ):
        return "structurally_supported_broad_parent"
    if stability_status:
        return "review_visible_only"
    return "not_applicable"


def _archetype_name(role: str, workflow: str, bottleneck: str, goal: str, output_mode: str, promotion_status: str) -> str:
    """Name personas as archetype plus recurring job plus blocker."""
    role_word = {
        "analyst": "Analyst",
        "manager": "Operator",
        "marketer": "Marketing Operator",
        "business_user": "Business Operator",
    }.get(str(role).strip().lower(), "Workflow Operator")
    job_word = {
        "reporting": "Reporting",
        "validation": "Metric Validation",
        "triage": "Dashboard Triage",
        "automation": "Automation",
    }.get(str(workflow).strip().lower(), _titleize(goal, "Workflow"))
    blocker_word = {
        "manual_reporting": "Blocked by Spreadsheet Rework",
        "data_quality": "Blocked by Number Reconciliation",
        "tool_limitation": "Blocked by Tool Limits",
        "handoff_dependency": "Blocked by Explanation Gaps",
        "general_friction": "Blocked by Workflow Friction",
    }.get(str(bottleneck).strip().lower(), f"Blocked by {_titleize(bottleneck, 'Workflow Friction')}")
    name = f"{role_word} {job_word} {blocker_word}"
    if promotion_status == "review_visible_persona":
        return f"Review-Visible {name}"
    if promotion_status != "promoted_persona":
        return f"Exploratory {name}"
    return name
