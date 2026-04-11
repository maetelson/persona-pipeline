"""Axis-based persona clustering and report-ready persona tables."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

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
from src.utils.io import ensure_dir


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
    example_config = load_example_selection_config(Path(__file__).resolve().parents[2])
    cluster_policy = _cluster_promotion_policy(persona_source_df, total_labeled_records)
    grounding_outputs = apply_promotion_grounding_policy(
        selected_df=cluster_outputs["selected_examples_df"],
        audit_df=cluster_outputs["example_audit_df"],
        promoted_persona_ids=[
            persona_id
            for persona_id, payload in cluster_policy["status_by_persona"].items()
            if str(payload.get("status", "") or "") == "promoted_persona"
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

    outputs = {
        "overview_df": pd.DataFrame(columns=["metric", "value"]),
        "persona_summary_df": persona_summary_df,
        "persona_axes_df": persona_axes_df,
        "persona_pains_df": persona_pains_df,
        "persona_cooccurrence_df": persona_cooccurrence_df,
        "persona_examples_df": persona_examples_df,
        "cluster_stats_df": cluster_stats_df,
        "quality_checks_df": pd.DataFrame(columns=["metric", "value", "threshold", "status", "level", "denominator_type", "denominator_value", "notes"]),
        "persona_assignments_df": persona_assignments_df,
        "persona_grounding_df": grounding_outputs["persona_grounding_df"],
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
        "cluster_naming_recommendations_df": cluster_outputs["cluster_naming_recommendations_df"],
        "old_vs_new_cluster_summary_df": cluster_outputs["old_vs_new_cluster_summary_df"],
        "bottleneck_feature_importance_df": cluster_outputs["bottleneck_feature_importance_df"],
        "role_feature_importance_before_after_df": cluster_outputs["role_feature_importance_before_after_df"],
        "cluster_comparison_before_after_df": cluster_outputs["cluster_comparison_before_after_df"],
        "cluster_comparison_before_after_md": cluster_outputs["cluster_comparison_before_after_md"],
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
        "cluster_stats_csv": output_dir / "cluster_stats.csv",
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
        "cluster_naming_recommendations_csv": output_dir / "cluster_naming_recommendations.csv",
        "old_vs_new_cluster_summary_csv": output_dir / "old_vs_new_cluster_summary.csv",
        "bottleneck_feature_importance_csv": output_dir / "bottleneck_feature_importance.csv",
        "role_feature_importance_before_after_csv": output_dir / "role_feature_importance_before_after.csv",
        "cluster_comparison_before_after_csv": output_dir / "cluster_comparison_before_after.csv",
        "cluster_comparison_before_after_md": output_dir / "cluster_comparison_before_after.md",
    }
    outputs["persona_summary_df"].to_csv(paths["persona_summary_csv"], index=False)
    outputs["persona_axes_df"].to_csv(paths["persona_axes_csv"], index=False)
    outputs["persona_pains_df"].to_csv(paths["persona_pains_csv"], index=False)
    outputs["persona_cooccurrence_df"].to_csv(paths["persona_cooccurrence_csv"], index=False)
    outputs["persona_examples_df"].to_csv(paths["persona_examples_csv"], index=False)
    outputs["persona_grounding_df"].to_csv(paths["persona_grounding_csv"], index=False)
    outputs["cluster_stats_df"].to_csv(paths["cluster_stats_csv"], index=False)
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
    outputs["cluster_naming_recommendations_df"].to_csv(paths["cluster_naming_recommendations_csv"], index=False)
    outputs["old_vs_new_cluster_summary_df"].to_csv(paths["old_vs_new_cluster_summary_csv"], index=False)
    outputs["bottleneck_feature_importance_df"].to_csv(paths["bottleneck_feature_importance_csv"], index=False)
    outputs["role_feature_importance_before_after_df"].to_csv(paths["role_feature_importance_before_after_csv"], index=False)
    outputs["cluster_comparison_before_after_df"].to_csv(paths["cluster_comparison_before_after_csv"], index=False)
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
        cluster_name = naming_lookup.get(str(persona_id), str(group.get("cluster_name", pd.Series([persona_id])).iloc[0]))
        promotion = cluster_policy["status_by_persona"].get(str(persona_id), {})
        persona_name = _archetype_name(role, workflow, bottleneck, goal, output_mode, promotion.get("status", "exploratory_bucket"))
        rows.append(
            {
                "persona_id": persona_id,
                "persona_name": persona_name,
                "persona_size": persona_size,
                "share_of_core_labeled": round_pct(persona_size, persona_core_labeled_records),
                "share_of_all_labeled": round_pct(persona_size, total_labeled_records),
                "denominator_type": DENOMINATOR_PERSONA_CORE_LABELED_ROWS,
                "denominator_value": persona_core_labeled_records,
                "min_cluster_size": int(cluster_policy["min_cluster_size"]),
                "base_promotion_status": promotion.get("base_promotion_status", promotion.get("status", "exploratory_bucket")),
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
                "selected_example_count": int(promotion.get("selected_example_count", 0) or 0),
                "fallback_selected_count": int(promotion.get("fallback_selected_count", 0) or 0),
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
                "top_pain_points": " | ".join(_top_themes(group, ["pain_codes", "question_codes"], limit=4)),
                "representative_examples": " | ".join(summary_examples_lookup.get(str(persona_id), [])),
                "why_this_persona_matters": _why_persona_matters(group, bottleneck, goal, output_mode),
                "legacy_cluster_name": cluster_name,
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
                "selected_example_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("selected_example_count", 0) or 0),
                "fallback_selected_count": int(cluster_policy["status_by_persona"].get(str(persona_id), {}).get("fallback_selected_count", 0) or 0),
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
        "cluster_stats_df": empty,
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
        "cluster_naming_recommendations_df": empty,
        "old_vs_new_cluster_summary_df": empty,
        "bottleneck_feature_importance_df": empty,
        "role_feature_importance_before_after_df": empty,
        "cluster_comparison_before_after_df": empty,
        "cluster_comparison_before_after_md": "",
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
    if promotion_grounding_status == "promoted_but_weakly_grounded":
        prefix = "Promoted but weakly grounded persona"
    elif promotion_grounding_status == "promoted_but_ungrounded":
        prefix = "Promoted but ungrounded persona"
    elif promotion_grounding_status == "downgraded_due_to_no_grounding":
        prefix = "Downgraded exploratory cluster"
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
    return str(payload.get("status", "") or "") == "promoted_persona"


def _is_final_usable_persona(payload: dict[str, Any]) -> bool:
    """Return whether a persona is usable for downstream reporting."""
    return str(payload.get("promotion_grounding_status", "") or "") == "promoted_and_grounded"


def _reporting_readiness_status(payload: dict[str, Any]) -> str:
    """Return the downstream reporting readiness class for one persona."""
    if _is_final_usable_persona(payload):
        return "final_usable_persona"
    if _is_workbook_review_visible(payload):
        combined_status = str(payload.get("promotion_grounding_status", "") or "")
        if combined_status == "promoted_but_weakly_grounded":
            return "review_only_weakly_grounded"
        if combined_status == "promoted_but_ungrounded":
            return "review_only_ungrounded"
    if _is_promoted_candidate_persona(payload):
        return "promotion_candidate_pending_review"
    return "not_final_usable"


def _cluster_promotion_policy(persona_source_df: pd.DataFrame, total_labeled_records: int) -> dict[str, Any]:
    """Classify clusters as promoted personas or exploratory residual buckets."""
    sizes = persona_source_df.groupby("persona_id")["episode_id"].nunique().sort_values(ascending=False)
    min_cluster_size = persona_min_cluster_size(total_labeled_records)
    largest_share = round_pct(int(sizes.iloc[0]) if not sizes.empty else 0, total_labeled_records)
    single_cluster_dominance = is_single_cluster_dominant(largest_share)
    dominant_persona = str(sizes.index[0]) if not sizes.empty else ""
    status_by_persona: dict[str, dict[str, str]] = {}
    for persona_id, size in sizes.items():
        persona_key = str(persona_id)
        share = round_pct(int(size), total_labeled_records)
        if int(size) < min_cluster_size:
            status = "exploratory_bucket"
            reason = f"sample size {int(size)} below min_cluster_size {min_cluster_size}"
        elif single_cluster_dominance and persona_key != dominant_persona:
            status = "exploratory_bucket"
            reason = f"residual cluster under single_cluster_dominance; largest cluster share {largest_share}%"
        else:
            status = "promoted_persona"
            reason = f"sample size {int(size)} meets min_cluster_size {min_cluster_size}"
        status_by_persona[persona_key] = {
            "status": status,
            "reason": reason,
            "share": str(share),
            "base_promotion_status": "promoted_candidate_persona" if status == "promoted_persona" else status,
            "grounding_status": "not_evaluated" if status == "promoted_persona" else "not_applicable",
            "promotion_grounding_status": status if status != "promoted_persona" else "promotion_pending_grounding_review",
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


def _merge_grounding_policy(
    cluster_policy: dict[str, Any],
    persona_grounding_df: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Merge explicit grounding outcomes into the promotion policy map."""
    merged = dict(cluster_policy)
    status_by_persona = {key: dict(value) for key, value in dict(cluster_policy.get("status_by_persona", {})).items()}
    grounding_lookup = (
        persona_grounding_df.set_index("persona_id").to_dict(orient="index")
        if persona_grounding_df is not None and not persona_grounding_df.empty and "persona_id" in persona_grounding_df.columns
        else {}
    )
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
        combined_status = str(grounding.get("promotion_grounding_status", "promoted_but_ungrounded") or "promoted_but_ungrounded")
        payload["grounding_status"] = str(grounding.get("grounding_status", "ungrounded") or "ungrounded")
        payload["promotion_grounding_status"] = combined_status
        payload["grounding_reason"] = str(grounding.get("grounding_reason", "") or "")
        payload["grounded_candidate_count"] = int(grounding.get("grounded_candidate_count", 0) or 0)
        payload["weak_candidate_count"] = int(grounding.get("weak_candidate_count", 0) or 0)
        payload["selected_example_count"] = int(grounding.get("selected_example_count", 0) or 0)
        payload["fallback_selected_count"] = int(grounding.get("fallback_selected_count", 0) or 0)
        if combined_status == downgraded_status:
            payload["status"] = exploratory_status
            payload["reason"] = f"{payload.get('reason', '')}; downgraded because no acceptable grounding evidence met policy".strip("; ")
        elif combined_status == "promoted_but_ungrounded":
            payload["status"] = "promoted_persona"
            payload["reason"] = f"{payload.get('reason', '')}; promoted remains visible but no acceptable grounding evidence met policy".strip("; ")
        elif combined_status == "promoted_but_weakly_grounded":
            payload["status"] = "promoted_persona"
            payload["reason"] = f"{payload.get('reason', '')}; only weak fallback evidence met policy".strip("; ")
        else:
            payload["status"] = "promoted_persona"
    merged["status_by_persona"] = status_by_persona
    merged["promoted_count"] = sum(1 for value in status_by_persona.values() if value.get("status") == "promoted_persona")
    merged["exploratory_count"] = sum(1 for value in status_by_persona.values() if value.get("status") != "promoted_persona")
    return merged


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
    if promotion_status != "promoted_persona":
        return f"Exploratory {name}"
    return name
