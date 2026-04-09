"""Audit, recommend, and apply persona-axis reduction decisions."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.analysis.persona_axes import build_axis_assignments
from src.analysis.persona_service import _assign_personas
from src.utils.io import ensure_dir
from src.utils.pipeline_schema import is_unknown_like

UNKNOWN_TOKENS = {"", "unknown", "unassigned", "null", "none", "other"}


def build_axis_quality_audit(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    candidate_df: pd.DataFrame,
    current_axis_schema: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Build wide/long axis assignments plus a metric audit for each axis."""
    axis_names = candidate_df.get("axis_name", pd.Series(dtype=str)).astype(str).tolist()
    axis_wide_df, axis_long_df = build_axis_assignments(episodes_df, labeled_df, axis_names=axis_names)
    current_core_axes = [str(row.get("axis_name", "")).strip() for row in current_axis_schema if str(row.get("axis_name", "")).strip()]
    current_core_axes = [axis for axis in current_core_axes if axis in axis_wide_df.columns]
    persona_assignments_df = _assign_personas(axis_wide_df, current_core_axes) if current_core_axes else pd.DataFrame()

    thresholds = config.get("audit", {})
    tiny_share_threshold = float(thresholds.get("tiny_class_share_threshold", 0.03))
    rows: list[dict[str, Any]] = []
    for axis_name in axis_names:
        if axis_name not in axis_wide_df.columns:
            continue
        series = axis_wide_df[axis_name].fillna("unassigned").astype(str).str.strip().str.lower()
        unknown_mask = series.map(_is_unknown_token)
        known = series[~unknown_mask]
        value_counts = known.value_counts()
        shares = known.value_counts(normalize=True)
        dominant_share = float(shares.iloc[0]) if not shares.empty else 0.0
        entropy = _normalized_entropy(shares)
        effective_class_count = int((shares >= tiny_share_threshold).sum()) if not shares.empty else 0
        long_tail_share = float(shares[shares < tiny_share_threshold].sum()) if not shares.empty else 0.0
        cluster_contribution = _cluster_contribution(persona_assignments_df, axis_name)
        axis_purity = _axis_purity(persona_assignments_df, axis_name)
        overlap_axis, overlap_score = _best_axis_overlap(axis_wide_df, axis_name)
        rows.append(
            {
                "axis_name": axis_name,
                "axis_description": _lookup_value(candidate_df, axis_name, "description"),
                "total_labeled_rows": int(len(series)),
                "known_rows": int((~unknown_mask).sum()),
                "known_coverage_rate": round(float((~unknown_mask).mean()), 4) if len(series) else 0.0,
                "unknown_rate": round(float(unknown_mask.mean()), 4) if len(series) else 0.0,
                "distinct_class_count": int(known.nunique()),
                "effective_class_count": effective_class_count,
                "top_classes_json": json.dumps(_top_class_rows(value_counts, shares), ensure_ascii=False),
                "long_tail_share": round(long_tail_share, 4),
                "dominant_class_share": round(dominant_share, 4),
                "normalized_entropy": round(entropy, 4),
                "class_imbalance": round(1.0 - entropy, 4),
                "cluster_contribution": round(cluster_contribution, 4),
                "axis_purity": round(axis_purity, 4),
                "best_overlap_axis": overlap_axis,
                "best_overlap_score": round(overlap_score, 4),
                "current_status": _current_axis_status(axis_name, current_axis_schema),
                "current_allowed_values": json.dumps(_current_allowed_values(axis_name, current_axis_schema), ensure_ascii=False),
            }
        )
    audit_df = pd.DataFrame(rows).sort_values(
        ["unknown_rate", "cluster_contribution", "dominant_class_share"],
        ascending=[False, True, False],
    ).reset_index(drop=True)
    return {
        "axis_wide_df": axis_wide_df,
        "axis_long_df": axis_long_df,
        "persona_assignments_df": persona_assignments_df,
        "audit_df": audit_df,
    }


def recommend_axis_reduction(audit_df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Recommend keep/merge/simplify/drop actions from audit metrics plus config overrides."""
    thresholds = config.get("audit", {})
    axis_config = config.get("axes", {})
    rows: list[dict[str, Any]] = []
    for _, row in audit_df.iterrows():
        axis_name = str(row["axis_name"])
        settings = axis_config.get(axis_name, {})
        recommendation_type, reason_parts = _recommendation_type(row, settings, thresholds)
        target_axis = str(settings.get("target_axis", "") or "")
        simplify_map = settings.get("simplify_map", {}) or {}
        merge_value_map = settings.get("merge_value_map", {}) or {}
        rows.append(
            {
                "axis_name": axis_name,
                "current_status": str(row.get("current_status", "candidate") or "candidate"),
                "current_unknown_rate": float(row.get("unknown_rate", 0.0)),
                "current_label_distribution": str(row.get("top_classes_json", "[]") or "[]"),
                "current_cluster_contribution": float(row.get("cluster_contribution", 0.0)),
                "dominant_class_share": float(row.get("dominant_class_share", 0.0)),
                "best_overlap_axis": str(row.get("best_overlap_axis", "") or ""),
                "best_overlap_score": float(row.get("best_overlap_score", 0.0)),
                "recommendation_type": recommendation_type,
                "target_axis": target_axis,
                "target_label_mapping": json.dumps(simplify_map or merge_value_map, ensure_ascii=False),
                "why_it_is_weak_or_useful": " | ".join(reason_parts),
                "expected_impact_on_persona_quality": _expected_impact(recommendation_type),
                "expected_risk_or_limitation": _expected_risk(recommendation_type, row),
                "manual_review_flag": recommendation_type == "recommend_manual_review",
            }
        )
    return pd.DataFrame(rows)


def apply_axis_reduction(
    axis_wide_df: pd.DataFrame,
    axis_long_df: pd.DataFrame,
    audit_df: pd.DataFrame,
    recommendations_df: pd.DataFrame,
    candidate_df: pd.DataFrame,
    current_axis_schema: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Apply reduction decisions and build reduced schemas plus comparison reports."""
    reduced_wide_df = axis_wide_df.copy()
    reduced_long_df = axis_long_df.copy()
    axis_config = config.get("axes", {})
    impacted_rows: list[dict[str, Any]] = []

    for _, rec in recommendations_df.iterrows():
        axis_name = str(rec["axis_name"])
        action = str(rec["recommendation_type"])
        settings = axis_config.get(axis_name, {})
        if axis_name not in reduced_wide_df.columns:
            continue
        if action == "simplify":
            impacted_rows.extend(_apply_simplify(reduced_wide_df, reduced_long_df, axis_name, settings.get("simplify_map", {}) or {}))
        elif action == "merge":
            target_axis = str(settings.get("target_axis", "") or rec.get("target_axis", "") or "")
            if target_axis:
                merge_impacts, reduced_long_df = _apply_merge(
                    reduced_wide_df,
                    reduced_long_df,
                    axis_name,
                    target_axis,
                    settings.get("merge_value_map", {}) or {},
                )
                impacted_rows.extend(merge_impacts)

    reduced_schema = _build_reduced_schema(candidate_df, current_axis_schema, recommendations_df, axis_config)
    before_core_axes = [str(row.get("axis_name", "")).strip() for row in current_axis_schema if str(row.get("axis_name", "")).strip()]
    after_core_axes = [
        str(row.get("axis_name", "")).strip()
        for row in reduced_schema
        if str(row.get("axis_name", "")).strip() and str(row.get("axis_role", "core")) == "core"
    ]
    before_persona_assignments_df = _assign_personas(axis_wide_df, [axis for axis in before_core_axes if axis in axis_wide_df.columns])
    after_persona_assignments_df = _assign_personas(reduced_wide_df, [axis for axis in after_core_axes if axis in reduced_wide_df.columns])
    before_after_unknown_rates_df = _before_after_unknown_rates(audit_df, reduced_wide_df, reduced_schema)
    before_after_cluster_quality_df = _before_after_cluster_quality(
        axis_wide_df=axis_wide_df,
        reduced_wide_df=reduced_wide_df,
        before_persona_assignments_df=before_persona_assignments_df,
        after_persona_assignments_df=after_persona_assignments_df,
        before_core_axes=before_core_axes,
        after_core_axes=after_core_axes,
    )
    impacted_df = pd.DataFrame(impacted_rows)
    recommendation_lookup = recommendations_df.set_index("axis_name").to_dict(orient="index") if not recommendations_df.empty else {}
    impacted_samples_df = _build_impacted_samples(impacted_df, recommendation_lookup)
    before_after_summary_md = _build_before_after_summary(
        recommendations_df=recommendations_df,
        before_after_unknown_rates_df=before_after_unknown_rates_df,
        before_after_cluster_quality_df=before_after_cluster_quality_df,
        after_core_axes=after_core_axes,
    )
    merge_map = {
        "merged_axes": [
            {
                "axis_name": str(row["axis_name"]),
                "target_axis": str(row["target_axis"]),
                "merge_value_map": axis_config.get(str(row["axis_name"]), {}).get("merge_value_map", {}),
            }
            for _, row in recommendations_df.iterrows()
            if str(row["recommendation_type"]) == "merge"
        ],
        "simplified_axes": [
            {
                "axis_name": str(row["axis_name"]),
                "simplify_map": axis_config.get(str(row["axis_name"]), {}).get("simplify_map", {}),
            }
            for _, row in recommendations_df.iterrows()
            if str(row["recommendation_type"]) == "simplify"
        ],
        "dropped_axes": [
            str(row["axis_name"])
            for _, row in recommendations_df.iterrows()
            if str(row["recommendation_type"]) == "drop"
        ],
        "optional_axes": [
            str(row["axis_name"])
            for _, row in recommendations_df.iterrows()
            if str(row["recommendation_type"]) == "keep_optional"
        ],
        "core_axes": after_core_axes,
    }
    return {
        "reduced_axis_wide_df": reduced_wide_df,
        "reduced_axis_long_df": reduced_long_df,
        "reduced_axis_schema": reduced_schema,
        "before_persona_assignments_df": before_persona_assignments_df,
        "after_persona_assignments_df": after_persona_assignments_df,
        "before_after_unknown_rates_df": before_after_unknown_rates_df,
        "before_after_cluster_quality_df": before_after_cluster_quality_df,
        "impacted_samples_df": impacted_samples_df,
        "before_after_summary_md": before_after_summary_md,
        "merge_map": merge_map,
    }


def write_axis_reduction_outputs(
    root_dir: Path,
    audit_df: pd.DataFrame,
    recommendations_df: pd.DataFrame,
    reduced_outputs: dict[str, Any],
    apply_changes: bool,
) -> dict[str, Path]:
    """Write axis audit, recommendation, and reduced-schema artifacts."""
    analysis_dir = ensure_dir(root_dir / "data" / "analysis")
    paths = {
        "axis_quality_audit_csv": analysis_dir / "axis_quality_audit.csv",
        "axis_quality_audit_parquet": analysis_dir / "axis_quality_audit.parquet",
        "axis_recommendations_csv": analysis_dir / "axis_recommendations.csv",
        "axis_recommendations_parquet": analysis_dir / "axis_recommendations.parquet",
        "axis_merge_map_yaml": analysis_dir / "axis_merge_map.yaml",
        "before_after_unknown_rates_csv": analysis_dir / "before_after_unknown_rates.csv",
        "before_after_cluster_quality_csv": analysis_dir / "before_after_cluster_quality.csv",
        "before_after_axis_summary_md": analysis_dir / "before_after_axis_summary.md",
        "axis_impacted_samples_csv": analysis_dir / "axis_impacted_samples.csv",
        "persona_axis_assignments_reduced_parquet": analysis_dir / "persona_axis_assignments_reduced.parquet",
        "persona_axis_values_reduced_parquet": analysis_dir / "persona_axis_values_reduced.parquet",
        "final_axis_schema_reduced_json": analysis_dir / "final_axis_schema_reduced.json",
        "axis_reduction_plan_json": analysis_dir / "axis_reduction_plan.json",
    }
    audit_df.to_csv(paths["axis_quality_audit_csv"], index=False)
    audit_df.to_parquet(paths["axis_quality_audit_parquet"], index=False)
    recommendations_df.to_csv(paths["axis_recommendations_csv"], index=False)
    recommendations_df.to_parquet(paths["axis_recommendations_parquet"], index=False)
    paths["axis_merge_map_yaml"].write_text(yaml.safe_dump(reduced_outputs["merge_map"], sort_keys=False, allow_unicode=True), encoding="utf-8")
    reduced_outputs["before_after_unknown_rates_df"].to_csv(paths["before_after_unknown_rates_csv"], index=False)
    reduced_outputs["before_after_cluster_quality_df"].to_csv(paths["before_after_cluster_quality_csv"], index=False)
    paths["before_after_axis_summary_md"].write_text(reduced_outputs["before_after_summary_md"], encoding="utf-8")
    reduced_outputs["impacted_samples_df"].to_csv(paths["axis_impacted_samples_csv"], index=False)
    reduced_outputs["reduced_axis_wide_df"].to_parquet(paths["persona_axis_assignments_reduced_parquet"], index=False)
    reduced_outputs["reduced_axis_long_df"].to_parquet(paths["persona_axis_values_reduced_parquet"], index=False)
    paths["final_axis_schema_reduced_json"].write_text(
        json.dumps(reduced_outputs["reduced_axis_schema"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    paths["axis_reduction_plan_json"].write_text(
        json.dumps(
            {
                "final_axis_schema": reduced_outputs["reduced_axis_schema"],
                "merge_map": reduced_outputs["merge_map"],
                "apply_changes": apply_changes,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    if apply_changes:
        (analysis_dir / "final_axis_schema.json").write_text(
            json.dumps(reduced_outputs["reduced_axis_schema"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return paths


def export_axis_samples(
    episodes_df: pd.DataFrame,
    axis_wide_df: pd.DataFrame,
    recommendations_df: pd.DataFrame,
    axis_name: str,
    output_path: Path,
    limit: int = 30,
) -> Path:
    """Export reviewable sample rows for one axis."""
    axis = str(axis_name).strip()
    if not axis or axis not in axis_wide_df.columns:
        raise ValueError(f"Axis not found in assignments: {axis_name}")
    lookup = recommendations_df.set_index("axis_name").to_dict(orient="index") if not recommendations_df.empty else {}
    merged = episodes_df.merge(axis_wide_df[["episode_id", axis]], on="episode_id", how="left").fillna("")
    rec = lookup.get(axis, {})
    sample_df = merged.assign(
        axis_value=merged[axis].astype(str),
        recommendation_type=str(rec.get("recommendation_type", "unknown")),
        why=str(rec.get("why_it_is_weak_or_useful", "")),
    )[["episode_id", "source", "axis_value", "recommendation_type", "why", "normalized_episode"]].head(limit)
    ensure_dir(output_path.parent)
    sample_df.to_csv(output_path, index=False)
    return output_path


def _recommendation_type(row: pd.Series, settings: dict[str, Any], thresholds: dict[str, Any]) -> tuple[str, list[str]]:
    """Combine metric heuristics and config overrides into one recommendation."""
    reasons: list[str] = []
    override = str(settings.get("preferred_action", "") or "").strip()
    unknown_rate = float(row.get("unknown_rate", 0.0))
    cluster_contribution = float(row.get("cluster_contribution", 0.0))
    dominant_share = float(row.get("dominant_class_share", 0.0))
    effective_classes = int(row.get("effective_class_count", 0))
    overlap_axis = str(row.get("best_overlap_axis", "") or "")
    overlap_score = float(row.get("best_overlap_score", 0.0))

    high_unknown = float(thresholds.get("high_unknown_rate", 0.45))
    warn_unknown = float(thresholds.get("warn_unknown_rate", 0.30))
    min_cluster_contribution = float(thresholds.get("min_cluster_contribution", 0.08))
    strong_cluster_contribution = float(thresholds.get("strong_cluster_contribution", 0.16))
    dominant_warn = float(thresholds.get("dominant_share_warn", 0.72))
    dominant_drop = float(thresholds.get("dominant_share_drop", 0.90))
    overlap_merge = float(thresholds.get("overlap_merge_threshold", 0.60))

    if cluster_contribution >= strong_cluster_contribution:
        reasons.append("known portion materially separates personas/clusters")
    if unknown_rate >= high_unknown:
        reasons.append("unknown/unassigned share is above the high-risk threshold")
    elif unknown_rate >= warn_unknown:
        reasons.append("unknown/unassigned share is elevated")
    if dominant_share >= dominant_warn:
        reasons.append("one class dominates most known rows")
    if effective_classes <= 1:
        reasons.append("effective class count is too small for a robust axis")
    if overlap_axis and overlap_score >= overlap_merge:
        reasons.append(f"strong semantic/statistical overlap with {overlap_axis}")
    if settings.get("simplify_map"):
        reasons.append("config provides a safe label-collapse mapping")
    if settings.get("target_axis"):
        reasons.append(f"config defines {settings['target_axis']} as a merge target")

    if override:
        reasons.append(f"config override: {override}")
        return override, reasons
    if unknown_rate >= high_unknown and cluster_contribution < min_cluster_contribution:
        return "drop", reasons
    if dominant_share >= dominant_drop and effective_classes <= 1:
        return "drop", reasons
    if overlap_axis and overlap_score >= overlap_merge and settings.get("target_axis"):
        return "merge", reasons
    if settings.get("simplify_map") and (unknown_rate >= warn_unknown or dominant_share >= dominant_warn):
        return "simplify", reasons
    if unknown_rate >= high_unknown and cluster_contribution >= strong_cluster_contribution:
        return "keep_optional", reasons
    if cluster_contribution >= min_cluster_contribution and effective_classes >= 2:
        return "keep_core", reasons
    return "recommend_manual_review", reasons or ["uncertain trade-off between coverage and discriminative value"]


def _expected_impact(action: str) -> str:
    """Describe expected persona-quality effect."""
    mapping = {
        "keep_core": "Retains a comparatively useful separator in the core persona feature set.",
        "keep_optional": "Preserves descriptive value while reducing core persona noise.",
        "merge": "Backfills signal into a stronger neighboring axis and reduces redundant sparsity.",
        "simplify": "Reduces tail classes and improves interpretability of the remaining axis.",
        "drop": "Removes weak or misleading noise from persona construction.",
        "recommend_manual_review": "Requires manual judgment before changing persona behavior.",
    }
    return mapping.get(action, "Unknown expected impact.")


def _expected_risk(action: str, row: pd.Series) -> str:
    """Describe main recommendation risk."""
    if action == "drop" and float(row.get("cluster_contribution", 0.0)) >= 0.08:
        return "Dropping may hide a niche but meaningful segment inside the known subset."
    if action == "merge":
        return "Merge target may blur operationally distinct cases if the mapping is too broad."
    if action == "simplify":
        return "Collapsed labels lose some nuance for downstream interpretation."
    if action == "keep_optional":
        return "Optional axis will no longer directly drive persona clustering."
    if action == "recommend_manual_review":
        return "Metrics are mixed; an automatic action would be brittle."
    return "Low direct risk under the current data distribution."


def _apply_simplify(
    axis_wide_df: pd.DataFrame,
    axis_long_df: pd.DataFrame,
    axis_name: str,
    simplify_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Collapse fine-grained axis values into broader values."""
    impacted_rows: list[dict[str, Any]] = []
    if not simplify_map:
        return impacted_rows
    before = axis_wide_df[axis_name].fillna("unassigned").astype(str)
    after = before.map(lambda value: simplify_map.get(str(value), str(value)))
    changed_mask = before != after
    for episode_id, before_value, after_value in zip(axis_wide_df.loc[changed_mask, "episode_id"], before[changed_mask], after[changed_mask]):
        impacted_rows.append(
            {
                "episode_id": str(episode_id),
                "axis_name": axis_name,
                "decision_type": "simplify",
                "before_value": str(before_value),
                "after_value": str(after_value),
            }
        )
    axis_wide_df[axis_name] = after
    long_mask = axis_long_df["axis_name"].astype(str) == axis_name
    axis_long_df.loc[long_mask, "axis_value"] = axis_long_df.loc[long_mask, "axis_value"].astype(str).map(
        lambda value: simplify_map.get(str(value), str(value))
    )
    return impacted_rows


def _apply_merge(
    axis_wide_df: pd.DataFrame,
    axis_long_df: pd.DataFrame,
    axis_name: str,
    target_axis: str,
    merge_value_map: dict[str, str],
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    """Merge one weak axis into a stronger axis via backfill mapping."""
    impacted_rows: list[dict[str, Any]] = []
    new_long_rows: list[dict[str, Any]] = []
    if target_axis not in axis_wide_df.columns:
        axis_wide_df[target_axis] = "unassigned"
    source_series = axis_wide_df[axis_name].fillna("unassigned").astype(str)
    target_series = axis_wide_df[target_axis].fillna("unassigned").astype(str)
    for index, (episode_id, source_value, target_value) in enumerate(zip(axis_wide_df["episode_id"], source_series, target_series)):
        if _is_unknown_token(source_value):
            continue
        mapped_value = merge_value_map.get(str(source_value), str(source_value))
        if _is_unknown_token(target_value):
            axis_wide_df.at[index, target_axis] = mapped_value
            impacted_rows.append(
                {
                    "episode_id": str(episode_id),
                    "axis_name": axis_name,
                    "decision_type": "merge",
                    "before_value": str(source_value),
                    "after_value": str(mapped_value),
                    "target_axis": target_axis,
                }
            )
            new_long_rows.append(
                {
                    "episode_id": str(episode_id),
                    "axis_name": target_axis,
                    "axis_value": str(mapped_value),
                    "value_rank": 1,
                    "is_primary": True,
                }
            )
    if new_long_rows:
        axis_long_df = pd.concat([axis_long_df, pd.DataFrame(new_long_rows)], ignore_index=True)
    return impacted_rows, axis_long_df


def _build_reduced_schema(
    candidate_df: pd.DataFrame,
    current_axis_schema: list[dict[str, Any]],
    recommendations_df: pd.DataFrame,
    axis_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build annotated final axis schema with core vs optional roles."""
    current_lookup = {str(row.get("axis_name", "")).strip(): row for row in current_axis_schema}
    recommendation_lookup = recommendations_df.set_index("axis_name").to_dict(orient="index") if not recommendations_df.empty else {}
    reduced_schema: list[dict[str, Any]] = []
    for _, candidate in candidate_df.iterrows():
        axis_name = str(candidate["axis_name"])
        recommendation = recommendation_lookup.get(axis_name, {})
        action = str(recommendation.get("recommendation_type", "recommend_manual_review"))
        if action == "drop":
            continue
        current = current_lookup.get(axis_name, {})
        settings = axis_config.get(axis_name, {})
        axis_role = "optional" if action in {"keep_optional", "merge", "simplify", "recommend_manual_review"} else "core"
        allowed_values = list(current.get("allowed_values_or_logic", []) or [])
        if settings.get("simplify_map"):
            allowed_values = sorted(set(settings.get("simplify_map", {}).values()))
        reduced_schema.append(
            {
                "axis_name": axis_name,
                "why_it_matters": str(current.get("why_it_matters", candidate.get("description", ""))).strip(),
                "allowed_values_or_logic": allowed_values,
                "evidence_fields_used": list(current.get("evidence_fields_used", [])),
                "axis_role": axis_role,
                "reduction_decision": action,
                "deprecation_notice": settings.get("deprecation_notice", ""),
                "compatibility_legacy_axis": axis_name if str(recommendation.get("current_status", "")) == "core" else "",
                "target_axis": settings.get("target_axis", ""),
            }
        )
    return reduced_schema


def _before_after_unknown_rates(audit_df: pd.DataFrame, reduced_wide_df: pd.DataFrame, reduced_schema: list[dict[str, Any]]) -> pd.DataFrame:
    """Compare per-axis unknown rates before vs after reduction."""
    rows: list[dict[str, Any]] = []
    before_lookup = audit_df.set_index("axis_name").to_dict(orient="index") if not audit_df.empty else {}
    for row in reduced_schema:
        axis_name = str(row.get("axis_name", "")).strip()
        if not axis_name or axis_name not in reduced_wide_df.columns:
            continue
        after_series = reduced_wide_df[axis_name].fillna("unassigned").astype(str).str.lower()
        after_unknown_rate = float(after_series.map(_is_unknown_token).mean()) if len(after_series) else 0.0
        before_unknown_rate = float(before_lookup.get(axis_name, {}).get("unknown_rate", 1.0))
        rows.append(
            {
                "axis_name": axis_name,
                "axis_role_after": str(row.get("axis_role", "core")),
                "before_unknown_rate": round(before_unknown_rate, 4),
                "after_unknown_rate": round(after_unknown_rate, 4),
                "delta": round(after_unknown_rate - before_unknown_rate, 4),
            }
        )
    return pd.DataFrame(rows).sort_values(["axis_role_after", "after_unknown_rate", "axis_name"]).reset_index(drop=True)


def _before_after_cluster_quality(
    axis_wide_df: pd.DataFrame,
    reduced_wide_df: pd.DataFrame,
    before_persona_assignments_df: pd.DataFrame,
    after_persona_assignments_df: pd.DataFrame,
    before_core_axes: list[str],
    after_core_axes: list[str],
) -> pd.DataFrame:
    """Compare coarse persona quality before vs after reduction."""
    rows = [
        _metric_row("persona_count", before_persona_assignments_df.get("persona_id", pd.Series(dtype=str)).nunique(), after_persona_assignments_df.get("persona_id", pd.Series(dtype=str)).nunique()),
        _metric_row("rows_with_any_core_unassigned_ratio", _row_unassigned_ratio(axis_wide_df, before_core_axes), _row_unassigned_ratio(reduced_wide_df, after_core_axes)),
        _metric_row("avg_known_core_axes_per_row", _avg_known_axes(axis_wide_df, before_core_axes), _avg_known_axes(reduced_wide_df, after_core_axes)),
        _metric_row("avg_persona_size", _avg_group_size(before_persona_assignments_df), _avg_group_size(after_persona_assignments_df)),
        _metric_row("min_persona_size", _min_group_size(before_persona_assignments_df), _min_group_size(after_persona_assignments_df)),
        _metric_row("core_axis_count", len(before_core_axes), len(after_core_axes)),
    ]
    return pd.DataFrame(rows)


def _build_impacted_samples(impacted_df: pd.DataFrame, recommendation_lookup: dict[str, dict[str, Any]]) -> pd.DataFrame:
    """Build a compact impacted-sample table from value changes."""
    if impacted_df.empty:
        return pd.DataFrame(columns=["episode_id", "axis_name", "decision_type", "before_value", "after_value", "recommendation_type"])
    result = impacted_df.copy()
    result["recommendation_type"] = result["axis_name"].map(
        lambda axis: recommendation_lookup.get(axis, {}).get("recommendation_type", "unknown")
    )
    return result.head(200)


def _build_before_after_summary(
    recommendations_df: pd.DataFrame,
    before_after_unknown_rates_df: pd.DataFrame,
    before_after_cluster_quality_df: pd.DataFrame,
    after_core_axes: list[str],
) -> str:
    """Build a short markdown summary of the proposed reduction."""
    keep_core = recommendations_df[recommendations_df["recommendation_type"] == "keep_core"]["axis_name"].tolist()
    keep_optional = recommendations_df[recommendations_df["recommendation_type"] == "keep_optional"]["axis_name"].tolist()
    merged = recommendations_df[recommendations_df["recommendation_type"] == "merge"]["axis_name"].tolist()
    simplified = recommendations_df[recommendations_df["recommendation_type"] == "simplify"]["axis_name"].tolist()
    dropped = recommendations_df[recommendations_df["recommendation_type"] == "drop"]["axis_name"].tolist()
    unknown_improvement = before_after_unknown_rates_df["delta"].sum() if not before_after_unknown_rates_df.empty else 0.0
    rows = [
        "# Axis Reduction Summary",
        "",
        f"- Core axes after reduction: {', '.join(after_core_axes) or 'none'}",
        f"- Keep core: {', '.join(keep_core) or 'none'}",
        f"- Keep optional: {', '.join(keep_optional) or 'none'}",
        f"- Merge: {', '.join(merged) or 'none'}",
        f"- Simplify: {', '.join(simplified) or 'none'}",
        f"- Drop: {', '.join(dropped) or 'none'}",
        f"- Net per-axis unknown-rate delta sum: {round(float(unknown_improvement), 4)}",
        "",
        "## Cluster Quality",
        "",
    ]
    for _, row in before_after_cluster_quality_df.iterrows():
        rows.append(f"- {row['metric_name']}: {row['before_value']} -> {row['after_value']} (delta {row['delta']})")
    return "\n".join(rows) + "\n"


def _metric_row(metric_name: str, before_value: float | int, after_value: float | int) -> dict[str, Any]:
    """Build a simple comparison row."""
    return {
        "metric_name": metric_name,
        "before_value": round(float(before_value), 4) if isinstance(before_value, float) else before_value,
        "after_value": round(float(after_value), 4) if isinstance(after_value, float) else after_value,
        "delta": round(float(after_value) - float(before_value), 4),
    }


def _current_axis_status(axis_name: str, current_axis_schema: list[dict[str, Any]]) -> str:
    """Return whether the axis is currently used downstream."""
    current_names = {str(row.get("axis_name", "")).strip() for row in current_axis_schema}
    return "core" if axis_name in current_names else "candidate"


def _current_allowed_values(axis_name: str, current_axis_schema: list[dict[str, Any]]) -> list[str]:
    """Return current allowed values for one axis when present."""
    for row in current_axis_schema:
        if str(row.get("axis_name", "")).strip() == axis_name:
            values = row.get("allowed_values_or_logic", [])
            return list(values) if isinstance(values, list) else []
    return []


def _top_class_rows(counts: pd.Series, shares: pd.Series, limit: int = 6) -> list[dict[str, Any]]:
    """Return top-class rows as JSON-serializable objects."""
    rows: list[dict[str, Any]] = []
    for label, count in counts.head(limit).items():
        rows.append(
            {
                "label": str(label),
                "count": int(count),
                "share": round(float(shares.get(label, 0.0)), 4),
            }
        )
    return rows


def _cluster_contribution(persona_assignments_df: pd.DataFrame, axis_name: str) -> float:
    """Estimate normalized information gain from one axis to persona assignment."""
    if persona_assignments_df.empty or axis_name not in persona_assignments_df.columns or "persona_id" not in persona_assignments_df.columns:
        return 0.0
    cluster_series = persona_assignments_df["persona_id"].fillna("unassigned").astype(str)
    axis_series = persona_assignments_df[axis_name].fillna("unassigned").astype(str)
    total_entropy = _series_entropy(cluster_series)
    if total_entropy <= 0:
        return 0.0
    conditional_entropy = 0.0
    total_rows = max(len(persona_assignments_df), 1)
    for _, group in persona_assignments_df.groupby(axis_name, dropna=False):
        conditional_entropy += (len(group) / total_rows) * _series_entropy(group["persona_id"].fillna("unassigned").astype(str))
    return max((total_entropy - conditional_entropy) / total_entropy, 0.0)


def _axis_purity(persona_assignments_df: pd.DataFrame, axis_name: str) -> float:
    """Average persona purity within each axis value."""
    if persona_assignments_df.empty or axis_name not in persona_assignments_df.columns or "persona_id" not in persona_assignments_df.columns:
        return 0.0
    purities: list[float] = []
    for _, group in persona_assignments_df.groupby(axis_name, dropna=False):
        vc = group["persona_id"].astype(str).value_counts(normalize=True)
        purities.append(float(vc.iloc[0]) if not vc.empty else 0.0)
    return sum(purities) / max(len(purities), 1)


def _best_axis_overlap(axis_wide_df: pd.DataFrame, axis_name: str) -> tuple[str, float]:
    """Find the strongest overlapping axis using normalized mutual information."""
    if axis_name not in axis_wide_df.columns:
        return "", 0.0
    best_axis = ""
    best_score = 0.0
    base_series = axis_wide_df[axis_name].fillna("unassigned").astype(str)
    for other_axis in axis_wide_df.columns:
        if other_axis in {"episode_id", axis_name}:
            continue
        other_series = axis_wide_df[other_axis].fillna("unassigned").astype(str)
        score = _normalized_mutual_information(base_series, other_series)
        if score > best_score:
            best_axis = other_axis
            best_score = score
    return best_axis, best_score


def _normalized_mutual_information(series_a: pd.Series, series_b: pd.Series) -> float:
    """Compute a small dependency score between two categorical series."""
    if series_a.empty or series_b.empty:
        return 0.0
    contingency = pd.crosstab(series_a, series_b)
    if contingency.empty:
        return 0.0
    total = contingency.to_numpy().sum()
    if total <= 0:
        return 0.0
    p_xy = contingency / total
    p_x = p_xy.sum(axis=1)
    p_y = p_xy.sum(axis=0)
    mi = 0.0
    for x_label in p_xy.index:
        for y_label in p_xy.columns:
            p_val = float(p_xy.at[x_label, y_label])
            if p_val <= 0:
                continue
            mi += p_val * math.log(p_val / (float(p_x.loc[x_label]) * float(p_y.loc[y_label])), 2)
    h_x = _series_entropy(series_a)
    h_y = _series_entropy(series_b)
    denominator = max((h_x + h_y) / 2, 1e-9)
    return max(mi / denominator, 0.0)


def _normalized_entropy(shares: pd.Series) -> float:
    """Return 0..1 entropy after normalizing by class count."""
    if shares.empty or len(shares) <= 1:
        return 0.0
    entropy = -sum(float(prob) * math.log(float(prob), 2) for prob in shares if prob > 0)
    return entropy / math.log(len(shares), 2)


def _series_entropy(series: pd.Series) -> float:
    """Return entropy for a categorical series."""
    shares = series.astype(str).value_counts(normalize=True)
    return -sum(float(prob) * math.log(float(prob), 2) for prob in shares if prob > 0)


def _row_unassigned_ratio(df: pd.DataFrame, axes: list[str]) -> float:
    """Return ratio of rows with any unassigned value across selected axes."""
    selected = [axis for axis in axes if axis in df.columns]
    if not selected or df.empty:
        return 1.0
    mask = df[selected].fillna("unassigned").apply(lambda row: any(_is_unknown_token(value) for value in row.tolist()), axis=1)
    return float(mask.mean())


def _avg_known_axes(df: pd.DataFrame, axes: list[str]) -> float:
    """Return average count of known axis values per row."""
    selected = [axis for axis in axes if axis in df.columns]
    if not selected or df.empty:
        return 0.0
    counts = df[selected].fillna("unassigned").apply(
        lambda row: sum(1 for value in row.tolist() if not _is_unknown_token(value)),
        axis=1,
    )
    return float(counts.mean())


def _avg_group_size(persona_assignments_df: pd.DataFrame) -> float:
    """Return average persona size."""
    if persona_assignments_df.empty or "persona_id" not in persona_assignments_df.columns:
        return 0.0
    return float(persona_assignments_df["persona_id"].astype(str).value_counts().mean())


def _min_group_size(persona_assignments_df: pd.DataFrame) -> int:
    """Return smallest persona size."""
    if persona_assignments_df.empty or "persona_id" not in persona_assignments_df.columns:
        return 0
    return int(persona_assignments_df["persona_id"].astype(str).value_counts().min())


def _lookup_value(frame: pd.DataFrame, axis_name: str, column: str) -> str:
    """Lookup one string value from a dataframe keyed by axis_name."""
    subset = frame[frame["axis_name"].astype(str) == axis_name]
    if subset.empty or column not in subset.columns:
        return ""
    return str(subset.iloc[0][column] or "")


def _is_unknown_token(value: object) -> bool:
    """Return whether a value is unresolved."""
    return is_unknown_like(value) or str(value or "").strip().lower() in UNKNOWN_TOKENS
