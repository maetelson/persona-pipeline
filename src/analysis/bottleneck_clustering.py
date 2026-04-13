"""Bottleneck-first clustering and audit utilities for persona generation."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.example_selection import select_persona_representative_examples
from src.utils.io import load_yaml
from src.utils.pipeline_schema import is_unknown_like, split_pipe_codes


def build_bottleneck_cluster_outputs(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    axis_wide_df: pd.DataFrame,
    final_axis_schema: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build bottleneck-first cluster assignments, audits, and comparison outputs."""
    config = load_yaml(Path(__file__).resolve().parents[2] / "config" / "bottleneck_clustering.yaml")
    feature_df = build_bottleneck_feature_table(episodes_df, labeled_df, axis_wide_df, config)
    persona_assignments_df = assign_bottleneck_clusters(feature_df, config)
    merged = (
        episodes_df.merge(labeled_df, on="episode_id", how="inner")
        .merge(axis_wide_df, on="episode_id", how="left")
        .merge(feature_df, on="episode_id", how="left")
        .merge(persona_assignments_df, on="episode_id", how="inner")
        .fillna("")
    )
    overlap_merge_outputs = merge_overlapping_personas(merged, persona_assignments_df, feature_df, config)
    persona_assignments_df = overlap_merge_outputs["assignments_df"]
    merged = (
        episodes_df.merge(labeled_df, on="episode_id", how="inner")
        .merge(axis_wide_df, on="episode_id", how="left")
        .merge(feature_df, on="episode_id", how="left")
        .merge(persona_assignments_df, on="episode_id", how="inner")
        .fillna("")
    )
    axis_names = [str(row.get("axis_name", "")).strip() for row in final_axis_schema if str(row.get("axis_name", "")).strip()]
    example_outputs = select_persona_representative_examples(
        merged,
        axis_names=axis_names,
        config=_example_config(),
        max_items=int(config.get("clustering", {}).get("max_examples_per_cluster", 6)),
    )
    cluster_audit_df = build_cluster_meaning_audit(merged, feature_df, persona_assignments_df, example_outputs["selected_df"], config)
    robustness_outputs = build_cluster_robustness_outputs(cluster_audit_df, persona_assignments_df, config)
    naming_df = build_cluster_naming_recommendations(cluster_audit_df, config)
    comparison_outputs = compare_cluster_versions(axis_wide_df, merged, persona_assignments_df, final_axis_schema, cluster_audit_df, config)
    cluster_profiles = build_bottleneck_cluster_profiles(cluster_audit_df, example_outputs["selected_df"])
    return {
        "feature_df": feature_df,
        "persona_assignments_df": persona_assignments_df,
        "selected_examples_df": example_outputs["selected_df"],
        "borderline_examples_df": example_outputs["borderline_df"],
        "rejected_examples_df": example_outputs["rejected_df"],
        "example_audit_df": example_outputs["audit_df"],
        "representative_examples_markdown": example_outputs["markdown"],
        "cluster_meaning_audit_df": cluster_audit_df,
        "cluster_robustness_audit_df": robustness_outputs["audit_df"],
        "cluster_robustness_summary_df": robustness_outputs["summary_df"],
        "cluster_naming_recommendations_df": naming_df,
        "cluster_profiles": cluster_profiles,
        "persona_overlap_merge_audit_df": overlap_merge_outputs["audit_df"],
        "persona_overlap_merge_summary_df": overlap_merge_outputs["summary_df"],
        **comparison_outputs,
    }


def merge_overlapping_personas(
    merged_df: pd.DataFrame,
    assignments_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Merge near-duplicate personas when they represent the same underlying user type."""
    merge_cfg = dict(config.get("persona_overlap_merge", {}) or {})
    if merged_df.empty or assignments_df.empty or not bool(merge_cfg.get("enabled", True)):
        empty_audit = pd.DataFrame(columns=[
            "persona_id",
            "merge_action",
            "merged_into_persona_id",
            "before_persona_count",
            "after_persona_count",
            "role_context",
            "recurring_job",
            "expected_output",
            "workaround_pattern",
            "trust_failure_mode",
            "differentiating_axis",
            "merge_rationale",
            "new_persona_name",
            "new_persona_naming_rationale",
        ])
        empty_summary = pd.DataFrame(columns=["metric", "value"])
        updated = assignments_df.copy()
        return {"assignments_df": updated, "audit_df": empty_audit, "summary_df": empty_summary}

    before_count = int(assignments_df["persona_id"].astype(str).nunique())
    feature_columns = [column for column in _feature_columns(config) if column in feature_df.columns]
    feature_lookup = (
        feature_df.set_index("episode_id")[feature_columns]
        if feature_columns and not feature_df.empty and "episode_id" in feature_df.columns
        else pd.DataFrame()
    )
    profiles = _persona_overlap_profiles(merged_df, feature_lookup)
    if not profiles:
        empty_summary = pd.DataFrame([
            {"metric": "before_persona_count", "value": before_count},
            {"metric": "after_persona_count", "value": before_count},
            {"metric": "merged_persona_count", "value": 0},
        ])
        return {"assignments_df": assignments_df.copy(), "audit_df": pd.DataFrame(), "summary_df": empty_summary}

    ordered_profiles = sorted(profiles.values(), key=lambda item: (-int(item["cluster_size"]), str(item["persona_id"])))
    merge_map = {str(profile["persona_id"]): str(profile["persona_id"]) for profile in ordered_profiles}
    audit_rows: list[dict[str, Any]] = []
    merge_role_families = {str(value) for value in list(merge_cfg.get("merge_role_families", []) or [])}
    for profile in ordered_profiles:
        persona_id = str(profile["persona_id"])
        if int(profile["cluster_size"]) >= int(merge_cfg.get("min_parent_cluster_size", 24)):
            best_target = ""
            best_comparison: dict[str, Any] | None = None
            for candidate in ordered_profiles:
                target_id = str(candidate["persona_id"])
                if target_id == persona_id:
                    continue
                if int(candidate["cluster_size"]) < int(profile["cluster_size"]):
                    continue
                if merge_role_families and str(profile["role_context"]) not in merge_role_families and str(candidate["role_context"]) not in merge_role_families:
                    continue
                comparison = _overlap_merge_comparison(profile, candidate, merge_cfg)
                if not comparison["should_merge"]:
                    continue
                if best_comparison is None or int(comparison["profile_match_score"]) > int(best_comparison["profile_match_score"]):
                    best_target = target_id
                    best_comparison = comparison
            if best_target and best_target != persona_id and best_comparison is not None:
                merge_map[persona_id] = best_target
                parent = profiles[best_target]
                audit_rows.append(
                    {
                        "persona_id": persona_id,
                        "merge_action": "merged_into_parent_persona",
                        "merged_into_persona_id": best_target,
                        "before_persona_count": before_count,
                        "after_persona_count": 0,
                        "role_context": profile["role_context"],
                        "recurring_job": profile["recurring_job"],
                        "expected_output": profile["expected_output"],
                        "workaround_pattern": profile["workaround_pattern"],
                        "trust_failure_mode": profile["trust_failure_mode"],
                        "differentiating_axis": "",
                        "merge_rationale": best_comparison["merge_rationale"],
                        "new_persona_name": parent["parent_persona_name"],
                        "new_persona_naming_rationale": parent["naming_rationale"],
                    }
                )

    updated = assignments_df.copy()
    updated["pre_overlap_persona_id"] = updated["persona_id"].astype(str)
    updated["persona_id"] = updated["persona_id"].astype(str).map(lambda value: merge_map.get(str(value), str(value)))
    parent_name_lookup = {persona_id: profile["parent_persona_name"] for persona_id, profile in profiles.items()}
    for persona_id, target_id in merge_map.items():
        if persona_id == target_id:
            continue
        parent_name_lookup[persona_id] = parent_name_lookup.get(target_id, target_id)
    updated["cluster_name"] = updated["persona_id"].astype(str).map(lambda value: parent_name_lookup.get(str(value), str(value)))
    update_mask = updated["pre_overlap_persona_id"].astype(str) != updated["persona_id"].astype(str)
    if "robustness_action" in updated.columns:
        updated.loc[update_mask, "robustness_action"] = updated.loc[update_mask, "robustness_action"].fillna("").astype(str).map(
            lambda value: f"{value} | merged_overlap_persona".strip(" |")
        )
    if "robustness_reason" in updated.columns:
        updated.loc[update_mask, "robustness_reason"] = updated.loc[update_mask].apply(
            lambda row: f"{str(row.get('robustness_reason', '') or '').strip('; ')}; merged into overlapping parent persona {row['persona_id']} because the role, job, output, and workaround pattern did not materially change product implication".strip("; "),
            axis=1,
        )

    after_count = int(updated["persona_id"].astype(str).nunique())
    parent_ids = {str(value) for value in updated["persona_id"].astype(str).tolist()}
    for row in audit_rows:
        row["after_persona_count"] = after_count
    kept_ids = sorted(parent_ids)
    merged_children = {str(row["persona_id"]) for row in audit_rows}
    for persona_id in kept_ids:
        profile = profiles.get(str(persona_id))
        if profile is None:
            continue
        action = "parent_persona_retained" if any(str(row["merged_into_persona_id"]) == str(persona_id) for row in audit_rows) else "kept_distinct"
        differentiating_axis = ""
        merge_rationale = "retained as broader parent persona after overlap merge" if action == "parent_persona_retained" else "kept distinct because no larger persona shared the same product implication profile"
        if action == "kept_distinct":
            best_candidate = _best_distinct_candidate(profile, profiles, merge_cfg)
            if best_candidate is not None:
                differentiating_axis = str(best_candidate.get("differentiating_axis", "") or "")
                merge_rationale = str(best_candidate.get("distinct_rationale", merge_rationale))
        if str(persona_id) in merged_children:
            continue
        audit_rows.append(
            {
                "persona_id": persona_id,
                "merge_action": action,
                "merged_into_persona_id": str(persona_id) if action == "parent_persona_retained" else "",
                "before_persona_count": before_count,
                "after_persona_count": after_count,
                "role_context": profile["role_context"],
                "recurring_job": profile["recurring_job"],
                "expected_output": profile["expected_output"],
                "workaround_pattern": profile["workaround_pattern"],
                "trust_failure_mode": profile["trust_failure_mode"],
                "differentiating_axis": differentiating_axis,
                "merge_rationale": merge_rationale,
                "new_persona_name": profile["parent_persona_name"],
                "new_persona_naming_rationale": profile["naming_rationale"],
            }
        )

    summary_df = pd.DataFrame(
        [
            {"metric": "before_persona_count", "value": before_count},
            {"metric": "after_persona_count", "value": after_count},
            {"metric": "merged_persona_count", "value": max(before_count - after_count, 0)},
            {"metric": "affected_persona_ids", "value": " | ".join(sorted({str(row['persona_id']) for row in audit_rows if str(row.get('merge_action', '')).startswith('merged_')}))},
        ]
    )
    audit_df = pd.DataFrame(audit_rows).sort_values(["merge_action", "persona_id"]).reset_index(drop=True) if audit_rows else pd.DataFrame()
    return {"assignments_df": updated, "audit_df": audit_df, "summary_df": summary_df}


def _persona_overlap_profiles(merged_df: pd.DataFrame, feature_lookup: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Build merge-oriented persona profiles from the current assignments."""
    profiles: dict[str, dict[str, Any]] = {}
    feature_columns = [column for column in feature_lookup.columns.tolist()] if not feature_lookup.empty else []
    for persona_id, group in merged_df.groupby("persona_id", dropna=False):
        persona_key = str(persona_id)
        dominant_role = _distribution(group.get("user_role", pd.Series(dtype=str)))
        dominant_role_label = dominant_role[0]["label"] if dominant_role else "workflow_operator"
        role_context = _overlap_role_family(dominant_role_label)
        workflow = _dominant_value(group, "workflow_stage")
        goal = _dominant_value(group, "analysis_goal")
        output = _dominant_value(group, "output_expectation")
        tool_mode = _dominant_value(group, "tool_dependency_mode")
        bottleneck = _dominant_value(group, "bottleneck_type")
        recurring_job = _overlap_recurring_job_family(workflow, goal, output)
        expected_output = _overlap_expected_output_family(output)
        workaround_pattern = _overlap_workaround_family(output, tool_mode, bottleneck, group)
        trust_failure_mode = _overlap_trust_family(workflow, goal, output, bottleneck, group)
        centroid = _group_vector_mean(group["episode_id"].astype(str).tolist(), feature_lookup, feature_columns) if feature_columns else {}
        parent_persona_name = _overlap_parent_persona_name(role_context, recurring_job, expected_output)
        profiles[persona_key] = {
            "persona_id": persona_key,
            "cluster_size": int(group["episode_id"].nunique()) if "episode_id" in group.columns else int(len(group)),
            "role_context": role_context,
            "role_share": float(dominant_role[0]["share"]) if dominant_role else 0.0,
            "recurring_job": recurring_job,
            "expected_output": expected_output,
            "workaround_pattern": workaround_pattern,
            "trust_failure_mode": trust_failure_mode,
            "tool_mode": tool_mode,
            "workflow": workflow,
            "analysis_goal": goal,
            "bottleneck": bottleneck,
            "centroid": centroid,
            "parent_persona_name": parent_persona_name,
            "naming_rationale": _overlap_naming_rationale(role_context, recurring_job, expected_output),
        }
    return profiles


def _overlap_merge_comparison(profile: dict[str, Any], candidate: dict[str, Any], merge_cfg: dict[str, Any]) -> dict[str, Any]:
    """Compare two persona profiles and decide whether they should merge."""
    similarity = _cosine_similarity(dict(profile.get("centroid", {}) or {}), dict(candidate.get("centroid", {}) or {}))
    compatible_trust = {str(value) for value in list(merge_cfg.get("compatible_trust_families", []) or [])}
    compatible_workarounds = {str(value) for value in list(merge_cfg.get("compatible_workaround_families", []) or [])}
    same_role = str(profile.get("role_context", "")) == str(candidate.get("role_context", ""))
    same_job = str(profile.get("recurring_job", "")) == str(candidate.get("recurring_job", ""))
    same_output = str(profile.get("expected_output", "")) == str(candidate.get("expected_output", ""))
    same_workaround = str(profile.get("workaround_pattern", "")) == str(candidate.get("workaround_pattern", ""))
    same_trust = str(profile.get("trust_failure_mode", "")) == str(candidate.get("trust_failure_mode", ""))
    compatible_workaround = str(profile.get("workaround_pattern", "")) in compatible_workarounds and str(candidate.get("workaround_pattern", "")) in compatible_workarounds
    compatible_trust_mode = str(profile.get("trust_failure_mode", "")) in compatible_trust and str(candidate.get("trust_failure_mode", "")) in compatible_trust
    role_gap_ok = abs(float(profile.get("role_share", 0.0) or 0.0) - float(candidate.get("role_share", 0.0) or 0.0)) <= float(merge_cfg.get("max_role_share_gap", 0.35))
    profile_match_score = sum([
        1 if same_role else 0,
        1 if same_job else 0,
        1 if same_output else 0,
        1 if (same_workaround or compatible_workaround) else 0,
        1 if (same_trust or compatible_trust_mode) else 0,
    ])
    should_merge = (
        same_role
        and same_job
        and same_output
        and role_gap_ok
        and profile_match_score >= int(merge_cfg.get("compatible_profile_score_floor", 4))
        and similarity >= float(merge_cfg.get("similarity_floor", 0.55))
    )
    merge_rationale = (
        f"merged because role context={profile.get('role_context', '')}, recurring job={profile.get('recurring_job', '')}, "
        f"expected output={profile.get('expected_output', '')}, workaround pattern={profile.get('workaround_pattern', '')}, "
        f"and trust failure mode={profile.get('trust_failure_mode', '')} do not materially change product implication"
    )
    return {
        "should_merge": should_merge,
        "similarity": round(float(similarity), 4),
        "profile_match_score": profile_match_score,
        "merge_rationale": merge_rationale,
    }


def _best_distinct_candidate(profile: dict[str, Any], profiles: dict[str, dict[str, Any]], merge_cfg: dict[str, Any]) -> dict[str, Any] | None:
    """Return the closest larger persona and the explicit differentiating axis, when distinct."""
    best: dict[str, Any] | None = None
    for candidate_id, candidate in profiles.items():
        if candidate_id == str(profile.get("persona_id", "")):
            continue
        if int(candidate.get("cluster_size", 0) or 0) < int(profile.get("cluster_size", 0) or 0):
            continue
        similarity = _cosine_similarity(dict(profile.get("centroid", {}) or {}), dict(candidate.get("centroid", {}) or {}))
        differentiating_axis = _differentiating_axis(profile, candidate)
        if best is None or similarity > float(best.get("similarity", -1.0)):
            best = {
                "candidate_id": candidate_id,
                "similarity": similarity,
                "differentiating_axis": differentiating_axis,
                "distinct_rationale": f"kept distinct because {differentiating_axis} changes the product implication relative to {candidate_id}" if differentiating_axis else "kept distinct because no explicit differentiating axis could be collapsed safely",
            }
    return best


def _differentiating_axis(profile: dict[str, Any], candidate: dict[str, Any]) -> str:
    """Return the first material axis that differentiates two candidate personas."""
    for axis in ["role_context", "recurring_job", "expected_output", "workaround_pattern", "trust_failure_mode"]:
        if str(profile.get(axis, "")) != str(candidate.get(axis, "")):
            return axis
    return ""


def _dominant_value(group: pd.DataFrame, column: str) -> str:
    """Return the dominant non-unknown value for one column."""
    if column not in group.columns:
        return "unassigned"
    values = group[column].astype(str).str.strip()
    values = values[~values.map(is_unknown_like)]
    if values.empty:
        return "unassigned"
    return str(values.value_counts().idxmax())


def _overlap_role_family(role_value: str) -> str:
    """Map raw dominant role values to stable merge families."""
    mapping = {
        "analyst": "analyst_operator",
        "manager": "manager_operator",
        "marketer": "marketing_operator",
        "business_user": "business_operator",
    }
    return mapping.get(str(role_value).strip().lower(), "workflow_operator")


def _overlap_recurring_job_family(workflow: str, goal: str, output: str) -> str:
    """Collapse workflow stages into broader recurring jobs when product implication is shared."""
    workflow_key = str(workflow).strip().lower()
    goal_key = str(goal).strip().lower()
    output_key = str(output).strip().lower()
    if output_key == "dashboard_update" and goal_key in {"diagnose_change", "automate_workflow"}:
        return "maintain_and_explain_dashboard_outputs"
    if output_key == "dashboard_update" and workflow_key in {"triage", "automation"}:
        return "maintain_and_explain_dashboard_outputs"
    if goal_key == "report_speed" or workflow_key == "reporting":
        return "deliver_recurring_reporting_outputs"
    if goal_key == "validate_numbers" or workflow_key == "validation":
        return "validate_metrics_before_distribution"
    return f"{workflow_key or 'workflow'}::{goal_key or 'analysis'}"


def _overlap_expected_output_family(output: str) -> str:
    """Normalize expected output artifact families."""
    mapping = {
        "dashboard_update": "dashboard_update",
        "excel_ready_output": "stakeholder_ready_report",
        "automation_output": "repeatable_workflow_output",
    }
    return mapping.get(str(output).strip().lower(), str(output).strip().lower() or "shareable_output")


def _overlap_workaround_family(output: str, tool_mode: str, bottleneck: str, group: pd.DataFrame) -> str:
    """Collapse surface bottleneck wording into broader workaround families."""
    output_key = str(output).strip().lower()
    tool_key = str(tool_mode).strip().lower()
    bottleneck_key = str(bottleneck).strip().lower()
    manual_signals = set(_top_feature_labels(group, ["manual_reporting", "spreadsheet_rework", "recurring_export_work", "tool_limitation_workaround"], limit=3))
    if output_key == "dashboard_update" and (tool_key == "bi_dashboard_heavy" or bottleneck_key in {"tool_limitation", "manual_reporting", "general_friction"}):
        return "dashboard_workaround_patch"
    if tool_key == "spreadsheet_heavy" or bottleneck_key == "manual_reporting" or {"manual_reporting", "spreadsheet_rework"} & manual_signals:
        return "manual_patch_and_rebuild"
    if bottleneck_key in {"tool_limitation", "general_friction"} or "tool_limitation_workaround" in manual_signals:
        return "out_of_tool_workaround"
    return "workflow_patchwork"


def _overlap_trust_family(workflow: str, goal: str, output: str, bottleneck: str, group: pd.DataFrame) -> str:
    """Map trust/output failure into broad product-implication families."""
    workflow_key = str(workflow).strip().lower()
    goal_key = str(goal).strip().lower()
    output_key = str(output).strip().lower()
    bottleneck_key = str(bottleneck).strip().lower()
    top_features = set(_top_feature_labels(group, ["root_cause_analysis_difficulty", "numbers_visible_but_not_explainable", "dashboard_mistrust", "metric_reconciliation", "repeated_validation_before_sending"], limit=3))
    if goal_key == "validate_numbers" or workflow_key == "validation" or bottleneck_key == "data_quality" or {"metric_reconciliation", "dashboard_mistrust", "repeated_validation_before_sending"} & top_features:
        return "numbers_not_safe_to_share"
    if output_key == "dashboard_update" and (goal_key in {"diagnose_change", "automate_workflow"} or workflow_key in {"triage", "automation"}):
        return "visible_but_not_explainable"
    if bottleneck_key in {"manual_reporting", "tool_limitation", "general_friction"}:
        return "delivery_or_explanation_breakdown"
    return "workflow_confidence_gap"


def _top_feature_labels(group: pd.DataFrame, columns: list[str], limit: int) -> list[str]:
    """Return the highest-average feature labels from a grouped frame."""
    rows: list[tuple[str, float]] = []
    for column in columns:
        if column not in group.columns:
            continue
        score = float(pd.to_numeric(group[column], errors="coerce").fillna(0.0).mean())
        if score > 0.0:
            rows.append((column, score))
    rows.sort(key=lambda item: (-item[1], item[0]))
    return [label for label, _ in rows[:limit]]


def _overlap_parent_persona_name(role_context: str, recurring_job: str, expected_output: str) -> str:
    """Build a broader parent persona name for merged overlapping personas."""
    role_label = {
        "analyst_operator": "Analyst",
        "manager_operator": "Manager",
        "marketing_operator": "Marketing Operator",
        "business_operator": "Business Operator",
        "workflow_operator": "Workflow Operator",
    }.get(str(role_context), "Workflow Operator")
    job_label = {
        "maintain_and_explain_dashboard_outputs": "Dashboard Resolution Operator",
        "deliver_recurring_reporting_outputs": "Reporting Delivery Operator",
        "validate_metrics_before_distribution": "Metric Assurance Operator",
    }.get(str(recurring_job), _humanize_merge_token(recurring_job))
    return f"{role_label} {job_label}".strip()


def _overlap_naming_rationale(role_context: str, recurring_job: str, expected_output: str) -> str:
    """Explain the broader parent naming choice."""
    return (
        f"Named from stable user type and recurring job, not surface bottleneck wording: "
        f"role={role_context}, recurring_job={recurring_job}, expected_output={expected_output}"
    )


def _humanize_merge_token(value: str) -> str:
    """Humanize merge-profile tokens for audit-friendly naming."""
    return str(value or "workflow operator").replace("::", " ").replace("_", " ").title()


def build_bottleneck_feature_table(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    axis_wide_df: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    """Build interpretable bottleneck scores from labels, text, and reduced axes."""
    merged = (
        episodes_df.merge(labeled_df, on="episode_id", how="inner")
        .merge(axis_wide_df, on="episode_id", how="left")
        .fillna("")
    )
    if merged.empty:
        columns = ["episode_id", "primary_bottleneck", "secondary_bottlenecks", "cluster_signature", "primary_score", "role_metadata", "source_metadata"]
        columns.extend(_feature_columns(config))
        return pd.DataFrame(columns=columns)

    feature_weights = {str(key): float(value) for key, value in dict(config.get("feature_weights", {})).items()}
    label_signal_weights = dict(config.get("label_signal_weights", {}))
    axis_signal_weights = dict(config.get("axis_signal_weights", {}))
    compiled_patterns = {
        feature: [str(pattern).lower() for pattern in list(patterns or [])]
        for feature, patterns in dict(config.get("text_patterns", {})).items()
    }
    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        scores = {feature: 0.0 for feature in _feature_columns(config)}
        for label_column, value_map in label_signal_weights.items():
            for code in split_pipe_codes(row.get(label_column, "")):
                for feature, increment in dict(value_map.get(code, {})).items():
                    scores[str(feature)] += float(increment)
        for axis_name, value_map in axis_signal_weights.items():
            axis_value = str(row.get(axis_name, "")).strip()
            if not axis_value:
                continue
            for feature, increment in dict(value_map.get(axis_value, {})).items():
                scores[str(feature)] += float(increment)
        text_blob = _row_text_blob(row)
        for feature, patterns in compiled_patterns.items():
            for pattern in patterns:
                if pattern and pattern in text_blob:
                    scores[str(feature)] += 0.35
        weighted_scores = {
            feature: round(score * feature_weights.get(feature, 1.0), 4)
            for feature, score in scores.items()
        }
        ranked = sorted(weighted_scores.items(), key=lambda item: (-item[1], item[0]))
        primary_feature, primary_score = ranked[0] if ranked else ("mixed_workflow_friction", 0.0)
        rows.append(
            {
                "episode_id": str(row.get("episode_id", "")),
                **weighted_scores,
                "primary_bottleneck": primary_feature if primary_score >= float(config.get("clustering", {}).get("min_primary_score", 0.8)) else "mixed_workflow_friction",
                "secondary_bottlenecks": " | ".join(_secondary_features(ranked, config)),
                "cluster_signature": _signature_from_ranked(ranked, config),
                "primary_score": round(primary_score, 4),
                "role_metadata": _role_metadata(row),
                "source_metadata": str(row.get("source", "")),
            }
        )
    return pd.DataFrame(rows)


def assign_bottleneck_clusters(feature_df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Assign rows to bottleneck-first clusters using weighted signatures and anchor merging."""
    columns = [
        "episode_id",
        "persona_id",
        "cluster_name",
        "cluster_signature",
        "primary_bottleneck",
        "secondary_bottlenecks",
        "cluster_fit_score",
        "cluster_mode",
        "initial_anchor_signature",
        "robustness_action",
        "robustness_reason",
    ]
    if feature_df.empty:
        return pd.DataFrame(columns=columns)

    signature_counts = feature_df["cluster_signature"].astype(str).value_counts()
    total_rows = max(len(feature_df), 1)
    min_anchor_size = int(config.get("clustering", {}).get("min_anchor_size", 45))
    min_anchor_share = float(config.get("clustering", {}).get("min_anchor_share", 0.04))
    max_anchor_count = int(config.get("clustering", {}).get("max_anchor_count", 8))
    anchor_signatures = [
        signature
        for signature, count in signature_counts.items()
        if count >= min_anchor_size or (count / total_rows) >= min_anchor_share
    ]
    if not anchor_signatures:
        anchor_signatures = list(signature_counts.head(min(max_anchor_count, len(signature_counts))).index)
    anchor_signatures = anchor_signatures[:max_anchor_count]
    signature_to_anchor = {signature: signature for signature in anchor_signatures}
    anchor_feature_lookup = {
        signature: _signature_vector(feature_df[feature_df["cluster_signature"].astype(str) == signature], _feature_columns(config))
        for signature in anchor_signatures
    }
    for signature in signature_counts.index:
        if signature in signature_to_anchor:
            continue
        signature_rows = feature_df[feature_df["cluster_signature"].astype(str) == signature]
        vector = _signature_vector(signature_rows, _feature_columns(config))
        nearest_anchor = _nearest_signature_anchor(
            vector,
            anchor_feature_lookup,
            float(config.get("clustering", {}).get("merge_similarity_threshold", 0.72)),
        )
        signature_to_anchor[signature] = nearest_anchor or signature
        if not nearest_anchor and signature not in anchor_feature_lookup:
            anchor_feature_lookup[signature] = vector
            anchor_signatures.append(signature)

    naming_lookup = {}
    all_signatures = list(dict.fromkeys([*anchor_signatures, *signature_to_anchor.keys()]))
    for signature in all_signatures:
        mapping = _parse_signature(signature)
        naming_lookup[signature] = _recommended_cluster_name(mapping.get("primary", "mixed_workflow_friction"), mapping.get("secondary", []), config)
    canonical_anchors: list[str] = []
    anchor_canonical_map: dict[str, str] = {}
    merge_signature_floor = float(config.get("clustering", {}).get("merge_signature_floor", 0.52))
    require_shared_secondary = bool(config.get("clustering", {}).get("require_shared_secondary_for_primary_merge", True))
    for signature in anchor_signatures:
        mapping = _parse_signature(signature)
        matched_anchor = ""
        for canonical in canonical_anchors:
            canonical_mapping = _parse_signature(canonical)
            same_name = naming_lookup.get(signature, "") == naming_lookup.get(canonical, "")
            same_primary = mapping.get("primary", "") == canonical_mapping.get("primary", "")
            shared_secondary = bool(set(mapping.get("secondary", [])) & set(canonical_mapping.get("secondary", [])))
            similar = _cosine_similarity(anchor_feature_lookup.get(signature, {}), anchor_feature_lookup.get(canonical, {})) >= merge_signature_floor
            can_merge_primary = same_primary and similar and (shared_secondary or not require_shared_secondary)
            if same_name or can_merge_primary:
                matched_anchor = canonical
                break
        if matched_anchor:
            anchor_canonical_map[signature] = matched_anchor
        else:
            canonical_anchors.append(signature)
            anchor_canonical_map[signature] = signature
    signature_to_anchor = {signature: anchor_canonical_map.get(anchor, anchor) for signature, anchor in signature_to_anchor.items()}
    anchor_signatures = canonical_anchors
    robustness_policy = _apply_cluster_robustness_policy(feature_df, signature_to_anchor, anchor_signatures, anchor_feature_lookup, naming_lookup, config)
    signature_to_anchor = robustness_policy["signature_to_anchor"]
    anchor_feature_lookup = robustness_policy["anchor_feature_lookup"]
    naming_lookup = robustness_policy["naming_lookup"]
    anchor_signatures = robustness_policy["anchor_signatures"]
    anchor_to_id = {signature: f"persona_{index:02d}" for index, signature in enumerate(anchor_signatures, start=1)}
    working = feature_df.copy()
    working["initial_anchor_signature"] = working["cluster_signature"].astype(str).map(anchor_canonical_map).fillna(working["cluster_signature"].astype(str))
    working["persona_signature"] = working["cluster_signature"].astype(str).map(signature_to_anchor)
    working["persona_id"] = working["persona_signature"].map(anchor_to_id)
    working["cluster_name"] = working["persona_signature"].map(naming_lookup)
    working["cluster_fit_score"] = working.apply(
        lambda row: _cluster_fit_score(row, anchor_feature_lookup.get(str(row["persona_signature"]), {}), _feature_columns(config)),
        axis=1,
    )
    action_lookup = robustness_policy["action_lookup"]
    reason_lookup = robustness_policy["reason_lookup"]
    working["robustness_action"] = working["initial_anchor_signature"].astype(str).map(action_lookup).fillna("kept_stable")
    working["robustness_reason"] = working["initial_anchor_signature"].astype(str).map(reason_lookup).fillna("cluster met stability thresholds")
    working["cluster_mode"] = "bottleneck_first_robust"
    return working[columns]


def build_cluster_robustness_outputs(
    cluster_audit_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Summarize cluster stability, concentration, and evidence sufficiency."""
    if cluster_audit_df.empty or persona_assignments_df.empty:
        empty_audit = pd.DataFrame(
            columns=[
                "persona_id",
                "cluster_name",
                "cluster_size",
                "share_of_core_labeled",
                "pre_merge_anchor_count",
                "stability_status",
                "evidence_status",
                "structural_support_status",
                "weak_separation_status",
                "concentration_status",
                "tail_fragility_status",
                "robustness_action_summary",
                "cohesion",
                "separation",
                "nearest_neighbor_similarity",
            ]
        )
        empty_summary = pd.DataFrame(columns=["metric", "value"])
        return {"audit_df": empty_audit, "summary_df": empty_summary}

    total_rows = max(int(cluster_audit_df["cluster_size"].sum()), 1)
    robustness_config = dict(config.get("robustness", {}))
    min_stable_cluster_size = int(robustness_config.get("min_stable_cluster_size", config.get("clustering", {}).get("min_anchor_size", 24)))
    min_stable_cluster_share = float(robustness_config.get("min_stable_cluster_share", 0.06))
    sufficient_separation_floor = float(robustness_config.get("sufficient_separation_floor", 0.12))
    sufficient_cohesion_floor = float(robustness_config.get("sufficient_cohesion_floor", 0.9))

    action_counts = (
        persona_assignments_df.groupby("persona_id", dropna=False)["robustness_action"]
        .value_counts()
        .unstack(fill_value=0)
    )
    initial_anchor_counts = persona_assignments_df.groupby("persona_id", dropna=False)["initial_anchor_signature"].nunique()
    largest_cluster_size = int(cluster_audit_df["cluster_size"].max()) if not cluster_audit_df.empty else 0

    rows: list[dict[str, Any]] = []
    for _, row in cluster_audit_df.iterrows():
        persona_id = str(row.get("persona_id", ""))
        cluster_size = int(row.get("cluster_size", 0) or 0)
        share_of_core_labeled = round(cluster_size / total_rows, 4)
        cohesion = float(row.get("cohesion", 0.0) or 0.0)
        separation = float(row.get("separation", 0.0) or 0.0)
        if cluster_size >= min_stable_cluster_size or share_of_core_labeled >= min_stable_cluster_share:
            stability_status = "stable"
        elif cluster_size <= max(1, min_stable_cluster_size // 3):
            stability_status = "micro"
        else:
            stability_status = "fragile"
        evidence_status = "sufficient" if cohesion >= sufficient_cohesion_floor and separation >= sufficient_separation_floor else "thin"
        structural_support_status = "structurally_supported" if stability_status == "stable" and evidence_status == "sufficient" else "review_visible_only"
        weak_separation_status = "weakly_separated" if separation < sufficient_separation_floor else "separation_clear"
        concentration_status = "dominant" if cluster_size == largest_cluster_size and share_of_core_labeled >= 0.4 else "distributed"
        tail_fragility_status = "tail_fragile" if stability_status in {"fragile", "micro"} else "tail_clear"
        actions = action_counts.loc[persona_id] if persona_id in action_counts.index else pd.Series(dtype=int)
        action_summary = " | ".join(f"{action}:{int(count)}" for action, count in actions.items() if int(count) > 0)
        rows.append(
            {
                "persona_id": persona_id,
                "cluster_name": str(row.get("cluster_name", "")),
                "cluster_size": cluster_size,
                "share_of_core_labeled": share_of_core_labeled,
                "pre_merge_anchor_count": int(initial_anchor_counts.get(persona_id, 0)),
                "stability_status": stability_status,
                "evidence_status": evidence_status,
                "structural_support_status": structural_support_status,
                "weak_separation_status": weak_separation_status,
                "concentration_status": concentration_status,
                "tail_fragility_status": tail_fragility_status,
                "robustness_action_summary": action_summary,
                "cohesion": round(cohesion, 4),
                "separation": round(separation, 4),
                "nearest_neighbor_similarity": round(max(1.0 - separation, 0.0), 4),
            }
        )
    audit_df = pd.DataFrame(rows).sort_values(["cluster_size", "persona_id"], ascending=[False, True]).reset_index(drop=True)
    summary_df = _cluster_robustness_summary(audit_df)
    return {"audit_df": audit_df, "summary_df": summary_df}


def _apply_cluster_robustness_policy(
    feature_df: pd.DataFrame,
    signature_to_anchor: dict[str, str],
    anchor_signatures: list[str],
    anchor_feature_lookup: dict[str, dict[str, float]],
    naming_lookup: dict[str, str],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Absorb fragile adjacent clusters and collapse only the smallest residual fragments."""
    robustness_config = dict(config.get("robustness", {}))
    total_rows = max(len(feature_df), 1)
    feature_columns = _feature_columns(config)
    min_stable_cluster_size = int(robustness_config.get("min_stable_cluster_size", config.get("clustering", {}).get("min_anchor_size", 24)))
    min_stable_cluster_share = float(robustness_config.get("min_stable_cluster_share", 0.06))
    micro_cluster_size = int(robustness_config.get("micro_cluster_size", 8))
    same_primary_absorb_similarity = float(robustness_config.get("same_primary_absorb_similarity", 0.8))
    same_base_name_absorb_similarity = float(robustness_config.get("same_base_name_absorb_similarity", 0.72))
    same_name_absorb_similarity = float(robustness_config.get("same_name_absorb_similarity", 0.78))
    general_absorb_similarity = float(robustness_config.get("general_absorb_similarity", 0.9))
    low_separation_absorb_similarity = float(robustness_config.get("low_separation_absorb_similarity", 0.955))
    low_separation_ceiling = float(robustness_config.get("low_separation_ceiling", 0.05))
    low_separation_max_share = float(robustness_config.get("low_separation_max_share", 0.3))
    residual_bucket_by_primary = bool(robustness_config.get("residual_bucket_by_primary", True))
    residual_bucket_max_count = int(robustness_config.get("residual_bucket_max_count", 4))
    residual_fragile_cluster_size = int(robustness_config.get("residual_fragile_cluster_size", max(micro_cluster_size, 14)))

    resolved_signatures = list(dict.fromkeys(signature_to_anchor.values()))
    anchor_sizes = feature_df["cluster_signature"].astype(str).map(signature_to_anchor).value_counts().to_dict()
    action_lookup = {signature: "kept_stable" for signature in resolved_signatures}
    reason_lookup = {signature: "cluster met stability thresholds" for signature in resolved_signatures}
    final_anchor_map = {signature: signature for signature in resolved_signatures}
    residual_keys: list[str] = []

    candidate_order = sorted(
        resolved_signatures,
        key=lambda signature: (anchor_sizes.get(signature, 0), signature),
    )
    for signature in candidate_order:
        cluster_size = int(anchor_sizes.get(signature, 0))
        share = cluster_size / total_rows
        if cluster_size >= min_stable_cluster_size or share >= min_stable_cluster_share:
            continue
        merge_target = _choose_cluster_absorption_target(
            signature,
            anchor_sizes,
            anchor_feature_lookup,
            naming_lookup,
            same_primary_absorb_similarity,
            same_base_name_absorb_similarity,
            same_name_absorb_similarity,
            general_absorb_similarity,
        )
        if merge_target:
            final_anchor_map[signature] = merge_target
            action_lookup[signature] = "merged_to_parent"
            reason_lookup[signature] = f"absorbed into adjacent stable cluster {naming_lookup.get(merge_target, merge_target)}"
            anchor_sizes[merge_target] = int(anchor_sizes.get(merge_target, 0)) + cluster_size
            anchor_sizes[signature] = 0
            continue
        if cluster_size <= residual_fragile_cluster_size:
            residual_key = _residual_anchor_key(signature, residual_bucket_by_primary)
            if residual_key not in residual_keys and len(residual_keys) < residual_bucket_max_count:
                residual_keys.append(residual_key)
            elif residual_key not in residual_keys and residual_keys:
                residual_key = residual_keys[-1]
            elif residual_key not in residual_keys:
                residual_keys.append(residual_key)
            final_anchor_map[signature] = residual_key
            action_lookup[signature] = "collapsed_to_residual"
            reason_lookup[signature] = "insufficient standalone evidence; collapsed into exploratory residual family"
            anchor_sizes[residual_key] = int(anchor_sizes.get(residual_key, 0)) + cluster_size
            anchor_sizes[signature] = 0

    updated_signature_to_anchor = {
        signature: final_anchor_map.get(anchor, anchor)
        for signature, anchor in signature_to_anchor.items()
    }

    while True:
        current_anchor_signatures = list(dict.fromkeys(updated_signature_to_anchor.values()))
        current_anchor_sizes = feature_df["cluster_signature"].astype(str).map(updated_signature_to_anchor).value_counts().to_dict()
        current_feature_lookup = {
            anchor: _signature_vector(
                feature_df[feature_df["cluster_signature"].astype(str).map(updated_signature_to_anchor) == anchor],
                feature_columns,
            )
            for anchor in current_anchor_signatures
        }
        candidate = _choose_low_separation_absorption_candidate(
            anchor_signatures=current_anchor_signatures,
            anchor_sizes=current_anchor_sizes,
            anchor_feature_lookup=current_feature_lookup,
            naming_lookup=naming_lookup,
            total_rows=total_rows,
            low_separation_absorb_similarity=low_separation_absorb_similarity,
            low_separation_ceiling=low_separation_ceiling,
            low_separation_max_share=low_separation_max_share,
        )
        if not candidate:
            break
        signature, merge_target, similarity = candidate
        updated_signature_to_anchor = {
            item_signature: merge_target if anchor == signature else anchor
            for item_signature, anchor in updated_signature_to_anchor.items()
        }
        action_lookup[signature] = "merged_low_separation_cluster"
        reason_lookup[signature] = (
            f"merged into adjacent cluster {naming_lookup.get(merge_target, merge_target)} "
            f"because centroid similarity {similarity:.4f} implied weak separability"
        )

    anchor_sizes = feature_df["cluster_signature"].astype(str).map(updated_signature_to_anchor).value_counts().to_dict()

    final_anchor_signatures = list(
        dict.fromkeys(
            sorted(
                {anchor for anchor in updated_signature_to_anchor.values()},
                key=lambda anchor: (-int(anchor_sizes.get(anchor, 0)), str(anchor)),
            )
        )
    )
    updated_naming_lookup = dict(naming_lookup)
    for residual_key in residual_keys:
        updated_naming_lookup[residual_key] = _residual_cluster_name(residual_key, config)
    updated_anchor_feature_lookup = dict(anchor_feature_lookup)
    for anchor in final_anchor_signatures:
        cluster_rows = feature_df[feature_df["cluster_signature"].astype(str).map(updated_signature_to_anchor) == anchor]
        updated_anchor_feature_lookup[anchor] = _signature_vector(cluster_rows, feature_columns)
        if anchor not in updated_naming_lookup:
            mapping = _parse_signature(anchor)
            updated_naming_lookup[anchor] = _recommended_cluster_name(mapping.get("primary", "mixed_workflow_friction"), mapping.get("secondary", []), config)
    return {
        "signature_to_anchor": updated_signature_to_anchor,
        "anchor_feature_lookup": updated_anchor_feature_lookup,
        "naming_lookup": updated_naming_lookup,
        "anchor_signatures": final_anchor_signatures,
        "action_lookup": action_lookup,
        "reason_lookup": reason_lookup,
    }


def _choose_cluster_absorption_target(
    signature: str,
    anchor_sizes: dict[str, int],
    anchor_feature_lookup: dict[str, dict[str, float]],
    naming_lookup: dict[str, str],
    same_primary_absorb_similarity: float,
    same_base_name_absorb_similarity: float,
    same_name_absorb_similarity: float,
    general_absorb_similarity: float,
) -> str:
    """Choose the best stable parent for a fragile cluster when similarity is strong enough."""
    mapping = _parse_signature(signature)
    best_target = ""
    best_score = -1.0
    for candidate, candidate_size in anchor_sizes.items():
        if candidate == signature or candidate_size <= anchor_sizes.get(signature, 0):
            continue
        candidate_mapping = _parse_signature(candidate)
        similarity = _cosine_similarity(anchor_feature_lookup.get(signature, {}), anchor_feature_lookup.get(candidate, {}))
        same_primary = mapping.get("primary", "") == candidate_mapping.get("primary", "")
        same_name = naming_lookup.get(signature, "") == naming_lookup.get(candidate, "")
        same_base_name = _base_cluster_name(naming_lookup.get(signature, "")) == _base_cluster_name(naming_lookup.get(candidate, ""))
        shared_secondary = bool(set(mapping.get("secondary", [])) & set(candidate_mapping.get("secondary", [])))
        can_absorb = False
        if same_primary and similarity >= same_primary_absorb_similarity:
            can_absorb = True
        elif same_base_name and similarity >= same_base_name_absorb_similarity:
            can_absorb = True
        elif same_name and similarity >= same_name_absorb_similarity:
            can_absorb = True
        elif shared_secondary and similarity >= general_absorb_similarity:
            can_absorb = True
        if can_absorb and similarity > best_score:
            best_target = candidate
            best_score = similarity
    return best_target


def _residual_anchor_key(signature: str, residual_bucket_by_primary: bool) -> str:
    """Build a constrained residual family key for the smallest orphan clusters."""
    if not residual_bucket_by_primary:
        return "residual::exploratory"
    primary = _parse_signature(signature).get("primary", "mixed_workflow_friction")
    return f"residual::{primary}"


def _residual_cluster_name(residual_key: str, config: dict[str, Any]) -> str:
    """Build a readable name for a residual exploratory family."""
    primary = str(residual_key).split("::", 1)[-1] if "::" in str(residual_key) else "mixed_workflow_friction"
    if primary == "exploratory":
        return "Exploratory Residual"
    base = _recommended_cluster_name(primary, [], config)
    return f"Exploratory {base} Residual"


def _base_cluster_name(cluster_name: str) -> str:
    """Return the leading bottleneck-family label from a composed cluster name."""
    return str(cluster_name or "").split(" + ", 1)[0].strip()


def _cluster_robustness_summary(cluster_robustness_df: pd.DataFrame) -> pd.DataFrame:
    """Produce compact overview metrics for clustering robustness."""
    if cluster_robustness_df.empty:
        return pd.DataFrame(columns=["metric", "value"])
    shares = pd.to_numeric(cluster_robustness_df["share_of_core_labeled"], errors="coerce").fillna(0.0)
    fragile_tail_mask = cluster_robustness_df["tail_fragility_status"].astype(str).eq("tail_fragile")
    weak_separation_mask = cluster_robustness_df["weak_separation_status"].astype(str).eq("weakly_separated")
    metrics = [
        {"metric": "robust_cluster_count", "value": int(len(cluster_robustness_df))},
        {"metric": "stable_cluster_count", "value": int((cluster_robustness_df["stability_status"] == "stable").sum())},
        {"metric": "fragile_cluster_count", "value": int((cluster_robustness_df["stability_status"] == "fragile").sum())},
        {"metric": "micro_cluster_count", "value": int((cluster_robustness_df["stability_status"] == "micro").sum())},
        {"metric": "thin_evidence_cluster_count", "value": int((cluster_robustness_df["evidence_status"] == "thin").sum())},
        {"metric": "structurally_supported_cluster_count", "value": int((cluster_robustness_df["structural_support_status"] == "structurally_supported").sum())},
        {"metric": "weak_separation_cluster_count", "value": int(weak_separation_mask.sum())},
        {"metric": "fragile_tail_cluster_count", "value": int(fragile_tail_mask.sum())},
        {"metric": "fragile_tail_share_of_core_labeled", "value": round(float(shares[fragile_tail_mask].sum()), 4)},
        {"metric": "largest_cluster_share_of_core_labeled", "value": round(float(shares.max()), 4)},
        {"metric": "top_3_cluster_share_of_core_labeled", "value": round(float(shares.sort_values(ascending=False).head(3).sum()), 4)},
        {"metric": "avg_cluster_separation", "value": round(float(pd.to_numeric(cluster_robustness_df["separation"], errors="coerce").fillna(0.0).mean()), 4)},
        {"metric": "min_cluster_separation", "value": round(float(pd.to_numeric(cluster_robustness_df["separation"], errors="coerce").fillna(0.0).min()), 4)},
    ]
    return pd.DataFrame(metrics)


def _choose_low_separation_absorption_candidate(
    anchor_signatures: list[str],
    anchor_sizes: dict[str, int],
    anchor_feature_lookup: dict[str, dict[str, float]],
    naming_lookup: dict[str, str],
    total_rows: int,
    low_separation_absorb_similarity: float,
    low_separation_ceiling: float,
    low_separation_max_share: float,
) -> tuple[str, str, float] | None:
    """Choose one near-duplicate cluster merge when centroids are too similar to justify separation."""
    ordered = sorted(anchor_signatures, key=lambda signature: (anchor_sizes.get(signature, 0), signature))
    for signature in ordered:
        cluster_size = int(anchor_sizes.get(signature, 0))
        if cluster_size <= 0:
            continue
        share = cluster_size / max(total_rows, 1)
        if share > low_separation_max_share:
            continue
        best_target = ""
        best_similarity = -1.0
        mapping = _parse_signature(signature)
        for candidate in ordered:
            candidate_size = int(anchor_sizes.get(candidate, 0))
            if candidate == signature or candidate_size <= cluster_size:
                continue
            candidate_mapping = _parse_signature(candidate)
            similarity = _cosine_similarity(anchor_feature_lookup.get(signature, {}), anchor_feature_lookup.get(candidate, {}))
            separation = max(1.0 - similarity, 0.0)
            same_primary = mapping.get("primary", "") == candidate_mapping.get("primary", "")
            same_name = naming_lookup.get(signature, "") == naming_lookup.get(candidate, "")
            same_base_name = _base_cluster_name(naming_lookup.get(signature, "")) == _base_cluster_name(naming_lookup.get(candidate, ""))
            shared_secondary = bool(set(mapping.get("secondary", [])) & set(candidate_mapping.get("secondary", [])))
            if similarity < low_separation_absorb_similarity or separation > low_separation_ceiling:
                continue
            if not (same_primary or same_name or same_base_name or shared_secondary):
                continue
            if similarity > best_similarity:
                best_target = candidate
                best_similarity = similarity
        if best_target:
            return signature, best_target, best_similarity
    return None


def build_cluster_meaning_audit(
    merged_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame,
    selected_examples_df: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    """Build cluster-level meaning audits focused on bottleneck clarity and role leakage."""
    if persona_assignments_df.empty:
        return pd.DataFrame()
    if "persona_id" in merged_df.columns and "cluster_name" in merged_df.columns:
        joined = merged_df.copy()
    else:
        joined = merged_df.merge(
            persona_assignments_df[["episode_id", "persona_id", "cluster_name", "cluster_fit_score"]],
            on="episode_id",
            how="inner",
        )
    feature_lookup = feature_df.set_index("episode_id")
    example_lookup = (
        selected_examples_df.groupby("persona_id")["grounded_text"].apply(lambda values: list(values[:3])).to_dict()
        if selected_examples_df is not None and not selected_examples_df.empty
        else {}
    )
    centroid_lookup = _cluster_centroids(joined, feature_lookup, _feature_columns(config))
    rows: list[dict[str, Any]] = []
    for persona_id, group in joined.groupby("persona_id", dropna=False):
        vectors = _group_vectors(group["episode_id"].astype(str).tolist(), feature_lookup, _feature_columns(config))
        centroid = centroid_lookup.get(str(persona_id), {})
        dominant_role = _distribution(group.get("user_role", pd.Series(dtype=str)))
        dominant_source = _distribution(group.get("source", pd.Series(dtype=str)))
        ranked_features = _rank_cluster_features(vectors)
        top_bottlenecks = [feature for feature, _ in ranked_features[:4]]
        top_signals = [feature for feature, score in ranked_features if score >= 1.0][:6]
        cohesion = _cluster_cohesion(vectors, centroid)
        separation = _cluster_separation(str(persona_id), centroid_lookup)
        role_dominance = dominant_role[0]["share"] if dominant_role else 0.0
        bottleneck_dominance = ranked_features[0][1] / max(sum(score for _, score in ranked_features[:5]), 1e-9) if ranked_features else 0.0
        rows.append(
            {
                "persona_id": str(persona_id),
                "cluster_name": str(group["cluster_name"].iloc[0]),
                "cluster_size": int(group["episode_id"].nunique()),
                "dominant_bottleneck_signals": " | ".join(top_bottlenecks),
                "dominant_output_need_signals": " | ".join([feature for feature in top_signals if feature in {"recurring_export_work", "presentation_ready_output_need"}][:3]),
                "dominant_trust_reporting_signals": " | ".join([feature for feature in top_signals if feature in {"dashboard_mistrust", "metric_reconciliation", "repeated_validation_before_sending", "reporting_deadline_pressure"}][:4]),
                "dominant_manual_work_signals": " | ".join([feature for feature in top_signals if feature in {"manual_reporting", "spreadsheet_rework", "tool_limitation_workaround"}][:4]),
                "role_distribution_json": json.dumps(dominant_role, ensure_ascii=False),
                "source_distribution_json": json.dumps(dominant_source, ensure_ascii=False),
                "cohesion": round(cohesion, 4),
                "separation": round(separation, 4),
                "bottleneck_coherence": round(bottleneck_dominance, 4),
                "role_dominance": round(role_dominance, 4),
                "cross_cluster_distinctiveness": round((cohesion + separation) / 2, 4),
                "representative_examples": " | ".join(example_lookup.get(str(persona_id), [])),
                "why_this_cluster_is_distinct": _cluster_distinct_reason(top_bottlenecks, role_dominance, separation),
            }
        )
    return pd.DataFrame(rows).sort_values(["cluster_size", "persona_id"], ascending=[False, True]).reset_index(drop=True)


def build_cluster_naming_recommendations(cluster_audit_df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Recommend bottleneck-first names from dominant cluster signals."""
    rows: list[dict[str, Any]] = []
    for _, row in cluster_audit_df.iterrows():
        top_bottlenecks = [item.strip() for item in str(row.get("dominant_bottleneck_signals", "")).split("|") if item.strip()]
        primary = top_bottlenecks[0] if top_bottlenecks else "mixed_workflow_friction"
        secondary = top_bottlenecks[1:3]
        rows.append(
            {
                "persona_id": str(row.get("persona_id", "")),
                "current_cluster_name": str(row.get("cluster_name", "")),
                "recommended_cluster_name": _recommended_cluster_name(primary, secondary, config),
                "primary_bottleneck": primary,
                "secondary_bottlenecks": " | ".join(secondary),
                "why_name": f"Name anchored on recurring workflow burden: {primary}" + (f" with {', '.join(secondary)} context" if secondary else ""),
            }
        )
    return pd.DataFrame(rows)


def compare_cluster_versions(
    axis_wide_df: pd.DataFrame,
    merged_df: pd.DataFrame,
    new_assignments_df: pd.DataFrame,
    final_axis_schema: list[dict[str, Any]],
    cluster_audit_df: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Compare old signature clusters with new bottleneck-first clusters."""
    core_axes = [
        str(row.get("axis_name", "")).strip()
        for row in final_axis_schema
        if str(row.get("axis_name", "")).strip() and str(row.get("axis_role", "core")) == "core"
    ]
    if not core_axes:
        core_axes = [str(row.get("axis_name", "")).strip() for row in final_axis_schema if str(row.get("axis_name", "")).strip()]
    legacy_assignments_df = _legacy_assign_personas(axis_wide_df, core_axes)
    legacy_merged = merged_df.merge(
        legacy_assignments_df[["episode_id", "persona_id"]].rename(columns={"persona_id": "legacy_persona_id"}),
        on="episode_id",
        how="left",
    )
    old_summary_df = _versioned_cluster_summary(legacy_merged, "before")
    if "persona_id" in merged_df.columns and "cluster_name" in merged_df.columns:
        new_summary_source = merged_df.copy()
    else:
        new_summary_source = merged_df.merge(new_assignments_df[["episode_id", "persona_id", "cluster_name"]], on="episode_id", how="left")
    new_summary_df = _versioned_cluster_summary(new_summary_source, "after")
    comparison_df = pd.DataFrame(
        [
            {"metric_name": "cluster_count", "before_value": int(old_summary_df["cluster_id"].nunique()), "after_value": int(new_summary_df["cluster_id"].nunique())},
            {"metric_name": "avg_role_dominance", "before_value": round(_avg_role_dominance(old_summary_df), 4), "after_value": round(_avg_role_dominance(new_summary_df), 4)},
            {"metric_name": "avg_bottleneck_coherence", "before_value": round(_avg_bottleneck_coherence(old_summary_df), 4), "after_value": round(_avg_bottleneck_coherence(cluster_audit_df.rename(columns={"persona_id": "cluster_id"})), 4)},
            {"metric_name": "clusters_with_problem_names", "before_value": int((old_summary_df.get("cluster_name", pd.Series(dtype=str)).astype(str).str.contains("Burden|Conflict|Workaround|Pressure", regex=True)).sum()), "after_value": int((cluster_audit_df.get("cluster_name", pd.Series(dtype=str)).astype(str).str.contains("Burden|Conflict|Workaround|Pressure", regex=True)).sum())},
        ]
    )
    comparison_df["delta"] = comparison_df.apply(lambda row: round(float(row["after_value"]) - float(row["before_value"]), 4), axis=1)
    role_feature_importance_df = pd.DataFrame(
        [
            {
                "metric": "role_cluster_dependency",
                "before_value": _role_importance(legacy_merged, "legacy_persona_id"),
                "after_value": _role_importance(new_summary_source, "persona_id"),
            },
            {
                "metric": "avg_role_dominance",
                "before_value": round(_avg_role_dominance(old_summary_df), 4),
                "after_value": round(_avg_role_dominance(new_summary_df), 4),
            },
        ]
    )
    role_feature_importance_df["delta"] = role_feature_importance_df.apply(lambda row: round(float(row["after_value"]) - float(row["before_value"]), 4), axis=1)
    return {
        "legacy_assignments_df": legacy_assignments_df,
        "old_vs_new_cluster_summary_df": pd.concat([old_summary_df, new_summary_df], ignore_index=True),
        "role_feature_importance_before_after_df": role_feature_importance_df,
        "bottleneck_feature_importance_df": _bottleneck_feature_importance(merged_df, new_assignments_df, config),
        "cluster_comparison_before_after_df": comparison_df,
        "cluster_comparison_before_after_md": _comparison_markdown(comparison_df),
    }


def build_bottleneck_cluster_profiles(cluster_audit_df: pd.DataFrame, selected_examples_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert cluster audits into persona-generation-ready profiles."""
    example_lookup = (
        selected_examples_df.groupby("persona_id")["grounded_text"].apply(list).to_dict()
        if selected_examples_df is not None and not selected_examples_df.empty
        else {}
    )
    profiles: list[dict[str, Any]] = []
    total = max(int(cluster_audit_df.get("cluster_size", pd.Series(dtype=int)).sum()) if not cluster_audit_df.empty else 0, 1)
    for _, row in cluster_audit_df.iterrows():
        profiles.append(
            {
                "cluster_id": str(row.get("persona_id", "")),
                "recommended_name": str(row.get("cluster_name", "")),
                "size": int(row.get("cluster_size", 0)),
                "share_of_total": round(int(row.get("cluster_size", 0)) / total, 6),
                "top_demographics": [item.get("label", "") for item in json.loads(str(row.get("role_distribution_json", "[]") or "[]"))[:3]],
                "top_need_codes": [item.strip() for item in str(row.get("dominant_bottleneck_signals", "")).split("|") if item.strip()][:5],
                "top_outputs": [item.strip() for item in str(row.get("dominant_output_need_signals", "")).split("|") if item.strip()][:4],
                "top_envs": [],
                "representative_texts": example_lookup.get(str(row.get("persona_id", "")), [])[:5],
                "top_bottlenecks": [item.strip() for item in str(row.get("dominant_bottleneck_signals", "")).split("|") if item.strip()][:4],
                "cohesion": float(row.get("cohesion", 0.0)),
                "separation": float(row.get("separation", 0.0)),
            }
        )
    return profiles


def render_cluster_examples_markdown(selected_examples_df: pd.DataFrame, naming_df: pd.DataFrame) -> str:
    """Render representative examples grouped by the new cluster names."""
    if selected_examples_df.empty:
        return "# Representative Examples By New Cluster\n\n_No representative examples available._\n"
    name_lookup = naming_df.set_index("persona_id")["recommended_cluster_name"].to_dict() if not naming_df.empty else {}
    sections = ["# Representative Examples By New Cluster", ""]
    for persona_id, group in selected_examples_df.groupby("persona_id", dropna=False):
        sections.append(f"## {name_lookup.get(str(persona_id), str(persona_id))}")
        sections.append("")
        for _, row in group.sort_values("example_rank").iterrows():
            sections.append(f"- {row.get('grounded_text', '')}")
        sections.append("")
    return "\n".join(sections).strip() + "\n"


def export_cluster_examples(selected_examples_df: pd.DataFrame, cluster_id: str, output_path: Path) -> Path:
    """Export representative examples for one cluster."""
    filtered = selected_examples_df[selected_examples_df["persona_id"].astype(str) == str(cluster_id)].copy()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(output_path, index=False)
    return output_path


def _feature_columns(config: dict[str, Any]) -> list[str]:
    """Return configured bottleneck feature columns in a stable order."""
    return [str(item) for item in list(config.get("core_features", []))]


def _secondary_features(ranked: list[tuple[str, float]], config: dict[str, Any]) -> list[str]:
    """Return secondary bottleneck features strong enough to define context."""
    if not ranked:
        return []
    primary_score = float(ranked[0][1])
    score_floor = float(config.get("clustering", {}).get("secondary_score_floor", 0.9))
    ratio_floor = float(config.get("clustering", {}).get("secondary_ratio_floor", 0.55))
    max_context_features = int(config.get("clustering", {}).get("max_context_features", 2))
    context_whitelist = set(str(item) for item in list(config.get("cluster_context_features", [])))
    secondary = [
        feature
        for feature, score in ranked[1:]
        if feature in context_whitelist and score >= score_floor and score >= (primary_score * ratio_floor)
    ]
    return secondary[:max_context_features]


def _signature_from_ranked(ranked: list[tuple[str, float]], config: dict[str, Any]) -> str:
    """Build a deterministic cluster signature from primary and secondary bottlenecks."""
    if not ranked:
        return "primary=mixed_workflow_friction"
    primary_feature, primary_score = ranked[0]
    if primary_score < float(config.get("clustering", {}).get("min_primary_score", 0.8)):
        return "primary=mixed_workflow_friction"
    secondary = _secondary_features(ranked, config)
    if secondary:
        return f"primary={primary_feature}||secondary={'|'.join(sorted(secondary))}"
    return f"primary={primary_feature}"


def _parse_signature(signature: str) -> dict[str, Any]:
    """Parse a cluster signature string into a structured mapping."""
    mapping: dict[str, Any] = {"primary": "mixed_workflow_friction", "secondary": []}
    for part in str(signature or "").split("||"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key == "primary":
            mapping["primary"] = value
        elif key == "secondary":
            mapping["secondary"] = [item for item in value.split("|") if item]
    return mapping


def _row_text_blob(row: pd.Series) -> str:
    """Build one lowercased text blob from grounded episode context."""
    columns = [
        "normalized_episode",
        "evidence_snippet",
        "role_clue",
        "work_moment",
        "business_question",
        "tool_env",
        "bottleneck_text",
        "workaround_text",
        "desired_output",
        "segmentation_note",
    ]
    return " ".join(str(row.get(column, "") or "").strip().lower() for column in columns if str(row.get(column, "") or "").strip())


def _role_metadata(row: pd.Series) -> str:
    """Build a compact role metadata value without using it for clustering."""
    user_role = str(row.get("user_role", "")).strip()
    if user_role and not is_unknown_like(user_role):
        return user_role
    codes = split_pipe_codes(row.get("role_codes", ""))
    if "R_MARKETER" in codes:
        return "marketer"
    if "R_MANAGER" in codes:
        return "manager"
    if "R_ANALYST" in codes:
        return "analyst"
    return "unassigned"


def _signature_vector(df: pd.DataFrame, feature_columns: list[str]) -> dict[str, float]:
    """Return mean feature scores for a signature slice."""
    if df.empty:
        return {feature: 0.0 for feature in feature_columns}
    return {feature: round(float(pd.to_numeric(df[feature], errors="coerce").fillna(0).mean()), 6) for feature in feature_columns}


def _nearest_signature_anchor(vector: dict[str, float], anchor_lookup: dict[str, dict[str, float]], threshold: float) -> str:
    """Map a sparse signature to the nearest anchor based on cosine similarity."""
    if not anchor_lookup:
        return ""
    best_anchor = next(iter(anchor_lookup.keys()))
    best_score = -1.0
    for anchor, anchor_vector in anchor_lookup.items():
        score = _cosine_similarity(vector, anchor_vector)
        if score > best_score:
            best_anchor = anchor
            best_score = score
    return best_anchor if best_score >= threshold else ""


def _cluster_fit_score(row: pd.Series, centroid: dict[str, float], feature_columns: list[str]) -> float:
    """Compute fit between a row vector and its assigned cluster centroid."""
    vector = {feature: float(row.get(feature, 0.0) or 0.0) for feature in feature_columns}
    return round(_cosine_similarity(vector, centroid), 4)


def _cluster_centroids(joined: pd.DataFrame, feature_lookup: pd.DataFrame, feature_columns: list[str]) -> dict[str, dict[str, float]]:
    """Build cluster centroid vectors from assigned rows."""
    centroids: dict[str, dict[str, float]] = {}
    for persona_id, group in joined.groupby("persona_id", dropna=False):
        centroids[str(persona_id)] = _group_vector_mean(group["episode_id"].astype(str).tolist(), feature_lookup, feature_columns)
    return centroids


def _group_vectors(episode_ids: list[str], feature_lookup: pd.DataFrame, feature_columns: list[str]) -> list[dict[str, float]]:
    """Collect row vectors for one cluster."""
    vectors: list[dict[str, float]] = []
    for episode_id in episode_ids:
        if episode_id not in feature_lookup.index:
            continue
        row = feature_lookup.loc[episode_id]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        vectors.append({feature: float(pd.to_numeric(row.get(feature, 0.0), errors="coerce") if feature in row.index else 0.0) for feature in feature_columns})
    return vectors


def _group_vector_mean(episode_ids: list[str], feature_lookup: pd.DataFrame, feature_columns: list[str]) -> dict[str, float]:
    """Average vectors for one cluster."""
    vectors = _group_vectors(episode_ids, feature_lookup, feature_columns)
    if not vectors:
        return {feature: 0.0 for feature in feature_columns}
    return {feature: round(sum(vector.get(feature, 0.0) for vector in vectors) / len(vectors), 6) for feature in feature_columns}


def _rank_cluster_features(vectors: list[dict[str, float]]) -> list[tuple[str, float]]:
    """Rank bottleneck features by average cluster score."""
    if not vectors:
        return []
    feature_names = sorted(vectors[0].keys())
    rows = [(feature, round(sum(vector.get(feature, 0.0) for vector in vectors) / len(vectors), 4)) for feature in feature_names]
    return sorted(rows, key=lambda item: (-item[1], item[0]))


def _cluster_cohesion(vectors: list[dict[str, float]], centroid: dict[str, float]) -> float:
    """Return average cosine similarity of rows to the cluster centroid."""
    if not vectors:
        return 0.0
    return sum(_cosine_similarity(vector, centroid) for vector in vectors) / len(vectors)


def _cluster_separation(persona_id: str, centroid_lookup: dict[str, dict[str, float]]) -> float:
    """Return distance from the nearest neighboring centroid."""
    current = centroid_lookup.get(persona_id, {})
    if not current:
        return 0.0
    nearest_similarity = max(
        (_cosine_similarity(current, other) for other_id, other in centroid_lookup.items() if other_id != persona_id),
        default=0.0,
    )
    return max(1.0 - nearest_similarity, 0.0)


def _cluster_distinct_reason(top_bottlenecks: list[str], role_dominance: float, separation: float) -> str:
    """Build a readable reason for cluster distinctiveness."""
    if not top_bottlenecks:
        return "Cluster is weakly defined and needs manual review."
    reason = f"Cluster is centered on {top_bottlenecks[0].replace('_', ' ')}"
    if len(top_bottlenecks) > 1:
        reason += f" with recurring {top_bottlenecks[1].replace('_', ' ')} context"
    if role_dominance <= 0.55:
        reason += "; role mix is diverse enough that job-title identity is not the main separator"
    if separation >= 0.25:
        reason += "; nearest neighboring clusters have meaningfully different bottleneck signatures"
    return reason + "."


def _recommended_cluster_name(primary: str, secondary: list[str], config: dict[str, Any]) -> str:
    """Build a bottleneck-first cluster name."""
    naming = {str(key): str(value) for key, value in dict(config.get("naming", {})).items()}
    base = naming.get(primary, primary.replace("_", " ").title())
    if secondary:
        secondary_name = naming.get(secondary[0], secondary[0].replace("_", " ").title())
        if secondary_name != base:
            return f"{base} + {secondary_name}"
    return base


def _distribution(series: pd.Series, limit: int = 4) -> list[dict[str, Any]]:
    """Convert a categorical series into compact distribution rows."""
    if series.empty:
        return []
    values = series.astype(str).str.strip()
    values = values[~values.map(is_unknown_like)]
    if values.empty:
        return []
    shares = values.value_counts(normalize=True)
    counts = values.value_counts()
    return [
        {"label": str(label), "count": int(counts.loc[label]), "share": round(float(shares.loc[label]), 4)}
        for label in counts.head(limit).index
    ]


def _role_importance(df: pd.DataFrame, cluster_column: str) -> float:
    """Estimate dependency between role metadata and cluster ids."""
    if df.empty or cluster_column not in df.columns or "user_role" not in df.columns:
        return 0.0
    contingency = pd.crosstab(df["user_role"].fillna("unassigned").astype(str), df[cluster_column].fillna("unassigned").astype(str))
    if contingency.empty:
        return 0.0
    total = contingency.to_numpy().sum()
    p_xy = contingency / total
    p_x = p_xy.sum(axis=1)
    p_y = p_xy.sum(axis=0)
    mutual_information = 0.0
    for x_label in p_xy.index:
        for y_label in p_xy.columns:
            p_value = float(p_xy.at[x_label, y_label])
            if p_value <= 0:
                continue
            mutual_information += p_value * math.log(p_value / (float(p_x.loc[x_label]) * float(p_y.loc[y_label])), 2)
    return round(mutual_information, 4)


def _versioned_cluster_summary(df: pd.DataFrame, version: str) -> pd.DataFrame:
    """Build a comparable cluster summary table for one clustering version."""
    cluster_column = "legacy_persona_id" if version == "before" else "persona_id"
    name_column = "cluster_name" if version == "after" else None
    rows: list[dict[str, Any]] = []
    for cluster_id, group in df.groupby(cluster_column, dropna=False):
        role_distribution = _distribution(group.get("user_role", pd.Series(dtype=str)))
        bottleneck_distribution = _distribution(group.get("bottleneck_type", pd.Series(dtype=str)))
        rows.append(
            {
                "version": version,
                "cluster_id": str(cluster_id),
                "cluster_name": str(group[name_column].iloc[0]) if name_column and name_column in group.columns else str(cluster_id),
                "cluster_size": int(group["episode_id"].nunique()) if "episode_id" in group.columns else int(len(group)),
                "role_dominance": role_distribution[0]["share"] if role_distribution else 0.0,
                "dominant_role": role_distribution[0]["label"] if role_distribution else "unassigned",
                "bottleneck_coherence": bottleneck_distribution[0]["share"] if bottleneck_distribution else 0.0,
                "dominant_bottleneck": bottleneck_distribution[0]["label"] if bottleneck_distribution else "unassigned",
            }
        )
    return pd.DataFrame(rows)


def _avg_role_dominance(summary_df: pd.DataFrame) -> float:
    """Return average dominant role share across clusters."""
    if summary_df.empty or "role_dominance" not in summary_df.columns:
        return 0.0
    return float(pd.to_numeric(summary_df["role_dominance"], errors="coerce").fillna(0).mean())


def _avg_bottleneck_coherence(summary_df: pd.DataFrame) -> float:
    """Return average dominant bottleneck share across clusters."""
    if summary_df.empty or "bottleneck_coherence" not in summary_df.columns:
        return 0.0
    return float(pd.to_numeric(summary_df["bottleneck_coherence"], errors="coerce").fillna(0).mean())


def _bottleneck_feature_importance(merged_df: pd.DataFrame, assignments_df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Measure how strongly each bottleneck feature explains new cluster assignment."""
    if "persona_id" in merged_df.columns:
        joined = merged_df.copy()
    else:
        joined = merged_df.merge(assignments_df[["episode_id", "persona_id"]], on="episode_id", how="inner")
    rows: list[dict[str, Any]] = []
    for feature in _feature_columns(config):
        scores = pd.to_numeric(joined.get(feature, pd.Series(dtype=float)), errors="coerce").fillna(0)
        mean_by_cluster = joined.assign(_score=scores).groupby("persona_id")["_score"].mean()
        rows.append(
            {
                "feature_name": feature,
                "configured_weight": float(config.get("feature_weights", {}).get(feature, 1.0)),
                "overall_mean_score": round(float(scores.mean()), 4),
                "cluster_variation": round(float(mean_by_cluster.std(ddof=0)) if len(mean_by_cluster) else 0.0, 4),
            }
        )
    return pd.DataFrame(rows).sort_values(["cluster_variation", "configured_weight"], ascending=[False, False]).reset_index(drop=True)


def _comparison_markdown(comparison_df: pd.DataFrame) -> str:
    """Render before/after clustering comparison in markdown."""
    rows = ["# Cluster Comparison Before vs After", ""]
    for _, row in comparison_df.iterrows():
        rows.append(f"- {row['metric_name']}: {row['before_value']} -> {row['after_value']} (delta {row['delta']})")
    return "\n".join(rows) + "\n"


def _cosine_similarity(vector_a: dict[str, float], vector_b: dict[str, float]) -> float:
    """Compute cosine similarity between two sparse vectors."""
    keys = sorted(set(vector_a) | set(vector_b))
    numerator = sum(float(vector_a.get(key, 0.0)) * float(vector_b.get(key, 0.0)) for key in keys)
    norm_a = math.sqrt(sum(float(vector_a.get(key, 0.0)) ** 2 for key in keys))
    norm_b = math.sqrt(sum(float(vector_b.get(key, 0.0)) ** 2 for key in keys))
    if norm_a <= 0 or norm_b <= 0:
        return 0.0
    return numerator / (norm_a * norm_b)


def _legacy_assign_personas(axis_wide_df: pd.DataFrame, axis_names: list[str]) -> pd.DataFrame:
    """Reproduce the legacy axis-signature clustering for before/after comparison."""
    if axis_wide_df.empty or not axis_names:
        return pd.DataFrame(columns=["episode_id", "persona_id", "persona_signature"])
    working = axis_wide_df.copy()
    working["signature"] = working.apply(lambda row: "||".join(f"{axis}={row.get(axis, 'unassigned')}" for axis in axis_names), axis=1)
    total_rows = len(working)
    min_persona_size = max(25, int(total_rows * 0.03))
    signature_counts = working["signature"].value_counts()
    anchors = [signature for signature, count in signature_counts.items() if count >= min_persona_size]
    if not anchors:
        anchors = list(signature_counts.head(min(5, len(signature_counts))).index)
    signature_to_anchor = {signature: signature for signature in anchors}
    for signature in signature_counts.index:
        if signature in signature_to_anchor:
            continue
        signature_to_anchor[signature] = _legacy_nearest_anchor(signature, anchors, axis_names)
    anchor_to_id = {signature: f"legacy_{index:02d}" for index, signature in enumerate(anchors, start=1)}
    working["persona_signature"] = working["signature"].map(signature_to_anchor)
    working["persona_id"] = working["persona_signature"].map(anchor_to_id)
    return working[["episode_id", "persona_id", "persona_signature"]]


def _legacy_nearest_anchor(signature: str, anchors: list[str], axis_names: list[str]) -> str:
    """Attach a sparse legacy signature to the nearest legacy anchor."""
    signature_map = _legacy_signature_map(signature)
    best_anchor = anchors[0]
    best_score = -1
    for anchor in anchors:
        anchor_map = _legacy_signature_map(anchor)
        score = sum(1 for axis in axis_names if signature_map.get(axis, "unassigned") == anchor_map.get(axis, "unassigned"))
        if score > best_score:
            best_anchor = anchor
            best_score = score
    return best_anchor


def _legacy_signature_map(signature: str) -> dict[str, str]:
    """Parse one legacy signature string."""
    mapping: dict[str, str] = {}
    for item in str(signature or "").split("||"):
        if "=" not in item:
            continue
        axis, value = item.split("=", 1)
        mapping[axis] = value
    return mapping


def _example_config() -> dict[str, Any]:
    """Load representative-example scoring config lazily."""
    return load_yaml(Path(__file__).resolve().parents[2] / "config" / "example_selection.yaml")
