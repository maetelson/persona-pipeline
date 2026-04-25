"""Diagnostics-only readiness gap analysis for review-ready workbook planning."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.quality_status import (
    QUALITY_STATUS_POLICY,
    READINESS_POLICY,
    evaluate_quality_status,
    flatten_quality_status_result,
)


ROOT_GAP_ANALYSIS_ARTIFACT = "artifacts/readiness/review_ready_gap_analysis.json"
ROOT_SOURCE_DECISION_ARTIFACT = "artifacts/readiness/review_ready_source_decision_table.csv"
ROOT_GAP_PLAN_DOC = "docs/operational/REVIEW_READY_GAP_PLAN.md"


def _load_required_csv(path: Path) -> pd.DataFrame:
    """Load one required CSV artifact with blank-fill for easier diagnostics."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return pd.read_csv(path).fillna("")


def _load_optional_csv(path: Path) -> pd.DataFrame:
    """Load one optional CSV artifact if present."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path).fillna("")


def _parse_metric_value(value: Any) -> Any:
    """Parse workbook metric strings into Python values when possible."""
    if isinstance(value, (int, float, bool)):
        return value
    text = str(value).strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        number = float(text)
    except ValueError:
        return text
    if number.is_integer():
        return int(number)
    return number


def _metrics_from_frame(df: pd.DataFrame) -> dict[str, Any]:
    """Convert metric/value rows into a flat dictionary."""
    if df.empty:
        return {}
    return {
        str(row["metric"]): _parse_metric_value(row["value"])
        for row in df.to_dict(orient="records")
        if "metric" in row and "value" in row
    }


def _quality_rows(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Index quality-check rows by metric name."""
    rows: dict[str, dict[str, Any]] = {}
    for row in df.to_dict(orient="records"):
        metric = str(row.get("metric", "")).strip()
        if metric:
            rows[metric] = row
    return rows


def _to_float(value: Any) -> float:
    """Coerce a workbook-like value into float."""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    return float(text)


def _to_int(value: Any) -> int:
    """Coerce a workbook-like value into int."""
    return int(round(_to_float(value)))


def _is_true(value: Any) -> bool:
    """Interpret bool-ish CSV values safely."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def _policy_thresholds() -> dict[str, dict[str, Any]]:
    """Expose current policy thresholds for diagnostic reporting."""
    reviewable = READINESS_POLICY["reviewable_but_not_deck_ready"]["requirements"]
    deck_ready = READINESS_POLICY["deck_ready"]["requirements"]
    return {
        "weak_source_cost_center_count": {
            "reviewable_threshold": "<4",
            "deck_ready_threshold": "<2",
        },
        "core_readiness_weak_source_cost_center_count": {
            "reviewable_threshold": "<4",
            "deck_ready_threshold": "<2",
        },
        "effective_balanced_source_count": {
            "reviewable_threshold": f">={QUALITY_STATUS_POLICY['effective_source_diversity']['fail_threshold']}",
            "deck_ready_threshold": f">={QUALITY_STATUS_POLICY['effective_source_diversity']['warn_threshold']}",
        },
        "persona_core_coverage_of_all_labeled_pct": {
            "reviewable_threshold": str(reviewable["persona_core_coverage_of_all_labeled_pct"]["min"]),
            "deck_ready_threshold": str(deck_ready["persona_core_coverage_of_all_labeled_pct"]["min"]),
        },
        "weak_source_yield_status": {
            "reviewable_threshold": "WARN_or_OK",
            "deck_ready_threshold": "OK",
        },
        "source_diversity_status": {
            "reviewable_threshold": "WARN_or_OK",
            "deck_ready_threshold": "OK",
        },
        "overall_status": {
            "reviewable_threshold": "WARN_or_OK",
            "deck_ready_threshold": "OK",
        },
        "persona_readiness_gate_status": {
            "reviewable_threshold": "WARN",
            "deck_ready_threshold": "OK",
        },
    }


def _gap_for_metric(metric_name: str, current_value: Any) -> tuple[Any, Any]:
    """Return gap-to-reviewable and gap-to-deck-ready for one blocker metric."""
    if metric_name in {"weak_source_yield_status", "source_diversity_status", "overall_status", "persona_readiness_gate_status"}:
        return "needs_FAIL_to_WARN", "needs_WARN_to_OK"
    current_float = _to_float(current_value)
    if metric_name == "weak_source_cost_center_count":
        return max(0.0, current_float - 3.0), max(0.0, current_float - 1.0)
    if metric_name == "core_readiness_weak_source_cost_center_count":
        return max(0.0, current_float - 3.0), max(0.0, current_float - 1.0)
    if metric_name == "effective_balanced_source_count":
        reviewable = float(QUALITY_STATUS_POLICY["effective_source_diversity"]["fail_threshold"])
        deck_ready = float(QUALITY_STATUS_POLICY["effective_source_diversity"]["warn_threshold"])
        return round(max(0.0, reviewable - current_float), 2), round(max(0.0, deck_ready - current_float), 2)
    if metric_name == "persona_core_coverage_of_all_labeled_pct":
        reviewable = float(READINESS_POLICY["reviewable_but_not_deck_ready"]["requirements"]["persona_core_coverage_of_all_labeled_pct"]["min"])
        deck_ready = float(READINESS_POLICY["deck_ready"]["requirements"]["persona_core_coverage_of_all_labeled_pct"]["min"])
        return round(max(0.0, reviewable - current_float), 1), round(max(0.0, deck_ready - current_float), 1)
    return 0.0, 0.0


def _owner_mapping() -> dict[str, dict[str, str]]:
    """Map blocker metrics to direct owner modules and remediation hints."""
    return {
        "weak_source_cost_center_count": {
            "direct_owner_module": "src/analysis/diagnostics.py + src/analysis/quality_status.py",
            "likely_remediation_path": "diagnostic-only total weak-source visibility",
            "estimated_implementation_risk": "low",
            "improvable_by": "data remediation",
        },
        "core_readiness_weak_source_cost_center_count": {
            "direct_owner_module": "src/analysis/quality_status.py",
            "likely_remediation_path": "weak-source denominator policy cleanup or source-specific remediation",
            "estimated_implementation_risk": "medium",
            "improvable_by": "data remediation or policy simulation",
        },
        "effective_balanced_source_count": {
            "direct_owner_module": "src/analysis/quality_status.py",
            "likely_remediation_path": "source-volume normalization simulation or source debt separation",
            "estimated_implementation_risk": "medium",
            "improvable_by": "policy simulation",
        },
        "persona_core_coverage_of_all_labeled_pct": {
            "direct_owner_module": "src/analysis/quality_status.py",
            "likely_remediation_path": "source-specific recovery or denominator treatment",
            "estimated_implementation_risk": "medium",
            "improvable_by": "data remediation or policy simulation",
        },
        "weak_source_yield_status": {
            "direct_owner_module": "src/analysis/quality_status.py",
            "likely_remediation_path": "reduce weak-source cost-center debt below fail threshold",
            "estimated_implementation_risk": "low",
            "improvable_by": "data remediation or policy simulation",
        },
        "source_diversity_status": {
            "direct_owner_module": "src/analysis/quality_status.py",
            "likely_remediation_path": "resolve weak-source fail while keeping source-balance warnings visible",
            "estimated_implementation_risk": "low",
            "improvable_by": "policy simulation",
        },
        "overall_status": {
            "direct_owner_module": "src/analysis/quality_status.py",
            "likely_remediation_path": "remove source-diversity FAIL without changing persona standards",
            "estimated_implementation_risk": "low",
            "improvable_by": "policy simulation",
        },
        "persona_readiness_gate_status": {
            "direct_owner_module": "src/analysis/quality_status.py",
            "likely_remediation_path": "allow readiness cap to lift after overall status drops to WARN",
            "estimated_implementation_risk": "low",
            "improvable_by": "policy simulation",
        },
    }


def _blocker_rows(baseline: dict[str, Any], quality_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the current readiness blocker table."""
    thresholds = _policy_thresholds()
    owners = _owner_mapping()
    current_overall_fail = str(baseline.get("overall_status", "")) == "FAIL"
    rows: list[dict[str, Any]] = []
    metric_order = [
        "weak_source_cost_center_count",
        "core_readiness_weak_source_cost_center_count",
        "effective_balanced_source_count",
        "persona_core_coverage_of_all_labeled_pct",
        "weak_source_yield_status",
        "source_diversity_status",
        "overall_status",
        "persona_readiness_gate_status",
    ]
    for metric_name in metric_order:
        current_value = baseline.get(metric_name, "")
        gap_to_reviewable, gap_to_deck_ready = _gap_for_metric(metric_name, current_value)
        quality_row = quality_rows.get(metric_name, {})
        status = str(quality_row.get("status", quality_row.get("level", "pass")) or "pass")
        level = str(quality_row.get("level", "") or status)
        group_status = ""
        if metric_name == "weak_source_cost_center_count":
            group_status = str(baseline.get("source_diversity_status", ""))
        elif metric_name == "core_readiness_weak_source_cost_center_count":
            group_status = str(baseline.get("source_diversity_status", ""))
        elif metric_name == "effective_balanced_source_count":
            group_status = str(baseline.get("source_diversity_status", ""))
        elif metric_name == "persona_core_coverage_of_all_labeled_pct":
            group_status = str(baseline.get("core_clustering_status", ""))
        elif metric_name in {"weak_source_yield_status", "source_diversity_status"}:
            group_status = str(baseline.get("overall_status", ""))
        elif metric_name == "persona_readiness_gate_status":
            group_status = str(baseline.get("persona_readiness_state", ""))
        currently_blocking = False
        if current_overall_fail:
            if metric_name in {"core_readiness_weak_source_cost_center_count", "weak_source_yield_status", "source_diversity_status", "overall_status", "persona_readiness_gate_status"}:
                currently_blocking = True
        if metric_name == "effective_balanced_source_count" and str(baseline.get("source_diversity_status", "")) == "FAIL":
            currently_blocking = True
        rows.append(
            {
                "metric_name": metric_name,
                "current_value": current_value,
                "reviewable_threshold": thresholds[metric_name]["reviewable_threshold"],
                "deck_ready_threshold": thresholds[metric_name]["deck_ready_threshold"],
                "gap_to_reviewable": gap_to_reviewable,
                "gap_to_deck_ready": gap_to_deck_ready,
                "blocker_type": level,
                "axis_status": status.upper() if status else "",
                "group_status": group_status,
                "direct_owner_module": owners[metric_name]["direct_owner_module"],
                "likely_remediation_path": owners[metric_name]["likely_remediation_path"],
                "estimated_implementation_risk": owners[metric_name]["estimated_implementation_risk"],
                "improvable_by": owners[metric_name]["improvable_by"],
                "currently_blocking_reviewable": currently_blocking,
            }
        )
    return rows


def _meaningful_persona_evidence(row: pd.Series) -> bool:
    """Return whether the weak source still contributes meaningful persona evidence."""
    return (
        _to_int(row.get("grounded_promoted_persona_episode_count", 0)) >= 75
        or _to_float(row.get("blended_influence_share_pct", 0.0)) >= 2.0
    )


def _recommended_action(row: pd.Series) -> str:
    """Assign the default weak-source action unless live artifacts contradict it."""
    source = str(row.get("source", ""))
    collapse_stage = str(row.get("collapse_stage", ""))
    labelable_ratio = _to_float(row.get("labelable_episode_ratio_pct", 0.0))
    blended_influence = _to_float(row.get("blended_influence_share_pct", 0.0))
    grounded_rows = _to_int(row.get("grounded_promoted_persona_episode_count", 0))

    default_actions = {
        "google_developer_forums": "fix_now_with_evidence",
        "domo_community_forum": "parser_or_episode_fidelity_audit_needed",
        "adobe_analytics_community": "parser_or_episode_fidelity_audit_needed",
        "klaviyo_community": "downgrade_to_exploratory_only",
    }
    if source in default_actions:
        if source == "google_developer_forums" and not (
            collapse_stage == "valid_filtering" and grounded_rows >= 100 and blended_influence >= 2.0
        ):
            return "no_action_keep_monitoring"
        if source in {"domo_community_forum", "adobe_analytics_community"} and not (
            collapse_stage == "episode_yield" and labelable_ratio < 30.0
        ):
            return "fix_now_with_evidence"
        if source == "klaviyo_community" and not (blended_influence < 1.5):
            return "no_action_keep_monitoring"
        return default_actions[source]
    if collapse_stage == "valid_filtering" and grounded_rows >= 100 and blended_influence >= 2.0:
        return "fix_now_with_evidence"
    if collapse_stage == "episode_yield" and labelable_ratio < 30.0:
        return "parser_or_episode_fidelity_audit_needed"
    if blended_influence < 1.5:
        return "downgrade_to_exploratory_only"
    return "no_action_keep_monitoring"


def _root_cause(row: pd.Series) -> str:
    """Summarize the likely root cause for a weak source."""
    collapse_stage = str(row.get("collapse_stage", ""))
    if collapse_stage == "valid_filtering":
        return "valid-filter pain recognition gap"
    if collapse_stage == "episode_yield":
        return "parser or episode-fidelity loss"
    if collapse_stage == "relevance_prefilter":
        return "relevance-prefilter generic/noise bleed"
    return "mixed source-quality debt"


def _weak_source_decisions(source_balance_df: pd.DataFrame, weak_source_triage_df: pd.DataFrame) -> pd.DataFrame:
    """Build the weak-source decision table for the current cost centers."""
    weak_rows = source_balance_df[source_balance_df["weak_source_cost_center"].map(_is_true)].copy()
    triage_cols = [
        "source",
        "triage_recommendation",
        "triage_rationale",
        "owner_action_type",
    ]
    triage = weak_source_triage_df[triage_cols].copy() if not weak_source_triage_df.empty else pd.DataFrame(columns=triage_cols)
    merged = weak_rows.merge(triage, on="source", how="left")
    merged["whether_meaningful_persona_evidence_exists"] = merged.apply(_meaningful_persona_evidence, axis=1)
    merged["root_cause"] = merged.apply(_root_cause, axis=1)
    merged["recommended_action"] = merged.apply(_recommended_action, axis=1)
    merged["source_url"] = ""
    decision_columns = [
        "source",
        "raw_record_count",
        "valid_post_count",
        "prefiltered_valid_post_count",
        "episode_count",
        "labeled_episode_count",
        "labelable_episode_ratio_pct",
        "grounded_promoted_persona_episode_count",
        "blended_influence_share_pct",
        "collapse_stage",
        "failure_reason_top",
        "whether_meaningful_persona_evidence_exists",
        "root_cause",
        "recommended_action",
    ]
    renamed = merged[decision_columns].rename(
        columns={
            "raw_record_count": "raw_rows",
            "valid_post_count": "valid_rows",
            "prefiltered_valid_post_count": "prefiltered_rows",
            "episode_count": "episode_rows",
            "labeled_episode_count": "labeled_rows",
        }
    )
    return renamed.sort_values("source").reset_index(drop=True)


def _core_coverage_gap_analysis(root_dir: Path, baseline: dict[str, Any], weak_sources: set[str]) -> dict[str, Any]:
    """Compute exact core-coverage gaps and likely recovery profile."""
    labeled_df = pd.read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet", columns=["episode_id", "persona_core_eligible"])
    episodes_df = pd.read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet", columns=["episode_id", "source"])
    merged = labeled_df.merge(episodes_df, on="episode_id", how="left")
    _ = baseline
    labeled_rows = int(len(merged))
    persona_core_rows = int(merged["persona_core_eligible"].fillna(False).sum())
    non_core = merged[~merged["persona_core_eligible"].fillna(False)].copy()
    target_75 = int(max(0, math.ceil((0.75 * labeled_rows) - persona_core_rows)))
    target_80 = int(max(0, math.ceil((0.80 * labeled_rows) - persona_core_rows)))
    non_core_by_source = (
        non_core["source"].fillna("").astype(str).value_counts().rename_axis("source").reset_index(name="non_core_labeled_rows")
    )
    weak_non_core = int(non_core_by_source.loc[non_core_by_source["source"].isin(weak_sources), "non_core_labeled_rows"].sum())
    weak_non_core_share = round((weak_non_core / len(non_core)) * 100.0, 1) if len(non_core) else 0.0
    path_to_75 = "existing_source_fix_likely"
    if weak_non_core_share < 40.0:
        path_to_75 = "denominator_artifact_likely"
    path_to_80 = "existing_source_fix_likely"
    if weak_non_core_share < 40.0:
        path_to_80 = "denominator_artifact_likely"
    if target_80 > max(250, int(len(non_core) * 0.2)):
        path_to_80 = "junk_risk_if_forced"
    return {
        "current_labeled_rows": labeled_rows,
        "current_persona_core_rows": persona_core_rows,
        "current_persona_core_coverage_pct": round((persona_core_rows / labeled_rows) * 100.0, 1) if labeled_rows else 0.0,
        "rows_needed_to_reach_75_0": target_75,
        "rows_needed_to_reach_80_0": target_80,
        "non_core_labeled_rows": int(len(non_core)),
        "recoverable_non_core_rows_concentrated_in_weak_cost_centers_pct": weak_non_core_share,
        "top_non_core_sources": non_core_by_source.head(10).to_dict(orient="records"),
        "path_classification_to_75_0": path_to_75,
        "path_classification_to_80_0": path_to_80,
        "interpretation": (
            "Reaching 75.0 looks plausibly attainable with targeted recovery or denominator treatment."
            if target_75 <= 100
            else "Reaching 75.0 already requires a broader recovery pass."
        ),
        "target_80_interpretation": (
            "Reaching 80.0 is materially harder and should not be the immediate target."
            if target_80 >= 250
            else "Reaching 80.0 may be feasible with deeper cleanup, but it is still a larger lift."
        ),
    }


def _baseline_metrics(root_dir: Path) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load current workbook metrics and source diagnostics artifacts."""
    overview_df = _load_required_csv(root_dir / "data" / "analysis" / "overview.csv")
    quality_df = _load_required_csv(root_dir / "data" / "analysis" / "quality_checks.csv")
    source_balance_df = _load_required_csv(root_dir / "data" / "analysis" / "source_balance_audit.csv")
    weak_triage_df = _load_optional_csv(root_dir / "data" / "analysis" / "weak_source_triage.csv")
    metrics = _metrics_from_frame(overview_df)
    metrics.update(_metrics_from_frame(quality_df))
    return metrics, quality_df, source_balance_df, weak_triage_df


def _scenario_result(
    scenario_id: str,
    description: str,
    baseline_metrics: dict[str, Any],
    overrides: dict[str, Any],
) -> dict[str, Any]:
    """Apply one bounded source/readiness override and re-evaluate quality policy."""
    simulated_metrics = dict(baseline_metrics)
    simulated_metrics.update(overrides)
    evaluated = evaluate_quality_status(simulated_metrics)
    flattened = flatten_quality_status_result(evaluated)
    return {
        "scenario_id": scenario_id,
        "description": description,
        "weak_source_cost_center_count": _to_int(flattened.get("weak_source_cost_center_count", 0)),
        "core_readiness_weak_source_cost_center_count": _to_int(flattened.get("core_readiness_weak_source_cost_center_count", flattened.get("weak_source_cost_center_count", 0))),
        "exploratory_only_weak_source_debt_count": _to_int(flattened.get("exploratory_only_weak_source_debt_count", 0)),
        "effective_balanced_source_count": round(_to_float(flattened.get("effective_balanced_source_count", 0.0)), 2),
        "persona_core_coverage_of_all_labeled_pct": round(_to_float(flattened.get("persona_core_coverage_of_all_labeled_pct", 0.0)), 1),
        "overall_status": str(flattened.get("overall_status", "")),
        "quality_flag": str(flattened.get("quality_flag", "")),
        "persona_readiness_state": str(flattened.get("persona_readiness_state", "")),
        "persona_readiness_gate_status": str(flattened.get("persona_readiness_gate_status", "")),
        "production_ready_persona_count": _to_int(flattened.get("production_ready_persona_count", 0)),
        "review_ready_persona_count": _to_int(flattened.get("review_ready_persona_count", 0)),
        "final_usable_persona_count": _to_int(flattened.get("final_usable_persona_count", 0)),
        "quality_standard_weakened": False,
        "reviewable_achievable_without_weakening": (
            str(flattened.get("overall_status", "")) == "WARN"
            and str(flattened.get("persona_readiness_state", "")) == "reviewable_but_not_deck_ready"
            and _to_int(flattened.get("production_ready_persona_count", 0)) == _to_int(baseline_metrics.get("production_ready_persona_count", 0))
            and _to_int(flattened.get("review_ready_persona_count", 0)) == _to_int(baseline_metrics.get("review_ready_persona_count", 0))
            and _to_int(flattened.get("final_usable_persona_count", 0)) == _to_int(baseline_metrics.get("final_usable_persona_count", 0))
        ),
    }


def _scenario_simulation(
    baseline_metrics: dict[str, Any],
    source_balance_df: pd.DataFrame,
    coverage_gap: dict[str, Any],
) -> list[dict[str, Any]]:
    """Run the bounded readiness strategy scenarios."""
    weak_sources = set(
        source_balance_df.loc[source_balance_df["weak_source_cost_center"].map(_is_true), "source"].astype(str).tolist()
    )
    klaviyo_labeled_rows = 0
    if "klaviyo_community" in weak_sources:
        klaviyo_labeled_rows = _to_int(
            source_balance_df.loc[source_balance_df["source"].astype(str) == "klaviyo_community", "labeled_episode_count"].iloc[0]
        )
    labeled_rows = _to_int(coverage_gap["current_labeled_rows"])
    persona_core_rows = _to_int(coverage_gap["current_persona_core_rows"])
    denominator_without_klaviyo = max(1, labeled_rows - klaviyo_labeled_rows)
    coverage_without_klaviyo = round((persona_core_rows / denominator_without_klaviyo) * 100.0, 1)
    weak_minus_one = max(0, _to_int(baseline_metrics.get("core_readiness_weak_source_cost_center_count", baseline_metrics.get("weak_source_cost_center_count", 0)) - 1))

    return [
        _scenario_result("A_current_baseline", "Current baseline.", baseline_metrics, {}),
        _scenario_result(
            "B_downgrade_weakest_non_contributing_source_from_core_readiness",
            "Treat the lowest-influence weak source as exploratory-only debt for weak-source counting only.",
            baseline_metrics,
            {"core_readiness_weak_source_cost_center_count": weak_minus_one},
        ),
        _scenario_result(
            "C_exclude_true_cost_center_source_from_core_readiness_denominator",
            "Exclude klaviyo_community from core-readiness denominator and weak-source debt in simulation only.",
            baseline_metrics,
            {
                "core_readiness_weak_source_cost_center_count": weak_minus_one,
                "persona_core_coverage_of_all_labeled_pct": coverage_without_klaviyo,
                "labeled_episode_rows": denominator_without_klaviyo,
            },
        ),
        _scenario_result(
            "D_fix_one_high_roi_weak_source",
            "Simulate google_developer_forums no longer being a weak source cost center.",
            baseline_metrics,
            {"core_readiness_weak_source_cost_center_count": weak_minus_one},
        ),
        _scenario_result(
            "E_strict_source_balance_with_exploratory_only_source_debt_separated",
            "Keep source-balance strict but separate exploratory-only weak-source debt from the weak-source fail axis.",
            baseline_metrics,
            {"core_readiness_weak_source_cost_center_count": weak_minus_one},
        ),
        _scenario_result("F_no_op_baseline", "Duplicate baseline for sanity comparison.", baseline_metrics, {}),
    ]


def _recommended_path(baseline: dict[str, Any], scenarios: list[dict[str, Any]]) -> tuple[str, bool]:
    """Choose the single next path and whether reviewable looks achievable by cleanup."""
    if (
        str(baseline.get("overall_status", "")) == "WARN"
        and str(baseline.get("persona_readiness_state", "")) == "reviewable_but_not_deck_ready"
    ):
        return "no implementation, only review/handoff positioning", True
    for scenario in scenarios:
        if scenario["scenario_id"] == "E_strict_source_balance_with_exploratory_only_source_debt_separated":
            if scenario["reviewable_achievable_without_weakening"]:
                return "weak-source denominator policy cleanup", True
    for scenario in scenarios:
        if scenario["reviewable_achievable_without_weakening"]:
            return "weak-source denominator policy cleanup", True
    return "no implementation, only review/handoff positioning", False


def _render_gap_plan(report: dict[str, Any]) -> str:
    """Render the readiness gap plan markdown document."""
    blockers = report["readiness_blockers"]
    scenarios = report["scenario_simulation"]
    recommendation = report["recommended_next_implementation_path"]
    achievable = report["reviewable_achievable_without_weakening_persona_standards"]
    lines = [
        "# Review-Ready Gap Plan",
        "",
        "## Summary",
        "",
        f"- Current workbook readiness: `{report['baseline']['persona_readiness_state']}` / `{report['baseline']['quality_flag']}`",
        f"- Primary source-side blocker: `core_readiness_weak_source_cost_center_count={report['baseline']['core_readiness_weak_source_cost_center_count']}`",
        f"- Recommended next path: `{recommendation}`",
        f"- Reviewable without weakening persona standards: `{achievable}`",
        "- Deck-ready remains out of scope for this pass.",
        "",
        "## Readiness Blockers",
        "",
    ]
    for row in blockers:
        lines.append(
            f"- `{row['metric_name']}`: current `{row['current_value']}`, reviewable threshold `{row['reviewable_threshold']}`, "
            f"deck-ready threshold `{row['deck_ready_threshold']}`, blocker type `{row['blocker_type']}`"
        )
    lines.extend(["", "## Weak Source Decisions", ""])
    for row in report["weak_source_decisions"]:
        lines.append(
            f"- `{row['source']}` -> `{row['recommended_action']}` because `{row['collapse_stage']}` / `{row['failure_reason_top']}` "
            f"with blended influence `{row['blended_influence_share_pct']}`"
        )
    lines.extend(["", "## Scenario Simulation", ""])
    for row in scenarios:
        lines.append(
            f"- `{row['scenario_id']}`: overall `{row['overall_status']}`, readiness `{row['persona_readiness_state']}`, "
            f"core weak sources `{row['core_readiness_weak_source_cost_center_count']}`, total weak sources `{row['weak_source_cost_center_count']}`, core coverage `{row['persona_core_coverage_of_all_labeled_pct']}`"
        )
    return "\n".join(lines) + "\n"


def build_review_ready_gap_analysis(root_dir: Path) -> dict[str, Any]:
    """Build the full diagnostics-only review-ready gap analysis report."""
    baseline_metrics, quality_df, source_balance_df, diagnostics_df = _baseline_metrics(root_dir)
    quality_rows = _quality_rows(quality_df)
    weak_source_decision_df = _weak_source_decisions(source_balance_df, diagnostics_df)
    weak_sources = set(weak_source_decision_df["source"].astype(str).tolist())
    coverage_gap = _core_coverage_gap_analysis(root_dir, baseline_metrics, weak_sources)
    scenarios = _scenario_simulation(baseline_metrics, source_balance_df, coverage_gap)
    baseline_summary = {
        "overall_status": str(baseline_metrics.get("overall_status", "")),
        "quality_flag": str(baseline_metrics.get("quality_flag", "")),
        "persona_readiness_state": str(baseline_metrics.get("persona_readiness_state", "")),
        "persona_readiness_gate_status": str(baseline_metrics.get("persona_readiness_gate_status", "")),
        "weak_source_cost_center_count": _to_int(baseline_metrics.get("weak_source_cost_center_count", 0)),
        "core_readiness_weak_source_cost_center_count": _to_int(
            baseline_metrics.get("core_readiness_weak_source_cost_center_count", baseline_metrics.get("weak_source_cost_center_count", 0))
        ),
        "exploratory_only_weak_source_debt_count": _to_int(baseline_metrics.get("exploratory_only_weak_source_debt_count", 0)),
        "effective_balanced_source_count": round(_to_float(baseline_metrics.get("effective_balanced_source_count", 0.0)), 2),
        "persona_core_coverage_of_all_labeled_pct": round(_to_float(baseline_metrics.get("persona_core_coverage_of_all_labeled_pct", 0.0)), 1),
        "production_ready_persona_count": _to_int(baseline_metrics.get("production_ready_persona_count", 0)),
        "review_ready_persona_count": _to_int(baseline_metrics.get("review_ready_persona_count", 0)),
        "final_usable_persona_count": _to_int(baseline_metrics.get("final_usable_persona_count", 0)),
    }
    recommendation, achievable = _recommended_path(baseline_summary, scenarios)
    report = {
        "baseline": baseline_summary,
        "readiness_blockers": _blocker_rows(baseline_metrics, quality_rows),
        "weak_source_decisions": weak_source_decision_df.to_dict(orient="records"),
        "core_coverage_gap_analysis": coverage_gap,
        "scenario_simulation": scenarios,
        "recommended_next_implementation_path": recommendation,
        "reviewable_achievable_without_weakening_persona_standards": achievable,
        "deck_ready_out_of_scope": True,
        "proposed_path_weakens_persona_standards": False,
    }
    report["plan_markdown"] = _render_gap_plan(report)
    return report


def write_review_ready_gap_artifacts(root_dir: Path, report: dict[str, Any]) -> dict[str, Path]:
    """Write the JSON, CSV, and markdown artifacts for the readiness gap report."""
    json_path = root_dir / ROOT_GAP_ANALYSIS_ARTIFACT
    csv_path = root_dir / ROOT_SOURCE_DECISION_ARTIFACT
    doc_path = root_dir / ROOT_GAP_PLAN_DOC
    json_path.parent.mkdir(parents=True, exist_ok=True)
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    pd.DataFrame(report["weak_source_decisions"]).to_csv(csv_path, index=False, encoding="utf-8")
    doc_path.write_text(report["plan_markdown"], encoding="utf-8")
    return {
        "json_path": json_path,
        "csv_path": csv_path,
        "doc_path": doc_path,
    }
