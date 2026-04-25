"""Diagnostics-only deck-ready feasibility analysis for workbook readiness."""

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


ROOT_DECK_READY_FEASIBILITY_ARTIFACT = "artifacts/readiness/deck_ready_feasibility_analysis.json"
ROOT_SOURCE_BALANCE_COVERAGE_GAP_ARTIFACT = "artifacts/readiness/source_balance_core_coverage_gap.csv"
ROOT_DECK_READY_FEASIBILITY_DOC = "docs/operational/DECK_READY_FEASIBILITY_PLAN.md"


def _load_required_csv(path: Path) -> pd.DataFrame:
    """Load one required CSV artifact with blanks instead of NaN."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return pd.read_csv(path).fillna("")


def _load_required_json(path: Path) -> dict[str, Any]:
    """Load one required JSON artifact."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_metric_value(value: Any) -> Any:
    """Parse workbook metric strings into Python values when possible."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return ""
        return int(value) if float(value).is_integer() else float(value)
    text = str(value).strip()
    if not text or text.lower() == "nan":
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
    return int(number) if number.is_integer() else number


def _metrics_from_frame(df: pd.DataFrame) -> dict[str, Any]:
    """Convert metric/value rows into a flat dictionary."""
    if df.empty:
        return {}
    return {
        str(row["metric"]): _parse_metric_value(row["value"])
        for row in df.to_dict(orient="records")
        if "metric" in row and "value" in row
    }


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
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _baseline_metrics(root_dir: Path) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Load current workbook metrics and source diagnostics artifacts."""
    overview_df = _load_required_csv(root_dir / "data" / "analysis" / "overview.csv")
    quality_df = _load_required_csv(root_dir / "data" / "analysis" / "quality_checks.csv")
    source_balance_df = _load_required_csv(root_dir / "data" / "analysis" / "source_balance_audit.csv")
    source_diagnostics_df = _load_required_csv(root_dir / "data" / "analysis" / "source_diagnostics.csv")
    review_ready_gap = _load_required_json(root_dir / "artifacts" / "readiness" / "review_ready_gap_analysis.json")
    metrics = _metrics_from_frame(overview_df)
    metrics.update(_metrics_from_frame(quality_df))
    return metrics, quality_df, source_balance_df, source_diagnostics_df, review_ready_gap


def _minimal_top_source_cap_for_target(shares: pd.Series, target: float) -> tuple[float, float]:
    """Return the smallest largest-share cap that reaches the effective-count target."""
    normalized = shares.astype(float)
    current_top = float(normalized.max())
    lo = 0.0
    hi = current_top
    best_cap = current_top
    best_effective = 1.0 / ((normalized / normalized.sum()).pow(2).sum())
    for _ in range(40):
        mid = (lo + hi) / 2.0
        adjusted = normalized.copy()
        top_idx = adjusted.idxmax()
        delta = adjusted.iloc[top_idx] - mid
        if delta > 0:
            others = adjusted.drop(top_idx)
            adjusted.iloc[top_idx] = mid
            adjusted.loc[others.index] = others + delta * (others / others.sum())
        effective = 1.0 / ((adjusted / adjusted.sum()).pow(2).sum())
        if effective >= target:
            best_cap = mid
            best_effective = effective
            lo = mid
        else:
            hi = mid
    return round(best_cap, 2), round(best_effective, 2)


def _source_balance_gap_analysis(source_balance_df: pd.DataFrame, baseline: dict[str, Any]) -> dict[str, Any]:
    """Analyze the current source-balance gap and likely drivers."""
    shares = source_balance_df["blended_influence_share_pct"].astype(float)
    top_sources = source_balance_df.sort_values("blended_influence_share_pct", ascending=False).head(8).copy()
    healthy = source_balance_df[~source_balance_df["weak_source_cost_center"].map(_is_true)].copy()
    weak = source_balance_df[source_balance_df["weak_source_cost_center"].map(_is_true)].copy()
    healthy_top_share = round(float(healthy.nlargest(5, "blended_influence_share_pct")["blended_influence_share_pct"].sum()), 1)
    weak_total_share = round(float(weak["blended_influence_share_pct"].sum()), 1)
    gap_to_deck_ready = round(
        max(0.0, QUALITY_STATUS_POLICY["effective_source_diversity"]["warn_threshold"] - _to_float(baseline["effective_balanced_source_count"])),
        2,
    )
    target_cap, normalized_effective = _minimal_top_source_cap_for_target(
        shares,
        float(QUALITY_STATUS_POLICY["effective_source_diversity"]["warn_threshold"]),
    )
    top_source = top_sources.iloc[0]
    imbalance_driver = "healthy_high_volume_sources"
    if weak_total_share >= 20.0:
        imbalance_driver = "weak_source_underperformance"
    elif gap_to_deck_ready <= 0.15 and healthy_top_share >= 75.0:
        imbalance_driver = "healthy_high_volume_sources"
    return {
        "current_effective_balanced_source_count": round(_to_float(baseline["effective_balanced_source_count"]), 2),
        "deck_ready_floor": float(QUALITY_STATUS_POLICY["effective_source_diversity"]["warn_threshold"]),
        "gap_to_deck_ready_floor": gap_to_deck_ready,
        "top_influence_sources": top_sources[
            [
                "source",
                "blended_influence_share_pct",
                "grounded_promoted_persona_episode_count",
                "weak_source_cost_center",
                "core_readiness_weak_source_cost_center",
            ]
        ].to_dict(orient="records"),
        "largest_source": str(top_source["source"]),
        "largest_source_influence_share_pct": round(float(top_source["blended_influence_share_pct"]), 1),
        "healthy_top_5_influence_share_pct": healthy_top_share,
        "weak_source_total_influence_share_pct": weak_total_share,
        "imbalance_driver": imbalance_driver,
        "single_source_remediation_realistically_moves_score": False,
        "source_volume_normalization_analysis": {
            "largest_source_cap_needed_to_reach_6_0": target_cap,
            "largest_source_shift_needed_pct_points": round(float(top_source["blended_influence_share_pct"]) - target_cap, 2),
            "normalized_effective_balanced_source_count": normalized_effective,
            "would_hide_real_concentration_risk": False,
            "interpretation": (
                "Source-volume normalization could move effective source balance above 6.0 with a modest reduction in top-source weight, "
                "but it would not resolve weak-source debt or core-coverage warnings on its own."
            ),
        },
        "analysis_summary": (
            "Current source-balance drag is driven more by concentration in healthy high-volume sources than by any single weak source. "
            "One source-specific remediation win is unlikely to move effective balance enough by itself."
        ),
    }


def _core_coverage_gap_analysis(root_dir: Path, source_balance_df: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    """Compute exact core-coverage gaps and likely recovery profile."""
    labeled_df = pd.read_parquet(
        root_dir / "data" / "labeled" / "labeled_episodes.parquet",
        columns=["episode_id", "persona_core_eligible", "labelability_reason"],
    )
    episodes_df = pd.read_parquet(
        root_dir / "data" / "episodes" / "episode_table.parquet",
        columns=["episode_id", "source"],
    )
    merged = labeled_df.merge(episodes_df, on="episode_id", how="left")
    labeled_rows = int(len(merged))
    persona_core_rows = int(merged["persona_core_eligible"].fillna(False).sum())
    non_core = merged[~merged["persona_core_eligible"].fillna(False)].copy()
    target_75_rows = int(max(0, math.ceil((0.75 * labeled_rows) - persona_core_rows)))
    target_80_rows = int(max(0, math.ceil((0.80 * labeled_rows) - persona_core_rows)))
    non_core_by_source = (
        non_core.groupby("source")
        .agg(
            non_core_labeled_rows=("episode_id", "count"),
            positive_hint_non_core_rows=("labelability_reason", lambda s: int(sum(str(v).strip().lower() != "weak_signal" for v in s))),
        )
        .reset_index()
    )
    non_core_by_source["weak_source_cost_center"] = non_core_by_source["source"].map(
        source_balance_df.set_index("source")["weak_source_cost_center"].to_dict()
    )
    non_core_by_source["blended_influence_share_pct"] = non_core_by_source["source"].map(
        source_balance_df.set_index("source")["blended_influence_share_pct"].to_dict()
    )
    non_core_by_source["recoverable_quality_read"] = non_core_by_source.apply(
        lambda row: (
            "mixed_recoverable"
            if _to_int(row["positive_hint_non_core_rows"]) >= 50
            else "mostly_low_signal"
        ),
        axis=1,
    )
    non_core_by_source = non_core_by_source.sort_values("non_core_labeled_rows", ascending=False).reset_index(drop=True)
    weak_non_core_share = round(
        (
            non_core_by_source.loc[non_core_by_source["weak_source_cost_center"].map(_is_true), "non_core_labeled_rows"].sum()
            / max(1, len(non_core))
        )
        * 100.0,
        1,
    )
    quality_read = "mostly_noise_or_low_signal"
    if int(non_core_by_source["positive_hint_non_core_rows"].sum()) >= 500:
        quality_read = "mixed_signal_with_some_recoverable_rows"
    gap = {
        "current_labeled_rows": labeled_rows,
        "current_persona_core_rows": persona_core_rows,
        "current_persona_core_coverage_pct": round((persona_core_rows / labeled_rows) * 100.0, 1) if labeled_rows else 0.0,
        "rows_needed_to_reach_75_0": target_75_rows,
        "rows_needed_to_reach_80_0": target_80_rows,
        "current_non_core_labeled_row_count": int(len(non_core)),
        "recoverable_non_core_rows_concentrated_in_weak_sources_pct": weak_non_core_share,
        "recoverable_rows_quality_read": quality_read,
        "pushing_to_80_creates_junk_persona_risk": True,
        "analysis_summary": (
            "Crossing 75.0 only needs a small number of additional persona-core rows. "
            "Reaching 80.0 requires converting hundreds of currently non-core rows, and most of that pool is still low-signal."
        ),
    }
    return gap, non_core_by_source


def _scenario_result(
    scenario_id: str,
    description: str,
    baseline_metrics: dict[str, Any],
    overrides: dict[str, Any],
    quality_standard_weakened: bool,
    junk_risk_assessment: str,
) -> dict[str, Any]:
    """Apply one bounded override and re-evaluate current readiness policy."""
    simulated_metrics = dict(baseline_metrics)
    simulated_metrics.update(overrides)
    evaluated = evaluate_quality_status(simulated_metrics)
    flattened = flatten_quality_status_result(evaluated)
    readiness_state = str(flattened.get("persona_readiness_state", ""))
    deck_ready_candidate = readiness_state in {"deck_ready", "production_persona_ready"}
    return {
        "scenario_id": scenario_id,
        "description": description,
        "effective_balanced_source_count": round(_to_float(flattened.get("effective_balanced_source_count", 0.0)), 2),
        "persona_core_coverage_of_all_labeled_pct": round(_to_float(flattened.get("persona_core_coverage_of_all_labeled_pct", 0.0)), 1),
        "weak_source_cost_center_count": _to_int(flattened.get("weak_source_cost_center_count", 0)),
        "core_readiness_weak_source_cost_center_count": _to_int(flattened.get("core_readiness_weak_source_cost_center_count", 0)),
        "overall_status": str(flattened.get("overall_status", "")),
        "quality_flag": str(flattened.get("quality_flag", "")),
        "persona_readiness_state": readiness_state,
        "deck_ready_candidate": deck_ready_candidate,
        "final_usable_persona_count": _to_int(flattened.get("final_usable_persona_count", 0)),
        "production_ready_persona_count": _to_int(flattened.get("production_ready_persona_count", 0)),
        "review_ready_persona_count": _to_int(flattened.get("review_ready_persona_count", 0)),
        "quality_standard_weakened": quality_standard_weakened,
        "junk_risk_assessment": junk_risk_assessment,
    }


def _scenario_simulation(
    baseline_metrics: dict[str, Any],
    balance_gap: dict[str, Any],
    coverage_gap: dict[str, Any],
) -> list[dict[str, Any]]:
    """Run the bounded deck-ready feasibility scenarios."""
    labeled_rows = _to_int(coverage_gap["current_labeled_rows"])
    core_rows = _to_int(coverage_gap["current_persona_core_rows"])
    recovery_60_pct = round(((core_rows + 60) / labeled_rows) * 100.0, 1)
    recovery_80_pct = round(((core_rows + _to_int(coverage_gap["rows_needed_to_reach_80_0"])) / labeled_rows) * 100.0, 1)
    normalized_effective = _to_float(
        balance_gap["source_volume_normalization_analysis"]["normalized_effective_balanced_source_count"]
    )
    baseline_total_weak = _to_int(baseline_metrics.get("weak_source_cost_center_count", 0))
    baseline_core_weak = _to_int(baseline_metrics.get("core_readiness_weak_source_cost_center_count", 0))
    weak_sources_text = str(baseline_metrics.get("weak_source_cost_centers", ""))
    scenarios = [
        _scenario_result(
            "A_no_op_current_baseline",
            "Current baseline with no changes.",
            baseline_metrics,
            {},
            quality_standard_weakened=False,
            junk_risk_assessment="none",
        ),
        _scenario_result(
            "B_high_quality_recovery_of_60_persona_core_rows",
            "Recover 60 high-quality persona-core rows without changing source balance or weak-source debt.",
            baseline_metrics,
            {"persona_core_coverage_of_all_labeled_pct": recovery_60_pct},
            quality_standard_weakened=False,
            junk_risk_assessment="low",
        ),
        _scenario_result(
            "C_high_quality_recovery_to_80_0",
            "Recover enough persona-core rows to reach 80.0 while keeping current source-balance and weak-source structure.",
            baseline_metrics,
            {"persona_core_coverage_of_all_labeled_pct": recovery_80_pct},
            quality_standard_weakened=False,
            junk_risk_assessment="high",
        ),
        _scenario_result(
            "D_source_volume_normalization_only",
            "Apply source-volume normalization to the effective source-balance metric only.",
            baseline_metrics,
            {"effective_balanced_source_count": normalized_effective},
            quality_standard_weakened=False,
            junk_risk_assessment="none",
        ),
        _scenario_result(
            "E_remove_remaining_true_weak_source_debt_from_deck_ready_denominator",
            "Simulate removing remaining weak-source debt from deck-ready denominator pressure while keeping current evidence untouched.",
            baseline_metrics,
            {
                "core_readiness_weak_source_cost_center_count": 0,
                "core_readiness_weak_source_cost_centers": "",
            },
            quality_standard_weakened=True,
            junk_risk_assessment="policy_only",
        ),
        _scenario_result(
            "F_combined_realistic_one_source_win_plus_modest_balance_improvement",
            "One remaining weak source is remediated, source balance improves modestly, and 60 high-quality core rows are recovered.",
            baseline_metrics,
            {
                "core_readiness_weak_source_cost_center_count": max(0, baseline_core_weak - 1),
                "weak_source_cost_center_count": max(0, baseline_total_weak - 1),
                "weak_source_cost_centers": weak_sources_text,
                "effective_balanced_source_count": normalized_effective,
                "persona_core_coverage_of_all_labeled_pct": recovery_60_pct,
            },
            quality_standard_weakened=False,
            junk_risk_assessment="medium",
        ),
        _scenario_result(
            "G_aggressive_hit_deck_ready_thresholds",
            "All remaining weak-source debt is resolved, effective source balance clears 6.0, and persona-core coverage reaches 80.0.",
            baseline_metrics,
            {
                "effective_balanced_source_count": normalized_effective,
                "persona_core_coverage_of_all_labeled_pct": recovery_80_pct,
                "weak_source_cost_center_count": 0,
                "core_readiness_weak_source_cost_center_count": 0,
                "weak_source_cost_centers": "",
                "core_readiness_weak_source_cost_centers": "",
                "source_failures": "",
                "exploratory_only_weak_source_debt_count": 0,
                "exploratory_only_weak_source_sources": "",
            },
            quality_standard_weakened=False,
            junk_risk_assessment="high",
        ),
    ]
    return scenarios


def _feasibility_decision(scenarios: list[dict[str, Any]], coverage_gap: dict[str, Any]) -> str:
    """Choose the single deck-ready feasibility decision label."""
    realistic = next(row for row in scenarios if row["scenario_id"] == "F_combined_realistic_one_source_win_plus_modest_balance_improvement")
    aggressive = next(row for row in scenarios if row["scenario_id"] == "G_aggressive_hit_deck_ready_thresholds")
    if realistic["deck_ready_candidate"]:
        return "deck_ready_feasible_with_targeted_cleanup"
    if aggressive["deck_ready_candidate"] and _to_int(coverage_gap["rows_needed_to_reach_80_0"]) >= 250:
        return "deck_ready_feasible_but_requires_large_data_quality_work"
    if aggressive["deck_ready_candidate"]:
        return "deck_ready_feasible_with_targeted_cleanup"
    return "reviewable_is_current_ceiling"


def _recommended_next_path(decision: str) -> str:
    """Map the feasibility decision to exactly one next path."""
    if decision == "deck_ready_feasible_with_targeted_cleanup":
        return "remaining weak-source remediation"
    if decision == "deck_ready_feasible_but_requires_large_data_quality_work":
        return "stop and freeze as reviewable release"
    if decision == "deck_ready_not_feasible_without_policy_redesign":
        return "deck-ready policy redesign"
    return "stop and freeze as reviewable release"


def _render_plan(report: dict[str, Any]) -> str:
    """Render the deck-ready feasibility markdown summary."""
    baseline = report["baseline"]
    lines = [
        "# Deck-Ready Feasibility Plan",
        "",
        "## Summary",
        "",
        f"- Current workbook readiness: `{baseline['persona_readiness_state']}` / `{baseline['quality_flag']}`",
        f"- Effective source balance: `{baseline['effective_balanced_source_count']}`",
        f"- Persona core coverage: `{baseline['persona_core_coverage_of_all_labeled_pct']}`",
        f"- Feasibility decision: `{report['deck_ready_feasibility_decision']}`",
        f"- Recommended next path: `{report['recommended_next_path']}`",
        "",
        "## Source Balance Gap",
        "",
        f"- Gap to deck-ready source-balance floor: `{report['source_balance_gap_analysis']['gap_to_deck_ready_floor']}`",
        f"- Main imbalance driver: `{report['source_balance_gap_analysis']['imbalance_driver']}`",
        "",
        "## Core Coverage Gap",
        "",
        f"- Rows needed to reach 75.0: `{report['core_coverage_gap_analysis']['rows_needed_to_reach_75_0']}`",
        f"- Rows needed to reach 80.0: `{report['core_coverage_gap_analysis']['rows_needed_to_reach_80_0']}`",
        f"- Current non-core labeled rows: `{report['core_coverage_gap_analysis']['current_non_core_labeled_row_count']}`",
        "",
        "## Scenario Simulation",
        "",
    ]
    for row in report["scenario_simulation"]:
        lines.append(
            f"- `{row['scenario_id']}`: status `{row['overall_status']}`, readiness `{row['persona_readiness_state']}`, "
            f"deck-ready candidate `{row['deck_ready_candidate']}`, source balance `{row['effective_balanced_source_count']}`, "
            f"core coverage `{row['persona_core_coverage_of_all_labeled_pct']}`, junk risk `{row['junk_risk_assessment']}`"
        )
    return "\n".join(lines) + "\n"


def build_deck_ready_feasibility_analysis(root_dir: Path) -> dict[str, Any]:
    """Build the diagnostics-only deck-ready feasibility report."""
    baseline_metrics, quality_df, source_balance_df, source_diagnostics_df, review_ready_gap = _baseline_metrics(root_dir)
    _ = quality_df
    _ = source_diagnostics_df
    balance_gap = _source_balance_gap_analysis(source_balance_df, baseline_metrics)
    coverage_gap, gap_df = _core_coverage_gap_analysis(root_dir, source_balance_df)
    scenarios = _scenario_simulation(baseline_metrics, balance_gap, coverage_gap)
    baseline = {
        "overall_status": str(baseline_metrics.get("overall_status", "")),
        "quality_flag": str(baseline_metrics.get("quality_flag", "")),
        "persona_readiness_state": str(baseline_metrics.get("persona_readiness_state", "")),
        "final_usable_persona_count": _to_int(baseline_metrics.get("final_usable_persona_count", 0)),
        "production_ready_persona_count": _to_int(baseline_metrics.get("production_ready_persona_count", 0)),
        "review_ready_persona_count": _to_int(baseline_metrics.get("review_ready_persona_count", 0)),
        "weak_source_cost_center_count": _to_int(baseline_metrics.get("weak_source_cost_center_count", 0)),
        "core_readiness_weak_source_cost_center_count": _to_int(
            baseline_metrics.get("core_readiness_weak_source_cost_center_count", 0)
        ),
        "effective_balanced_source_count": round(_to_float(baseline_metrics.get("effective_balanced_source_count", 0.0)), 2),
        "persona_core_coverage_of_all_labeled_pct": round(
            _to_float(baseline_metrics.get("persona_core_coverage_of_all_labeled_pct", 0.0)),
            1,
        ),
        "largest_source_influence_share_pct": round(_to_float(baseline_metrics.get("largest_source_influence_share_pct", 0.0)), 1),
        "top_3_cluster_share_of_core_labeled": round(_to_float(baseline_metrics.get("top_3_cluster_share_of_core_labeled", 0.0)), 3),
        "promoted_persona_example_coverage_pct": round(
            _to_float(baseline_metrics.get("promoted_persona_example_coverage_pct", 0.0)),
            1,
        ),
        "overall_unknown_ratio": round(_to_float(baseline_metrics.get("overall_unknown_ratio", 0.0)), 5),
    }
    decision = _feasibility_decision(scenarios, coverage_gap)
    report = {
        "baseline": baseline,
        "review_ready_gap_reference": review_ready_gap.get("baseline", {}),
        "source_balance_gap_analysis": balance_gap,
        "core_coverage_gap_analysis": coverage_gap,
        "scenario_simulation": scenarios,
        "deck_ready_feasibility_decision": decision,
        "recommended_next_path": _recommended_next_path(decision),
        "continue_toward_deck_ready": _recommended_next_path(decision) != "stop and freeze as reviewable release",
        "freeze_at_reviewable_release": _recommended_next_path(decision) == "stop and freeze as reviewable release",
        "plan_markdown": "",
    }
    report["plan_markdown"] = _render_plan(report)
    report["gap_table"] = gap_df.to_dict(orient="records")
    return report


def write_deck_ready_feasibility_artifacts(root_dir: Path, report: dict[str, Any]) -> dict[str, Path]:
    """Write the deck-ready feasibility JSON, CSV, and markdown artifacts."""
    json_path = root_dir / ROOT_DECK_READY_FEASIBILITY_ARTIFACT
    csv_path = root_dir / ROOT_SOURCE_BALANCE_COVERAGE_GAP_ARTIFACT
    doc_path = root_dir / ROOT_DECK_READY_FEASIBILITY_DOC
    json_path.parent.mkdir(parents=True, exist_ok=True)
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    pd.DataFrame(report["gap_table"]).to_csv(csv_path, index=False, encoding="utf-8")
    doc_path.write_text(report["plan_markdown"], encoding="utf-8")
    return {"json_path": json_path, "csv_path": csv_path, "doc_path": doc_path}
