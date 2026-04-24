"""Centralized quality metrics and status policy for workbook generation."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.utils.pipeline_schema import (
    CLUSTER_DOMINANCE_SHARE_PCT,
    CORE_LABEL_COLUMNS,
    DENOMINATOR_LABELED_EPISODE_ROWS,
    DENOMINATOR_RAW_RECORD_ROWS,
    DENOMINATOR_VALID_CANDIDATE_ROWS,
    PIPELINE_STAGE_METRIC_NAMES,
    QUALITY_FLAG_EXPLORATORY,
    QUALITY_FLAG_OK,
    QUALITY_FLAG_UNSTABLE,
    STATUS_FAIL,
    STATUS_OK,
    STATUS_WARN,
    row_has_unknown_labels,
    round_pct,
)

QUALITY_STATUS_POLICY: dict[str, dict[str, object]] = {
    "core_unknown": {
        "metric": "persona_core_unknown_ratio",
        "warn_threshold": 0.15,
        "fail_threshold": 0.30,
        "high_is_bad": True,
        "warn_reason": "core_unknown_high",
        "fail_reason": "core_unknown_critical",
        "display_threshold": "warn>=0.15; fail>=0.30",
    },
    "overall_unknown": {
        "metric": "overall_unknown_ratio",
        "warn_threshold": 0.25,
        "fail_threshold": 0.40,
        "high_is_bad": True,
        "warn_reason": "overall_unknown_high",
        "fail_reason": "overall_unknown_critical",
        "display_threshold": "warn>=0.25; fail>=0.40",
    },
    "core_coverage": {
        "metric": "persona_core_coverage_of_all_labeled_pct",
        "warn_threshold": 75.0,
        "fail_threshold": 60.0,
        "high_is_bad": False,
        "warn_reason": "core_coverage_low",
        "fail_reason": "core_coverage_critical",
        "display_threshold": "warn<75.0; fail<60.0",
    },
    "effective_source_diversity": {
        "metric": "effective_balanced_source_count",
        "warn_threshold": 6.0,
        "fail_threshold": 5.0,
        "high_is_bad": False,
        "warn_reason": "effective_source_balance_low",
        "fail_reason": "effective_source_balance_critical",
        "display_threshold": "warn<6.0; fail<5.0",
    },
    "source_concentration": {
        "metric": "largest_labeled_source_share_pct",
        "warn_threshold": 50.0,
        "fail_threshold": 70.0,
        "high_is_bad": True,
        "warn_reason": "source_concentration_high",
        "fail_reason": "source_concentration_critical",
        "display_threshold": "warn>=50.0; fail>=70.0",
    },
    "source_influence_concentration": {
        "metric": "largest_source_influence_share_pct",
        "warn_threshold": 35.0,
        "fail_threshold": 45.0,
        "high_is_bad": True,
        "warn_reason": "source_influence_concentration_high",
        "fail_reason": "source_influence_concentration_critical",
        "display_threshold": "warn>=35.0; fail>=45.0",
    },
    "weak_source_yield": {
        "metric": "weak_source_cost_center_count",
        "warn_threshold": 2.0,
        "fail_threshold": 4.0,
        "high_is_bad": True,
        "warn_reason": "weak_source_cost_centers_present",
        "fail_reason": "weak_source_cost_centers_excessive",
        "display_threshold": "warn>=2; fail>=4",
    },
    "largest_cluster_dominance": {
        "metric": "largest_cluster_share_of_core_labeled",
        "warn_threshold": 55.0,
        "fail_threshold": 70.0,
        "high_is_bad": True,
        "warn_reason": "largest_cluster_too_dominant_warn",
        "fail_reason": "largest_cluster_too_dominant",
        "display_threshold": "warn>=55.0; fail>=70.0",
    },
    "cluster_concentration_tail": {
        "metric": "top_3_cluster_share_of_core_labeled",
        "warn_threshold": 0.8,
        "fail_threshold": 0.9,
        "high_is_bad": True,
        "warn_reason": "top_3_cluster_concentration_high",
        "fail_reason": "top_3_cluster_concentration_critical",
        "display_threshold": "warn>=0.80; fail>=0.90",
    },
    "cluster_fragility": {
        "metric": "micro_cluster_count",
        "warn_threshold": 1.0,
        "fail_threshold": 3.0,
        "high_is_bad": True,
        "warn_reason": "micro_clusters_present",
        "fail_reason": "micro_clusters_excessive",
        "display_threshold": "warn>=1; fail>=3",
    },
    "cluster_evidence": {
        "metric": "thin_evidence_cluster_count",
        "warn_threshold": 1.0,
        "fail_threshold": 2.0,
        "high_is_bad": True,
        "warn_reason": "thin_evidence_clusters_present",
        "fail_reason": "thin_evidence_clusters_excessive",
        "display_threshold": "warn>=1; fail>=2",
    },
    "cluster_separation": {
        "metric": "min_cluster_separation",
        "warn_threshold": 0.12,
        "fail_threshold": 0.08,
        "high_is_bad": False,
        "warn_reason": "cluster_separation_low",
        "fail_reason": "cluster_separation_critical",
        "display_threshold": "warn<0.12; fail<0.08",
    },
    "grounding_coverage": {
        "metric": "promoted_persona_example_coverage_pct",
        "warn_threshold": 100.0,
        "fail_threshold": 80.0,
        "high_is_bad": False,
        "warn_reason": "promoted_persona_examples_missing",
        "fail_reason": "promoted_persona_examples_coverage_critical",
        "display_threshold": "warn<100.0; fail<80.0",
    },
}

READINESS_POLICY: dict[str, dict[str, object]] = {
    "reviewable_but_not_deck_ready": {
        "label": "Reviewable Draft",
        "asset_class": "reviewable_draft",
        "gate_status": STATUS_WARN,
        "usage_restriction": "Reviewable draft only. Not a final persona asset and not safe for deck-ready or production use.",
        "summary": "Reviewable draft with enough structure for analyst review, but still blocked from final persona use.",
        "requirements": {
            "overall_unknown_ratio": {"max": 0.30, "display": "overall_unknown_ratio<=0.30"},
            "persona_core_coverage_of_all_labeled_pct": {"min": 70.0, "display": "persona_core_coverage_of_all_labeled_pct>=70.0"},
            "promoted_persona_example_coverage_pct": {"min": 80.0, "display": "promoted_persona_example_coverage_pct>=80.0"},
            "largest_source_influence_share_pct": {"max": 45.0, "display": "largest_source_influence_share_pct<=45.0"},
            "fragile_tail_share_of_core_labeled": {"max": 0.12, "display": "fragile_tail_share_of_core_labeled<=0.12"},
            "final_usable_persona_count": {"min": 2.0, "display": "final_usable_persona_count>=2"},
        },
    },
    "deck_ready": {
        "label": "Final Persona Asset",
        "asset_class": "final_persona_asset",
        "gate_status": STATUS_OK,
        "usage_restriction": "Deck-ready final persona asset. Safe for presentation use, but still below the stricter production persona bar.",
        "summary": "Final persona asset for deck and stakeholder presentation use.",
        "requirements": {
            "overall_unknown_ratio": {"max": 0.20, "display": "overall_unknown_ratio<=0.20"},
            "persona_core_coverage_of_all_labeled_pct": {"min": 80.0, "display": "persona_core_coverage_of_all_labeled_pct>=80.0"},
            "promoted_persona_example_coverage_pct": {"min": 100.0, "display": "promoted_persona_example_coverage_pct>=100.0"},
            "largest_source_influence_share_pct": {"max": 35.0, "display": "largest_source_influence_share_pct<=35.0"},
            "fragile_tail_share_of_core_labeled": {"max": 0.08, "display": "fragile_tail_share_of_core_labeled<=0.08"},
            "final_usable_persona_count": {"min": 3.0, "display": "final_usable_persona_count>=3"},
        },
    },
    "production_persona_ready": {
        "label": "Final Persona Asset",
        "asset_class": "final_persona_asset",
        "gate_status": STATUS_OK,
        "usage_restriction": "Production-ready final persona asset. Safe for downstream production persona usage under the current policy.",
        "summary": "Final persona asset that clears the stricter production persona gate.",
        "requirements": {
            "overall_unknown_ratio": {"max": 0.15, "display": "overall_unknown_ratio<=0.15"},
            "persona_core_coverage_of_all_labeled_pct": {"min": 90.0, "display": "persona_core_coverage_of_all_labeled_pct>=90.0"},
            "promoted_persona_example_coverage_pct": {"min": 100.0, "display": "promoted_persona_example_coverage_pct>=100.0"},
            "largest_source_influence_share_pct": {"max": 25.0, "display": "largest_source_influence_share_pct<=25.0"},
            "fragile_tail_share_of_core_labeled": {"max": 0.05, "display": "fragile_tail_share_of_core_labeled<=0.05"},
            "final_usable_persona_count": {"min": 4.0, "display": "final_usable_persona_count>=4"},
        },
    },
}

READINESS_STATE_META: dict[str, dict[str, object]] = {
    "exploratory_only": {
        "label": "Hypothesis Material",
        "asset_class": "hypothesis_material",
        "gate_status": STATUS_FAIL,
        "usage_restriction": "Hypothesis material only. Not a final persona asset and not safe for review sign-off, deck-ready use, or production persona use.",
        "summary": "Exploratory workbook only. Treat the contents as hypothesis material, not final personas.",
    },
    "reviewable_but_not_deck_ready": {
        "label": str(READINESS_POLICY["reviewable_but_not_deck_ready"]["label"]),
        "asset_class": str(READINESS_POLICY["reviewable_but_not_deck_ready"]["asset_class"]),
        "gate_status": str(READINESS_POLICY["reviewable_but_not_deck_ready"]["gate_status"]),
        "usage_restriction": str(READINESS_POLICY["reviewable_but_not_deck_ready"]["usage_restriction"]),
        "summary": str(READINESS_POLICY["reviewable_but_not_deck_ready"]["summary"]),
    },
    "deck_ready": {
        "label": str(READINESS_POLICY["deck_ready"]["label"]),
        "asset_class": str(READINESS_POLICY["deck_ready"]["asset_class"]),
        "gate_status": str(READINESS_POLICY["deck_ready"]["gate_status"]),
        "usage_restriction": str(READINESS_POLICY["deck_ready"]["usage_restriction"]),
        "summary": str(READINESS_POLICY["deck_ready"]["summary"]),
    },
    "production_persona_ready": {
        "label": str(READINESS_POLICY["production_persona_ready"]["label"]),
        "asset_class": str(READINESS_POLICY["production_persona_ready"]["asset_class"]),
        "gate_status": str(READINESS_POLICY["production_persona_ready"]["gate_status"]),
        "usage_restriction": str(READINESS_POLICY["production_persona_ready"]["usage_restriction"]),
        "summary": str(READINESS_POLICY["production_persona_ready"]["summary"]),
    },
}


def build_quality_metrics(
    stage_counts: dict[str, int],
    labeled_df: pd.DataFrame,
    source_stage_counts_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
    cluster_profiles: list[dict[str, object]] | None = None,
    cluster_robustness_summary_df: pd.DataFrame | None = None,
) -> dict[str, object]:
    """Compute raw quality metrics only, without applying status policy."""
    cluster_profiles = cluster_profiles or []
    cluster_robustness_summary_df = cluster_robustness_summary_df if cluster_robustness_summary_df is not None else pd.DataFrame()
    core_labeled_df = _persona_core_subset(labeled_df)
    labeled_count = int(len(labeled_df))
    persona_core_labeled_count = int(len(core_labeled_df))
    labeled_sources = int((source_stage_counts_df.get("labeled_episode_count", pd.Series(dtype=int)) > 0).sum()) if not source_stage_counts_df.empty else 0
    raw_sources = int((source_stage_counts_df.get("raw_record_count", pd.Series(dtype=int)) > 0).sum()) if not source_stage_counts_df.empty else 0
    effective_labeled_source_count = round(float(_effective_labeled_source_count(source_stage_counts_df)), 2)
    effective_balanced_source_count = round(float(_effective_balanced_source_count(source_stage_counts_df)), 2)
    largest_cluster_share = _largest_cluster_share(cluster_stats_df)
    largest_labeled_source_share = _largest_labeled_source_share(source_stage_counts_df, labeled_count)
    promoted_persona_episode_rows = int(pd.to_numeric(source_stage_counts_df.get("promoted_persona_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not source_stage_counts_df.empty else 0
    grounded_promoted_persona_episode_rows = int(pd.to_numeric(source_stage_counts_df.get("grounded_promoted_persona_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not source_stage_counts_df.empty else 0
    largest_promoted_source_share = _largest_source_share(source_stage_counts_df, "promoted_persona_episode_count")
    largest_grounded_source_share = _largest_source_share(source_stage_counts_df, "grounded_promoted_persona_episode_count")
    largest_source_influence_share = _largest_source_influence_share(source_stage_counts_df)
    weak_source_cost_centers = _weak_source_cost_centers(source_stage_counts_df)
    promoted_persona_count, promoted_with_examples, promoted_missing_examples, grounding_counts = _promoted_persona_example_counts(cluster_stats_df, persona_examples_df)
    promotion_semantics = _persona_promotion_semantics(cluster_stats_df)
    visible_micro_cluster_count = _visible_micro_cluster_count(cluster_stats_df)
    effective_thin_evidence_cluster_count = _effective_thin_evidence_cluster_count(cluster_stats_df)
    promoted_persona_example_coverage_pct = round_pct(promoted_with_examples, promoted_persona_count) if promoted_persona_count else 100.0
    min_cluster_size = _persona_min_cluster_size(labeled_count)
    selected_example_grounding_issue_count = _selected_example_grounding_issue_count(persona_examples_df)
    promoted_persona_grounding_failure_count = int(grounding_counts.get("weakly_grounded", 0)) + int(grounding_counts.get("ungrounded", 0))
    cluster_distribution = [
        {
            "cluster_id": str(row.get("cluster_id", "")),
            "size": int(row.get("size", 0)),
            "share_of_core_labeled": float(row.get("share_of_total", 0.0)),
        }
        for row in cluster_profiles
    ]
    robustness_metrics = _cluster_robustness_metric_lookup(cluster_robustness_summary_df)
    return {
        **{metric: int(stage_counts.get(metric, 0) or 0) for metric in PIPELINE_STAGE_METRIC_NAMES},
        "persona_core_labeled_rows": persona_core_labeled_count,
        "persona_core_unknown_ratio": round(_row_unknown_ratio(core_labeled_df), 6),
        "overall_unknown_ratio": round(_row_unknown_ratio(labeled_df), 6),
        "persona_core_coverage_of_all_labeled_pct": round_pct(persona_core_labeled_count, labeled_count) if labeled_count else 0.0,
        "cluster_count": int(len(cluster_profiles)),
        "cluster_distribution": cluster_distribution,
        "robust_cluster_count": int(robustness_metrics.get("robust_cluster_count", len(cluster_profiles))),
        "stable_cluster_count": int(robustness_metrics.get("stable_cluster_count", 0)),
        "fragile_cluster_count": int(robustness_metrics.get("fragile_cluster_count", 0)),
        "micro_cluster_count": int(visible_micro_cluster_count),
        "thin_evidence_cluster_count": int(effective_thin_evidence_cluster_count),
        "structurally_supported_cluster_count": int(robustness_metrics.get("structurally_supported_cluster_count", 0)),
        "weak_separation_cluster_count": int(robustness_metrics.get("weak_separation_cluster_count", 0)),
        "fragile_tail_cluster_count": int(robustness_metrics.get("fragile_tail_cluster_count", 0)),
        "fragile_tail_share_of_core_labeled": round(float(robustness_metrics.get("fragile_tail_share_of_core_labeled", 0.0)), 4),
        "top_3_cluster_share_of_core_labeled": round(float(robustness_metrics.get("top_3_cluster_share_of_core_labeled", 0.0)), 4),
        "avg_cluster_separation": round(float(robustness_metrics.get("avg_cluster_separation", 0.0)), 4),
        "min_cluster_separation": round(float(robustness_metrics.get("min_cluster_separation", 0.0)), 4),
        "labeled_source_count": labeled_sources,
        "effective_labeled_source_count": effective_labeled_source_count,
        "effective_balanced_source_count": effective_balanced_source_count,
        "raw_source_count": raw_sources,
        "min_cluster_size": min_cluster_size,
        "largest_cluster_share_of_core_labeled": largest_cluster_share,
        "largest_labeled_source_share_pct": largest_labeled_source_share,
        "promoted_persona_episode_rows": promoted_persona_episode_rows,
        "grounded_promoted_persona_episode_rows": grounded_promoted_persona_episode_rows,
        "largest_promoted_source_share_pct": largest_promoted_source_share,
        "largest_grounded_source_share_pct": largest_grounded_source_share,
        "largest_source_influence_share_pct": largest_source_influence_share,
        "weak_source_cost_center_count": len(weak_source_cost_centers),
        "weak_source_cost_centers": " | ".join(weak_source_cost_centers),
        "single_cluster_dominance": largest_cluster_share > CLUSTER_DOMINANCE_SHARE_PCT,
        "small_promoted_persona_count": _small_promoted_count(cluster_stats_df, min_cluster_size),
        "selected_example_grounding_issue_count": selected_example_grounding_issue_count,
        "promoted_persona_grounding_failure_count": promoted_persona_grounding_failure_count,
        "promoted_candidate_persona_count": int(promotion_semantics.get("promoted_candidate_persona_count", 0)),
        "promotion_visibility_persona_count": int(promotion_semantics.get("promotion_visibility_persona_count", promoted_persona_count)),
        "headline_persona_count": int(promotion_semantics.get("headline_persona_count", grounding_counts.get("grounded", 0))),
        "final_usable_persona_count": int(promotion_semantics.get("final_usable_persona_count", grounding_counts.get("grounded", 0))),
        "deck_ready_persona_count": int(promotion_semantics.get("deck_ready_persona_count", grounding_counts.get("grounded", 0))),
        "promoted_persona_count": promoted_persona_count,
        "promoted_personas_with_examples": promoted_with_examples,
        "promoted_personas_missing_examples": " | ".join(promoted_missing_examples),
        "promoted_persona_example_coverage_pct": promoted_persona_example_coverage_pct,
        "promoted_persona_grounded_count": int(grounding_counts.get("grounded", 0)),
        "promoted_persona_weakly_grounded_count": int(grounding_counts.get("weakly_grounded", 0)),
        "promoted_persona_ungrounded_count": int(grounding_counts.get("ungrounded", 0)),
        "promoted_personas_weakly_grounded": " | ".join(grounding_counts.get("weak_ids", [])),
        "source_failures": " | ".join(_failed_sources(source_stage_counts_df)),
        "denominator_consistency": "explicit",
        "largest_cluster_share_denominator_type": "persona_core_labeled_rows",
    }


def _cluster_robustness_metric_lookup(cluster_robustness_summary_df: pd.DataFrame) -> dict[str, float]:
    """Flatten robustness summary rows into a metric lookup."""
    if cluster_robustness_summary_df is None or cluster_robustness_summary_df.empty:
        return {}
    metrics: dict[str, float] = {}
    for _, row in cluster_robustness_summary_df.iterrows():
        metric = str(row.get("metric", "")).strip()
        if not metric:
            continue
        value = row.get("value", 0)
        try:
            metrics[metric] = float(value)
        except (TypeError, ValueError):
            continue
    return metrics


def _workbook_visible_cluster_rows(cluster_stats_df: pd.DataFrame) -> pd.DataFrame:
    """Return only clusters that remain visible in the final workbook."""
    if cluster_stats_df.empty:
        return cluster_stats_df
    workbook_review_visible = cluster_stats_df.get("workbook_review_visible", pd.Series(dtype=bool))
    if workbook_review_visible.empty:
        return cluster_stats_df[cluster_stats_df.get("promotion_status", pd.Series(dtype=str)).astype(str).isin({"promoted_persona", "review_visible_persona"})].copy()
    return cluster_stats_df[workbook_review_visible.fillna(False).astype(bool)].copy()


def _visible_micro_cluster_count(cluster_stats_df: pd.DataFrame) -> int:
    """Count only workbook-visible micro clusters for final quality gating."""
    visible = _workbook_visible_cluster_rows(cluster_stats_df)
    if visible.empty:
        return 0
    return int(visible.get("cluster_stability_status", pd.Series(dtype=str)).astype(str).eq("micro").sum())


def _effective_thin_evidence_cluster_count(cluster_stats_df: pd.DataFrame) -> int:
    """Count thin clusters only when final visible evidence is still weak."""
    visible = _workbook_visible_cluster_rows(cluster_stats_df)
    if visible.empty:
        return 0
    thin_mask = visible.get("cluster_evidence_status", pd.Series(dtype=str)).astype(str).eq("thin")
    well_grounded_mask = (
        visible.get("promotion_grounding_status", pd.Series(dtype=str)).astype(str).eq("promoted_and_grounded")
        & (pd.to_numeric(visible.get("bundle_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0) >= 3)
        & (pd.to_numeric(visible.get("selected_example_count", pd.Series(dtype=int)), errors="coerce").fillna(0) >= 2)
    )
    return int((thin_mask & ~well_grounded_mask).sum())


def evaluate_quality_status(metrics: dict[str, object]) -> dict[str, object]:
    """Apply the single source-of-truth quality policy to raw metrics."""
    axes = {
        axis_name: _evaluate_axis_from_policy(axis_name, metrics)
        for axis_name in QUALITY_STATUS_POLICY
    }
    axes["effective_source_diversity"] = _append_reason_if(
        axes["effective_source_diversity"],
        bool(str(metrics.get("source_failures", "") or "").strip()),
        "raw_covered_sources_missing_labels",
    )
    axes["weak_source_yield"] = _append_reason_if(
        axes["weak_source_yield"],
        bool(str(metrics.get("weak_source_cost_centers", "") or "").strip()),
        "weak_source_cost_centers_listed",
    )
    axes["grounding_coverage"] = _append_reason_if(
        axes["grounding_coverage"],
        int(metrics.get("selected_example_grounding_issue_count", 0) or 0) > 0,
        "selected_example_grounding_weak",
    )
    axes["grounding_coverage"] = _append_reason_if(
        axes["grounding_coverage"],
        bool(str(metrics.get("promoted_personas_missing_examples", "") or "").strip()),
        "promoted_persona_examples_missing",
    )
    axes["grounding_coverage"] = _escalate_axis_status(
        axes["grounding_coverage"],
        int(metrics.get("promoted_persona_weakly_grounded_count", 0) or 0) > 0,
        STATUS_WARN,
        "promoted_persona_grounding_weak",
    )
    if int(metrics.get("final_usable_persona_count", 0) or 0) <= 3:
        axes["cluster_concentration_tail"] = {
            **axes["cluster_concentration_tail"],
            "status": STATUS_OK,
            "reason_keys": [],
        }
    grouped = {
        "core_clustering": _compose_axis_group(
            [
                "core_unknown",
                "core_coverage",
                "largest_cluster_dominance",
                "cluster_concentration_tail",
                "cluster_fragility",
                "cluster_evidence",
                "cluster_separation",
            ],
            axes,
        ),
        "source_diversity": _compose_axis_group(
            ["effective_source_diversity", "source_concentration", "source_influence_concentration", "weak_source_yield"],
            axes,
        ),
        "example_grounding": _compose_axis_group(["grounding_coverage"], axes),
    }
    composite_reason_keys = _collect_reason_keys(axes)
    composite_status = _compose_status([axis["status"] for axis in axes.values()])
    readiness = evaluate_persona_readiness(metrics)
    readiness = _cap_readiness_by_quality_status(
        readiness=readiness,
        composite_status=composite_status,
        quality_flag=_quality_flag_from_status(composite_status),
    )
    result = {
        "metrics": dict(metrics),
        "axes": axes,
        "groups": grouped,
        "composite_status": composite_status,
        "composite_reason_keys": composite_reason_keys,
        "quality_flag": _quality_flag_from_status(composite_status),
        "quality_flag_rule": "UNSTABLE if any axis status is FAIL; EXPLORATORY if no FAIL and any axis status is WARN; otherwise OK.",
        "readiness": readiness,
    }
    return result


def flatten_quality_status_result(evaluated: dict[str, object]) -> dict[str, object]:
    """Flatten evaluated quality result for workbook rendering without changing policy."""
    metrics = dict(evaluated.get("metrics", {}) or {})
    axes = dict(evaluated.get("axes", {}) or {})
    groups = dict(evaluated.get("groups", {}) or {})
    readiness = dict(evaluated.get("readiness", {}) or {})
    result = dict(metrics)
    for axis_name, payload in axes.items():
        result[f"{axis_name}_status"] = payload["status"]
        result[f"{axis_name}_reason_keys"] = " | ".join(payload["reason_keys"])
        result[f"{axis_name}_threshold_rule"] = str(payload.get("display_threshold", "") or "")
    result["core_clustering_status"] = groups.get("core_clustering", {}).get("status", STATUS_OK)
    result["core_clustering_reason_keys"] = " | ".join(groups.get("core_clustering", {}).get("reason_keys", []))
    result["source_diversity_status"] = groups.get("source_diversity", {}).get("status", STATUS_OK)
    result["source_diversity_reason_keys"] = " | ".join(groups.get("source_diversity", {}).get("reason_keys", []))
    result["example_grounding_status"] = groups.get("example_grounding", {}).get("status", STATUS_OK)
    result["example_grounding_reason_keys"] = " | ".join(groups.get("example_grounding", {}).get("reason_keys", []))
    result["overall_status"] = str(evaluated.get("composite_status", STATUS_OK))
    result["composite_reason_keys"] = " | ".join(evaluated.get("composite_reason_keys", []) or [])
    result["quality_flag"] = str(evaluated.get("quality_flag", QUALITY_FLAG_OK))
    result["quality_flag_rule"] = str(evaluated.get("quality_flag_rule", "") or "")
    result["persona_readiness_state"] = str(readiness.get("state", "exploratory_only"))
    result["persona_readiness_label"] = str(readiness.get("label", "Hypothesis Material"))
    result["persona_asset_class"] = str(readiness.get("asset_class", "hypothesis_material"))
    result["persona_readiness_gate_status"] = str(readiness.get("gate_status", STATUS_FAIL))
    result["persona_readiness_rule"] = str(readiness.get("rule", "") or "")
    result["persona_readiness_blockers"] = str(readiness.get("blockers", "") or "")
    result["persona_readiness_summary"] = str(readiness.get("summary", "") or "")
    result["persona_usage_restriction"] = str(readiness.get("usage_restriction", "") or "")
    result["persona_completion_claim_allowed"] = bool(readiness.get("completion_claim_allowed", False))
    return result


def quality_display_thresholds() -> dict[str, str]:
    """Return workbook display thresholds derived from the centralized policy."""
    return {
        "persona_core_unknown_ratio": str(QUALITY_STATUS_POLICY["core_unknown"]["display_threshold"]),
        "overall_unknown_ratio": str(QUALITY_STATUS_POLICY["overall_unknown"]["display_threshold"]),
        "persona_core_coverage_of_all_labeled_pct": str(QUALITY_STATUS_POLICY["core_coverage"]["display_threshold"]),
        "effective_balanced_source_count": str(QUALITY_STATUS_POLICY["effective_source_diversity"]["display_threshold"]),
        "largest_labeled_source_share_pct": str(QUALITY_STATUS_POLICY["source_concentration"]["display_threshold"]),
        "largest_source_influence_share_pct": str(QUALITY_STATUS_POLICY["source_influence_concentration"]["display_threshold"]),
        "weak_source_cost_center_count": str(QUALITY_STATUS_POLICY["weak_source_yield"]["display_threshold"]),
        "largest_cluster_share_of_core_labeled": str(QUALITY_STATUS_POLICY["largest_cluster_dominance"]["display_threshold"]),
        "top_3_cluster_share_of_core_labeled": str(QUALITY_STATUS_POLICY["cluster_concentration_tail"]["display_threshold"]),
        "fragile_tail_share_of_core_labeled": _readiness_threshold_display("fragile_tail_share_of_core_labeled"),
        "micro_cluster_count": str(QUALITY_STATUS_POLICY["cluster_fragility"]["display_threshold"]),
        "thin_evidence_cluster_count": str(QUALITY_STATUS_POLICY["cluster_evidence"]["display_threshold"]),
        "min_cluster_separation": str(QUALITY_STATUS_POLICY["cluster_separation"]["display_threshold"]),
        "promoted_persona_example_coverage_pct": str(QUALITY_STATUS_POLICY["grounding_coverage"]["display_threshold"]),
        "final_usable_persona_count": _readiness_threshold_display("final_usable_persona_count"),
        "persona_readiness_state": _persona_readiness_rule(),
        "persona_readiness_gate_status": "FAIL below reviewable threshold; WARN for reviewable_but_not_deck_ready; OK for deck_ready or production_persona_ready.",
        "persona_completion_claim_allowed": "true only when persona_readiness_state is deck_ready or production_persona_ready.",
        "overall_status": "FAIL if any axis FAIL; WARN if no FAIL and any axis WARN; otherwise OK.",
        "quality_flag": "UNSTABLE if overall_status=FAIL; EXPLORATORY if overall_status=WARN; otherwise OK.",
    }


def evaluate_persona_readiness(metrics: dict[str, object]) -> dict[str, object]:
    """Evaluate workbook readiness so exploratory work cannot be mistaken for a final asset."""
    ordered_states = ["production_persona_ready", "deck_ready", "reviewable_but_not_deck_ready"]
    unmet_by_state = {
        state: _unmet_readiness_requirements(metrics, dict(READINESS_POLICY[state].get("requirements", {})))
        for state in ordered_states
    }
    chosen_state = "exploratory_only"
    for state in ordered_states:
        if not unmet_by_state[state]:
            chosen_state = state
            break
    meta = dict(READINESS_STATE_META[chosen_state])
    next_target = _next_readiness_target(chosen_state)
    blockers_source = unmet_by_state.get(next_target, []) if next_target else []
    blockers = " | ".join(blockers_source)
    return {
        "state": chosen_state,
        "label": str(meta.get("label", "")),
        "asset_class": str(meta.get("asset_class", "")),
        "gate_status": str(meta.get("gate_status", STATUS_FAIL)),
        "rule": _persona_readiness_rule(),
        "blockers": blockers,
        "summary": str(meta.get("summary", "")),
        "usage_restriction": str(meta.get("usage_restriction", "")),
        "completion_claim_allowed": chosen_state in {"deck_ready", "production_persona_ready"},
        "next_target_state": next_target or "",
    }


def _cap_readiness_by_quality_status(
    readiness: dict[str, object],
    composite_status: str,
    quality_flag: str,
) -> dict[str, object]:
    """Keep workbook readiness conservative when workbook quality is still warning or failing."""
    chosen_state = str(readiness.get("state", "exploratory_only") or "exploratory_only")
    if composite_status == STATUS_OK and quality_flag == QUALITY_FLAG_OK:
        return readiness

    capped_state = "exploratory_only"
    if composite_status == STATUS_WARN and quality_flag == QUALITY_FLAG_EXPLORATORY:
        capped_state = "reviewable_but_not_deck_ready"
    if chosen_state not in {"deck_ready", "production_persona_ready"} and chosen_state == capped_state:
        return readiness
    if chosen_state == "exploratory_only" and capped_state == "reviewable_but_not_deck_ready":
        return readiness

    meta = dict(READINESS_STATE_META[capped_state])
    existing_blockers = [item for item in str(readiness.get("blockers", "") or "").split(" | ") if item]
    quality_blocker = f"overall_status={composite_status} keeps workbook below deck_ready"
    blockers = " | ".join([quality_blocker, *existing_blockers]) if quality_blocker not in existing_blockers else " | ".join(existing_blockers)
    next_target = _next_readiness_target(capped_state)
    return {
        **readiness,
        "state": capped_state,
        "label": str(meta.get("label", "")),
        "asset_class": str(meta.get("asset_class", "")),
        "gate_status": str(meta.get("gate_status", STATUS_FAIL)),
        "summary": str(meta.get("summary", "")),
        "usage_restriction": str(meta.get("usage_restriction", "")),
        "completion_claim_allowed": False,
        "blockers": blockers,
        "next_target_state": next_target or "",
    }


def _unmet_readiness_requirements(metrics: dict[str, object], requirements: dict[str, dict[str, float | str]]) -> list[str]:
    """Return unmet readiness requirements using human-readable threshold labels."""
    unmet: list[str] = []
    for metric_name, payload in requirements.items():
        value = float(metrics.get(metric_name, 0.0) or 0.0)
        minimum = payload.get("min")
        maximum = payload.get("max")
        if minimum is not None and value < float(minimum):
            unmet.append(str(payload.get("display", f"{metric_name}>={minimum}")))
            continue
        if maximum is not None and value > float(maximum):
            unmet.append(str(payload.get("display", f"{metric_name}<={maximum}")))
    return unmet


def _next_readiness_target(state: str) -> str | None:
    """Return the next readiness tier above the chosen state."""
    if state == "exploratory_only":
        return "reviewable_but_not_deck_ready"
    if state == "reviewable_but_not_deck_ready":
        return "deck_ready"
    if state == "deck_ready":
        return "production_persona_ready"
    return None


def _persona_readiness_rule() -> str:
    """Render the workbook readiness rule in one reviewer-facing sentence."""
    return (
        "exploratory_only below reviewable thresholds; reviewable_but_not_deck_ready requires explicit floors for overall_unknown_ratio, persona_core_coverage_of_all_labeled_pct, promoted_persona_example_coverage_pct, final_usable_persona_count, largest_source_influence_share_pct, and fragile_tail_share_of_core_labeled; "
        "deck_ready is the first state allowed to claim a final persona asset; production_persona_ready requires the stricter production thresholds for those same metrics."
    )


def _readiness_threshold_display(metric_name: str) -> str:
    """Render one readiness-threshold summary for workbook-facing display."""
    ordered_states = ["reviewable_but_not_deck_ready", "deck_ready", "production_persona_ready"]
    parts: list[str] = []
    for state in ordered_states:
        requirement = dict(READINESS_POLICY.get(state, {}).get("requirements", {})).get(metric_name)
        if not requirement:
            continue
        parts.append(f"{state}:{requirement.get('display', metric_name)}")
    return "; ".join(parts)


def _persona_core_subset(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Return persona-core subset when available."""
    if labeled_df.empty or "persona_core_eligible" not in labeled_df.columns:
        return labeled_df
    return labeled_df[labeled_df["persona_core_eligible"].fillna(True)]


def _row_unknown_ratio(labeled_df: pd.DataFrame) -> float:
    """Return ratio of rows with unresolved core label families."""
    if labeled_df.empty:
        return 1.0
    label_columns = [column for column in CORE_LABEL_COLUMNS if column in labeled_df.columns]
    unknown_mask = labeled_df[label_columns].apply(lambda row: row_has_unknown_labels(row.tolist()), axis=1)
    return float(unknown_mask.mean())


def _effective_labeled_source_count(source_stage_counts_df: pd.DataFrame) -> float:
    """Return weak-contribution weighted labeled source count."""
    if source_stage_counts_df.empty or "labeled_episode_count" not in source_stage_counts_df.columns:
        return 0.0
    counts = pd.to_numeric(source_stage_counts_df["labeled_episode_count"], errors="coerce").fillna(0).astype(int)
    return float(sum(min(1.0, float(count) / 5.0) for count in counts.tolist()))


def _effective_balanced_source_count(source_stage_counts_df: pd.DataFrame) -> float:
    """Return effective source count after blending labeled, promoted, and grounded influence."""
    influence_df = _source_influence_frame(source_stage_counts_df)
    if influence_df.empty:
        return 0.0
    shares = pd.to_numeric(influence_df["blended_influence_share_pct"], errors="coerce").fillna(0.0) / 100.0
    hhi = float((shares.pow(2)).sum())
    if hhi <= 0.0:
        return 0.0
    return 1.0 / hhi


def _largest_cluster_share(cluster_stats_df: pd.DataFrame) -> float:
    """Return largest persona-core cluster share percentage."""
    if cluster_stats_df.empty or "share_of_core_labeled" not in cluster_stats_df.columns:
        return 0.0
    values = pd.to_numeric(cluster_stats_df["share_of_core_labeled"], errors="coerce").fillna(0)
    return round(float(values.max()), 1) if not values.empty else 0.0


def _largest_labeled_source_share(source_stage_counts_df: pd.DataFrame, labeled_count: int) -> float:
    """Return largest labeled-source share across all labeled rows."""
    if source_stage_counts_df.empty or labeled_count <= 0 or "labeled_episode_count" not in source_stage_counts_df.columns:
        return 0.0
    counts = pd.to_numeric(source_stage_counts_df["labeled_episode_count"], errors="coerce").fillna(0)
    return round(float(counts.max()) / float(labeled_count) * 100.0, 1) if not counts.empty else 0.0


def _largest_source_share(source_stage_counts_df: pd.DataFrame, column: str) -> float:
    """Return largest share percentage for one source metric column."""
    if source_stage_counts_df.empty or column not in source_stage_counts_df.columns:
        return 0.0
    counts = pd.to_numeric(source_stage_counts_df[column], errors="coerce").fillna(0.0)
    total = float(counts.sum())
    if total <= 0.0 or counts.empty:
        return 0.0
    return round(float(counts.max()) / total * 100.0, 1)


def _largest_source_influence_share(source_stage_counts_df: pd.DataFrame) -> float:
    """Return largest blended downstream influence share across all sources."""
    influence_df = _source_influence_frame(source_stage_counts_df)
    if influence_df.empty:
        return 0.0
    values = pd.to_numeric(influence_df["blended_influence_share_pct"], errors="coerce").fillna(0.0)
    return round(float(values.max()), 1) if not values.empty else 0.0


def _weak_source_cost_centers(source_stage_counts_df: pd.DataFrame) -> list[str]:
    """Return raw-covered sources that remain weak downstream cost centers."""
    influence_df = _source_influence_frame(source_stage_counts_df)
    if influence_df.empty:
        return []
    matches = influence_df[
        (influence_df["raw_record_count"] >= 100)
        & (influence_df["blended_influence_share_pct"] < 10.0)
        & (
            (influence_df["prefilter_retention_pct"] < 10.0)
            | (influence_df["episode_yield"] < 0.5)
            | (influence_df["labelable_episode_ratio_pct"] < 50.0)
            | (influence_df["grounded_promoted_persona_episode_count"] <= 0)
        )
    ]
    return sorted(matches.get("source", pd.Series(dtype=str)).astype(str).tolist())


def _source_influence_frame(source_stage_counts_df: pd.DataFrame) -> pd.DataFrame:
    """Return a per-source frame with labeled, promoted, grounded, and blended shares."""
    if source_stage_counts_df.empty:
        return pd.DataFrame()
    frame = source_stage_counts_df.copy()
    for column in [
        "raw_record_count",
        "valid_post_count",
        "prefiltered_valid_post_count",
        "episode_count",
        "labelable_episode_count",
        "labeled_episode_count",
        "promoted_persona_episode_count",
        "grounded_promoted_persona_episode_count",
    ]:
        if column not in frame.columns:
            frame[column] = 0
    promoted_contribution_column = "source_normalized_promoted_persona_contribution" if "source_normalized_promoted_persona_contribution" in frame.columns else "promoted_persona_episode_count"
    grounded_contribution_column = "source_normalized_grounded_persona_contribution" if "source_normalized_grounded_persona_contribution" in frame.columns else "grounded_promoted_persona_episode_count"
    labeled_total = float(pd.to_numeric(frame.get("labeled_episode_count", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum())
    promoted_total = float(pd.to_numeric(frame.get(promoted_contribution_column, pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum())
    grounded_total = float(pd.to_numeric(frame.get(grounded_contribution_column, pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum())
    frame["labeled_share_pct"] = pd.to_numeric(frame.get("labeled_episode_count", pd.Series(dtype=float)), errors="coerce").fillna(0.0).map(
        lambda value: round(float(value) / labeled_total * 100.0, 1) if labeled_total > 0.0 else 0.0
    )
    frame["promoted_share_pct"] = pd.to_numeric(frame.get(promoted_contribution_column, pd.Series(dtype=float)), errors="coerce").fillna(0.0).map(
        lambda value: round(float(value) / promoted_total * 100.0, 1) if promoted_total > 0.0 else 0.0
    )
    frame["grounded_share_pct"] = pd.to_numeric(frame.get(grounded_contribution_column, pd.Series(dtype=float)), errors="coerce").fillna(0.0).map(
        lambda value: round(float(value) / grounded_total * 100.0, 1) if grounded_total > 0.0 else 0.0
    )
    active_columns = [column for column, total in [("labeled_share_pct", labeled_total), ("promoted_share_pct", promoted_total), ("grounded_share_pct", grounded_total)] if total > 0.0]
    if active_columns:
        frame["blended_influence_share_pct"] = frame[active_columns].mean(axis=1).round(1)
    else:
        frame["blended_influence_share_pct"] = 0.0
    frame["prefilter_retention_pct"] = frame.apply(
        lambda row: round_pct(row.get("prefiltered_valid_post_count", 0), row.get("valid_post_count", 0)) if float(row.get("valid_post_count", 0) or 0) > 0 else 0.0,
        axis=1,
    )
    frame["episode_yield"] = frame.apply(
        lambda row: round(float(row.get("episode_count", 0) or 0) / float(row.get("prefiltered_valid_post_count", 0) or 1), 2)
        if float(row.get("prefiltered_valid_post_count", 0) or 0) > 0
        else 0.0,
        axis=1,
    )
    frame["labelable_episode_ratio_pct"] = frame.apply(
        lambda row: round_pct(row.get("labelable_episode_count", 0), row.get("labeled_episode_count", 0)) if float(row.get("labeled_episode_count", 0) or 0) > 0 else 0.0,
        axis=1,
    )
    return frame


def _failed_sources(source_stage_counts_df: pd.DataFrame) -> list[str]:
    """Return raw-covered sources with zero labeled output."""
    if source_stage_counts_df.empty:
        return []
    raw_counts = pd.to_numeric(source_stage_counts_df.get("raw_record_count", pd.Series(dtype=int)), errors="coerce").fillna(0)
    labeled_counts = pd.to_numeric(source_stage_counts_df.get("labeled_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0)
    return source_stage_counts_df[(raw_counts > 0) & (labeled_counts <= 0)]["source"].astype(str).tolist()


def _promoted_persona_example_counts(
    cluster_stats_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
) -> tuple[int, int, list[str], dict[str, object]]:
    """Return promoted persona count, with-example count, and missing persona ids."""
    if cluster_stats_df.empty or "promotion_status" not in cluster_stats_df.columns:
        return 0, 0, [], {"grounded": 0, "weakly_grounded": 0, "ungrounded": 0, "weak_ids": []}
    workbook_review_visible = cluster_stats_df.get("workbook_review_visible", pd.Series(dtype=bool))
    if workbook_review_visible.empty:
        visible_mask = cluster_stats_df["promotion_status"].astype(str).isin({"promoted_persona", "review_visible_persona"})
    else:
        visible_mask = workbook_review_visible.fillna(False).astype(bool)
    promoted = cluster_stats_df[visible_mask].copy()
    promoted_ids = promoted.get("persona_id", pd.Series(dtype=str)).astype(str).tolist()
    if not promoted_ids:
        return 0, 0, [], {"grounded": 0, "weakly_grounded": 0, "ungrounded": 0, "weak_ids": []}
    grounding_lookup = promoted.set_index("persona_id").get("promotion_grounding_status", pd.Series(dtype=str)).astype(str).to_dict()
    example_ids = set(persona_examples_df.get("persona_id", pd.Series(dtype=str)).astype(str).tolist()) if not persona_examples_df.empty else set()
    with_examples = [persona_id for persona_id in promoted_ids if persona_id in example_ids]
    missing = [persona_id for persona_id in promoted_ids if persona_id not in example_ids]
    weak_ids = [persona_id for persona_id in promoted_ids if grounding_lookup.get(persona_id, "") == "promoted_but_weakly_grounded"]
    counts = {
        "grounded": sum(1 for persona_id in promoted_ids if grounding_lookup.get(persona_id, "") in {"promoted_and_grounded", "grounded_but_structurally_weak"}),
        "weakly_grounded": len(weak_ids),
        "ungrounded": sum(1 for persona_id in promoted_ids if grounding_lookup.get(persona_id, "") == "promoted_but_ungrounded"),
        "weak_ids": weak_ids,
    }
    if not grounding_lookup:
        with_examples = [persona_id for persona_id in promoted_ids if persona_id in example_ids]
        missing = [persona_id for persona_id in promoted_ids if persona_id not in example_ids]
        counts["grounded"] = len(with_examples)
        counts["ungrounded"] = len(missing)
    return len(promoted_ids), len(with_examples), missing, counts


def _small_promoted_count(cluster_stats_df: pd.DataFrame, min_cluster_size: int) -> int:
    """Count promoted personas below the size floor."""
    if cluster_stats_df.empty or "promotion_status" not in cluster_stats_df.columns:
        return 0
    workbook_review_visible = cluster_stats_df.get("workbook_review_visible", pd.Series(dtype=bool))
    if workbook_review_visible.empty:
        promoted = cluster_stats_df[cluster_stats_df["promotion_status"].astype(str).isin({"promoted_persona", "review_visible_persona"})]
    else:
        promoted = cluster_stats_df[workbook_review_visible.fillna(False).astype(bool)]
    sizes = pd.to_numeric(promoted.get("persona_size", pd.Series(dtype=int)), errors="coerce").fillna(0)
    return int((sizes < min_cluster_size).sum())


def _persona_promotion_semantics(cluster_stats_df: pd.DataFrame) -> dict[str, int]:
    """Return explicit promoted-vs-usable persona counts from cluster stats."""
    if cluster_stats_df.empty:
        return {
            "promoted_candidate_persona_count": 0,
            "promotion_visibility_persona_count": 0,
            "headline_persona_count": 0,
            "final_usable_persona_count": 0,
            "deck_ready_persona_count": 0,
        }
    base_status = cluster_stats_df.get("base_promotion_status", pd.Series(dtype=str)).astype(str)
    promotion_status = cluster_stats_df.get("promotion_status", pd.Series(dtype=str)).astype(str)
    workbook_review_visible = cluster_stats_df.get("workbook_review_visible", pd.Series(dtype=bool))
    grounding_status = cluster_stats_df.get("promotion_grounding_status", pd.Series(dtype=str)).astype(str)
    final_usable_series = cluster_stats_df.get("final_usable_persona", pd.Series(dtype=bool))
    promoted_candidate_count = int(base_status.isin({"promoted_candidate_persona", "promoted_persona"}).sum()) if not base_status.empty else int(promotion_status.eq("promoted_persona").sum())
    if workbook_review_visible.empty:
        promotion_visibility_count = int(promotion_status.isin({"promoted_persona", "review_visible_persona"}).sum())
    else:
        promotion_visibility_count = int(workbook_review_visible.fillna(False).astype(bool).sum())
    if final_usable_series.empty:
        final_usable_count = int(grounding_status.eq("promoted_and_grounded").sum())
    else:
        final_usable_count = int(final_usable_series.fillna(False).astype(bool).sum())
    return {
        "promoted_candidate_persona_count": promoted_candidate_count,
        "promotion_visibility_persona_count": promotion_visibility_count,
        "headline_persona_count": final_usable_count,
        "final_usable_persona_count": final_usable_count,
        "deck_ready_persona_count": final_usable_count,
    }


def _selected_example_grounding_issue_count(persona_examples_df: pd.DataFrame) -> int:
    """Count selected example rows whose evidence is weak or otherwise degraded."""
    if persona_examples_df.empty:
        return 0
    selection_strength = persona_examples_df.get("selection_strength", pd.Series(dtype=str)).astype(str)
    grounding_strength = persona_examples_df.get("grounding_strength", pd.Series(dtype=str)).astype(str)
    quality = persona_examples_df.get("quote_quality", pd.Series(dtype=str)).astype(str)
    text_len = pd.to_numeric(persona_examples_df.get("source_text_length", pd.Series(dtype=int)), errors="coerce").fillna(0)
    reasons = persona_examples_df.get("rejection_reason", pd.Series(dtype=str)).fillna("").astype(str)
    bundle_grounded = persona_examples_df.get("bundle_grounded_example", pd.Series(dtype=bool)).fillna(False).astype(bool)
    issue_mask = selection_strength.eq("weak_grounding_fallback") | grounding_strength.eq("weak") | quality.isin({"reject", "borderline"}) | (text_len < 80) | reasons.ne("")
    return int((issue_mask & ~bundle_grounded).sum())


def _escalate_axis_status(axis: dict[str, object], condition: bool, status: str, reason_key: str) -> dict[str, object]:
    """Escalate a quality axis when an additional grounding policy condition is met."""
    if not condition:
        return axis
    updated = dict(axis)
    priority = {STATUS_OK: 0, STATUS_WARN: 1, STATUS_FAIL: 2}
    if priority.get(str(status), 0) > priority.get(str(updated.get("status", STATUS_OK)), 0):
        updated["status"] = status
    reason_keys = list(updated.get("reason_keys", []) or [])
    if reason_key not in reason_keys:
        reason_keys.append(reason_key)
    updated["reason_keys"] = reason_keys
    return updated


def _persona_min_cluster_size(labeled_count: int) -> int:
    """Return promoted cluster floor without importing rendering modules."""
    return max(5, int(__import__("math").ceil(float(labeled_count) * 0.05)))


def _evaluate_axis(
    value: float,
    warn_threshold: float,
    fail_threshold: float,
    high_is_bad: bool,
    warn_reason: str,
    fail_reason: str,
) -> dict[str, object]:
    """Evaluate one quality axis from a numeric value."""
    reason_keys: list[str] = []
    status = STATUS_OK
    if high_is_bad:
        if value >= fail_threshold:
            status = STATUS_FAIL
            reason_keys.append(fail_reason)
        elif value >= warn_threshold:
            status = STATUS_WARN
            reason_keys.append(warn_reason)
    else:
        if value < fail_threshold:
            status = STATUS_FAIL
            reason_keys.append(fail_reason)
        elif value < warn_threshold:
            status = STATUS_WARN
            reason_keys.append(warn_reason)
    return {"status": status, "reason_keys": reason_keys}


def _evaluate_axis_from_policy(axis_name: str, metrics: dict[str, object]) -> dict[str, object]:
    """Evaluate one axis using centralized policy metadata."""
    policy = QUALITY_STATUS_POLICY[axis_name]
    value = float(metrics.get(str(policy["metric"]), 0.0) or 0.0)
    result = _evaluate_axis(
        value=value,
        warn_threshold=float(policy["warn_threshold"]),
        fail_threshold=float(policy["fail_threshold"]),
        high_is_bad=bool(policy["high_is_bad"]),
        warn_reason=str(policy["warn_reason"]),
        fail_reason=str(policy["fail_reason"]),
    )
    result["metric"] = str(policy["metric"])
    result["value"] = value
    result["warn_threshold"] = float(policy["warn_threshold"])
    result["fail_threshold"] = float(policy["fail_threshold"])
    result["high_is_bad"] = bool(policy["high_is_bad"])
    result["display_threshold"] = str(policy["display_threshold"])
    return result


def _append_reason_if(axis_result: dict[str, object], condition: bool, reason_key: str) -> dict[str, object]:
    """Append a reason and escalate axis status to WARN if needed."""
    result = {"status": str(axis_result.get("status", STATUS_OK)), "reason_keys": list(axis_result.get("reason_keys", []) or [])}
    if not condition:
        return result
    if reason_key not in result["reason_keys"]:
        result["reason_keys"].append(reason_key)
    if result["status"] == STATUS_OK:
        result["status"] = STATUS_WARN
    return result


def _compose_axis_group(axis_names: list[str], axes: dict[str, dict[str, object]]) -> dict[str, object]:
    """Compose a grouped status from multiple axis results."""
    statuses = [str(axes.get(axis_name, {}).get("status", STATUS_OK)) for axis_name in axis_names]
    reason_keys: list[str] = []
    for axis_name in axis_names:
        reason_keys.extend(list(axes.get(axis_name, {}).get("reason_keys", []) or []))
    return {"status": _compose_status(statuses), "reason_keys": sorted(set(reason_keys))}


def _collect_reason_keys(axes: dict[str, dict[str, object]]) -> list[str]:
    """Collect unique reason keys across all axes."""
    reason_keys: list[str] = []
    for payload in axes.values():
        reason_keys.extend(list(payload.get("reason_keys", []) or []))
    return sorted(set(reason_keys))


def _compose_status(statuses: list[str]) -> str:
    """Compose one status from many axis statuses."""
    if any(status == STATUS_FAIL for status in statuses):
        return STATUS_FAIL
    if any(status == STATUS_WARN for status in statuses):
        return STATUS_WARN
    return STATUS_OK


def _quality_flag_from_status(status: str) -> str:
    """Map composite status to workbook quality flag."""
    if status == STATUS_FAIL:
        return QUALITY_FLAG_UNSTABLE
    if status == STATUS_WARN:
        return QUALITY_FLAG_EXPLORATORY
    return QUALITY_FLAG_OK
