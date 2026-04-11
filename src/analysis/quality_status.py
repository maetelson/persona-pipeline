"""Centralized quality metrics and status policy for workbook generation."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.utils.pipeline_schema import (
    CLUSTER_DOMINANCE_SHARE_PCT,
    CORE_LABEL_COLUMNS,
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
        "metric": "effective_labeled_source_count",
        "warn_threshold": 4.0,
        "fail_threshold": 4.0,
        "high_is_bad": False,
        "warn_reason": "effective_source_diversity_low",
        "fail_reason": "effective_source_diversity_low",
        "display_threshold": "fail<4.0",
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
    "largest_cluster_dominance": {
        "metric": "largest_cluster_share_of_core_labeled",
        "warn_threshold": 55.0,
        "fail_threshold": 70.0,
        "high_is_bad": True,
        "warn_reason": "largest_cluster_too_dominant_warn",
        "fail_reason": "largest_cluster_too_dominant",
        "display_threshold": "warn>=55.0; fail>=70.0",
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


def build_quality_metrics(
    total_raw_count: int,
    cleaned_count: int,
    labeled_df: pd.DataFrame,
    source_stage_counts_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
    cluster_profiles: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """Compute raw quality metrics only, without applying status policy."""
    cluster_profiles = cluster_profiles or []
    core_labeled_df = _persona_core_subset(labeled_df)
    labeled_count = int(len(labeled_df))
    persona_core_labeled_count = int(len(core_labeled_df))
    labeled_sources = int((source_stage_counts_df.get("labeled_episode_count", pd.Series(dtype=int)) > 0).sum()) if not source_stage_counts_df.empty else 0
    raw_sources = int((source_stage_counts_df.get("raw_record_count", pd.Series(dtype=int)) > 0).sum()) if not source_stage_counts_df.empty else 0
    effective_labeled_source_count = round(float(_effective_labeled_source_count(source_stage_counts_df)), 2)
    largest_cluster_share = _largest_cluster_share(cluster_stats_df)
    largest_labeled_source_share = _largest_labeled_source_share(source_stage_counts_df, labeled_count)
    promoted_persona_count, promoted_with_examples, promoted_missing_examples, grounding_counts = _promoted_persona_example_counts(cluster_stats_df, persona_examples_df)
    promoted_persona_example_coverage_pct = round_pct(promoted_with_examples, promoted_persona_count) if promoted_persona_count else 100.0
    min_cluster_size = _persona_min_cluster_size(labeled_count)
    cluster_distribution = [
        {
            "cluster_id": str(row.get("cluster_id", "")),
            "size": int(row.get("size", 0)),
            "share_of_core_labeled": float(row.get("share_of_total", 0.0)),
        }
        for row in cluster_profiles
    ]
    return {
        "total_raw_count": int(total_raw_count),
        "cleaned_count": int(cleaned_count),
        "labeled_count": labeled_count,
        "persona_core_labeled_count": persona_core_labeled_count,
        "persona_core_labeled_records": persona_core_labeled_count,
        "persona_core_unknown_ratio": round(_row_unknown_ratio(core_labeled_df), 6),
        "overall_unknown_ratio": round(_row_unknown_ratio(labeled_df), 6),
        "persona_core_coverage_of_all_labeled_pct": round_pct(persona_core_labeled_count, labeled_count) if labeled_count else 0.0,
        "cluster_count": int(len(cluster_profiles)),
        "cluster_distribution": cluster_distribution,
        "labeled_source_count": labeled_sources,
        "effective_labeled_source_count": effective_labeled_source_count,
        "raw_source_count": raw_sources,
        "min_cluster_size": min_cluster_size,
        "largest_cluster_share_of_core_labeled": largest_cluster_share,
        "largest_labeled_source_share_pct": largest_labeled_source_share,
        "single_cluster_dominance": largest_cluster_share > CLUSTER_DOMINANCE_SHARE_PCT,
        "small_promoted_persona_count": _small_promoted_count(cluster_stats_df, min_cluster_size),
        "example_grounding_failure_count": _example_failure_count(persona_examples_df),
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
    axes["grounding_coverage"] = _append_reason_if(
        axes["grounding_coverage"],
        int(metrics.get("example_grounding_failure_count", 0) or 0) > 0,
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
    grouped = {
        "core_clustering": _compose_axis_group(["core_unknown", "core_coverage", "largest_cluster_dominance"], axes),
        "source_diversity": _compose_axis_group(["effective_source_diversity", "source_concentration"], axes),
        "example_grounding": _compose_axis_group(["grounding_coverage"], axes),
    }
    composite_reason_keys = _collect_reason_keys(axes)
    composite_status = _compose_status([axis["status"] for axis in axes.values()])
    result = {
        "metrics": dict(metrics),
        "axes": axes,
        "groups": grouped,
        "composite_status": composite_status,
        "composite_reason_keys": composite_reason_keys,
        "quality_flag": _quality_flag_from_status(composite_status),
        "quality_flag_rule": "UNSTABLE if any axis status is FAIL; EXPLORATORY if no FAIL and any axis status is WARN; otherwise OK.",
    }
    return result


def flatten_quality_status_result(evaluated: dict[str, object]) -> dict[str, object]:
    """Flatten evaluated quality result for workbook rendering without changing policy."""
    metrics = dict(evaluated.get("metrics", {}) or {})
    axes = dict(evaluated.get("axes", {}) or {})
    groups = dict(evaluated.get("groups", {}) or {})
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
    return result


def quality_display_thresholds() -> dict[str, str]:
    """Return workbook display thresholds derived from the centralized policy."""
    return {
        "persona_core_unknown_ratio": str(QUALITY_STATUS_POLICY["core_unknown"]["display_threshold"]),
        "overall_unknown_ratio": str(QUALITY_STATUS_POLICY["overall_unknown"]["display_threshold"]),
        "persona_core_coverage_of_all_labeled_pct": str(QUALITY_STATUS_POLICY["core_coverage"]["display_threshold"]),
        "effective_labeled_source_count": str(QUALITY_STATUS_POLICY["effective_source_diversity"]["display_threshold"]),
        "largest_labeled_source_share_pct": str(QUALITY_STATUS_POLICY["source_concentration"]["display_threshold"]),
        "largest_cluster_share_of_core_labeled": str(QUALITY_STATUS_POLICY["largest_cluster_dominance"]["display_threshold"]),
        "promoted_persona_example_coverage_pct": str(QUALITY_STATUS_POLICY["grounding_coverage"]["display_threshold"]),
        "overall_status": "FAIL if any axis FAIL; WARN if no FAIL and any axis WARN; otherwise OK.",
        "quality_flag": "UNSTABLE if overall_status=FAIL; EXPLORATORY if overall_status=WARN; otherwise OK.",
    }


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
    promoted = cluster_stats_df[cluster_stats_df["promotion_status"].astype(str).eq("promoted_persona")].copy()
    promoted_ids = promoted.get("persona_id", pd.Series(dtype=str)).astype(str).tolist()
    if not promoted_ids:
        return 0, 0, [], {"grounded": 0, "weakly_grounded": 0, "ungrounded": 0, "weak_ids": []}
    grounding_lookup = promoted.set_index("persona_id").get("promotion_grounding_status", pd.Series(dtype=str)).astype(str).to_dict()
    grounded_statuses = {"promoted_and_grounded", "promoted_but_weakly_grounded"}
    with_examples = [persona_id for persona_id in promoted_ids if grounding_lookup.get(persona_id, "") in grounded_statuses]
    missing = [persona_id for persona_id in promoted_ids if grounding_lookup.get(persona_id, "") not in grounded_statuses]
    weak_ids = [persona_id for persona_id in promoted_ids if grounding_lookup.get(persona_id, "") == "promoted_but_weakly_grounded"]
    counts = {
        "grounded": sum(1 for persona_id in promoted_ids if grounding_lookup.get(persona_id, "") == "promoted_and_grounded"),
        "weakly_grounded": len(weak_ids),
        "ungrounded": sum(1 for persona_id in promoted_ids if grounding_lookup.get(persona_id, "") == "promoted_but_ungrounded"),
        "weak_ids": weak_ids,
    }
    if not grounding_lookup:
        example_ids = set(persona_examples_df.get("persona_id", pd.Series(dtype=str)).astype(str).tolist()) if not persona_examples_df.empty else set()
        with_examples = [persona_id for persona_id in promoted_ids if persona_id in example_ids]
        missing = [persona_id for persona_id in promoted_ids if persona_id not in example_ids]
        counts["grounded"] = len(with_examples)
        counts["ungrounded"] = len(missing)
    return len(promoted_ids), len(with_examples), missing, counts


def _small_promoted_count(cluster_stats_df: pd.DataFrame, min_cluster_size: int) -> int:
    """Count promoted personas below the size floor."""
    if cluster_stats_df.empty or "promotion_status" not in cluster_stats_df.columns:
        return 0
    promoted = cluster_stats_df[cluster_stats_df["promotion_status"].astype(str).eq("promoted_persona")]
    sizes = pd.to_numeric(promoted.get("persona_size", pd.Series(dtype=int)), errors="coerce").fillna(0)
    return int((sizes < min_cluster_size).sum())


def _example_failure_count(persona_examples_df: pd.DataFrame) -> int:
    """Count selected examples with weak grounding evidence."""
    if persona_examples_df.empty:
        return 0
    selection_strength = persona_examples_df.get("selection_strength", pd.Series(dtype=str)).astype(str)
    grounding_strength = persona_examples_df.get("grounding_strength", pd.Series(dtype=str)).astype(str)
    quality = persona_examples_df.get("quote_quality", pd.Series(dtype=str)).astype(str)
    text_len = pd.to_numeric(persona_examples_df.get("source_text_length", pd.Series(dtype=int)), errors="coerce").fillna(0)
    reasons = persona_examples_df.get("rejection_reason", pd.Series(dtype=str)).fillna("").astype(str)
    return int((selection_strength.eq("weak_grounding_fallback") | grounding_strength.eq("weak") | quality.isin({"reject", "borderline"}) | (text_len < 80) | reasons.ne("")).sum())


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
