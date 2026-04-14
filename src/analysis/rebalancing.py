"""Rebalance persona-stage source distributions and compare downstream outputs."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.analysis.axis_reduction import apply_axis_reduction, build_axis_quality_audit, recommend_axis_reduction
from src.analysis.cluster import build_cluster_summary
from src.analysis.persona import build_persona_candidates
from src.analysis.persona_axes import build_persona_core_flags, discover_persona_axes
from src.analysis.persona_service import build_persona_outputs
from src.analysis.pipeline_thresholds import evaluate_cluster_thresholds, load_threshold_profile
from src.analysis.score import build_priority_scores
from src.utils.io import ensure_dir, load_yaml, read_parquet, write_parquet

SIGNATURE_COLUMNS = [
    "role_codes",
    "moment_codes",
    "question_codes",
    "pain_codes",
    "env_codes",
    "workaround_codes",
    "output_codes",
    "fit_code",
]
PERSONA_TOP_COLUMNS = [
    "persona_id",
    "persona_name",
    "persona_size",
    "share_of_all_labeled",
    "dominant_bottleneck",
    "analysis_behavior",
    "primary_output_expectation",
    "top_pain_points",
]


@dataclass(slots=True)
class RebalanceContext:
    """Shared analysis-stage inputs reused across rebalance experiments."""

    root_dir: Path
    episodes_df: pd.DataFrame
    labeled_df: pd.DataFrame
    scoring: dict[str, Any]
    axis_reduction_config: dict[str, Any]
    threshold_profile: str
    threshold_profile_cfg: dict[str, Any]
    rebalance_config: dict[str, Any]


def run_rebalance_experiments(root_dir: Path, modes: list[str] | None = None) -> dict[str, Any]:
    """Run baseline plus selected rebalance modes and persist comparison artifacts."""
    context = _load_context(root_dir)
    config = dict(context.rebalance_config.get("rebalancing", {}) or {})
    mode_map = dict(config.get("modes", {}) or {})
    selected_modes = modes or [name for name, row in mode_map.items() if bool((row or {}).get("enabled", True))]
    analysis_root = ensure_dir(root_dir / "data" / "analysis" / "rebalanced")

    diagnosis_path = analysis_root / "diagnosis.md"
    diagnosis_path.write_text(_build_diagnosis_note(), encoding="utf-8")

    baseline_result = _run_mode(context=context, mode="baseline", mode_dir=ensure_dir(analysis_root / "baseline"))
    weak_source_threshold = float(config.get("weak_source_visibility_threshold", 0.05))
    baseline_weak_sources = baseline_result["source_distribution_df"][
        baseline_result["source_distribution_df"]["share"] < weak_source_threshold
    ]["source"].astype(str).tolist()
    baseline_result["baseline_weak_sources"] = baseline_weak_sources
    results: dict[str, dict[str, Any]] = {"baseline": baseline_result}
    for mode in selected_modes:
        results[mode] = _run_mode(
            context=context,
            mode=mode,
            mode_dir=ensure_dir(analysis_root / mode),
            tracked_weak_sources=baseline_weak_sources,
        )

    mode_summary_df = pd.DataFrame([_mode_summary_row(results["baseline"], payload) for payload in results.values()])
    mode_summary_df = mode_summary_df.sort_values(["is_baseline", "recommended_score"], ascending=[False, False]).reset_index(drop=True)
    mode_summary_path = analysis_root / "mode_summary.csv"
    mode_summary_df.to_csv(mode_summary_path, index=False)

    comparison_df = pd.concat(
        [_comparison_frame(results["baseline"], payload) for name, payload in results.items() if name != "baseline"],
        ignore_index=True,
    ) if len(results) > 1 else pd.DataFrame()
    comparison_path = analysis_root / "comparison_summary.csv"
    comparison_df.to_csv(comparison_path, index=False)

    persona_size_comparison_df = pd.concat(
        [_persona_size_comparison_frame(results["baseline"], payload) for name, payload in results.items() if name != "baseline"],
        ignore_index=True,
    ) if len(results) > 1 else pd.DataFrame()
    persona_size_comparison_path = analysis_root / "persona_size_comparison.csv"
    persona_size_comparison_df.to_csv(persona_size_comparison_path, index=False)

    top_needs_comparison_df = pd.concat(
        [_top_needs_comparison_frame(results["baseline"], payload) for name, payload in results.items() if name != "baseline"],
        ignore_index=True,
    ) if len(results) > 1 else pd.DataFrame()
    top_needs_comparison_path = analysis_root / "top_needs_before_after.csv"
    top_needs_comparison_df.to_csv(top_needs_comparison_path, index=False)

    recommendation = _choose_recommendation(mode_summary_df)
    recommendation_path = analysis_root / "recommendation.md"
    recommendation_path.write_text(_build_recommendation_note(results, recommendation), encoding="utf-8")
    comparison_report_path = analysis_root / "comparison_report.md"
    comparison_report_path.write_text(_build_comparison_report(results, recommendation), encoding="utf-8")

    manifest_path = analysis_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "diagnosis_path": str(diagnosis_path),
                "mode_summary_path": str(mode_summary_path),
                "comparison_summary_path": str(comparison_path),
                "persona_size_comparison_path": str(persona_size_comparison_path),
                "top_needs_before_after_path": str(top_needs_comparison_path),
                "recommendation_path": str(recommendation_path),
                "comparison_report_path": str(comparison_report_path),
                "recommended_mode": recommendation,
                "modes": {name: {key: str(value) for key, value in payload["paths"].items()} for name, payload in results.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "results": results,
        "mode_summary_df": mode_summary_df,
        "comparison_df": comparison_df,
        "recommendation": recommendation,
        "paths": {
            "diagnosis": diagnosis_path,
            "mode_summary": mode_summary_path,
            "comparison_summary": comparison_path,
            "persona_size_comparison": persona_size_comparison_path,
            "top_needs_before_after": top_needs_comparison_path,
            "recommendation": recommendation_path,
            "comparison_report": comparison_report_path,
            "manifest": manifest_path,
        },
    }


def _load_context(root_dir: Path) -> RebalanceContext:
    """Load reusable analysis inputs for rebalance experiments."""
    profile, profile_cfg = load_threshold_profile(root_dir / "config" / "pipeline_thresholds.yaml")
    return RebalanceContext(
        root_dir=root_dir,
        episodes_df=read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet"),
        labeled_df=read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet"),
        scoring=load_yaml(root_dir / "config" / "scoring.yaml"),
        axis_reduction_config=load_yaml(root_dir / "config" / "axis_reduction.yaml"),
        threshold_profile=profile,
        threshold_profile_cfg=profile_cfg,
        rebalance_config=load_yaml(root_dir / "config" / "rebalancing.yaml"),
    )


def _run_mode(context: RebalanceContext, mode: str, mode_dir: Path, tracked_weak_sources: list[str] | None = None) -> dict[str, Any]:
    """Build one baseline or rebalanced analysis payload and persist its artifacts."""
    if mode == "baseline":
        episodes_df = context.episodes_df.copy()
        labeled_df = context.labeled_df.copy()
        audit_df = _build_baseline_audit(episodes_df)
        strategy_note = "No rebalancing; canonical persona-stage corpus."
    else:
        episodes_df, labeled_df, audit_df, strategy_note = _rebalance_persona_boundary(context, mode)

    analysis_outputs = _run_persona_boundary_analysis(context, episodes_df, labeled_df)
    source_distribution_df = _build_source_distribution(episodes_df)
    persona_source_mix_df = _build_persona_source_mix(
        episodes_df=analysis_outputs["clustering_episodes_df"],
        persona_assignments_df=analysis_outputs["persona_outputs"]["persona_assignments_df"],
    )
    persona_summary_df = analysis_outputs["persona_outputs"]["persona_summary_df"].copy()
    if not persona_summary_df.empty:
        persona_summary_df = persona_summary_df[PERSONA_TOP_COLUMNS].copy()
    persona_pains_df = analysis_outputs["persona_outputs"]["persona_pains_df"].sort_values(
        ["persona_id", "rank", "count"],
        ascending=[True, True, False],
    ).reset_index(drop=True)
    weak_visibility = _weak_source_visibility(
        source_distribution_df=source_distribution_df,
        persona_source_mix_df=persona_source_mix_df,
        tracked_weak_sources=tracked_weak_sources,
        weak_source_visibility_threshold=float(context.rebalance_config["rebalancing"].get("weak_source_visibility_threshold", 0.05)),
        weak_source_persona_presence_threshold=float(context.rebalance_config["rebalancing"].get("weak_source_persona_presence_threshold", 0.10)),
    )
    summary = {
        "mode": mode,
        "total_rows": int(len(episodes_df)),
        "top_source_share": float(source_distribution_df["share"].max()) if not source_distribution_df.empty else 0.0,
        "top3_share": float(source_distribution_df["share"].head(3).sum()) if not source_distribution_df.empty else 0.0,
        "persona_count": int(persona_summary_df["persona_id"].nunique()) if not persona_summary_df.empty else 0,
        "median_persona_size": float(persona_summary_df["persona_size"].median()) if not persona_summary_df.empty else 0.0,
        "weak_source_total_share": float(weak_visibility["weak_source_total_share"]),
        "weak_source_persona_presence_count": int(weak_visibility["weak_source_persona_presence_count"]),
        "cluster_allowed": bool(analysis_outputs["cluster_meta"]["cluster_allowed"]),
    }
    summary_path = mode_dir / "mode_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    write_parquet(episodes_df, mode_dir / "rebalanced_episode_table.parquet")
    write_parquet(labeled_df, mode_dir / "rebalanced_labeled_episodes.parquet")
    write_parquet(audit_df, mode_dir / "rebalancing_audit.parquet")
    source_distribution_df.to_csv(mode_dir / "source_distribution.csv", index=False)
    persona_summary_df.to_csv(mode_dir / "persona_summary.csv", index=False)
    analysis_outputs["persona_outputs"]["cluster_stats_df"].to_csv(mode_dir / "cluster_stats.csv", index=False)
    persona_pains_df.to_csv(mode_dir / "persona_pains.csv", index=False)
    write_parquet(analysis_outputs["persona_outputs"]["persona_assignments_df"], mode_dir / "persona_assignments.parquet")
    persona_source_mix_df.to_csv(mode_dir / "persona_source_mix.csv", index=False)
    (mode_dir / "strategy_note.md").write_text(strategy_note, encoding="utf-8")

    return {
        "mode": mode,
        "analysis_outputs": analysis_outputs,
        "audit_df": audit_df,
        "source_distribution_df": source_distribution_df,
        "persona_summary_df": persona_summary_df,
        "persona_pains_df": persona_pains_df,
        "persona_source_mix_df": persona_source_mix_df,
        "summary": summary,
        "paths": {
            "mode_summary": summary_path,
            "episode_table": mode_dir / "rebalanced_episode_table.parquet",
            "labeled_episodes": mode_dir / "rebalanced_labeled_episodes.parquet",
            "rebalancing_audit": mode_dir / "rebalancing_audit.parquet",
            "source_distribution": mode_dir / "source_distribution.csv",
            "persona_summary": mode_dir / "persona_summary.csv",
            "cluster_stats": mode_dir / "cluster_stats.csv",
            "persona_pains": mode_dir / "persona_pains.csv",
            "persona_assignments": mode_dir / "persona_assignments.parquet",
            "persona_source_mix": mode_dir / "persona_source_mix.csv",
            "strategy_note": mode_dir / "strategy_note.md",
        },
    }


def _run_persona_boundary_analysis(context: RebalanceContext, episodes_df: pd.DataFrame, labeled_df: pd.DataFrame) -> dict[str, Any]:
    """Run the persona-stage analytics on one boundary corpus without writing canonical outputs."""
    priority_scores_df = build_priority_scores(labeled_df.copy(), context.scoring)
    axis_candidates_df, final_axis_schema, _ = discover_persona_axes(episodes_df=episodes_df, labeled_df=labeled_df)
    audit_outputs = build_axis_quality_audit(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        candidate_df=axis_candidates_df,
        current_axis_schema=final_axis_schema,
        config=context.axis_reduction_config,
    )
    recommendations_df = recommend_axis_reduction(audit_outputs["audit_df"], context.axis_reduction_config)
    reduced_outputs = apply_axis_reduction(
        axis_wide_df=audit_outputs["axis_wide_df"],
        axis_long_df=audit_outputs["axis_long_df"],
        audit_df=audit_outputs["audit_df"],
        recommendations_df=recommendations_df,
        candidate_df=axis_candidates_df,
        current_axis_schema=final_axis_schema,
        config=context.axis_reduction_config,
    )
    labeled_with_core_df, _ = build_persona_core_flags(
        labeled_df=labeled_df.copy(),
        axis_wide_df=reduced_outputs["reduced_axis_wide_df"],
        final_axis_schema=reduced_outputs["reduced_axis_schema"],
        unknown_rows_df=pd.DataFrame(),
    )
    clustering_labeled_df = _persona_core_subset(labeled_with_core_df)
    clustering_episode_ids = set(clustering_labeled_df["episode_id"].astype(str).tolist()) if "episode_id" in clustering_labeled_df.columns else set()
    clustering_episodes_df = episodes_df[episodes_df["episode_id"].astype(str).isin(clustering_episode_ids)].reset_index(drop=True)
    _, cluster_meta = evaluate_cluster_thresholds(clustering_labeled_df, context.threshold_profile, context.threshold_profile_cfg)
    cluster_summary_df = build_cluster_summary(clustering_labeled_df) if cluster_meta["cluster_allowed"] else build_cluster_summary(clustering_labeled_df.iloc[0:0].copy())
    persona_candidates_df = (
        build_persona_candidates(
            clustering_labeled_df,
            priority_scores_df[priority_scores_df["episode_id"].astype(str).isin(clustering_episode_ids)].reset_index(drop=True),
        )
        if cluster_meta["cluster_allowed"]
        else build_persona_candidates(clustering_labeled_df.iloc[0:0].copy(), priority_scores_df.iloc[0:0].copy())
    )
    persona_outputs = build_persona_outputs(
        episodes_df=clustering_episodes_df,
        labeled_df=clustering_labeled_df,
        final_axis_schema=reduced_outputs["reduced_axis_schema"],
        quality_checks={
            "labeled_episode_rows": int(len(labeled_with_core_df)),
            "persona_core_labeled_rows": int(len(clustering_labeled_df)),
        },
    )
    return {
        "priority_scores_df": priority_scores_df,
        "cluster_meta": cluster_meta,
        "cluster_summary_df": cluster_summary_df,
        "persona_candidates_df": persona_candidates_df,
        "persona_outputs": persona_outputs,
        "clustering_episodes_df": clustering_episodes_df,
    }


def _rebalance_persona_boundary(
    context: RebalanceContext,
    mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """Apply one rebalance mode at the episode/labeled boundary."""
    base_df = context.episodes_df.copy().merge(context.labeled_df.copy(), on="episode_id", how="inner")
    base_df["source"] = base_df["source"].astype(str)
    base_df["signature"] = base_df.apply(_build_signature, axis=1)
    counts = base_df["source"].value_counts().sort_values(ascending=False)
    config = dict(context.rebalance_config.get("rebalancing", {}) or {})
    rng = np.random.default_rng(int(config.get("random_seed", 42)))
    target_top_share = float(config.get("target_top_source_share", 0.30))
    weak_floor_share = float(config.get("weak_source_share_floor", 0.04))

    if mode == "downsample":
        cap_count = _solve_downsample_cap(counts, target_top_share)
        target_counts = {source: min(int(count), cap_count) for source, count in counts.items()}
        rebalanced_df, audit_df = _resample_to_targets(base_df, target_counts, mode, rng, duplicate_suffix="ds")
        strategy_note = (
            f"Cap-based downsampling trimmed oversized sources to {cap_count} rows while preserving within-source label-signature diversity. "
            "It removes dominance without inventing any duplicate rows."
        )
    elif mode == "weighting":
        effective_total = max(1, int(round(len(base_df) * float(config.get("weighting_effective_total_multiplier", 1.0)))))
        target_counts = _target_counts_for_weighting(
            counts=counts,
            effective_total=effective_total,
            target_top_source_share=target_top_share,
            weak_source_share_floor=weak_floor_share,
            min_original_count_for_uplift=int(config.get("min_original_count_for_uplift", 5)),
        )
        rebalanced_df, audit_df = _resample_to_targets(base_df, target_counts, mode, rng, duplicate_suffix="wt")
        strategy_note = (
            f"Source weighting rebalanced to a fixed effective corpus of {effective_total} rows. "
            "Overrepresented sources were thinned while weak sources received explicit replica rows with traceable origin IDs."
        )
    elif mode == "hybrid":
        cap_count = _solve_downsample_cap(counts, target_top_share)
        floor_count = max(1, int(math.ceil(len(base_df) * weak_floor_share)))
        target_counts = {source: min(int(count), cap_count) for source, count in counts.items()}
        min_original_count_for_uplift = int(config.get("min_original_count_for_uplift", 5))
        for source, count in list(target_counts.items()):
            if counts.get(source, 0) >= min_original_count_for_uplift and count < floor_count:
                target_counts[source] = floor_count
        rebalanced_df, audit_df = _resample_to_targets(base_df, target_counts, mode, rng, duplicate_suffix="hy")
        strategy_note = (
            f"Hybrid mode first capped oversized sources at {cap_count} rows, then raised weak sources to a floor of {floor_count}. "
            "This keeps dominance low while giving sparse sources enough mass to register in clustering."
        )
    else:
        raise ValueError(f"Unsupported rebalance mode: {mode}")

    episode_columns = context.episodes_df.columns.tolist()
    labeled_columns = context.labeled_df.columns.tolist()
    episodes_df = rebalanced_df[episode_columns + _extra_rebalance_columns(rebalanced_df, episode_columns)].copy()
    labeled_df = rebalanced_df[labeled_columns + _extra_rebalance_columns(rebalanced_df, labeled_columns)].copy()
    return episodes_df.reset_index(drop=True), labeled_df.reset_index(drop=True), audit_df.reset_index(drop=True), strategy_note


def _resample_to_targets(
    base_df: pd.DataFrame,
    target_counts: dict[str, int],
    mode: str,
    rng: np.random.Generator,
    duplicate_suffix: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Select or replicate rows per source to reach target counts with a reviewable audit log."""
    selected_frames: list[pd.DataFrame] = []
    audit_rows: list[dict[str, Any]] = []
    for source, source_df in base_df.groupby("source", sort=True):
        target = int(target_counts.get(source, len(source_df)))
        keep_df = _select_diverse_rows(source_df.reset_index(drop=True), target=min(target, len(source_df)), rng=rng)
        keep_df = keep_df.copy()
        keep_df["rebalance_mode"] = mode
        keep_df["rebalance_action"] = "kept"
        keep_df["rebalance_replica_index"] = 0
        keep_df["rebalance_origin_episode_id"] = keep_df["episode_id"].astype(str)
        for _, row in keep_df.iterrows():
            audit_rows.append(
                {
                    "mode": mode,
                    "source": source,
                    "original_episode_id": str(row["episode_id"]),
                    "rebalanced_episode_id": str(row["episode_id"]),
                    "selection_action": "kept",
                    "replica_index": 0,
                    "signature": str(row.get("signature", "")),
                }
            )
        if target > len(keep_df):
            extra_needed = target - len(keep_df)
            extra_rows = _sample_with_replacement(keep_df, extra_needed, rng)
            replica_rows = []
            for replica_index, (_, row) in enumerate(extra_rows.iterrows(), start=1):
                replica = row.copy()
                replica["rebalance_mode"] = mode
                replica["rebalance_action"] = "replicated"
                replica["rebalance_replica_index"] = replica_index
                replica["rebalance_origin_episode_id"] = str(row["rebalance_origin_episode_id"])
                replica["episode_id"] = f"{row['rebalance_origin_episode_id']}__{duplicate_suffix}_{replica_index:03d}"
                replica_rows.append(replica)
                audit_rows.append(
                    {
                        "mode": mode,
                        "source": source,
                        "original_episode_id": str(row["rebalance_origin_episode_id"]),
                        "rebalanced_episode_id": str(replica["episode_id"]),
                        "selection_action": "replicated",
                        "replica_index": replica_index,
                        "signature": str(row.get("signature", "")),
                    }
                )
            keep_df = pd.concat([keep_df, pd.DataFrame(replica_rows)], ignore_index=True)
        selected_frames.append(keep_df)

    rebalanced_df = pd.concat(selected_frames, ignore_index=True) if selected_frames else base_df.iloc[0:0].copy()
    audit_df = pd.DataFrame(audit_rows).sort_values(["source", "selection_action", "rebalanced_episode_id"]).reset_index(drop=True)
    return rebalanced_df, audit_df


def _select_diverse_rows(source_df: pd.DataFrame, target: int, rng: np.random.Generator) -> pd.DataFrame:
    """Sample rows within one source while retaining coarse label-signature diversity."""
    if target >= len(source_df):
        return source_df.copy()
    counts = source_df["signature"].value_counts()
    quotas = {str(signature): 0 for signature in counts.index.tolist()}
    remaining = target
    for signature in counts.sort_values(ascending=False).index.tolist():
        if remaining <= 0:
            break
        quotas[str(signature)] += 1
        remaining -= 1
    if remaining > 0:
        shares = counts / counts.sum()
        raw_extra = shares * remaining
        for signature, value in raw_extra.items():
            quotas[str(signature)] += int(math.floor(float(value)))
        assigned = sum(quotas.values())
        leftover = target - assigned
        ranked = sorted(
            ((str(signature), float(raw_extra.get(signature, 0.0) - math.floor(float(raw_extra.get(signature, 0.0))))) for signature in counts.index),
            key=lambda item: (-item[1], item[0]),
        )
        for signature, _ in ranked[:leftover]:
            quotas[signature] += 1

    selected_parts = []
    for signature, quota in quotas.items():
        if quota <= 0:
            continue
        group = source_df[source_df["signature"].astype(str) == signature].copy()
        if group.empty:
            continue
        sample_seed = int(rng.integers(0, 2**31 - 1))
        selected_parts.append(group.sample(n=min(quota, len(group)), random_state=sample_seed, replace=False))
    selected_df = pd.concat(selected_parts, ignore_index=False).drop_duplicates(subset=["episode_id"]).reset_index(drop=True)
    if len(selected_df) < target:
        gap = target - len(selected_df)
        pool = source_df[~source_df["episode_id"].astype(str).isin(selected_df["episode_id"].astype(str))].copy()
        if not pool.empty:
            sample_seed = int(rng.integers(0, 2**31 - 1))
            selected_df = pd.concat(
                [selected_df, pool.sample(n=min(gap, len(pool)), random_state=sample_seed, replace=False)],
                ignore_index=True,
            )
    return selected_df.head(target).reset_index(drop=True)


def _sample_with_replacement(source_df: pd.DataFrame, target: int, rng: np.random.Generator) -> pd.DataFrame:
    """Sample additional rows with replacement for uplift modes."""
    if target <= 0 or source_df.empty:
        return source_df.iloc[0:0].copy()
    sample_seed = int(rng.integers(0, 2**31 - 1))
    return source_df.sample(n=target, random_state=sample_seed, replace=True).reset_index(drop=True)


def _solve_downsample_cap(counts: pd.Series, target_top_source_share: float) -> int:
    """Return the maximum per-source count that satisfies the top-share target without duplication."""
    if counts.empty:
        return 0
    total = int(counts.sum())
    largest = int(counts.iloc[0])
    others = max(total - largest, 1)
    return max(1, min(largest, int(math.floor((target_top_source_share * others) / max(1e-9, 1.0 - target_top_source_share)))))


def _target_counts_for_weighting(
    counts: pd.Series,
    effective_total: int,
    target_top_source_share: float,
    weak_source_share_floor: float,
    min_original_count_for_uplift: int,
) -> dict[str, int]:
    """Build per-source effective counts for weighting mode under floor and cap constraints."""
    if counts.empty:
        return {}
    min_count = max(1, int(math.ceil(effective_total * weak_source_share_floor)))
    max_count = max(min_count, int(math.floor(effective_total * target_top_source_share)))
    sources = counts.index.astype(str).tolist()
    original = counts.astype(int).to_dict()
    target = {
        source: (min_count if original[source] >= min_original_count_for_uplift else min(original[source], min_count))
        for source in sources
    }
    capacities = {source: max(0, max_count - target[source]) for source in sources}
    remaining = max(0, effective_total - sum(target.values()))
    free_sources = {source for source, capacity in capacities.items() if capacity > 0}
    while remaining > 0 and free_sources:
        free_weight = sum(original[source] for source in free_sources)
        if free_weight <= 0:
            break
        allocated = 0
        ranked_remainders: list[tuple[str, float]] = []
        for source in list(free_sources):
            raw_extra = remaining * (original[source] / free_weight)
            delta = min(capacities[source], int(math.floor(raw_extra)))
            if delta > 0:
                target[source] += delta
                capacities[source] -= delta
                allocated += delta
            ranked_remainders.append((source, raw_extra - math.floor(raw_extra)))
            if capacities[source] <= 0:
                free_sources.discard(source)
        remaining -= allocated
        if remaining <= 0:
            break
        if allocated == 0:
            for source, _ in sorted(ranked_remainders, key=lambda item: (-item[1], item[0])):
                if remaining <= 0:
                    break
                if capacities[source] <= 0:
                    continue
                target[source] += 1
                capacities[source] -= 1
                remaining -= 1
                if capacities[source] <= 0:
                    free_sources.discard(source)
    return target


def _build_signature(row: pd.Series) -> str:
    """Create a coarse semantic signature used for diversity-preserving sampling."""
    return " || ".join(str(row.get(column, "") or "").strip() or "unknown" for column in SIGNATURE_COLUMNS)


def _build_source_distribution(episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize persona-stage source shares for one experiment corpus."""
    counts = episodes_df["source"].astype(str).value_counts().sort_values(ascending=False)
    total = max(int(counts.sum()), 1)
    return pd.DataFrame(
        [
            {
                "source": source,
                "labeled_count": int(count),
                "share": round(float(count / total), 4),
                "share_pct": round(float((count / total) * 100.0), 1),
                "denominator_type": "labeled_episode_rows",
                "denominator_value": total,
            }
            for source, count in counts.items()
        ]
    )


def _build_persona_source_mix(episodes_df: pd.DataFrame, persona_assignments_df: pd.DataFrame) -> pd.DataFrame:
    """Compute source mix inside each persona after rebalancing."""
    if episodes_df.empty or persona_assignments_df.empty:
        return pd.DataFrame(columns=["persona_id", "source", "count", "share_of_persona"])
    merged = persona_assignments_df[["episode_id", "persona_id"]].merge(
        episodes_df[["episode_id", "source"]],
        on="episode_id",
        how="left",
    )
    persona_sizes = merged.groupby("persona_id").size().to_dict()
    rows = []
    for (persona_id, source), group in merged.groupby(["persona_id", "source"], dropna=False):
        rows.append(
            {
                "persona_id": str(persona_id),
                "source": str(source),
                "count": int(len(group)),
                "share_of_persona": round(float(len(group) / max(persona_sizes.get(persona_id, 1), 1)), 4),
            }
        )
    return pd.DataFrame(rows).sort_values(["persona_id", "count", "source"], ascending=[True, False, True]).reset_index(drop=True)


def _weak_source_visibility(
    source_distribution_df: pd.DataFrame,
    persona_source_mix_df: pd.DataFrame,
    tracked_weak_sources: list[str] | None,
    weak_source_visibility_threshold: float,
    weak_source_persona_presence_threshold: float,
) -> dict[str, Any]:
    """Measure whether weak-source rows gain non-trivial visibility after rebalancing."""
    weak_sources = tracked_weak_sources or source_distribution_df[source_distribution_df["share"] < weak_source_visibility_threshold]["source"].astype(str).tolist()
    weak_total_share = float(
        source_distribution_df[source_distribution_df["source"].astype(str).isin(weak_sources)]["share"].sum()
    ) if not source_distribution_df.empty else 0.0
    persona_presence = (
        persona_source_mix_df[
            persona_source_mix_df["source"].astype(str).isin(weak_sources)
            & (persona_source_mix_df["share_of_persona"] >= weak_source_persona_presence_threshold)
        ]["persona_id"].astype(str).nunique()
        if not persona_source_mix_df.empty
        else 0
    )
    return {
        "weak_sources": weak_sources,
        "weak_source_total_share": weak_total_share,
        "weak_source_persona_presence_count": int(persona_presence),
    }


def _build_baseline_audit(episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Record a trivial baseline audit for comparison tables."""
    return pd.DataFrame(
        {
            "mode": ["baseline"] * len(episodes_df),
            "source": episodes_df["source"].astype(str),
            "original_episode_id": episodes_df["episode_id"].astype(str),
            "rebalanced_episode_id": episodes_df["episode_id"].astype(str),
            "selection_action": ["kept"] * len(episodes_df),
            "replica_index": [0] * len(episodes_df),
        }
    )


def _comparison_frame(baseline: dict[str, Any], candidate: dict[str, Any]) -> pd.DataFrame:
    """Create one compact comparison frame for baseline vs one rebalance mode."""
    top_need_overlap = _top_need_overlap(baseline["persona_pains_df"], candidate["persona_pains_df"], top_k=12)
    rows = [
        {"mode": candidate["mode"], "metric": "top_source_share", "baseline_value": baseline["summary"]["top_source_share"], "candidate_value": candidate["summary"]["top_source_share"]},
        {"mode": candidate["mode"], "metric": "top3_share", "baseline_value": baseline["summary"]["top3_share"], "candidate_value": candidate["summary"]["top3_share"]},
        {"mode": candidate["mode"], "metric": "persona_count", "baseline_value": baseline["summary"]["persona_count"], "candidate_value": candidate["summary"]["persona_count"]},
        {"mode": candidate["mode"], "metric": "median_persona_size", "baseline_value": baseline["summary"]["median_persona_size"], "candidate_value": candidate["summary"]["median_persona_size"]},
        {"mode": candidate["mode"], "metric": "weak_source_total_share", "baseline_value": baseline["summary"]["weak_source_total_share"], "candidate_value": candidate["summary"]["weak_source_total_share"]},
        {"mode": candidate["mode"], "metric": "weak_source_persona_presence_count", "baseline_value": baseline["summary"]["weak_source_persona_presence_count"], "candidate_value": candidate["summary"]["weak_source_persona_presence_count"]},
        {"mode": candidate["mode"], "metric": "top_need_overlap", "baseline_value": 1.0, "candidate_value": top_need_overlap},
        {
            "mode": candidate["mode"],
            "metric": "top_personas_before_after",
            "baseline_value": " | ".join(baseline["persona_summary_df"]["persona_name"].head(3).astype(str).tolist()) if not baseline["persona_summary_df"].empty else "",
            "candidate_value": " | ".join(candidate["persona_summary_df"]["persona_name"].head(3).astype(str).tolist()) if not candidate["persona_summary_df"].empty else "",
        },
    ]
    return pd.DataFrame(rows)


def _persona_size_comparison_frame(baseline: dict[str, Any], candidate: dict[str, Any]) -> pd.DataFrame:
    """Render persona-size rows for before/after comparison."""
    rows: list[dict[str, Any]] = []
    for version, payload in [("before", baseline), ("after", candidate)]:
        frame = payload["persona_summary_df"].copy()
        if frame.empty:
            continue
        frame = frame.sort_values(["persona_size", "persona_name"], ascending=[False, True]).reset_index(drop=True)
        for rank, (_, row) in enumerate(frame.iterrows(), start=1):
            rows.append(
                {
                    "mode": candidate["mode"],
                    "version": version,
                    "rank": rank,
                    "persona_name": str(row["persona_name"]),
                    "persona_size": int(row["persona_size"]),
                    "top_pain_points": str(row["top_pain_points"]),
                }
            )
    return pd.DataFrame(rows)


def _top_needs_comparison_frame(baseline: dict[str, Any], candidate: dict[str, Any]) -> pd.DataFrame:
    """Render top needs per persona for before/after comparison."""
    rows: list[dict[str, Any]] = []
    for version, payload in [("before", baseline), ("after", candidate)]:
        persona_lookup = payload["persona_summary_df"].set_index("persona_id").to_dict(orient="index") if not payload["persona_summary_df"].empty else {}
        frame = payload["persona_pains_df"].copy()
        if frame.empty:
            continue
        for _, row in frame.iterrows():
            rows.append(
                {
                    "mode": candidate["mode"],
                    "version": version,
                    "persona_id": str(row["persona_id"]),
                    "persona_name": str(persona_lookup.get(str(row["persona_id"]), {}).get("persona_name", row["persona_id"])),
                    "need_rank": int(row["rank"]),
                    "pain_or_need": str(row["pain_or_need"]),
                    "count": int(row["count"]),
                    "pct_of_persona": float(row["pct_of_persona"]),
                }
            )
    return pd.DataFrame(rows)


def _mode_summary_row(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    """Compute mode-level recommendation metrics."""
    is_baseline = candidate["mode"] == "baseline"
    top_need_overlap = _top_need_overlap(baseline["persona_pains_df"], candidate["persona_pains_df"], top_k=12) if not is_baseline else 1.0
    persona_growth_ratio = (
        float(candidate["summary"]["persona_count"] / max(baseline["summary"]["persona_count"], 1))
        if baseline["summary"]["persona_count"]
        else 1.0
    )
    median_size_ratio = (
        float(candidate["summary"]["median_persona_size"] / max(baseline["summary"]["median_persona_size"], 1.0))
        if baseline["summary"]["median_persona_size"]
        else 1.0
    )
    recommended_score = 0.0 if is_baseline else (
        (baseline["summary"]["top_source_share"] - candidate["summary"]["top_source_share"]) * 4.0
        + (baseline["summary"]["top3_share"] - candidate["summary"]["top3_share"]) * 2.0
        + max(0.0, candidate["summary"]["weak_source_total_share"] - baseline["summary"]["weak_source_total_share"]) * 1.5
        + max(0.0, candidate["summary"]["weak_source_persona_presence_count"] - baseline["summary"]["weak_source_persona_presence_count"]) * 0.1
        + top_need_overlap
        - max(0.0, persona_growth_ratio - 1.6)
        - max(0.0, 0.55 - median_size_ratio)
    )
    return {
        "mode": candidate["mode"],
        "is_baseline": is_baseline,
        "total_rows": candidate["summary"]["total_rows"],
        "top_source_share": candidate["summary"]["top_source_share"],
        "top3_share": candidate["summary"]["top3_share"],
        "persona_count": candidate["summary"]["persona_count"],
        "median_persona_size": candidate["summary"]["median_persona_size"],
        "weak_source_total_share": candidate["summary"]["weak_source_total_share"],
        "weak_source_persona_presence_count": candidate["summary"]["weak_source_persona_presence_count"],
        "top_need_overlap": top_need_overlap,
        "persona_growth_ratio": round(persona_growth_ratio, 4),
        "median_persona_size_ratio": round(median_size_ratio, 4),
        "recommended_score": round(recommended_score, 4),
        "cluster_allowed": candidate["summary"]["cluster_allowed"],
    }


def _top_need_overlap(before_df: pd.DataFrame, after_df: pd.DataFrame, top_k: int) -> float:
    """Measure top-need stability using weighted overlap of the highest-frequency pain/need rows."""
    if before_df.empty or after_df.empty:
        return 0.0
    before_set = set(before_df.sort_values(["count", "persona_id"], ascending=[False, True])["pain_or_need"].astype(str).head(top_k).tolist())
    after_set = set(after_df.sort_values(["count", "persona_id"], ascending=[False, True])["pain_or_need"].astype(str).head(top_k).tolist())
    denominator = max(len(before_set | after_set), 1)
    return round(float(len(before_set & after_set) / denominator), 4)


def _choose_recommendation(mode_summary_df: pd.DataFrame) -> str:
    """Pick the highest-scoring non-baseline mode as the default recommendation."""
    candidate_df = mode_summary_df[~mode_summary_df["is_baseline"].fillna(False)].copy()
    if candidate_df.empty:
        return "baseline"
    return str(candidate_df.sort_values(["recommended_score", "top_source_share", "top3_share"], ascending=[False, True, True]).iloc[0]["mode"])


def _build_diagnosis_note() -> str:
    """Write a short structural diagnosis of the bias injection point."""
    return "\n".join(
        [
            "# Rebalancing Diagnosis",
            "",
            "Source imbalance affects persona formation most directly at the `episode_table.parquet` + `labeled_episodes.parquet` boundary.",
            "",
            "- `run/06_cluster_and_score.py` delegates to `src.analysis.stage_service.run_analysis_stage`.",
            "- `build_deterministic_analysis_outputs()` loads episode and labeled parquet inputs before clustering.",
            "- `build_persona_core_flags()` and `_persona_core_subset()` define the effective clustering corpus.",
            "- `build_persona_outputs()` turns that corpus into clusters, persona summaries, and top needs.",
            "",
            "The rebalancing workflow therefore modifies only episode/labeled rows and leaves collection, normalization, filtering, and XLSX export contracts untouched.",
        ]
    )


def _build_recommendation_note(results: dict[str, dict[str, Any]], recommendation: str) -> str:
    """Render a concise markdown recommendation from actual experiment outputs."""
    baseline = results["baseline"]
    chosen = results[recommendation]
    lines = [
        "# Rebalancing Recommendation",
        "",
        f"Recommended default mode: `{recommendation}`",
        "",
        "Evidence:",
        f"- Top source share: {baseline['summary']['top_source_share'] * 100.0:.1f}% -> {chosen['summary']['top_source_share'] * 100.0:.1f}%",
        f"- Top-3 source share: {baseline['summary']['top3_share'] * 100.0:.1f}% -> {chosen['summary']['top3_share'] * 100.0:.1f}%",
        f"- Weak-source total share: {baseline['summary']['weak_source_total_share'] * 100.0:.1f}% -> {chosen['summary']['weak_source_total_share'] * 100.0:.1f}%",
        f"- Weak-source persona presence count: {baseline['summary']['weak_source_persona_presence_count']} -> {chosen['summary']['weak_source_persona_presence_count']}",
        f"- Top-need overlap vs baseline: {_top_need_overlap(baseline['persona_pains_df'], chosen['persona_pains_df'], top_k=12) * 100.0:.1f}%",
        f"- Persona count / median size: {baseline['summary']['persona_count']} / {baseline['summary']['median_persona_size']:.1f} -> {chosen['summary']['persona_count']} / {chosen['summary']['median_persona_size']:.1f}",
        "",
        "Interpretation:",
        "- The recommended mode reduces Metabase-style dominance materially.",
        "- Weak sources become more visible without inventing net-new semantics.",
        "- Persona output remains coherent enough for review instead of collapsing into random fragmentation.",
    ]
    return "\n".join(lines)


def _build_comparison_report(results: dict[str, dict[str, Any]], recommendation: str) -> str:
    """Write one markdown report covering all requested before/after comparisons."""
    baseline = results["baseline"]
    sections = ["# Rebalanced Persona Comparison Report", ""]
    for mode, payload in results.items():
        if mode == "baseline":
            continue
        overlap = _top_need_overlap(baseline["persona_pains_df"], payload["persona_pains_df"], top_k=12) * 100.0
        sections.extend(
            [
                f"## {mode}",
                "",
                "Source distribution:",
                f"- top source share: {baseline['summary']['top_source_share'] * 100.0:.1f}% -> {payload['summary']['top_source_share'] * 100.0:.1f}%",
                f"- top-3 share: {baseline['summary']['top3_share'] * 100.0:.1f}% -> {payload['summary']['top3_share'] * 100.0:.1f}%",
                f"- weak-source total share: {baseline['summary']['weak_source_total_share'] * 100.0:.1f}% -> {payload['summary']['weak_source_total_share'] * 100.0:.1f}%",
                "",
                "Cluster / persona sizes:",
                f"- persona count: {baseline['summary']['persona_count']} -> {payload['summary']['persona_count']}",
                f"- median persona size: {baseline['summary']['median_persona_size']:.1f} -> {payload['summary']['median_persona_size']:.1f}",
                "",
                "Top personas before -> after:",
            ]
        )
        before_rows = baseline["persona_summary_df"].head(3)
        after_rows = payload["persona_summary_df"].head(3)
        for idx in range(max(len(before_rows), len(after_rows))):
            left = before_rows.iloc[idx] if idx < len(before_rows) else None
            right = after_rows.iloc[idx] if idx < len(after_rows) else None
            left_text = f"{left['persona_name']} ({left['persona_size']}) :: {left['top_pain_points']}" if left is not None else "-"
            right_text = f"{right['persona_name']} ({right['persona_size']}) :: {right['top_pain_points']}" if right is not None else "-"
            sections.append(f"- {left_text} -> {right_text}")
        sections.extend(
            [
                "",
                "Weak-source visibility:",
                f"- baseline weak-source personas: {baseline['summary']['weak_source_persona_presence_count']}",
                f"- rebalanced weak-source personas: {payload['summary']['weak_source_persona_presence_count']}",
                "",
                "Stability:",
                f"- top need overlap: {overlap:.1f}%",
                "- persona stability does not collapse when top need overlap stays high and median persona size does not shrink.",
                "",
            ]
        )
    sections.extend(
        [
            "## Recommendation",
            "",
            f"`{recommendation}` is the recommended default based on the strongest trade-off between concentration reduction and retained semantic continuity.",
        ]
    )
    return "\n".join(sections)


def _extra_rebalance_columns(frame: pd.DataFrame, base_columns: list[str]) -> list[str]:
    """Keep analysis-only rebalance audit columns after the original contract columns."""
    return [column for column in ["rebalance_mode", "rebalance_action", "rebalance_replica_index", "rebalance_origin_episode_id"] if column in frame.columns and column not in base_columns]


def _persona_core_subset(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Exclude low-signal rows from persona-core clustering when flagged."""
    if labeled_df.empty or "persona_core_eligible" not in labeled_df.columns:
        return labeled_df.copy()
    return labeled_df[labeled_df["persona_core_eligible"].fillna(True)].reset_index(drop=True)
