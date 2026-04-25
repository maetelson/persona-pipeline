"""Build and validate a curated evaluation set for reconciliation/signoff separation."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.bottleneck_clustering import (
    build_bottleneck_feature_table,
    assign_bottleneck_clusters,
    merge_overlapping_personas,
)
from src.utils.io import load_yaml, read_parquet

CURATED_LABELS = {
    "reconciliation_signoff_positive",
    "reporting_packager_parent",
    "ambiguous_boundary",
    "noise",
    "hard_negative",
}

BUCKET_TARGETS = {
    "reconciliation_signoff_positive": 50,
    "reporting_packager_parent": 50,
    "ambiguous_boundary": 40,
    "noise": 30,
    "hard_negative": 30,
}

SPLIT_BUCKET_TARGETS = {
    "dev": {
        "reconciliation_signoff_positive": 30,
        "reporting_packager_parent": 30,
        "ambiguous_boundary": 24,
        "noise": 18,
        "hard_negative": 18,
    },
    "eval_locked": {
        "reconciliation_signoff_positive": 20,
        "reporting_packager_parent": 20,
        "ambiguous_boundary": 16,
        "noise": 12,
        "hard_negative": 12,
    },
}

REQUIRED_COLUMNS = [
    "episode_id",
    "source",
    "persona_id_current",
    "normalized_episode",
    "business_question",
    "bottleneck_text",
    "desired_output",
    "pain_codes",
    "question_codes",
    "output_codes",
    "workflow_stage",
    "analysis_goal",
    "bottleneck_type",
    "trust_validation_need",
    "current_cluster_signature",
    "source_url",
    "curated_label",
    "reason",
    "should_move_out_of_persona_01",
    "should_join_persona_04_like",
    "confidence",
]

POSITIVE_TERMS = {
    "mismatch",
    "discrepancy",
    "inconsistent numbers",
    "numbers do not match",
    "numbers don't match",
    "totals do not match",
    "wrong total",
    "missing data",
    "no data",
    "dashboard does not match",
    "report does not match",
    "validate numbers",
    "reconcile",
    "reconciliation",
    "verify report",
    "check before sending",
    "repeated validation",
    "sanity check",
    "audit numbers",
    "signoff",
    "approval",
    "stakeholder",
    "client report",
    "leadership report",
    "explain the difference",
    "cross-team alignment",
    "numbers before sending",
}

METRIC_CONTEXT_TERMS = {
    "metric",
    "kpi",
    "revenue",
    "conversion",
    "dashboard",
    "report",
    "reporting",
    "spreadsheet",
    "export",
    "scorecard",
    "data",
    "numbers",
}

NOISE_TERMS = {
    "newsletter",
    "roundup",
    "community update",
    "vendor marketing",
    "product announcement",
    "release note",
    "governance",
    "docs",
    "documentation",
    "design issue",
    "ai fp&a",
    "thought leadership",
    "hiring",
    "job",
    "career",
    "training",
    "certification",
    "community call",
    "community digest",
}

HARD_NEGATIVE_TERMS = {
    "setup",
    "configuration",
    "contact support",
    "support ticket",
    "customer care",
    "login issue",
    "permission issue",
    "ui bug",
    "feature request",
    "roadmap",
    "docs",
    "documentation",
    "training",
    "certification",
    "career",
    "job",
    "release note",
    "vendor",
    "announcement",
    "install",
    "setup guide",
    "how to connect",
    "authentication",
    "oauth",
    "schema setup",
}


def build_curation_artifacts(root_dir: Path) -> dict[str, Any]:
    """Build the curated evaluation set plus baseline vs simulation summary."""
    bundle = _load_bundle(root_dir)
    curation_df = _build_curated_df(bundle)
    split_frames = split_curation_df(curation_df)
    evaluation_summary = _evaluate_curation(curation_df, split_frames, bundle)
    return {
        "curation_df": curation_df,
        "split_frames": split_frames,
        "evaluation_summary": evaluation_summary,
    }


def validate_curation_df(curation_df: pd.DataFrame) -> list[str]:
    """Return human-readable validation failures for the curated evaluation set."""
    errors: list[str] = []
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in curation_df.columns]
    if missing_columns:
        errors.append(f"missing required columns: {', '.join(missing_columns)}")
        return errors

    invalid_labels = sorted(set(curation_df["curated_label"].astype(str)) - CURATED_LABELS)
    if invalid_labels:
        errors.append(f"invalid curated_label values: {', '.join(invalid_labels)}")

    counts = curation_df["curated_label"].astype(str).value_counts().to_dict()
    for label, minimum in BUCKET_TARGETS.items():
        if int(counts.get(label, 0)) < minimum:
            errors.append(f"{label} has {counts.get(label, 0)} rows; expected at least {minimum}")

    if curation_df["episode_id"].astype(str).duplicated().any():
        dupes = curation_df["episode_id"].astype(str).duplicated().sum()
        errors.append(f"duplicate episode_id rows detected: {dupes}")

    source_counts = curation_df["source"].astype(str).value_counts()
    if source_counts.empty:
        errors.append("source distribution is empty")
    else:
        top_source_share = float(source_counts.iloc[0]) / max(len(curation_df), 1)
        if top_source_share > 0.35:
            errors.append(f"top source dominates curation set: {source_counts.index[0]} share {top_source_share:.2%}")
        if len(source_counts) < 8:
            errors.append(f"source diversity is too low: only {len(source_counts)} unique sources")

    return errors


def validate_curation_splits(split_frames: dict[str, pd.DataFrame]) -> list[str]:
    """Return validation failures for the dev/eval split artifacts."""
    errors: list[str] = []
    expected_names = {"dev", "eval_locked"}
    missing = sorted(expected_names - set(split_frames))
    if missing:
        errors.append(f"missing split frames: {', '.join(missing)}")
        return errors

    for split_name, minimums in SPLIT_BUCKET_TARGETS.items():
        frame = split_frames[split_name]
        missing_columns = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
        if missing_columns:
            errors.append(f"{split_name}: missing required columns: {', '.join(missing_columns)}")
            continue
        invalid_labels = sorted(set(frame["curated_label"].astype(str)) - CURATED_LABELS)
        if invalid_labels:
            errors.append(f"{split_name}: invalid curated_label values: {', '.join(invalid_labels)}")
        if frame["episode_id"].astype(str).duplicated().any():
            dupes = int(frame["episode_id"].astype(str).duplicated().sum())
            errors.append(f"{split_name}: duplicate episode_id rows detected: {dupes}")
        counts = frame["curated_label"].astype(str).value_counts().to_dict()
        for label, minimum in minimums.items():
            if int(counts.get(label, 0)) < minimum:
                errors.append(f"{split_name}: {label} has {counts.get(label, 0)} rows; expected at least {minimum}")
        source_counts = frame["source"].astype(str).value_counts()
        if not source_counts.empty:
            top_source_share = float(source_counts.iloc[0]) / max(len(frame), 1)
            if top_source_share > 0.45:
                errors.append(
                    f"{split_name}: top source dominates split set: {source_counts.index[0]} share {top_source_share:.2%}"
                )

    dev_ids = set(split_frames["dev"]["episode_id"].astype(str).tolist())
    eval_ids = set(split_frames["eval_locked"]["episode_id"].astype(str).tolist())
    overlap_ids = sorted(dev_ids & eval_ids)
    if overlap_ids:
        errors.append(f"duplicate episode_id across dev/eval_locked: {len(overlap_ids)}")
    return errors


def split_curation_df(curation_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split the curated set into dev and locked eval subsets with bucket diversity."""
    eval_parts: list[pd.DataFrame] = []
    dev_parts: list[pd.DataFrame] = []

    for label in sorted(CURATED_LABELS):
        bucket = curation_df[curation_df["curated_label"].astype(str) == label].copy()
        eval_target = int(SPLIT_BUCKET_TARGETS["eval_locked"][label])
        eval_bucket = _balanced_take(bucket, eval_target)
        eval_ids = set(eval_bucket["episode_id"].astype(str).tolist())
        dev_bucket = bucket[~bucket["episode_id"].astype(str).isin(eval_ids)].copy()
        eval_parts.append(eval_bucket)
        dev_parts.append(dev_bucket)

    dev_df = pd.concat(dev_parts, ignore_index=True).sort_values(["curated_label", "source", "episode_id"]).reset_index(drop=True)
    eval_df = pd.concat(eval_parts, ignore_index=True).sort_values(["curated_label", "source", "episode_id"]).reset_index(drop=True)
    return {
        "dev": dev_df,
        "eval_locked": eval_df,
    }


def write_curation_outputs(root_dir: Path, outputs: dict[str, Any]) -> dict[str, Path]:
    """Write curated CSVs and evaluation summary JSON to the artifacts directory."""
    curation_dir = root_dir / "artifacts" / "curation"
    curation_dir.mkdir(parents=True, exist_ok=True)
    csv_path = curation_dir / "reconciliation_signoff_eval.csv"
    dev_path = curation_dir / "reconciliation_signoff_dev.csv"
    eval_locked_path = curation_dir / "reconciliation_signoff_eval_locked.csv"
    summary_path = curation_dir / "reconciliation_signoff_eval_summary.json"
    outputs["curation_df"].to_csv(csv_path, index=False)
    outputs["split_frames"]["dev"].to_csv(dev_path, index=False)
    outputs["split_frames"]["eval_locked"].to_csv(eval_locked_path, index=False)
    summary_path.write_text(json.dumps(outputs["evaluation_summary"], ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "curation_csv": csv_path,
        "dev_csv": dev_path,
        "eval_locked_csv": eval_locked_path,
        "summary_json": summary_path,
    }


def _load_bundle(root_dir: Path) -> dict[str, Any]:
    """Load current persona artifacts and build reconcile-boost simulation assignments."""
    config = load_yaml(root_dir / "config" / "bottleneck_clustering.yaml")
    episodes_all = read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet")
    labeled_all = read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet")
    core_mask = labeled_all["persona_core_eligible"] == True
    core_ids = set(labeled_all.loc[core_mask, "episode_id"].astype(str).tolist())

    episodes = episodes_all[episodes_all["episode_id"].astype(str).isin(core_ids)].copy()
    labeled = labeled_all[labeled_all["episode_id"].astype(str).isin(core_ids)].copy()
    axis_wide = read_parquet(root_dir / "data" / "analysis" / "persona_axis_assignments.parquet")
    axis_wide = axis_wide[axis_wide["episode_id"].astype(str).isin(core_ids)].copy()
    assignments = read_parquet(root_dir / "data" / "analysis" / "persona_assignments.parquet").copy()

    base_df = (
        assignments[["episode_id", "persona_id", "cluster_signature"]]
        .rename(columns={"persona_id": "persona_id_current", "cluster_signature": "current_cluster_signature"})
        .merge(axis_wide, on="episode_id", how="left")
        .merge(
            labeled[
                [
                    "episode_id",
                    "pain_codes",
                    "question_codes",
                    "output_codes",
                    "labelability_status",
                    "labelability_reason",
                ]
            ],
            on="episode_id",
            how="left",
        )
        .merge(
            episodes[
                [
                    "episode_id",
                    "source",
                    "url",
                    "normalized_episode",
                    "business_question",
                    "bottleneck_text",
                    "desired_output",
                    "tool_env",
                    "work_moment",
                    "evidence_snippet",
                ]
            ],
            on="episode_id",
            how="left",
        )
        .fillna("")
    )
    base_df["source_url"] = base_df["url"].astype(str)
    base_df["text_blob"] = (
        base_df[
            [
                "normalized_episode",
                "business_question",
                "bottleneck_text",
                "desired_output",
                "tool_env",
                "work_moment",
                "evidence_snippet",
            ]
        ]
        .astype(str)
        .agg(" ".join, axis=1)
        .str.lower()
    )
    base_df["positive_term_hits"] = base_df["text_blob"].map(lambda text: _count_matches(text, POSITIVE_TERMS))
    base_df["metric_term_hits"] = base_df["text_blob"].map(lambda text: _count_matches(text, METRIC_CONTEXT_TERMS))
    base_df["noise_term_hits"] = base_df["text_blob"].map(lambda text: _count_matches(text, NOISE_TERMS))
    base_df["hard_negative_term_hits"] = base_df["text_blob"].map(lambda text: _count_matches(text, HARD_NEGATIVE_TERMS))

    p1 = base_df[base_df["persona_id_current"].astype(str) == "persona_01"].copy()
    p1["A_hits"] = p1["text_blob"].map(lambda text: _count_matches(text, {
        "mismatch",
        "discrepancy",
        "inconsistent numbers",
        "numbers do not match",
        "numbers don't match",
        "totals do not match",
        "wrong total",
        "missing data",
        "no data",
        "dashboard does not match",
        "report does not match",
    }))
    p1["B_hits"] = p1["text_blob"].map(lambda text: _count_matches(text, {
        "validate numbers",
        "reconcile",
        "reconciliation",
        "verify report",
        "check before sending",
        "repeated validation",
        "sanity check",
        "audit numbers",
    }))
    p1["C_hits"] = p1["text_blob"].map(lambda text: _count_matches(text, {
        "signoff",
        "approval",
        "stakeholder",
        "client report",
        "leadership report",
        "explain the difference",
        "cross-team alignment",
        "numbers before sending",
    }))
    p1["D_hits"] = p1["metric_term_hits"]
    p1["NEG_hits"] = p1["noise_term_hits"] + p1["hard_negative_term_hits"]
    p1["axis_assist"] = p1.apply(_axis_assist, axis=1)
    p1["pos_groups"] = p1[["A_hits", "B_hits", "C_hits", "D_hits"]].gt(0).sum(axis=1)

    loose_tail = p1[(p1[["A_hits", "B_hits", "C_hits"]].gt(0).any(axis=1)) & (p1["D_hits"] > 0)].copy()
    strict_tail = p1[
        (p1["A_hits"] > 0)
        & (p1["D_hits"] > 0)
        & ((p1["pos_groups"] >= 3) | ((p1["pos_groups"] >= 2) & p1["axis_assist"]))
        & (p1["NEG_hits"] == 0)
    ].copy()
    two_groups_neg_tail = p1[(p1["pos_groups"] >= 2) & (p1["NEG_hits"] == 0)].copy()

    reject_ids = _load_example_ids(root_dir / "data" / "analysis" / "rejected_example_samples.csv")
    borderline_ids = _load_example_ids(root_dir / "data" / "analysis" / "borderline_example_samples.csv")

    simulated = _simulate_reconcile_boost(
        episodes=episodes,
        labeled=labeled,
        axis_wide=axis_wide,
        current_assignments=assignments,
        config=config,
    )

    bundle = {
        "base_df": base_df,
        "p1_core": p1[~p1["episode_id"].isin(loose_tail["episode_id"])].copy(),
        "p1_loose_tail": loose_tail,
        "p1_strict_tail": strict_tail,
        "p1_two_groups_neg_tail": two_groups_neg_tail,
        "persona_04_df": base_df[base_df["persona_id_current"].astype(str) == "persona_04"].copy(),
        "moved_p1_to_p4_df": base_df[base_df["episode_id"].astype(str).isin(simulated["moved_p1_to_p4_ids"])].copy(),
        "reject_ids": reject_ids,
        "borderline_ids": borderline_ids,
        "simulated_assignments": simulated["simulated_assignments"],
        "simulated_mapping": simulated["mapping"],
        "persona_04_like_variant_id": simulated["persona_04_like_variant_id"],
        "persona_01_like_variant_id": simulated["persona_01_like_variant_id"],
    }
    return bundle


def _simulate_reconcile_boost(
    episodes: pd.DataFrame,
    labeled: pd.DataFrame,
    axis_wide: pd.DataFrame,
    current_assignments: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Run the best simulation-only reconcile boost variant and return remapped assignments."""
    variant_config = deepcopy(config)
    for key, mult in {
        "dashboard_mistrust": 1.35,
        "metric_reconciliation": 1.45,
        "repeated_validation_before_sending": 1.45,
        "cross_team_number_alignment": 1.35,
        "metric_definition_mismatch": 1.25,
    }.items():
        variant_config["feature_weights"][key] = round(float(variant_config["feature_weights"][key]) * mult, 4)
    for axis_name, axis_value, feature, delta in [
        ("analysis_goal", "validate_numbers", "metric_reconciliation", 0.4),
        ("analysis_goal", "validate_numbers", "repeated_validation_before_sending", 0.35),
        ("workflow_stage", "validation", "repeated_validation_before_sending", 0.35),
        ("tool_dependency_mode", "bi_dashboard_heavy", "dashboard_mistrust", 0.2),
        ("bottleneck_type", "data_quality", "metric_reconciliation", 0.25),
    ]:
        current = float(variant_config["axis_signal_weights"][axis_name][axis_value].get(feature, 0.0))
        variant_config["axis_signal_weights"][axis_name][axis_value][feature] = round(current + delta, 4)

    feature_df = build_bottleneck_feature_table(episodes, labeled, axis_wide, variant_config)
    simulated_assignments = assign_bottleneck_clusters(feature_df, variant_config)
    merged = (
        episodes.merge(labeled, on="episode_id", how="inner")
        .merge(axis_wide, on="episode_id", how="left")
        .merge(feature_df, on="episode_id", how="left")
        .merge(simulated_assignments, on="episode_id", how="inner")
        .fillna("")
    )
    simulated_assignments = merge_overlapping_personas(
        merged_df=merged,
        assignments_df=simulated_assignments,
        feature_df=feature_df,
        config=variant_config,
    )["assignments_df"]

    cross = current_assignments[["episode_id", "persona_id"]].merge(
        simulated_assignments[["episode_id", "persona_id"]],
        on="episode_id",
        how="inner",
        suffixes=("_base", "_variant"),
    )
    mapping: dict[str, str] = {}
    for persona_id, group in cross.groupby("persona_id_base", dropna=False):
        counts = group["persona_id_variant"].astype(str).value_counts()
        mapping[str(persona_id)] = str(counts.idxmax()) if not counts.empty else ""

    persona_04_like_variant_id = mapping.get("persona_04", "")
    moved_p1_to_p4_ids = set(
        cross[
            (cross["persona_id_base"].astype(str) == "persona_01")
            & (cross["persona_id_variant"].astype(str) == persona_04_like_variant_id)
        ]["episode_id"].astype(str)
    )
    return {
        "simulated_assignments": simulated_assignments.rename(columns={"persona_id": "reconcile_boost_persona_id"}),
        "mapping": mapping,
        "persona_04_like_variant_id": persona_04_like_variant_id,
        "persona_01_like_variant_id": mapping.get("persona_01", ""),
        "moved_p1_to_p4_ids": moved_p1_to_p4_ids,
    }


def _build_curated_df(bundle: dict[str, Any]) -> pd.DataFrame:
    """Assemble a balanced curated evaluation set from deterministic candidate pools."""
    base_df = bundle["base_df"].copy()
    simulated_assignments = bundle["simulated_assignments"][["episode_id", "reconcile_boost_persona_id"]].copy()
    base_df["episode_id"] = base_df["episode_id"].astype(str)
    simulated_assignments["episode_id"] = simulated_assignments["episode_id"].astype(str)
    base_df = base_df.merge(simulated_assignments, on="episode_id", how="left").fillna("")
    base_df["reconcile_boost_persona_04_like"] = (
        base_df["reconcile_boost_persona_id"].astype(str) == str(bundle["persona_04_like_variant_id"])
    )

    def enrich(pool_key: str, origin: str) -> pd.DataFrame:
        episode_ids = set(bundle[pool_key]["episode_id"].astype(str).tolist())
        return _mark_bucket_origin(base_df[base_df["episode_id"].astype(str).isin(episode_ids)].copy(), origin)

    p1_core = enrich("p1_core", "persona_01_core")
    p1_loose = enrich("p1_loose_tail", "persona_01_loose_tail")
    p1_strict = enrich("p1_strict_tail", "persona_01_strict_tail")
    p1_two_groups = enrich("p1_two_groups_neg_tail", "persona_01_two_groups_neg_tail")
    p4 = enrich("persona_04_df", "persona_04_current")
    moved = enrich("moved_p1_to_p4_df", "reconcile_boost_moved_from_persona_01")

    reject_ids = bundle["reject_ids"] | bundle["borderline_ids"]
    rejected_rows = _mark_bucket_origin(
        base_df[base_df["episode_id"].astype(str).isin(reject_ids)].copy(),
        "rejected_or_borderline_examples",
    )

    positive_pool = pd.concat(
        [
            p4[
                (
                    p4["analysis_goal"].astype(str).eq("validate_numbers")
                    | p4["question_codes"].astype(str).str.contains("Q_VALIDATE_NUMBERS", regex=False)
                    | p4["trust_validation_need"].astype(str).eq("high")
                )
                & (p4["noise_term_hits"] == 0)
                & (p4["hard_negative_term_hits"] == 0)
            ],
            moved[
                (
                    moved["analysis_goal"].astype(str).eq("validate_numbers")
                    | moved["question_codes"].astype(str).str.contains("Q_VALIDATE_NUMBERS", regex=False)
                    | moved["trust_validation_need"].astype(str).isin(["high", "medium"])
                )
                & (moved["metric_term_hits"] > 0)
                & (moved["noise_term_hits"] == 0)
                & (moved["hard_negative_term_hits"] == 0)
            ],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["episode_id"])

    parent_pool = p1_core[
        p1_core["analysis_goal"].astype(str).eq("report_speed")
        & p1_core["workflow_stage"].astype(str).eq("reporting")
        & p1_core["bottleneck_type"].astype(str).isin(["manual_reporting", "tool_limitation", "handoff_dependency"])
        & p1_core["trust_validation_need"].astype(str).isin(["", "unassigned", "low"])
        & ~p1_core["question_codes"].astype(str).str.contains("Q_VALIDATE_NUMBERS", regex=False)
        & (p1_core["positive_term_hits"] == 0)
        & (p1_core["noise_term_hits"] == 0)
        & (p1_core["hard_negative_term_hits"] == 0)
    ].copy()

    ambiguous_pool = pd.concat(
        [
            p1_loose[~p1_loose["episode_id"].isin(p1_strict["episode_id"])],
            p1_two_groups[~p1_two_groups["episode_id"].isin(positive_pool["episode_id"])],
            moved[
                moved["analysis_goal"].astype(str).eq("report_speed")
                & (
                    moved["question_codes"].astype(str).str.contains("Q_VALIDATE_NUMBERS", regex=False)
                    | moved["trust_validation_need"].astype(str).eq("medium")
                )
            ],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["episode_id"])
    ambiguous_pool = ambiguous_pool[
        (ambiguous_pool["metric_term_hits"] > 0)
        & (ambiguous_pool["noise_term_hits"] <= 1)
    ].copy()

    noise_pool = pd.concat(
        [
            rejected_rows[rejected_rows["noise_term_hits"] > 0],
            base_df[(base_df["noise_term_hits"] > 0) & (base_df["metric_term_hits"] <= 1)],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["episode_id"])

    hard_negative_pool = pd.concat(
        [
            rejected_rows[
                (rejected_rows["metric_term_hits"] > 0)
                & ((rejected_rows["hard_negative_term_hits"] > 0) | (rejected_rows["noise_term_hits"] > 0))
            ],
            base_df[
                (base_df["metric_term_hits"] > 0)
                & (base_df["hard_negative_term_hits"] > 0)
                & ~base_df["episode_id"].isin(positive_pool["episode_id"])
            ],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["episode_id"])

    selected_ids: set[str] = set()

    positive_df = _finalize_bucket(
        _take_unique(positive_pool, BUCKET_TARGETS["reconciliation_signoff_positive"], selected_ids),
        curated_label="reconciliation_signoff_positive",
        should_move_out=True,
        should_join_p4=True,
        confidence="high",
    )
    parent_df = _finalize_bucket(
        _take_unique(parent_pool, BUCKET_TARGETS["reporting_packager_parent"], selected_ids),
        curated_label="reporting_packager_parent",
        should_move_out=False,
        should_join_p4=False,
        confidence="high",
    )
    ambiguous_df = _finalize_bucket(
        _take_unique(ambiguous_pool, BUCKET_TARGETS["ambiguous_boundary"], selected_ids),
        curated_label="ambiguous_boundary",
        should_move_out=False,
        should_join_p4=False,
        confidence="low",
    )
    noise_df = _finalize_bucket(
        _take_unique(noise_pool, BUCKET_TARGETS["noise"], selected_ids),
        curated_label="noise",
        should_move_out=False,
        should_join_p4=False,
        confidence="medium",
    )
    hard_negative_df = _finalize_bucket(
        _take_unique(hard_negative_pool, BUCKET_TARGETS["hard_negative"], selected_ids),
        curated_label="hard_negative",
        should_move_out=False,
        should_join_p4=False,
        confidence="high",
    )

    curated_df = pd.concat([positive_df, parent_df, ambiguous_df, noise_df, hard_negative_df], ignore_index=True)
    curated_df = curated_df.drop_duplicates(subset=["episode_id"]).reset_index(drop=True)
    curated_df["reason"] = curated_df.apply(_build_reason, axis=1)
    curated_df = curated_df.sort_values(["curated_label", "source", "episode_id"]).reset_index(drop=True)
    return curated_df[REQUIRED_COLUMNS + ["pool_origin", "reconcile_boost_persona_id", "reconcile_boost_persona_04_like"]]


def _evaluate_curation(
    curation_df: pd.DataFrame,
    split_frames: dict[str, pd.DataFrame],
    bundle: dict[str, Any],
) -> dict[str, Any]:
    """Compare baseline and reconcile-boost simulation against the curated full and split sets."""
    total_by_label = curation_df["curated_label"].astype(str).value_counts().to_dict()

    return {
        "curation_counts": total_by_label,
        "source_distribution": curation_df["source"].astype(str).value_counts().to_dict(),
        "baseline_vs_reconcile_boost_full": _evaluate_subset(curation_df, bundle),
        "baseline_vs_reconcile_boost_dev": _evaluate_subset(split_frames["dev"], bundle),
        "baseline_vs_reconcile_boost_eval_locked": _evaluate_subset(split_frames["eval_locked"], bundle),
        "split_counts": {
            split_name: frame["curated_label"].astype(str).value_counts().to_dict()
            for split_name, frame in split_frames.items()
        },
        "persona_04_like_variant_id": str(bundle["persona_04_like_variant_id"]),
        "persona_01_like_variant_id": str(bundle["persona_01_like_variant_id"]),
        "bucket_examples": {
            label: (
                curation_df[curation_df["curated_label"].astype(str) == label][
                    ["episode_id", "source", "business_question", "bottleneck_text", "reason"]
                ]
                .head(3)
                .to_dict(orient="records")
            )
            for label in sorted(CURATED_LABELS)
        },
    }


def _balanced_take(df: pd.DataFrame, target_count: int) -> pd.DataFrame:
    """Take a source-balanced deterministic sample."""
    if df.empty:
        return df.copy()
    ordered = df.copy()
    ordered = ordered.sort_values(["source", "episode_id"]).reset_index(drop=True)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ordered.to_dict(orient="records"):
        groups[str(row.get("source", ""))].append(row)
    source_order = sorted(groups)
    taken: list[dict[str, Any]] = []
    while len(taken) < target_count and source_order:
        next_sources: list[str] = []
        for source in source_order:
            if len(taken) >= target_count:
                break
            items = groups.get(source, [])
            if not items:
                continue
            taken.append(items.pop(0))
            if items:
                next_sources.append(source)
        source_order = next_sources
    if len(taken) < target_count:
        taken_ids = {str(row.get("episode_id", "")) for row in taken}
        remaining = ordered[~ordered["episode_id"].astype(str).isin(taken_ids)]
        for row in remaining.to_dict(orient="records"):
            if len(taken) >= target_count:
                break
            taken.append(row)
    return pd.DataFrame(taken)


def _evaluate_subset(curation_df: pd.DataFrame, bundle: dict[str, Any]) -> dict[str, Any]:
    """Evaluate one curated subset against baseline and reconcile-boost assignments."""
    p4_variant_id = str(bundle["persona_04_like_variant_id"])
    p1_variant_id = str(bundle["persona_01_like_variant_id"])

    def bucket_frame(label: str) -> pd.DataFrame:
        return curation_df[curation_df["curated_label"].astype(str) == label].copy()

    positives = bucket_frame("reconciliation_signoff_positive")
    parents = bucket_frame("reporting_packager_parent")
    ambiguous = bucket_frame("ambiguous_boundary")
    hard_negatives = bucket_frame("hard_negative")

    baseline_positive_recall = _share(
        positives["persona_id_current"].astype(str).eq("persona_04").sum(),
        len(positives),
    )
    variant_positive_recall = _share(
        positives["reconcile_boost_persona_id"].astype(str).eq(p4_variant_id).sum(),
        len(positives),
    )
    baseline_parent_retention = _share(
        parents["persona_id_current"].astype(str).eq("persona_01").sum(),
        len(parents),
    )
    variant_parent_retention = _share(
        parents["reconcile_boost_persona_id"].astype(str).eq(p1_variant_id).sum(),
        len(parents),
    )
    baseline_hard_negative_fp = _share(
        hard_negatives["persona_id_current"].astype(str).eq("persona_04").sum(),
        len(hard_negatives),
    )
    variant_hard_negative_fp = _share(
        hard_negatives["reconcile_boost_persona_id"].astype(str).eq(p4_variant_id).sum(),
        len(hard_negatives),
    )
    baseline_ambiguous_move_rate = _share(
        ambiguous["persona_id_current"].astype(str).eq("persona_04").sum(),
        len(ambiguous),
    )
    variant_ambiguous_move_rate = _share(
        ambiguous["reconcile_boost_persona_id"].astype(str).eq(p4_variant_id).sum(),
        len(ambiguous),
    )
    return {
        "positive_recall": {
            "baseline": baseline_positive_recall,
            "reconcile_boost": variant_positive_recall,
        },
        "hard_negative_false_positive_rate": {
            "baseline": baseline_hard_negative_fp,
            "reconcile_boost": variant_hard_negative_fp,
        },
        "ambiguous_movement_rate": {
            "baseline": baseline_ambiguous_move_rate,
            "reconcile_boost": variant_ambiguous_move_rate,
        },
        "parent_retention_rate": {
            "baseline": baseline_parent_retention,
            "reconcile_boost": variant_parent_retention,
        },
        "positives_in_persona_04_like": {
            "baseline": int(positives["persona_id_current"].astype(str).eq("persona_04").sum()),
            "reconcile_boost": int(positives["reconcile_boost_persona_id"].astype(str).eq(p4_variant_id).sum()),
        },
        "parent_examples_wrongly_pulled_out_of_persona_01": {
            "baseline": int((~parents["persona_id_current"].astype(str).eq("persona_01")).sum()),
            "reconcile_boost": int((~parents["reconcile_boost_persona_id"].astype(str).eq(p1_variant_id)).sum()),
        },
    }


def _take_unique(df: pd.DataFrame, target_count: int, selected_ids: set[str]) -> pd.DataFrame:
    """Take a balanced sample while excluding rows already selected by prior buckets."""
    if df.empty:
        return df.copy()
    available = df[~df["episode_id"].astype(str).isin(selected_ids)].copy()
    taken = _balanced_take(available, target_count)
    selected_ids.update(taken.get("episode_id", pd.Series(dtype=str)).astype(str).tolist())
    return taken


def _finalize_bucket(
    df: pd.DataFrame,
    curated_label: str,
    should_move_out: bool,
    should_join_p4: bool,
    confidence: str,
) -> pd.DataFrame:
    """Attach manual-style decision fields to one curated bucket."""
    result = df.copy()
    result["curated_label"] = curated_label
    result["should_move_out_of_persona_01"] = bool(should_move_out)
    result["should_join_persona_04_like"] = bool(should_join_p4)
    result["confidence"] = str(confidence)
    return result


def _build_reason(row: pd.Series) -> str:
    """Build one concise curation reason from the row's strongest evidence."""
    label = str(row.get("curated_label", ""))
    goal = str(row.get("analysis_goal", ""))
    workflow = str(row.get("workflow_stage", ""))
    bottleneck = str(row.get("bottleneck_type", ""))
    trust = str(row.get("trust_validation_need", ""))
    if label == "reconciliation_signoff_positive":
        return f"{goal or 'validate'} + {workflow or 'validation'} with {bottleneck or 'data quality'} and {trust or 'trust'} indicates genuine reconciliation/signoff pain"
    if label == "reporting_packager_parent":
        return f"{goal or 'report_speed'} + {workflow or 'reporting'} with {bottleneck or 'manual reporting'} reflects recurring reporting-packager work, not metric trust failure"
    if label == "ambiguous_boundary":
        return f"mixed {goal or 'reporting'} and validation cues create a boundary case between reporting-packager and reconciliation personas"
    if label == "noise":
        return "content is dominated by community/newsletter/vendor/docs-style material rather than persona evidence"
    return "metric/reporting words appear, but the real issue is setup/support/helpdesk-style rather than reconciliation/signoff pain"


def _mark_bucket_origin(df: pd.DataFrame, origin: str) -> pd.DataFrame:
    """Return a copy of one pool tagged with its origin."""
    result = df.copy()
    result["pool_origin"] = origin
    return result


def _load_example_ids(path: Path) -> set[str]:
    """Load example sample episode ids when the CSV exists."""
    if not path.exists():
        return set()
    frame = pd.read_csv(path)
    return set(frame.get("episode_id", pd.Series(dtype=str)).astype(str).tolist())


def _count_matches(text: str, terms: set[str]) -> int:
    """Count substring matches from a small phrase set."""
    value = str(text or "")
    return sum(1 for term in terms if term in value)


def _axis_assist(row: pd.Series) -> bool:
    """Return True when axis or label cues support reconciliation/validation interpretation."""
    return any(
        [
            "Q_VALIDATE_NUMBERS" in str(row.get("question_codes", "")),
            "metric_reconciliation" in str(row.get("cluster_signature", "")),
            str(row.get("analysis_goal", "")) == "validate_numbers",
            str(row.get("trust_validation_need", "")) in {
                "high",
                "numbers_do_not_reconcile_or_feel_safe_to_share",
                "needs_manual_validation_before_share",
            },
        ]
    )


def _share(numerator: int, denominator: int) -> float:
    """Return a rounded percentage share."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 1)
