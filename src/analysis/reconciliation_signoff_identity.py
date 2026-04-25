"""Identity and release-gate helpers for reconciliation/signoff diagnostics."""

from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd


def share(numerator: int, denominator: int) -> float:
    """Return a rounded percentage share."""
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 1)


def split_codes(value: str) -> list[str]:
    """Split one pipe-delimited code string into cleaned parts."""
    raw = str(value or "").replace("||", "|")
    parts = [part.strip() for part in raw.split("|")]
    return [part for part in parts if part and part.lower() not in {"nan", "none", "unknown", "unassigned"}]


def top_terms(series: pd.Series, limit: int = 5) -> list[dict[str, Any]]:
    """Return the most common code terms from one series."""
    counts: Counter[str] = Counter()
    for value in series.fillna("").astype(str):
        counts.update(split_codes(value))
    return [
        {"value": term, "count": int(count)}
        for term, count in counts.most_common(limit)
    ]


def top_distribution(series: pd.Series, limit: int = 5) -> list[dict[str, Any]]:
    """Return the most common scalar values from one series."""
    cleaned = series.fillna("").astype(str)
    cleaned = cleaned[~cleaned.isin(["", "unknown", "unassigned", "nan", "None"])]
    counts = cleaned.value_counts().head(limit)
    total = max(len(series), 1)
    return [
        {"value": str(value), "count": int(count), "share_pct": share(int(count), total)}
        for value, count in counts.items()
    ]


def build_cluster_semantic_profiles(
    frame: pd.DataFrame,
    persona_column: str,
    persona_ids: list[str] | None = None,
    persona_summary_df: pd.DataFrame | None = None,
    persona_examples_df: pd.DataFrame | None = None,
    profile_label: str = "",
) -> pd.DataFrame:
    """Build one semantic profile row per persona id for diagnostic comparison."""
    working = frame.copy()
    working[persona_column] = working[persona_column].fillna("").astype(str)
    if persona_ids is not None:
        working = working[working[persona_column].isin([str(value) for value in persona_ids])].copy()
    if working.empty:
        return pd.DataFrame()

    summary_lookup = {}
    if persona_summary_df is not None and not persona_summary_df.empty and "persona_id" in persona_summary_df.columns:
        summary_lookup = (
            persona_summary_df.set_index("persona_id")
            .to_dict(orient="index")
        )

    examples_lookup: dict[str, list[dict[str, str]]] = {}
    if persona_examples_df is not None and not persona_examples_df.empty and "persona_id" in persona_examples_df.columns:
        for persona_id, group in persona_examples_df.groupby("persona_id", dropna=False):
            examples_lookup[str(persona_id)] = (
                group.sort_values(["example_rank", "final_example_score"], ascending=[True, False])
                .head(3)[["episode_id", "source", "grounded_text", "why_selected"]]
                .fillna("")
                .to_dict(orient="records")
            )

    rows: list[dict[str, Any]] = []
    for persona_id, group in working.groupby(persona_column, dropna=False):
        persona_id = str(persona_id)
        size = len(group)
        summary = summary_lookup.get(persona_id, {})
        signature_series = (
            group["cluster_signature"]
            if "cluster_signature" in group.columns
            else group.get("current_cluster_signature", pd.Series(dtype=str))
        )
        profile = {
            "profile_label": profile_label,
            "persona_id": persona_id,
            "persona_size": int(size),
            "cluster_signature_examples": top_distribution(signature_series, limit=3),
            "top_pain_codes": top_terms(group.get("pain_codes", pd.Series(dtype=str))),
            "top_question_codes": top_terms(group.get("question_codes", pd.Series(dtype=str))),
            "top_output_codes": top_terms(group.get("output_codes", pd.Series(dtype=str))),
            "workflow_stage_distribution": top_distribution(group.get("workflow_stage", pd.Series(dtype=str))),
            "analysis_goal_distribution": top_distribution(group.get("analysis_goal", pd.Series(dtype=str))),
            "bottleneck_type_distribution": top_distribution(group.get("bottleneck_type", pd.Series(dtype=str))),
            "trust_validation_need_distribution": top_distribution(group.get("trust_validation_need", pd.Series(dtype=str))),
            "source_mix_top_5": top_distribution(group.get("source", pd.Series(dtype=str))),
            "validation_share_pct": share(
                int(
                    group.get("analysis_goal", pd.Series(dtype=str)).fillna("").astype(str).eq("validate_numbers").sum()
                    + group.get("workflow_stage", pd.Series(dtype=str)).fillna("").astype(str).eq("validation").sum()
                    + group.get("bottleneck_type", pd.Series(dtype=str)).fillna("").astype(str).eq("data_quality").sum()
                ),
                size * 3,
            ),
            "trust_medium_or_high_share_pct": share(
                int(group.get("trust_validation_need", pd.Series(dtype=str)).fillna("").astype(str).isin(["high", "medium"]).sum()),
                size,
            ),
            "manual_reporting_share_pct": share(
                int(group.get("bottleneck_type", pd.Series(dtype=str)).fillna("").astype(str).eq("manual_reporting").sum()),
                size,
            ),
            "report_speed_share_pct": share(
                int(group.get("analysis_goal", pd.Series(dtype=str)).fillna("").astype(str).eq("report_speed").sum()),
                size,
            ),
            "promotion_status": str(summary.get("promotion_status", "")),
            "share_rank": summary.get("share_rank", ""),
            "dominant_signature": str(summary.get("dominant_signature", "")),
            "selected_examples": examples_lookup.get(persona_id, []),
            "representative_examples": (
                group[["episode_id", "source", "business_question", "bottleneck_text"]]
                .fillna("")
                .head(3)
                .to_dict(orient="records")
            ),
        }
        rows.append(profile)
    return pd.DataFrame(rows)


def select_reconciliation_like_persona(
    dev_df: pd.DataFrame,
    profile_df: pd.DataFrame,
    persona_column: str = "persona_id_current",
) -> dict[str, Any]:
    """Select the reconciliation-like target by semantic evidence, not fixed persona id."""
    candidates: list[dict[str, Any]] = []
    all_personas = sorted(set(profile_df["persona_id"].astype(str).tolist()) | set(dev_df[persona_column].astype(str).tolist()))
    for persona_id in all_personas:
        profile_row = profile_df[profile_df["persona_id"].astype(str) == str(persona_id)]
        profile = profile_row.iloc[0].to_dict() if not profile_row.empty else {}
        positives = dev_df[
            (dev_df["curated_label"].astype(str) == "reconciliation_signoff_positive")
            & (dev_df[persona_column].astype(str) == str(persona_id))
        ]
        parents = dev_df[
            (dev_df["curated_label"].astype(str) == "reporting_packager_parent")
            & (dev_df[persona_column].astype(str) == str(persona_id))
        ]
        hard_negatives = dev_df[
            (dev_df["curated_label"].astype(str) == "hard_negative")
            & (dev_df[persona_column].astype(str) == str(persona_id))
        ]
        ambiguous = dev_df[
            (dev_df["curated_label"].astype(str) == "ambiguous_boundary")
            & (dev_df[persona_column].astype(str) == str(persona_id))
        ]
        total_positive = int((dev_df["curated_label"].astype(str) == "reconciliation_signoff_positive").sum())
        total_hard_negative = int((dev_df["curated_label"].astype(str) == "hard_negative").sum())
        total_ambiguous = int((dev_df["curated_label"].astype(str) == "ambiguous_boundary").sum())
        total_parent = int((dev_df["curated_label"].astype(str) == "reporting_packager_parent").sum())
        positive_recall = share(len(positives), total_positive)
        hard_negative_fp = share(len(hard_negatives), total_hard_negative)
        ambiguous_move = share(len(ambiguous), total_ambiguous)
        parent_capture = share(len(parents), total_parent)
        validation_share = float(profile.get("validation_share_pct", 0.0) or 0.0)
        trust_share = float(profile.get("trust_medium_or_high_share_pct", 0.0) or 0.0)
        manual_reporting_share = float(profile.get("manual_reporting_share_pct", 0.0) or 0.0)
        report_speed_share = float(profile.get("report_speed_share_pct", 0.0) or 0.0)
        score = round(
            (positive_recall * 1.3)
            + (validation_share * 0.9)
            + (trust_share * 0.5)
            - (hard_negative_fp * 1.2)
            - (ambiguous_move * 0.4)
            - (parent_capture * 0.7)
            - (manual_reporting_share * 0.2)
            - (report_speed_share * 0.1),
            3,
        )
        candidates.append(
            {
                "persona_id": str(persona_id),
                "selection_score": score,
                "positive_recall": positive_recall,
                "hard_negative_false_positive_rate": hard_negative_fp,
                "ambiguous_movement_rate": ambiguous_move,
                "parent_capture_rate": parent_capture,
                "validation_share_pct": validation_share,
                "trust_medium_or_high_share_pct": trust_share,
                "manual_reporting_share_pct": manual_reporting_share,
                "report_speed_share_pct": report_speed_share,
                "promotion_status": str(profile.get("promotion_status", "")),
                "dominant_signature": str(profile.get("dominant_signature", "")),
            }
        )
    ordered = sorted(
        candidates,
        key=lambda row: (
            -float(row["selection_score"]),
            -float(row["positive_recall"]),
            float(row["hard_negative_false_positive_rate"]),
            float(row["parent_capture_rate"]),
        ),
    )
    if not ordered:
        raise ValueError("No persona candidates available for reconciliation-like target selection.")
    selected = ordered[0]
    return {
        "selected_persona_id": str(selected["persona_id"]),
        "selection_reason": (
            f"selected by semantic evidence: positive_recall={selected['positive_recall']}, "
            f"validation_share_pct={selected['validation_share_pct']}, "
            f"hard_negative_fp={selected['hard_negative_false_positive_rate']}, "
            f"parent_capture_rate={selected['parent_capture_rate']}"
        ),
        "candidate_scores": ordered,
    }


def build_overlap_matrix(
    baseline_df: pd.DataFrame,
    variant_df: pd.DataFrame,
    baseline_column: str,
    variant_column: str,
    baseline_persona_ids: list[str],
) -> list[dict[str, Any]]:
    """Build a Jaccard overlap matrix from baseline personas to variant personas."""
    base = baseline_df[["episode_id", baseline_column]].copy()
    var = variant_df[["episode_id", variant_column]].copy()
    merged = base.merge(var, on="episode_id", how="inner")
    rows: list[dict[str, Any]] = []
    variant_ids = sorted(merged[variant_column].astype(str).unique().tolist())
    for baseline_persona_id in baseline_persona_ids:
        baseline_ids = set(
            merged.loc[merged[baseline_column].astype(str) == str(baseline_persona_id), "episode_id"].astype(str).tolist()
        )
        for variant_persona_id in variant_ids:
            variant_ids_set = set(
                merged.loc[merged[variant_column].astype(str) == str(variant_persona_id), "episode_id"].astype(str).tolist()
            )
            union = baseline_ids | variant_ids_set
            intersection = baseline_ids & variant_ids_set
            rows.append(
                {
                    "baseline_persona_id": str(baseline_persona_id),
                    "variant_persona_id": str(variant_persona_id),
                    "intersection_count": int(len(intersection)),
                    "baseline_size": int(len(baseline_ids)),
                    "variant_size": int(len(variant_ids_set)),
                    "jaccard_overlap": round((len(intersection) / len(union)) if union else 0.0, 4),
                    "baseline_retention_pct": share(len(intersection), len(baseline_ids)),
                }
            )
    return rows


def build_promotion_drift_flags(
    persona_status_lookup: dict[str, str],
    selected_persona_id: str,
) -> dict[str, bool]:
    """Return simple drift flags for release-gate diagnostics."""
    persona_04_status = str(persona_status_lookup.get("persona_04", ""))
    persona_05_status = str(persona_status_lookup.get("persona_05", ""))
    return {
        "persona_04_still_exploratory": persona_04_status == "exploratory_bucket",
        "persona_05_promotion_drift": persona_05_status == "promoted_persona" and str(selected_persona_id) != "persona_05",
        "target_is_not_persona_04": str(selected_persona_id) != "persona_04",
    }


def classify_target_change_type(
    baseline_target_id: str,
    variant_target_id: str,
    baseline_target_best_match: str,
    jaccard_overlap: float,
    semantic_similarity_score: float,
) -> str:
    """Classify whether one target change is stable, renumbered, or semantic drift."""
    if str(variant_target_id) == str(baseline_target_id) and jaccard_overlap >= 0.6:
        return "stable_same_id"
    if (
        str(variant_target_id) == str(baseline_target_best_match)
        and str(variant_target_id) != str(baseline_target_id)
        and jaccard_overlap >= 0.6
        and semantic_similarity_score >= 80.0
    ):
        return "renumbered_with_continuity"
    return "semantic_drift"


def evaluate_identity_continuity_gate(
    *,
    baseline_target_id: str,
    variant_target_id: str,
    baseline_target_best_match: str,
    jaccard_overlap: float,
    selected_example_overlap_pct: float,
    positive_recall: float,
    hard_negative_false_positive_rate: float,
    ambiguous_movement_rate: float,
    raw_reconcile_boost_ambiguous_movement_rate: float,
    persona_01_parent_leakage_pct: float,
    persona_05_promotion_drift_risk: bool,
    semantic_similarity_score: float,
    reference_only: bool = False,
) -> dict[str, Any]:
    """Evaluate whether one simulation variant preserves reconciliation target identity."""
    target_change_type = classify_target_change_type(
        baseline_target_id=baseline_target_id,
        variant_target_id=variant_target_id,
        baseline_target_best_match=baseline_target_best_match,
        jaccard_overlap=jaccard_overlap,
        semantic_similarity_score=semantic_similarity_score,
    )
    checks = {
        "baseline_target_matches_best_match": str(variant_target_id) == str(baseline_target_best_match),
        "jaccard_overlap_high": float(jaccard_overlap) >= 0.6,
        "selected_example_overlap_high": float(selected_example_overlap_pct) >= 80.0,
        "positive_recall_high_enough": float(positive_recall) >= 80.0,
        "hard_negative_fp_within_guarded_range": float(hard_negative_false_positive_rate) <= 16.7,
        "ambiguous_movement_improves_vs_raw_reconcile_boost": float(ambiguous_movement_rate) < float(raw_reconcile_boost_ambiguous_movement_rate),
        "persona_01_leakage_below_ceiling": float(persona_01_parent_leakage_pct) <= 5.0,
        "persona_05_promotion_drift_absent": not bool(persona_05_promotion_drift_risk),
        "target_change_is_not_semantic_drift": target_change_type != "semantic_drift",
    }
    fail_reasons = [name for name, passed in checks.items() if not passed]
    if reference_only:
        return {
            "reference_only": True,
            "eligible_for_future_implementation": False,
            "target_change_type": "baseline_reference",
            "checks": checks,
            "fail_reasons": [],
        }
    return {
        "reference_only": False,
        "eligible_for_future_implementation": len(fail_reasons) == 0,
        "target_change_type": target_change_type,
        "checks": checks,
        "fail_reasons": fail_reasons,
    }
