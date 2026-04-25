"""Run anchor-level reconciliation/signoff simulation variants without changing production."""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Callable

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.reconciliation_signoff_anchor_simulation import (
    anchor_variant_eligibility,
    estimate_final_usable_persona_count,
    evaluate_anchor_subset,
    persona_statuses,
    top_3_cluster_share,
)
from src.analysis.reconciliation_signoff_identity import (
    build_cluster_semantic_profiles,
    build_overlap_matrix,
    build_promotion_drift_flags,
    evaluate_identity_continuity_gate,
)


def _load_variant_cli_module():
    """Load the existing reconcile variant CLI helpers."""
    cli_path = ROOT_DIR / "run" / "cli" / "21_evaluate_reconciliation_signoff_variants.py"
    spec = importlib.util.spec_from_file_location("anchor_sim_variant_cli", cli_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load reconciliation variant CLI module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class AnchorSimulationVariant:
    """One anchor-driven simulation-only variant."""

    variant_id: str
    description: str
    builder: Callable[[pd.DataFrame], pd.Series]


def _load_csv(path: Path) -> pd.DataFrame:
    """Load one required CSV artifact."""
    if not path.exists():
        raise SystemExit(f"Missing required artifact: {path}")
    return pd.read_csv(path).fillna("")


def _selected_example_overlap(
    persona_examples_df: pd.DataFrame,
    crosswalk: pd.DataFrame,
    baseline_persona_id: str,
    variant_persona_id: str,
) -> dict[str, Any]:
    """Return overlap of baseline selected examples with one variant cluster."""
    baseline_examples = persona_examples_df[
        (persona_examples_df["persona_id"].astype(str) == str(baseline_persona_id))
        & (persona_examples_df["selection_decision"].astype(str) == "selected")
    ].copy()
    example_ids = set(baseline_examples["episode_id"].astype(str).tolist())
    if not example_ids:
        return {"selected_example_overlap_count": 0, "selected_example_overlap_pct": 0.0}
    moved = crosswalk[
        crosswalk["episode_id"].astype(str).isin(example_ids)
        & crosswalk["variant_persona_id"].astype(str).eq(str(variant_persona_id))
    ]
    return {
        "selected_example_overlap_count": int(len(moved)),
        "selected_example_overlap_pct": round((len(moved) / len(example_ids)) * 100.0, 1),
    }


def _semantic_similarity(
    baseline_profiles: pd.DataFrame,
    variant_profiles: pd.DataFrame,
    baseline_persona_id: str,
    variant_persona_id: str,
) -> float:
    """Return one compact semantic similarity score between profile rows."""
    base_row = baseline_profiles[baseline_profiles["persona_id"].astype(str) == str(baseline_persona_id)]
    variant_row = variant_profiles[variant_profiles["persona_id"].astype(str) == str(variant_persona_id)]
    if base_row.empty or variant_row.empty:
        return 0.0
    base = base_row.iloc[0]
    variant = variant_row.iloc[0]
    diffs = [
        abs(float(base.get("validation_share_pct", 0.0)) - float(variant.get("validation_share_pct", 0.0))),
        abs(float(base.get("trust_medium_or_high_share_pct", 0.0)) - float(variant.get("trust_medium_or_high_share_pct", 0.0))),
        abs(float(base.get("manual_reporting_share_pct", 0.0)) - float(variant.get("manual_reporting_share_pct", 0.0))),
        abs(float(base.get("report_speed_share_pct", 0.0)) - float(variant.get("report_speed_share_pct", 0.0))),
    ]
    return round(max(0.0, 100.0 - (sum(diffs) / max(len(diffs), 1))), 1)


def _persona_01_leakage_pct(crosswalk: pd.DataFrame, variant_target_id: str) -> float:
    """Return how much baseline persona_01 leaks into one persona_04-like target."""
    baseline_p1 = crosswalk[crosswalk["persona_id_current"].astype(str) == "persona_01"].copy()
    if baseline_p1.empty:
        return 0.0
    hits = baseline_p1["variant_persona_id"].astype(str).eq(str(variant_target_id)).sum()
    return round((float(hits) / float(len(baseline_p1))) * 100.0, 1)


def _prepare_frame(root_dir: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    """Build one reusable simulation frame with anchor-specific features."""
    variant_cli = _load_variant_cli_module()
    bundle, frame = variant_cli._prepare_simulation_frame(root_dir)
    frame = frame.copy()
    frame["episode_id"] = frame["episode_id"].astype(str)
    frame["persona_id_current"] = frame["persona_id_current"].astype(str)
    frame["is_baseline_persona_04"] = frame["persona_id_current"].eq("persona_04")
    frame["is_baseline_persona_01"] = frame["persona_id_current"].eq("persona_01")
    frame["high_trust_validation_profile"] = (
        frame["analysis_goal"].eq("validate_numbers")
        & frame["workflow_stage"].eq("validation")
        & frame["bottleneck_type"].eq("data_quality")
        & frame["trust_strong"]
        & frame["has_q_validate"]
        & frame["has_p_data_quality"]
    )
    frame["anchor_similarity_score"] = (
        frame["analysis_goal"].eq("validate_numbers").astype(int) * 3
        + frame["workflow_stage"].eq("validation").astype(int) * 2
        + frame["bottleneck_type"].eq("data_quality").astype(int) * 2
        + frame["trust_strong"].astype(int) * 2
        + frame["has_q_validate"].astype(int) * 2
        + frame["has_p_data_quality"].astype(int) * 2
        + frame["discrepancy_phrase_hits"].clip(upper=2)
        - frame["helpdesk_phrase_hits"].clip(upper=2) * 3
        - frame["manual_reporting_like"].astype(int) * 2
    ).astype(int)
    return bundle, frame


def _variant_definitions() -> list[AnchorSimulationVariant]:
    """Return all anchor-level simulation variants."""

    def baseline_variant(frame: pd.DataFrame) -> pd.Series:
        return frame["persona_id_current"].astype(str)

    def variant_a(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & frame["high_trust_validation_profile"]
            & frame["discrepancy_phrase_hits"].ge(1)
            & frame["helpdesk_phrase_hits"].eq(0)
        )
        result.loc[move_mask] = "persona_04"
        result.loc[frame["is_baseline_persona_04"]] = "persona_04"
        return result

    def variant_b(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & frame["anchor_similarity_score"].ge(10)
            & frame["trust_strong"]
            & ~frame["manual_reporting_like"]
        )
        result.loc[move_mask] = "persona_04"
        return result

    def variant_c(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & frame["reconcile_boost_persona_id"].astype(str).eq("persona_03")
            & frame["anchor_similarity_score"].ge(10)
            & frame["helpdesk_phrase_hits"].eq(0)
        )
        result.loc[move_mask] = "persona_04"
        result.loc[frame["is_baseline_persona_04"]] = "persona_04"
        return result

    def variant_d(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & frame["high_trust_validation_profile"]
            & frame["discrepancy_phrase_hits"].ge(2)
            & ~frame["manual_reporting_like"]
        )
        result.loc[move_mask] = "persona_04"
        return result

    def variant_e(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & frame["anchor_similarity_score"].ge(9)
            & frame["helpdesk_phrase_hits"].eq(0)
            & frame["hard_negative_term_hits"].eq(0)
        )
        result.loc[move_mask] = "persona_04"
        return result

    def variant_f(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & frame["anchor_similarity_score"].ge(9)
            & ~frame["manual_reporting_like"]
            & ~(
                frame["analysis_goal"].eq("report_speed")
                & frame["workflow_stage"].eq("reporting")
                & frame["trust_validation_need"].isin(["", "unassigned", "low"])
            )
        )
        result.loc[move_mask] = "persona_04"
        return result

    def variant_g(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & frame["high_trust_validation_profile"]
            & frame["anchor_similarity_score"].ge(10)
            & frame["discrepancy_phrase_hits"].ge(1)
            & frame["helpdesk_phrase_hits"].eq(0)
            & ~frame["manual_reporting_like"]
        )
        result.loc[move_mask] = "persona_04"
        return result

    return [
        AnchorSimulationVariant("A", "persona_04 anchor-protected expansion", variant_a),
        AnchorSimulationVariant("B", "persona_04 positive-anchor nearest-neighbor expansion", variant_b),
        AnchorSimulationVariant("C", "persona_04 pre-merge anchor guard", variant_c),
        AnchorSimulationVariant("D", "persona_04 expansion only for high-trust validation profile", variant_d),
        AnchorSimulationVariant("E", "persona_04 expansion with hard-negative phrase block", variant_e),
        AnchorSimulationVariant("F", "persona_04 expansion with parent-reporting retention guard", variant_f),
        AnchorSimulationVariant("G", "combined conservative anchor variant", variant_g),
        AnchorSimulationVariant("H", "no-op baseline", baseline_variant),
    ]


def _representative_examples(
    frame: pd.DataFrame,
    variant_persona_id: str,
    variant_target_id: str,
    anchor_df: pd.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return representative moved and blocked examples."""
    moved = frame[
        frame["persona_id_current"].astype(str).eq("persona_01")
        & frame["variant_persona_id"].astype(str).eq(str(variant_target_id))
    ][["episode_id", "source", "business_question", "bottleneck_text"]].head(5)

    blocked_ids = set(
        anchor_df[
            anchor_df["anchor_label"].astype(str).isin(
                {"anchor_hard_negative", "anchor_parent_reporting_packager", "non_anchor_ambiguous"}
            )
            & ~anchor_df["variant_persona_id"].astype(str).eq(str(variant_target_id))
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    blocked = frame[frame["episode_id"].astype(str).isin(blocked_ids)][
        ["episode_id", "source", "business_question", "bottleneck_text"]
    ].head(5)
    return moved.to_dict(orient="records"), blocked.to_dict(orient="records")


def _build_variant_result(
    variant: AnchorSimulationVariant,
    frame: pd.DataFrame,
    anchor_df: pd.DataFrame,
    baseline_profiles: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
) -> dict[str, Any]:
    """Evaluate one anchor-level simulation variant."""
    variant_frame = frame.copy()
    variant_frame["variant_persona_id"] = variant.builder(variant_frame).astype(str)
    anchor_eval = anchor_df.drop(columns=["variant_persona_id"], errors="ignore").merge(
        variant_frame[["episode_id", "variant_persona_id"]],
        on="episode_id",
        how="left",
    )
    metrics = evaluate_anchor_subset(anchor_eval, "variant_persona_id", target_id="persona_04")

    variant_profiles = build_cluster_semantic_profiles(
        variant_frame.rename(columns={"variant_persona_id": "persona_id_variant"}),
        persona_column="persona_id_variant",
        profile_label=variant.variant_id,
    )
    overlap_rows = build_overlap_matrix(
        baseline_df=frame[["episode_id", "persona_id_current"]],
        variant_df=variant_frame[["episode_id", "variant_persona_id"]],
        baseline_column="persona_id_current",
        variant_column="variant_persona_id",
        baseline_persona_ids=["persona_04", "persona_01", "persona_05"],
    )
    overlap_df = pd.DataFrame(overlap_rows)
    p4_match = overlap_df[
        (overlap_df["baseline_persona_id"].astype(str) == "persona_04")
        & (overlap_df["variant_persona_id"].astype(str) == "persona_04")
    ]
    crosswalk = frame[["episode_id", "persona_id_current"]].merge(
        variant_frame[["episode_id", "variant_persona_id"]],
        on="episode_id",
        how="inner",
    )
    selected_overlap = _selected_example_overlap(
        persona_examples_df,
        crosswalk,
        baseline_persona_id="persona_04",
        variant_persona_id="persona_04",
    )
    leakage = _persona_01_leakage_pct(crosswalk, "persona_04")
    persona_05_drift_risk = False
    top3 = top_3_cluster_share(variant_frame["variant_persona_id"])
    identity_gate = evaluate_identity_continuity_gate(
        baseline_target_id="persona_04",
        variant_target_id="persona_04",
        baseline_target_best_match="persona_04",
        jaccard_overlap=float(p4_match.iloc[0]["jaccard_overlap"]) if not p4_match.empty else 0.0,
        selected_example_overlap_pct=float(selected_overlap["selected_example_overlap_pct"]),
        positive_recall=float(metrics["positive_anchor_capture_rate"]),
        hard_negative_false_positive_rate=float(metrics["hard_negative_anchor_false_positive_rate"]),
        ambiguous_movement_rate=float(metrics["ambiguous_non_anchor_movement_rate"]),
        raw_reconcile_boost_ambiguous_movement_rate=100.0,
        persona_01_parent_leakage_pct=leakage,
        persona_05_promotion_drift_risk=persona_05_drift_risk,
        semantic_similarity_score=_semantic_similarity(baseline_profiles, variant_profiles, "persona_04", "persona_04"),
    )
    eligibility = anchor_variant_eligibility(
        identity_gate=identity_gate,
        positive_anchor_capture_rate=float(metrics["positive_anchor_capture_rate"]),
        baseline_positive_anchor_capture_rate=0.0,  # overwritten by caller later
        hard_negative_anchor_false_positive_rate=float(metrics["hard_negative_anchor_false_positive_rate"]),
        parent_anchor_retention_rate=float(metrics["parent_anchor_retention_rate"]),
        ambiguous_non_anchor_movement_rate=float(metrics["ambiguous_non_anchor_movement_rate"]),
        baseline_ambiguous_non_anchor_movement_rate=0.0,  # overwritten by caller later
        top_3_share_pct=float(top3),
        persona_05_drift_risk=persona_05_drift_risk,
    )
    moved_examples, blocked_examples = _representative_examples(variant_frame, "variant_persona_id", "persona_04", anchor_eval)
    result = {
        "variant_id": variant.variant_id,
        "description": variant.description,
        **metrics,
        "persona_04_identity_overlap": float(p4_match.iloc[0]["jaccard_overlap"]) if not p4_match.empty else 0.0,
        "selected_example_overlap_pct": float(selected_overlap["selected_example_overlap_pct"]),
        "persona_01_leakage_pct": leakage,
        "persona_05_drift_risk": persona_05_drift_risk,
        "identity_continuity_gate": identity_gate,
        "top_3_cluster_share_simulation": top3,
        "final_usable_persona_count_simulation": estimate_final_usable_persona_count(top3, persona_05_drift_risk),
        **persona_statuses(top3, persona_05_drift_risk),
        "rows_moved_into_persona_04": int(
            (
                variant_frame["persona_id_current"].astype(str).ne("persona_04")
                & variant_frame["variant_persona_id"].astype(str).eq("persona_04")
            ).sum()
        ),
        "representative_moved_examples": moved_examples,
        "representative_blocked_examples": blocked_examples,
        "promotion_drift_flags": build_promotion_drift_flags({"persona_04": "exploratory_bucket", "persona_05": "exploratory_bucket"}, "persona_04"),
        "eligibility": eligibility,
    }
    return result


def build_anchor_simulation_report(root_dir: Path) -> dict[str, Any]:
    """Build the full anchor-level simulation report."""
    bundle, frame = _prepare_frame(root_dir)
    anchor_df = _load_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_anchor_set.csv")
    persona_examples_df = pd.read_csv(root_dir / "data" / "analysis" / "persona_examples.csv").fillna("")
    persona_summary_df = pd.read_csv(root_dir / "data" / "analysis" / "persona_summary.csv").fillna("")
    cluster_stats_df = pd.read_csv(root_dir / "data" / "analysis" / "cluster_stats.csv").fillna("")
    profile_summary = persona_summary_df.merge(
        cluster_stats_df[[column for column in ["persona_id", "dominant_signature", "share_rank"] if column in cluster_stats_df.columns]],
        on="persona_id",
        how="left",
        suffixes=("", "_cluster"),
    )
    baseline_profiles = build_cluster_semantic_profiles(
        bundle["base_df"],
        persona_column="persona_id_current",
        persona_summary_df=profile_summary,
        persona_examples_df=persona_examples_df,
        profile_label="baseline_production",
    )

    baseline_anchor_eval = anchor_df.merge(frame[["episode_id", "persona_id_current"]], on="episode_id", how="left")
    baseline_metrics = evaluate_anchor_subset(baseline_anchor_eval, "persona_id_current", target_id="persona_04")
    baseline_top3 = top_3_cluster_share(frame["persona_id_current"])
    baseline_result = {
        "variant_id": "H",
        "description": "no-op baseline",
        **baseline_metrics,
        "persona_04_identity_overlap": 1.0,
        "selected_example_overlap_pct": 100.0,
        "persona_01_leakage_pct": 0.0,
        "persona_05_drift_risk": False,
        "identity_continuity_gate": evaluate_identity_continuity_gate(
            baseline_target_id="persona_04",
            variant_target_id="persona_04",
            baseline_target_best_match="persona_04",
            jaccard_overlap=1.0,
            selected_example_overlap_pct=100.0,
            positive_recall=float(baseline_metrics["positive_anchor_capture_rate"]),
            hard_negative_false_positive_rate=float(baseline_metrics["hard_negative_anchor_false_positive_rate"]),
            ambiguous_movement_rate=float(baseline_metrics["ambiguous_non_anchor_movement_rate"]),
            raw_reconcile_boost_ambiguous_movement_rate=100.0,
            persona_01_parent_leakage_pct=0.0,
            persona_05_promotion_drift_risk=False,
            semantic_similarity_score=100.0,
            reference_only=True,
        ),
        "top_3_cluster_share_simulation": baseline_top3,
        "final_usable_persona_count_simulation": estimate_final_usable_persona_count(baseline_top3, False),
        **persona_statuses(baseline_top3, False),
        "rows_moved_into_persona_04": 0,
        "representative_moved_examples": [],
        "representative_blocked_examples": [],
    }

    variant_results: list[dict[str, Any]] = []
    for variant in _variant_definitions():
        if variant.variant_id == "H":
            continue
        result = _build_variant_result(variant, frame, anchor_df, baseline_profiles, persona_examples_df)
        result["eligibility"] = anchor_variant_eligibility(
            identity_gate=result["identity_continuity_gate"],
            positive_anchor_capture_rate=float(result["positive_anchor_capture_rate"]),
            baseline_positive_anchor_capture_rate=float(baseline_result["positive_anchor_capture_rate"]),
            hard_negative_anchor_false_positive_rate=float(result["hard_negative_anchor_false_positive_rate"]),
            parent_anchor_retention_rate=float(result["parent_anchor_retention_rate"]),
            ambiguous_non_anchor_movement_rate=float(result["ambiguous_non_anchor_movement_rate"]),
            baseline_ambiguous_non_anchor_movement_rate=float(baseline_result["ambiguous_non_anchor_movement_rate"]),
            top_3_share_pct=float(result["top_3_cluster_share_simulation"]),
            persona_05_drift_risk=bool(result["persona_05_drift_risk"]),
        )
        variant_results.append(result)

    eligible_variants = [row["variant_id"] for row in variant_results if row["eligibility"]["eligible_for_future_implementation"]]

    return {
        "anchor_set_path": str(root_dir / "artifacts" / "curation" / "reconciliation_signoff_anchor_set.csv"),
        "baseline_persona_04_identity_profile": baseline_profiles[
            baseline_profiles["persona_id"].astype(str) == "persona_04"
        ].to_dict(orient="records"),
        "baseline_reference": baseline_result,
        "variant_results": variant_results,
        "eligible_variants": eligible_variants,
        "recommendation": (
            "No anchor-level variant is implementation-eligible yet; keep production unchanged and use the anchor set for future anchor-construction simulations."
            if not eligible_variants
            else "At least one anchor-level variant is eligible for future implementation review, but do not implement automatically."
        ),
    }


def main() -> None:
    """Run the anchor-level simulation and write one report artifact."""
    report = build_anchor_simulation_report(ROOT_DIR)
    output_path = ROOT_DIR / "artifacts" / "curation" / "reconciliation_signoff_anchor_simulation.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "report_path": str(output_path),
                "eligible_variants": report["eligible_variants"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
