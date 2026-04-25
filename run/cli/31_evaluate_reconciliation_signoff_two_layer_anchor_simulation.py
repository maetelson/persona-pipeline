"""Run the final bounded two-layer reconciliation/signoff anchor simulation."""

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

from src.analysis.reconciliation_signoff_identity import build_cluster_semantic_profiles, build_overlap_matrix
from src.analysis.reconciliation_signoff_two_layer_simulation import (
    estimated_final_usable_persona_count,
    evaluate_label_subset,
    top_3_cluster_share,
    two_layer_variant_decision,
)


def _load_variant_cli_module():
    """Load the existing reconcile variant helpers for reusable features."""
    cli_path = ROOT_DIR / "run" / "cli" / "21_evaluate_reconciliation_signoff_variants.py"
    spec = importlib.util.spec_from_file_location("two_layer_anchor_variant_cli", cli_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load reconciliation variant CLI module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class TwoLayerVariant:
    """One bounded two-layer simulation variant."""

    variant_id: str
    description: str
    builder: Callable[[pd.DataFrame], pd.Series]


def _load_csv(path: Path) -> pd.DataFrame:
    """Load one required CSV artifact."""
    if not path.exists():
        raise SystemExit(f"Missing required artifact: {path}")
    return pd.read_csv(path).fillna("")


def _selected_example_overlap(persona_examples_df: pd.DataFrame, crosswalk: pd.DataFrame) -> dict[str, Any]:
    """Return overlap of baseline persona_04 selected examples with variant persona_04."""
    baseline_examples = persona_examples_df[
        (persona_examples_df["persona_id"].astype(str) == "persona_04")
        & (persona_examples_df["selection_decision"].astype(str) == "selected")
    ].copy()
    example_ids = set(baseline_examples["episode_id"].astype(str).tolist())
    if not example_ids:
        return {"selected_example_overlap_count": 0, "selected_example_overlap_pct": 0.0}
    moved = crosswalk[
        crosswalk["episode_id"].astype(str).isin(example_ids)
        & crosswalk["variant_persona_id"].astype(str).eq("persona_04")
    ]
    return {
        "selected_example_overlap_count": int(len(moved)),
        "selected_example_overlap_pct": round((len(moved) / len(example_ids)) * 100.0, 1),
    }


def _prepare_frame(root_dir: Path) -> pd.DataFrame:
    """Build one reusable simulation frame with bounded variant features."""
    variant_cli = _load_variant_cli_module()
    bundle, frame = variant_cli._prepare_simulation_frame(root_dir)
    frame = frame.copy()
    frame["episode_id"] = frame["episode_id"].astype(str)
    frame["persona_id_current"] = frame["persona_id_current"].astype(str)
    frame["is_baseline_persona_04"] = frame["persona_id_current"].eq("persona_04")
    frame["is_baseline_persona_01"] = frame["persona_id_current"].eq("persona_01")
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
    frame["high_trust_validation_profile"] = (
        frame["analysis_goal"].eq("validate_numbers")
        & frame["workflow_stage"].eq("validation")
        & frame["bottleneck_type"].eq("data_quality")
        & frame["trust_strong"]
    )
    _ = bundle
    return frame


def _build_variant_definitions(
    expansion_positive_ids: set[str],
    parent_ids: set[str],
    hard_negative_ids: set[str],
    ambiguous_ids: set[str],
) -> list[TwoLayerVariant]:
    """Return the final bounded two-layer simulation family."""

    def baseline(frame: pd.DataFrame) -> pd.Series:
        return frame["persona_id_current"].astype(str)

    def variant_b(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        result.loc[frame["episode_id"].isin(expansion_positive_ids)] = "persona_04"
        return result

    def variant_c(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = frame["episode_id"].isin(expansion_positive_ids)
        result.loc[move_mask] = "persona_04"
        result.loc[frame["is_baseline_persona_04"]] = "persona_04"
        return result

    def variant_d(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = frame["episode_id"].isin(expansion_positive_ids) & ~frame["episode_id"].isin(parent_ids)
        result.loc[move_mask] = "persona_04"
        return result

    def variant_e(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = frame["episode_id"].isin(expansion_positive_ids) & ~frame["episode_id"].isin(hard_negative_ids)
        result.loc[move_mask] = "persona_04"
        return result

    def variant_f(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = frame["episode_id"].isin(expansion_positive_ids) & ~frame["episode_id"].isin(ambiguous_ids)
        result.loc[move_mask] = "persona_04"
        return result

    def variant_g(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["episode_id"].isin(expansion_positive_ids)
            | (
                frame["is_baseline_persona_01"]
                & frame["anchor_similarity_score"].ge(10)
                & frame["discrepancy_phrase_hits"].ge(1)
                & frame["helpdesk_phrase_hits"].eq(0)
                & ~frame["manual_reporting_like"]
                & ~frame["episode_id"].isin(parent_ids | hard_negative_ids | ambiguous_ids)
            )
        )
        result.loc[move_mask] = "persona_04"
        return result

    def variant_h(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["episode_id"].isin(expansion_positive_ids)
            | (
                frame["is_baseline_persona_01"]
                & frame["anchor_similarity_score"].ge(8)
                & frame["trust_strong"]
                & frame["validation_axis_present"]
                & frame["helpdesk_phrase_hits"].le(1)
                & ~frame["episode_id"].isin(parent_ids | hard_negative_ids | ambiguous_ids)
            )
        )
        result.loc[move_mask] = "persona_04"
        return result

    return [
        TwoLayerVariant("A", "no-op baseline", baseline),
        TwoLayerVariant("B", "persona_04 expansion using expansion-positive anchors only", variant_b),
        TwoLayerVariant("C", "persona_04 expansion with identity-anchor protection", variant_c),
        TwoLayerVariant("D", "persona_04 expansion with parent-retention guard", variant_d),
        TwoLayerVariant("E", "persona_04 expansion with hard-negative block", variant_e),
        TwoLayerVariant("F", "persona_04 expansion with ambiguous dampening", variant_f),
        TwoLayerVariant("G", "combined conservative variant", variant_g),
        TwoLayerVariant("H", "combined aggressive-but-guarded variant", variant_h),
    ]


def _representative_examples(
    variant_frame: pd.DataFrame,
    identity_eval: pd.DataFrame,
    expansion_eval: pd.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return representative moved and blocked examples."""
    moved = variant_frame[
        variant_frame["persona_id_current"].astype(str).ne("persona_04")
        & variant_frame["variant_persona_id"].astype(str).eq("persona_04")
    ][["episode_id", "source", "business_question", "bottleneck_text"]].head(5)

    blocked_ids = set(
        expansion_eval[
            expansion_eval["expansion_label"].astype(str).isin(
                {
                    "expansion_hard_negative_block",
                    "expansion_parent_should_stay_persona_01",
                    "expansion_ambiguous_do_not_anchor",
                }
            )
            & ~expansion_eval["variant_persona_id"].astype(str).eq("persona_04")
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    blocked_ids |= set(
        identity_eval[
            identity_eval["anchor_label"].astype(str).isin(
                {
                    "anchor_hard_negative",
                    "anchor_parent_reporting_packager",
                    "non_anchor_ambiguous",
                }
            )
            & ~identity_eval["variant_persona_id"].astype(str).eq("persona_04")
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    blocked = variant_frame[variant_frame["episode_id"].astype(str).isin(blocked_ids)][
        ["episode_id", "source", "business_question", "bottleneck_text"]
    ].head(5)
    return moved.to_dict(orient="records"), blocked.to_dict(orient="records")


def build_two_layer_report(root_dir: Path) -> dict[str, Any]:
    """Build the final bounded two-layer simulation report."""
    frame = _prepare_frame(root_dir)
    identity_anchor_df = _load_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_anchor_set.csv")
    expansion_anchor_df = _load_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_expansion_anchor_set.csv")
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
        frame,
        persona_column="persona_id_current",
        persona_summary_df=profile_summary,
        persona_examples_df=persona_examples_df,
        profile_label="baseline_production",
    )

    expansion_positive_ids = set(
        expansion_anchor_df[
            expansion_anchor_df["expansion_label"].astype(str) == "expansion_positive_should_join_persona_04"
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    parent_ids = set(
        expansion_anchor_df[
            expansion_anchor_df["expansion_label"].astype(str) == "expansion_parent_should_stay_persona_01"
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    hard_negative_ids = set(
        expansion_anchor_df[
            expansion_anchor_df["expansion_label"].astype(str) == "expansion_hard_negative_block"
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    ambiguous_ids = set(
        expansion_anchor_df[
            expansion_anchor_df["expansion_label"].astype(str) == "expansion_ambiguous_do_not_anchor"
        ]["episode_id"]
        .astype(str)
        .tolist()
    )

    results: list[dict[str, Any]] = []
    for variant in _build_variant_definitions(expansion_positive_ids, parent_ids, hard_negative_ids, ambiguous_ids):
        variant_frame = frame.copy()
        variant_frame["variant_persona_id"] = variant.builder(variant_frame).astype(str)

        identity_eval = identity_anchor_df.merge(
            variant_frame[["episode_id", "variant_persona_id"]],
            on="episode_id",
            how="left",
        )
        expansion_eval = expansion_anchor_df.merge(
            variant_frame[["episode_id", "variant_persona_id"]],
            on="episode_id",
            how="left",
        )
        identity_metrics = evaluate_label_subset(
            identity_eval,
            label_column="anchor_label",
            positive_label="anchor_positive_reconciliation_signoff",
            hard_negative_label="anchor_hard_negative",
            parent_label="anchor_parent_reporting_packager",
            ambiguous_label="non_anchor_ambiguous",
            persona_column="variant_persona_id",
            target_persona_id="persona_04",
        )
        expansion_metrics = evaluate_label_subset(
            expansion_eval,
            label_column="expansion_label",
            positive_label="expansion_positive_should_join_persona_04",
            hard_negative_label="expansion_hard_negative_block",
            parent_label="expansion_parent_should_stay_persona_01",
            ambiguous_label="expansion_ambiguous_do_not_anchor",
            persona_column="variant_persona_id",
            target_persona_id="persona_04",
        )
        overlap_df = pd.DataFrame(
            build_overlap_matrix(
                baseline_df=frame[["episode_id", "persona_id_current"]],
                variant_df=variant_frame[["episode_id", "variant_persona_id"]],
                baseline_column="persona_id_current",
                variant_column="variant_persona_id",
                baseline_persona_ids=["persona_04", "persona_01", "persona_05"],
            )
        )
        p4_overlap = overlap_df[
            (overlap_df["baseline_persona_id"].astype(str) == "persona_04")
            & (overlap_df["variant_persona_id"].astype(str) == "persona_04")
        ]
        crosswalk = frame[["episode_id", "persona_id_current"]].merge(
            variant_frame[["episode_id", "variant_persona_id"]],
            on="episode_id",
            how="inner",
        )
        selected_overlap = _selected_example_overlap(persona_examples_df, crosswalk)
        persona_01_leakage = round(
            (
                crosswalk[
                    crosswalk["persona_id_current"].astype(str).eq("persona_01")
                    & crosswalk["variant_persona_id"].astype(str).eq("persona_04")
                ].shape[0]
                / max(crosswalk[crosswalk["persona_id_current"].astype(str).eq("persona_01")].shape[0], 1)
            )
            * 100.0,
            1,
        )
        persona_05_drift_risk = False
        top3 = top_3_cluster_share(variant_frame["variant_persona_id"])
        decision = two_layer_variant_decision(
            identity_overlap=float(p4_overlap.iloc[0]["jaccard_overlap"]) if not p4_overlap.empty else 0.0,
            selected_example_overlap_pct=float(selected_overlap["selected_example_overlap_pct"]),
            identity_positive_capture_rate=float(identity_metrics["positive_capture_rate"]),
            identity_hard_negative_fp_rate=float(identity_metrics["hard_negative_false_positive_rate"]),
            identity_parent_retention_rate=float(identity_metrics["parent_retention_rate"]),
            expansion_positive_capture_rate=float(expansion_metrics["positive_capture_rate"]),
            baseline_expansion_positive_capture_rate=0.0,
            expansion_hard_negative_fp_rate=float(expansion_metrics["hard_negative_false_positive_rate"]),
            expansion_parent_retention_rate=float(expansion_metrics["parent_retention_rate"]),
            expansion_ambiguous_movement_rate=float(expansion_metrics["ambiguous_movement_rate"]),
            persona_01_leakage_pct=float(persona_01_leakage),
            persona_05_drift_risk=persona_05_drift_risk,
            top_3_share_pct=float(top3),
        )
        moved_examples, blocked_examples = _representative_examples(variant_frame, identity_eval, expansion_eval)
        results.append(
            {
                "variant_id": variant.variant_id,
                "description": variant.description,
                "identity_anchor_pass": bool(decision["identity_anchor_pass"]),
                "expansion_anchor_pass": bool(decision["expansion_anchor_pass"]),
                "persona_04_identity_overlap": float(p4_overlap.iloc[0]["jaccard_overlap"]) if not p4_overlap.empty else 0.0,
                "selected_example_overlap": selected_overlap,
                "identity_metrics": identity_metrics,
                "expansion_metrics": expansion_metrics,
                "persona_01_leakage_pct": persona_01_leakage,
                "persona_05_drift_risk": persona_05_drift_risk,
                "top_3_cluster_share": top3,
                "final_usable_persona_count_simulation": estimated_final_usable_persona_count(top3, persona_05_drift_risk),
                "persona_04_would_become_unblocked": top3 < 80.0,
                "persona_05_remains_blocked": not persona_05_drift_risk,
                "rows_moved_into_persona_04_like_target": int(
                    (
                        variant_frame["persona_id_current"].astype(str).ne("persona_04")
                        & variant_frame["variant_persona_id"].astype(str).eq("persona_04")
                    ).sum()
                ),
                "representative_moved_examples": moved_examples,
                "representative_blocked_examples": blocked_examples,
                "decision": decision,
            }
        )

    baseline = next(result for result in results if result["variant_id"] == "A")
    for result in results:
        if result["variant_id"] == "A":
            continue
        result["decision"] = two_layer_variant_decision(
            identity_overlap=float(result["persona_04_identity_overlap"]),
            selected_example_overlap_pct=float(result["selected_example_overlap"]["selected_example_overlap_pct"]),
            identity_positive_capture_rate=float(result["identity_metrics"]["positive_capture_rate"]),
            identity_hard_negative_fp_rate=float(result["identity_metrics"]["hard_negative_false_positive_rate"]),
            identity_parent_retention_rate=float(result["identity_metrics"]["parent_retention_rate"]),
            expansion_positive_capture_rate=float(result["expansion_metrics"]["positive_capture_rate"]),
            baseline_expansion_positive_capture_rate=float(baseline["expansion_metrics"]["positive_capture_rate"]),
            expansion_hard_negative_fp_rate=float(result["expansion_metrics"]["hard_negative_false_positive_rate"]),
            expansion_parent_retention_rate=float(result["expansion_metrics"]["parent_retention_rate"]),
            expansion_ambiguous_movement_rate=float(result["expansion_metrics"]["ambiguous_movement_rate"]),
            persona_01_leakage_pct=float(result["persona_01_leakage_pct"]),
            persona_05_drift_risk=bool(result["persona_05_drift_risk"]),
            top_3_share_pct=float(result["top_3_cluster_share"]),
        )

    eligible_variants = [
        result["variant_id"]
        for result in results
        if result["decision"]["future_production_patch_candidate"]
    ]
    return {
        "identity_anchor_set_path": str(root_dir / "artifacts" / "curation" / "reconciliation_signoff_anchor_set.csv"),
        "expansion_anchor_set_path": str(root_dir / "artifacts" / "curation" / "reconciliation_signoff_expansion_anchor_set.csv"),
        "baseline_two_layer_metrics": baseline,
        "variant_results": results,
        "future_production_patch_candidates": eligible_variants,
        "single_recommended_non_clustering_path": (
            "workbook policy redesign" if not eligible_variants else ""
        ),
    }


def main() -> None:
    """Run the final bounded two-layer simulation and write one report artifact."""
    report = build_two_layer_report(ROOT_DIR)
    output_path = ROOT_DIR / "artifacts" / "curation" / "reconciliation_signoff_two_layer_anchor_simulation.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "report_path": str(output_path),
                "future_production_patch_candidates": report["future_production_patch_candidates"],
                "single_recommended_non_clustering_path": report["single_recommended_non_clustering_path"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
