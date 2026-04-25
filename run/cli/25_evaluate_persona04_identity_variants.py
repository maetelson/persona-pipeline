"""Evaluate identity-preserving persona_04 simulation variants without changing production."""

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

from src.analysis.reconciliation_signoff_curation import _load_bundle
from src.analysis.reconciliation_signoff_identity import (
    build_cluster_semantic_profiles,
    build_promotion_drift_flags,
    build_overlap_matrix,
    evaluate_identity_continuity_gate,
    select_reconciliation_like_persona,
)


def _load_variant_cli_module():
    """Load the existing reconcile variant CLI helpers."""
    cli_path = ROOT_DIR / "run" / "cli" / "21_evaluate_reconciliation_signoff_variants.py"
    spec = importlib.util.spec_from_file_location("reconciliation_variant_cli_identity_preserving", cli_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load reconciliation variant CLI module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class IdentityPreservingVariant:
    """One simulation-only persona_04 identity-preserving variant."""

    variant_id: str
    description: str
    builder: Callable[[pd.DataFrame], pd.Series]


def _load_csv(path: Path) -> pd.DataFrame:
    """Load one required CSV artifact."""
    if not path.exists():
        raise SystemExit(f"Missing required artifact: {path}")
    return pd.read_csv(path)


def _share(numerator: int, denominator: int) -> float:
    """Return one rounded percentage."""
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 1)


def _top_3_share(assignments: pd.Series) -> float:
    """Compute top-3 share for one assignment series."""
    counts = assignments.astype(str).value_counts()
    return _share(int(counts.head(3).sum()), int(len(assignments)))


def _persona04_selected_example_ids(root_dir: Path) -> set[str]:
    """Return baseline selected example ids for persona_04."""
    persona_examples = pd.read_csv(root_dir / "data" / "analysis" / "persona_examples.csv")
    selected = persona_examples[
        (persona_examples["persona_id"].astype(str) == "persona_04")
        & (persona_examples["selection_decision"].astype(str) == "selected")
    ]
    return set(selected["episode_id"].astype(str).tolist())


def _prepare_frame(root_dir: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    """Build one reusable simulation frame anchored on baseline persona_04 identity."""
    variant_cli = _load_variant_cli_module()
    bundle, frame = variant_cli._prepare_simulation_frame(root_dir)
    frame = frame.copy()
    frame["episode_id"] = frame["episode_id"].astype(str)
    frame["persona_id_current"] = frame["persona_id_current"].astype(str)
    frame["is_baseline_persona_04"] = frame["persona_id_current"].eq("persona_04")
    frame["is_baseline_persona_01"] = frame["persona_id_current"].eq("persona_01")

    selected_example_ids = _persona04_selected_example_ids(root_dir)
    frame["is_persona04_selected_example"] = frame["episode_id"].isin(selected_example_ids)
    frame["high_conf_validation"] = (
        frame["analysis_goal"].eq("validate_numbers")
        & frame["workflow_stage"].eq("validation")
        & frame["bottleneck_type"].eq("data_quality")
        & frame["trust_validation_need"].isin(["high", "medium"])
    )
    frame["hard_negative_like"] = frame["helpdesk_phrase_hits"].ge(1)
    frame["manual_reporting_heavy"] = (
        frame["analysis_goal"].eq("report_speed")
        & frame["workflow_stage"].eq("reporting")
        & frame["bottleneck_type"].eq("manual_reporting")
    )
    frame["persona04_profile_score"] = (
        frame["analysis_goal"].eq("validate_numbers").astype(int) * 3
        + frame["workflow_stage"].eq("validation").astype(int) * 2
        + frame["bottleneck_type"].eq("data_quality").astype(int) * 2
        + frame["trust_validation_need"].isin(["high", "medium"]).astype(int) * 2
        + frame["has_q_validate"].astype(int) * 2
        + frame["has_p_data_quality"].astype(int) * 2
        + frame["discrepancy_phrase_hits"].clip(upper=2)
        - frame["helpdesk_phrase_hits"].clip(upper=2) * 2
        - frame["manual_reporting_heavy"].astype(int) * 2
    ).astype(int)
    return bundle, frame


def _identity_preserving_variants() -> list[IdentityPreservingVariant]:
    """Return the next simulation family centered on persona_04 identity preservation."""

    def baseline_variant(frame: pd.DataFrame) -> pd.Series:
        return frame["persona_id_current"].astype(str)

    def variant_a(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        raw_target = frame["reconcile_boost_persona_id"].astype(str)
        move_mask = (
            frame["is_baseline_persona_01"]
            & raw_target.ne(frame["persona_id_current"].astype(str))
            & frame["high_conf_validation"]
            & ~frame["hard_negative_like"]
        )
        result.loc[move_mask] = "persona_04"
        result.loc[frame["is_baseline_persona_04"]] = "persona_04"
        return result

    def variant_b(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & (frame["persona04_profile_score"] >= 10)
            & (frame["is_persona04_selected_example"] | frame["high_conf_validation"] | frame["discrepancy_phrase_hits"].ge(2))
            & ~frame["hard_negative_like"]
        )
        result.loc[move_mask] = "persona_04"
        return result

    def variant_c(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & (frame["persona04_profile_score"] >= 9)
            & frame["trust_strong"]
            & frame["has_q_validate"]
            & ~frame["manual_reporting_heavy"]
            & ~frame["hard_negative_like"]
        )
        result.loc[move_mask] = "persona_04"
        return result

    def variant_d(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        candidates = frame[
            frame["is_baseline_persona_01"]
            & (frame["persona04_profile_score"] >= 9)
            & frame["trust_strong"]
            & ~frame["hard_negative_like"]
        ].copy()
        for _, group in candidates.groupby("source", dropna=False):
            keep_n = max(1, int(round(len(group) * 0.35)))
            keep_ids = set(
                group.sort_values(["persona04_profile_score", "discrepancy_phrase_hits", "episode_id"], ascending=[False, False, True])
                .head(keep_n)["episode_id"]
                .astype(str)
                .tolist()
            )
            result.loc[result.index.isin(group.index) & frame["episode_id"].isin(keep_ids)] = "persona_04"
        return result

    def variant_e(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & (frame["persona04_profile_score"] >= 9)
            & frame["trust_strong"]
            & frame["validation_axis_present"]
            & ~frame["manual_reporting_like"]
            & ~frame["hard_negative_like"]
        )
        result.loc[move_mask] = "persona_04"
        return result

    def variant_f(frame: pd.DataFrame) -> pd.Series:
        result = frame["persona_id_current"].astype(str).copy()
        move_mask = (
            frame["is_baseline_persona_01"]
            & (frame["persona04_profile_score"] >= 11)
            & frame["trust_strong"]
            & frame["validation_axis_present"]
            & frame["discrepancy_phrase_hits"].ge(1)
            & frame["helpdesk_phrase_hits"].eq(0)
            & ~frame["manual_reporting_heavy"]
        )
        result.loc[move_mask] = "persona_04"
        return result

    return [
        IdentityPreservingVariant("G", "no-op baseline reference", baseline_variant),
        IdentityPreservingVariant("A", "pre-merge guard keeping persona_04 anchors stable", variant_a),
        IdentityPreservingVariant("B", "persona_04 anchor-protection using selected examples / high-confidence validation rows", variant_b),
        IdentityPreservingVariant("C", "identity-preserving split allowing persona_01 rows into persona_04 only on semantic profile match", variant_c),
        IdentityPreservingVariant("D", "source-normalized persona_04 expansion", variant_d),
        IdentityPreservingVariant("E", "stricter merge-conflict style guard against reporting/report-speed absorption", variant_e),
        IdentityPreservingVariant("F", "conservative persona_04 expansion with hard-negative phrase penalty", variant_f),
    ]


def _evaluate_subset(curated_df: pd.DataFrame, persona_column: str, reconciliation_target_id: str) -> dict[str, Any]:
    """Evaluate one curated subset against one assignment column."""
    positives = curated_df[curated_df["curated_label"].astype(str) == "reconciliation_signoff_positive"]
    parents = curated_df[curated_df["curated_label"].astype(str) == "reporting_packager_parent"]
    hard_negatives = curated_df[curated_df["curated_label"].astype(str) == "hard_negative"]
    ambiguous = curated_df[curated_df["curated_label"].astype(str) == "ambiguous_boundary"]
    positive_hits = int(positives[persona_column].astype(str).eq(reconciliation_target_id).sum())
    parent_hits = int(parents[persona_column].astype(str).eq("persona_01").sum())
    hard_negative_hits = int(hard_negatives[persona_column].astype(str).eq(reconciliation_target_id).sum())
    ambiguous_hits = int(ambiguous[persona_column].astype(str).eq(reconciliation_target_id).sum())
    return {
        "positive_recall": _share(positive_hits, len(positives)),
        "parent_retention": _share(parent_hits, len(parents)),
        "hard_negative_false_positive_rate": _share(hard_negative_hits, len(hard_negatives)),
        "ambiguous_movement_rate": _share(ambiguous_hits, len(ambiguous)),
        "parent_examples_wrongly_pulled_out_of_persona_01": int((~parents[persona_column].astype(str).eq("persona_01")).sum()),
        "positives_moved_to_persona_04_like": positive_hits,
    }


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
        "selected_example_overlap_pct": _share(len(moved), len(example_ids)),
    }


def _semantic_similarity(
    baseline_profiles: pd.DataFrame,
    variant_profiles: pd.DataFrame,
    baseline_persona_id: str,
    variant_persona_id: str,
) -> float:
    """Return one compact semantic similarity score."""
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
    """Return baseline persona_01 leakage into one target."""
    baseline_p1 = crosswalk[crosswalk["persona_id_current"].astype(str) == "persona_01"].copy()
    if baseline_p1.empty:
        return 0.0
    hits = baseline_p1["variant_persona_id"].astype(str).eq(str(variant_target_id)).sum()
    return _share(int(hits), len(baseline_p1))


def _persona04_unblocked(top_3_share: float, gate_passed: bool) -> bool:
    """Return whether persona_04 would be closer to unblocked under simulation."""
    return gate_passed and top_3_share < 80.0


def _estimated_final_usable_count(top_3_share: float, gate_passed: bool, persona_05_drift_risk: bool) -> int:
    """Return one conservative simulated final usable count."""
    if gate_passed and top_3_share < 80.0 and not persona_05_drift_risk:
        return 4
    return 3


def build_identity_preserving_report(root_dir: Path) -> dict[str, Any]:
    """Build the identity-preserving simulation family report."""
    release_gate = json.loads((root_dir / "artifacts" / "curation" / "reconciliation_signoff_release_gate.json").read_text(encoding="utf-8"))
    identity_gate = json.loads((root_dir / "artifacts" / "curation" / "reconciliation_signoff_identity_continuity_gate.json").read_text(encoding="utf-8"))
    variant_eval = json.loads((root_dir / "artifacts" / "curation" / "reconciliation_signoff_variant_eval.json").read_text(encoding="utf-8"))

    bundle, frame = _prepare_frame(root_dir)
    baseline_target_id = str(release_gate["reconciliation_like_persona_id"])
    baseline_target_profile = build_cluster_semantic_profiles(
        bundle["base_df"],
        persona_column="persona_id_current",
        persona_ids=[baseline_target_id],
        persona_summary_df=pd.read_csv(root_dir / "data" / "analysis" / "persona_summary.csv").merge(
            pd.read_csv(root_dir / "data" / "analysis" / "cluster_stats.csv")[["persona_id", "dominant_signature"]],
            on="persona_id",
            how="left",
            suffixes=("", "_cluster"),
        ),
        persona_examples_df=pd.read_csv(root_dir / "data" / "analysis" / "persona_examples.csv"),
        profile_label="baseline_persona_04",
    )
    persona_examples = pd.read_csv(root_dir / "data" / "analysis" / "persona_examples.csv")
    persona_summary = pd.read_csv(root_dir / "data" / "analysis" / "persona_summary.csv")
    persona_status_lookup = persona_summary.set_index("persona_id")["promotion_status"].astype(str).to_dict()
    dev_df = _load_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_dev.csv")
    eval_locked_df = _load_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_eval_locked.csv")

    raw_ambiguous_rate = float(variant_eval["baseline_vs_reconcile_boost"]["dev"]["reconcile_boost"]["ambiguous_movement_rate"])
    baseline_profiles = build_cluster_semantic_profiles(
        bundle["base_df"],
        persona_column="persona_id_current",
        persona_summary_df=persona_summary.merge(
            pd.read_csv(root_dir / "data" / "analysis" / "cluster_stats.csv")[["persona_id", "dominant_signature"]],
            on="persona_id",
            how="left",
            suffixes=("", "_cluster"),
        ),
        persona_examples_df=persona_examples,
        profile_label="baseline",
    )

    states: list[dict[str, Any]] = []
    for variant in _identity_preserving_variants():
        assignments = variant.builder(frame)
        variant_frame = bundle["base_df"].copy()
        variant_frame["variant_persona_id"] = assignments.astype(str)
        variant_profiles = build_cluster_semantic_profiles(
            variant_frame.rename(columns={"variant_persona_id": "persona_id_variant"}),
            persona_column="persona_id_variant",
            profile_label=f"persona04_identity_{variant.variant_id}",
        )

        dev_variant = dev_df.drop(columns=["persona_id_current"], errors="ignore").merge(
            variant_frame[["episode_id", "variant_persona_id"]],
            on="episode_id",
            how="left",
        ).rename(columns={"variant_persona_id": "persona_id_current"})
        eval_variant = eval_locked_df.drop(columns=["persona_id_current"], errors="ignore").merge(
            variant_frame[["episode_id", "variant_persona_id"]],
            on="episode_id",
            how="left",
        ).rename(columns={"variant_persona_id": "persona_id_current"})

        selection = select_reconciliation_like_persona(dev_variant, variant_profiles, persona_column="persona_id_current")
        target_id = str(selection["selected_persona_id"])
        crosswalk = bundle["base_df"][["episode_id", "persona_id_current"]].merge(
            variant_frame[["episode_id", "variant_persona_id"]],
            on="episode_id",
            how="inner",
        )
        overlap_rows = build_overlap_matrix(
            baseline_df=bundle["base_df"][["episode_id", "persona_id_current"]],
            variant_df=variant_frame[["episode_id", "variant_persona_id"]],
            baseline_column="persona_id_current",
            variant_column="variant_persona_id",
            baseline_persona_ids=[baseline_target_id],
        )
        overlap_df = pd.DataFrame(overlap_rows)
        target_overlap = overlap_df[
            (overlap_df["baseline_persona_id"].astype(str) == baseline_target_id)
            & (overlap_df["variant_persona_id"].astype(str) == target_id)
        ]
        best_match = overlap_df.sort_values(["jaccard_overlap", "intersection_count"], ascending=[False, False]).head(1)
        selected_example_overlap = _selected_example_overlap(
            persona_examples,
            crosswalk,
            baseline_persona_id=baseline_target_id,
            variant_persona_id=target_id,
        )
        semantic_similarity = _semantic_similarity(
            baseline_profiles,
            variant_profiles,
            baseline_persona_id=baseline_target_id,
            variant_persona_id=target_id,
        )
        drift_risk = target_id != baseline_target_id and target_id == "persona_05"
        top_3_share = _top_3_share(variant_frame["variant_persona_id"])
        gate = evaluate_identity_continuity_gate(
            baseline_target_id=baseline_target_id,
            variant_target_id=target_id,
            baseline_target_best_match=str(best_match.iloc[0]["variant_persona_id"]) if not best_match.empty else "",
            jaccard_overlap=float(target_overlap.iloc[0]["jaccard_overlap"]) if not target_overlap.empty else 0.0,
            selected_example_overlap_pct=float(selected_example_overlap["selected_example_overlap_pct"]),
            positive_recall=float(_evaluate_subset(dev_variant, "persona_id_current", target_id)["positive_recall"]),
            hard_negative_false_positive_rate=float(_evaluate_subset(dev_variant, "persona_id_current", target_id)["hard_negative_false_positive_rate"]),
            ambiguous_movement_rate=float(_evaluate_subset(dev_variant, "persona_id_current", target_id)["ambiguous_movement_rate"]),
            raw_reconcile_boost_ambiguous_movement_rate=raw_ambiguous_rate,
            persona_01_parent_leakage_pct=float(_persona_01_leakage_pct(crosswalk, target_id)),
            persona_05_promotion_drift_risk=drift_risk,
            semantic_similarity_score=semantic_similarity,
            reference_only=variant.variant_id == "G",
        )
        states.append(
            {
                "variant_id": variant.variant_id,
                "description": variant.description,
                "variant_reconciliation_target_id": target_id,
                "release_gate_target_selection_reason": selection["selection_reason"],
                "dev_metrics": _evaluate_subset(dev_variant, "persona_id_current", target_id),
                "eval_locked_metrics": _evaluate_subset(eval_variant, "persona_id_current", target_id),
                "persona_01_leakage_pct": _persona_01_leakage_pct(crosswalk, target_id),
                "persona_04_identity_overlap": target_overlap.to_dict(orient="records"),
                "selected_example_overlap": selected_example_overlap,
                "persona_05_promotion_drift_risk": drift_risk,
                "top_3_cluster_share": top_3_share,
                "final_usable_persona_count_simulation": _estimated_final_usable_count(top_3_share, gate["eligible_for_future_implementation"], drift_risk),
                "persona_04_becomes_unblocked": _persona04_unblocked(top_3_share, gate["eligible_for_future_implementation"]),
                "persona_05_remains_blocked": not drift_risk,
                "promotion_drift_flags": build_promotion_drift_flags(persona_status_lookup, target_id),
                "identity_continuity_gate": gate,
            }
        )

    eligible = [state["variant_id"] for state in states if state["identity_continuity_gate"]["eligible_for_future_implementation"]]
    return {
        "baseline_persona_04_identity_profile": baseline_target_profile.to_dict(orient="records"),
        "simulation_family": [
            {"variant_id": variant.variant_id, "description": variant.description}
            for variant in _identity_preserving_variants()
        ],
        "states": states,
        "eligible_variants": eligible,
        "recommendation": (
            "No production patch should be attempted unless one of these persona_04 identity-preserving variants "
            "passes curation eval, release gate, and identity continuity together."
        ),
        "reference_identity_gate": identity_gate["identity_continuity_gate_definition"],
    }


def main() -> None:
    """Write the persona_04 identity-preserving simulation report artifact."""
    report = build_identity_preserving_report(ROOT_DIR)
    output_path = ROOT_DIR / "artifacts" / "curation" / "persona_04_identity_variant_eval.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report_path": str(output_path), "eligible_variants": report["eligible_variants"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
