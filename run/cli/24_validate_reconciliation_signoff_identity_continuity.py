"""Validate identity continuity for reconciliation/signoff simulation variants."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.reconciliation_signoff_curation import _load_bundle
from src.analysis.reconciliation_signoff_identity import (
    build_cluster_semantic_profiles,
    build_overlap_matrix,
    build_promotion_drift_flags,
    evaluate_identity_continuity_gate,
    select_reconciliation_like_persona,
)


def _load_variant_cli_module():
    """Load the variant evaluation CLI module for simulation helpers."""
    cli_path = ROOT_DIR / "run" / "cli" / "21_evaluate_reconciliation_signoff_variants.py"
    spec = importlib.util.spec_from_file_location("reconciliation_variant_cli_gate", cli_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load reconciliation variant CLI module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_release_gate() -> dict[str, Any]:
    """Load the current production release-gate report."""
    path = ROOT_DIR / "artifacts" / "curation" / "reconciliation_signoff_release_gate.json"
    if not path.exists():
        raise SystemExit(f"Missing required release gate artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_variant_eval() -> dict[str, Any]:
    """Load the current variant evaluation report."""
    path = ROOT_DIR / "artifacts" / "curation" / "reconciliation_signoff_variant_eval.json"
    if not path.exists():
        raise SystemExit(f"Missing required variant evaluation artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


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
    """Return how much baseline persona_01 leaks into one reconciliation-like target."""
    baseline_p1 = crosswalk[crosswalk["persona_id_current"].astype(str) == "persona_01"].copy()
    if baseline_p1.empty:
        return 0.0
    hits = baseline_p1["variant_persona_id"].astype(str).eq(str(variant_target_id)).sum()
    return round((float(hits) / float(len(baseline_p1))) * 100.0, 1)


def _build_variant_state(
    variant_name: str,
    variant_assignments: pd.Series,
    baseline_frame: pd.DataFrame,
    baseline_profiles: pd.DataFrame,
    persona_examples: pd.DataFrame,
    dev_df: pd.DataFrame,
    raw_ambiguous_rate: float,
    persona_status_lookup: dict[str, str],
) -> dict[str, Any]:
    """Build one identity-continuity evaluation state."""
    variant_frame = baseline_frame.copy()
    variant_frame["variant_persona_id"] = variant_assignments.astype(str)
    variant_profiles = build_cluster_semantic_profiles(
        variant_frame.rename(columns={"variant_persona_id": "persona_id_variant"}),
        persona_column="persona_id_variant",
        profile_label=variant_name,
    )
    variant_dev = dev_df.drop(columns=["persona_id_current"], errors="ignore").merge(
        variant_frame[["episode_id", "variant_persona_id"]],
        on="episode_id",
        how="left",
    ).rename(columns={"variant_persona_id": "persona_id_current"})
    selection = select_reconciliation_like_persona(variant_dev, variant_profiles, persona_column="persona_id_current")

    overlap_rows = build_overlap_matrix(
        baseline_df=baseline_frame[["episode_id", "persona_id_current"]],
        variant_df=variant_frame[["episode_id", "variant_persona_id"]],
        baseline_column="persona_id_current",
        variant_column="variant_persona_id",
        baseline_persona_ids=["persona_04", "persona_05", "persona_01"],
    )
    overlap_df = pd.DataFrame(overlap_rows)
    p4_match = overlap_df[
        (overlap_df["baseline_persona_id"].astype(str) == "persona_04")
        & (overlap_df["variant_persona_id"].astype(str) == str(selection["selected_persona_id"]))
    ]
    best_match = overlap_df[
        overlap_df["baseline_persona_id"].astype(str) == "persona_04"
    ].sort_values(["jaccard_overlap", "intersection_count"], ascending=[False, False]).head(1)
    crosswalk = baseline_frame[["episode_id", "persona_id_current"]].merge(
        variant_frame[["episode_id", "variant_persona_id"]],
        on="episode_id",
        how="inner",
    )
    selected_example_overlap = _selected_example_overlap(
        persona_examples,
        crosswalk,
        baseline_persona_id="persona_04",
        variant_persona_id=str(selection["selected_persona_id"]),
    )
    semantic_similarity = _semantic_similarity(
        baseline_profiles,
        variant_profiles,
        baseline_persona_id="persona_04",
        variant_persona_id=str(selection["selected_persona_id"]),
    )
    drift_risk = (
        str(selection["selected_persona_id"]) != "persona_04"
        and _persona_01_leakage_pct(crosswalk, str(selection["selected_persona_id"])) > 0.0
        and selection["selected_persona_id"] != "persona_05"
    )
    gate = evaluate_identity_continuity_gate(
        baseline_target_id="persona_04",
        variant_target_id=str(selection["selected_persona_id"]),
        baseline_target_best_match=str(best_match.iloc[0]["variant_persona_id"]) if not best_match.empty else "",
        jaccard_overlap=float(p4_match.iloc[0]["jaccard_overlap"]) if not p4_match.empty else 0.0,
        selected_example_overlap_pct=float(selected_example_overlap["selected_example_overlap_pct"]),
        positive_recall=float(selection["candidate_scores"][0]["positive_recall"]) if selection["candidate_scores"] else 0.0,
        hard_negative_false_positive_rate=float(selection["candidate_scores"][0]["hard_negative_false_positive_rate"]) if selection["candidate_scores"] else 0.0,
        ambiguous_movement_rate=float(selection["candidate_scores"][0]["ambiguous_movement_rate"]) if selection["candidate_scores"] else 0.0,
        raw_reconcile_boost_ambiguous_movement_rate=float(raw_ambiguous_rate),
        persona_01_parent_leakage_pct=_persona_01_leakage_pct(crosswalk, str(selection["selected_persona_id"])),
        persona_05_promotion_drift_risk=drift_risk,
        semantic_similarity_score=semantic_similarity,
    )
    return {
        "variant_name": variant_name,
        "variant_reconciliation_target_id": selection["selected_persona_id"],
        "release_gate_target_selection_reason": selection["selection_reason"],
        "baseline_target_to_variant_best_match": str(best_match.iloc[0]["variant_persona_id"]) if not best_match.empty else "",
        "baseline_target_to_variant_target_overlap": p4_match.to_dict(orient="records"),
        "baseline_target_best_match_overlap": best_match.to_dict(orient="records"),
        "selected_example_overlap": selected_example_overlap,
        "persona_01_parent_leakage_pct": _persona_01_leakage_pct(crosswalk, str(selection["selected_persona_id"])),
        "persona_05_promotion_drift_risk": drift_risk,
        "semantic_similarity_score": semantic_similarity,
        "candidate_scores": selection["candidate_scores"],
        "promotion_drift_flags": build_promotion_drift_flags(persona_status_lookup, str(selection["selected_persona_id"])),
        "identity_continuity_gate": gate,
    }


def build_identity_continuity_report(root_dir: Path) -> dict[str, Any]:
    """Build the identity continuity gate report for baseline and simulation variants."""
    release_gate = _load_release_gate()
    variant_eval = _load_variant_eval()
    variant_cli = _load_variant_cli_module()
    bundle, frame = variant_cli._prepare_simulation_frame(root_dir)

    persona_summary = pd.read_csv(root_dir / "data" / "analysis" / "persona_summary.csv")
    cluster_stats = pd.read_csv(root_dir / "data" / "analysis" / "cluster_stats.csv")
    profile_summary = persona_summary.merge(
        cluster_stats[[column for column in ["persona_id", "dominant_signature", "share_rank"] if column in cluster_stats.columns]],
        on="persona_id",
        how="left",
        suffixes=("", "_cluster"),
    )
    persona_examples = pd.read_csv(root_dir / "data" / "analysis" / "persona_examples.csv")
    baseline_profiles = build_cluster_semantic_profiles(
        bundle["base_df"],
        persona_column="persona_id_current",
        persona_summary_df=profile_summary,
        persona_examples_df=persona_examples,
        profile_label="baseline_production",
    )
    persona_status_lookup = persona_summary.set_index("persona_id")["promotion_status"].astype(str).to_dict()
    dev_df = pd.read_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_dev.csv")
    raw_ambiguous_rate = float(
        variant_eval["baseline_vs_reconcile_boost"]["dev"]["reconcile_boost"]["ambiguous_movement_rate"]
    )

    baseline_state = {
        "variant_name": "baseline",
        "variant_reconciliation_target_id": str(release_gate["reconciliation_like_persona_id"]),
        "release_gate_target_selection_reason": str(release_gate["release_gate_target_selection_reason"]),
        "baseline_target_to_variant_best_match": "persona_04",
        "baseline_target_to_variant_target_overlap": [
            {
                "baseline_persona_id": "persona_04",
                "variant_persona_id": "persona_04",
                "jaccard_overlap": 1.0,
                "baseline_retention_pct": 100.0,
            }
        ],
        "selected_example_overlap": {"selected_example_overlap_count": 5, "selected_example_overlap_pct": 100.0},
        "persona_01_parent_leakage_pct": 0.0,
        "persona_05_promotion_drift_risk": False,
        "semantic_similarity_score": 100.0,
        "promotion_drift_flags": build_promotion_drift_flags(persona_status_lookup, "persona_04"),
        "identity_continuity_gate": evaluate_identity_continuity_gate(
            baseline_target_id="persona_04",
            variant_target_id="persona_04",
            baseline_target_best_match="persona_04",
            jaccard_overlap=1.0,
            selected_example_overlap_pct=100.0,
            positive_recall=float(release_gate["dev"]["positive_recall"]),
            hard_negative_false_positive_rate=float(release_gate["dev"]["hard_negative_false_positive_rate"]),
            ambiguous_movement_rate=float(release_gate["dev"]["ambiguous_movement_rate"]),
            raw_reconcile_boost_ambiguous_movement_rate=raw_ambiguous_rate,
            persona_01_parent_leakage_pct=0.0,
            persona_05_promotion_drift_risk=False,
            semantic_similarity_score=100.0,
            reference_only=True,
        ),
    }

    states = [baseline_state]
    raw_state = _build_variant_state(
        variant_name="raw_reconcile_boost",
        variant_assignments=frame["reconcile_boost_persona_id"],
        baseline_frame=bundle["base_df"],
        baseline_profiles=baseline_profiles,
        persona_examples=persona_examples,
        dev_df=dev_df,
        raw_ambiguous_rate=raw_ambiguous_rate,
        persona_status_lookup=persona_status_lookup,
    )
    states.append(raw_state)

    for variant in variant_cli._variant_definitions():
        variant_frame = variant_cli._apply_variant(bundle, frame, variant)
        state = _build_variant_state(
            variant_name=f"variant_{variant.variant_id}",
            variant_assignments=variant_frame["variant_persona_id"],
            baseline_frame=bundle["base_df"],
            baseline_profiles=baseline_profiles,
            persona_examples=persona_examples,
            dev_df=dev_df,
            raw_ambiguous_rate=raw_ambiguous_rate,
            persona_status_lookup=persona_status_lookup,
        )
        states.append(state)

    eligible = [
        state["variant_name"]
        for state in states
        if not state["identity_continuity_gate"].get("reference_only")
        and state["identity_continuity_gate"].get("eligible_for_future_implementation")
    ]
    return {
        "identity_continuity_gate_definition": {
            "baseline_reconciliation_target_id": "persona_04",
            "checks": [
                "baseline_target_matches_best_match",
                "jaccard_overlap_high",
                "selected_example_overlap_high",
                "positive_recall_high_enough",
                "hard_negative_fp_within_guarded_range",
                "ambiguous_movement_improves_vs_raw_reconcile_boost",
                "persona_01_leakage_below_ceiling",
                "persona_05_promotion_drift_absent",
                "target_change_is_not_semantic_drift",
            ],
            "thresholds": {
                "jaccard_overlap_min": 0.6,
                "selected_example_overlap_pct_min": 80.0,
                "positive_recall_min": 80.0,
                "hard_negative_false_positive_rate_max": 16.7,
                "persona_01_parent_leakage_pct_max": 5.0,
            },
        },
        "states": states,
        "eligible_variants": eligible,
        "recommendation": (
            "No production patch should be attempted until a variant passes this identity-continuity gate. "
            "If none pass, the next structural move should happen before production patching."
        ),
    }


def main() -> None:
    """Write the identity continuity gate report artifact."""
    report = build_identity_continuity_report(ROOT_DIR)
    output_path = ROOT_DIR / "artifacts" / "curation" / "reconciliation_signoff_identity_continuity_gate.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report_path": str(output_path), "eligible_variants": report["eligible_variants"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
