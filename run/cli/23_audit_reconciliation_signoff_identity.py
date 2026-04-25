"""Audit identity stability for reconciliation/signoff simulation targets."""

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
    select_reconciliation_like_persona,
)


def _load_variant_cli_module():
    """Load the variant evaluation CLI module for simulation helpers."""
    cli_path = ROOT_DIR / "run" / "cli" / "21_evaluate_reconciliation_signoff_variants.py"
    spec = importlib.util.spec_from_file_location("reconciliation_variant_cli", cli_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load reconciliation variant CLI module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _top_destinations(crosswalk: pd.DataFrame, baseline_persona_id: str, limit: int = 5) -> list[dict[str, Any]]:
    """Return the top destination personas for one baseline persona."""
    subset = crosswalk[crosswalk["persona_id_current"].astype(str) == str(baseline_persona_id)]
    counts = subset["variant_persona_id"].astype(str).value_counts().head(limit)
    total = max(len(subset), 1)
    return [
        {
            "variant_persona_id": str(persona_id),
            "count": int(count),
            "share_pct": round((float(count) / float(total)) * 100.0, 1),
        }
        for persona_id, count in counts.items()
    ]


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


def _semantic_continuity(
    baseline_profiles: pd.DataFrame,
    variant_profiles: pd.DataFrame,
    baseline_persona_id: str,
    variant_persona_id: str,
) -> dict[str, Any]:
    """Return a compact semantic continuity readout between two profile rows."""
    base_row = baseline_profiles[baseline_profiles["persona_id"].astype(str) == str(baseline_persona_id)]
    variant_row = variant_profiles[variant_profiles["persona_id"].astype(str) == str(variant_persona_id)]
    if base_row.empty or variant_row.empty:
        return {"semantic_similarity_score": 0.0}
    base = base_row.iloc[0]
    variant = variant_row.iloc[0]
    diffs = [
        abs(float(base.get("validation_share_pct", 0.0)) - float(variant.get("validation_share_pct", 0.0))),
        abs(float(base.get("trust_medium_or_high_share_pct", 0.0)) - float(variant.get("trust_medium_or_high_share_pct", 0.0))),
        abs(float(base.get("manual_reporting_share_pct", 0.0)) - float(variant.get("manual_reporting_share_pct", 0.0))),
        abs(float(base.get("report_speed_share_pct", 0.0)) - float(variant.get("report_speed_share_pct", 0.0))),
    ]
    similarity = round(max(0.0, 100.0 - (sum(diffs) / max(len(diffs), 1))), 1)
    return {
        "semantic_similarity_score": similarity,
        "baseline_dominant_signature": str(base.get("dominant_signature", "")),
        "variant_dominant_signature": str(variant.get("dominant_signature", "")),
    }


def _identity_judgement(
    baseline_persona_id: str,
    best_match_persona_id: str,
    jaccard_overlap: float,
    same_id_overlap: float,
) -> str:
    """Classify whether the identity looks stable, renumbered, or unstable."""
    if best_match_persona_id == baseline_persona_id and jaccard_overlap >= 0.5:
        return "stable_identity"
    if best_match_persona_id != baseline_persona_id and jaccard_overlap >= 0.35 and jaccard_overlap > same_id_overlap:
        return "renumbered_or_shifted_identity"
    return "unstable_identity"


def build_identity_audit(root_dir: Path) -> dict[str, Any]:
    """Build the reconciliation/signoff identity audit artifact."""
    variant_cli = _load_variant_cli_module()
    bundle, frame = variant_cli._prepare_simulation_frame(root_dir)
    variant_def = [item for item in variant_cli._variant_definitions() if item.variant_id == "B"][0]
    variant_frame = variant_cli._apply_variant(bundle, frame, variant_def)

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
    variant_profiles = build_cluster_semantic_profiles(
        variant_frame.rename(columns={"variant_persona_id": "persona_id_variant"}),
        persona_column="persona_id_variant",
        persona_summary_df=profile_summary,
        persona_examples_df=persona_examples,
        profile_label="simulation_variant_B",
    )

    focus_personas = ["persona_01", "persona_04", "persona_05"]
    overlap_rows = build_overlap_matrix(
        baseline_df=bundle["base_df"][["episode_id", "persona_id_current"]],
        variant_df=variant_frame[["episode_id", "variant_persona_id"]],
        baseline_column="persona_id_current",
        variant_column="variant_persona_id",
        baseline_persona_ids=focus_personas,
    )
    overlap_df = pd.DataFrame(overlap_rows)

    crosswalk = bundle["base_df"][["episode_id", "persona_id_current"]].merge(
        variant_frame[["episode_id", "variant_persona_id"]],
        on="episode_id",
        how="inner",
    )

    baseline_dev = pd.read_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_dev.csv")
    baseline_dev = baseline_dev.drop(columns=["persona_id_current"], errors="ignore").merge(
        bundle["base_df"][["episode_id", "persona_id_current"]],
        on="episode_id",
        how="left",
    )
    variant_dev = baseline_dev.drop(columns=["persona_id_current"], errors="ignore").merge(
        variant_frame[["episode_id", "variant_persona_id"]],
        on="episode_id",
        how="left",
    ).rename(columns={"variant_persona_id": "persona_id_current"})

    baseline_selection = select_reconciliation_like_persona(baseline_dev, baseline_profiles, persona_column="persona_id_current")
    variant_selection = select_reconciliation_like_persona(variant_dev, variant_profiles, persona_column="persona_id_current")

    persona_status_lookup = persona_summary.set_index("persona_id")["promotion_status"].astype(str).to_dict()

    focus_map: list[dict[str, Any]] = []
    for persona_id in focus_personas:
        persona_overlaps = overlap_df[overlap_df["baseline_persona_id"].astype(str) == persona_id].copy()
        persona_overlaps = persona_overlaps.sort_values(["jaccard_overlap", "intersection_count"], ascending=[False, False])
        best = persona_overlaps.head(1).to_dict(orient="records")[0] if not persona_overlaps.empty else {}
        same_id_row = persona_overlaps[persona_overlaps["variant_persona_id"].astype(str) == persona_id]
        same_id_overlap = float(same_id_row.iloc[0]["jaccard_overlap"]) if not same_id_row.empty else 0.0
        best_variant_id = str(best.get("variant_persona_id", ""))
        continuity = _semantic_continuity(baseline_profiles, variant_profiles, persona_id, best_variant_id)
        focus_map.append(
            {
                "baseline_persona_id": persona_id,
                "baseline_profile": baseline_profiles[baseline_profiles["persona_id"].astype(str) == persona_id].to_dict(orient="records"),
                "top_variant_destinations": _top_destinations(crosswalk, persona_id),
                "best_match_variant_persona_id": best_variant_id,
                "best_match_overlap": best,
                "selected_example_overlap": _selected_example_overlap(persona_examples, crosswalk, persona_id, best_variant_id),
                **continuity,
                "identity_judgement": _identity_judgement(persona_id, best_variant_id, float(best.get("jaccard_overlap", 0.0)), same_id_overlap),
            }
        )

    patched_attempt_artifact = root_dir / "artifacts" / "curation" / "reconciliation_signoff_release_gate_attempted_patch.json"
    patched_attempt = None
    if patched_attempt_artifact.exists():
        patched_attempt = json.loads(patched_attempt_artifact.read_text(encoding="utf-8"))

    return {
        "baseline_to_simulation_identity_map": focus_map,
        "baseline_to_simulation_overlap_matrix": overlap_rows,
        "baseline_release_gate_selection": baseline_selection,
        "simulation_variant_b_release_gate_selection": variant_selection,
        "baseline_variant_mapping_metadata": {
            "persona_04_like_variant_id": str(bundle["persona_04_like_variant_id"]),
            "persona_01_like_variant_id": str(bundle["persona_01_like_variant_id"]),
        },
        "promotion_drift_flags": build_promotion_drift_flags(persona_status_lookup, variant_selection["selected_persona_id"]),
        "release_gate_target_selection_reason": {
            "baseline": baseline_selection["selection_reason"],
            "simulation_variant_b": variant_selection["selection_reason"],
        },
        "patched_attempt_available": patched_attempt is not None,
        "patched_attempt_artifact_path": str(patched_attempt_artifact),
        "patched_attempt_snapshot": patched_attempt,
        "persona_04_like_identity_stable_enough_for_implementation": False,
        "stability_conclusion": (
            "Not stable enough yet. Simulation variant B maps the reconciliation-like target to a semantically shifted "
            "cluster id, so another production attempt should wait until target identity is anchored semantically."
        ),
    }


def main() -> None:
    """Write the identity audit artifact."""
    audit = build_identity_audit(ROOT_DIR)
    output_path = ROOT_DIR / "artifacts" / "curation" / "reconciliation_signoff_identity_audit.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report_path": str(output_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
