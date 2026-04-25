"""Validate the current production reconciliation/signoff separation against curation splits."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.reconciliation_signoff_curation import _load_bundle
from src.analysis.reconciliation_signoff_identity import (
    build_cluster_semantic_profiles,
    build_promotion_drift_flags,
    select_reconciliation_like_persona,
)


def _load_csv(path: Path) -> pd.DataFrame:
    """Load one required CSV artifact."""
    if not path.exists():
        raise SystemExit(f"Missing required artifact: {path}")
    return pd.read_csv(path)


def _share(numerator: int, denominator: int) -> float:
    """Return one simple percentage."""
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 1)


def _evaluate(frame: pd.DataFrame, p4_like_persona_id: str, p1_persona_id: str) -> dict[str, float | int]:
    """Evaluate one curated subset against the current production persona assignments."""
    positives = frame[frame["curated_label"].astype(str) == "reconciliation_signoff_positive"]
    parents = frame[frame["curated_label"].astype(str) == "reporting_packager_parent"]
    hard_negatives = frame[frame["curated_label"].astype(str) == "hard_negative"]
    ambiguous = frame[frame["curated_label"].astype(str) == "ambiguous_boundary"]
    return {
        "positive_recall": _share(int(positives["persona_id_current"].astype(str).eq(p4_like_persona_id).sum()), len(positives)),
        "parent_retention": _share(int(parents["persona_id_current"].astype(str).eq(p1_persona_id).sum()), len(parents)),
        "hard_negative_false_positive_rate": _share(int(hard_negatives["persona_id_current"].astype(str).eq(p4_like_persona_id).sum()), len(hard_negatives)),
        "ambiguous_movement_rate": _share(int(ambiguous["persona_id_current"].astype(str).eq(p4_like_persona_id).sum()), len(ambiguous)),
        "parent_examples_wrongly_pulled_out_of_persona_01": int((~parents["persona_id_current"].astype(str).eq(p1_persona_id)).sum()),
        "positives_moved_to_persona_04_like": int(positives["persona_id_current"].astype(str).eq(p4_like_persona_id).sum()),
    }


def main() -> None:
    """Evaluate current production assignments on dev and locked eval splits."""
    curation_dir = ROOT_DIR / "artifacts" / "curation"
    dev_df = _load_csv(curation_dir / "reconciliation_signoff_dev.csv")
    eval_locked_df = _load_csv(curation_dir / "reconciliation_signoff_eval_locked.csv")
    bundle = _load_bundle(ROOT_DIR)
    assignments_df = pd.read_parquet(ROOT_DIR / "data" / "analysis" / "persona_assignments.parquet")[["episode_id", "persona_id"]]
    assignments_df["episode_id"] = assignments_df["episode_id"].astype(str)
    profile_frame = bundle["base_df"].drop(columns=["persona_id_current"], errors="ignore").merge(
        assignments_df.rename(columns={"persona_id": "persona_id_current"}),
        on="episode_id",
        how="left",
    )

    dev_df = dev_df.drop(columns=["persona_id_current"], errors="ignore").merge(
        assignments_df.rename(columns={"persona_id": "persona_id_current"}),
        on="episode_id",
        how="left",
    )
    eval_locked_df = eval_locked_df.drop(columns=["persona_id_current"], errors="ignore").merge(
        assignments_df.rename(columns={"persona_id": "persona_id_current"}),
        on="episode_id",
        how="left",
    )

    persona_summary = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_summary.csv")
    cluster_stats = pd.read_csv(ROOT_DIR / "data" / "analysis" / "cluster_stats.csv")
    profile_summary = persona_summary.merge(
        cluster_stats[[column for column in ["persona_id", "dominant_signature", "share_rank"] if column in cluster_stats.columns]],
        on="persona_id",
        how="left",
        suffixes=("", "_cluster"),
    )
    persona_examples = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_examples.csv")
    profile_df = build_cluster_semantic_profiles(
        profile_frame,
        persona_column="persona_id_current",
        persona_summary_df=profile_summary,
        persona_examples_df=persona_examples,
        profile_label="current_production",
    )
    selection = select_reconciliation_like_persona(dev_df, profile_df, persona_column="persona_id_current")
    p4_like_persona_id = str(selection["selected_persona_id"])
    p1_persona_id = "persona_01"

    overview_path = ROOT_DIR / "data" / "analysis" / "validation_snapshot.json"
    overview = json.loads(overview_path.read_text(encoding="utf-8")).get("overview_metrics", {})
    persona_status_lookup = (
        persona_summary.set_index("persona_id")["promotion_status"].astype(str).to_dict()
        if not persona_summary.empty and "persona_id" in persona_summary.columns
        else {}
    )
    report = {
        "reconciliation_like_persona_id": p4_like_persona_id,
        "persona_01_like_persona_id": p1_persona_id,
        "dev": _evaluate(dev_df, p4_like_persona_id, p1_persona_id),
        "eval_locked": _evaluate(eval_locked_df, p4_like_persona_id, p1_persona_id),
        "top_3_cluster_share_of_core_labeled": overview.get("top_3_cluster_share_of_core_labeled"),
        "largest_source_influence_share_pct": overview.get("largest_source_influence_share_pct"),
        "overall_unknown_ratio": overview.get("overall_unknown_ratio"),
        "final_usable_persona_count": overview.get("final_usable_persona_count"),
        "persona_04_status": persona_status_lookup.get("persona_04", ""),
        "persona_05_status": persona_status_lookup.get("persona_05", ""),
        "release_gate_target_selection_reason": selection["selection_reason"],
        "release_gate_candidate_scores": selection["candidate_scores"],
        "promotion_drift_flags": build_promotion_drift_flags(persona_status_lookup, p4_like_persona_id),
    }

    output_path = curation_dir / "reconciliation_signoff_release_gate.json"
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report_path": str(output_path), "reconciliation_like_persona_id": p4_like_persona_id}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
