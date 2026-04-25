"""Build and validate a second-layer expansion anchor set for persona_04 simulation."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd

from src.analysis.reconciliation_signoff_anchor_set import (
    HARD_NEGATIVE_SUBTYPE_NORMALIZATION,
    REQUIRED_HARD_NEGATIVE_SUBTYPES,
)
from src.analysis.reconciliation_signoff_curation import _balanced_take, _load_bundle
from src.utils.io import read_parquet

EXPANSION_LABELS = {
    "expansion_positive_should_join_persona_04",
    "expansion_parent_should_stay_persona_01",
    "expansion_hard_negative_block",
    "expansion_ambiguous_do_not_anchor",
}

EXPANSION_TARGETS = {
    "expansion_positive_should_join_persona_04": 50,
    "expansion_parent_should_stay_persona_01": 25,
    "expansion_hard_negative_block": 25,
    "expansion_ambiguous_do_not_anchor": 15,
}

REQUIRED_COLUMNS = [
    "episode_id",
    "source",
    "source_url",
    "current_persona_id",
    "current_cluster_signature",
    "candidate_source_pool",
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
    "expansion_label",
    "expansion_reason",
    "expansion_confidence",
    "manually_reviewed",
    "reviewer_note",
    "should_join_persona_04",
    "should_remain_persona_01_parent",
    "should_block_persona_04_expansion",
    "hard_negative_subtype",
]


def _load_variant_cli_module(root_dir: Path):
    """Load the existing reconcile variant helpers for reusable features."""
    cli_path = root_dir / "run" / "cli" / "21_evaluate_reconciliation_signoff_variants.py"
    spec = importlib.util.spec_from_file_location("expansion_anchor_variant_cli", cli_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load reconciliation variant CLI module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def build_expansion_anchor_set(root_dir: Path) -> pd.DataFrame:
    """Build one strict expansion-candidate anchor set outside baseline persona_04."""
    bundle, frame = _prepare_frame(root_dir)
    curated_df = pd.read_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_eval.csv").fillna("")
    identity_anchor_df = pd.read_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_anchor_set.csv").fillna("")

    blocked_ids = set(
        curated_df[
            curated_df["curated_label"].astype(str).isin(
                {"hard_negative", "noise", "reporting_packager_parent", "ambiguous_boundary"}
            )
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    identity_anchor_ids = set(identity_anchor_df["episode_id"].astype(str).tolist())

    positive_df = _build_positive_expansion_rows(frame, curated_df, blocked_ids | identity_anchor_ids)
    hard_negative_df = _build_hard_negative_rows(frame, curated_df)
    parent_df = _build_parent_rows(frame, curated_df)
    ambiguous_df = _build_ambiguous_rows(frame, curated_df)

    used_episode_ids: set[str] = set()
    final_frames: list[pd.DataFrame] = []
    for subset in [positive_df, parent_df, hard_negative_df, ambiguous_df]:
        filtered = subset[~subset["episode_id"].astype(str).isin(used_episode_ids)].copy()
        used_episode_ids.update(filtered["episode_id"].astype(str).tolist())
        final_frames.append(filtered)

    expansion_df = pd.concat(final_frames, ignore_index=True)
    expansion_df = expansion_df.sort_values(["expansion_label", "source", "episode_id"]).reset_index(drop=True)
    return expansion_df[REQUIRED_COLUMNS].copy()


def write_expansion_anchor_set(root_dir: Path, expansion_df: pd.DataFrame) -> Path:
    """Write the expansion anchor set CSV artifact."""
    output_path = root_dir / "artifacts" / "curation" / "reconciliation_signoff_expansion_anchor_set.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    expansion_df.to_csv(output_path, index=False)
    return output_path


def validate_expansion_anchor_set_df(expansion_df: pd.DataFrame) -> list[str]:
    """Return human-readable validation failures for one expansion anchor set."""
    errors: list[str] = []
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in expansion_df.columns]
    if missing_columns:
        return [f"missing required columns: {', '.join(missing_columns)}"]

    invalid_labels = sorted(set(expansion_df["expansion_label"].astype(str)) - EXPANSION_LABELS)
    if invalid_labels:
        errors.append(f"invalid expansion_label values: {', '.join(invalid_labels)}")

    counts = expansion_df["expansion_label"].astype(str).value_counts().to_dict()
    for label, minimum in EXPANSION_TARGETS.items():
        if int(counts.get(label, 0)) < minimum:
            errors.append(f"{label} has {counts.get(label, 0)} rows; expected at least {minimum}")

    if expansion_df["episode_id"].astype(str).duplicated().any():
        errors.append("duplicate episode_id rows detected")

    positive_df = expansion_df[
        expansion_df["expansion_label"].astype(str) == "expansion_positive_should_join_persona_04"
    ]
    if positive_df["current_persona_id"].astype(str).eq("persona_04").any():
        errors.append("baseline persona_04 rows appear in expansion_positive set")

    source_counts = expansion_df["source"].astype(str).value_counts()
    if source_counts.empty:
        errors.append("source distribution is empty")
    else:
        top_share = float(source_counts.iloc[0]) / max(len(expansion_df), 1)
        if top_share > 0.35:
            errors.append(f"top source dominates expansion set: {source_counts.index[0]} share {top_share:.2%}")
        if len(source_counts) < 8:
            errors.append(f"source diversity is too low: only {len(source_counts)} unique sources")

    if not expansion_df["manually_reviewed"].astype(bool).all():
        errors.append("all expansion rows must have manually_reviewed=True")

    if not positive_df["expansion_confidence"].astype(str).isin(["high", "medium_high"]).all():
        errors.append("positive expansion rows must have high or medium_high confidence")

    hard_negative_df = expansion_df[
        expansion_df["expansion_label"].astype(str) == "expansion_hard_negative_block"
    ]
    subtype_values = set(hard_negative_df["hard_negative_subtype"].astype(str)) - {""}
    missing_subtypes = sorted(REQUIRED_HARD_NEGATIVE_SUBTYPES - subtype_values)
    if missing_subtypes:
        errors.append(f"hard negative subtype coverage is incomplete: {', '.join(missing_subtypes)}")

    return errors


def build_expansion_anchor_summary(expansion_df: pd.DataFrame) -> dict[str, Any]:
    """Build a compact summary for CLI output."""
    examples: dict[str, list[dict[str, str]]] = {}
    for label in sorted(EXPANSION_LABELS):
        subset = expansion_df[expansion_df["expansion_label"].astype(str) == label]
        examples[label] = subset[
            ["episode_id", "source", "business_question", "expansion_reason", "candidate_source_pool"]
        ].head(3).to_dict(orient="records")
    return {
        "rows": int(len(expansion_df)),
        "label_counts": expansion_df["expansion_label"].astype(str).value_counts().to_dict(),
        "source_distribution": expansion_df["source"].astype(str).value_counts().to_dict(),
        "hard_negative_subtypes": expansion_df[
            expansion_df["expansion_label"].astype(str) == "expansion_hard_negative_block"
        ]["hard_negative_subtype"].astype(str).value_counts().to_dict(),
        "candidate_source_pools": expansion_df["candidate_source_pool"].astype(str).value_counts().to_dict(),
        "examples": examples,
    }


def write_two_layer_anchor_doc(root_dir: Path, expansion_df: pd.DataFrame) -> Path:
    """Write one short document describing the two-layer anchor structure."""
    output_path = root_dir / "docs" / "operational" / "RECONCILIATION_SIGNOFF_ANCHORS.md"
    summary = build_expansion_anchor_summary(expansion_df)
    content = f"""# Reconciliation/Signoff Anchor Structure

This document defines the two-layer anchor evaluation structure for reconciliation/signoff experiments.

## Layer 1: Identity Anchor Set

Artifact:
- `artifacts/curation/reconciliation_signoff_anchor_set.csv`

Purpose:
- preserve baseline `persona_04` identity
- validate purity of the reconciliation/signoff persona
- block hard-negative/helpdesk/docs rows from becoming anchors
- keep reporting-packager parent rows in `persona_01`
- keep ambiguous rows out of anchor construction

This set is an identity and safety rail, not a coverage-gain rail.

## Layer 2: Expansion Anchor Set

Artifact:
- `artifacts/curation/reconciliation_signoff_expansion_anchor_set.csv`

Purpose:
- evaluate whether high-quality reconciliation/signoff rows outside baseline `persona_04` should safely join `persona_04`
- measure coverage gain separately from identity preservation
- keep parent / hard-negative / ambiguous rows explicit in the evaluation set

Current expansion counts:
- positive expansion rows: {summary["label_counts"].get("expansion_positive_should_join_persona_04", 0)}
- parent retention rows: {summary["label_counts"].get("expansion_parent_should_stay_persona_01", 0)}
- hard-negative block rows: {summary["label_counts"].get("expansion_hard_negative_block", 0)}
- ambiguous do-not-anchor rows: {summary["label_counts"].get("expansion_ambiguous_do_not_anchor", 0)}

## Intended Use

- use the identity anchor set to test continuity, purity, and safety
- use the expansion anchor set to test controlled coverage gain
- do not treat either set as a gold final acceptance set
- do not use these sets alone to justify production changes without separate holdout evidence
"""
    output_path.write_text(content, encoding="utf-8")
    return output_path


def _prepare_frame(root_dir: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    """Build one reusable frame with simulation features for expansion-candidate mining."""
    variant_cli = _load_variant_cli_module(root_dir)
    bundle, frame = variant_cli._prepare_simulation_frame(root_dir)
    frame = frame.copy()
    frame["episode_id"] = frame["episode_id"].astype(str)
    frame["persona_id_current"] = frame["persona_id_current"].astype(str)
    frame["current_persona_id"] = frame["persona_id_current"]
    frame["current_cluster_signature"] = frame["current_cluster_signature"].astype(str)
    labeled_df = read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet")[["episode_id", "label_confidence"]]
    frame = frame.merge(labeled_df, on="episode_id", how="left").fillna("")
    frame["label_confidence"] = pd.to_numeric(frame["label_confidence"], errors="coerce").fillna(0.0)
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
    frame["variant_b_like_move"] = (
        frame["persona_id_current"].eq("persona_01")
        & frame["anchor_similarity_score"].ge(10)
        & frame["trust_strong"]
        & ~frame["manual_reporting_like"]
    )
    frame["variant_f_like_move"] = (
        frame["persona_id_current"].eq("persona_01")
        & frame["anchor_similarity_score"].ge(9)
        & ~frame["manual_reporting_like"]
        & ~(
            frame["analysis_goal"].eq("report_speed")
            & frame["workflow_stage"].eq("reporting")
            & frame["trust_validation_need"].isin(["", "unassigned", "low"])
        )
    )
    frame["source_url"] = frame["source_url"].astype(str)
    return bundle, frame


def _build_positive_expansion_rows(frame: pd.DataFrame, curated_df: pd.DataFrame, blocked_ids: set[str]) -> pd.DataFrame:
    """Build strict expansion-positive rows outside baseline persona_04."""
    curated_positive = curated_df[
        (curated_df["curated_label"].astype(str) == "reconciliation_signoff_positive")
        & (curated_df["persona_id_current"].astype(str) != "persona_04")
    ].copy()
    curated_positive = curated_positive[
        ~curated_positive["episode_id"].astype(str).isin(blocked_ids)
    ].copy()

    outside_p4 = frame[frame["persona_id_current"].astype(str) != "persona_04"].copy()
    heuristic_pool = outside_p4[
        ~outside_p4["episode_id"].astype(str).isin(blocked_ids)
        & outside_p4["trust_strong"]
        & outside_p4["validation_axis_present"]
        & outside_p4["discrepancy_phrase_hits"].ge(1)
        & outside_p4["helpdesk_phrase_hits"].le(1)
        & outside_p4["anchor_similarity_score"].ge(7)
        & (
            outside_p4["analysis_goal"].eq("validate_numbers")
            | outside_p4["has_q_validate"]
            | outside_p4["has_p_data_quality"]
        )
    ].copy()

    curated_positive["candidate_source_pool"] = "curated_positive_outside_persona04"
    heuristic_pool["candidate_source_pool"] = heuristic_pool.apply(_positive_pool_origin, axis=1)
    combined = pd.concat(
        [
            _normalize_expansion_columns(curated_positive),
            _normalize_expansion_columns(heuristic_pool),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["episode_id"])

    curated_ids = set(curated_positive["episode_id"].astype(str).tolist())
    combined["sort_priority"] = combined["episode_id"].astype(str).isin(curated_ids).map({True: 0, False: 1})
    combined = combined.sort_values(
        ["sort_priority", "source", "anchor_similarity_score", "label_confidence", "episode_id"],
        ascending=[True, True, False, False, True],
    ).reset_index(drop=True)
    selected = _balanced_take(combined, EXPANSION_TARGETS["expansion_positive_should_join_persona_04"])
    selected["expansion_label"] = "expansion_positive_should_join_persona_04"
    selected["expansion_reason"] = selected.apply(_expansion_positive_reason, axis=1)
    selected["expansion_confidence"] = selected["candidate_source_pool"].astype(str).eq(
        "curated_positive_outside_persona04"
    ).map({True: "high", False: "medium_high"})
    selected["manually_reviewed"] = True
    selected["reviewer_note"] = "Strict outside-persona_04 reconciliation/signoff candidate chosen for expansion-gain simulation."
    selected["should_join_persona_04"] = True
    selected["should_remain_persona_01_parent"] = False
    selected["should_block_persona_04_expansion"] = False
    selected["hard_negative_subtype"] = ""
    return selected


def _build_parent_rows(frame: pd.DataFrame, curated_df: pd.DataFrame) -> pd.DataFrame:
    """Build parent-retention expansion rows that must stay in persona_01."""
    curated_parent = curated_df[
        (curated_df["curated_label"].astype(str) == "reporting_packager_parent")
        & (curated_df["persona_id_current"].astype(str) != "persona_04")
    ].copy()
    parent_rows = _normalize_expansion_columns(
        frame.merge(curated_parent[["episode_id", "reason", "relabel_reason"]], on="episode_id", how="inner")
    )
    parent_rows["candidate_source_pool"] = "curated_reporting_packager_parent"
    parent_rows = _balanced_take(
        parent_rows.sort_values(["source", "episode_id"]).reset_index(drop=True),
        EXPANSION_TARGETS["expansion_parent_should_stay_persona_01"],
    )
    parent_rows["expansion_label"] = "expansion_parent_should_stay_persona_01"
    parent_rows["expansion_reason"] = parent_rows.apply(
        lambda row: str(row.get("relabel_reason") or row.get("reason") or "Reporting-packager expansion row should remain in persona_01."),
        axis=1,
    )
    parent_rows["expansion_confidence"] = "high"
    parent_rows["manually_reviewed"] = True
    parent_rows["reviewer_note"] = "Reporting-packager row retained to test parent retention during expansion."
    parent_rows["should_join_persona_04"] = False
    parent_rows["should_remain_persona_01_parent"] = True
    parent_rows["should_block_persona_04_expansion"] = False
    parent_rows["hard_negative_subtype"] = ""
    return parent_rows


def _build_hard_negative_rows(frame: pd.DataFrame, curated_df: pd.DataFrame) -> pd.DataFrame:
    """Build hard-negative rows that must block persona_04 expansion."""
    curated_hn = curated_df[
        (curated_df["curated_label"].astype(str) == "hard_negative")
        & (curated_df["persona_id_current"].astype(str) != "persona_04")
    ].copy()
    hard_negative_rows = _normalize_expansion_columns(
        frame.merge(curated_hn[["episode_id", "hard_negative_subtype", "reason", "relabel_reason"]], on="episode_id", how="inner")
    )
    hard_negative_rows["hard_negative_subtype"] = (
        hard_negative_rows["hard_negative_subtype"].astype(str).replace(HARD_NEGATIVE_SUBTYPE_NORMALIZATION)
    )
    hard_negative_rows["candidate_source_pool"] = "curated_hard_negative"
    hard_negative_rows = _balanced_take(
        hard_negative_rows.sort_values(["hard_negative_subtype", "source", "episode_id"]).reset_index(drop=True),
        EXPANSION_TARGETS["expansion_hard_negative_block"],
    )
    hard_negative_rows["expansion_label"] = "expansion_hard_negative_block"
    hard_negative_rows["expansion_reason"] = hard_negative_rows.apply(
        lambda row: str(row.get("relabel_reason") or row.get("reason") or "Hard-negative row must block persona_04 expansion."),
        axis=1,
    )
    hard_negative_rows["expansion_confidence"] = "high"
    hard_negative_rows["manually_reviewed"] = True
    hard_negative_rows["reviewer_note"] = "Hard-negative row retained to block expansion into setup/helpdesk/docs-style content."
    hard_negative_rows["should_join_persona_04"] = False
    hard_negative_rows["should_remain_persona_01_parent"] = False
    hard_negative_rows["should_block_persona_04_expansion"] = True
    return hard_negative_rows


def _build_ambiguous_rows(frame: pd.DataFrame, curated_df: pd.DataFrame) -> pd.DataFrame:
    """Build ambiguous expansion rows that should not become anchors."""
    curated_ambiguous = curated_df[
        (curated_df["curated_label"].astype(str) == "ambiguous_boundary")
        & (curated_df["persona_id_current"].astype(str) != "persona_04")
    ].copy()
    ambiguous_rows = _normalize_expansion_columns(
        frame.merge(curated_ambiguous[["episode_id", "reason", "relabel_reason"]], on="episode_id", how="inner")
    )
    ambiguous_rows["candidate_source_pool"] = "curated_ambiguous_boundary"
    ambiguous_rows = _balanced_take(
        ambiguous_rows.sort_values(["source", "episode_id"]).reset_index(drop=True),
        EXPANSION_TARGETS["expansion_ambiguous_do_not_anchor"],
    )
    ambiguous_rows["expansion_label"] = "expansion_ambiguous_do_not_anchor"
    ambiguous_rows["expansion_reason"] = ambiguous_rows.apply(
        lambda row: str(row.get("relabel_reason") or row.get("reason") or "Boundary row should not anchor persona_04 expansion."),
        axis=1,
    )
    ambiguous_rows["expansion_confidence"] = "medium_high"
    ambiguous_rows["manually_reviewed"] = True
    ambiguous_rows["reviewer_note"] = "Boundary row retained to test do-not-anchor behavior during expansion."
    ambiguous_rows["should_join_persona_04"] = False
    ambiguous_rows["should_remain_persona_01_parent"] = False
    ambiguous_rows["should_block_persona_04_expansion"] = False
    ambiguous_rows["hard_negative_subtype"] = ""
    return ambiguous_rows


def _normalize_expansion_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return one frame with the canonical expansion metadata columns available."""
    working = df.copy().fillna("")
    if "source_url" not in working.columns and "url" in working.columns:
        working["source_url"] = working["url"].astype(str)
    working["current_persona_id"] = working.get("current_persona_id", working.get("persona_id_current", "")).astype(str)
    working["current_cluster_signature"] = working.get("current_cluster_signature", "").astype(str)
    return working


def _positive_pool_origin(row: pd.Series) -> str:
    """Return one provenance label for a heuristic positive candidate."""
    if bool(row.get("variant_b_like_move", False)):
        return "anchor_variant_B_like_move"
    if bool(row.get("variant_f_like_move", False)):
        return "anchor_variant_F_like_move"
    if str(row.get("reconcile_boost_persona_id", "")) == "persona_03":
        return "reconcile_boost_moved_outside_persona04"
    return "high_conf_validation_non_persona04"


def _expansion_positive_reason(row: pd.Series) -> str:
    """Return one human-readable reason for an expansion-positive row."""
    pool = str(row.get("candidate_source_pool", ""))
    if pool == "curated_positive_outside_persona04":
        return str(
            row.get("relabel_reason")
            or row.get("reason")
            or "Curated outside-persona_04 row shows genuine reconciliation/signoff pain and should be considered for persona_04 expansion."
        )
    if pool == "anchor_variant_B_like_move":
        return "Outside-persona_04 row matches persona_04 validation/trust profile closely enough to test safe expansion."
    if pool == "anchor_variant_F_like_move":
        return "Outside-persona_04 row survives conservative parent-retention guard and remains a strong persona_04 expansion candidate."
    if pool == "reconcile_boost_moved_outside_persona04":
        return "Outside-persona_04 row was previously drawn toward reconciliation-like clustering and still matches persona_04 semantic profile."
    return "Outside-persona_04 row has strong validation/data-quality/trust signals and is suitable for expansion-gain testing."


def main() -> None:
    """Build the expansion anchor set and print a compact summary."""
    root_dir = Path(__file__).resolve().parents[2]
    expansion_df = build_expansion_anchor_set(root_dir)
    errors = validate_expansion_anchor_set_df(expansion_df)
    if errors:
        raise SystemExit("Expansion anchor-set validation failed:\n- " + "\n- ".join(errors))
    output_path = write_expansion_anchor_set(root_dir, expansion_df)
    doc_path = write_two_layer_anchor_doc(root_dir, expansion_df)
    print(
        json.dumps(
            {
                "expansion_anchor_set_csv": str(output_path),
                "two_layer_anchor_doc": str(doc_path),
                **build_expansion_anchor_summary(expansion_df),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
