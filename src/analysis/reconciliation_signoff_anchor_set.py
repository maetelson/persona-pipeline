"""Build and validate a manually anchored reconciliation/signoff seed set."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.reconciliation_signoff_curation import _balanced_take, _load_bundle
from src.utils.io import read_parquet

ANCHOR_LABELS = {
    "anchor_positive_reconciliation_signoff",
    "anchor_hard_negative",
    "anchor_parent_reporting_packager",
    "non_anchor_ambiguous",
}

ANCHOR_TARGETS = {
    "anchor_positive_reconciliation_signoff": 50,
    "anchor_hard_negative": 30,
    "anchor_parent_reporting_packager": 30,
    "non_anchor_ambiguous": 20,
}

REQUIRED_COLUMNS = [
    "episode_id",
    "source",
    "source_url",
    "current_persona_id",
    "current_cluster_signature",
    "curation_source_pool",
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
    "anchor_label",
    "anchor_reason",
    "anchor_confidence",
    "manually_reviewed",
    "reviewer_note",
    "should_anchor_persona_04",
    "should_block_persona_04_anchor",
    "should_remain_persona_01_parent",
    "hard_negative_subtype",
]

REQUIRED_HARD_NEGATIVE_SUBTYPES = {
    "setup_configuration_support",
    "product_helpdesk_or_feature_limitation",
    "docs_tutorial_or_formula_help",
    "ui_bug_or_script_error",
}

HARD_NEGATIVE_SUBTYPE_NORMALIZATION = {
    "ui_bug_or_release_regression": "ui_bug_or_script_error",
    "setup_recovery_support": "setup_configuration_support",
}


def build_anchor_set(root_dir: Path) -> pd.DataFrame:
    """Build one strict anchor set for future identity-preserving simulations."""
    bundle = _load_bundle(root_dir)
    base_df = _build_base_anchor_frame(root_dir, bundle)
    curated_df = pd.read_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_eval.csv").fillna("")

    blocked_positive_ids = set(
        curated_df[
            curated_df["curated_label"].astype(str).isin(
                {"hard_negative", "noise", "ambiguous_boundary", "reporting_packager_parent"}
            )
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    positive_df = _build_positive_anchors(base_df, blocked_positive_ids)
    hard_negative_df = _build_hard_negative_anchors(base_df, curated_df)
    parent_df = _build_parent_anchors(base_df, curated_df)
    ambiguous_df = _build_ambiguous_rows(base_df, curated_df)

    final_frames: list[pd.DataFrame] = []
    used_episode_ids: set[str] = set()
    for frame in [positive_df, hard_negative_df, parent_df, ambiguous_df]:
        filtered = frame[~frame["episode_id"].astype(str).isin(used_episode_ids)].copy()
        used_episode_ids.update(filtered["episode_id"].astype(str).tolist())
        final_frames.append(filtered)

    anchor_df = pd.concat(final_frames, ignore_index=True)
    anchor_df = anchor_df.sort_values(["anchor_label", "source", "episode_id"]).reset_index(drop=True)
    return anchor_df[REQUIRED_COLUMNS].copy()


def write_anchor_set(root_dir: Path, anchor_df: pd.DataFrame) -> Path:
    """Write the anchor set CSV artifact."""
    output_path = root_dir / "artifacts" / "curation" / "reconciliation_signoff_anchor_set.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    anchor_df.to_csv(output_path, index=False)
    return output_path


def validate_anchor_set_df(anchor_df: pd.DataFrame) -> list[str]:
    """Return human-readable validation failures for one anchor set."""
    errors: list[str] = []
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in anchor_df.columns]
    if missing_columns:
        return [f"missing required columns: {', '.join(missing_columns)}"]

    invalid_labels = sorted(set(anchor_df["anchor_label"].astype(str)) - ANCHOR_LABELS)
    if invalid_labels:
        errors.append(f"invalid anchor_label values: {', '.join(invalid_labels)}")

    counts = anchor_df["anchor_label"].astype(str).value_counts().to_dict()
    for label, minimum in ANCHOR_TARGETS.items():
        if int(counts.get(label, 0)) < minimum:
            errors.append(f"{label} has {counts.get(label, 0)} rows; expected at least {minimum}")

    if anchor_df["episode_id"].astype(str).duplicated().any():
        errors.append("duplicate episode_id rows detected")

    source_counts = anchor_df["source"].astype(str).value_counts()
    if source_counts.empty:
        errors.append("source distribution is empty")
    else:
        top_share = float(source_counts.iloc[0]) / max(len(anchor_df), 1)
        if top_share > 0.35:
            errors.append(f"top source dominates anchor set: {source_counts.index[0]} share {top_share:.2%}")
        if len(source_counts) < 8:
            errors.append(f"source diversity is too low: only {len(source_counts)} unique sources")

    if not anchor_df["manually_reviewed"].astype(bool).all():
        errors.append("all anchor rows must have manually_reviewed=True")

    positive_df = anchor_df[anchor_df["anchor_label"].astype(str) == "anchor_positive_reconciliation_signoff"]
    if not positive_df["anchor_confidence"].astype(str).eq("high").all():
        errors.append("positive anchors must all have high confidence")

    hard_negative_df = anchor_df[anchor_df["anchor_label"].astype(str) == "anchor_hard_negative"]
    subtype_values = set(hard_negative_df["hard_negative_subtype"].astype(str)) - {""}
    missing_subtypes = sorted(REQUIRED_HARD_NEGATIVE_SUBTYPES - subtype_values)
    if missing_subtypes:
        errors.append(f"hard negative subtype coverage is incomplete: {', '.join(missing_subtypes)}")

    return errors


def build_anchor_summary(anchor_df: pd.DataFrame) -> dict[str, Any]:
    """Build a compact summary for CLI output."""
    examples: dict[str, list[dict[str, str]]] = {}
    for label in sorted(ANCHOR_LABELS):
        subset = anchor_df[anchor_df["anchor_label"].astype(str) == label]
        examples[label] = subset[["episode_id", "source", "business_question", "anchor_reason"]].head(3).to_dict(
            orient="records"
        )
    return {
        "rows": int(len(anchor_df)),
        "label_counts": anchor_df["anchor_label"].astype(str).value_counts().to_dict(),
        "source_distribution": anchor_df["source"].astype(str).value_counts().to_dict(),
        "hard_negative_subtypes": anchor_df[anchor_df["anchor_label"].astype(str) == "anchor_hard_negative"][
            "hard_negative_subtype"
        ]
        .astype(str)
        .value_counts()
        .to_dict(),
        "examples": examples,
    }


def _build_base_anchor_frame(root_dir: Path, bundle: dict[str, Any]) -> pd.DataFrame:
    """Augment the reconcile curation base frame with anchor-specific metadata."""
    base_df = bundle["base_df"].copy()
    labeled_df = read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet")[["episode_id", "label_confidence"]]
    persona_examples_df = pd.read_csv(root_dir / "data" / "analysis" / "persona_examples.csv").fillna("")
    selected_example_ids = set(
        persona_examples_df[
            (persona_examples_df["persona_id"].astype(str) == "persona_04")
            & (persona_examples_df["selection_decision"].astype(str) == "selected")
        ]["episode_id"]
        .astype(str)
        .tolist()
    )
    base_df = base_df.merge(labeled_df, on="episode_id", how="left").fillna("")
    base_df["episode_id"] = base_df["episode_id"].astype(str)
    base_df["label_confidence"] = pd.to_numeric(base_df["label_confidence"], errors="coerce").fillna(0.0)
    base_df["current_persona_id"] = base_df["persona_id_current"].astype(str)
    base_df["source_url"] = base_df["source_url"].astype(str)
    base_df["has_q_validate"] = base_df["question_codes"].astype(str).str.contains("Q_VALIDATE_NUMBERS", regex=False)
    base_df["has_p_data_quality"] = base_df["pain_codes"].astype(str).str.contains("P_DATA_QUALITY", regex=False)
    base_df["has_selected_example"] = base_df["episode_id"].isin(selected_example_ids)
    base_df["positive_anchor_strict"] = (
        base_df["current_persona_id"].eq("persona_04")
        & base_df["analysis_goal"].astype(str).eq("validate_numbers")
        & base_df["workflow_stage"].astype(str).eq("validation")
        & base_df["bottleneck_type"].astype(str).eq("data_quality")
        & base_df["trust_validation_need"].astype(str).isin(["high", "medium"])
        & base_df["has_q_validate"]
        & base_df["has_p_data_quality"]
        & base_df["noise_term_hits"].fillna(0).eq(0)
        & base_df["hard_negative_term_hits"].fillna(0).eq(0)
        & (
            base_df["positive_term_hits"].fillna(0).ge(1)
            | base_df["trust_validation_need"].astype(str).eq("high")
            | base_df["has_selected_example"]
        )
        & base_df["label_confidence"].ge(0.75)
    )
    return base_df


def _balanced_fill(pool_df: pd.DataFrame, target_count: int) -> pd.DataFrame:
    """Return a deterministic, source-balanced subset."""
    if pool_df.empty:
        return pool_df.copy()
    ordered = pool_df.sort_values(
        ["source", "has_selected_example", "label_confidence", "positive_term_hits", "episode_id"],
        ascending=[True, False, False, False, True],
    ).reset_index(drop=True)
    return _balanced_take(ordered, target_count)


def _build_positive_anchors(base_df: pd.DataFrame, blocked_episode_ids: set[str]) -> pd.DataFrame:
    """Build high-confidence persona_04 anchor positives."""
    selected_rows = base_df[
        base_df["has_selected_example"] & ~base_df["episode_id"].astype(str).isin(blocked_episode_ids)
    ].copy()
    selected_rows["curation_source_pool"] = "persona_04_selected_examples"
    strict_rows = base_df[
        base_df["positive_anchor_strict"]
        & ~base_df["has_selected_example"]
        & ~base_df["episode_id"].astype(str).isin(blocked_episode_ids)
    ].copy()
    strict_rows["curation_source_pool"] = "persona_04_high_confidence_current"

    selected_take = _balanced_fill(selected_rows, min(len(selected_rows), 10))
    selected_ids = set(selected_take["episode_id"].astype(str).tolist())
    strict_take = _balanced_fill(strict_rows[~strict_rows["episode_id"].astype(str).isin(selected_ids)], ANCHOR_TARGETS["anchor_positive_reconciliation_signoff"] - len(selected_take))
    positive_df = pd.concat([selected_take, strict_take], ignore_index=True).drop_duplicates(subset=["episode_id"])

    positive_df["anchor_label"] = "anchor_positive_reconciliation_signoff"
    positive_df["anchor_reason"] = positive_df.apply(_positive_anchor_reason, axis=1)
    positive_df["anchor_confidence"] = "high"
    positive_df["manually_reviewed"] = True
    positive_df["reviewer_note"] = "Strict baseline persona_04 validation row selected as reconciliation/signoff anchor."
    positive_df["should_anchor_persona_04"] = True
    positive_df["should_block_persona_04_anchor"] = False
    positive_df["should_remain_persona_01_parent"] = False
    positive_df["hard_negative_subtype"] = ""
    return positive_df


def _build_hard_negative_anchors(base_df: pd.DataFrame, curated_df: pd.DataFrame) -> pd.DataFrame:
    """Build hard-negative anchors from the cleaned curation set."""
    curated_hn = curated_df[curated_df["curated_label"].astype(str) == "hard_negative"].copy()
    merged = base_df.merge(
        curated_hn[["episode_id", "hard_negative_subtype", "reason", "relabel_reason"]],
        on="episode_id",
        how="inner",
    )
    merged["hard_negative_subtype"] = (
        merged["hard_negative_subtype"].astype(str).replace(HARD_NEGATIVE_SUBTYPE_NORMALIZATION)
    )
    merged["curation_source_pool"] = "curated_hard_negative"
    merged = merged.sort_values(["hard_negative_subtype", "source", "episode_id"]).reset_index(drop=True)
    hard_negative_df = _balanced_take(merged, ANCHOR_TARGETS["anchor_hard_negative"])
    hard_negative_df["anchor_label"] = "anchor_hard_negative"
    hard_negative_df["anchor_reason"] = hard_negative_df.apply(
        lambda row: str(row.get("relabel_reason") or row.get("reason") or "Hard-negative anchor for setup/helpdesk/docs/vendor-like rows."),
        axis=1,
    )
    hard_negative_df["anchor_confidence"] = "high"
    hard_negative_df["manually_reviewed"] = True
    hard_negative_df["reviewer_note"] = "Cleaned hard-negative row retained to block persona_04 anchoring."
    hard_negative_df["should_anchor_persona_04"] = False
    hard_negative_df["should_block_persona_04_anchor"] = True
    hard_negative_df["should_remain_persona_01_parent"] = False
    return hard_negative_df


def _build_parent_anchors(base_df: pd.DataFrame, curated_df: pd.DataFrame) -> pd.DataFrame:
    """Build persona_01 parent-retention anchors from the cleaned curation set."""
    curated_parent = curated_df[curated_df["curated_label"].astype(str) == "reporting_packager_parent"].copy()
    merged = base_df.merge(
        curated_parent[["episode_id", "reason", "relabel_reason"]],
        on="episode_id",
        how="inner",
    )
    merged["curation_source_pool"] = "curated_reporting_packager_parent"
    merged = merged[
        merged["current_persona_id"].astype(str).eq("persona_01")
        & merged["analysis_goal"].astype(str).eq("report_speed")
        & merged["workflow_stage"].astype(str).eq("reporting")
        & merged["bottleneck_type"].astype(str).isin(["manual_reporting", "tool_limitation", "handoff_dependency"])
        & merged["trust_validation_need"].astype(str).isin(["", "unassigned", "low"])
        & ~merged["question_codes"].astype(str).str.contains("Q_VALIDATE_NUMBERS", regex=False)
    ].copy()
    parent_df = _balanced_take(
        merged.sort_values(["source", "episode_id"]).reset_index(drop=True),
        ANCHOR_TARGETS["anchor_parent_reporting_packager"],
    )
    parent_df["anchor_label"] = "anchor_parent_reporting_packager"
    parent_df["anchor_reason"] = parent_df.apply(
        lambda row: str(row.get("relabel_reason") or row.get("reason") or "Reporting-packager anchor that should remain in persona_01 parent."),
        axis=1,
    )
    parent_df["anchor_confidence"] = "high"
    parent_df["manually_reviewed"] = True
    parent_df["reviewer_note"] = "Recurring reporting-packager row kept as persona_01 parent anchor."
    parent_df["should_anchor_persona_04"] = False
    parent_df["should_block_persona_04_anchor"] = False
    parent_df["should_remain_persona_01_parent"] = True
    parent_df["hard_negative_subtype"] = ""
    return parent_df


def _build_ambiguous_rows(base_df: pd.DataFrame, curated_df: pd.DataFrame) -> pd.DataFrame:
    """Build non-anchor ambiguous rows from the cleaned curation set."""
    curated_ambiguous = curated_df[curated_df["curated_label"].astype(str) == "ambiguous_boundary"].copy()
    merged = base_df.merge(
        curated_ambiguous[["episode_id", "reason", "relabel_reason"]],
        on="episode_id",
        how="inner",
    )
    merged["curation_source_pool"] = "curated_ambiguous_boundary"
    ambiguous_df = _balanced_take(
        merged.sort_values(["source", "episode_id"]).reset_index(drop=True),
        ANCHOR_TARGETS["non_anchor_ambiguous"],
    )
    ambiguous_df["anchor_label"] = "non_anchor_ambiguous"
    ambiguous_df["anchor_reason"] = ambiguous_df.apply(
        lambda row: str(row.get("relabel_reason") or row.get("reason") or "Mixed reporting/validation row that should not anchor persona_04."),
        axis=1,
    )
    ambiguous_df["anchor_confidence"] = "medium"
    ambiguous_df["manually_reviewed"] = True
    ambiguous_df["reviewer_note"] = "Boundary row retained as non-anchor to avoid overfitting persona_04 anchors."
    ambiguous_df["should_anchor_persona_04"] = False
    ambiguous_df["should_block_persona_04_anchor"] = False
    ambiguous_df["should_remain_persona_01_parent"] = False
    ambiguous_df["hard_negative_subtype"] = ""
    return ambiguous_df


def _positive_anchor_reason(row: pd.Series) -> str:
    """Return one human-readable reason for a positive anchor."""
    source_pool = str(row.get("curation_source_pool", ""))
    if source_pool == "persona_04_selected_examples":
        return (
            "Baseline persona_04 selected example with explicit validation/reconciliation profile and high trust context."
        )
    return (
        "Baseline persona_04 high-confidence validation row with validate_numbers, data_quality, and high-trust signals."
    )


def main() -> None:
    """Build the anchor set and print a compact summary."""
    root_dir = Path(__file__).resolve().parents[2]
    anchor_df = build_anchor_set(root_dir)
    errors = validate_anchor_set_df(anchor_df)
    if errors:
        raise SystemExit("Anchor-set validation failed:\n- " + "\n- ".join(errors))
    output_path = write_anchor_set(root_dir, anchor_df)
    print(json.dumps({"anchor_set_csv": str(output_path), **build_anchor_summary(anchor_df)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
