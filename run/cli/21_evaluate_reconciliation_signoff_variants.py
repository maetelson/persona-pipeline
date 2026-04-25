"""Evaluate reconciliation/signoff simulation variants on dev and locked holdout."""

from __future__ import annotations

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


HELPDESK_PHRASES = [
    "how to",
    "guide",
    "tutorial",
    "docs",
    "documentation",
    "support ticket",
    "contact support",
    "customer care",
    "feature request",
    "roadmap",
    "release note",
    "announcement",
    "community update",
    "newsletter",
    "roundup",
    "vendor",
    "discord",
    "need help",
    "help me",
    "looking around",
    "can you help",
    "thanks",
    "thank you",
    "api",
    "oauth",
    "authentication",
    "configure",
    "configuration",
    "setup",
    "connect",
    "login",
    "permission",
    "ui bug",
    "script error",
    "stale bot",
    "chargeback",
    "chat",
    "welcome flow",
    "can't find",
    "cant find",
    "filter expression",
    "zapier",
    "google ads",
    "shopify payments",
]

DISCREPANCY_PHRASES = [
    "mismatch",
    "discrepancy",
    "different",
    "differs",
    "not matching",
    "does not match",
    "do not match",
    "wrong values",
    "wrong numbers",
    "wrong total",
    "missing data",
    "no data",
    "inconsistent",
    "inflated",
    "duplicate",
    "duplicating",
    "reconcile",
    "reconciliation",
    "validate",
    "sanity check",
    "audit numbers",
]


@dataclass(frozen=True)
class VariantDefinition:
    """One simulation-only guard overlay applied on top of reconcile_boost."""

    variant_id: str
    description: str
    builder: Callable[[pd.DataFrame], pd.Series]


def _load_csv(path: Path) -> pd.DataFrame:
    """Load one curated split CSV."""
    if not path.exists():
        raise SystemExit(f"Missing required curation artifact: {path}")
    return pd.read_csv(path)


def _count_phrase_hits(text_series: pd.Series, phrases: list[str]) -> pd.Series:
    """Count substring hits from a small phrase list."""
    values = text_series.fillna("").astype(str).str.lower()
    return pd.Series(
        [sum(1 for phrase in phrases if phrase in value) for value in values],
        index=text_series.index,
    )


def _prepare_simulation_frame(root_dir: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    """Load current bundle plus a reusable simulation frame."""
    bundle = _load_bundle(root_dir)
    base_df = bundle["base_df"].copy()
    base_df["episode_id"] = base_df["episode_id"].astype(str)
    base_df["persona_id_current"] = base_df["persona_id_current"].astype(str)

    simulated = bundle["simulated_assignments"][["episode_id", "reconcile_boost_persona_id"]].copy()
    simulated["episode_id"] = simulated["episode_id"].astype(str)
    frame = base_df.merge(simulated, on="episode_id", how="left")

    text_columns = [
        "normalized_episode",
        "business_question",
        "bottleneck_text",
        "desired_output",
        "text_blob",
    ]
    for column in [
        "question_codes",
        "pain_codes",
        "analysis_goal",
        "workflow_stage",
        "bottleneck_type",
        "trust_validation_need",
        "source",
        *text_columns,
    ]:
        frame[column] = frame[column].fillna("").astype(str)

    text_blob = frame[text_columns].agg(" ".join, axis=1).str.lower()
    frame["helpdesk_phrase_hits"] = _count_phrase_hits(text_blob, HELPDESK_PHRASES)
    frame["discrepancy_phrase_hits"] = _count_phrase_hits(text_blob, DISCREPANCY_PHRASES)
    frame["has_q_validate"] = frame["question_codes"].str.contains("Q_VALIDATE_NUMBERS", regex=False)
    frame["has_p_data_quality"] = frame["pain_codes"].str.contains("P_DATA_QUALITY", regex=False)
    frame["trust_strong"] = frame["trust_validation_need"].isin(["high", "medium"])
    frame["trust_weak"] = ~frame["trust_strong"]
    frame["manual_reporting_like"] = (
        frame["analysis_goal"].eq("report_speed")
        & frame["workflow_stage"].eq("reporting")
        & frame["bottleneck_type"].isin(["manual_reporting", "tool_limitation", "handoff_friction", "general_friction"])
    )
    frame["validation_axis_present"] = (
        frame["analysis_goal"].eq("validate_numbers")
        | frame["workflow_stage"].eq("validation")
        | frame["bottleneck_type"].eq("data_quality")
        | frame["has_q_validate"]
        | frame["has_p_data_quality"]
    )
    frame["signal_score"] = (
        frame["discrepancy_phrase_hits"] * 2
        + frame["trust_strong"].astype(int) * 2
        + frame["has_q_validate"].astype(int) * 2
        + frame["has_p_data_quality"].astype(int) * 2
        + frame["analysis_goal"].eq("validate_numbers").astype(int)
        + frame["workflow_stage"].eq("validation").astype(int)
        - frame["helpdesk_phrase_hits"] * 2
        - frame["manual_reporting_like"].astype(int)
    ).astype(int)
    return bundle, frame


def _variant_definitions() -> list[VariantDefinition]:
    """Return all simulation-only overlay variants."""

    def variant_a(frame: pd.DataFrame) -> pd.Series:
        moved = frame["reconcile_boost_persona_id"].astype(str).eq(str(frame.attrs["persona_04_like_variant_id"]))
        return moved & ~(frame["trust_strong"] | frame["has_q_validate"])

    def variant_b(frame: pd.DataFrame) -> pd.Series:
        moved = frame["reconcile_boost_persona_id"].astype(str).eq(str(frame.attrs["persona_04_like_variant_id"]))
        return moved & frame["helpdesk_phrase_hits"].ge(2)

    def variant_c(frame: pd.DataFrame) -> pd.Series:
        moved = frame["reconcile_boost_persona_id"].astype(str).eq(str(frame.attrs["persona_04_like_variant_id"]))
        return moved & frame["manual_reporting_like"] & frame["trust_weak"] & ~frame["has_q_validate"]

    def variant_d(frame: pd.DataFrame) -> pd.Series:
        moved = frame["reconcile_boost_persona_id"].astype(str).eq(str(frame.attrs["persona_04_like_variant_id"]))
        return moved & ~frame["validation_axis_present"]

    def variant_e(frame: pd.DataFrame) -> pd.Series:
        moved = frame["reconcile_boost_persona_id"].astype(str).eq(str(frame.attrs["persona_04_like_variant_id"]))
        strong_discrepancy = frame["discrepancy_phrase_hits"].ge(2) & frame["trust_strong"]
        return moved & frame["helpdesk_phrase_hits"].ge(1) & ~strong_discrepancy

    def variant_f(frame: pd.DataFrame) -> pd.Series:
        moved = frame["reconcile_boost_persona_id"].astype(str).eq(str(frame.attrs["persona_04_like_variant_id"]))
        keep_signal = (
            frame["discrepancy_phrase_hits"].ge(1)
            | frame["has_q_validate"]
            | (frame["has_p_data_quality"] & frame["trust_strong"])
        )
        return moved & frame["manual_reporting_like"] & ~keep_signal

    def variant_g(frame: pd.DataFrame) -> pd.Series:
        moved = frame["reconcile_boost_persona_id"].astype(str).eq(str(frame.attrs["persona_04_like_variant_id"]))
        revert = pd.Series(False, index=frame.index)
        p1_rows = frame[moved & frame["persona_id_current"].astype(str).eq("persona_01")].copy()
        if p1_rows.empty:
            return revert
        for _, group in p1_rows.groupby("source", dropna=False):
            keep_n = max(1, int(round(len(group) * 0.7)))
            keep_ids = set(
                group.sort_values(["signal_score", "episode_id"], ascending=[False, True])
                .head(keep_n)["episode_id"]
                .astype(str)
                .tolist()
            )
            revert.loc[group.index] = ~group["episode_id"].astype(str).isin(keep_ids)
        return revert

    def variant_h(frame: pd.DataFrame) -> pd.Series:
        moved = frame["reconcile_boost_persona_id"].astype(str).eq(str(frame.attrs["persona_04_like_variant_id"]))
        keep_signal = (
            frame["discrepancy_phrase_hits"].ge(1)
            | frame["has_q_validate"]
            | (frame["has_p_data_quality"] & frame["trust_strong"])
        )
        revert = moved & (
            frame["helpdesk_phrase_hits"].ge(2)
            | (frame["manual_reporting_like"] & ~keep_signal)
        )
        source_cap = pd.Series(False, index=frame.index)
        p1_rows = frame[moved & frame["persona_id_current"].astype(str).eq("persona_01")].copy()
        if not p1_rows.empty:
            for _, group in p1_rows.groupby("source", dropna=False):
                keep_n = max(1, int(round(len(group) * 0.8)))
                keep_ids = set(
                    group.sort_values(["signal_score", "episode_id"], ascending=[False, True])
                    .head(keep_n)["episode_id"]
                    .astype(str)
                    .tolist()
                )
                source_cap.loc[group.index] = ~group["episode_id"].astype(str).isin(keep_ids)
        return revert | source_cap

    return [
        VariantDefinition("A", "reconcile_boost + trust_validation_need gate", variant_a),
        VariantDefinition("B", "reconcile_boost + hard_negative phrase penalty", variant_b),
        VariantDefinition("C", "reconcile_boost + manual_reporting/report_speed dampening when trust is weak", variant_c),
        VariantDefinition("D", "reconcile_boost + require validation axis or data_quality bottleneck for movement", variant_d),
        VariantDefinition("E", "reconcile_boost + merge guard for helpdesk/setup/docs rows", variant_e),
        VariantDefinition("F", "reconcile_boost + ambiguous boundary dampening", variant_f),
        VariantDefinition("G", "reconcile_boost + source-normalized movement guard", variant_g),
        VariantDefinition("H", "reconcile_boost + combined conservative guards", variant_h),
    ]


def _apply_variant(bundle: dict[str, Any], frame: pd.DataFrame, variant: VariantDefinition) -> pd.DataFrame:
    """Apply one simulation-only guard overlay on top of reconcile_boost assignments."""
    working = frame.copy()
    working.attrs["persona_04_like_variant_id"] = str(bundle["persona_04_like_variant_id"])
    working.attrs["persona_01_like_variant_id"] = str(bundle["persona_01_like_variant_id"])
    working["variant_persona_id"] = working["reconcile_boost_persona_id"].astype(str)

    revert_mask = variant.builder(working)
    revert_mask = revert_mask & ~working["persona_id_current"].astype(str).eq("persona_04")
    fallback_map = {
        str(base_persona): str(variant_persona)
        for base_persona, variant_persona in dict(bundle["simulated_mapping"]).items()
    }
    fallback_persona = working["persona_id_current"].astype(str).map(fallback_map).fillna(working["persona_id_current"].astype(str))
    working.loc[revert_mask, "variant_persona_id"] = fallback_persona.loc[revert_mask]
    return working


def _share(numerator: int, denominator: int) -> float:
    """Return one percentage with one decimal place."""
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 1)


def _evaluate_subset(curated_df: pd.DataFrame, persona_column: str, p4_variant: str, p1_variant: str) -> dict[str, Any]:
    """Evaluate one curated subset against one assignment column."""
    positives = curated_df[curated_df["curated_label"].astype(str) == "reconciliation_signoff_positive"]
    parents = curated_df[curated_df["curated_label"].astype(str) == "reporting_packager_parent"]
    hard_negatives = curated_df[curated_df["curated_label"].astype(str) == "hard_negative"]
    ambiguous = curated_df[curated_df["curated_label"].astype(str) == "ambiguous_boundary"]

    positive_hits = int(positives[persona_column].astype(str).eq(p4_variant).sum())
    parent_hits = int(parents[persona_column].astype(str).eq(p1_variant).sum())
    hard_negative_hits = int(hard_negatives[persona_column].astype(str).eq(p4_variant).sum())
    ambiguous_hits = int(ambiguous[persona_column].astype(str).eq(p4_variant).sum())
    return {
        "positive_recall": _share(positive_hits, len(positives)),
        "parent_retention": _share(parent_hits, len(parents)),
        "hard_negative_false_positive_rate": _share(hard_negative_hits, len(hard_negatives)),
        "ambiguous_movement_rate": _share(ambiguous_hits, len(ambiguous)),
        "parent_examples_wrongly_pulled_out_of_persona_01": int((~parents[persona_column].astype(str).eq(p1_variant)).sum()),
        "positives_moved_to_persona_04_like": positive_hits,
        "hard_negative_examples_wrongly_moved": hard_negative_hits,
        "ambiguous_examples_moved": ambiguous_hits,
        "parent_examples_wrongly_moved": int((~parents[persona_column].astype(str).eq(p1_variant)).sum()),
    }


def _top_3_share(assignments: pd.Series) -> float:
    """Compute top-3 persona share from one assignment series."""
    counts = assignments.astype(str).value_counts()
    return _share(int(counts.head(3).sum()), int(len(assignments)))


def _persona_statuses(top_3_share: float) -> dict[str, str]:
    """Heuristic workbook-concentration status readout for simulation reports."""
    persona_04_status = "likely_unblocked_by_concentration" if top_3_share < 80.0 else "blocked_by_concentration"
    persona_05_status = "still_likely_blocked"
    return {
        "persona_04_status_simulation": persona_04_status,
        "persona_05_status_simulation": persona_05_status,
    }


def _summarize_assignment(
    frame: pd.DataFrame,
    persona_column: str,
    curated_subset: pd.DataFrame,
    p4_variant: str,
    p1_variant: str,
) -> dict[str, Any]:
    """Attach subset metrics plus whole-population concentration metrics."""
    if persona_column in curated_subset.columns:
        subset = curated_subset.copy()
    else:
        subset = curated_subset.merge(frame[["episode_id", persona_column]], on="episode_id", how="left")
    metrics = _evaluate_subset(subset, persona_column, p4_variant, p1_variant)
    top_3_share = _top_3_share(frame[persona_column].astype(str))
    metrics["top_3_cluster_share_simulation"] = top_3_share
    metrics.update(_persona_statuses(top_3_share))
    return metrics


def _variant_dev_summary(
    variant: VariantDefinition,
    variant_frame: pd.DataFrame,
    dev_df: pd.DataFrame,
    p4_variant: str,
    p1_variant: str,
) -> dict[str, Any]:
    """Build the dev-only comparison payload for one variant."""
    merged_dev = dev_df.merge(variant_frame[["episode_id", "variant_persona_id"]], on="episode_id", how="left")
    metrics = _evaluate_subset(merged_dev, "variant_persona_id", p4_variant, p1_variant)
    top_3_share = _top_3_share(variant_frame["variant_persona_id"])
    moved_from_p1 = int(
        (
            variant_frame["persona_id_current"].astype(str).eq("persona_01")
            & variant_frame["variant_persona_id"].astype(str).eq(p4_variant)
        ).sum()
    )
    result = {
        "variant_id": variant.variant_id,
        "description": variant.description,
        **metrics,
        "top_3_cluster_share_simulation": top_3_share,
        **_persona_statuses(top_3_share),
        "rows_moved_from_persona_01_to_persona_04_like": moved_from_p1,
    }
    return result


def _variant_selection_score(dev_metrics: dict[str, Any], baseline_dev: dict[str, Any], raw_dev: dict[str, Any]) -> float:
    """Score one dev variant without peeking at eval_locked."""
    score = 0.0
    if dev_metrics["positive_recall"] > baseline_dev["positive_recall"]:
        score += 3.0
    if dev_metrics["parent_retention"] >= 99.0:
        score += 3.0
    if dev_metrics["hard_negative_false_positive_rate"] < raw_dev["hard_negative_false_positive_rate"]:
        score += 4.0
    if dev_metrics["ambiguous_movement_rate"] < raw_dev["ambiguous_movement_rate"]:
        score += 4.0
    if dev_metrics["top_3_cluster_share_simulation"] < 80.0:
        score += 2.0
    score += max(0.0, (dev_metrics["positive_recall"] - baseline_dev["positive_recall"]) / 10.0)
    score -= max(0.0, dev_metrics["hard_negative_false_positive_rate"] - raw_dev["hard_negative_false_positive_rate"]) / 10.0
    score -= max(0.0, dev_metrics["ambiguous_movement_rate"] - raw_dev["ambiguous_movement_rate"]) / 20.0
    return round(score, 4)


def _choose_best_variant(variant_rows: list[dict[str, Any]], baseline_dev: dict[str, Any], raw_dev: dict[str, Any]) -> dict[str, Any]:
    """Choose at most one best variant using dev metrics only."""
    enriched: list[dict[str, Any]] = []
    for row in variant_rows:
        candidate = dict(row)
        candidate["selection_score"] = _variant_selection_score(candidate, baseline_dev, raw_dev)
        enriched.append(candidate)
    viable = [
        row
        for row in enriched
        if row["positive_recall"] > baseline_dev["positive_recall"]
        and row["parent_retention"] >= 99.0
        and row["hard_negative_false_positive_rate"] < raw_dev["hard_negative_false_positive_rate"]
        and row["ambiguous_movement_rate"] < raw_dev["ambiguous_movement_rate"]
        and row["top_3_cluster_share_simulation"] <= 80.0
    ]
    ordered = viable if viable else enriched
    ordered = sorted(
        ordered,
        key=lambda row: (
            -float(row["selection_score"]),
            -float(row["positive_recall"]),
            float(row["hard_negative_false_positive_rate"]),
            float(row["ambiguous_movement_rate"]),
        ),
    )
    if not ordered:
        raise SystemExit("No variant rows were produced")
    return ordered[0]


def build_variant_report(root_dir: Path) -> dict[str, Any]:
    """Build the full variant simulation report without changing production behavior."""
    bundle, frame = _prepare_simulation_frame(root_dir)
    p4_variant = str(bundle["persona_04_like_variant_id"])
    p1_variant = str(bundle["persona_01_like_variant_id"])

    dev_df = _load_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_dev.csv")
    eval_locked_df = _load_csv(root_dir / "artifacts" / "curation" / "reconciliation_signoff_eval_locked.csv")

    baseline_dev = _summarize_assignment(frame, "persona_id_current", dev_df, "persona_04", "persona_01")
    baseline_eval = _summarize_assignment(frame, "persona_id_current", eval_locked_df, "persona_04", "persona_01")
    raw_dev = _summarize_assignment(frame, "reconcile_boost_persona_id", dev_df, p4_variant, p1_variant)
    raw_eval = _summarize_assignment(frame, "reconcile_boost_persona_id", eval_locked_df, p4_variant, p1_variant)

    variant_frames: dict[str, pd.DataFrame] = {}
    variant_rows: list[dict[str, Any]] = []
    for variant in _variant_definitions():
        variant_frame = _apply_variant(bundle, frame, variant)
        variant_frames[variant.variant_id] = variant_frame
        variant_rows.append(_variant_dev_summary(variant, variant_frame, dev_df, p4_variant, p1_variant))

    best_variant = _choose_best_variant(variant_rows, baseline_dev, raw_dev)
    best_variant_id = str(best_variant["variant_id"])
    selected_frame = variant_frames[best_variant_id]
    selected_eval = _summarize_assignment(selected_frame, "variant_persona_id", eval_locked_df, p4_variant, p1_variant)

    implementation_ready = (
        best_variant["positive_recall"] > baseline_dev["positive_recall"]
        and best_variant["parent_retention"] >= 99.0
        and best_variant["hard_negative_false_positive_rate"] < raw_dev["hard_negative_false_positive_rate"]
        and best_variant["ambiguous_movement_rate"] < raw_dev["ambiguous_movement_rate"]
        and best_variant["top_3_cluster_share_simulation"] <= 80.0
        and selected_eval["positive_recall"] > baseline_eval["positive_recall"]
        and selected_eval["parent_retention"] >= 99.0
        and selected_eval["hard_negative_false_positive_rate"] < raw_eval["hard_negative_false_positive_rate"]
        and selected_eval["ambiguous_movement_rate"] < raw_eval["ambiguous_movement_rate"]
    )

    if implementation_ready:
        failure_mode = ""
        recommendation = (
            "Smallest candidate production change would be a post-feature movement guard that "
            "penalizes helpdesk/docs/setup-style rows before they can join the persona_04-like cluster."
        )
    else:
        failure_mode = (
            "Best dev variant still leaves too much hard-negative or ambiguous over-expansion, "
            "or it does not hold up on the locked eval split."
        )
        recommendation = "Do not implement yet; continue with dev-only tuning and keep eval_locked as holdout."

    report = {
        "baseline_vs_reconcile_boost": {
            "dev": {
                "baseline": baseline_dev,
                "reconcile_boost": raw_dev,
            },
            "eval_locked": {
                "baseline": baseline_eval,
                "reconcile_boost": raw_eval,
            },
        },
        "dev_variant_comparison": variant_rows,
        "selected_best_dev_variant": best_variant,
        "selected_variant_eval_locked": selected_eval,
        "implementation_ready": implementation_ready,
        "failure_mode_if_not_ready": failure_mode,
        "recommendation": recommendation,
        "variant_metadata": {
            "persona_04_like_variant_id": p4_variant,
            "persona_01_like_variant_id": p1_variant,
        },
    }
    return report


def main() -> None:
    """Run the variant evaluation and write one report artifact."""
    report = build_variant_report(ROOT_DIR)
    output_path = ROOT_DIR / "artifacts" / "curation" / "reconciliation_signoff_variant_eval.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report_path": str(output_path), "selected_variant": report["selected_best_dev_variant"]["variant_id"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
