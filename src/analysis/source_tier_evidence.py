"""Tier-aware deck-ready evidence accounting for workbook-facing analysis outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.source_tiers import annotate_source_tiers


ROOT_DECK_READY_EVIDENCE_TIER_COUNTS_ARTIFACT = "artifacts/readiness/deck_ready_evidence_tier_counts.json"
ROOT_PERSONA_EVIDENCE_TIER_BREAKDOWN_ARTIFACT = "artifacts/readiness/persona_evidence_tier_breakdown.csv"

TIER_TO_PREFIX = {
    "core_representative_source": "core_representative",
    "supporting_validation_source": "supporting_validation",
    "exploratory_edge_source": "exploratory_edge",
    "excluded_from_deck_ready_core": "excluded_from_deck_ready_core",
}

GLOBAL_LABELED_FIELDS = {
    "core_representative_source": "deck_ready_core_labeled_row_count",
    "supporting_validation_source": "supporting_validation_labeled_row_count",
    "exploratory_edge_source": "exploratory_edge_labeled_row_count",
    "excluded_from_deck_ready_core": "excluded_from_deck_ready_core_labeled_row_count",
}

GLOBAL_PERSONA_CORE_FIELDS = {
    "core_representative_source": "deck_ready_core_persona_core_row_count",
    "supporting_validation_source": "supporting_validation_persona_core_row_count",
    "exploratory_edge_source": "exploratory_edge_persona_core_row_count",
    "excluded_from_deck_ready_core": "excluded_from_deck_ready_core_persona_core_row_count",
}


def build_source_tier_evidence_outputs(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame,
    persona_summary_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
) -> dict[str, Any]:
    """Build global and persona-level evidence accounting by source tier."""
    episode_sources = _episode_sources(episodes_df)
    labeled_with_source = _labeled_with_source(labeled_df, episode_sources)
    persona_assignments_with_source = _persona_assignments_with_source(persona_assignments_df, episode_sources)

    global_counts = _global_evidence_counts(labeled_with_source)
    persona_breakdown_df = _persona_tier_breakdown(persona_assignments_with_source)
    persona_breakdown_df = _apply_claim_anchor_diagnostics(persona_breakdown_df)

    persona_summary_with_tiers = _merge_persona_breakdown(persona_summary_df, persona_breakdown_df)
    cluster_stats_with_tiers = _merge_persona_breakdown(cluster_stats_df, persona_breakdown_df)

    focus_personas = _focus_persona_diagnostics(persona_breakdown_df, cluster_stats_with_tiers)
    report = {
        "global_tier_evidence_counts": global_counts,
        "persona_claim_anchor_diagnostics": focus_personas,
        "persona_count_invariants": {
            "final_usable_persona_count": int(
                pd.to_numeric(
                    cluster_stats_with_tiers.get("final_usable_persona", pd.Series(dtype=bool)).fillna(False).astype(bool),
                    errors="coerce",
                ).fillna(0).astype(int).sum()
            )
            if not cluster_stats_with_tiers.empty
            else 0,
            "production_ready_persona_count": int(
                cluster_stats_with_tiers.get("production_ready_persona", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()
            )
            if not cluster_stats_with_tiers.empty
            else 0,
            "review_ready_persona_count": int(
                cluster_stats_with_tiers.get("review_ready_persona", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()
            )
            if not cluster_stats_with_tiers.empty
            else 0,
        },
    }
    return {
        "report": report,
        "persona_breakdown_df": persona_breakdown_df,
        "persona_summary_df": persona_summary_with_tiers,
        "cluster_stats_df": cluster_stats_with_tiers,
        "global_counts": global_counts,
    }


def write_source_tier_evidence_artifacts(root_dir: Path, report: dict[str, Any], persona_breakdown_df: pd.DataFrame) -> dict[str, Path]:
    """Write Phase 2 deck-ready evidence accounting artifacts."""
    json_path = root_dir / ROOT_DECK_READY_EVIDENCE_TIER_COUNTS_ARTIFACT
    csv_path = root_dir / ROOT_PERSONA_EVIDENCE_TIER_BREAKDOWN_ARTIFACT
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    persona_breakdown_df.to_csv(csv_path, index=False)
    return {"json_path": json_path, "csv_path": csv_path}


def _episode_sources(episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Return one episode-to-source lookup annotated with source tiers."""
    if episodes_df.empty or not {"episode_id", "source"}.issubset(episodes_df.columns):
        return annotate_source_tiers(pd.DataFrame(columns=["episode_id", "source"]))
    return annotate_source_tiers(
        episodes_df[["episode_id", "source"]].drop_duplicates(subset=["episode_id"], keep="first").copy()
    )


def _labeled_with_source(labeled_df: pd.DataFrame, episode_sources: pd.DataFrame) -> pd.DataFrame:
    """Attach source-tier annotations to labeled rows."""
    if labeled_df.empty:
        return pd.DataFrame(columns=["episode_id", "persona_core_eligible", "source", "source_tier"])
    labeled = labeled_df.copy()
    if "persona_core_eligible" not in labeled.columns:
        labeled["persona_core_eligible"] = False
    return labeled.merge(episode_sources, on="episode_id", how="left")


def _persona_assignments_with_source(persona_assignments_df: pd.DataFrame, episode_sources: pd.DataFrame) -> pd.DataFrame:
    """Attach source-tier annotations to persona-assigned core rows."""
    if persona_assignments_df.empty:
        return pd.DataFrame(columns=["episode_id", "persona_id", "source", "source_tier"])
    assignments = persona_assignments_df.copy()
    return assignments.merge(episode_sources, on="episode_id", how="left")


def _global_evidence_counts(labeled_with_source: pd.DataFrame) -> dict[str, int]:
    """Compute global labeled and persona-core row counts by source tier."""
    counts = {field: 0 for field in [*GLOBAL_LABELED_FIELDS.values(), *GLOBAL_PERSONA_CORE_FIELDS.values()]}
    if labeled_with_source.empty:
        return counts
    labeled = labeled_with_source.copy()
    labeled["persona_core_eligible"] = labeled["persona_core_eligible"].fillna(False).astype(bool)
    for tier, field in GLOBAL_LABELED_FIELDS.items():
        counts[field] = int(labeled["source_tier"].astype(str).eq(tier).sum())
    core_only = labeled[labeled["persona_core_eligible"]].copy()
    for tier, field in GLOBAL_PERSONA_CORE_FIELDS.items():
        counts[field] = int(core_only["source_tier"].astype(str).eq(tier).sum())
    return counts


def _persona_tier_breakdown(persona_assignments_with_source: pd.DataFrame) -> pd.DataFrame:
    """Compute per-persona source-tier counts and shares over persona-core rows."""
    columns = [
        "persona_id",
        "total_persona_core_rows",
        "core_representative_persona_core_rows",
        "supporting_validation_persona_core_rows",
        "exploratory_edge_persona_core_rows",
        "excluded_from_deck_ready_core_persona_core_rows",
        "core_representative_share_of_persona_core",
        "supporting_validation_share_of_persona_core",
        "exploratory_edge_share_of_persona_core",
        "excluded_share_of_persona_core",
    ]
    if persona_assignments_with_source.empty:
        return pd.DataFrame(columns=columns)

    grouped = (
        persona_assignments_with_source.groupby(["persona_id", "source_tier"], dropna=False)["episode_id"]
        .nunique()
        .reset_index(name="persona_core_rows")
    )
    wide = (
        grouped.pivot(index="persona_id", columns="source_tier", values="persona_core_rows")
        .fillna(0)
        .reset_index()
    )
    for tier in TIER_TO_PREFIX:
        if tier not in wide.columns:
            wide[tier] = 0
    wide["total_persona_core_rows"] = sum(pd.to_numeric(wide.get(tier, 0), errors="coerce").fillna(0).astype(int) for tier in TIER_TO_PREFIX)
    for tier, prefix in TIER_TO_PREFIX.items():
        count_column = f"{prefix}_persona_core_rows"
        wide[count_column] = pd.to_numeric(wide.get(tier, 0), errors="coerce").fillna(0).astype(int)
    wide["core_representative_share_of_persona_core"] = wide.apply(
        lambda row: _safe_share(row["core_representative_persona_core_rows"], row["total_persona_core_rows"]),
        axis=1,
    )
    wide["supporting_validation_share_of_persona_core"] = wide.apply(
        lambda row: _safe_share(row["supporting_validation_persona_core_rows"], row["total_persona_core_rows"]),
        axis=1,
    )
    wide["exploratory_edge_share_of_persona_core"] = wide.apply(
        lambda row: _safe_share(row["exploratory_edge_persona_core_rows"], row["total_persona_core_rows"]),
        axis=1,
    )
    wide["excluded_share_of_persona_core"] = wide.apply(
        lambda row: _safe_share(row["excluded_from_deck_ready_core_persona_core_rows"], row["total_persona_core_rows"]),
        axis=1,
    )
    return wide[columns].sort_values("persona_id").reset_index(drop=True)


def _apply_claim_anchor_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    """Add deck-ready claim anchoring diagnostics to the persona-tier breakdown."""
    if df.empty:
        frame = df.copy()
        for column in [
            "has_core_representative_anchor",
            "core_anchor_strength",
            "supporting_validation_strength",
            "exploratory_dependency_risk",
            "excluded_source_dependency_risk",
            "deck_ready_claim_evidence_status",
        ]:
            frame[column] = pd.Series(dtype=object)
        return frame
    frame = df.copy()
    frame["has_core_representative_anchor"] = frame["core_representative_persona_core_rows"].astype(int).gt(0)
    frame["core_anchor_strength"] = frame.apply(
        lambda row: _strength_label(
            count=int(row["core_representative_persona_core_rows"]),
            share=float(row["core_representative_share_of_persona_core"]),
        ),
        axis=1,
    )
    frame["supporting_validation_strength"] = frame.apply(
        lambda row: _strength_label(
            count=int(row["supporting_validation_persona_core_rows"]),
            share=float(row["supporting_validation_share_of_persona_core"]),
        ),
        axis=1,
    )
    frame["exploratory_dependency_risk"] = frame["exploratory_edge_share_of_persona_core"].map(_dependency_risk_label)
    frame["excluded_source_dependency_risk"] = frame["excluded_share_of_persona_core"].map(_dependency_risk_label)
    frame["deck_ready_claim_evidence_status"] = frame.apply(_claim_evidence_status, axis=1)
    return frame


def _merge_persona_breakdown(frame: pd.DataFrame, breakdown_df: pd.DataFrame) -> pd.DataFrame:
    """Merge persona-tier breakdown fields into one persona-facing output frame."""
    if frame.empty:
        return frame.copy()
    merge_columns = [column for column in breakdown_df.columns if column != "persona_id"]
    cleaned = frame.drop(columns=[column for column in merge_columns if column in frame.columns], errors="ignore")
    return cleaned.merge(breakdown_df, on="persona_id", how="left")


def _focus_persona_diagnostics(breakdown_df: pd.DataFrame, cluster_stats_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Return anchoring diagnostics for production-ready and review-ready personas."""
    if breakdown_df.empty or cluster_stats_df.empty:
        return []
    focus = cluster_stats_df[
        cluster_stats_df.get("production_ready_persona", pd.Series(dtype=bool)).fillna(False).astype(bool)
        | cluster_stats_df.get("review_ready_persona", pd.Series(dtype=bool)).fillna(False).astype(bool)
    ].copy()
    if focus.empty:
        return []
    columns = [
        "persona_id",
        "total_persona_core_rows",
        "core_representative_persona_core_rows",
        "supporting_validation_persona_core_rows",
        "exploratory_edge_persona_core_rows",
        "excluded_from_deck_ready_core_persona_core_rows",
        "core_representative_share_of_persona_core",
        "supporting_validation_share_of_persona_core",
        "exploratory_edge_share_of_persona_core",
        "excluded_share_of_persona_core",
        "has_core_representative_anchor",
        "core_anchor_strength",
        "supporting_validation_strength",
        "exploratory_dependency_risk",
        "excluded_source_dependency_risk",
        "deck_ready_claim_evidence_status",
    ]
    merged = focus[["persona_id", "production_ready_persona", "review_ready_persona", "readiness_tier"]].merge(
        breakdown_df[columns],
        on="persona_id",
        how="left",
    )
    return merged.sort_values("persona_id").to_dict(orient="records")


def _safe_share(count: int, total: int) -> float:
    """Return one rounded share over persona-core rows."""
    if total <= 0:
        return 0.0
    return round(float(count) / float(total), 4)


def _strength_label(count: int, share: float) -> str:
    """Label evidence strength from one count/share pair."""
    if count <= 0:
        return "none"
    if count >= 100 or share >= 0.6:
        return "strong"
    if count >= 30 or share >= 0.3:
        return "moderate"
    return "weak"


def _dependency_risk_label(share: float) -> str:
    """Label dependency risk from one source-tier share."""
    if share >= 0.5:
        return "high"
    if share >= 0.2:
        return "medium"
    return "low"


def _claim_evidence_status(row: pd.Series) -> str:
    """Classify one persona's deck-ready claim evidence posture."""
    core_strength = str(row.get("core_anchor_strength", "none"))
    supporting_strength = str(row.get("supporting_validation_strength", "none"))
    exploratory_share = float(row.get("exploratory_edge_share_of_persona_core", 0.0) or 0.0)
    excluded_share = float(row.get("excluded_share_of_persona_core", 0.0) or 0.0)
    if core_strength in {"strong", "moderate"}:
        return "core_anchored"
    if core_strength == "weak" and supporting_strength in {"strong", "moderate", "weak"}:
        return "supporting_validated"
    if core_strength == "none" and (supporting_strength != "none" or exploratory_share > 0.0 or excluded_share > 0.0):
        return "exploratory_dependent"
    return "insufficient_core_evidence"
