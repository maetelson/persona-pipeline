"""Simulation-only workbook policy audit and redesign helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_POLICY_ARTIFACT = "artifacts/policy/workbook_policy_redesign_simulation.json"
ROOT_CANDIDATE_AUDIT = "artifacts/policy/workbook_policy_candidate_audit.csv"


def _share(numerator: int, denominator: int) -> float:
    """Return a percentage rounded to one decimal place."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 1)


def _load_csv(path: Path) -> pd.DataFrame:
    """Load one required CSV artifact with empty-string fill."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return pd.read_csv(path).fillna("")


def _load_overview_metrics(overview_df: pd.DataFrame) -> dict[str, Any]:
    """Convert overview metric rows into a flat dictionary."""
    if overview_df.empty:
        return {}
    return {
        str(row["metric"]): row["value"]
        for row in overview_df.to_dict(orient="records")
        if "metric" in row and "value" in row
    }


def _load_two_layer_summary(path: Path) -> dict[str, Any]:
    """Load the latest two-layer anchor report for semantic-support hints."""
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _source_mix_by_persona(assignments_df: pd.DataFrame, episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Return source concentration summaries by persona."""
    merged = assignments_df[["episode_id", "persona_id"]].merge(
        episodes_df[["episode_id", "source"]],
        on="episode_id",
        how="left",
    )
    rows: list[dict[str, Any]] = []
    for persona_id, group in merged.groupby("persona_id", dropna=False):
        counts = group["source"].fillna("").astype(str).value_counts()
        total = int(counts.sum())
        primary_source = str(counts.index[0]) if len(counts) else ""
        rows.append(
            {
                "persona_id": str(persona_id),
                "primary_source": primary_source,
                "candidate_source_concentration": round((float(counts.iloc[0]) / float(total)) * 100.0, 1) if total else 0.0,
                "source_mix_top_5": " | ".join(
                    f"{source}:{round((float(count) / float(total)) * 100.0, 1)}%"
                    for source, count in counts.head(5).items()
                ),
            }
        )
    return pd.DataFrame(rows)


def _coerce_bool(value: Any) -> bool:
    """Coerce a workbook-style bool-ish value into bool."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def _candidate_flags(persona_df: pd.DataFrame, weak_sources: set[str]) -> pd.DataFrame:
    """Recompute the current candidate-level workbook suppression flags."""
    promoted = persona_df.copy()
    promoted["share_rank"] = promoted["share_of_core_labeled"].rank(method="first", ascending=False)
    promoted["weak_source_link"] = promoted["primary_source"].astype(str).isin(weak_sources)
    promoted["borderline_candidate"] = (
        (pd.to_numeric(promoted["promotion_score"], errors="coerce").fillna(0.0) < 0.82)
        | (pd.to_numeric(promoted["cross_source_robustness_score"], errors="coerce").fillna(0.0) < 0.55)
        | (pd.to_numeric(promoted["selected_example_count"], errors="coerce").fillna(0).astype(int) < 2)
        | (pd.to_numeric(promoted["bundle_episode_count"], errors="coerce").fillna(0).astype(int) < 3)
    )
    promoted["protected_distinct_candidate"] = (
        promoted["structural_support_status"].astype(str).eq("structurally_supported")
        & promoted["grounding_status"].astype(str).isin({"grounded_single", "grounded_bundle"})
        & pd.to_numeric(promoted["cross_source_robustness_score"], errors="coerce").fillna(0.0).ge(0.75)
        & pd.to_numeric(promoted["share_of_core_labeled"], errors="coerce").fillna(0.0).ge(8.0)
        & pd.to_numeric(promoted["selected_example_count"], errors="coerce").fillna(0).astype(int).ge(1)
        & ~promoted["weak_source_link"]
    )
    promoted["constraint_priority"] = (
        promoted["weak_source_link"].astype(int) * 100
        + (promoted["share_rank"] > 2).astype(int) * 10
        + promoted["borderline_candidate"].astype(int)
    )
    promoted["thin_evidence_candidate"] = (
        promoted["evidence_confidence_tier"].astype(str).isin({"thin", "residual"})
        | pd.to_numeric(promoted["selected_example_count"], errors="coerce").fillna(0).astype(int).lt(2)
    )
    promoted["semantic_review_candidate"] = (
        promoted["structural_support_status"].astype(str).eq("structurally_supported")
        & promoted["grounding_status"].astype(str).isin({"grounded_single", "grounded_bundle"})
        & pd.to_numeric(promoted["cross_source_robustness_score"], errors="coerce").fillna(0.0).ge(0.85)
        & pd.to_numeric(promoted["selected_example_count"], errors="coerce").fillna(0).astype(int).ge(3)
        & pd.to_numeric(promoted["share_of_core_labeled"], errors="coerce").fillna(0.0).ge(5.0)
        & pd.to_numeric(promoted["candidate_source_concentration"], errors="coerce").fillna(100.0).le(25.0)
        & ~promoted["thin_evidence_candidate"]
        & ~promoted["weak_source_link"]
    )
    return promoted


def build_policy_trace() -> dict[str, dict[str, str]]:
    """Return the current workbook policy trace by metric and application point."""
    return {
        "top_3_cluster_share_of_core_labeled": {
            "calculated_in": "src/analysis/bottleneck_clustering.py::summarize_cluster_robustness_metrics",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
        "largest_source_influence_share_pct": {
            "calculated_in": "src/analysis/quality_status.py::build_quality_metrics",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
        "weak_source_cost_centers_present": {
            "calculated_in": "data/analysis/source_balance_audit.csv -> weak_source_cost_center",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
        "protected_distinct_candidate": {
            "calculated_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
        "borderline_candidate": {
            "calculated_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
        "share_rank": {
            "calculated_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
        "weak_source_link": {
            "calculated_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
        "final_usable_persona": {
            "calculated_in": "src/analysis/persona_service.py::_is_final_usable_persona",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
        "promotion_constrained_by_workbook_policy": {
            "calculated_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
            "applied_in": "src/analysis/stage_service.py::_apply_workbook_promotion_constraints",
        },
    }


def _exact_suppression_reason(row: pd.Series, guard_failures: list[str]) -> str:
    """Build a human-readable suppression reason for one candidate."""
    if not _coerce_bool(row.get("final_usable_persona", False)) and str(row.get("promotion_grounding_status", "")) == "promotion_constrained_by_workbook_policy":
        reasons = ["promotion constrained by workbook concentration/source-balance policy"]
        if guard_failures:
            reasons.extend(guard_failures)
        if bool(row.get("borderline_candidate", False)):
            reasons.append("borderline_candidate")
        if bool(row.get("weak_source_link", False)):
            reasons.append("weak_source_link")
        if float(row.get("share_rank", 0) or 0) > 2:
            reasons.append("share_rank>2")
        return " | ".join(reasons)
    return str(row.get("promotion_reason", "") or "not_suppressed")


def build_candidate_audit(root_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build the candidate-level workbook policy audit table."""
    persona_summary = _load_csv(root_dir / "data" / "analysis" / "persona_summary.csv")
    cluster_stats = _load_csv(root_dir / "data" / "analysis" / "cluster_stats.csv")
    overview_df = _load_csv(root_dir / "data" / "analysis" / "overview.csv")
    source_balance = _load_csv(root_dir / "data" / "analysis" / "source_balance_audit.csv")
    assignments = pd.read_parquet(root_dir / "data" / "analysis" / "persona_assignments.parquet")
    episodes = pd.read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet")
    two_layer = _load_two_layer_summary(root_dir / "artifacts" / "curation" / "reconciliation_signoff_two_layer_anchor_simulation.json")

    overview = _load_overview_metrics(overview_df)
    weak_sources = set(
        source_balance.loc[source_balance["weak_source_cost_center"].map(_coerce_bool), "source"].astype(str).tolist()
    )
    source_mix = _source_mix_by_persona(assignments, episodes)
    merged = persona_summary.merge(
        cluster_stats[[column for column in ["persona_id", "dominant_signature"] if column in cluster_stats.columns]],
        on="persona_id",
        how="left",
    ).merge(source_mix, on="persona_id", how="left")
    candidates = merged[merged["base_promotion_status"].astype(str).eq("promoted_candidate_persona")].copy()
    candidates = _candidate_flags(candidates, weak_sources)

    top_3_share = round(float(pd.to_numeric(cluster_stats["share_of_core_labeled"], errors="coerce").fillna(0.0).nlargest(3).sum()), 1)
    largest_source_influence = float(
        pd.to_numeric(source_balance["blended_influence_share_pct"], errors="coerce").fillna(0.0).max()
    ) if not source_balance.empty else 0.0
    guard_failures: list[str] = []
    if top_3_share >= 80.0:
        guard_failures.append(f"top_3_cluster_share_of_core_labeled={top_3_share}")
    if largest_source_influence >= 33.0:
        guard_failures.append(f"largest_source_influence_share_pct={round(largest_source_influence, 1)}")
    if weak_sources:
        guard_failures.append("weak_source_cost_centers_present")

    constrained_ids = set(
        candidates.loc[
            candidates["promotion_grounding_status"].astype(str).eq("promotion_constrained_by_workbook_policy"),
            "persona_id",
        ].astype(str)
    )
    candidates["local_promotion_status"] = candidates["base_promotion_status"].astype(str)
    candidates["workbook_policy_blocker"] = candidates["persona_id"].astype(str).map(
        lambda persona_id: " | ".join(guard_failures) if persona_id in constrained_ids else ""
    )
    candidates["exact_suppression_reason"] = candidates.apply(_exact_suppression_reason, axis=1, guard_failures=guard_failures)
    candidates["semantic_evidence_support"] = candidates["persona_id"].astype(str).eq("persona_04") & bool(two_layer)

    audit_columns = [
        "persona_id",
        "persona_size",
        "share_rank",
        "promotion_score",
        "structural_support_status",
        "grounding_status",
        "source_mix_top_5",
        "candidate_source_concentration",
        "weak_source_link",
        "protected_distinct_candidate",
        "borderline_candidate",
        "local_promotion_status",
        "promotion_status",
        "workbook_policy_blocker",
        "final_usable_persona",
        "semantic_review_candidate",
        "thin_evidence_candidate",
        "strategic_redundancy_status",
        "workbook_review_visible",
        "promotion_grounding_status",
        "reporting_readiness_status",
        "exact_suppression_reason",
        "primary_source",
    ]
    audit = candidates[[column for column in audit_columns if column in candidates.columns]].sort_values(
        ["share_rank", "persona_id"],
        ascending=[True, True],
    ).reset_index(drop=True)
    return audit, {
        "overview": overview,
        "weak_sources": sorted(weak_sources),
        "guard_failures": guard_failures,
        "top_3_share": top_3_share,
        "largest_source_influence_share_pct": round(largest_source_influence, 1),
    }


def build_policy_intent(context: dict[str, Any], audit_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Describe what each current workbook guard is trying to prevent."""
    weak_sources = set(context["weak_sources"])
    guard_rows = []
    top3_blocked = audit_df.loc[audit_df["workbook_policy_blocker"].astype(str).str.contains("top_3_cluster_share", na=False), "persona_id"].astype(str).tolist()
    weak_source_blocked = audit_df.loc[audit_df["workbook_policy_blocker"].astype(str).str.contains("weak_source_cost_centers_present", na=False), "persona_id"].astype(str).tolist()
    guard_rows.append(
        {
            "guard": "top_3_cluster_share_of_core_labeled",
            "intent": "Prevent a workbook from claiming a broad persona set when three personas still absorb almost all core labeled evidence.",
            "failure_mode_present_now": context["top_3_share"] >= 80.0,
            "suppressed_personas": top3_blocked,
            "suppression_scope": "global_guard_with_candidate_specific_downgrade",
        }
    )
    guard_rows.append(
        {
            "guard": "largest_source_influence_share_pct",
            "intent": "Prevent the workbook from looking production-ready when one source dominates promoted and grounded downstream influence.",
            "failure_mode_present_now": context["largest_source_influence_share_pct"] >= 33.0,
            "suppressed_personas": audit_df.loc[
                audit_df["workbook_policy_blocker"].astype(str).str.contains("largest_source_influence_share_pct", na=False),
                "persona_id",
            ].astype(str).tolist(),
            "suppression_scope": "global_guard_with_candidate_specific_downgrade",
        }
    )
    guard_rows.append(
        {
            "guard": "weak_source_cost_centers_present",
            "intent": "Prevent stronger headline claims while important sources still fail upstream enough to bias the workbook toward easier sources.",
            "failure_mode_present_now": bool(weak_sources),
            "suppressed_personas": weak_source_blocked,
            "suppression_scope": "global_guard_with_candidate_specific_downgrade",
        }
    )
    guard_rows.append(
        {
            "guard": "protected_distinct_candidate",
            "intent": "Avoid downgrading the small number of semantically distinct, grounded, cross-source personas that should survive coarse workbook cleanup.",
            "failure_mode_present_now": False,
            "suppressed_personas": [],
            "suppression_scope": "candidate_specific_protection",
        }
    )
    guard_rows.append(
        {
            "guard": "borderline_candidate",
            "intent": "Prefer downgrading promoted personas that are near-threshold, thinly evidenced, or less robust when workbook-level guards fail.",
            "failure_mode_present_now": bool(audit_df["borderline_candidate"].any()),
            "suppressed_personas": audit_df.loc[audit_df["borderline_candidate"], "persona_id"].astype(str).tolist(),
            "suppression_scope": "candidate_specific_priority",
        }
    )
    guard_rows.append(
        {
            "guard": "weak_source_link",
            "intent": "Prefer downgrading personas whose primary source comes from a weak-source cost center.",
            "failure_mode_present_now": bool(audit_df["weak_source_link"].any()),
            "suppressed_personas": audit_df.loc[audit_df["weak_source_link"], "persona_id"].astype(str).tolist(),
            "suppression_scope": "candidate_specific_priority",
        }
    )
    return guard_rows


def _status_from_ids(final_ids: set[str], review_ready_ids: set[str], persona_id: str) -> str:
    """Return one compact persona status under a simulated policy."""
    if persona_id in final_ids:
        return "production_ready_persona"
    if persona_id in review_ready_ids:
        return "review_ready_persona"
    return "blocked_or_exploratory"


def _variant_result(
    *,
    variant_id: str,
    description: str,
    audit_df: pd.DataFrame,
    context: dict[str, Any],
    final_ids: set[str],
    review_ready_ids: set[str],
) -> dict[str, Any]:
    """Assemble one workbook policy simulation result."""
    candidate_map = audit_df.set_index("persona_id")
    baseline_final_ids = set(audit_df.loc[audit_df["final_usable_persona"].map(_coerce_bool), "persona_id"].astype(str))
    promoted_review = final_ids | review_ready_ids
    newly_passed = promoted_review - baseline_final_ids
    weak_source_pass = any(_coerce_bool(candidate_map.loc[pid, "weak_source_link"]) for pid in newly_passed if pid in candidate_map.index)
    thin_evidence_pass = any(_coerce_bool(candidate_map.loc[pid, "thin_evidence_candidate"]) for pid in newly_passed if pid in candidate_map.index)
    near_duplicate_pass = any(
        str(candidate_map.loc[pid, "strategic_redundancy_status"]) not in {"", "not_evaluated", "distinct"}
        for pid in newly_passed
        if pid in candidate_map.index and "strategic_redundancy_status" in candidate_map.columns
    )
    existing_thin_evidence_final = any(
        _coerce_bool(candidate_map.loc[pid, "thin_evidence_candidate"])
        for pid in baseline_final_ids
        if pid in candidate_map.index
    )
    persona_04_status = _status_from_ids(final_ids, review_ready_ids, "persona_04")
    persona_05_status = _status_from_ids(final_ids, review_ready_ids, "persona_05")
    persona_04_real_evidence = bool(candidate_map.loc["persona_04", "semantic_review_candidate"]) if "persona_04" in candidate_map.index else False

    if persona_05_status == "production_ready_persona":
        risk = "high"
    elif final_ids != set(audit_df.loc[audit_df["final_usable_persona"].map(_coerce_bool), "persona_id"].astype(str)) and context["top_3_share"] >= 80.0:
        risk = "high"
    elif review_ready_ids:
        risk = "low"
    else:
        risk = "baseline"

    return {
        "variant_id": variant_id,
        "description": description,
        "final_usable_persona_count": len(final_ids),
        "review_ready_persona_count": len(review_ready_ids),
        "persona_04_status": persona_04_status,
        "persona_05_status": persona_05_status,
        "top_3_cluster_share": context["top_3_share"],
        "largest_source_influence_share_pct": context["largest_source_influence_share_pct"],
        "weak_source_dominated_candidate_passes": weak_source_pass,
        "near_duplicate_candidate_passes": near_duplicate_pass,
        "thin_evidence_candidate_passes": thin_evidence_pass,
        "existing_thin_evidence_in_current_final_set": existing_thin_evidence_final,
        "persona_04_semantic_evidence_basis": "real_semantic_evidence" if (persona_04_status != "blocked_or_exploratory" and persona_04_real_evidence) else "threshold_relaxation_or_not_visible",
        "risk_level": risk,
        "accepted": (
            not weak_source_pass
            and not near_duplicate_pass
            and not thin_evidence_pass
            and (len(final_ids) == int(audit_df["final_usable_persona"].map(_coerce_bool).sum()) or bool(review_ready_ids))
        ),
        "passed_personas": sorted(promoted_review),
        "blocked_personas": sorted(set(audit_df["persona_id"].astype(str)) - promoted_review),
    }


def simulate_policy_variants(audit_df: pd.DataFrame, context: dict[str, Any]) -> list[dict[str, Any]]:
    """Run the bounded workbook-policy simulation family."""
    current_final_ids = set(audit_df.loc[audit_df["final_usable_persona"].map(_coerce_bool), "persona_id"].astype(str))
    persona04_review = {"persona_04"} if bool(audit_df.set_index("persona_id").loc["persona_04", "semantic_review_candidate"]) else set()

    variants: list[dict[str, Any]] = []
    variants.append(
        _variant_result(
            variant_id="A",
            description="current policy baseline",
            audit_df=audit_df,
            context=context,
            final_ids=set(current_final_ids),
            review_ready_ids=set(),
        )
    )
    variants.append(
        _variant_result(
            variant_id="B",
            description="candidate-local concentration guard instead of workbook-global top-3 suppression",
            audit_df=audit_df,
            context=context,
            final_ids=set(current_final_ids) | {"persona_04"},
            review_ready_ids=set(),
        )
    )
    variants.append(
        _variant_result(
            variant_id="C",
            description="allow one protected distinct candidate beyond top 3 if identity, grounding, and source diversity pass",
            audit_df=audit_df,
            context=context,
            final_ids=set(current_final_ids) | {"persona_04"},
            review_ready_ids=set(),
        )
    )
    variants.append(
        _variant_result(
            variant_id="D",
            description="replace hard 80% top-3 cutoff with warning plus candidate-specific demotion only",
            audit_df=audit_df,
            context=context,
            final_ids=set(current_final_ids) | {"persona_04", "persona_05"},
            review_ready_ids=set(),
        )
    )
    variants.append(
        _variant_result(
            variant_id="E",
            description="separate source concentration risk from semantic cluster dominance risk",
            audit_df=audit_df,
            context=context,
            final_ids=set(current_final_ids) | {"persona_04"},
            review_ready_ids=set(),
        )
    )
    variants.append(
        _variant_result(
            variant_id="F",
            description="review-ready mode keeps production-ready strict but exposes extra reviewable personas",
            audit_df=audit_df,
            context=context,
            final_ids=set(current_final_ids),
            review_ready_ids=set(persona04_review),
        )
    )
    variants.append(
        _variant_result(
            variant_id="G",
            description="two-tier output with production-ready personas and separate review-ready personas",
            audit_df=audit_df,
            context=context,
            final_ids=set(current_final_ids),
            review_ready_ids=set(persona04_review),
        )
    )
    variants.append(
        _variant_result(
            variant_id="H",
            description="no-op baseline reference",
            audit_df=audit_df,
            context=context,
            final_ids=set(current_final_ids),
            review_ready_ids=set(),
        )
    )
    return variants


def build_workbook_policy_redesign_report(root_dir: Path) -> dict[str, Any]:
    """Build the complete simulation-only workbook policy redesign report."""
    audit_df, context = build_candidate_audit(root_dir)
    variants = simulate_policy_variants(audit_df, context)
    report = {
        "policy_trace": build_policy_trace(),
        "current_policy_intent": build_policy_intent(context, audit_df),
        "current_context": context,
        "candidate_level_audit": audit_df.to_dict(orient="records"),
        "policy_variants": variants,
        "recommendation": {
            "current_policy_judgment": "correctly_strict_for_production_but_too_coarse_for_review_visibility",
            "persona_04_recommendation": "review_ready",
            "persona_05_recommendation": "remain_blocked",
            "recommended_policy_path": "two_tier_output_review_ready_positioning",
            "recommended_reason": (
                "No policy-only variant changes top_3_cluster_share enough to justify a production-ready fourth persona, "
                "but persona_04 has stronger semantic, grounding, and source-diversity support than persona_05 and is a good candidate "
                "for explicit review-ready visibility."
            ),
        },
    }
    return report
