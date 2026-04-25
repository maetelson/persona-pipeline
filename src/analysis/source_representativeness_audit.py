"""Diagnostics-only source representativeness audit for deck-ready core decisions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.quality_status import evaluate_quality_status, flatten_quality_status_result


ROOT_SOURCE_REPRESENTATIVENESS_AUDIT_ARTIFACT = "artifacts/readiness/source_representativeness_audit.json"
ROOT_SOURCE_TIER_RECOMMENDATION_ARTIFACT = "artifacts/readiness/source_tier_recommendation.csv"
ROOT_SOURCE_REPRESENTATIVENESS_POLICY_DOC = "docs/operational/SOURCE_REPRESENTATIVENESS_POLICY_DRAFT.md"


QUESTIONED_SOURCES = [
    "google_developer_forums",
    "adobe_analytics_community",
    "domo_community_forum",
    "klaviyo_community",
]

SOURCE_QUALITATIVE_PROFILES: dict[str, dict[str, str]] = {
    "power_bi_community": {
        "target_user_alignment": "strong",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "medium",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "medium",
    },
    "metabase_discussions": {
        "target_user_alignment": "strong",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "medium",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "medium",
    },
    "stackoverflow": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "medium",
        "post_dashboard_interpretation_fit": "medium",
        "vendor_specific_bias": "low",
        "developer_support_bias": "high",
        "setup_helpdesk_noise_risk": "high",
        "platform_specificity": "low",
    },
    "github_discussions": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "medium",
        "post_dashboard_interpretation_fit": "medium",
        "vendor_specific_bias": "low",
        "developer_support_bias": "high",
        "setup_helpdesk_noise_risk": "high",
        "platform_specificity": "medium",
    },
    "hubspot_community": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "high",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "high",
    },
    "shopify_community": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "high",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "high",
    },
    "sisense_community": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "high",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "high",
    },
    "qlik_community": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "high",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "high",
    },
    "mixpanel_community": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "high",
        "developer_support_bias": "medium",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "high",
    },
    "reddit": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "medium",
        "post_dashboard_interpretation_fit": "medium",
        "vendor_specific_bias": "low",
        "developer_support_bias": "medium",
        "setup_helpdesk_noise_risk": "high",
        "platform_specificity": "low",
    },
    "google_developer_forums": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "medium",
        "post_dashboard_interpretation_fit": "medium",
        "vendor_specific_bias": "high",
        "developer_support_bias": "high",
        "setup_helpdesk_noise_risk": "high",
        "platform_specificity": "high",
    },
    "adobe_analytics_community": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "high",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "high",
    },
    "domo_community_forum": {
        "target_user_alignment": "medium",
        "BI_or_analytics_workflow_alignment": "strong",
        "post_dashboard_interpretation_fit": "strong",
        "vendor_specific_bias": "high",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "medium",
        "platform_specificity": "high",
    },
    "klaviyo_community": {
        "target_user_alignment": "low",
        "BI_or_analytics_workflow_alignment": "medium",
        "post_dashboard_interpretation_fit": "medium",
        "vendor_specific_bias": "high",
        "developer_support_bias": "low",
        "setup_helpdesk_noise_risk": "high",
        "platform_specificity": "high",
    },
}


def _load_required_csv(path: Path) -> pd.DataFrame:
    """Load one required CSV artifact with blanks instead of NaN."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return pd.read_csv(path).fillna("")


def _load_required_json(path: Path) -> dict[str, Any]:
    """Load one required JSON artifact."""
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_metric_value(value: Any) -> Any:
    """Parse workbook metric strings into Python values when possible."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return ""
        return int(value) if float(value).is_integer() else float(value)
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        number = float(text)
    except ValueError:
        return text
    return int(number) if number.is_integer() else number


def _metrics_from_frame(df: pd.DataFrame) -> dict[str, Any]:
    """Convert metric/value rows into a flat dictionary."""
    return {
        str(row["metric"]): _parse_metric_value(row["value"])
        for row in df.to_dict(orient="records")
        if "metric" in row and "value" in row
    }


def _to_float(value: Any) -> float:
    """Coerce a workbook-like value into float."""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    return float(text)


def _to_int(value: Any) -> int:
    """Coerce a workbook-like value into int."""
    return int(round(_to_float(value)))


def _is_true(value: Any) -> bool:
    """Interpret bool-ish values safely."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _qualitative_score(label: str) -> int:
    """Map qualitative profile labels onto the 1-5 audit scale."""
    return {
        "strong": 5,
        "medium": 3,
        "low": 1,
        "high": 5,
        "moderate": 3,
    }.get(str(label).strip().lower(), 3)


def _noise_risk_label(row: pd.Series) -> str:
    """Convert live funnel weakness into a coarse noise-risk label."""
    ratio = _to_float(row.get("labelable_episode_ratio_pct", 0.0))
    collapse = str(row.get("collapse_stage", "") or "")
    if collapse == "relevance_prefilter" or ratio < 15.0:
        return "high"
    if collapse in {"valid_filtering", "episode_yield"} or ratio < 40.0:
        return "medium"
    return "low"


def _noise_risk_reverse_score(row: pd.Series) -> int:
    """Reverse-score noise so higher means more usable for core claims."""
    label = _noise_risk_label(row)
    return {"low": 5, "medium": 3, "high": 1}[label]


def _evidence_quality_score(row: pd.Series) -> int:
    """Score live evidence quality from retention and grounded contribution."""
    ratio = _to_float(row.get("labelable_episode_ratio_pct", 0.0))
    grounded = _to_int(row.get("grounded_promoted_persona_episode_count", 0))
    weak = _is_true(row.get("weak_source_cost_center", False))
    score = 1
    if ratio >= 80.0 and grounded >= 100:
        score = 5
    elif ratio >= 50.0 and grounded >= 50:
        score = 4
    elif ratio >= 25.0 and grounded >= 20:
        score = 3
    elif ratio >= 10.0 and grounded >= 5:
        score = 2
    if weak and score > 1:
        score -= 1
    return max(1, min(5, score))


def _target_representativeness_score(row: pd.Series) -> int:
    """Score how directly the source matches the target user set."""
    profile = SOURCE_QUALITATIVE_PROFILES.get(str(row.get("source", "")), {})
    score = _qualitative_score(profile.get("target_user_alignment", "medium"))
    if _is_true(row.get("exploratory_only_weak_source_debt", False)):
        score = max(1, score - 1)
    return score


def _semantic_fit_score(row: pd.Series) -> int:
    """Score how well the source matches BI interpretation and reporting workflows."""
    profile = SOURCE_QUALITATIVE_PROFILES.get(str(row.get("source", "")), {})
    workflow = _qualitative_score(profile.get("BI_or_analytics_workflow_alignment", "medium"))
    interpretation = _qualitative_score(profile.get("post_dashboard_interpretation_fit", "medium"))
    score = int(round((workflow + interpretation) / 2))
    if _to_int(row.get("review_ready_persona_contribution", 0)) >= 20:
        score = min(5, score + 1)
    return max(1, min(5, score))


def _uniqueness_score(row: pd.Series) -> int:
    """Score how irreplaceable the source evidence looks in the current persona set."""
    blended = _to_float(row.get("blended_influence_share_pct", 0.0))
    review_ready = _to_int(row.get("review_ready_persona_contribution", 0))
    selected_examples = _to_int(row.get("selected_example_support_count", 0))
    top_persona_count = len(list(row.get("top_persona_contributions", [])))
    score = 1
    if blended >= 8.0 or selected_examples >= 2:
        score = 4
    elif blended >= 3.0 or review_ready >= 20:
        score = 3
    elif blended >= 1.0 or review_ready > 0:
        score = 2
    if top_persona_count >= 3 and score < 5:
        score += 1
    return max(1, min(5, score))


def _core_necessity_score(row: pd.Series) -> int:
    """Score whether excluding the source would materially damage current persona structure."""
    production = _to_int(row.get("production_ready_persona_contribution", 0))
    review_ready = _to_int(row.get("review_ready_persona_contribution", 0))
    selected_examples = _to_int(row.get("selected_example_support_count", 0))
    damage = _is_true(row.get("removal_would_damage_production_ready_personas", False))
    if damage and (production >= 500 or review_ready >= 40 or selected_examples >= 2):
        return 5
    if damage and (production >= 150 or review_ready >= 15 or selected_examples >= 1):
        return 4
    if production >= 50 or review_ready >= 5:
        return 3
    if _to_float(row.get("blended_influence_share_pct", 0.0)) >= 1.0:
        return 2
    return 1


def _representativeness_tier(row: pd.Series) -> str:
    """Assign one source tier from fixed scores plus live contribution facts."""
    target = _to_int(row.get("target_representativeness_score", 0))
    semantic = _to_int(row.get("semantic_fit_score", 0))
    quality = _to_int(row.get("evidence_quality_score", 0))
    uniqueness = _to_int(row.get("uniqueness_score", 0))
    noise = _to_int(row.get("noise_risk_reverse_score", 0))
    necessity = _to_int(row.get("core_necessity_score", 0))
    reviewable = _is_true(row.get("keep_in_reviewable", False))
    raw_rows = _to_int(row.get("raw_rows", 0))
    if raw_rows == 0:
        return "archive_only"
    if _is_true(row.get("exploratory_only_weak_source_debt", False)):
        return "exclude_from_deck_ready_core"
    if str(row.get("weak_source_recommended_action", "")).strip() == "downgrade_to_exploratory_only":
        return "exclude_from_deck_ready_core"
    if target <= 1 and noise <= 1:
        return "exclude_from_deck_ready_core"
    if target >= 4 and semantic >= 4 and quality >= 3 and necessity >= 4 and noise >= 3:
        return "core_representative_source"
    if reviewable and necessity >= 3 and semantic >= 3 and quality >= 2:
        return "supporting_validation_source"
    if reviewable and (uniqueness >= 2 or quality >= 2):
        return "exploratory_edge_source"
    if reviewable:
        return "exclude_from_deck_ready_core"
    return "archive_only"


def _weighted_methodological_fit(df: pd.DataFrame) -> float:
    """Compute one weighted fit score for scenario-to-scenario comparison."""
    if df.empty:
        return 0.0
    weights = df["labeled_rows"].astype(float).clip(lower=0.0)
    score = (
        df["target_representativeness_score"].astype(float)
        + df["semantic_fit_score"].astype(float)
        + df["core_necessity_score"].astype(float)
        + df["noise_risk_reverse_score"].astype(float)
    ) / 4.0
    return round(float((score * weights).sum() / max(1.0, weights.sum())), 2)


def _load_artifacts(root_dir: Path) -> dict[str, Any]:
    """Load the current artifact set needed by the representativeness audit."""
    overview_df = _load_required_csv(root_dir / "data" / "analysis" / "overview.csv")
    quality_df = _load_required_csv(root_dir / "data" / "analysis" / "quality_checks.csv")
    source_balance_df = _load_required_csv(root_dir / "data" / "analysis" / "source_balance_audit.csv")
    source_diagnostics_df = _load_required_csv(root_dir / "data" / "analysis" / "source_diagnostics.csv")
    persona_summary_df = _load_required_csv(root_dir / "data" / "analysis" / "persona_summary.csv")
    cluster_stats_df = _load_required_csv(root_dir / "data" / "analysis" / "cluster_stats.csv")
    review_ready_gap_report = _load_required_json(root_dir / "artifacts" / "readiness" / "review_ready_gap_analysis.json")
    weak_source_decisions_df = _load_required_csv(root_dir / "artifacts" / "readiness" / "review_ready_source_decision_table.csv")
    labeled_df = pd.read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet")
    episode_df = pd.read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet", columns=["episode_id", "source"])
    assignments_df = pd.read_parquet(root_dir / "data" / "analysis" / "persona_assignments.parquet", columns=["episode_id", "persona_id"])
    persona_examples_df = _load_required_csv(root_dir / "data" / "analysis" / "persona_examples.csv")
    metrics = _metrics_from_frame(overview_df)
    metrics.update(_metrics_from_frame(quality_df))
    return {
        "overview_df": overview_df,
        "quality_df": quality_df,
        "source_balance_df": source_balance_df,
        "source_diagnostics_df": source_diagnostics_df,
        "persona_summary_df": persona_summary_df,
        "cluster_stats_df": cluster_stats_df,
        "review_ready_gap_report": review_ready_gap_report,
        "weak_source_decisions_df": weak_source_decisions_df,
        "labeled_df": labeled_df,
        "episode_df": episode_df,
        "assignments_df": assignments_df,
        "persona_examples_df": persona_examples_df,
        "metrics": metrics,
    }


def _build_source_fact_table(artifacts: dict[str, Any]) -> pd.DataFrame:
    """Build one per-source fact table from live workbook artifacts."""
    source_balance_df = artifacts["source_balance_df"].copy()
    persona_summary_df = artifacts["persona_summary_df"].copy()
    weak_source_decisions_df = artifacts["weak_source_decisions_df"].copy()
    assignments_df = artifacts["assignments_df"].copy()
    episode_df = artifacts["episode_df"].copy()
    labeled_df = artifacts["labeled_df"].copy()
    persona_examples_df = artifacts["persona_examples_df"].copy()

    persona_tier_lookup = persona_summary_df.set_index("persona_id")[
        ["production_ready_persona", "review_ready_persona", "final_usable_persona", "readiness_tier"]
    ].to_dict(orient="index")

    source_balance_df = source_balance_df.rename(
        columns={
            "raw_record_count": "raw_rows",
            "valid_post_count": "valid_rows",
            "prefiltered_valid_post_count": "prefiltered_rows",
            "episode_count": "episode_rows",
            "labeled_episode_count": "labeled_rows",
        }
    )
    source_balance_df["source"] = source_balance_df["source"].astype(str)

    source_persona_df = assignments_df.merge(episode_df, on="episode_id", how="left")
    persona_source_counts = (
        source_persona_df.groupby(["source", "persona_id"], dropna=False)["episode_id"]
        .nunique()
        .reset_index(name="persona_episode_count")
    )
    persona_sizes = source_persona_df.groupby("persona_id")["episode_id"].nunique().to_dict()
    source_core_counts = source_persona_df.groupby("source")["episode_id"].nunique().to_dict()

    production_persona_ids = {
        str(row["persona_id"])
        for row in persona_summary_df.to_dict(orient="records")
        if _is_true(row.get("production_ready_persona", False))
    }
    review_ready_persona_ids = {
        str(row["persona_id"])
        for row in persona_summary_df.to_dict(orient="records")
        if _is_true(row.get("review_ready_persona", False))
    }

    production_contrib = (
        persona_source_counts.loc[persona_source_counts["persona_id"].astype(str).isin(production_persona_ids)]
        .groupby("source")["persona_episode_count"]
        .sum()
        .to_dict()
    )
    review_contrib = (
        persona_source_counts.loc[persona_source_counts["persona_id"].astype(str).isin(review_ready_persona_ids)]
        .groupby("source")["persona_episode_count"]
        .sum()
        .to_dict()
    )

    example_counts = persona_examples_df.groupby("source")["episode_id"].nunique().to_dict()
    example_tier_counts = (
        persona_examples_df.assign(
            production_ready_persona=persona_examples_df["persona_id"].map(
                lambda persona_id: _is_true(persona_tier_lookup.get(str(persona_id), {}).get("production_ready_persona", False))
            ),
            review_ready_persona=persona_examples_df["persona_id"].map(
                lambda persona_id: _is_true(persona_tier_lookup.get(str(persona_id), {}).get("review_ready_persona", False))
            ),
        )
        .groupby("source")
        .agg(
            selected_example_support_count=("episode_id", "nunique"),
            production_ready_example_support_count=("production_ready_persona", lambda s: int(sum(bool(v) for v in s))),
            review_ready_example_support_count=("review_ready_persona", lambda s: int(sum(bool(v) for v in s))),
        )
        .to_dict(orient="index")
    )

    top_persona_payload: dict[str, list[dict[str, Any]]] = {}
    production_damage: dict[str, bool] = {}
    review_damage: dict[str, bool] = {}
    for source, group in persona_source_counts.groupby("source", dropna=False):
        ranked = group.sort_values("persona_episode_count", ascending=False).copy()
        payload: list[dict[str, Any]] = []
        for _, row in ranked.head(3).iterrows():
            persona_id = str(row["persona_id"])
            persona_total = int(persona_sizes.get(persona_id, 0) or 0)
            payload.append(
                {
                    "persona_id": persona_id,
                    "episode_count": int(row["persona_episode_count"]),
                    "share_of_persona_pct": round((int(row["persona_episode_count"]) / max(1, persona_total)) * 100.0, 1),
                    "readiness_tier": str(persona_tier_lookup.get(persona_id, {}).get("readiness_tier", "")),
                }
            )
        top_persona_payload[str(source)] = payload
        production_damage[str(source)] = bool(
            any(
                _is_true(persona_tier_lookup.get(str(pid), {}).get("production_ready_persona", False))
                and (int(count) / max(1, int(persona_sizes.get(str(pid), 0) or 0))) >= 0.05
                for pid, count in zip(group["persona_id"].astype(str), group["persona_episode_count"])
            )
            or example_tier_counts.get(str(source), {}).get("production_ready_example_support_count", 0) > 0
        )
        review_damage[str(source)] = bool(
            any(
                _is_true(persona_tier_lookup.get(str(pid), {}).get("review_ready_persona", False))
                and (int(count) / max(1, int(persona_sizes.get(str(pid), 0) or 0))) >= 0.05
                for pid, count in zip(group["persona_id"].astype(str), group["persona_episode_count"])
            )
            or example_tier_counts.get(str(source), {}).get("review_ready_example_support_count", 0) > 0
        )

    for column, lookup in {
        "persona_core_rows": source_core_counts,
        "production_ready_persona_contribution": production_contrib,
        "review_ready_persona_contribution": review_contrib,
        "selected_example_support_count": {k: v.get("selected_example_support_count", 0) for k, v in example_tier_counts.items()},
        "production_ready_example_support_count": {k: v.get("production_ready_example_support_count", 0) for k, v in example_tier_counts.items()},
        "review_ready_example_support_count": {k: v.get("review_ready_example_support_count", 0) for k, v in example_tier_counts.items()},
    }.items():
        source_balance_df[column] = source_balance_df["source"].map(lookup).fillna(0).astype(int)

    source_balance_df["top_persona_contributions"] = source_balance_df["source"].map(top_persona_payload)
    source_balance_df["source_concentration_contribution"] = source_balance_df["blended_influence_share_pct"].map(_to_float)
    source_balance_df["source_specific_noise_risk"] = source_balance_df.apply(_noise_risk_label, axis=1)
    source_balance_df["removal_would_damage_production_ready_personas"] = source_balance_df["source"].map(production_damage).fillna(False)
    source_balance_df["removal_would_damage_review_ready_personas"] = source_balance_df["source"].map(review_damage).fillna(False)

    decisions_lookup = weak_source_decisions_df.set_index("source").to_dict(orient="index") if not weak_source_decisions_df.empty else {}
    source_balance_df["weak_source_recommended_action"] = source_balance_df["source"].map(
        lambda source: str(decisions_lookup.get(str(source), {}).get("recommended_action", ""))
    )

    source_balance_df["keep_in_reviewable"] = source_balance_df["labeled_rows"].astype(int) > 0
    source_balance_df["keep_in_raw_archive"] = source_balance_df["raw_rows"].astype(int) > 0
    return source_balance_df


def _apply_representativeness_rubric(fact_df: pd.DataFrame) -> pd.DataFrame:
    """Assign qualitative dimensions, scores, and recommended tiers."""
    frame = fact_df.copy()
    for field in [
        "target_user_alignment",
        "BI_or_analytics_workflow_alignment",
        "post_dashboard_interpretation_fit",
        "vendor_specific_bias",
        "developer_support_bias",
        "setup_helpdesk_noise_risk",
        "platform_specificity",
    ]:
        frame[field] = frame["source"].map(
            lambda source: str(SOURCE_QUALITATIVE_PROFILES.get(str(source), {}).get(field, "medium"))
        )

    frame["evidence_uniqueness"] = frame.apply(
        lambda row: (
            "high"
            if _to_int(row.get("uniqueness_score", 0)) >= 4
            else "medium"
            if _to_int(row.get("uniqueness_score", 0)) >= 2
            else "low"
        ),
        axis=1,
    )
    frame["removal_risk_to_persona_structure"] = frame.apply(
        lambda row: (
            "high"
            if _is_true(row.get("removal_would_damage_production_ready_personas", False))
            or _is_true(row.get("removal_would_damage_review_ready_personas", False))
            else "medium"
            if _to_int(row.get("production_ready_persona_contribution", 0)) > 0
            or _to_int(row.get("review_ready_persona_contribution", 0)) > 0
            else "low"
        ),
        axis=1,
    )

    frame["target_representativeness_score"] = frame.apply(_target_representativeness_score, axis=1)
    frame["semantic_fit_score"] = frame.apply(_semantic_fit_score, axis=1)
    frame["evidence_quality_score"] = frame.apply(_evidence_quality_score, axis=1)
    frame["uniqueness_score"] = frame.apply(_uniqueness_score, axis=1)
    frame["noise_risk_reverse_score"] = frame.apply(_noise_risk_reverse_score, axis=1)
    frame["core_necessity_score"] = frame.apply(_core_necessity_score, axis=1)

    frame["evidence_uniqueness"] = frame["uniqueness_score"].map(
        lambda score: "high" if int(score) >= 4 else "medium" if int(score) >= 2 else "low"
    )
    frame["recommended_tier"] = frame.apply(_representativeness_tier, axis=1)
    frame["keep_in_deck_ready_core"] = frame["recommended_tier"].isin(
        {"core_representative_source", "supporting_validation_source"}
    )
    frame["remediation_needed"] = frame.apply(
        lambda row: bool(
            _is_true(row.get("weak_source_cost_center", False))
            and str(row.get("recommended_tier", "")) in {"core_representative_source", "supporting_validation_source"}
        ),
        axis=1,
    )
    frame["exclusion_risk"] = frame["removal_risk_to_persona_structure"]
    return frame


def _baseline_summary(metrics: dict[str, Any], scored_df: pd.DataFrame) -> dict[str, Any]:
    """Build one compact baseline summary from current live metrics."""
    remaining_core_weak_sources = scored_df.loc[
        scored_df["core_readiness_weak_source_cost_center"].map(_is_true), "source"
    ].astype(str).tolist()
    return {
        "persona_readiness_state": str(metrics.get("persona_readiness_state", "")),
        "overall_status": str(metrics.get("overall_status", "")),
        "quality_flag": str(metrics.get("quality_flag", "")),
        "final_usable_persona_count": _to_int(metrics.get("final_usable_persona_count", 0)),
        "production_ready_persona_count": _to_int(metrics.get("production_ready_persona_count", 0)),
        "review_ready_persona_count": _to_int(metrics.get("review_ready_persona_count", 0)),
        "weak_source_cost_center_count": _to_int(metrics.get("weak_source_cost_center_count", 0)),
        "core_readiness_weak_source_cost_center_count": _to_int(metrics.get("core_readiness_weak_source_cost_center_count", 0)),
        "effective_balanced_source_count": round(_to_float(metrics.get("effective_balanced_source_count", 0.0)), 2),
        "persona_core_coverage_of_all_labeled_pct": round(_to_float(metrics.get("persona_core_coverage_of_all_labeled_pct", 0.0)), 1),
        "largest_source_influence_share_pct": round(_to_float(metrics.get("largest_source_influence_share_pct", 0.0)), 1),
        "top_3_cluster_share_of_core_labeled": round(_to_float(metrics.get("top_3_cluster_share_of_core_labeled", 0.0)), 3),
        "remaining_core_weak_sources": remaining_core_weak_sources,
        "remaining_warnings": [
            key
            for key in [
                "effective_balanced_source_count",
                "persona_core_coverage_of_all_labeled_pct",
                "core_readiness_weak_source_cost_center_count",
            ]
            if _to_float(metrics.get(key, 0.0)) or str(metrics.get(key, "")).strip()
        ],
    }


def _questioned_source_assessments(scored_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Create the explicit judgment rows for the four questioned sources."""
    rows: list[dict[str, Any]] = []
    for source in QUESTIONED_SOURCES:
        row = scored_df.loc[scored_df["source"] == source]
        if row.empty:
            continue
        payload = row.iloc[0]
        rows.append(
            {
                "source": source,
                "is_representative_of_target_user": _to_int(payload["target_representativeness_score"]) >= 4,
                "mostly_vendor_support_or_developer_troubleshooting": (
                    _qualitative_score(str(payload.get("vendor_specific_bias", "medium"))) >= 5
                    or _qualitative_score(str(payload.get("developer_support_bias", "medium"))) >= 5
                ),
                "contributes_unique_persona_evidence": _to_int(payload["uniqueness_score"]) >= 3,
                "removing_it_would_damage_production_ready_personas": _is_true(
                    payload.get("removal_would_damage_production_ready_personas", False)
                ),
                "recommended_tier": str(payload.get("recommended_tier", "")),
                "keep_in_reviewable_release": _is_true(payload.get("keep_in_reviewable", False)),
                "keep_in_deck_ready_core": _is_true(payload.get("keep_in_deck_ready_core", False)),
                "reason": (
                    f"Target score {payload['target_representativeness_score']}, semantic fit {payload['semantic_fit_score']}, "
                    f"evidence quality {payload['evidence_quality_score']}, uniqueness {payload['uniqueness_score']}, "
                    f"noise reverse {payload['noise_risk_reverse_score']}, core necessity {payload['core_necessity_score']}."
                ),
            }
        )
    return rows


def _effective_balance_from_included(df: pd.DataFrame) -> float:
    """Recompute effective balanced source count from included blended shares."""
    shares = pd.to_numeric(df["blended_influence_share_pct"], errors="coerce").fillna(0.0)
    total = float(shares.sum())
    if total <= 0.0:
        return 0.0
    normalized = shares / total
    return round(float(1.0 / (normalized.pow(2).sum())), 2)


def _largest_source_share_from_included(df: pd.DataFrame) -> float:
    """Recompute largest-source influence after a source-core ablation."""
    shares = pd.to_numeric(df["blended_influence_share_pct"], errors="coerce").fillna(0.0)
    total = float(shares.sum())
    if total <= 0.0:
        return 0.0
    return round(float((shares / total).max() * 100.0), 1)


def _persona_loss_payload(included_sources: set[str], source_persona_counts: pd.DataFrame, persona_summary_df: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Compute persona evidence loss by persona and recomputed tier counts."""
    included = source_persona_counts.loc[source_persona_counts["source"].astype(str).isin(included_sources)].copy()
    baseline = source_persona_counts.groupby("persona_id")["persona_episode_count"].sum().to_dict()
    remaining = included.groupby("persona_id")["persona_episode_count"].sum().to_dict()
    tier_lookup = persona_summary_df.set_index("persona_id")[
        ["final_usable_persona", "production_ready_persona", "review_ready_persona"]
    ].to_dict(orient="index")
    rows: list[dict[str, Any]] = []
    final_usable = 0
    production_ready = 0
    review_ready = 0
    for persona_id, total in baseline.items():
        left = int(remaining.get(persona_id, 0) or 0)
        removed = int(total - left)
        row = {
            "persona_id": str(persona_id),
            "baseline_episode_count": int(total),
            "remaining_episode_count": left,
            "removed_episode_count": removed,
            "share_removed_pct": round((removed / max(1, int(total))) * 100.0, 1),
        }
        rows.append(row)
        if left > 0:
            if _is_true(tier_lookup.get(str(persona_id), {}).get("final_usable_persona", False)):
                final_usable += 1
            if _is_true(tier_lookup.get(str(persona_id), {}).get("production_ready_persona", False)):
                production_ready += 1
            if _is_true(tier_lookup.get(str(persona_id), {}).get("review_ready_persona", False)):
                review_ready += 1
    return sorted(rows, key=lambda item: item["persona_id"]), {
        "final_usable_persona_count": final_usable,
        "production_ready_persona_count": production_ready,
        "review_ready_persona_count": review_ready,
    }


def _promoted_example_coverage(included_sources: set[str], persona_examples_df: pd.DataFrame, persona_summary_df: pd.DataFrame) -> float:
    """Approximate example coverage after source exclusion without rerunning selection."""
    production_personas = persona_summary_df.loc[
        persona_summary_df["production_ready_persona"].map(_is_true), "persona_id"
    ].astype(str)
    if production_personas.empty:
        return 0.0
    included = persona_examples_df.loc[
        persona_examples_df["source"].astype(str).isin(included_sources)
        & persona_examples_df["persona_id"].astype(str).isin(set(production_personas))
    ]
    covered = int(included["persona_id"].astype(str).nunique())
    return round((covered / max(1, len(production_personas))) * 100.0, 1)


def _cluster_metrics_from_included(included_sources: set[str], source_persona_counts: pd.DataFrame) -> dict[str, Any]:
    """Recompute simple cluster concentration metrics under ablation."""
    included = source_persona_counts.loc[source_persona_counts["source"].astype(str).isin(included_sources)].copy()
    persona_counts = included.groupby("persona_id")["persona_episode_count"].sum().sort_values(ascending=False)
    total = int(persona_counts.sum())
    if total <= 0:
        return {
            "persona_core_labeled_rows": 0,
            "largest_cluster_share_of_core_labeled": 0.0,
            "top_3_cluster_share_of_core_labeled": 0.0,
        }
    largest = round(float(persona_counts.iloc[0] / total * 100.0), 1)
    top3 = round(float(persona_counts.head(3).sum() / total), 3)
    return {
        "persona_core_labeled_rows": total,
        "largest_cluster_share_of_core_labeled": largest,
        "top_3_cluster_share_of_core_labeled": top3,
    }


def _deck_ready_plausibility_label(flattened: dict[str, Any], baseline: dict[str, Any], persona_counts: dict[str, int]) -> str:
    """Turn scenario metrics into one practical deck-ready plausibility label."""
    baseline_gaps = 0
    scenario_gaps = 0
    for metric, target, high_is_bad in [
        ("effective_balanced_source_count", 6.0, False),
        ("persona_core_coverage_of_all_labeled_pct", 80.0, False),
        ("core_readiness_weak_source_cost_center_count", 1.0, True),
        ("largest_source_influence_share_pct", 35.0, True),
    ]:
        current = _to_float(baseline.get(metric, 0.0))
        trial = _to_float(flattened.get(metric, 0.0))
        baseline_gaps += int(current < target) if not high_is_bad else int(current > target)
        scenario_gaps += int(trial < target) if not high_is_bad else int(trial > target)
    if persona_counts["final_usable_persona_count"] < _to_int(baseline.get("final_usable_persona_count", 0)):
        return "less_plausible"
    if scenario_gaps < baseline_gaps:
        return "more_plausible"
    if scenario_gaps > baseline_gaps:
        return "less_plausible"
    return "unchanged"


def _methodological_shift_label(current: float, baseline: float) -> str:
    """Convert weighted methodological fit delta into one comparison label."""
    if current >= baseline + 0.15:
        return "improves"
    if current <= baseline - 0.15:
        return "worsens"
    return "unchanged"


def _scenario_result(
    scenario_id: str,
    description: str,
    included_sources: set[str],
    baseline_metrics: dict[str, Any],
    scored_df: pd.DataFrame,
    source_persona_counts: pd.DataFrame,
    persona_summary_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
    baseline_methodological_fit: float,
) -> dict[str, Any]:
    """Compute one ablation scenario over the current artifact-derived dataset."""
    included_df = scored_df.loc[scored_df["source"].astype(str).isin(included_sources)].copy()
    persona_loss, persona_counts = _persona_loss_payload(included_sources, source_persona_counts, persona_summary_df)
    cluster_metrics = _cluster_metrics_from_included(included_sources, source_persona_counts)
    overrides = dict(baseline_metrics)
    overrides.update(
        {
            "raw_record_rows": int(included_df["raw_rows"].astype(int).sum()),
            "valid_candidate_rows": int(included_df["valid_rows"].astype(int).sum()),
            "prefiltered_valid_rows": int(included_df["prefiltered_rows"].astype(int).sum()),
            "episode_rows": int(included_df["episode_rows"].astype(int).sum()),
            "labeled_episode_rows": int(included_df["labeled_rows"].astype(int).sum()),
            "persona_core_labeled_rows": int(cluster_metrics["persona_core_labeled_rows"]),
            "effective_balanced_source_count": _effective_balance_from_included(included_df),
            "persona_core_coverage_of_all_labeled_pct": round(
                (
                    int(cluster_metrics["persona_core_labeled_rows"])
                    / max(1, int(included_df["labeled_rows"].astype(int).sum()))
                )
                * 100.0,
                1,
            ),
            "weak_source_cost_center_count": int(included_df["weak_source_cost_center"].map(_is_true).sum()),
            "core_readiness_weak_source_cost_center_count": int(
                included_df["core_readiness_weak_source_cost_center"].map(_is_true).sum()
            ),
            "largest_source_influence_share_pct": _largest_source_share_from_included(included_df),
            "largest_cluster_share_of_core_labeled": cluster_metrics["largest_cluster_share_of_core_labeled"],
            "top_3_cluster_share_of_core_labeled": cluster_metrics["top_3_cluster_share_of_core_labeled"],
            "promoted_persona_example_coverage_pct": _promoted_example_coverage(
                included_sources, persona_examples_df, persona_summary_df
            ),
            "final_usable_persona_count": persona_counts["final_usable_persona_count"],
            "production_ready_persona_count": persona_counts["production_ready_persona_count"],
            "review_ready_persona_count": persona_counts["review_ready_persona_count"],
        }
    )
    evaluated = evaluate_quality_status(overrides)
    flattened = flatten_quality_status_result(evaluated)
    methodological_fit = _weighted_methodological_fit(included_df)
    return {
        "scenario_id": scenario_id,
        "description": description,
        "excluded_sources": sorted(set(scored_df["source"].astype(str)) - included_sources),
        "remaining_raw_rows": _to_int(overrides["raw_record_rows"]),
        "remaining_labeled_rows": _to_int(overrides["labeled_episode_rows"]),
        "remaining_source_count": int(len(included_df)),
        "final_usable_persona_count": persona_counts["final_usable_persona_count"],
        "production_ready_persona_count": persona_counts["production_ready_persona_count"],
        "review_ready_persona_count": persona_counts["review_ready_persona_count"],
        "effective_balanced_source_count": round(_to_float(overrides["effective_balanced_source_count"]), 2),
        "persona_core_coverage_of_all_labeled_pct": round(_to_float(overrides["persona_core_coverage_of_all_labeled_pct"]), 1),
        "weak_source_cost_center_count": _to_int(overrides["weak_source_cost_center_count"]),
        "core_readiness_weak_source_cost_center_count": _to_int(overrides["core_readiness_weak_source_cost_center_count"]),
        "overall_status": str(flattened.get("overall_status", "")),
        "persona_readiness_state": str(flattened.get("persona_readiness_state", "")),
        "deck_ready_candidate": str(flattened.get("persona_readiness_state", "")) == "deck_ready",
        "persona_evidence_loss_by_persona": persona_loss,
        "deck_ready_plausibility": _deck_ready_plausibility_label(flattened, baseline_metrics, persona_counts),
        "methodological_representativeness": _methodological_shift_label(methodological_fit, baseline_methodological_fit),
        "persona_standards_weakened": False,
        "junk_risk_assessment": (
            "lower"
            if _methodological_shift_label(methodological_fit, baseline_methodological_fit) == "improves"
            else "higher"
            if _to_int(overrides["production_ready_persona_count"]) < _to_int(baseline_metrics.get("production_ready_persona_count", 0))
            else "unchanged"
        ),
    }


def _scenario_simulation(
    baseline_metrics: dict[str, Any],
    scored_df: pd.DataFrame,
    source_persona_counts: pd.DataFrame,
    persona_summary_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Run the required source-core ablation scenarios."""
    all_sources = set(scored_df["source"].astype(str).tolist())
    core_sources = set(scored_df.loc[scored_df["recommended_tier"] == "core_representative_source", "source"].astype(str).tolist())
    supporting_or_core = set(
        scored_df.loc[
            scored_df["recommended_tier"].isin({"core_representative_source", "supporting_validation_source"}),
            "source",
        ].astype(str).tolist()
    )
    baseline_fit = _weighted_methodological_fit(scored_df)
    scenarios = [
        ("A_current_baseline", "Keep the full current reviewable corpus.", all_sources),
        ("B_exclude_klaviyo_from_core_only", "Remove klaviyo_community from the deck-ready core denominator only.", all_sources - {"klaviyo_community"}),
        ("C_exclude_google_from_deck_ready_core", "Remove google_developer_forums from the deck-ready core denominator.", all_sources - {"google_developer_forums"}),
        ("D_exclude_adobe_from_deck_ready_core", "Remove adobe_analytics_community from the deck-ready core denominator.", all_sources - {"adobe_analytics_community"}),
        ("E_exclude_domo_from_deck_ready_core", "Remove domo_community_forum from the deck-ready core denominator.", all_sources - {"domo_community_forum"}),
        (
            "F_exclude_google_adobe_domo",
            "Remove google_developer_forums, adobe_analytics_community, and domo_community_forum from the deck-ready core denominator.",
            all_sources - {"google_developer_forums", "adobe_analytics_community", "domo_community_forum"},
        ),
        (
            "G_exclude_all_non_core_representative_sources",
            "Keep core-representative and supporting-validation sources only.",
            supporting_or_core,
        ),
        (
            "H_keep_only_core_representative_sources",
            "Keep only sources recommended as core representative.",
            core_sources,
        ),
    ]
    return [
        _scenario_result(
            scenario_id,
            description,
            included_sources,
            baseline_metrics,
            scored_df,
            source_persona_counts,
            persona_summary_df,
            persona_examples_df,
            baseline_fit,
        )
        for scenario_id, description, included_sources in scenarios
    ]


def _recommended_next_path(scored_df: pd.DataFrame, scenarios: list[dict[str, Any]]) -> str:
    """Choose the single next path after the representativeness audit."""
    questioned = scored_df.loc[scored_df["source"].isin(QUESTIONED_SOURCES)].copy()
    if not questioned.empty and all(
        str(tier) in {"exclude_from_deck_ready_core", "exploratory_edge_source"}
        for tier in questioned["recommended_tier"].tolist()
    ):
        return "source-tier policy change"
    better_exclusion = any(
        row["scenario_id"] in {"B_exclude_klaviyo_from_core_only", "G_exclude_all_non_core_representative_sources"}
        and row["deck_ready_plausibility"] == "more_plausible"
        and row["methodological_representativeness"] == "improves"
        for row in scenarios
    )
    if better_exclusion:
        return "source-tier policy change"
    return "source-specific remediation"


def _render_policy_draft(report: dict[str, Any]) -> str:
    """Render the policy-draft markdown for analyst review."""
    lines = [
        "# Source Representativeness Policy Draft",
        "",
        "## Summary",
        "",
        f"- Current workbook readiness: `{report['baseline']['persona_readiness_state']}` / `{report['baseline']['overall_status']}`",
        f"- Recommended next implementation path: `{report['recommended_next_implementation_path']}`",
        f"- Deck-ready by source exclusion instead of source fixing: `{report['pursue_deck_ready_by_source_exclusion_instead_of_source_fixing']}`",
        f"- Weak-source remediation still worth doing: `{report['weak_source_remediation_still_worth_doing']}`",
        "",
        "## Tier Definitions",
        "",
        "- `core_representative_source`: strongly aligned with target users and recurring BI or analytics interpretation pain.",
        "- `supporting_validation_source`: useful for evidence and triangulation, but too product-specific or support-heavy to anchor deck-ready claims alone.",
        "- `exploratory_edge_source`: useful for discovery, but not required for deck-ready core readiness.",
        "- `exclude_from_deck_ready_core`: keep visible for reviewable analysis, but do not rely on it for deck-ready core claims.",
        "- `archive_only`: keep raw data for reproducibility only.",
        "",
        "## Tier Recommendations",
        "",
    ]
    for row in report["source_tier_recommendations"]:
        lines.append(
            f"- `{row['source']}` -> `{row['recommended_tier']}` | keep in reviewable `{row['keep_in_reviewable']}` | "
            f"keep in deck-ready core `{row['keep_in_deck_ready_core']}` | remediation needed `{row['remediation_needed']}`"
        )
    lines.extend(["", "## Questioned Sources", ""])
    for row in report["questioned_source_assessments"]:
        lines.append(
            f"- `{row['source']}`: representative `{row['is_representative_of_target_user']}`, mostly vendor/developer support "
            f"`{row['mostly_vendor_support_or_developer_troubleshooting']}`, unique evidence `{row['contributes_unique_persona_evidence']}`, "
            f"damage if removed `{row['removing_it_would_damage_production_ready_personas']}`, recommended tier `{row['recommended_tier']}`"
        )
    lines.extend(["", "## Ablation Summary", ""])
    for row in report["ablation_scenarios"]:
        lines.append(
            f"- `{row['scenario_id']}`: readiness `{row['persona_readiness_state']}`, deck-ready plausibility `{row['deck_ready_plausibility']}`, "
            f"methodological representativeness `{row['methodological_representativeness']}`, remaining source count `{row['remaining_source_count']}`"
        )
    lines.extend(
        [
            "",
            "## Policy Direction",
            "",
            "- Reviewable release and deck-ready core may use different source membership if the exclusion improves methodological representativeness without damaging persona structure.",
            "- Raw source archives remain intact even when a source is excluded from deck-ready core claims.",
            "- Excluding weak sources from deck-ready core is not threshold relaxation. It is a corpus-representativeness decision and must stay visible in diagnostics.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_source_representativeness_audit(root_dir: Path) -> dict[str, Any]:
    """Build the diagnostics-only source representativeness audit report."""
    artifacts = _load_artifacts(root_dir)
    fact_df = _build_source_fact_table(artifacts)
    scored_df = _apply_representativeness_rubric(fact_df)

    source_persona_counts = (
        artifacts["assignments_df"]
        .merge(artifacts["episode_df"], on="episode_id", how="left")
        .groupby(["source", "persona_id"], dropna=False)["episode_id"]
        .nunique()
        .reset_index(name="persona_episode_count")
    )

    scenarios = _scenario_simulation(
        artifacts["metrics"],
        scored_df,
        source_persona_counts,
        artifacts["persona_summary_df"],
        artifacts["persona_examples_df"],
    )
    baseline = _baseline_summary(artifacts["metrics"], scored_df)
    questioned = _questioned_source_assessments(scored_df)
    recommendations_df = scored_df[
        [
            "source",
            "recommended_tier",
            "keep_in_reviewable",
            "keep_in_deck_ready_core",
            "keep_in_raw_archive",
            "remediation_needed",
            "exclusion_risk",
        ]
    ].copy()
    recommendations_df["reason"] = scored_df.apply(
        lambda row: (
            f"target={row['target_representativeness_score']}, semantic={row['semantic_fit_score']}, "
            f"quality={row['evidence_quality_score']}, uniqueness={row['uniqueness_score']}, "
            f"noise={row['noise_risk_reverse_score']}, necessity={row['core_necessity_score']}"
        ),
        axis=1,
    )
    next_path = _recommended_next_path(scored_df, scenarios)
    pursue_by_exclusion = next_path == "source-tier policy change"
    weak_source_remediation_worth_doing = any(
        str(row["recommended_tier"]) in {"core_representative_source", "supporting_validation_source"}
        and bool(row["remediation_needed"])
        for row in scored_df.to_dict(orient="records")
    )
    report = {
        "baseline": baseline,
        "source_representativeness_scores": scored_df[
            [
                "source",
                "target_representativeness_score",
                "semantic_fit_score",
                "evidence_quality_score",
                "uniqueness_score",
                "noise_risk_reverse_score",
                "core_necessity_score",
                "target_user_alignment",
                "BI_or_analytics_workflow_alignment",
                "post_dashboard_interpretation_fit",
                "vendor_specific_bias",
                "developer_support_bias",
                "setup_helpdesk_noise_risk",
                "platform_specificity",
                "evidence_uniqueness",
                "removal_risk_to_persona_structure",
            ]
        ].to_dict(orient="records"),
        "source_fact_table": scored_df.to_dict(orient="records"),
        "source_tier_recommendations": recommendations_df.sort_values(["recommended_tier", "source"]).to_dict(orient="records"),
        "questioned_source_assessments": questioned,
        "ablation_scenarios": scenarios,
        "weak_source_remediation_still_worth_doing": weak_source_remediation_worth_doing,
        "pursue_deck_ready_by_source_exclusion_instead_of_source_fixing": pursue_by_exclusion,
        "recommended_next_implementation_path": next_path,
        "decision_summary": (
            "Prefer source-tier policy change next if exclusion improves methodological representativeness without damaging current persona structure. "
            "Otherwise continue source-specific remediation only on still-representative supporting sources."
        ),
    }
    report["policy_draft_markdown"] = _render_policy_draft(report)
    return report


def write_source_representativeness_artifacts(root_dir: Path, report: dict[str, Any]) -> dict[str, Path]:
    """Write the JSON, CSV, and markdown outputs for the representativeness audit."""
    json_path = root_dir / ROOT_SOURCE_REPRESENTATIVENESS_AUDIT_ARTIFACT
    csv_path = root_dir / ROOT_SOURCE_TIER_RECOMMENDATION_ARTIFACT
    doc_path = root_dir / ROOT_SOURCE_REPRESENTATIVENESS_POLICY_DOC
    json_path.parent.mkdir(parents=True, exist_ok=True)
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    pd.DataFrame(report["source_tier_recommendations"]).to_csv(csv_path, index=False, encoding="utf-8")
    doc_path.write_text(report["policy_draft_markdown"], encoding="utf-8")
    return {
        "json_path": json_path,
        "csv_path": csv_path,
        "doc_path": doc_path,
    }
