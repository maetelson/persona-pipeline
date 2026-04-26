"""Deterministic source-tier annotations for workbook-facing source outputs."""

from __future__ import annotations

from typing import Any

import pandas as pd


SOURCE_TIER_SPECS: dict[str, dict[str, Any]] = {
    "power_bi_community": {
        "source_tier": "core_representative_source",
        "source_membership_layer": "deck_ready_core_evidence",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": True,
        "keep_as_supporting_validation": False,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": True,
        "source_tier_reason": "Strong target-user alignment, strong BI workflow fit, and high structural importance.",
    },
    "metabase_discussions": {
        "source_tier": "core_representative_source",
        "source_membership_layer": "deck_ready_core_evidence",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": True,
        "keep_as_supporting_validation": False,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": True,
        "source_tier_reason": "Strong target-user alignment, strong BI workflow fit, and low-noise recurring interpretation evidence.",
    },
    "github_discussions": {
        "source_tier": "supporting_validation_source",
        "source_membership_layer": "supporting_validation_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": True,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Useful supporting evidence, but too developer-support-heavy to anchor deck-ready persona claims alone.",
    },
    "hubspot_community": {
        "source_tier": "supporting_validation_source",
        "source_membership_layer": "supporting_validation_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": True,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Strong workflow fit, but highly vendor-specific and better used for triangulation than anchoring.",
    },
    "reddit": {
        "source_tier": "supporting_validation_source",
        "source_membership_layer": "supporting_validation_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": True,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Useful cross-context evidence, but not stable enough to anchor deck-ready claims by itself.",
    },
    "stackoverflow": {
        "source_tier": "supporting_validation_source",
        "source_membership_layer": "supporting_validation_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": True,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Valuable supporting signal, but high developer-support bias makes it a poor deck-ready anchor.",
    },
    "shopify_community": {
        "source_tier": "supporting_validation_source",
        "source_membership_layer": "supporting_validation_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": True,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Strong workflow fit with vendor specificity; better used as supporting validation.",
    },
    "sisense_community": {
        "source_tier": "supporting_validation_source",
        "source_membership_layer": "supporting_validation_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": True,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Good evidence quality and strong workflow fit, but too platform-specific for core anchoring.",
    },
    "adobe_analytics_community": {
        "source_tier": "supporting_validation_source",
        "source_membership_layer": "supporting_validation_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": True,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "High workflow fit, high uniqueness, and high removal risk, but not core-representative enough to anchor deck-ready claims.",
    },
    "google_developer_forums": {
        "source_tier": "supporting_validation_source",
        "source_membership_layer": "supporting_validation_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": True,
        "keep_as_exploratory_edge": False,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Meaningful supporting evidence remains, but strong developer-support bias makes it unsuitable as a core deck-ready anchor.",
    },
    "mixpanel_community": {
        "source_tier": "exploratory_edge_source",
        "source_membership_layer": "exploratory_edge_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": False,
        "keep_as_exploratory_edge": True,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Small-volume platform-specific source with limited core necessity.",
    },
    "qlik_community": {
        "source_tier": "exploratory_edge_source",
        "source_membership_layer": "exploratory_edge_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": False,
        "keep_as_exploratory_edge": True,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Platform-specific source with weaker core necessity and exploratory-only value.",
    },
    "domo_community_forum": {
        "source_tier": "exploratory_edge_source",
        "source_membership_layer": "exploratory_edge_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": False,
        "keep_as_exploratory_edge": True,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Useful for breadth and balance, but not strong enough methodologically to anchor deck-ready claims.",
    },
    "klaviyo_community": {
        "source_tier": "excluded_from_deck_ready_core",
        "source_membership_layer": "raw_archive_only_sources",
        "keep_in_reviewable_release_corpus": True,
        "keep_in_deck_ready_core_evidence": False,
        "keep_as_supporting_validation": False,
        "keep_as_exploratory_edge": True,
        "keep_in_raw_archive": True,
        "deck_ready_claim_anchor_allowed": False,
        "source_tier_reason": "Already behaves like exploratory-only weak-source debt and should not anchor deck-ready interpretation.",
    },
}

SOURCE_TIER_COLUMNS = [
    "source_tier",
    "source_membership_layer",
    "keep_in_reviewable_release_corpus",
    "keep_in_deck_ready_core_evidence",
    "keep_as_supporting_validation",
    "keep_as_exploratory_edge",
    "keep_in_raw_archive",
    "deck_ready_claim_anchor_allowed",
    "source_tier_reason",
]


def source_tier_payload(source: str) -> dict[str, Any]:
    """Return the canonical tier payload for one source."""
    payload = SOURCE_TIER_SPECS.get(str(source).strip())
    if payload is None:
        raise ValueError(f"Missing deck-ready source-tier mapping for source: {source}")
    return {"source": str(source).strip(), **payload}


def annotate_source_tiers(df: pd.DataFrame, source_column: str = "source") -> pd.DataFrame:
    """Annotate one source-level frame with deterministic source-tier fields."""
    if df.empty:
        annotated = df.copy()
        for column in SOURCE_TIER_COLUMNS:
            if column not in annotated.columns:
                annotated[column] = pd.Series(dtype=object)
        return annotated
    if source_column not in df.columns:
        raise ValueError(f"Cannot annotate source tiers: missing source column '{source_column}'.")
    annotated = df.copy()
    sources = annotated[source_column].astype(str).str.strip()
    tier_df = pd.DataFrame([source_tier_payload(source) for source in sorted(sources.unique().tolist())])
    return annotated.merge(tier_df, left_on=source_column, right_on="source", how="left").drop(columns=["source_y"], errors="ignore").rename(columns={"source_x": source_column})


def source_tier_counts(df: pd.DataFrame, source_column: str = "source") -> dict[str, int]:
    """Count active workbook-visible sources by deterministic tier."""
    annotated = annotate_source_tiers(df[[source_column]].drop_duplicates().copy(), source_column=source_column)
    source_tier = annotated["source_tier"].astype(str)
    return {
        "core_representative_source_count": int(source_tier.eq("core_representative_source").sum()),
        "supporting_validation_source_count": int(source_tier.eq("supporting_validation_source").sum()),
        "exploratory_edge_source_count": int(source_tier.eq("exploratory_edge_source").sum()),
        "excluded_from_deck_ready_core_source_count": int(source_tier.eq("excluded_from_deck_ready_core").sum()),
    }
