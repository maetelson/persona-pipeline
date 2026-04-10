"""Pre-label signal checks that separate labelable rows from low-signal noise."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.utils.pipeline_schema import contains_any_term
from src.utils.record_access import get_record_source, get_record_text

SIGNAL_COLUMNS = [
    "normalized_episode",
    "evidence_snippet",
    "business_question",
    "bottleneck_text",
    "workaround_text",
    "desired_output",
]

BUSINESS_COMMUNITY_SOURCES = {
    "shopify_community",
    "hubspot_community",
    "klaviyo_community",
    "google_ads_community",
    "merchant_center_community",
}


def build_labelability_table(episodes_df: pd.DataFrame, policy: dict[str, Any]) -> pd.DataFrame:
    """Score each episode for labelability before full LLM labeling."""
    cfg = dict(policy.get("labelability", {}) or {})
    positive_terms = [str(item) for item in cfg.get("positive_terms", []) or []]
    business_positive_terms = [str(item) for item in cfg.get("business_community_positive_terms", []) or []]
    negative_terms = [str(item) for item in cfg.get("negative_terms", []) or []]
    min_text_length = int(cfg.get("low_signal_min_text_length", 80))
    strong_threshold = int(cfg.get("strong_signal_threshold", 5))
    borderline_threshold = int(cfg.get("borderline_signal_threshold", 3))

    rows: list[dict[str, Any]] = []
    for _, row in episodes_df.iterrows():
        text = get_record_text(row, fields=SIGNAL_COLUMNS)
        source = get_record_source(row)
        lowered = text.lower()
        positive_hits = [term for term in positive_terms if term.lower() in lowered]
        if source in BUSINESS_COMMUNITY_SOURCES:
            positive_hits.extend(term for term in business_positive_terms if term.lower() in lowered)
        negative_hits = [term for term in negative_terms if term.lower() in lowered]
        score = len(set(positive_hits)) - min(len(set(negative_hits)), 3)
        if len(text.strip()) < min_text_length:
            score -= 2
        if source == "stackoverflow" and contains_any_term(lowered, ["power bi", "tableau", "excel", "dashboard", "report"]):
            score += 1
        if source == "reddit" and contains_any_term(lowered, ["every week", "stakeholder", "leadership", "follow-up", "follow up"]):
            score += 1

        positive_count = len(set(positive_hits))
        business_directional_signal = source in BUSINESS_COMMUNITY_SOURCES and positive_count >= 2 and len(text.strip()) >= min_text_length
        if score >= strong_threshold or (positive_count >= max(strong_threshold - 1, 1) and len(text.strip()) >= min_text_length):
            status = "labelable"
        elif score >= borderline_threshold or business_directional_signal:
            status = "borderline"
        else:
            status = "low_signal"

        reason_parts: list[str] = []
        if positive_hits:
            reason_parts.append(f"positive<{','.join(sorted(set(positive_hits))[:6])}>")
        if negative_hits:
            reason_parts.append(f"negative<{','.join(sorted(set(negative_hits))[:4])}>")
        if len(text.strip()) < min_text_length:
            reason_parts.append("short_text")
        if not reason_parts:
            reason_parts.append("weak_signal")
        rows.append(
            {
                "episode_id": str(row.get("episode_id", "")),
                "source": source,
                "labelability_status": status,
                "labelability_score": score,
                "labelability_reason": " | ".join(reason_parts),
                "positive_signal_count": positive_count,
                "negative_signal_count": len(set(negative_hits)),
                "persona_core_eligible": status != "low_signal",
            }
        )
    return pd.DataFrame(rows)
