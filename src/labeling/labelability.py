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
    "google_ads_help_community",
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
        force_low_signal = source == "google_ads_help_community" and contains_any_term(
            lowered,
            [
                "launched:",
                "guided help article",
                "new guided troubleshooter",
                "resources and videos",
            ],
        )
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
        score, source_reason_parts = _apply_source_labelability_adjustments(
            row=row,
            source=source,
            lowered=lowered,
            score=score,
            min_text_length=min_text_length,
        )

        positive_count = len(set(positive_hits))
        business_directional_signal = source in BUSINESS_COMMUNITY_SOURCES and positive_count >= 2 and len(text.strip()) >= min_text_length
        if source == "google_ads_help_community":
            positive_count += _google_ads_help_signal_bonus_count(row, lowered)
            business_directional_signal = (
                business_directional_signal
                or (
                    str(row.get("quality_bucket", "") or "") in {"hard_pass", "borderline"}
                    and positive_count >= 2
                    and len(text.strip()) >= min_text_length
                )
            )
        if force_low_signal:
            status = "low_signal"
        elif score >= strong_threshold or (positive_count >= max(strong_threshold - 1, 1) and len(text.strip()) >= min_text_length):
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
        if force_low_signal:
            reason_parts.append("google_ads_help_announcement_force_low_signal")
        reason_parts.extend(source_reason_parts)
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


def _apply_source_labelability_adjustments(
    row: pd.Series,
    source: str,
    lowered: str,
    score: int,
    min_text_length: int,
) -> tuple[int, list[str]]:
    """Apply source-specific labelability calibration while keeping the gate deterministic."""
    reason_parts: list[str] = []
    if source != "google_ads_help_community":
        return score, reason_parts

    quality_bucket = str(row.get("quality_bucket", "") or "")
    quality_score = float(row.get("quality_score", 0.0) or 0.0)
    if quality_bucket == "hard_pass":
        score += 2
        reason_parts.append("google_ads_help_quality_hard_pass")
    elif quality_bucket == "borderline":
        score += 1
        reason_parts.append("google_ads_help_quality_borderline")

    if contains_any_term(
        lowered,
        [
            "conversion tracking",
            "conversion action",
            "conversions",
            "impressions",
            "clicks",
            "reporting",
            "report not matching",
            "performance not matching",
            "metrics discrepancy",
            "merchant center",
            "shopping campaign",
            "google shopping",
            "zero impressions",
            "not generating impressions",
            "not showing",
        ],
    ):
        score += 1
        reason_parts.append("google_ads_help_metric_reporting_context")

    if contains_any_term(
        lowered,
        [
            "what could be the issue",
            "could this be related",
            "help identify",
            "preventing",
            "root cause",
            "why",
        ],
    ):
        score += 1
        reason_parts.append("google_ads_help_explanation_burden")

    if quality_score >= 4.0 and len(lowered.strip()) >= min_text_length:
        score += 1
        reason_parts.append("google_ads_help_quality_score_bonus")

    if contains_any_term(
        lowered,
        [
            "launched:",
            "guided help article",
            "welcome to the community",
            "new guided troubleshooter",
            "resources and videos",
        ],
    ):
        score -= 2
        reason_parts.append("google_ads_help_announcement_penalty")

    return score, reason_parts


def _google_ads_help_signal_bonus_count(row: pd.Series, lowered: str) -> int:
    """Return an extra positive-count bonus for Google Ads Help reporting/problem signals."""
    bonus = 0
    if str(row.get("quality_bucket", "") or "") in {"hard_pass", "borderline"}:
        bonus += 1
    if contains_any_term(
        lowered,
        [
            "conversion",
            "impressions",
            "clicks",
            "reporting",
            "merchant center",
            "shopping campaign",
            "campaign performance",
            "metrics discrepancy",
        ],
    ):
        bonus += 1
    return bonus
