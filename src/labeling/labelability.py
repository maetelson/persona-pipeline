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
    "mixpanel_community",
    "power_bi_community",
    "qlik_community",
    "sisense_community",
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
        force_low_signal = False
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
    if source == "power_bi_community":
        quality_bucket = str(row.get("quality_bucket", "") or "")
        quality_score = float(row.get("quality_score", 0.0) or 0.0)

        if quality_bucket == "hard_pass":
            score += 2
            reason_parts.append("power_bi_quality_hard_pass")
        elif quality_bucket == "borderline":
            score += 1
            reason_parts.append("power_bi_quality_borderline")

        if contains_any_term(
            lowered,
            [
                "power bi",
                "dax",
                "measure",
                "measures",
                "matrix",
                "slicer",
                "visual",
                "desktop",
                "service",
                "gateway",
                "refresh",
                "filter context",
                "row context",
                "distinct count",
                "wrong total",
                "wrong totals",
                "not matching",
                "mismatch",
                "export",
                "csv",
            ],
        ):
            score += 1
            reason_parts.append("power_bi_metric_reporting_context")

        if contains_any_term(
            lowered,
            [
                "why",
                "workaround",
                "limitation",
                "client wants",
                "expected outcome",
                "trying to",
                "need to",
                "is there any way",
                "is there an option",
            ],
        ):
            score += 1
            reason_parts.append("power_bi_explanation_burden")

        if quality_score >= 4.0 and len(lowered.strip()) >= min_text_length:
            score += 1
            reason_parts.append("power_bi_quality_score_bonus")

        if contains_any_term(
            lowered,
            [
                "accepted solution",
                "has your issue been resolved",
                "please feel free to contact us",
                "community member addressed your query",
            ],
        ):
            score -= 2
            reason_parts.append("power_bi_support_boilerplate_penalty")

        return score, reason_parts

    if source == "qlik_community":
        quality_bucket = str(row.get("quality_bucket", "") or "")
        quality_score = float(row.get("quality_score", 0.0) or 0.0)

        if quality_bucket == "hard_pass":
            score += 2
            reason_parts.append("qlik_quality_hard_pass")
        elif quality_bucket == "borderline":
            score += 1
            reason_parts.append("qlik_quality_borderline")

        if contains_any_term(
            lowered,
            [
                "qlik",
                "qlik sense",
                "qlikview",
                "nprinting",
                "set analysis",
                "pivot table",
                "straight table",
                "combo chart",
                "cross tab",
                "pixel perfect",
                "expression",
                "measure",
                "dimension",
                "total",
                "totals",
                "kpi",
                "dashboard",
                "report",
                "excel",
                "newsstand",
            ],
        ):
            score += 1
            reason_parts.append("qlik_metric_reporting_context")

        if contains_any_term(
            lowered,
            [
                "not aggregating",
                "different totals",
                "collapsed or expanded",
                "desired level",
                "stuck",
                "trying to",
                "need to",
                "goal is",
                "would like to know",
                "how to",
                "issue",
                "problem",
                "not updating",
                "greyed out",
            ],
        ):
            score += 1
            reason_parts.append("qlik_problem_framing_bonus")

        if quality_score >= 4.0 and len(lowered.strip()) >= min_text_length:
            score += 1
            reason_parts.append("qlik_quality_score_bonus")

        return score, reason_parts

    if source == "mixpanel_community":
        quality_bucket = str(row.get("quality_bucket", "") or "")
        quality_score = float(row.get("quality_score", 0.0) or 0.0)

        if quality_bucket == "hard_pass":
            score += 2
            reason_parts.append("mixpanel_quality_hard_pass")
        elif quality_bucket == "borderline":
            score += 1
            reason_parts.append("mixpanel_quality_borderline")

        if contains_any_term(
            lowered,
            [
                "mixpanel",
                "event",
                "events",
                "funnel",
                "funnels",
                "retention",
                "insights",
                "query",
                "jql",
                "distinct users",
                "distinct id",
                "identify",
                "browser",
                "country",
                "city",
                "dashboard",
                "tracking",
                "mirror sync",
            ],
        ):
            score += 1
            reason_parts.append("mixpanel_metric_reporting_context")

        if contains_any_term(
            lowered,
            [
                "undefined",
                "not set",
                "missing",
                "not seeing events",
                "not receiving any events",
                "issue",
                "troubleshooting",
                "trying to",
                "can't figure out",
                "cannot figure out",
                "what is this error",
                "difference",
            ],
        ):
            score += 1
            reason_parts.append("mixpanel_problem_framing_bonus")

        if quality_score >= 4.0 and len(lowered.strip()) >= min_text_length:
            score += 1
            reason_parts.append("mixpanel_quality_score_bonus")

        return score, reason_parts

    if source == "sisense_community":
        quality_bucket = str(row.get("quality_bucket", "") or "")
        quality_score = float(row.get("quality_score", 0.0) or 0.0)

        if quality_bucket == "hard_pass":
            score += 2
            reason_parts.append("sisense_quality_hard_pass")
        elif quality_bucket == "borderline":
            score += 1
            reason_parts.append("sisense_quality_borderline")

        if contains_any_term(
            lowered,
            [
                "sisense",
                "dashboard",
                "widget",
                "pivot table",
                "chart",
                "filter",
                "break by",
                "data model",
                "jump to dashboard",
                "scientific units",
                "javascript",
                "column width",
            ],
        ):
            score += 1
            reason_parts.append("sisense_metric_reporting_context")

        if contains_any_term(
            lowered,
            [
                "reset",
                "limits",
                "dynamically change",
                "multiple columns",
                "same month or not",
                "better way",
                "how to",
                "is it possible",
                "trying to",
                "need to",
            ],
        ):
            score += 1
            reason_parts.append("sisense_problem_framing_bonus")

        if quality_score >= 4.0 and len(lowered.strip()) >= min_text_length:
            score += 1
            reason_parts.append("sisense_quality_score_bonus")

        return score, reason_parts

    if source == "github_discussions":
        if contains_any_term(
            lowered,
            [
                "dashboard",
                "report",
                "reporting",
                "metric",
                "metrics",
                "pivot table",
                "table panel",
                "matrix",
                "filter",
                "filters",
                "drill down",
                "drill-through",
                "xlsx",
                "csv",
                "question",
                "model",
                "semantic layer",
            ],
        ):
            score += 1
            reason_parts.append("github_discussions_reporting_context")

        if contains_any_term(
            lowered,
            [
                "bug description",
                "what problem does this solve",
                "incorrect",
                "wrong",
                "fails",
                "failed",
                "not working",
                "not showing",
                "unable",
                "can't",
                "cannot",
                "workaround",
                "trying to",
                "need to",
                "should",
            ],
        ):
            score += 1
            reason_parts.append("github_discussions_problem_frame")

        return score, reason_parts

    if source == "metabase_discussions":
        quality_bucket = str(row.get("quality_bucket", "") or "")
        quality_score = float(row.get("quality_score", 0.0) or 0.0)

        if quality_bucket == "hard_pass":
            score += 2
            reason_parts.append("metabase_quality_hard_pass")
        elif quality_bucket == "borderline":
            score += 1
            reason_parts.append("metabase_quality_borderline")

        if contains_any_term(
            lowered,
            [
                "metabase",
                "dashboard",
                "dashboards",
                "question",
                "questions",
                "model",
                "models",
                "query",
                "queries",
                "native sql",
                "filter",
                "filters",
                "dropdown",
                "chart",
                "charts",
                "table",
                "pivot",
                "pivot table",
                "metadata sync",
                "csv",
                "xlsx",
                "export",
            ],
        ):
            score += 1
            reason_parts.append("metabase_reporting_context")

        if contains_any_term(
            lowered,
            [
                "issue",
                "problem",
                "failed",
                "fails",
                "failing",
                "can't",
                "cannot",
                "unable",
                "wrong",
                "incorrect",
                "not working",
                "not showing",
                "not syncing",
                "breaking dashboards",
                "workaround",
                "trying to",
                "need to",
                "how do you handle",
                "is there a way",
            ],
        ):
            score += 1
            reason_parts.append("metabase_problem_frame")

        if quality_score >= 3.0:
            score += 1
            reason_parts.append("metabase_quality_score_bonus")

        return score, reason_parts

    return score, reason_parts
