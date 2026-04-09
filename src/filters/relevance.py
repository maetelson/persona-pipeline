"""Source-aware relevance scoring for Reddit and Stack Overflow BI pain discovery."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any

import pandas as pd

from src.utils.record_access import (
    get_record_id,
    get_record_source,
    get_record_source_meta,
    get_record_source_text,
    get_record_tags,
    get_record_text,
)

LlmHook = Any

POSITIVE_SCORE_COLUMNS = [
    "biz_workflow_score",
    "bi_tool_score",
    "reporting_pain_score",
    "dashboard_trust_score",
    "excel_rework_score",
    "adhoc_analysis_score",
    "metric_definition_score",
    "segmentation_breakdown_score",
    "root_cause_score",
    "stakeholder_pressure_score",
]

NEGATIVE_SCORE_COLUMNS = [
    "dev_heavy_score",
    "generic_programming_score",
    "infra_noise_score",
    "implementation_only_score",
]

ALL_SCORE_COLUMNS = POSITIVE_SCORE_COLUMNS + NEGATIVE_SCORE_COLUMNS + ["final_relevance_score"]


@dataclass(slots=True)
class RelevanceEvaluation:
    """Detailed scored result for one normalized row."""

    scores: dict[str, float]
    relevance_decision: str
    top_positive_signals: str
    top_negative_signals: str
    score_breakdown: str
    source_specific_reason: str


def apply_relevance_prefilter(
    df: pd.DataFrame,
    rules: dict[str, Any],
    llm_hook: LlmHook | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split rows into keep, borderline, and drop with transparent scores."""
    if df.empty:
        empty = pd.DataFrame(columns=list(df.columns) + _output_columns())
        return empty.copy(), empty.copy(), empty.copy()

    result = _ensure_required_columns(df.copy())
    normalized_contexts = [_normalize_row_context(row, rules) for _, row in result.iterrows()]
    evaluations = [_evaluate_row_from_context(row, rules, normalized) for (_, row), normalized in zip(result.iterrows(), normalized_contexts, strict=False)]
    result["subreddit_or_forum"] = [
        (str(existing).strip() or normalized["subreddit"])
        for existing, normalized in zip(result["subreddit_or_forum"].tolist(), normalized_contexts, strict=False)
    ]

    for column in ALL_SCORE_COLUMNS:
        result[column] = [item.scores.get(column, 0.0) for item in evaluations]
    result["relevance_decision"] = [item.relevance_decision for item in evaluations]
    result["prefilter_status"] = result["relevance_decision"]
    result["final_relevance_score"] = result["final_relevance_score"].round(4)
    result["relevance_score"] = result["final_relevance_score"]
    result["biz_user_score"] = result[POSITIVE_SCORE_COLUMNS].sum(axis=1).round(4)
    result["dev_heavy_score"] = result["dev_heavy_score"].round(4)
    result["top_positive_signals"] = [item.top_positive_signals for item in evaluations]
    result["top_negative_signals"] = [item.top_negative_signals for item in evaluations]
    result["score_breakdown"] = [item.score_breakdown for item in evaluations]
    result["source_specific_reason"] = [item.source_specific_reason for item in evaluations]
    result["prefilter_reason"] = result["source_specific_reason"]
    result = _apply_optional_llm_hook(result, rules, llm_hook)

    keep_df = result[result["relevance_decision"] == "keep"].copy().reset_index(drop=True)
    borderline_df = result[result["relevance_decision"] == "borderline"].copy().reset_index(drop=True)
    drop_df = result[result["relevance_decision"] == "drop"].copy().reset_index(drop=True)
    return keep_df, borderline_df, drop_df


def build_prefilter_summary(keep_df: pd.DataFrame, borderline_df: pd.DataFrame, drop_df: pd.DataFrame) -> pd.DataFrame:
    """Build aggregate keep/borderline/drop metrics."""
    rows = []
    for decision, frame in [("keep", keep_df), ("borderline", borderline_df), ("drop", drop_df)]:
        rows.append(
            {
                "relevance_decision": decision,
                "row_count": len(frame),
                "avg_final_relevance_score": round(float(frame["final_relevance_score"].mean()) if not frame.empty else 0.0, 4),
                "avg_biz_workflow_score": round(float(frame["biz_workflow_score"].mean()) if not frame.empty else 0.0, 4),
                "avg_dev_heavy_score": round(float(frame["dev_heavy_score"].mean()) if not frame.empty else 0.0, 4),
            }
        )
    return pd.DataFrame(rows)


def build_source_ratio_summary(result_df: pd.DataFrame) -> pd.DataFrame:
    """Build source-level keep/drop ratios."""
    if result_df.empty:
        return pd.DataFrame(columns=["source", "relevance_decision", "row_count", "share"])
    grouped = result_df.groupby(["source", "relevance_decision"], dropna=False).size().reset_index(name="row_count")
    totals = grouped.groupby("source", dropna=False)["row_count"].sum().reset_index(name="total_count")
    merged = grouped.merge(totals, on="source", how="left")
    merged["share"] = (merged["row_count"] / merged["total_count"].clip(lower=1)).round(4)
    return merged.drop(columns=["total_count"])


def build_top_negative_signal_report(result_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the most common negative signals."""
    if result_df.empty:
        return pd.DataFrame(columns=["signal", "count"])
    exploded = (
        result_df.assign(top_negative_signals=result_df["top_negative_signals"].fillna("").astype(str).str.split("|"))
        .explode("top_negative_signals")
        .query("top_negative_signals != ''")
    )
    if exploded.empty:
        return pd.DataFrame(columns=["signal", "count"])
    return (
        exploded.groupby("top_negative_signals", dropna=False)
        .size()
        .reset_index(name="count")
        .rename(columns={"top_negative_signals": "signal"})
        .sort_values(["count", "signal"], ascending=[False, True])
        .reset_index(drop=True)
    )


def build_reddit_subreddit_summary(result_df: pd.DataFrame) -> pd.DataFrame:
    """Build subreddit-level keep/drop ratios for Reddit rows."""
    if result_df.empty:
        return pd.DataFrame(columns=["subreddit_or_forum", "relevance_decision", "row_count"])
    reddit_df = result_df[result_df["source"] == "reddit"].copy()
    if reddit_df.empty:
        return pd.DataFrame(columns=["subreddit_or_forum", "relevance_decision", "row_count"])
    return (
        reddit_df.groupby(["subreddit_or_forum", "relevance_decision"], dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["subreddit_or_forum", "row_count"], ascending=[True, False])
        .reset_index(drop=True)
    )


def build_stackoverflow_tag_summary(result_df: pd.DataFrame) -> pd.DataFrame:
    """Build tag-level keep/drop ratios for Stack Overflow rows."""
    if result_df.empty:
        return pd.DataFrame(columns=["tag", "relevance_decision", "row_count"])
    stack_df = result_df[result_df["source"] == "stackoverflow"].copy()
    if stack_df.empty:
        return pd.DataFrame(columns=["tag", "relevance_decision", "row_count"])
    stack_df["parsed_tags"] = stack_df.apply(get_record_tags, axis=1)
    exploded = stack_df.explode("parsed_tags").rename(columns={"parsed_tags": "tag"})
    exploded["tag"] = exploded["tag"].fillna("").astype(str)
    exploded = exploded[exploded["tag"] != ""]
    if exploded.empty:
        return pd.DataFrame(columns=["tag", "relevance_decision", "row_count"])
    return (
        exploded.groupby(["tag", "relevance_decision"], dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["tag", "row_count"], ascending=[True, False])
        .reset_index(drop=True)
    )


def build_before_after_comparison(
    normalized_df: pd.DataFrame,
    keep_df: pd.DataFrame,
    borderline_df: pd.DataFrame,
    previous_valid_df: pd.DataFrame,
    previous_invalid_df: pd.DataFrame,
    source: str,
    rules: dict[str, Any] | None = None,
    limit: int = 25,
) -> dict[str, pd.DataFrame]:
    """Compare previous valid/invalid membership against the new relevance outcome."""
    target_df = normalized_df[normalized_df["source"] == source].copy()
    if target_df.empty:
        empty = pd.DataFrame()
        return {"previously_kept_now_dropped": empty, "previously_dropped_now_kept": empty, "signal_density": empty}

    previous_valid_ids = set(_row_identity_strings(previous_valid_df[previous_valid_df["source"] == source]))
    previous_invalid_ids = set(_row_identity_strings(previous_invalid_df[previous_invalid_df["source"] == source]))

    new_result_df = pd.concat([keep_df, borderline_df], ignore_index=True)
    new_keep_ids = set(_row_identity_strings(new_result_df[new_result_df["source"] == source]))
    new_drop_ids = set(_row_identity_strings(target_df)) - new_keep_ids

    target_df["identity_tuple"] = _row_identity_series(target_df)
    previously_kept_now_dropped = (
        target_df[target_df["identity_tuple"].isin(previous_valid_ids & new_drop_ids)]
        .head(limit)
        .drop(columns=["identity_tuple"])
        .reset_index(drop=True)
    )
    previously_dropped_now_kept = (
        pd.concat([keep_df, borderline_df], ignore_index=True)
        .query("source == @source")
        .assign(identity_tuple=lambda frame: _row_identity_series(frame))
    )
    previously_dropped_now_kept = (
        previously_dropped_now_kept[previously_dropped_now_kept["identity_tuple"].isin(previous_invalid_ids & new_keep_ids)]
        .head(limit)
        .drop(columns=["identity_tuple"])
        .reset_index(drop=True)
    )

    before_density = _signal_density(previous_valid_df[previous_valid_df["source"] == source], rules=rules)
    after_density = _signal_density(new_result_df[new_result_df["source"] == source])
    signal_density = pd.DataFrame(
        [
            {"metric": "avg_positive_score_sum", "before_value": before_density["positive"], "after_value": after_density["positive"]},
            {"metric": "avg_negative_score_sum", "before_value": before_density["negative"], "after_value": after_density["negative"]},
            {"metric": "avg_final_relevance_score", "before_value": before_density["final"], "after_value": after_density["final"]},
        ]
    )
    signal_density["delta"] = (signal_density["after_value"] - signal_density["before_value"]).round(4)
    return {
        "previously_kept_now_dropped": previously_kept_now_dropped,
        "previously_dropped_now_kept": previously_dropped_now_kept,
        "signal_density": signal_density,
    }


def _evaluate_row_from_context(row: pd.Series, rules: dict[str, Any], normalized: dict[str, Any]) -> RelevanceEvaluation:
    """Evaluate one row with source-specific and pattern-aware scoring."""
    source = normalized["source"]
    text = normalized["combined_text"]
    source_cfg = rules.get(source, {}) if isinstance(rules.get(source, {}), dict) else {}
    score_weights = rules.get("score_weights", {}) or {}
    scores = {column: 0.0 for column in ALL_SCORE_COLUMNS}
    positive_hits: list[tuple[str, float]] = []
    negative_hits: list[tuple[str, float]] = []
    source_reasons: list[str] = []

    for score_name, phrases in (rules.get("strong_positive_lexicon", {}) or {}).items():
        for phrase in phrases or []:
            if phrase.lower() in text:
                scores[score_name] += 1.6
                positive_hits.append((phrase, 1.6))

    positive_context_count = sum(1 for term in rules.get("positive_context_terms", []) if str(term).lower() in text)
    for score_name, phrases in (rules.get("medium_positive_lexicon", {}) or {}).items():
        for phrase in phrases or []:
            if phrase.lower() in text:
                weight = 1.1 if positive_context_count >= 1 else 0.35
                scores[score_name] += weight
                positive_hits.append((phrase, weight))

    for score_name, pattern_rows in (rules.get("positive_patterns", {}) or {}).items():
        for pattern_row in pattern_rows or []:
            pattern = str(pattern_row.get("pattern", "") or "")
            if pattern and re.search(pattern, text, flags=re.IGNORECASE):
                weight = float(pattern_row.get("weight", 1.5))
                signal = str(pattern_row.get("signal", pattern) or pattern)
                scores[score_name] += weight
                positive_hits.append((signal, weight))

    bi_tool_hits = 0
    for tool in rules.get("bi_tool_terms", []) or []:
        if str(tool).lower() in text:
            bi_tool_hits += 1
    scores["bi_tool_score"] += bi_tool_hits * 0.9
    if bi_tool_hits:
        positive_hits.append(("bi_tool_terms", round(bi_tool_hits * 0.9, 2)))

    for score_name, phrases in (rules.get("strong_negative_lexicon", {}) or {}).items():
        for phrase in phrases or []:
            if phrase.lower() in text:
                scores[score_name] += 1.7
                negative_hits.append((phrase, 1.7))

    if source == "reddit":
        subreddit = normalized["subreddit"]
        reddit_cfg = source_cfg
        subreddit_weight = float((reddit_cfg.get("relevant_subreddits", {}) or {}).get(subreddit, 0.0))
        if subreddit_weight:
            scores["biz_workflow_score"] += subreddit_weight
            positive_hits.append((f"subreddit:{subreddit}", subreddit_weight))
            source_reasons.append(f"reddit_subreddit_boost:{subreddit}")
        for pattern in reddit_cfg.get("negative_subreddit_patterns", []) or []:
            if re.search(pattern, subreddit, flags=re.IGNORECASE):
                penalty = abs(float(reddit_cfg.get("negative_subreddit_weight", -1.8)))
                scores["generic_programming_score"] += penalty
                negative_hits.append((f"subreddit:{subreddit}", penalty))
                source_reasons.append(f"reddit_subreddit_downweight:{subreddit}")
                break
        flair = normalized["flair"]
        if flair and any(term in flair for term in ["report", "dashboard", "excel", "analytics"]):
            scores["reporting_pain_score"] += 0.8
            positive_hits.append((f"flair:{flair}", 0.8))

    if source == "stackoverflow":
        stack_cfg = source_cfg
        tags = normalized["tags"]
        for tag in tags:
            if tag in (stack_cfg.get("strong_positive_tags", {}) or {}):
                weight = float(stack_cfg["strong_positive_tags"][tag])
                scores["bi_tool_score"] += weight
                positive_hits.append((f"tag:{tag}", weight))
                source_reasons.append(f"stackoverflow_tag_boost:{tag}")
            elif tag in (stack_cfg.get("conditional_tags", {}) or {}):
                weight = float(stack_cfg["conditional_tags"][tag])
                conditional_weight = weight if positive_context_count >= 1 else weight * 0.35
                scores["bi_tool_score"] += conditional_weight
                positive_hits.append((f"tag:{tag}", conditional_weight))
            elif tag in (stack_cfg.get("negative_tags", {}) or {}):
                weight = float(stack_cfg["negative_tags"][tag])
                scores["generic_programming_score"] += weight
                negative_hits.append((f"tag:{tag}", weight))
                source_reasons.append(f"stackoverflow_tag_downweight:{tag}")
        if normalized["is_answered"]:
            scores["bi_tool_score"] += float(stack_cfg.get("accepted_answer_bonus", 0.0))

    technical_recovery = _apply_technical_but_relevant_recovery(text, scores, rules)
    if technical_recovery:
        positive_hits.append(("technical_but_relevant_recovery", technical_recovery))
        source_reasons.append("technical_but_relevant_recovery")

    _apply_source_default_weight(scores, source, rules)

    weighted_positive = sum(scores[column] * float(score_weights.get(column, 1.0)) for column in POSITIVE_SCORE_COLUMNS)
    weighted_negative = sum(abs(scores[column]) * abs(float(score_weights.get(column, -1.0))) for column in NEGATIVE_SCORE_COLUMNS)
    scores["final_relevance_score"] = round(weighted_positive - weighted_negative, 4)

    decision = _classify_decision(scores, rules)
    top_positive_signals = "|".join(signal for signal, _ in sorted(positive_hits, key=lambda item: item[1], reverse=True)[:5])
    top_negative_signals = "|".join(signal for signal, _ in sorted(negative_hits, key=lambda item: item[1], reverse=True)[:5])
    source_specific_reason = "|".join(source_reasons) or f"{source}:generic"
    score_breakdown = json.dumps({column: round(scores[column], 4) for column in ALL_SCORE_COLUMNS}, ensure_ascii=False, sort_keys=True)
    return RelevanceEvaluation(
        scores=scores,
        relevance_decision=decision,
        top_positive_signals=top_positive_signals,
        top_negative_signals=top_negative_signals,
        score_breakdown=score_breakdown,
        source_specific_reason=source_specific_reason,
    )


def _normalize_row_context(row: pd.Series, rules: dict[str, Any]) -> dict[str, Any]:
    """Normalize context fields and parse source metadata."""
    source = get_record_source(row).lower()
    source_meta = get_record_source_meta(row)
    raw_post = source_meta.get("raw_post", {}) if isinstance(source_meta.get("raw_post"), dict) else {}
    raw_question = source_meta.get("raw_question", {}) if isinstance(source_meta.get("raw_question"), dict) else {}
    subreddit = str(
        row.get("subreddit_or_forum", "")
        or source_meta.get("subreddit_name_prefixed", "")
        or raw_post.get("subreddit_name_prefixed", "")
        or (f"r/{raw_post.get('subreddit', '')}" if raw_post.get("subreddit") else "")
    ).strip().lower()
    flair = str(raw_post.get("link_flair_text", "") or source_meta.get("flair", "") or "").strip().lower()
    tags = get_record_tags(row)
    title = get_record_text(row, fields=["title"])
    body = get_record_source_text(row)
    combined_text = f"{title} {body} {subreddit} {flair} {' '.join(tags)}".lower()
    normalized_source = source if source in {"reddit", "stackoverflow"} else source
    return {
        "source": normalized_source,
        "source_meta": source_meta,
        "subreddit": subreddit,
        "flair": flair,
        "tags": tags,
        "combined_text": combined_text,
        "is_answered": bool(raw_question.get("is_answered") or source_meta.get("is_answered") or False),
    }


def _apply_technical_but_relevant_recovery(text: str, scores: dict[str, float], rules: dict[str, Any]) -> float:
    """Reduce technical penalties when BI workflow context is clearly present."""
    has_recovery_term = any(str(term).lower() in text for term in rules.get("recovery_terms", []) or [])
    has_recovery_context = any(str(term).lower() in text for term in rules.get("recovery_context_terms", []) or [])
    positive_total = sum(scores[column] for column in POSITIVE_SCORE_COLUMNS)
    if has_recovery_term and (has_recovery_context or positive_total >= 4.0):
        for column in NEGATIVE_SCORE_COLUMNS:
            scores[column] = max(0.0, scores[column] - 0.7)
        scores["metric_definition_score"] += 0.8
        scores["biz_workflow_score"] += 0.5
        return 1.3
    return 0.0


def _apply_source_default_weight(scores: dict[str, float], source: str, rules: dict[str, Any]) -> None:
    """Apply a small source-level prior after lexical scoring."""
    source_defaults = rules.get("source_defaults", {}) or {}
    source_weight = float((source_defaults.get(source, {}) or {}).get("source_weight", 1.0))
    for column in POSITIVE_SCORE_COLUMNS:
        scores[column] *= source_weight


def _classify_decision(scores: dict[str, float], rules: dict[str, Any]) -> str:
    """Turn scores into keep/borderline/drop."""
    positive_total = sum(scores[column] for column in POSITIVE_SCORE_COLUMNS)
    negative_total = sum(scores[column] for column in NEGATIVE_SCORE_COLUMNS)
    final_score = float(scores["final_relevance_score"])
    keep_threshold = float(rules.get("keep_threshold", 9.5))
    borderline_threshold = float(rules.get("borderline_threshold", 5.5))
    technical_rule = rules.get("technical_only_drop", {}) or {}
    if negative_total >= float(technical_rule.get("min_negative_total", 5.0)) and positive_total <= float(technical_rule.get("max_positive_total", 3.5)):
        return "drop"
    if final_score >= keep_threshold:
        return "keep"
    if final_score >= borderline_threshold:
        return "borderline"
    return "drop"


def _signal_density(df: pd.DataFrame, rules: dict[str, Any] | None = None) -> dict[str, float]:
    """Compute average positive/negative/final score density."""
    if df.empty:
        return {"positive": 0.0, "negative": 0.0, "final": 0.0}
    frame = df.copy()
    if rules is not None and "final_relevance_score" not in frame.columns:
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, rules)
        frame = pd.concat([keep_df, borderline_df, drop_df], ignore_index=True)
    for column in ALL_SCORE_COLUMNS:
        if column not in frame.columns:
            frame[column] = 0.0
    positive = float(frame[POSITIVE_SCORE_COLUMNS].sum(axis=1).mean())
    negative = float(frame[NEGATIVE_SCORE_COLUMNS].sum(axis=1).mean())
    final = float(frame["final_relevance_score"].mean())
    return {"positive": round(positive, 4), "negative": round(negative, 4), "final": round(final, 4)}


def _row_identity_strings(df: pd.DataFrame) -> list[str]:
    """Build a stable identity list for comparisons."""
    if df.empty:
        return []
    return [f"{get_record_source(row)}::{get_record_id(row)}" for _, row in df.iterrows()]


def _row_identity_series(df: pd.DataFrame) -> pd.Series:
    """Build a stable identity series for comparisons."""
    if df.empty:
        return pd.Series(dtype=object)
    return pd.Series([f"{get_record_source(row)}::{get_record_id(row)}" for _, row in df.iterrows()], index=df.index, dtype=object)


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Attach missing columns needed by the scorer."""
    required = [
        "source",
        "raw_id",
        "title",
        "body",
        "body_text",
        "comments_text",
        "raw_text",
        "thread_title",
        "parent_context",
        "subreddit_or_forum",
        "source_meta",
    ]
    for column in required:
        if column not in df.columns:
            df[column] = ""
    return df


def _output_columns() -> list[str]:
    """Return all appended prefilter columns."""
    return ALL_SCORE_COLUMNS + [
        "relevance_decision",
        "prefilter_status",
        "relevance_score",
        "biz_user_score",
        "top_positive_signals",
        "top_negative_signals",
        "score_breakdown",
        "source_specific_reason",
        "prefilter_reason",
    ]


def _apply_optional_llm_hook(df: pd.DataFrame, rules: dict[str, Any], llm_hook: LlmHook | None):
    """Run an optional LLM relevance hook only when explicitly enabled."""
    if not bool(rules.get("enable_llm_relevance", False)):
        return df
    if llm_hook is None:
        return df
    return llm_hook(df.copy())
