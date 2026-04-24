"""Source-aware relevance scoring for forum-like BI pain discovery."""

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
    prefilter_score: float
    whitelist_hits: str
    rescue_reason: str
    dropped_reason: str


def apply_relevance_prefilter(
    df: pd.DataFrame,
    rules: dict[str, Any],
    llm_hook: LlmHook | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split rows into keep, borderline, and drop with transparent scores."""
    if df.empty:
        empty = pd.DataFrame(columns=_dedupe_column_names([*df.columns, *_output_columns()]))
        return empty.copy(), empty.copy(), empty.copy()

    result = _ensure_required_columns(df.loc[:, ~df.columns.duplicated()].copy())
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
    result["prefilter_score"] = [item.prefilter_score for item in evaluations]
    result["whitelist_hits"] = [item.whitelist_hits for item in evaluations]
    result["rescue_reason"] = [item.rescue_reason for item in evaluations]
    result["dropped_reason"] = [item.dropped_reason for item in evaluations]
    result = _apply_optional_llm_hook(result, rules, llm_hook)
    result = _apply_source_balance_reduction(result, rules)

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

    if source == "github_discussions":
        github_cfg = source_cfg
        workflow_term_hits = sum(
            1 for term in github_cfg.get("workflow_context_terms", []) or [] if str(term).lower() in text
        )
        strong_workflow_hits = sum(
            1 for term in github_cfg.get("strong_workflow_context_terms", []) or [] if _text_contains_term(text, str(term).lower())
        )
        repo_term_hits = sum(
            1 for term in github_cfg.get("positive_repo_terms", []) or [] if str(term).lower() in text
        )
        technical_issue_hits = sum(
            1 for term in github_cfg.get("technical_issue_patterns", []) or [] if str(term).lower() in text
        )
        maintainer_noise_hits = sum(
            1 for term in github_cfg.get("maintainer_noise_patterns", []) or [] if str(term).lower() in text
        )
        if workflow_term_hits:
            base_bonus = float(github_cfg.get("workflow_context_bonus", 0.6))
            per_hit_bonus = float(github_cfg.get("workflow_context_hit_bonus", 0.0))
            bonus = base_bonus + max(workflow_term_hits - 1, 0) * per_hit_bonus
            scores["biz_workflow_score"] += bonus
            positive_hits.append(("github_workflow_context", round(bonus, 2)))
            source_reasons.append("github_discussions_workflow_context")
        if strong_workflow_hits:
            strong_bonus = strong_workflow_hits * float(github_cfg.get("strong_workflow_context_bonus", 0.0))
            if strong_bonus:
                scores["reporting_pain_score"] += strong_bonus
                positive_hits.append(("github_strong_workflow_context", round(strong_bonus, 2)))
                source_reasons.append("github_discussions_strong_workflow_context")
        penalty_multiplier = 1.0
        if workflow_term_hits and strong_workflow_hits:
            penalty_multiplier = float(github_cfg.get("workflow_penalty_relief_multiplier", 0.35))
        if technical_issue_hits and workflow_term_hits < 2 and strong_workflow_hits == 0:
            penalty = technical_issue_hits * float(github_cfg.get("issue_template_penalty", 1.6)) * penalty_multiplier
            scores["implementation_only_score"] += penalty
            negative_hits.append(("github_issue_template_noise", round(penalty, 2)))
            source_reasons.append("github_discussions_issue_template_downweight")
        if maintainer_noise_hits and workflow_term_hits < 2 and strong_workflow_hits == 0:
            penalty = maintainer_noise_hits * float(github_cfg.get("maintainer_noise_penalty", 1.2)) * penalty_multiplier
            scores["generic_programming_score"] += penalty
            negative_hits.append(("github_maintainer_noise", round(penalty, 2)))
            source_reasons.append("github_discussions_maintainer_downweight")
        if repo_term_hits and workflow_term_hits == 0:
            penalty = float(github_cfg.get("low_context_extra_penalty", 1.0))
            scores["infra_noise_score"] += penalty
            negative_hits.append(("github_repo_without_workflow_context", penalty))
            source_reasons.append("github_discussions_low_context_repo_match")

    technical_recovery = _apply_technical_but_relevant_recovery(text, scores, rules)
    if technical_recovery:
        positive_hits.append(("technical_but_relevant_recovery", technical_recovery))
        source_reasons.append("technical_but_relevant_recovery")

    _apply_source_default_weight(scores, source, rules)

    weighted_positive = sum(scores[column] * float(score_weights.get(column, 1.0)) for column in POSITIVE_SCORE_COLUMNS)
    weighted_negative = sum(abs(scores[column]) * abs(float(score_weights.get(column, -1.0))) for column in NEGATIVE_SCORE_COLUMNS)
    scores["final_relevance_score"] = round(weighted_positive - weighted_negative, 4)
    if source == "reddit":
        rescue_bonus, _ = _apply_reddit_rescue_signals(text, normalized["subreddit"], scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "stackoverflow":
        rescue_bonus, _ = _apply_stackoverflow_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "mixpanel_community":
        rescue_bonus, _ = _apply_mixpanel_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "klaviyo_community":
        rescue_bonus, _ = _apply_klaviyo_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "qlik_community":
        rescue_bonus, _ = _apply_qlik_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "sisense_community":
        rescue_bonus, _ = _apply_sisense_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "shopify_community":
        rescue_bonus, _ = _apply_shopify_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "google_developer_forums":
        rescue_bonus, _ = _apply_google_developer_forums_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "adobe_analytics_community":
        rescue_bonus, _ = _apply_adobe_analytics_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    if source == "domo_community_forum":
        rescue_bonus, _ = _apply_domo_rescue_signals(text, scores, positive_hits, source_reasons)
        if rescue_bonus:
            scores["final_relevance_score"] = round(float(scores["final_relevance_score"]) + rescue_bonus, 4)
    whitelist_labels = _source_whitelist_hits(source=source, text=text, rules=rules)
    rescue_reason = ""
    whitelist_hits = "|".join(whitelist_labels)
    decision = _classify_decision(scores, rules)
    decision = _apply_source_specific_floor_override(
        source=source,
        source_cfg=source_cfg,
        text=text,
        source_reasons=source_reasons,
        final_score=float(scores["final_relevance_score"]),
        current_decision=decision,
    )
    if decision == "drop" and whitelist_labels:
        rescue_reason = f"rescued_by_source_whitelist={whitelist_labels[0]}"
        decision = "borderline"
        source_reasons.append(rescue_reason)
    top_positive_signals = "|".join(signal for signal, _ in sorted(positive_hits, key=lambda item: item[1], reverse=True)[:5])
    top_negative_signals = "|".join(signal for signal, _ in sorted(negative_hits, key=lambda item: item[1], reverse=True)[:5])
    source_specific_reason = "|".join(source_reasons) or f"{source}:generic"
    score_breakdown = json.dumps({column: round(scores[column], 4) for column in ALL_SCORE_COLUMNS}, ensure_ascii=False, sort_keys=True)
    dropped_reason = "" if decision != "drop" else _derive_dropped_reason(source, text, scores, whitelist_labels, source_reasons)
    return RelevanceEvaluation(
        scores=scores,
        relevance_decision=decision,
        top_positive_signals=top_positive_signals,
        top_negative_signals=top_negative_signals,
        score_breakdown=score_breakdown,
        source_specific_reason=source_specific_reason,
        prefilter_score=float(scores["final_relevance_score"]),
        whitelist_hits=whitelist_hits,
        rescue_reason=rescue_reason,
        dropped_reason=dropped_reason,
    )


def _apply_source_balance_reduction(result_df: pd.DataFrame, rules: dict[str, Any]) -> pd.DataFrame:
    """Cap retained source dominance by demoting low-score overflow rows to drop."""
    if result_df.empty:
        return result_df
    balance_cfg = rules.get("source_balance", {}) if isinstance(rules.get("source_balance", {}), dict) else {}
    if not bool(balance_cfg.get("enabled", False)):
        return result_df
    max_share = float(balance_cfg.get("max_retained_source_share", 0.45))
    min_total_retained = int(balance_cfg.get("min_total_retained_rows", 150))
    protect_score = float(balance_cfg.get("protect_keep_score_at_or_above", 12.0))
    protected_sources = {str(item) for item in list(balance_cfg.get("protected_sources", []) or [])}
    if max_share <= 0.0 or max_share >= 1.0:
        return result_df

    frame = result_df.copy()
    retained_mask = frame["relevance_decision"].astype(str).isin({"keep", "borderline"})
    retained_total = int(retained_mask.sum())
    if retained_total < min_total_retained:
        return frame

    retained = frame[retained_mask].copy()
    if retained.empty:
        return frame
    max_allowed = max(int(retained_total * max_share), 1)
    source_counts = retained["source"].astype(str).value_counts().to_dict()
    for source, count in source_counts.items():
        if source in protected_sources:
            continue
        overflow = int(count) - max_allowed
        if overflow <= 0:
            continue
        source_mask = retained["source"].astype(str).eq(str(source))
        candidates = retained[source_mask].copy()
        if candidates.empty:
            continue
        candidates["_drop_priority"] = candidates["relevance_decision"].astype(str).map({"borderline": 0, "keep": 1}).fillna(2)
        candidates = candidates.sort_values(
            ["_drop_priority", "final_relevance_score", "raw_id"],
            ascending=[True, True, True],
        )
        to_demote = candidates.head(overflow)
        if to_demote.empty:
            continue
        protected_mask = (
            to_demote["relevance_decision"].astype(str).eq("keep")
            & pd.to_numeric(to_demote["final_relevance_score"], errors="coerce").fillna(0.0).ge(protect_score)
        )
        to_demote = to_demote[~protected_mask]
        if to_demote.empty:
            continue
        demote_idx = to_demote.index
        frame.loc[demote_idx, "relevance_decision"] = "drop"
        frame.loc[demote_idx, "prefilter_status"] = "drop"
        frame.loc[demote_idx, "dropped_reason"] = "rebalanced_for_source_diversity_cap"
        frame.loc[demote_idx, "rescue_reason"] = ""
        frame.loc[demote_idx, "source_specific_reason"] = (
            frame.loc[demote_idx, "source_specific_reason"].fillna("").astype(str)
            + "|rebalanced_source_cap"
        ).str.strip("|")
        frame.loc[demote_idx, "prefilter_reason"] = frame.loc[demote_idx, "source_specific_reason"]
    return frame


def _source_whitelist_hits(source: str, text: str, rules: dict[str, Any]) -> list[str]:
    """Return matched source-specific whitelist labels for rescue-pass decisions."""
    source_terms = (rules.get("source_whitelist_terms", {}) or {}).get(source, []) or []
    hits: list[str] = []
    lowered = str(text or "").lower()
    if source == "reddit":
        return _reddit_whitelist_hits(lowered)
    if source == "stackoverflow":
        return _stackoverflow_whitelist_hits(lowered)
    if source == "mixpanel_community":
        return _mixpanel_whitelist_hits(lowered)
    if source == "klaviyo_community":
        return _klaviyo_whitelist_hits(lowered)
    if source == "qlik_community":
        return _qlik_whitelist_hits(lowered)
    if source == "sisense_community":
        hits.extend(_sisense_whitelist_hits(lowered))
    if source == "shopify_community":
        return _shopify_whitelist_hits(lowered)
    if source == "google_developer_forums":
        return _google_developer_forums_whitelist_hits(lowered)
    if source == "adobe_analytics_community":
        return _adobe_analytics_whitelist_hits(lowered)
    if source == "domo_community_forum":
        return _domo_whitelist_hits(lowered)
    for row in source_terms:
        label = str(row.get("label", "") or "").strip()
        terms = [str(term).lower().strip() for term in row.get("terms", []) or [] if str(term).strip()]
        if not label or not terms:
            continue
        if any(_text_contains_term(lowered, term) for term in terms):
            hits.append(label)
    return hits


def _stackoverflow_whitelist_hits(lowered: str) -> list[str]:
    """Return stricter Stack Overflow rescue labels for BI reporting workflow posts."""
    bi_terms = [
        "power bi",
        "powerbi",
        "dax",
        "power query",
        "powerquery",
        "sql server",
        "postgresql",
        "mysql",
        "tableau",
        "reporting services",
        "ssrs",
        "pivot table",
        "powerpivot",
        "analysis services",
    ]
    reporting_terms = [
        "report",
        "reporting",
        "dashboard",
        "visual",
        "table",
        "matrix",
        "pivot",
        "paginated report",
        "template",
        "refresh",
    ]
    discrepancy_terms = [
        "wrong total",
        "wrong totals",
        "not matching",
        "mismatch",
        "incorrect",
        "zeros instead",
        "does not match",
        "doesn't match",
        "doesnt match",
    ]
    export_terms = ["export", "excel", "spreadsheet", "csv"]
    workflow_terms = [
        "every month",
        "monthly invoice",
        "weekly",
        "manual",
        "refresh it",
        "data is updated",
        "export to excel",
        "keep the visuals",
        "recreate the report",
    ]
    output_shape_terms = [
        "datatable",
        "data table",
        "grid",
        "table header",
        "row above table header",
        "current page",
        "column names",
        "white spaces",
        "blank spaces",
        "format",
        "filename",
        "sheet1",
        "cell",
        "cells",
        "rows",
        "columns",
    ]

    bi_hit = any(_text_contains_term(lowered, term) for term in bi_terms)
    reporting_hit = any(_text_contains_term(lowered, term) for term in reporting_terms)
    discrepancy_hit = any(_text_contains_term(lowered, term) for term in discrepancy_terms)
    export_hit = any(_text_contains_term(lowered, term) for term in export_terms)
    workflow_hit = any(_text_contains_term(lowered, term) for term in workflow_terms)
    output_shape_hit = any(_text_contains_term(lowered, term) for term in output_shape_terms)

    hits: list[str] = []
    if bi_hit and reporting_hit:
        hits.append("stackoverflow_bi_reporting_workflow")
    if bi_hit and discrepancy_hit:
        hits.append("stackoverflow_bi_metric_mismatch")
    if bi_hit and export_hit and workflow_hit:
        hits.append("stackoverflow_manual_reporting_export")
    if export_hit and reporting_hit and output_shape_hit:
        hits.append("stackoverflow_export_output_shape")
    return hits


def _apply_stackoverflow_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Stack Overflow rescue scoring for BI reconciliation and report-integrity questions."""
    lowered = str(text or "").lower()
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in ["dashboard", "report", "reporting", "matrix", "pivot", "visual", "table", "csv", "excel", "spreadsheet"]
    )
    bi_tool_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "power bi",
            "powerbi",
            "dax",
            "power query",
            "powerquery",
            "tableau",
            "sql server",
            "postgresql",
            "mysql",
            "reporting services",
            "ssrs",
        ]
    )
    reconciliation_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "wrong total",
            "wrong totals",
            "not matching",
            "mismatch",
            "count distinct",
            "group by",
            "left join",
            "duplicate rows",
            "date filter",
            "created_at",
            "event_date",
            "source of truth",
            "summary detail",
        ]
    )
    generic_dev_noise = any(
        _text_contains_term(lowered, term)
        for term in ["react", "javascript", "css", "html", "docker", "oauth", "selenium", "web scraping", "strapi", "grafana"]
    )
    generic_help_only = any(
        _text_contains_term(lowered, term)
        for term in [
            "how to export",
            "how can i export",
            "save to csv",
            "write to excel",
            "openpyxl",
            "xlsxwriter",
            "pandas excel writer",
        ]
    )
    personal_automation_noise = any(
        _text_contains_term(lowered, term)
        for term in [
            "sharpen my python skills",
            "personal project",
            "copy it manually",
            "paste it into a new workbook",
            "outlook email",
        ]
    )

    bonus = 0.0
    labels: list[str] = []
    if reporting_hit and reconciliation_hit and (bi_tool_hit or "sql" in lowered):
        scores["reporting_pain_score"] += 0.75
        scores["metric_definition_score"] += 0.55
        positive_hits.append(("stackoverflow_reporting_reconciliation", 1.3))
        source_reasons.append("stackoverflow_reporting_reconciliation")
        bonus += 0.55
        labels.append("stackoverflow_reporting_reconciliation")
    if reporting_hit and any(_text_contains_term(lowered, term) for term in ["export", "csv", "excel"]) and any(
        _text_contains_term(lowered, term) for term in ["rows", "columns", "format", "sheet", "table"]
    ):
        scores["excel_rework_score"] += 0.45
        positive_hits.append(("stackoverflow_export_integrity", 0.45))
        source_reasons.append("stackoverflow_export_integrity")
        bonus += 0.2
        labels.append("stackoverflow_export_integrity")
    if reporting_hit and any(_text_contains_term(lowered, term) for term in ["source of truth", "not matching", "mismatch", "different numbers", "reconcile"]):
        scores["dashboard_trust_score"] += 0.8
        scores["stakeholder_pressure_score"] += 0.35
        positive_hits.append(("stackoverflow_source_of_truth_conflict", 1.15))
        source_reasons.append("stackoverflow_source_of_truth_conflict")
        bonus += 0.45
        labels.append("stackoverflow_source_of_truth_conflict")
    if generic_dev_noise and not (reporting_hit and reconciliation_hit):
        scores["generic_programming_score"] += 0.8
    if generic_help_only and not (reconciliation_hit or bi_tool_hit):
        scores["generic_programming_score"] += 1.2
        scores["implementation_only_score"] += 0.9
    if personal_automation_noise and not any(_text_contains_term(lowered, term) for term in ["stakeholder", "leadership", "finance", "reporting pack", "board report"]):
        scores["generic_programming_score"] += 1.1
        scores["implementation_only_score"] += 0.6
    return round(bonus, 4), labels


def _mixpanel_whitelist_hits(lowered: str) -> list[str]:
    """Return stricter Mixpanel rescue labels for trust and interpretation pain."""
    reporting_terms = ["report", "reports", "dashboard", "export", "csv", "insights", "funnel", "funnels", "retention", "breakdown", "session duration"]
    trust_terms = [
        "not matching", "mismatch", "discrepancy", "wrong", "source of truth", "says one thing", "says another",
        "duplicate events", "export compared to reports", "timezone differences",
    ]
    workflow_terms = [
        "what changed", "which report should i use", "explain this drop", "trend changed", "figure out", "why does", "why is",
        "difference between", "different from", "undefined user emails",
    ]
    noise_terms = ["api", "sdk", "instrumentation", "webhook", "mobile sdk", "send events"]

    reporting_hit = any(_text_contains_term(lowered, term) for term in reporting_terms)
    trust_hit = any(_text_contains_term(lowered, term) for term in trust_terms)
    workflow_hit = any(_text_contains_term(lowered, term) for term in workflow_terms)
    export_integrity_hit = any(
        _text_contains_term(lowered, term)
        for term in ["exported data", "export compared to reports", "undefined user emails", "timestamp timezone", "raw usage data"]
    )
    noise_hit = any(_text_contains_term(lowered, term) for term in noise_terms)

    if noise_hit and not (trust_hit or workflow_hit):
        return []

    hits: list[str] = []
    if reporting_hit and trust_hit:
        hits.append("mixpanel_reporting_trust")
    if reporting_hit and workflow_hit:
        hits.append("mixpanel_trend_diagnosis")
    if trust_hit and any(_text_contains_term(lowered, term) for term in ["export", "csv", "dashboard"]):
        hits.append("mixpanel_export_discrepancy")
    if reporting_hit and export_integrity_hit:
        hits.append("mixpanel_export_integrity")
    return hits


def _klaviyo_whitelist_hits(lowered: str) -> list[str]:
    """Return Klaviyo rescue labels for reporting trust and segmentation pain."""
    reporting_terms = [
        "report", "reporting", "analytics", "benchmark", "export", "csv", "weekly reporting",
        "attributed revenue", "revenue", "attribution", "segment count", "list count", "profile count",
        "ga4", "google analytics", "overview dashboard", "campaigns breakdown by segment", "segment export",
        "custom report", "conversion rate", "visualizations", "external app", "api", "form metrics",
        "sign ups", "views by form", "average days between orders", "churn rate",
        "flow analytics", "open rate", "open rates", "click rate", "click rates", "bounce rate", "bounce rates",
        "custom reports page", "predictive metrics", "churn risk", "attribution window",
    ]
    trust_terms = [
        "source of truth", "not matching", "mismatch", "discrepancy", "reporting lag",
        "reconcile", "reconciliation", "what changed", "why did", "different", "compare",
        "does not equal", "doesn't equal", "doesnt equal", "limitation", "missing something",
        "not able to", "can't", "cannot", "not working as expected", "can't seem to find",
        "inflated", "full price", "discounted price", "at zero", "showing conversions are at zero",
        "error loading", "not count as revenue", "more revenue than klaviyo", "cancelled orders",
        "inaccurate numbers", "odd and inaccurate numbers", "only pulling", "failing its own filters",
        "open rates dropping", "open rate dropped", "dropped pretty significantly",
    ]
    ops_terms = [
        "export excel", "manual spreadsheet", "before sending", "weekly reporting", "google sheets", "power query",
        "external app", "visualizations", "conversion rate via api", "pull form metrics via api", "custom report",
        "dynamic one", "static value", "further data manipulation", "custom reports page", "predictive metrics",
        "churn risk", "attribution window", "flow analytics",
    ]
    reporting_hit = any(_text_contains_term(lowered, term) for term in reporting_terms)
    trust_hit = any(_text_contains_term(lowered, term) for term in trust_terms)
    ops_hit = any(_text_contains_term(lowered, term) for term in ops_terms)
    segment_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "segment count",
            "list count",
            "profile count",
            "segmentation",
            "segment export",
            "campaigns breakdown by segment",
            "suppressed profiles",
            "total profile number",
            "compare segments",
            "segment not working properly",
        ]
    )
    attribution_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "attributed revenue",
            "revenue mismatch",
            "attribution mismatch",
            "attribution",
            "ga4",
            "google analytics",
            "active on site metric",
            "users metric",
            "source/medium discrepancy",
        ]
    )

    hits: list[str] = []
    if reporting_hit and trust_hit:
        hits.append("klaviyo_reporting_trust")
    if segment_hit and trust_hit:
        hits.append("klaviyo_segment_count_reconcile")
    if ops_hit and (reporting_hit or trust_hit):
        hits.append("klaviyo_export_report_trust")
    if attribution_hit and trust_hit:
        hits.append("klaviyo_revenue_attribution_trust")
    if segment_hit and reporting_hit and any(
        _text_contains_term(lowered, term)
        for term in ["breakdown", "compare", "performance", "suppressed profiles", "total profile number", "not equaling"]
    ):
        hits.append("klaviyo_segment_reporting_breakdown")
    if any(_text_contains_term(lowered, term) for term in ["google sheets", "segment export", "exported emails", "custom report"]) and (reporting_hit or trust_hit):
        hits.append("klaviyo_export_integrity")
    if any(_text_contains_term(lowered, term) for term in ["skipped report", "message was skipped", "skip reason"]) and reporting_hit:
        hits.append("klaviyo_skip_reason_reporting")
    if reporting_hit and any(
        _text_contains_term(lowered, term)
        for term in [
            "external app",
            "visualizations",
            "conversion rate",
            "form metrics",
            "views by form",
            "sign ups",
            "custom report",
            "average days between orders",
            "churn rate",
        ]
    ) and trust_hit:
        hits.append("klaviyo_analysis_workaround")
    if reporting_hit and any(
        _text_contains_term(lowered, term)
        for term in [
            "cancelled orders",
            "full price",
            "discounted price",
            "attribution window",
            "conversions are at zero",
            "custom reports page",
            "error loading",
            "predictive metrics",
            "churn risk",
        ]
    ) and trust_hit:
        hits.append("klaviyo_reporting_math")
    return hits


def _qlik_whitelist_hits(lowered: str) -> list[str]:
    """Return stricter Qlik rescue labels only when chart/expression issues carry analyst pain."""
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in ["report", "reporting", "export", "nprinting", "pixel perfect", "dashboard", "manual reporting"]
    )
    mismatch_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "wrong total", "wrong totals", "not matching", "mismatch", "cannot explain", "can't explain", "root cause",
            "wrong values exported", "export issue", "does not appear", "0 values", "total line", "not exported",
            "doesn't work", "doesnt work", "date export", "percentage changes", "too large", "correct results",
            "right results", "incorrect results", "not correct", "does not add up",
        ]
    )
    set_analysis_hit = any(_text_contains_term(lowered, term) for term in ["set analysis", "aggr", "expression", "measure"])
    visual_hit = any(_text_contains_term(lowered, term) for term in ["straight table", "pivot table", "combo chart", "gauge chart"])
    export_context_hit = any(
        _text_contains_term(lowered, term)
        for term in ["board report", "export to excel", "export excel", "export issue", "excel export", "hypercube results are too large"]
    )

    hits: list[str] = []
    if mismatch_hit and reporting_hit:
        hits.append("qlik_reporting_mismatch")
    if set_analysis_hit and mismatch_hit:
        hits.append("qlik_set_analysis")
    if visual_hit and (mismatch_hit or reporting_hit):
        hits.append("qlik_visual_filter")
    if export_context_hit and (mismatch_hit or reporting_hit):
        hits.append("qlik_export_reporting")
    if export_context_hit and any(
        _text_contains_term(lowered, term)
        for term in ["wrong values exported", "total line", "date export", "percentage changes", "0 values", "not exported"]
    ):
        hits.append("qlik_export_integrity")
    return hits


def _reddit_whitelist_hits(lowered: str) -> list[str]:
    """Return stricter Reddit rescue labels for operational reporting pain."""
    reporting_terms = ["report", "reporting", "dashboard", "analytics", "attribution", "power bi", "hubspot", "crm"]
    export_terms = ["export", "exports", "excel", "spreadsheet", "pdf"]
    pain_terms = ["manual", "manually", "mismatch", "not matching", "reconcile", "reconciliation", "double check", "stale", "weird", "wrong"]

    reporting_hit = any(_text_contains_term(lowered, term) for term in reporting_terms)
    export_hit = any(_text_contains_term(lowered, term) for term in export_terms)
    pain_hit = any(_text_contains_term(lowered, term) for term in pain_terms)

    hits: list[str] = []
    if reporting_hit and pain_hit:
        hits.append("reddit_reporting_pain")
    if export_hit and pain_hit:
        hits.append("reddit_export_reconciliation")
    if reporting_hit and export_hit:
        hits.append("reddit_reporting_export_combo")
    return hits


def _sisense_whitelist_hits(lowered: str) -> list[str]:
    """Return stricter Sisense rescue labels for reporting/export/build workflows."""
    reporting_terms = ["dashboard", "report", "reporting", "ssrs", "export", "exports", "xlsx", "csv", "pdf"]
    build_terms = ["build", "builds", "schedule builds", "builds in bulk", "cube", "cubes", "schedule all"]
    pain_terms = ["manual", "manually", "bulk", "best practice", "currently", "error details", "show filters", "show dashboard title"]

    reporting_hit = any(_text_contains_term(lowered, term) for term in reporting_terms)
    build_hit = any(_text_contains_term(lowered, term) for term in build_terms)
    pain_hit = any(_text_contains_term(lowered, term) for term in pain_terms)

    hits: list[str] = []
    if reporting_hit and pain_hit:
        hits.append("sisense_reporting_export_combo")
    if build_hit and pain_hit:
        hits.append("sisense_build_workflow_combo")
    if _text_contains_term(lowered, "replacing ssrs"):
        hits.append("sisense_replacing_ssrs")
    return hits


def _shopify_whitelist_hits(lowered: str) -> list[str]:
    """Return stricter Shopify rescue labels based on source-specific combos."""
    analytics_terms = ["report", "reporting", "analytics", "dashboard"]
    export_terms = ["export", "csv", "spreadsheet", "spreadsheets", "bank account", "tax", "payout", "settlement"]
    metric_terms = [
        "conversion",
        "checkout",
        "sessions",
        "sales",
        "revenue",
        "aov",
        "roas",
        "inventory",
        "stock on hand",
        "units sold",
        "sales history",
        "days on hand",
        "transaction",
        "total sales",
        "shipping",
        "refund",
        "payout",
        "fees",
        "taxes",
    ]
    mismatch_terms = ["discrepancy", "mismatch", "wrong numbers", "not matching", "off by", "source of truth"]
    trend_terms = ["compare periods", "weekly sales", "monthly sales", "trend", "cannot explain drop", "sales drop", "conversion drop"]
    reconciliation_terms = [
        "match against",
        "merge them together",
        "manual",
        "manually",
        "confusing",
        "basic report",
        "0 metrics",
        "stopped syncing",
        "has anyone found",
        "found any solutions",
        "reconcile",
        "reconciliation",
        "double check",
        "validate",
        "before sending",
        "sign off",
    ]
    finance_terms = ["finance", "accounting", "payout", "settlement", "bank deposit", "fees", "tax", "taxes"]
    cross_source_terms = ["ga4", "google analytics", "google ads", "meta ads", "facebook ads", "source of truth"]
    commerce_ops_terms = ["orders", "order count", "shipping", "shipment", "fulfilment", "payment", "payments", "inventory sync", "refund"]

    analytics_hit = any(_text_contains_term(lowered, term) for term in analytics_terms)
    metric_hit = any(_text_contains_term(lowered, term) for term in metric_terms)
    export_hit = any(_text_contains_term(lowered, term) for term in export_terms)
    mismatch_hit = any(_text_contains_term(lowered, term) for term in mismatch_terms)
    trend_hit = any(_text_contains_term(lowered, term) for term in trend_terms)
    reconciliation_hit = any(_text_contains_term(lowered, term) for term in reconciliation_terms)
    finance_hit = any(_text_contains_term(lowered, term) for term in finance_terms)
    cross_source_hit = any(_text_contains_term(lowered, term) for term in cross_source_terms)
    commerce_ops_hit = any(_text_contains_term(lowered, term) for term in commerce_ops_terms)

    hits: list[str] = []
    if analytics_hit and metric_hit:
        hits.append("shopify_reporting_metrics_combo")
    if mismatch_hit:
        hits.append("shopify_discrepancy_terms")
    if export_hit or _text_contains_term(lowered, "report"):
        hits.append("shopify_export_report")
    if metric_hit and reconciliation_hit:
        hits.append("shopify_inventory_reconcile_combo")
    if finance_hit and (mismatch_hit or reconciliation_hit or export_hit):
        hits.append("shopify_finance_reconciliation")
    if cross_source_hit and (mismatch_hit or analytics_hit):
        hits.append("shopify_cross_source_mismatch")
    if commerce_ops_hit and (mismatch_hit or reconciliation_hit):
        hits.append("shopify_checkout_shipping_discrepancy")
    if trend_hit or (metric_hit and any(_text_contains_term(lowered, term) for term in ["drop", "down", "decline", "fell"])):
        hits.append("shopify_conversion_sales_trend")
    return hits


def _google_developer_forums_whitelist_hits(lowered: str) -> list[str]:
    """Return Google Developer Forums rescue labels for BI trust and reporting workflow threads."""
    looker_terms = [
        "looker",
        "looker studio",
        "data studio",
        "lookml",
        "ga4",
        "google analytics 4",
        "bigquery",
        "scorecard",
        "bar chart",
        "dashboard",
        "alerts",
        "scheduled",
        "slack",
        "pivot",
    ]
    trust_terms = [
        "doesn't match",
        "does not match",
        "not the same",
        "values are different",
        "wrong",
        "mismatch",
        "showstopper",
        "not usable",
        "missing data",
        "filter confusion",
        "source of truth",
        "configuration error",
        "summary row incorrect",
        "stopped working",
        "returning null values",
        "incorrect result",
        "not working",
    ]
    workflow_terms = [
        "schedule",
        "scheduled report",
        "alerting",
        "dashboard",
        "export",
        "client",
        "stakeholder",
        "send",
        "slack",
        "look/dashboard",
        "pivot table",
        "blend data",
        "calculated metric",
        "calculated field",
        "summary row",
    ]

    looker_hit = any(_text_contains_term(lowered, term) for term in looker_terms)
    trust_hit = any(_text_contains_term(lowered, term) for term in trust_terms)
    workflow_hit = any(_text_contains_term(lowered, term) for term in workflow_terms)

    hits: list[str] = []
    if looker_hit and trust_hit:
        hits.append("google_bi_metric_mismatch")
    if looker_hit and workflow_hit:
        hits.append("google_reporting_workflow")
    if any(_text_contains_term(lowered, term) for term in ["scheduled", "slack", "alert", "alerts"]) and workflow_hit:
        hits.append("google_scheduled_reporting_delivery")
    return hits


def _adobe_analytics_whitelist_hits(lowered: str) -> list[str]:
    """Return Adobe Analytics rescue labels for Workspace trust and admin-analysis friction."""
    adobe_terms = [
        "adobe analytics",
        "workspace",
        "report suite",
        "segment",
        "calculated metric",
        "evar",
        "prop",
        "classification",
        "data feed",
        "debugger",
        "attribution",
        "anomaly",
        "cja",
        "customer journey analytics",
        "data warehouse",
        "virtual report suite",
        "people metric",
        "unique visitors",
    ]
    trust_terms = [
        "doesn't match",
        "does not appear",
        "not in workspace",
        "visible in debugger",
        "looks wrong",
        "mismatch",
        "spike",
        "drop",
        "not reliable",
        "difference between",
        "time delay",
        "different numbers",
        "grand total",
        "incorrect",
        "none",
        "unspecified",
    ]
    workflow_terms = [
        "analysis",
        "report",
        "workspace",
        "alert",
        "breakpoints",
        "monitor resolution",
        "mobile screen size",
        "customers metric",
        "seo",
        "direct traffic",
        "csv",
        "full table export",
        "data warehouse",
        "virtual report suite",
    ]

    adobe_hit = any(_text_contains_term(lowered, term) for term in adobe_terms)
    trust_hit = any(_text_contains_term(lowered, term) for term in trust_terms)
    workflow_hit = any(_text_contains_term(lowered, term) for term in workflow_terms)

    hits: list[str] = []
    if adobe_hit and trust_hit:
        hits.append("adobe_workspace_trust")
    if adobe_hit and workflow_hit:
        hits.append("adobe_reporting_analysis_workflow")
    if any(_text_contains_term(lowered, term) for term in ["debugger", "workspace"]) and trust_hit:
        hits.append("adobe_debugger_workspace_gap")
    return hits


def _domo_whitelist_hits(lowered: str) -> list[str]:
    """Return Domo rescue labels for card, ETL, and dataset workflow friction."""
    domo_terms = [
        "domo",
        "card",
        "dashboard",
        "analyzer",
        "beast mode",
        "magic etl",
        "dataflow",
        "dataset",
        "connector",
        "api",
        "filter card",
        "data table",
        "hourly chart",
        "pivot",
    ]
    trust_terms = [
        "wrong",
        "doesn't match",
        "does not match",
        "broken",
        "forcing",
        "issue",
        "error",
        "not allow",
        "not usable",
        "mismatch",
        "sync failed",
        "changed schema",
    ]
    workflow_terms = [
        "chart",
        "table",
        "filter",
        "export",
        "dataset",
        "etl",
        "append api",
        "connector",
        "card",
        "app",
    ]

    domo_hit = any(_text_contains_term(lowered, term) for term in domo_terms)
    trust_hit = any(_text_contains_term(lowered, term) for term in trust_terms)
    workflow_hit = any(_text_contains_term(lowered, term) for term in workflow_terms)

    hits: list[str] = []
    if domo_hit and trust_hit:
        hits.append("domo_card_workflow_friction")
    if domo_hit and workflow_hit:
        hits.append("domo_reporting_or_etl_workflow")
    if any(_text_contains_term(lowered, term) for term in ["beast mode", "magic etl", "dataset", "connector", "append api"]) and trust_hit:
        hits.append("domo_data_model_or_pipeline_gap")
    return hits


def _apply_google_developer_forums_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Google Developer Forums rescue scoring for Looker and reporting trust threads."""
    lowered = str(text or "").lower()
    looker_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "looker",
            "looker studio",
            "data studio",
            "lookml",
            "ga4",
            "google analytics 4",
            "scorecard",
            "bar chart",
            "pivot table",
            "blend data",
            "calculated metric",
            "calculated field",
        ]
    )
    mismatch_hit = any(
        _text_contains_term(lowered, term)
        for term in ["doesn't match", "does not match", "not the same", "values are different", "wrong", "mismatch", "showstopper", "not usable"]
    )
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "dashboard",
            "report",
            "scheduled",
            "slack",
            "alert",
            "alerts",
            "client",
            "stakeholder",
            "export",
            "look/dashboard",
            "pivot table",
            "blend data",
            "summary row",
            "configuration error",
        ]
    )
    operational_bug_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "configuration error",
            "summary row incorrect",
            "returning null values",
            "stopped working",
            "incorrect result",
            "not working",
            "unable to create a report",
            "502 error",
        ]
    )

    bonus = 0.0
    labels: list[str] = []
    if looker_hit and mismatch_hit:
        scores["dashboard_trust_score"] += 1.0
        scores["metric_definition_score"] += 0.65
        positive_hits.append(("google_looker_metric_mismatch", 1.65))
        source_reasons.append("google_looker_metric_mismatch")
        bonus += 0.85
        labels.append("google_looker_metric_mismatch")
    if looker_hit and reporting_hit:
        scores["reporting_pain_score"] += 0.75
        scores["stakeholder_pressure_score"] += 0.35
        positive_hits.append(("google_reporting_delivery", 1.1))
        source_reasons.append("google_reporting_delivery")
        bonus += 0.45
        labels.append("google_reporting_delivery")
    if looker_hit and operational_bug_hit:
        scores["root_cause_score"] += 0.7
        scores["reporting_pain_score"] += 0.45
        positive_hits.append(("google_operational_reporting_bug", 1.15))
        source_reasons.append("google_operational_reporting_bug")
        bonus += 0.55
        labels.append("google_operational_reporting_bug")
    return round(bonus, 4), labels


def _apply_adobe_analytics_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Adobe Analytics rescue scoring for Workspace trust and validation pain."""
    lowered = str(text or "").lower()
    adobe_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "adobe analytics",
            "workspace",
            "report suite",
            "segment",
            "evar",
            "prop",
            "classification",
            "debugger",
            "attribution",
            "cja",
            "customer journey analytics",
            "data warehouse",
            "virtual report suite",
            "people metric",
            "unique visitors",
        ]
    )
    trust_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "doesn't match",
            "not in workspace",
            "visible in debugger",
            "spike",
            "drop",
            "time delay",
            "not reliable",
            "difference between",
            "looks wrong",
            "different numbers",
            "grand total",
            "incorrect",
            "unspecified",
            "under none",
            "under direct",
        ]
    )
    analysis_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "analysis",
            "alert",
            "workspace",
            "customers metric",
            "seo",
            "direct traffic",
            "monitor resolution",
            "mobile screen size",
            "csv",
            "full table export",
            "data warehouse",
            "virtual report suite",
            "power bi",
        ]
    )
    export_or_metric_integrity_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "full table export",
            "csv downloads",
            "export csv",
            "data warehouse",
            "grand total",
            "people metric",
            "unique visitors",
            "typed/bookmarked",
            "none",
            "direct marketing channels",
        ]
    )

    bonus = 0.0
    labels: list[str] = []
    if adobe_hit and trust_hit:
        scores["dashboard_trust_score"] += 0.95
        scores["reporting_pain_score"] += 0.55
        positive_hits.append(("adobe_workspace_trust_gap", 1.5))
        source_reasons.append("adobe_workspace_trust_gap")
        bonus += 0.8
        labels.append("adobe_workspace_trust_gap")
    if adobe_hit and analysis_hit:
        scores["biz_workflow_score"] += 0.7
        positive_hits.append(("adobe_analysis_workflow_context", 0.7))
        source_reasons.append("adobe_analysis_workflow_context")
        bonus += 0.3
        labels.append("adobe_analysis_workflow_context")
    if adobe_hit and export_or_metric_integrity_hit:
        scores["reporting_pain_score"] += 0.55
        scores["metric_definition_score"] += 0.45
        positive_hits.append(("adobe_metric_export_integrity", 1.0))
        source_reasons.append("adobe_metric_export_integrity")
        bonus += 0.45
        labels.append("adobe_metric_export_integrity")
    return round(bonus, 4), labels


def _apply_domo_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Domo rescue scoring for chart, ETL, and card workflow pain."""
    lowered = str(text or "").lower()
    domo_hit = any(
        _text_contains_term(lowered, term)
        for term in ["domo", "card", "dashboard", "analyzer", "beast mode", "magic etl", "dataset", "connector", "append api", "filter card", "data table"]
    )
    trust_hit = any(
        _text_contains_term(lowered, term)
        for term in ["wrong", "forcing", "broken", "issue", "error", "mismatch", "not allow", "not usable", "sync failed", "changed schema"]
    )
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in ["chart", "table", "filter", "export", "dataset", "etl", "card", "app"]
    )

    bonus = 0.0
    labels: list[str] = []
    if domo_hit and trust_hit:
        scores["reporting_pain_score"] += 0.8
        scores["dashboard_trust_score"] += 0.65
        positive_hits.append(("domo_card_or_etl_friction", 1.45))
        source_reasons.append("domo_card_or_etl_friction")
        bonus += 0.75
        labels.append("domo_card_or_etl_friction")
    if domo_hit and reporting_hit:
        scores["biz_workflow_score"] += 0.65
        positive_hits.append(("domo_reporting_workflow_context", 0.65))
        source_reasons.append("domo_reporting_workflow_context")
        bonus += 0.25
        labels.append("domo_reporting_workflow_context")
    return round(bonus, 4), labels


def _apply_reddit_rescue_signals(
    text: str,
    subreddit: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Reddit-specific rescue scoring for operator-style reporting pain."""
    lowered = str(text or "").lower()
    relevant_subreddit = subreddit in {
        "r/analytics",
        "r/businessintelligence",
        "r/excel",
        "r/marketinganalytics",
        "r/powerbi",
        "r/tableau",
        "r/hubspot",
        "r/b2bmarketing",
    }
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in ["report", "reporting", "dashboard", "attribution", "analytics", "power bi", "hubspot", "crm"]
    )
    export_hit = any(_text_contains_term(lowered, term) for term in ["export", "exports", "excel", "spreadsheet", "pdf"])
    workflow_hit = any(
        _text_contains_term(lowered, term)
        for term in ["manual", "manually", "reconcile", "reconciliation", "double check", "weekly", "monthly", "stale"]
    )
    trust_hit = any(_text_contains_term(lowered, term) for term in ["mismatch", "not matching", "wrong", "weird", "source of truth"])

    bonus = 0.0
    labels: list[str] = []
    if relevant_subreddit and reporting_hit and (workflow_hit or trust_hit):
        scores["reporting_pain_score"] += 1.4
        bonus += 1.4
        labels.append("reddit_reporting_ops_context")
    if relevant_subreddit and export_hit and (workflow_hit or trust_hit):
        scores["excel_rework_score"] += 1.2
        bonus += 1.2
        labels.append("reddit_export_reconciliation")
    if relevant_subreddit and trust_hit:
        scores["dashboard_trust_score"] += 1.0
        bonus += 1.0
        labels.append("reddit_trust_pain")
    if labels:
        unique_labels = list(dict.fromkeys(labels))
        positive_hits.append(("reddit_source_whitelist_score", round(bonus, 2)))
        source_reasons.append(f"reddit_rescue_candidate:{'|'.join(unique_labels)}")
        return round(bonus, 4), unique_labels
    return 0.0, []


def _apply_sisense_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Sisense-specific rescue scoring for reporting/export/build workflows."""
    lowered = str(text or "").lower()
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in ["dashboard", "report", "reporting", "ssrs", "export", "exports", "xlsx", "csv", "pdf"]
    )
    build_hit = any(
        _text_contains_term(lowered, term)
        for term in ["build", "builds", "schedule builds", "builds in bulk", "cube", "cubes", "schedule all"]
    )
    pain_hit = any(
        _text_contains_term(lowered, term)
        for term in ["manual", "manually", "bulk", "best practice", "currently", "error details", "show filters", "show dashboard title"]
    )

    bonus = 0.0
    labels: list[str] = []
    if reporting_hit and pain_hit:
        scores["reporting_pain_score"] += 1.4
        scores["excel_rework_score"] += 0.9
        bonus += 2.3
        labels.append("sisense_reporting_export_combo")
    if build_hit and pain_hit:
        scores["biz_workflow_score"] += 1.1
        scores["root_cause_score"] += 0.9
        bonus += 2.0
        labels.append("sisense_build_workflow_combo")
    if _text_contains_term(lowered, "replacing ssrs"):
        scores["metric_definition_score"] += 1.2
        bonus += 1.2
        labels.append("sisense_replacing_ssrs")
    if labels:
        unique_labels = list(dict.fromkeys(labels))
        positive_hits.append(("sisense_source_whitelist_score", round(bonus, 2)))
        source_reasons.append(f"sisense_rescue_candidate:{'|'.join(unique_labels)}")
        return round(bonus, 4), unique_labels
    return 0.0, []


def _apply_mixpanel_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Mixpanel-specific rescue scoring for reporting trust and trend diagnosis."""
    lowered = str(text or "").lower()
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in ["report", "reports", "dashboard", "export", "csv", "insights", "funnel", "funnels", "retention", "breakdown", "session duration"]
    )
    trust_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "not matching",
            "mismatch",
            "discrepancy",
            "wrong",
            "source of truth",
            "says one thing",
            "says another",
            "doesn't match",
            "doesnt match",
            "difference between",
            "not correctly",
        ]
    )
    workflow_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "what changed", "which report should i use", "explain this drop", "trend changed", "figure out", "why does", "why is",
            "duplicate events", "duplicate event", "timezone differences", "timestamp timezone", "timezone mismatch",
            "undefined user emails", "export compared to reports", "raw usage data", "raw activity feed",
        ]
    )
    noise_hit = any(
        _text_contains_term(lowered, term)
        for term in ["api", "sdk", "instrumentation", "webhook", "mobile sdk", "send events"]
    )

    if noise_hit and not (trust_hit or workflow_hit):
        return 0.0, []

    bonus = 0.0
    labels: list[str] = []
    if reporting_hit and trust_hit:
        scores["dashboard_trust_score"] += 1.3
        scores["reporting_pain_score"] += 1.0
        bonus += 2.3
        labels.append("mixpanel_reporting_trust")
    if reporting_hit and workflow_hit:
        scores["root_cause_score"] += 1.0
        bonus += 1.0
        labels.append("mixpanel_trend_diagnosis")
    if trust_hit and any(_text_contains_term(lowered, term) for term in ["export", "csv", "dashboard", "reports"]):
        scores["excel_rework_score"] += 0.9
        bonus += 0.9
        labels.append("mixpanel_export_discrepancy")
    if reporting_hit and any(_text_contains_term(lowered, term) for term in ["source of truth", "which report should i use", "why does", "explain this drop"]):
        scores["dashboard_trust_score"] += 0.8
        scores["root_cause_score"] += 0.6
        bonus += 0.8
        labels.append("mixpanel_report_interpretation")
    if reporting_hit and any(
        _text_contains_term(lowered, term)
        for term in [
            "duplicate events",
            "duplicate event",
            "timestamp timezone",
            "timezone mismatch",
            "exported data",
            "export compared to reports",
            "raw usage data",
            "raw activity feed",
            "undefined user emails",
        ]
    ):
        scores["dashboard_trust_score"] += 0.9
        scores["excel_rework_score"] += 0.55
        bonus += 0.9
        labels.append("mixpanel_export_integrity")
    if labels:
        unique_labels = list(dict.fromkeys(labels))
        positive_hits.append(("mixpanel_source_whitelist_score", round(bonus, 2)))
        source_reasons.append(f"mixpanel_rescue_candidate:{'|'.join(unique_labels)}")
        return round(bonus, 4), unique_labels
    return 0.0, []


def _apply_klaviyo_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Klaviyo-specific rescue scoring for attribution, segmentation, and reporting trust."""
    lowered = str(text or "").lower()
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "report",
            "reporting",
            "analytics",
            "benchmark",
            "export",
            "csv",
            "weekly reporting",
            "export excel",
            "custom report",
            "google sheets",
            "segment export",
            "power query",
            "skip reason",
            "message was skipped",
            "skipped report",
            "overview dashboard",
            "campaigns breakdown by segment",
            "bulk export",
            "ga4",
            "google analytics",
            "campaign attribution",
            "signup report",
            "conversion report",
            "stakeholder reporting",
            "performance summary",
            "report mismatch",
            "custom reports page",
            "predictive metrics",
            "churn risk",
            "attribution window",
            "flow analytics",
            "open rate",
            "open rates",
            "click rate",
            "click rates",
            "bounce rate",
            "bounce rates",
        ]
    )
    trust_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "source of truth",
            "not matching",
            "mismatch",
            "discrepancy",
            "reporting lag",
            "reconcile",
            "reconciliation",
            "what changed",
            "why did",
            "missing profiles",
            "missing emails",
            "skewed",
            "inflated",
            "compare",
            "does not equal",
            "doesn't equal",
            "doesnt equal",
            "source of truth changed",
            "numbers drifted",
            "count changed",
            "report is off",
            "numbers do not line up",
            "inflated",
            "full price",
            "discounted price",
            "cancelled orders",
            "showing conversions are at zero",
            "error loading",
            "not count as revenue",
            "more revenue than klaviyo",
            "odd and inaccurate numbers",
            "inaccurate numbers",
            "only pulling",
            "failing its own filters",
            "open rates dropping",
            "open rate dropped",
            "dropped pretty significantly",
        ]
    )
    segment_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "segment count",
            "list count",
            "profile count",
            "segmentation",
            "segment export",
            "missing profiles",
            "campaigns breakdown by segment",
            "compare segments",
            "segment performance",
            "segment report discrepancy",
            "segment mismatch",
            "segment not working properly",
        ]
    )
    attribution_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "attributed revenue",
            "revenue mismatch",
            "attribution mismatch",
            "attribution",
            "ga4",
            "google analytics",
            "recharge",
            "revenue attribution skewed",
            "campaign attribution",
            "conversion attribution",
        ]
    )
    ops_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "before sending",
            "manual spreadsheet",
            "weekly reporting",
            "export excel",
            "google sheets",
            "power query",
            "custom report",
            "overview dashboard",
            "bulk export",
            "export all metric data",
            "stakeholder reporting",
            "performance summary",
            "before sign-off",
            "flow analytics",
        ]
    )
    export_integrity_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "google sheets",
            "segment export",
            "exported emails",
            "custom report",
            "power query",
            "revenue attribution skewed",
            "missing profiles",
            "missing emails",
            "bulk export",
            "export all metric data",
            "overview dashboard",
            "report mismatch",
            "campaign attribution",
            "conversion report",
            "custom report",
            "form metrics",
            "conversion rate",
            "views by form",
            "sign ups",
            "flow analytics",
            "open rate",
            "open rates",
            "click rate",
            "click rates",
            "bounce rate",
            "bounce rates",
        ]
    )
    analysis_workaround_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "external app",
            "visualizations",
            "conversion rate via api",
            "pull form metrics via api",
            "custom report",
            "average days between orders",
            "churn rate",
            "static value",
            "dynamic one",
            "further data manipulation",
            "predictive metrics",
            "churn risk",
        ]
    )
    skip_reason_hit = any(
        _text_contains_term(lowered, term)
        for term in ["skipped report", "message was skipped", "skip reason"]
    )

    bonus = 0.0
    labels: list[str] = []
    if reporting_hit and trust_hit:
        scores["dashboard_trust_score"] += 1.0
        bonus += 1.0
        labels.append("klaviyo_reporting_trust")
    if segment_hit and trust_hit:
        scores["segmentation_breakdown_score"] += 0.9
        scores["dashboard_trust_score"] += 0.5
        bonus += 0.9
        labels.append("klaviyo_segment_count_reconcile")
    if attribution_hit and trust_hit:
        scores["dashboard_trust_score"] += 0.9
        scores["root_cause_score"] += 0.5
        bonus += 0.8
        labels.append("klaviyo_revenue_attribution_trust")
    if ops_hit and (reporting_hit or trust_hit):
        scores["excel_rework_score"] += 0.7
        scores["reporting_pain_score"] += 0.4
        bonus += 0.6
        labels.append("klaviyo_export_report_trust")
    if reporting_hit and segment_hit and any(
        _text_contains_term(lowered, term)
        for term in ["compare", "breakdown", "overview dashboard", "performance", "not showing", "missing"]
    ):
        scores["segmentation_breakdown_score"] += 0.8
        scores["reporting_pain_score"] += 0.4
        bonus += 0.6
        labels.append("klaviyo_segment_reporting_breakdown")
    if export_integrity_hit and (reporting_hit or trust_hit or attribution_hit or segment_hit):
        scores["excel_rework_score"] += 0.55
        scores["reporting_pain_score"] += 0.45
        bonus += 0.65
        labels.append("klaviyo_export_integrity")
    if skip_reason_hit and reporting_hit:
        scores["root_cause_score"] += 1.0
        scores["reporting_pain_score"] += 0.6
        bonus += 0.95
        labels.append("klaviyo_skip_reason_reporting")
    if reporting_hit and any(
        _text_contains_term(lowered, term)
        for term in [
            "stakeholder reporting",
            "performance summary",
            "before sign-off",
            "before sharing numbers",
        ]
    ):
        scores["stakeholder_pressure_score"] += 0.8
        scores["reporting_pain_score"] += 0.35
        bonus += 0.55
        labels.append("klaviyo_stakeholder_reporting")
    if analysis_workaround_hit and reporting_hit and trust_hit:
        scores["adhoc_analysis_score"] += 0.8
        scores["excel_rework_score"] += 0.35
        scores["reporting_pain_score"] += 0.35
        bonus += 0.7
        labels.append("klaviyo_analysis_workaround")
    if reporting_hit and trust_hit and any(
        _text_contains_term(lowered, term)
        for term in [
            "cancelled orders",
            "full price",
            "discounted price",
            "attribution window",
            "showing conversions are at zero",
            "error loading",
            "custom reports page",
            "more revenue than klaviyo",
            "predictive metrics",
            "churn risk",
            "flow analytics",
            "open rate",
            "open rates",
            "click rate",
            "click rates",
            "bounce rate",
            "bounce rates",
            "failing its own filters",
            "inaccurate numbers",
            "only pulling",
        ]
    ):
        scores["dashboard_trust_score"] += 0.9
        scores["root_cause_score"] += 0.45
        bonus += 0.7
        labels.append("klaviyo_reporting_math")
    if labels:
        unique_labels = list(dict.fromkeys(labels))
        positive_hits.append(("klaviyo_source_whitelist_score", round(bonus, 2)))
        source_reasons.append(f"klaviyo_rescue_candidate:{'|'.join(unique_labels)}")
        return round(bonus, 4), unique_labels
    return 0.0, []


def _apply_qlik_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Qlik-specific rescue scoring for export integrity and reconciliation pain."""
    lowered = str(text or "").lower()
    reporting_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "report",
            "reporting",
            "dashboard",
            "board report",
            "adhoc reporting",
            "nprinting",
            "pixel perfect",
            "export",
            "export to excel",
            "export excel",
            "pivot table",
            "straight table",
            "measure",
            "measures",
            "reload",
        ]
    )
    trust_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "wrong total",
            "wrong totals",
            "not matching",
            "mismatch",
            "cannot explain",
            "can't explain",
            "root cause",
            "wrong values exported",
            "total line",
            "not exported",
            "incorrect",
            "incorrect values",
            "correct in excel",
            "huge difference",
            "reload result mismatch",
            "measure inconsistency",
            "number trust",
            "dashboard number trust",
        ]
    )
    export_integrity_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "wrong values exported",
            "total line",
            "date export",
            "percentage changes",
            "0 values",
            "not exported",
            "e format",
            "shown as text",
            "same excel file",
            "one excel workbook",
            "pivot export",
            "straight table export",
            "pixel perfect export",
        ]
    )
    reconcile_hit = any(
        _text_contains_term(lowered, term)
        for term in [
            "reconcile",
            "reconciled",
            "difference in reconciliation",
            "compare the data",
            "comparison table",
            "summary and detail",
            "business users",
            "reload",
            "reload task",
            "after reload",
        ]
    )
    generic_helper_only = any(
        _text_contains_term(lowered, term)
        for term in [
            "sort order",
            "chart color",
            "color few columns",
            "trendline",
            "show totals",
            "subtotals",
            "drill another sheet",
        ]
    )

    if generic_helper_only and not (trust_hit or export_integrity_hit or reconcile_hit):
        return 0.0, []

    bonus = 0.0
    labels: list[str] = []
    if reporting_hit and trust_hit:
        scores["reporting_pain_score"] += 0.9
        scores["dashboard_trust_score"] += 0.8
        bonus += 0.9
        labels.append("qlik_reporting_mismatch")
    if export_integrity_hit:
        scores["excel_rework_score"] += 0.9
        scores["dashboard_trust_score"] += 0.35
        bonus += 0.65
        labels.append("qlik_export_integrity")
    if reconcile_hit and (trust_hit or reporting_hit):
        scores["root_cause_score"] += 0.65
        scores["biz_workflow_score"] += 0.4
        bonus += 0.55
        labels.append("qlik_reconciliation_context")
    if reporting_hit and any(
        _text_contains_term(lowered, term)
        for term in [
            "pivot table",
            "straight table",
            "measure inconsistency",
            "dashboard number trust",
            "reload result mismatch",
        ]
    ):
        scores["metric_definition_score"] += 0.7
        scores["dashboard_trust_score"] += 0.35
        bonus += 0.55
        labels.append("qlik_measure_trust")
    if labels:
        unique_labels = list(dict.fromkeys(labels))
        positive_hits.append(("qlik_source_whitelist_score", round(bonus, 2)))
        source_reasons.append(f"qlik_rescue_candidate:{'|'.join(unique_labels)}")
        return round(bonus, 4), unique_labels
    return 0.0, []


def _apply_shopify_rescue_signals(
    text: str,
    scores: dict[str, float],
    positive_hits: list[tuple[str, float]],
    source_reasons: list[str],
) -> tuple[float, list[str]]:
    """Apply Shopify-specific rescue scoring for reporting and performance language."""
    lowered = str(text or "").lower()
    analytics_terms = ["report", "reporting", "analytics", "dashboard"]
    export_terms = ["export", "csv", "spreadsheet", "payout", "settlement"]
    metric_terms = ["conversion", "checkout", "sessions", "sales", "revenue", "aov", "roas", "orders", "shipping", "refund", "inventory", "payout"]
    mismatch_terms = ["discrepancy", "mismatch", "wrong numbers", "not matching", "off by", "source of truth"]
    trend_terms = ["compare periods", "weekly sales", "monthly sales", "trend", "sales drop", "conversion drop", "cannot explain drop"]
    tracking_terms = ["attribution", "pixel", "tracking", "customer segment"]
    performance_terms = ["product performance", "channel performance", "store performance"]
    finance_terms = ["finance", "accounting", "bank account", "bank deposit", "payout", "settlement", "fees", "tax", "taxes"]
    cross_source_terms = ["ga4", "google analytics", "google ads", "meta ads", "facebook ads"]
    commerce_ops_terms = ["orders", "order count", "shipping", "shipment", "fulfilment", "payment", "payments", "refund", "inventory sync"]
    validation_terms = ["before sending", "sign off", "validate", "validation", "double check", "reconcile", "reconciliation"]

    analytics_hit = any(_text_contains_term(lowered, term) for term in analytics_terms)
    metric_hit = any(_text_contains_term(lowered, term) for term in metric_terms)
    mismatch_hit = any(_text_contains_term(lowered, term) for term in mismatch_terms)
    export_hit = any(_text_contains_term(lowered, term) for term in export_terms)
    trend_hit = any(_text_contains_term(lowered, term) for term in trend_terms)
    tracking_hit = any(_text_contains_term(lowered, term) for term in tracking_terms)
    performance_hit = any(_text_contains_term(lowered, term) for term in performance_terms)
    finance_hit = any(_text_contains_term(lowered, term) for term in finance_terms)
    cross_source_hit = any(_text_contains_term(lowered, term) for term in cross_source_terms)
    commerce_ops_hit = any(_text_contains_term(lowered, term) for term in commerce_ops_terms)
    validation_hit = any(_text_contains_term(lowered, term) for term in validation_terms)

    bonus = 0.0
    labels: list[str] = []
    if analytics_hit and metric_hit:
        scores["reporting_pain_score"] += 1.2
        bonus += 1.2
        labels.append("shopify_analytics_metric_combo")
    if mismatch_hit:
        scores["dashboard_trust_score"] += 1.3
        bonus += 1.3
        labels.append("shopify_mismatch_discrepancy")
    if export_hit or _text_contains_term(lowered, "report"):
        scores["excel_rework_score"] += 0.9
        bonus += 0.9
        labels.append("shopify_export_report")
    if trend_hit:
        scores["root_cause_score"] += 1.0
        bonus += 1.0
        labels.append("shopify_conversion_sales_trend")
    if tracking_hit:
        scores["metric_definition_score"] += 0.8
        bonus += 0.8
        labels.append("shopify_attribution_tracking")
    if performance_hit:
        scores["segmentation_breakdown_score"] += 0.8
        bonus += 0.8
        labels.append("shopify_performance_views")
    if finance_hit and (mismatch_hit or validation_hit or export_hit):
        scores["dashboard_trust_score"] += 1.2
        scores["excel_rework_score"] += 0.8
        bonus += 2.0
        labels.append("shopify_finance_reconciliation")
    if cross_source_hit and (mismatch_hit or analytics_hit or tracking_hit):
        scores["metric_definition_score"] += 1.0
        scores["dashboard_trust_score"] += 1.0
        bonus += 2.0
        labels.append("shopify_cross_source_mismatch")
    if commerce_ops_hit and (mismatch_hit or validation_hit):
        scores["biz_workflow_score"] += 0.9
        scores["root_cause_score"] += 0.8
        bonus += 1.7
        labels.append("shopify_checkout_shipping_discrepancy")
    if validation_hit:
        scores["stakeholder_pressure_score"] += 0.8
        bonus += 0.8
        labels.append("shopify_validation_signoff")
    if labels:
        unique_labels = list(dict.fromkeys(labels))
        positive_hits.append(("shopify_source_whitelist_score", round(bonus, 2)))
        source_reasons.append(f"shopify_rescue_candidate:{'|'.join(unique_labels)}")
        return round(bonus, 4), unique_labels
    return 0.0, []


def _text_contains_term(text: str, term: str) -> bool:
    """Match a single word or phrase using token-aware boundaries."""
    normalized_term = str(term or "").strip().lower()
    if not normalized_term:
        return False
    pattern = r"\b" + r"\s+".join(re.escape(part) for part in normalized_term.split()) + r"\b"
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def _derive_dropped_reason(
    source: str,
    text: str,
    scores: dict[str, float],
    whitelist_labels: list[str],
    source_reasons: list[str],
) -> str:
    """Return a more specific dropped-reason string than generic source fallback."""
    if whitelist_labels:
        return f"drop_after_whitelist_no_rescue:{whitelist_labels[0]}"
    positive_total = sum(scores[column] for column in POSITIVE_SCORE_COLUMNS)
    negative_total = sum(scores[column] for column in NEGATIVE_SCORE_COLUMNS)
    lowered = str(text or "").lower()
    if source == "hubspot_community":
        return "prefilter_missing_hubspot_reporting_terms"
    if source == "klaviyo_community":
        lowered_reasons = " ".join(source_reasons).lower()
        if "klaviyo_revenue_attribution_trust" in lowered_reasons or "klaviyo_reporting" in lowered_reasons:
            return "missing_klaviyo_revenue_reporting_terms"
        if "klaviyo_segment_count_reconcile" in lowered_reasons:
            return "missing_klaviyo_segment_reconciliation_terms"
        if "klaviyo_export_report_trust" in lowered_reasons:
            return "missing_klaviyo_export_trust_terms"
        return "prefilter_missing_klaviyo_reporting_terms"
    if source == "mixpanel_community":
        return "prefilter_missing_mixpanel_reporting_terms"
    if source == "qlik_community":
        return "prefilter_missing_qlik_reporting_terms"
    if source == "metabase_discussions":
        return "prefilter_missing_metabase_query_dashboard_terms"
    if source == "shopify_community":
        lowered_reasons = " ".join(source_reasons).lower()
        if "shopify_export_report" in lowered_reasons or "shopify_analytics_metric_combo" in lowered_reasons:
            return "missing_shopify_reporting_terms"
        if "shopify_mismatch_discrepancy" in lowered_reasons:
            return "missing_shopify_discrepancy_terms"
        if "shopify_conversion_sales_trend" in lowered_reasons or "shopify_attribution_tracking" in lowered_reasons:
            return "missing_shopify_performance_terms"
        return "prefilter_missing_shopify_reporting_terms"
    if source == "github_discussions" and any("issue_template_downweight" in reason for reason in source_reasons):
        return "prefilter_github_issue_template_penalty"
    if positive_total <= 0.0:
        return "prefilter_missing_source_language"
    if negative_total >= 5.0:
        return "prefilter_technical_only_drop"
    return "prefilter_below_threshold"


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


def _apply_source_specific_floor_override(
    source: str,
    source_cfg: dict[str, Any],
    text: str,
    source_reasons: list[str],
    final_score: float,
    current_decision: str,
) -> str:
    """Apply narrow source-specific false-negative rescue floors for known discourse patterns."""
    if current_decision != "drop":
        return current_decision
    lowered = str(text or "").lower()
    if source == "stackoverflow":
        floor = float(source_cfg.get("borderline_floor_with_tag_boost", 5.5))
        discourse_floor = float(source_cfg.get("borderline_floor_with_discourse_context", floor))
        has_tag_boost = any("stackoverflow_tag_boost:" in reason for reason in source_reasons)
        has_persona_pain_language = any(
            _text_contains_term(lowered, term)
            for term in [
                "export",
                "excel",
                "pivot",
                "dashboard",
                "report",
                "reporting",
                "spreadsheet",
                "manual upload",
                "workaround",
                "mismatch",
                "not matching",
                "wrong values",
            ]
        )
        has_export_output_shape = any(
            _text_contains_term(lowered, term)
            for term in [
                "datatable",
                "data table",
                "grid",
                "table header",
                "row above table header",
                "current page",
                "column names",
                "white spaces",
                "blank spaces",
                "format",
                "filename",
                "sheet1",
                "cell",
                "cells",
                "rows",
                "columns",
            ]
        )
        if has_tag_boost and has_persona_pain_language and final_score >= floor:
            return "borderline"
        if has_persona_pain_language and has_export_output_shape and final_score >= max(discourse_floor - 0.25, 4.75):
            return "borderline"
        discourse_hits = sum(
            1 for term in source_cfg.get("discourse_rescue_terms", []) or [] if _text_contains_term(lowered, str(term).lower())
        )
        if discourse_hits >= 2 and has_persona_pain_language and final_score >= discourse_floor:
            return "borderline"
        has_reconciliation_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "source of truth",
                "summary different from detail",
                "dashboard vs",
                "wrong total",
                "wrong totals",
                "group by",
                "duplicate rows",
                "date basis",
                "date filter",
                "created_at",
                "event_date",
                "rows vs totals",
            ]
        )
        if has_tag_boost and has_reconciliation_context and final_score >= 4.3:
            return "borderline"
    if source == "github_discussions":
        floor = float(source_cfg.get("borderline_floor_with_workflow_context", 5.5))
        strong_floor = float(source_cfg.get("borderline_floor_with_strong_workflow_context", floor))
        has_workflow_context = any("github_discussions_workflow_context" in reason for reason in source_reasons)
        has_persona_pain_language = any(
            _text_contains_term(lowered, term)
            for term in [
                "export",
                "excel",
                "csv",
                "pivot",
                "matrix",
                "metric definition",
                "source of truth",
                "numbers don't match",
                "report mismatch",
            ]
        )
        if has_workflow_context and has_persona_pain_language and final_score >= floor:
            return "borderline"
        strong_workflow_hits = sum(
            1
            for term in source_cfg.get("strong_workflow_context_terms", []) or []
            if _text_contains_term(lowered, str(term).lower())
        )
        if has_workflow_context and strong_workflow_hits >= 2 and final_score >= strong_floor:
            return "borderline"
        has_metric_semantics_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "source of truth",
                "numbers don't match",
                "numbers do not match",
                "metric definition",
                "wrong totals",
                "ratio metric",
                "denominator",
                "drill down",
                "drill-down",
                "share of total",
                "conversion rate",
            ]
        )
        if has_workflow_context and has_metric_semantics_context and final_score >= 4.7:
            return "borderline"
    if source == "reddit":
        floor = float(source_cfg.get("borderline_floor_with_reporting_context", 4.6))
        strong_floor = float(source_cfg.get("borderline_floor_with_operational_reporting_context", 3.0))
        has_subreddit_context = any("reddit_subreddit_boost:" in reason for reason in source_reasons)
        has_reporting_combo = any(
            _text_contains_term(lowered, term)
            for term in ["report", "reporting", "dashboard", "excel", "spreadsheet", "export", "reconcile", "attribution"]
        )
        has_operational_pain = any(
            _text_contains_term(lowered, term)
            for term in ["manual", "manually", "stale", "double check", "not matching", "mismatch", "wrong", "weird", "hours"]
        )
        if has_subreddit_context and has_reporting_combo and final_score >= floor:
            return "borderline"
        if has_subreddit_context and has_reporting_combo and has_operational_pain and final_score >= strong_floor:
            return "borderline"
    if source == "shopify_community":
        floor = float(source_cfg.get("borderline_floor_with_reporting_context", 4.8))
        has_reporting_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "report",
                "reporting",
                "analytics",
                "dashboard",
                "export",
                "spreadsheet",
                "inventory",
                "stock on hand",
                "units sold",
                "sales history",
                "days on hand",
                "google ads",
                "ga4",
                "payout",
                "settlement",
                "fees",
                "taxes",
                "shipping",
                "refund",
            ]
        )
        has_operational_pain = any(
            _text_contains_term(lowered, term)
            for term in [
                "manual",
                "manually",
                "confusing",
                "match against",
                "merge them together",
                "0 metrics",
                "stopped syncing",
                "looking for",
                "has anyone found",
                "what works",
                "reconcile",
                "reconciliation",
                "before sending",
                "sign off",
                "validate",
                "double check",
                "source of truth",
            ]
        )
        if has_reporting_context and has_operational_pain and final_score >= floor:
            return "borderline"
        has_cross_source_validation = any(
            _text_contains_term(lowered, term)
            for term in [
                "ga4",
                "google analytics",
                "google ads",
                "meta ads",
                "facebook ads",
                "shop pay",
                "checkout",
                "payout",
                "stripe",
                "source of truth",
            ]
        )
        if has_reporting_context and has_cross_source_validation and final_score >= max(floor - 1.2, 2.6):
            return "borderline"
    if source == "qlik_community":
        floor = float(source_cfg.get("borderline_floor_with_reporting_context", 4.8))
        strong_reporting_pain = any(
            _text_contains_term(lowered, term)
            for term in [
                "wrong total",
                "wrong totals",
                "not matching",
                "mismatch",
                "cannot explain",
                "can't explain",
                "root cause",
                "manual reporting",
                "nprinting",
                "pixel perfect",
                "export",
                "export to excel",
                "wrong values exported",
                "total line",
                "date export",
                "adhoc reporting",
                "reconcile",
                "reconciled",
                "difference in reconciliation",
                "correct in excel",
                "huge difference",
            ]
        )
        generic_chart_help = any(
            _text_contains_term(lowered, term)
            for term in [
                "straight table",
                "pivot table",
                "combo chart",
                "chart color",
                "chart label",
                "dimension",
                "expression",
                "filter pane",
                "show value",
                "hide column",
                "sort order",
            ]
        )
        chart_integrity_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "wrong results",
                "right results",
                "not correct",
                "incorrect",
                "too large",
                "0 values",
                "missing values",
                "exporting",
                "export to excel",
            ]
        )
        if generic_chart_help and not (strong_reporting_pain or chart_integrity_context):
            return "drop"
        qlik_export_context = any(
            _text_contains_term(lowered, term)
            for term in ["board report", "export to excel", "export excel", "export issue", "nprinting", "pixel perfect", "adhoc reporting"]
        )
        qlik_export_pain = any(
            _text_contains_term(lowered, term)
            for term in [
                "wrong", "mismatch", "not correct", "doesn't add up", "doesnt add up", "failed", "limitation", "unavailable",
                "wrong values exported", "total line", "percentage changes", "date export",
                "e format", "shown as text", "correct in excel", "huge difference",
            ]
        )
        if qlik_export_context and qlik_export_pain and final_score >= floor:
            return "borderline"
        if qlik_export_context and qlik_export_pain and final_score >= 2.4:
            return "borderline"
        qlik_reconcile_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "reconcile",
                "reconciled",
                "difference in reconciliation",
                "compare the data",
                "comparison table",
                "summary and detail",
                "correct in excel",
            ]
        )
        if qlik_reconcile_context and strong_reporting_pain and final_score >= 2.55:
            return "borderline"
        if chart_integrity_context and final_score >= 1.8:
            return "borderline"
    if source == "klaviyo_community":
        floor = float(source_cfg.get("borderline_floor_with_reporting_context", 4.9))
        has_reporting_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "revenue",
                "attributed revenue",
                "attribution",
                "reporting",
                "analytics",
                "export",
                "csv",
                "benchmark",
                "conversion",
                "segment count",
                "list count",
                "profile count",
                "source of truth",
                "weekly reporting",
                "export excel",
                "manual spreadsheet",
                "segmentation",
                "google sheets",
                "power query",
                "custom report",
                "overview dashboard",
                "segment export",
                "ga4",
                "google analytics",
                "flow analytics",
                "open rate",
                "open rates",
                "click rate",
                "click rates",
                "bounce rate",
                "bounce rates",
            ]
        )
        has_operational_pain = any(
            _text_contains_term(lowered, term)
            for term in [
                "mismatch",
                "not matching",
                "discrepancy",
                "reporting lag",
                "what changed",
                "not syncing",
                "not being added",
                "manual process",
                "reconcile",
                "reconciliation",
                "before sending",
                "why did",
                "skip reason",
                "skipped report",
                "inflated",
                "skewed",
                "missing profiles",
                "missing emails",
                "limitation",
                "missing something",
                "not able to",
                "can't seem to find",
                "cannot seem to find",
                "inflated",
                "full price",
                "discounted price",
                "cancelled orders",
                "conversions are at zero",
                "error loading",
                "not count as revenue",
                "more revenue than klaviyo",
                "odd and inaccurate numbers",
                "inaccurate numbers",
                "only pulling",
                "failing its own filters",
                "open rates dropping",
                "open rate dropped",
                "dropped pretty significantly",
            ]
        )
        if has_reporting_context and has_operational_pain and final_score >= floor:
            return "borderline"
        has_export_integrity_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "google sheets",
                "segment export",
                "exported emails",
                "custom report",
                "power query",
                "skip reason",
                "message was skipped",
                "skipped report",
                "missing profiles",
                "missing emails",
                "bulk export",
                "export all metric data",
                "overview dashboard",
                "ga4",
                "google analytics",
                "custom reports page",
                "predictive metrics",
                "churn risk",
                "attribution window",
            ]
        )
        if has_reporting_context and has_export_integrity_context and final_score >= max(floor - 1.2, 2.15):
            return "borderline"
        has_analysis_workaround_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "external app",
                "visualizations",
                "conversion rate via api",
                "pull form metrics via api",
                "form metrics",
                "views by form",
                "custom report",
                "average days between orders",
                "churn rate",
                "static value",
                "dynamic one",
                "predictive metrics",
                "churn risk",
            ]
        )
        if has_reporting_context and has_analysis_workaround_context and final_score >= max(floor - 1.45, 2.0):
            return "borderline"
    if source == "google_developer_forums":
        floor = float(source_cfg.get("borderline_floor_with_reporting_context", 4.0))
        has_reporting_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "looker",
                "looker studio",
                "data studio",
                "dashboard",
                "report",
                "export",
                "pivot table",
                "blend data",
                "calculated metric",
                "calculated field",
                "summary row",
            ]
        )
        has_operational_pain = any(
            _text_contains_term(lowered, term)
            for term in [
                "configuration error",
                "returning null values",
                "stopped working",
                "incorrect result",
                "not working",
                "502 error",
                "null values",
                "summary row incorrect",
            ]
        )
        if has_reporting_context and has_operational_pain and final_score >= max(floor - 1.6, 2.1):
            return "borderline"
    if source == "adobe_analytics_community":
        floor = float(source_cfg.get("borderline_floor_with_reporting_context", 4.0))
        has_reporting_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "workspace",
                "report suite",
                "cja",
                "customer journey analytics",
                "data warehouse",
                "segment",
                "calculated metric",
                "data feed",
                "csv",
                "full table export",
                "virtual report suite",
                "unique visitors",
                "people metric",
            ]
        )
        has_operational_pain = any(
            _text_contains_term(lowered, term)
            for term in [
                "different numbers",
                "grand total",
                "not in workspace",
                "visible in debugger",
                "difference between",
                "looks wrong",
                "under none",
                "under direct",
                "incorrect",
            ]
        )
        if has_reporting_context and has_operational_pain and final_score >= max(floor - 1.7, 2.0):
            return "borderline"
    if source == "mixpanel_community":
        floor = float(source_cfg.get("borderline_floor_with_reporting_context", 4.2))
        has_reporting_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "report",
                "dashboard",
                "export",
                "csv",
                "insights",
                "funnel",
                "retention",
                "breakdown",
                "session duration",
                "source of truth",
            ]
        )
        has_operational_pain = any(
            _text_contains_term(lowered, term)
            for term in [
                "mismatch",
                "not matching",
                "discrepancy",
                "wrong",
                "what changed",
                "which report should i use",
                "explain this drop",
                "figure out",
                "duplicate events",
                "timestamp timezone",
                "undefined user emails",
            ]
        )
        if has_reporting_context and has_operational_pain and final_score >= floor:
            return "borderline"
        has_export_integrity_context = any(
            _text_contains_term(lowered, term)
            for term in [
                "exported data",
                "export compared to reports",
                "timestamp timezone",
                "timezone mismatch",
                "duplicate events",
                "duplicate event",
                "raw usage data",
                "raw activity feed",
                "undefined user emails",
            ]
        )
        if has_reporting_context and has_export_integrity_context and final_score >= max(floor - 1.0, 2.35):
            return "borderline"
    if source == "sisense_community":
        floor = float(source_cfg.get("borderline_floor_with_reporting_context", 3.4))
        has_reporting_combo = any(
            _text_contains_term(lowered, term)
            for term in ["export", "exports", "xlsx", "csv", "pdf", "reporting", "report", "ssrs", "schedule builds", "builds in bulk"]
        )
        if has_reporting_combo and final_score >= floor:
            return "borderline"
    return current_decision


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
        "prefilter_score",
        "biz_user_score",
        "top_positive_signals",
        "top_negative_signals",
        "score_breakdown",
        "source_specific_reason",
        "prefilter_reason",
        "whitelist_hits",
        "rescue_reason",
        "dropped_reason",
    ]


def _dedupe_column_names(columns: list[str]) -> list[str]:
    """Return column names without duplicates while preserving order."""
    seen: set[str] = set()
    deduped: list[str] = []
    for column in columns:
        if column in seen:
            continue
        seen.add(column)
        deduped.append(column)
    return deduped


def _apply_optional_llm_hook(df: pd.DataFrame, rules: dict[str, Any], llm_hook: LlmHook | None):
    """Run an optional LLM relevance hook only when explicitly enabled."""
    if not bool(rules.get("enable_llm_relevance", False)):
        return df
    if llm_hook is None:
        return df
    return llm_hook(df.copy())
