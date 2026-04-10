"""Validity filtering for normalized posts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(slots=True)
class FilterEvaluation:
    """Evaluation result for one normalized row."""

    invalid_reason: str
    business_signal_score: int
    pain_signal_score: int
    business_signal_terms: str
    pain_signal_terms: str


def _match_keywords(text: str, keywords: list[str]) -> list[str]:
    """Return keywords that appear in the text."""
    lowered = text.lower()
    return [keyword for keyword in keywords if keyword.lower() in lowered]


def _keywords_for_row(rules: dict[str, Any], row: pd.Series, key: str) -> list[str]:
    """Return global plus optional source-specific keywords for one row."""
    keywords = [str(item) for item in rules.get(key, []) or []]
    source_id = str(row.get("source", "") or "")
    overrides = rules.get("source_signal_overrides", {}) or {}
    source_overrides = overrides.get(source_id, {}) if isinstance(overrides, dict) else {}
    extra_keywords = [str(item) for item in source_overrides.get(key, []) or []]
    return list(dict.fromkeys([*keywords, *extra_keywords]))


def _evaluate_row(row: pd.Series, rules: dict[str, Any]) -> FilterEvaluation:
    """Evaluate one normalized row against invalid and signal rules."""
    reasons: list[str] = []
    text_len = int(row.get("text_len", 0) or 0)
    language = str(row.get("language", "") or "").lower()
    title = str(row.get("title", "") or "")
    body = str(row.get("body", "") or "")
    comments_text = str(row.get("comments_text", "") or "")
    raw_text = str(row.get("raw_text", "") or "")
    combined_text = " ".join([title, body, comments_text, raw_text]).lower()

    if text_len < int(rules.get("min_text_len", 0)):
        reasons.append("text_too_short")
    if language in {lang.lower() for lang in rules.get("exclude_languages", [])}:
        reasons.append("excluded_language")
    if _match_keywords(combined_text, rules.get("spam_keywords", [])):
        reasons.append("spam_keyword")

    tutorial_hits = _match_keywords(combined_text, rules.get("tutorial_keywords", []))
    syntax_hits = _match_keywords(combined_text, rules.get("syntax_keywords", []))
    promo_hits = _match_keywords(combined_text, rules.get("promo_keywords", []))
    homework_hits = _match_keywords(combined_text, rules.get("student_homework_keywords", []))
    career_hits = _match_keywords(combined_text, rules.get("career_advice_keywords", []))

    if tutorial_hits:
        reasons.append("tutorial_content")
    if syntax_hits:
        reasons.append("syntax_only_question")
    if promo_hits:
        reasons.append("promotional_content")
    if homework_hits:
        reasons.append("student_homework")
    if career_hits:
        reasons.append("career_advice")

    business_hits = _match_keywords(combined_text, _keywords_for_row(rules, row, "business_signal_keywords"))
    pain_hits = _match_keywords(combined_text, _keywords_for_row(rules, row, "pain_signal_keywords"))
    mode_name = str(rules.get("_active_mode", "") or "")
    mode_profiles = rules.get("mode_profiles", {}) or {}
    mode = mode_profiles.get(mode_name, {}) if isinstance(mode_profiles, dict) else {}
    require_business_signal = bool(mode.get("require_business_signal", True))
    require_pain_signal = bool(mode.get("require_pain_signal", True))
    require_any_signal = bool(mode.get("require_any_signal", False))

    if require_business_signal and not business_hits:
        reasons.append("missing_business_signal")
    if require_pain_signal and not pain_hits:
        reasons.append("missing_pain_signal")
    if require_any_signal and not business_hits and not pain_hits:
        reasons.append("missing_any_signal")

    return FilterEvaluation(
        invalid_reason="|".join(dict.fromkeys(reasons)),
        business_signal_score=len(set(business_hits)),
        pain_signal_score=len(set(pain_hits)),
        business_signal_terms="|".join(dict.fromkeys(business_hits)),
        pain_signal_terms="|".join(dict.fromkeys(pain_hits)),
    )


def apply_invalid_filter(df: pd.DataFrame, rules: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split normalized posts into valid and invalid dataframes."""
    if df.empty:
        signal_columns = [
            "business_signal_score",
            "pain_signal_score",
            "business_signal_terms",
            "pain_signal_terms",
        ]
        valid_columns = list(df.columns) + signal_columns
        invalid_columns = list(df.columns) + signal_columns + ["invalid_reason"]
        return pd.DataFrame(columns=valid_columns), pd.DataFrame(columns=invalid_columns)

    result = df.copy()
    evaluations = [_evaluate_row(row, rules) for _, row in result.iterrows()]
    result["invalid_reason"] = [item.invalid_reason for item in evaluations]
    result["business_signal_score"] = [item.business_signal_score for item in evaluations]
    result["pain_signal_score"] = [item.pain_signal_score for item in evaluations]
    result["business_signal_terms"] = [item.business_signal_terms for item in evaluations]
    result["pain_signal_terms"] = [item.pain_signal_terms for item in evaluations]

    invalid_df = result[result["invalid_reason"] != ""].copy()
    valid_df = result[result["invalid_reason"] == ""].drop(columns=["invalid_reason"]).copy()
    return valid_df.reset_index(drop=True), invalid_df.reset_index(drop=True)


def activate_rule_mode(rules: dict[str, Any], mode: str | None = None) -> dict[str, Any]:
    """Attach the chosen filter mode so row evaluation can stay stateless."""
    updated = dict(rules)
    selected_mode = str(mode or rules.get("default_mode", "analysis") or "analysis")
    updated["_active_mode"] = selected_mode
    return updated
