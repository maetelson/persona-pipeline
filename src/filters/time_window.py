"""Time-window filtering for normalized posts."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from src.utils.dates import build_relative_time_window, parse_datetime


def apply_time_window_filter(
    df: pd.DataFrame,
    window_config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split normalized posts into in-window, all-invalid, and missing-created-at rows."""
    if df.empty:
        return (
            pd.DataFrame(columns=list(df.columns)),
            pd.DataFrame(columns=list(df.columns) + ["invalid_reason"]),
            pd.DataFrame(columns=list(df.columns) + ["invalid_reason"]),
        )

    start_at, end_at = build_relative_time_window(window_config)
    exclude_missing = bool(window_config.get("exclude_missing_created_at", True))
    missing_reason = str(window_config.get("missing_created_at_reason", "missing_created_at"))
    outside_reason = str(window_config.get("outside_time_window_reason", "outside_time_window"))
    source_rescue_rules = dict(window_config.get("source_rescue_rules", {}) or {})

    invalid_reasons: list[str] = []
    for _, row in df.iterrows():
        created_at = parse_datetime(str(row.get("created_at", "") or ""))
        if created_at is None:
            invalid_reasons.append(missing_reason if exclude_missing else "")
            continue
        if created_at < start_at or created_at > end_at:
            if _should_rescue_outside_window(row=row, source_rescue_rules=source_rescue_rules):
                invalid_reasons.append("")
                continue
            invalid_reasons.append(outside_reason)
            continue
        invalid_reasons.append("")

    result = df.copy()
    result["invalid_reason"] = invalid_reasons
    valid_df = result[result["invalid_reason"] == ""].drop(columns=["invalid_reason"]).reset_index(drop=True)
    invalid_df = result[result["invalid_reason"] != ""].reset_index(drop=True)
    missing_df = result[result["invalid_reason"] == missing_reason].reset_index(drop=True)
    return valid_df, invalid_df, missing_df


def _should_rescue_outside_window(row: pd.Series, source_rescue_rules: dict[str, Any]) -> bool:
    """Allow narrowly-scoped evergreen analyst pain to survive source-specific stale-window drops."""
    source = str(row.get("source", "") or "")
    rule = dict(source_rescue_rules.get(source, {}) or {})
    if not rule:
        return False
    text = " ".join(
        str(row.get(column, "") or "")
        for column in ["title", "body", "raw_text", "comments_text", "thread_title", "normalized_text"]
        if column in row.index
    ).lower()
    include_terms = [str(term).lower() for term in rule.get("include_any_terms", []) or [] if str(term).strip()]
    exclude_terms = [str(term).lower() for term in rule.get("exclude_any_terms", []) or [] if str(term).strip()]
    if include_terms and not any(term in text for term in include_terms):
        return False
    if exclude_terms and any(term in text for term in exclude_terms):
        return False
    year_matches = [int(match) for match in re.findall(r"\b(20\d{2})\b", text)]
    oldest_allowed_year = int(rule.get("oldest_allowed_text_year", 2021))
    if any(year < oldest_allowed_year for year in year_matches):
        return False
    stale_version_pattern = str(rule.get("stale_version_pattern", "") or "").strip()
    if stale_version_pattern and re.search(stale_version_pattern, text, flags=re.IGNORECASE):
        return False
    return True
