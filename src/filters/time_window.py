"""Time-window filtering for normalized posts."""

from __future__ import annotations

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

    invalid_reasons: list[str] = []
    for _, row in df.iterrows():
        created_at = parse_datetime(str(row.get("created_at", "") or ""))
        if created_at is None:
            invalid_reasons.append(missing_reason if exclude_missing else "")
            continue
        if created_at < start_at or created_at > end_at:
            invalid_reasons.append(outside_reason)
            continue
        invalid_reasons.append("")

    result = df.copy()
    result["invalid_reason"] = invalid_reasons
    valid_df = result[result["invalid_reason"] == ""].drop(columns=["invalid_reason"]).reset_index(drop=True)
    invalid_df = result[result["invalid_reason"] != ""].reset_index(drop=True)
    missing_df = result[result["invalid_reason"] == missing_reason].reset_index(drop=True)
    return valid_df, invalid_df, missing_df
