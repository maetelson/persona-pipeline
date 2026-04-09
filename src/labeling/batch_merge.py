"""Merge prepared batch results back into labeled episode rows."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def merge_batch_results(labeled_df: pd.DataFrame, batch_result_path: Path) -> pd.DataFrame:
    """Apply a JSONL batch result file to unresolved label columns only.

    Expected line format:
    {"episode_id":"...", "suggestion": {...}}
    """
    if labeled_df.empty or not batch_result_path.exists():
        return labeled_df.copy()

    result = labeled_df.copy()
    suggestions: dict[str, dict[str, object]] = {}
    for line in batch_result_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        suggestions[str(payload.get("episode_id", ""))] = dict(payload.get("suggestion", {}) or {})

    for index, row in result.iterrows():
        episode_id = str(row.get("episode_id", ""))
        suggestion = suggestions.get(episode_id, {})
        for column in [
            "role_codes",
            "moment_codes",
            "question_codes",
            "pain_codes",
            "env_codes",
            "workaround_codes",
            "output_codes",
            "fit_code",
        ]:
            proposed = str(suggestion.get(column, "") or "")
            if proposed and proposed != "unknown" and str(result.at[index, column]) == "unknown":
                result.at[index, column] = proposed
    return result
