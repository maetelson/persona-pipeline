"""Simple deduplication helpers."""

from __future__ import annotations

import pandas as pd


def split_duplicate_posts(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split valid candidates into kept rows and duplicate-invalid rows."""
    if df.empty or "dedupe_key" not in df.columns:
        return df.copy(), pd.DataFrame(columns=list(df.columns) + ["invalid_reason"])

    ranked = df.copy()
    ranked["_text_rank"] = ranked.get("text_len", pd.Series([0] * len(ranked))).fillna(0)
    dedupe_subset = ["source", "dedupe_key"] if "source" in ranked.columns else ["dedupe_key"]
    ranked = ranked.sort_values(
        by=[*dedupe_subset, "_text_rank", "created_at"],
        ascending=[True] * len(dedupe_subset) + [False, False],
    )

    kept = ranked.drop_duplicates(subset=dedupe_subset, keep="first").drop(columns=["_text_rank"]).reset_index(drop=True)
    duplicate_mask = ranked.duplicated(subset=dedupe_subset, keep="first")
    duplicates = ranked[duplicate_mask].drop(columns=["_text_rank"]).copy().reset_index(drop=True)
    if not duplicates.empty:
        duplicates["invalid_reason"] = "duplicate_candidate"
    return kept, duplicates
