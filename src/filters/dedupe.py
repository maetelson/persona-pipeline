"""Simple deduplication helpers."""

from __future__ import annotations

import re

import pandas as pd


def split_duplicate_posts(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split valid candidates into kept rows and duplicate-invalid rows."""
    if df.empty or "dedupe_key" not in df.columns:
        return df.copy(), pd.DataFrame(columns=list(df.columns) + ["invalid_reason"])

    ranked = df.copy()
    ranked["_text_rank"] = ranked.get("text_len", pd.Series([0] * len(ranked))).fillna(0)
    ranked["_effective_dedupe_key"] = ranked.apply(_effective_dedupe_key, axis=1)
    dedupe_subset = ["source", "_effective_dedupe_key"] if "source" in ranked.columns else ["_effective_dedupe_key"]
    ranked = ranked.sort_values(
        by=[*dedupe_subset, "_text_rank", "created_at"],
        ascending=[True] * len(dedupe_subset) + [False, False],
    )

    kept = ranked.drop_duplicates(subset=dedupe_subset, keep="first").drop(columns=["_text_rank", "_effective_dedupe_key"]).reset_index(drop=True)
    duplicate_mask = ranked.duplicated(subset=dedupe_subset, keep="first")
    duplicates = ranked[duplicate_mask].drop(columns=["_text_rank", "_effective_dedupe_key"]).copy().reset_index(drop=True)
    if not duplicates.empty:
        duplicates["invalid_reason"] = "duplicate_candidate"
    return kept, duplicates


def _effective_dedupe_key(row: pd.Series) -> str:
    """Use source-aware semantic dedupe when repeated workaround threads dominate one source."""
    source = str(row.get("source", "") or "")
    base_key = str(row.get("dedupe_key", "") or "")
    if source != "power_bi_community":
        return base_key
    semantic_key = _power_bi_semantic_fingerprint(row)
    return semantic_key or base_key


def _power_bi_semantic_fingerprint(row: pd.Series) -> str:
    """Collapse repeated Power BI workaround threads with nearly identical analyst pain patterns."""
    title = str(row.get("title", "") or "")
    body = str(row.get("body", "") or "")
    raw_text = str(row.get("raw_text", "") or "")
    comments_text = str(row.get("comments_text", "") or "")
    lowered = " ".join([title, body, raw_text, comments_text]).lower()
    if not any(term in lowered for term in ["power bi", "powerbi", "dax", "measure", "matrix", "visual", "export", "excel", "csv"]):
        return str(row.get("dedupe_key", "") or "")

    buckets: list[str] = []
    term_buckets = {
        "manual_reporting": ["manual", "copy paste", "spreadsheet", "excel", "csv export", "export data"],
        "refresh_publish": ["publish", "republish", "refresh", "desktop", "service", "cached visual"],
        "measure_logic": ["measure", "dax", "calculated column", "calculated field", "wrong total", "matrix total"],
        "filter_context": ["filter context", "row context", "slicer", "relationship", "inactive relationship"],
        "mismatch": ["not matching", "mismatch", "wrong numbers", "different numbers", "incorrect data"],
        "detail_vs_summary": ["drill through", "drill down", "summary different from detail", "duplicate count", "distinct count"],
    }
    for bucket, terms in term_buckets.items():
        if any(term in lowered for term in terms):
            buckets.append(bucket)
    if len(buckets) < 2:
        return str(row.get("dedupe_key", "") or "")

    cadence_bucket = "recurring" if any(term in lowered for term in ["every week", "weekly", "monthly", "before sending"]) else "adhoc"
    return f"power_bi_semantic::{cadence_bucket}::{ '|'.join(sorted(buckets)) }"
