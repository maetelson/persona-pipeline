"""Exploratory cluster summaries for labeled episodes."""

from __future__ import annotations

import hashlib

import pandas as pd

from src.utils.pipeline_schema import LABEL_CODE_COLUMNS


def build_cluster_summary(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Build exploratory cluster summaries from label signatures.

    This is intentionally exploratory, not a production clustering model.
    It groups episodes by recurring label signatures so a human can inspect themes.
    """
    columns = [
        "cluster_id",
        "cluster_key",
        "episode_count",
        "role_codes",
        "moment_codes",
        "question_codes",
        "pain_codes",
        "env_codes",
        "output_codes",
        "fit_code",
        "cluster_label",
        "cluster_note",
    ]
    if labeled_df.empty:
        return pd.DataFrame(columns=columns)

    signature_columns = [column for column in LABEL_CODE_COLUMNS if column in labeled_df.columns]
    if not signature_columns:
        return pd.DataFrame(columns=columns)
    grouped = (
        labeled_df.groupby(signature_columns, dropna=False)
        .size()
        .reset_index(name="episode_count")
        .sort_values("episode_count", ascending=False)
        .reset_index(drop=True)
    )
    grouped["cluster_key"] = grouped.apply(_cluster_key, axis=1)
    grouped["cluster_id"] = grouped["cluster_key"].map(lambda key: f"cluster_{key[:8]}")
    grouped["cluster_label"] = grouped.apply(_cluster_label, axis=1)
    grouped["cluster_note"] = "exploratory signature cluster; inspect manually before using for decisions"
    return grouped[columns]


def _cluster_key(row: pd.Series) -> str:
    """Create a stable hash key from cluster dimensions."""
    raw = "||".join(
        str(row.get(column, ""))
        for column in LABEL_CODE_COLUMNS
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cluster_label(row: pd.Series) -> str:
    """Generate a readable exploratory label for a cluster."""
    parts = [
        str(row.get("role_codes", "unknown")),
        str(row.get("pain_codes", "unknown")),
        str(row.get("env_codes", "unknown")),
    ]
    return " / ".join(part for part in parts if part and part != "unknown") or "unknown cluster"
