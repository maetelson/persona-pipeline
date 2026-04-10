"""Cluster-level profiling from labeled episodes and code clusters."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
from src.analysis.example_selection import select_cluster_representative_texts
from src.utils.io import load_yaml
from src.utils.pipeline_schema import LABEL_CODE_COLUMNS, split_pipe_codes, unique_record_count
from src.utils.record_access import get_record_codes


LABEL_COLUMNS = LABEL_CODE_COLUMNS


def build_cluster_profiles(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    clusters_df: pd.DataFrame,
    cluster_summary_rows: list[dict[str, Any]],
    priority_df: pd.DataFrame | None = None,
    max_representative_texts: int = 8,
) -> list[dict[str, Any]]:
    """Aggregate cluster-level demographics, needs, and representative texts."""
    if episodes_df.empty or labeled_df.empty or clusters_df.empty:
        return []

    merged = episodes_df.merge(labeled_df, on="episode_id", how="inner")
    if priority_df is not None and not priority_df.empty:
        merged = merged.merge(priority_df[["episode_id", "priority_score"]], on="episode_id", how="left")
    else:
        merged["priority_score"] = 0.0

    code_to_cluster = {
        str(row["code"]): str(row["cluster_id"])
        for _, row in clusters_df.iterrows()
    }
    cluster_summary_lookup = {
        str(row["cluster_id"]): dict(row)
        for row in cluster_summary_rows
    }
    cluster_code_lookup: dict[str, set[str]] = {}
    for _, row in clusters_df.iterrows():
        cluster_code_lookup.setdefault(str(row["cluster_id"]), set()).add(str(row["code"]))

    exploded_rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        row_codes = {code for codes in get_record_codes(row, columns=LABEL_COLUMNS).values() for code in codes}
        cluster_ids = sorted({code_to_cluster[code] for code in row_codes if code in code_to_cluster})
        for cluster_id in cluster_ids:
            cluster_codes = cluster_code_lookup.get(cluster_id, set())
            overlap_codes = sorted(row_codes & cluster_codes)
            exploded_rows.append(
                {
                    **row.to_dict(),
                    "cluster_id": cluster_id,
                    "cluster_overlap_codes": overlap_codes,
                    "cluster_overlap_count": len(overlap_codes),
                }
            )

    if not exploded_rows:
        return []

    cluster_df = pd.DataFrame(exploded_rows)
    example_config = load_yaml(Path(__file__).resolve().parents[2] / "config" / "example_selection.yaml")
    profiles: list[dict[str, Any]] = []
    total_rows = max(unique_record_count(cluster_df), 1)
    for cluster_id, cluster_rows in cluster_df.groupby("cluster_id", dropna=False):
        summary = cluster_summary_lookup.get(str(cluster_id), {})
        cluster_codes = cluster_code_lookup.get(str(cluster_id), set())
        top_demographics = _top_codes(cluster_rows, "role_codes", limit=5)
        top_need_codes = _top_need_codes(cluster_rows, cluster_codes=cluster_codes, limit=8)
        representative_texts = _representative_texts(cluster_rows, config=example_config, max_items=max_representative_texts)
        profiles.append(
            {
                "cluster_id": str(cluster_id),
                "size": unique_record_count(cluster_rows),
                "share_of_total": round(unique_record_count(cluster_rows) / total_rows, 6),
                "top_demographics": top_demographics,
                "top_need_codes": top_need_codes,
                "top_outputs": _top_codes(cluster_rows, "output_codes", limit=5, allowed_codes=cluster_codes),
                "top_envs": _top_codes(cluster_rows, "env_codes", limit=5, allowed_codes=cluster_codes),
                "representative_texts": representative_texts,
                "top_codes": summary.get("top_codes", []),
                "edge_density": summary.get("edge_density", 0.0),
                "cluster_size": summary.get("size", 0),
            }
        )
    return sorted(profiles, key=lambda row: (-int(row["size"]), str(row["cluster_id"])))


def _top_need_codes(cluster_rows: pd.DataFrame, cluster_codes: set[str], limit: int) -> list[str]:
    """Return the dominant need-style codes for a cluster."""
    counts: Counter[str] = Counter()
    for family in ["question_codes", "pain_codes", "output_codes", "workaround_codes"]:
        for value in cluster_rows.get(family, pd.Series(dtype=str)):
            counts.update(code for code in split_pipe_codes(value) if code in cluster_codes)
    return [code for code, _ in counts.most_common(limit)]


def _top_codes(cluster_rows: pd.DataFrame, column: str, limit: int, allowed_codes: set[str] | None = None) -> list[str]:
    """Return the most common codes for one family."""
    counts: Counter[str] = Counter()
    for value in cluster_rows.get(column, pd.Series(dtype=str)):
        codes = split_pipe_codes(value)
        if allowed_codes is not None:
            codes = [code for code in codes if code in allowed_codes]
        counts.update(codes)
    return [code for code, _ in counts.most_common(limit)]


def _representative_texts(cluster_rows: pd.DataFrame, config: dict[str, Any], max_items: int) -> list[str]:
    """Pick representative episode texts grounded in the source rows."""
    ranked = cluster_rows.sort_values(
        ["cluster_overlap_count", "priority_score", "label_confidence", "episode_id"],
        ascending=[False, False, False, True],
    )
    return select_cluster_representative_texts(ranked, config=config, max_items=max_items)
