"""Code frequency and co-occurrence analysis for labeled episodes."""

from __future__ import annotations

from collections import Counter
from itertools import combinations
from math import log
from typing import Iterable

import pandas as pd
from src.utils.pipeline_schema import LABEL_CODE_COLUMNS, round_ratio, split_pipe_codes


CODE_COLUMNS = LABEL_CODE_COLUMNS


def build_code_frequency_table(labeled_df: pd.DataFrame, code_columns: Iterable[str] = CODE_COLUMNS) -> pd.DataFrame:
    """Build one frequency row per observed label code."""
    code_columns = list(code_columns)
    if labeled_df.empty:
        return pd.DataFrame(columns=["code", "code_family", "count", "row_ratio"])

    code_counts: Counter[tuple[str, str]] = Counter()
    total_rows = max(len(labeled_df), 1)
    for _, row in labeled_df.iterrows():
        for family in code_columns:
            for code in split_pipe_codes(row.get(family, "unknown")):
                code_counts[(code, family)] += 1

    rows = [
        {
            "code": code,
            "code_family": family,
            "count": count,
            "row_ratio": round_ratio(count / total_rows),
        }
        for (code, family), count in code_counts.items()
    ]
    return pd.DataFrame(rows).sort_values(["count", "code"], ascending=[False, True]).reset_index(drop=True)


def build_code_edges(
    labeled_df: pd.DataFrame,
    code_freq_df: pd.DataFrame,
    min_pair_count: int = 5,
    normalization: str = "count",
    code_columns: Iterable[str] = CODE_COLUMNS,
) -> pd.DataFrame:
    """Build co-occurrence edges between codes observed in the same row."""
    code_columns = list(code_columns)
    columns = ["code_a", "code_b", "count", "normalized_score", "normalization"]
    if labeled_df.empty:
        return pd.DataFrame(columns=columns)

    pair_counts: Counter[tuple[str, str]] = Counter()
    total_rows = max(len(labeled_df), 1)
    freq_lookup = {
        str(row["code"]): int(row["count"])
        for _, row in code_freq_df.iterrows()
    }

    for _, row in labeled_df.iterrows():
        codes = sorted({code for family in code_columns for code in split_pipe_codes(row.get(family, "unknown"))})
        for code_a, code_b in combinations(codes, 2):
            pair_counts[(code_a, code_b)] += 1

    edge_rows: list[dict[str, object]] = []
    for (code_a, code_b), count in pair_counts.items():
        if count < int(min_pair_count):
            continue
        normalized_score = float(count)
        if normalization == "pmi":
            prob_ab = count / total_rows
            prob_a = max(freq_lookup.get(code_a, 1), 1) / total_rows
            prob_b = max(freq_lookup.get(code_b, 1), 1) / total_rows
            normalized_score = round_ratio(log(prob_ab / (prob_a * prob_b)))
        edge_rows.append(
            {
                "code_a": code_a,
                "code_b": code_b,
                "count": int(count),
                "normalized_score": normalized_score,
                "normalization": normalization,
            }
        )

    if not edge_rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(edge_rows, columns=columns).sort_values(
        ["count", "normalized_score", "code_a", "code_b"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)
