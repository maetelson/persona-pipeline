"""Canonical pipeline-stage count builders for workbook summary sheets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.diagnostics import count_raw_jsonl_by_source
from src.utils.io import read_parquet
from src.utils.pipeline_schema import (
    DENOMINATOR_EPISODE_ROWS,
    DENOMINATOR_LABELED_EPISODE_ROWS,
    DENOMINATOR_NORMALIZED_POST_ROWS,
    DENOMINATOR_PREFILTERED_VALID_ROWS,
    DENOMINATOR_RAW_RECORD_ROWS,
    DENOMINATOR_VALID_CANDIDATE_ROWS,
    PIPELINE_STAGE_DEFINITIONS,
)


def build_pipeline_stage_counts(
    raw_audit_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    root_dir: Path | None = None,
) -> dict[str, int]:
    """Build the canonical stage-count dictionary used across workbook summary sheets."""
    raw_counts_df = count_raw_jsonl_by_source(root_dir) if root_dir is not None else pd.DataFrame()
    prefiltered_df = pd.DataFrame()
    if root_dir is not None:
        prefiltered_df = read_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet")

    raw_record_rows = int(raw_counts_df.get("raw_count", pd.Series(dtype=int)).fillna(0).sum()) if not raw_counts_df.empty else 0
    if raw_record_rows <= 0 and not raw_audit_df.empty:
        raw_record_rows = int(raw_audit_df.get("raw_record_count", pd.Series(dtype=int)).fillna(0).sum())

    return {
        DENOMINATOR_RAW_RECORD_ROWS: raw_record_rows,
        DENOMINATOR_NORMALIZED_POST_ROWS: int(len(normalized_df)),
        DENOMINATOR_VALID_CANDIDATE_ROWS: int(len(valid_df)),
        DENOMINATOR_PREFILTERED_VALID_ROWS: int(len(prefiltered_df)),
        DENOMINATOR_EPISODE_ROWS: int(len(episodes_df)),
        DENOMINATOR_LABELED_EPISODE_ROWS: int(len(labeled_df)),
    }


def build_pipeline_stage_rows(stage_counts: dict[str, Any]) -> list[dict[str, object]]:
    """Render canonical stage counts into workbook row dictionaries."""
    rows: list[dict[str, object]] = []
    for metric, payload in PIPELINE_STAGE_DEFINITIONS.items():
        count = int(stage_counts.get(metric, 0) or 0)
        rows.append(
            {
                "metric": metric,
                "count": count,
                "value": count,
                "denominator_type": metric,
                "denominator_value": count,
                "definition": str(payload.get("definition", "") or ""),
            }
        )
    return rows