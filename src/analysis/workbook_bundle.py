"""Canonical workbook bundle assembly and persistence helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir, read_parquet, write_parquet
from src.utils.pipeline_schema import WORKBOOK_RATIO_COLUMNS, WORKBOOK_SHEET_NAMES, reorder_frame_columns, round_frame_ratios


def assemble_workbook_frames(
    overview_df: pd.DataFrame,
    counts_df: pd.DataFrame,
    source_distribution_df: pd.DataFrame,
    taxonomy_summary_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_summary_df: pd.DataFrame,
    persona_axes_df: pd.DataFrame,
    persona_needs_df: pd.DataFrame,
    persona_cooccurrence_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
    quality_checks_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Build the canonical workbook-frame mapping."""
    frames = [
        overview_df,
        counts_df,
        source_distribution_df,
        taxonomy_summary_df,
        cluster_stats_df,
        persona_summary_df,
        persona_axes_df,
        persona_needs_df,
        persona_cooccurrence_df,
        persona_examples_df,
        quality_checks_df,
    ]
    return {
        sheet_name: round_frame_ratios(sheet_name, reorder_frame_columns(sheet_name, frame))
        for sheet_name, frame in zip(WORKBOOK_SHEET_NAMES, frames, strict=True)
    }


def write_workbook_bundle(root_dir: Path, frames: dict[str, pd.DataFrame]) -> dict[str, Path]:
    """Persist the canonical workbook bundle as parquet tables plus a manifest."""
    bundle_dir = ensure_dir(root_dir / "data" / "analysis" / "workbook_bundle")
    paths: dict[str, Path] = {}
    manifest: dict[str, str] = {}
    for sheet_name in WORKBOOK_SHEET_NAMES:
        frame = _normalize_bundle_frame(frames.get(sheet_name, pd.DataFrame()))
        path = bundle_dir / f"{sheet_name}.parquet"
        write_parquet(frame, path)
        paths[sheet_name] = path
        manifest[sheet_name] = str(path)
    manifest_path = bundle_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["manifest"] = manifest_path
    return paths


def read_workbook_bundle(root_dir: Path) -> dict[str, pd.DataFrame]:
    """Read the canonical workbook bundle if present."""
    bundle_dir = root_dir / "data" / "analysis" / "workbook_bundle"
    frames: dict[str, pd.DataFrame] = {}
    for sheet_name in WORKBOOK_SHEET_NAMES:
        path = bundle_dir / f"{sheet_name}.parquet"
        frames[sheet_name] = read_parquet(path)
    return frames


def workbook_bundle_exists(root_dir: Path) -> bool:
    """Return whether the canonical workbook bundle exists."""
    bundle_dir = root_dir / "data" / "analysis" / "workbook_bundle"
    return all((bundle_dir / f"{sheet_name}.parquet").exists() for sheet_name in WORKBOOK_SHEET_NAMES)


def validate_workbook_frames(frames: dict[str, pd.DataFrame]) -> list[str]:
    """Validate required workbook frames and return warning/error messages."""
    messages: list[str] = []
    for sheet_name in WORKBOOK_SHEET_NAMES:
        if sheet_name not in frames:
            messages.append(f"missing required sheet frame: {sheet_name}")
            continue
        frame = frames.get(sheet_name)
        if frame is None:
            messages.append(f"sheet frame is null: {sheet_name}")
            continue
        ratio_columns = WORKBOOK_RATIO_COLUMNS.get(sheet_name, {})
        for column, digits in ratio_columns.items():
            if column not in frame.columns:
                messages.append(f"missing optional ratio column: {sheet_name}.{column}")
                continue
            numeric = pd.to_numeric(frame[column], errors="coerce")
            if numeric.isna().any():
                messages.append(f"non-numeric ratio values: {sheet_name}.{column}")
                continue
            rounded = numeric.round(int(digits))
            if not rounded.equals(numeric):
                messages.append(f"unrounded ratio values normalized: {sheet_name}.{column}")
        if sheet_name in {"cluster_stats", "persona_summary", "persona_examples"} and frame.empty:
            messages.append(f"sparse data: empty {sheet_name} sheet")
    return messages


def _normalize_bundle_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize mixed object columns so workbook bundle parquet writes reliably."""
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    frame = df.copy()
    for column in frame.columns:
        if frame[column].dtype == "object":
            frame[column] = frame[column].map(_normalize_bundle_value).astype(str)
    return frame


def _normalize_bundle_value(value: object) -> object:
    """Convert nested or mixed workbook values into stable parquet-safe scalars."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if value is None:
        return ""
    return value
