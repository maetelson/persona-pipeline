"""Canonical workbook bundle assembly and persistence helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.utils.io import ensure_dir, read_parquet, write_parquet
from src.utils.pipeline_schema import (
    WORKBOOK_COLUMN_ORDERS,
    WORKBOOK_RATIO_COLUMNS,
    WORKBOOK_SHEET_NAMES,
    reorder_frame_columns,
    round_frame_ratios,
    share_column_for_denominator,
)


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
    source_diagnostics_df: pd.DataFrame | None = None,
    quality_failures_df: pd.DataFrame | None = None,
    metric_glossary_df: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Build the canonical workbook-frame mapping."""
    frame_by_sheet = {
        "overview": overview_df,
        "counts": counts_df,
        "source_distribution": source_distribution_df,
        "taxonomy_summary": taxonomy_summary_df,
        "cluster_stats": cluster_stats_df,
        "persona_summary": persona_summary_df,
        "persona_axes": persona_axes_df,
        "persona_needs": persona_needs_df,
        "persona_cooccurrence": persona_cooccurrence_df,
        "persona_examples": persona_examples_df,
        "quality_checks": quality_checks_df,
        "source_diagnostics": source_diagnostics_df if source_diagnostics_df is not None else pd.DataFrame(),
        "quality_failures": quality_failures_df if quality_failures_df is not None else pd.DataFrame(),
        "metric_glossary": metric_glossary_df if metric_glossary_df is not None else pd.DataFrame(),
    }
    return {
        sheet_name: round_frame_ratios(sheet_name, reorder_frame_columns(sheet_name, frame_by_sheet.get(sheet_name, pd.DataFrame())))
        for sheet_name in WORKBOOK_SHEET_NAMES
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
        expected_columns = WORKBOOK_COLUMN_ORDERS.get(sheet_name, [])
        for column in expected_columns:
            if column not in frame.columns:
                messages.append(f"missing optional column: {sheet_name}.{column}")
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
        messages.extend(_validate_share_denominator_contract(sheet_name, frame))
        messages.extend(_validate_source_diagnostics_contract(sheet_name, frame))
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


def _validate_share_denominator_contract(sheet_name: str, frame: pd.DataFrame) -> list[str]:
    """Validate that share column labels match declared denominator semantics."""
    if sheet_name not in {"cluster_stats", "persona_summary"}:
        return []
    if frame.empty or "denominator_type" not in frame.columns:
        return []
    messages: list[str] = []
    if "share_of_total" in frame.columns:
        messages.append(f"forbidden generic share column: {sheet_name}.share_of_total")
    share_columns = [column for column in frame.columns if column.startswith("share_of_")]
    if not share_columns:
        return messages
    for _, row in frame.iterrows():
        denominator_type = str(row.get("denominator_type", "") or "").strip()
        expected = share_column_for_denominator(denominator_type)
        if not expected:
            continue
        if expected not in share_columns:
            messages.append(f"share denominator mismatch: {sheet_name}.{expected} missing for denominator_type={denominator_type}")
    return messages


def _validate_source_diagnostics_contract(sheet_name: str, frame: pd.DataFrame) -> list[str]:
    """Reject legacy mixed-grain source diagnostics columns from workbook export."""
    if sheet_name != "source_diagnostics" or frame.empty:
        return []
    forbidden_columns = {
        "raw_count",
        "normalized_count",
        "valid_count",
        "prefiltered_valid_count",
        "prefilter_survival_rate",
        "episode_survival_rate",
        "labelable_count",
        "labeled_count",
        "labeling_survival_rate",
        "promoted_to_persona_count",
    }
    messages = [f"ambiguous source_diagnostics column: {sheet_name}.{column}" for column in sorted(forbidden_columns & set(frame.columns))]
    required = {"section", "grain", "metric_name", "metric_value", "metric_type", "metric_definition"}
    for column in sorted(required - set(frame.columns)):
        messages.append(f"missing source_diagnostics structure column: {sheet_name}.{column}")
    if {"grain", "metric_name"}.issubset(frame.columns):
        mixed = frame[frame["grain"].astype(str).eq("mixed_grain_bridge")]
        if not mixed.empty:
            bad = mixed[mixed["metric_name"].astype(str).str.contains("rate|share|survival", case=False, regex=True)]
            for metric_name in sorted(bad.get("metric_name", pd.Series(dtype=str)).astype(str).unique().tolist()):
                messages.append(f"mixed-grain metric mislabeled as rate: {sheet_name}.{metric_name}")
    return messages
