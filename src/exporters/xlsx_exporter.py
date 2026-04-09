"""Final workbook exporter for service-fit persona outputs."""

from __future__ import annotations

import json
import re
import warnings
from pathlib import Path

import pandas as pd

from src.analysis.workbook_bundle import validate_workbook_frames
from src.utils.io import ensure_dir, load_yaml
from src.utils.pipeline_schema import WORKBOOK_SHEET_NAMES, round_frame_ratios


ILLEGAL_EXCEL_CHAR_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")


def export_workbook(
    root_dir: Path,
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
) -> Path:
    """Write the final persona workbook from deterministic report tables."""
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
    return export_workbook_from_frames(
        root_dir=root_dir,
        frames=dict(zip(WORKBOOK_SHEET_NAMES, frames, strict=True)),
    )


def export_workbook_from_frames(root_dir: Path, frames: dict[str, pd.DataFrame]) -> Path:
    """Write the final workbook from the canonical workbook-frame mapping."""
    export_config = load_yaml(root_dir / "config" / "export_schema.yaml")
    workbook_name = export_config.get("workbook_name", "persona_pipeline_output.xlsx")
    output_path = ensure_dir(root_dir / "data" / "output") / workbook_name
    validated_frames = _prepare_workbook_frames(frames)
    sheet_specs = [(sheet_name, validated_frames.get(sheet_name, pd.DataFrame())) for sheet_name in WORKBOOK_SHEET_NAMES]

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, frame in sheet_specs:
            _prepare_for_excel(frame).to_excel(writer, sheet_name=sheet_name, index=False)

    _verify_workbook_sheets(output_path)

    return output_path


def _prepare_workbook_frames(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Validate and normalize workbook frames before export."""
    normalized = {sheet_name: round_frame_ratios(sheet_name, frames.get(sheet_name, pd.DataFrame())) for sheet_name in WORKBOOK_SHEET_NAMES}
    messages = validate_workbook_frames(normalized)
    for message in messages:
        warnings.warn(message, RuntimeWarning, stacklevel=2)
    if any(message.startswith("missing required sheet frame:") or message.startswith("sheet frame is null:") for message in messages):
        missing = [message for message in messages if message.startswith("missing required sheet frame:") or message.startswith("sheet frame is null:")]
        raise ValueError("Workbook export validation failed: " + "; ".join(missing))
    return normalized


def _prepare_for_excel(df: pd.DataFrame, max_len: int = 32000) -> pd.DataFrame:
    """Trim long string cells for Excel export while keeping source artifacts untouched."""
    if df.empty:
        return df.copy()

    export_df = df.copy()
    for column in export_df.columns:
        export_df[column] = export_df[column].map(lambda value: _excel_safe_value(value, max_len=max_len))
    return export_df


def _excel_safe_value(value: object, max_len: int) -> object:
    """Convert nested values to strings and enforce Excel cell length limits."""
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, str):
        value = ILLEGAL_EXCEL_CHAR_RE.sub("", value)
    if isinstance(value, str) and len(value) > max_len:
        return value[:max_len] + "...[truncated]"
    return value


def _verify_workbook_sheets(path: Path) -> None:
    """Verify the written workbook contains the required sheets."""
    from openpyxl import load_workbook

    workbook = load_workbook(path, read_only=True)
    try:
        sheet_names = list(workbook.sheetnames)
    finally:
        workbook.close()
    missing = [sheet_name for sheet_name in WORKBOOK_SHEET_NAMES if sheet_name not in sheet_names]
    if missing:
        raise ValueError(f"Workbook missing required sheets: {', '.join(missing)}")
