"""Final workbook exporter for service-fit persona outputs."""

from __future__ import annotations

import json
import re
import warnings
from pathlib import Path

import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from src.analysis.workbook_bundle import validate_workbook_frames
from src.utils.io import ensure_dir, load_yaml
from src.utils.pipeline_schema import WORKBOOK_SHEET_NAMES, round_frame_ratios


ILLEGAL_EXCEL_CHAR_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
HEADER_FILL = PatternFill(fill_type="solid", fgColor="D9EAF7")
HEADER_FONT = Font(bold=True, color="1F1F1F")
TITLE_FONT = Font(bold=True, size=12)
SUBTLE_FILL = PatternFill(fill_type="solid", fgColor="F3F6F9")
PERCENT_LITERAL_FORMAT = '0.0"%"'
RATIO_FORMAT = '0.00'
INTEGER_FORMAT = '0'
DECIMAL_FORMAT = '0.0'

DISPLAY_HEADER_OVERRIDES = {
    "share_of_core_labeled": "share_of_persona_core_labeled_pct",
    "share_of_all_labeled": "share_of_all_labeled_pct",
    "denominator_type": "denominator_type_key",
    "denominator_value": "denominator_row_count",
    "pct_of_persona": "pct_of_persona_rows",
    "grain": "row_grain",
    "metric_value": "metric_value_numeric",
    "metric_type": "metric_value_type",
    "failure_reason_top": "top_failure_reason",
    "selection_strength": "selected_example_strength",
    "grounding_strength": "example_grounding_strength",
}

PERCENT_LIKE_COLUMNS = {
    "share_of_core_labeled",
    "share_of_all_labeled",
    "share_of_labeled",
    "pct_of_persona",
}

INTEGER_LIKE_COLUMNS = {
    "count",
    "persona_size",
    "denominator_value",
    "min_cluster_size",
    "example_rank",
    "mismatch_count",
    "critical_mismatch_count",
    "matched_axis_count",
    "grounded_candidate_count",
    "weak_candidate_count",
    "selected_example_count",
    "fallback_selected_count",
}


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
    source_diagnostics_df: pd.DataFrame | None = None,
    quality_failures_df: pd.DataFrame | None = None,
    metric_glossary_df: pd.DataFrame | None = None,
) -> Path:
    """Write the final persona workbook from deterministic report tables."""
    return export_workbook_from_frames(
        root_dir=root_dir,
        frames={
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
        },
    )


def export_workbook_from_frames(root_dir: Path, frames: dict[str, pd.DataFrame]) -> Path:
    """Write the final workbook from the canonical workbook-frame mapping."""
    export_config = load_yaml(root_dir / "config" / "export_schema.yaml")
    workbook_name = export_config.get("workbook_name", "persona_pipeline_output.xlsx")
    output_path = ensure_dir(root_dir / "data" / "output") / workbook_name
    validated_frames = _prepare_workbook_frames(frames)
    sheet_specs = []
    for sheet_name in WORKBOOK_SHEET_NAMES:
        raw_frame = validated_frames.get(sheet_name, pd.DataFrame())
        export_frame = _display_frame(sheet_name, raw_frame)
        sheet_specs.append((sheet_name, raw_frame, export_frame))

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, raw_frame, export_frame in sheet_specs:
            _prepare_for_excel(export_frame).to_excel(writer, sheet_name=sheet_name, index=False)
            _format_worksheet(writer.sheets[sheet_name], sheet_name, raw_frame, export_frame)
        _write_readme_sheet(writer.book)

    _verify_workbook_sheets(output_path)

    return output_path


def _prepare_workbook_frames(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Validate and normalize workbook frames before export."""
    normalized = {sheet_name: round_frame_ratios(sheet_name, frames.get(sheet_name, pd.DataFrame())) for sheet_name in WORKBOOK_SHEET_NAMES}
    messages = validate_workbook_frames(normalized)
    for message in messages:
        warnings.warn(message, RuntimeWarning, stacklevel=2)
    if any(
        message.startswith("missing required sheet frame:")
        or message.startswith("sheet frame is null:")
        or message.startswith("forbidden generic share column:")
        or message.startswith("share denominator mismatch:")
        or message.startswith("ambiguous source_diagnostics column:")
        or message.startswith("missing source_diagnostics structure column:")
        or message.startswith("mixed-grain metric mislabeled as rate:")
        for message in messages
    ):
        missing = [message for message in messages if message.startswith("missing required sheet frame:") or message.startswith("sheet frame is null:")]
        denominator_errors = [
            message
            for message in messages
            if message.startswith("forbidden generic share column:")
            or message.startswith("share denominator mismatch:")
            or message.startswith("ambiguous source_diagnostics column:")
            or message.startswith("missing source_diagnostics structure column:")
            or message.startswith("mixed-grain metric mislabeled as rate:")
        ]
        failures = [*missing, *denominator_errors]
        raise ValueError("Workbook export validation failed: " + "; ".join(failures))
    return normalized


def _prepare_for_excel(df: pd.DataFrame, max_len: int = 32000) -> pd.DataFrame:
    """Trim long string cells for Excel export while keeping source artifacts untouched."""
    if df.empty:
        return df.copy()

    export_df = df.copy()
    for column in export_df.columns:
        export_df[column] = export_df[column].map(lambda value: _excel_safe_value(value, max_len=max_len))
    return export_df


def _display_frame(sheet_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Return an export-only display frame with clearer reviewer-facing headers."""
    if df is None:
        return pd.DataFrame()
    frame = df.copy()
    rename_map = {
        column: DISPLAY_HEADER_OVERRIDES.get(column, column)
        for column in frame.columns
    }
    return frame.rename(columns=rename_map)


def _format_worksheet(worksheet, sheet_name: str, raw_frame: pd.DataFrame, export_frame: pd.DataFrame) -> None:
    """Apply reviewer-focused formatting to one analytical sheet."""
    worksheet.freeze_panes = "A2"
    if worksheet.max_row >= 1 and worksheet.max_column >= 1:
        worksheet.auto_filter.ref = worksheet.dimensions
    for cell in worksheet[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    worksheet.sheet_view.showGridLines = True

    original_columns = list(raw_frame.columns)
    display_columns = list(export_frame.columns)
    for index, display_column in enumerate(display_columns, start=1):
        original_column = original_columns[index - 1] if index - 1 < len(original_columns) else display_column
        width = _column_width(display_column, worksheet, index)
        worksheet.column_dimensions[get_column_letter(index)].width = width
        _apply_column_format(worksheet, index, sheet_name, original_column, raw_frame)


def _column_width(display_column: str, worksheet, column_index: int) -> float:
    """Pick a practical Excel width for the column."""
    values = [len(str(display_column or ""))]
    for row_index in range(2, min(worksheet.max_row, 16) + 1):
        value = worksheet.cell(row=row_index, column=column_index).value
        values.append(len(str(value or "")))
    if any(token in str(display_column) for token in ["definition", "reason", "summary", "examples", "text", "why_"]):
        return 42
    if any(token in str(display_column) for token in ["metric", "status", "type", "grain", "source", "persona_id"]):
        return max(14, min(max(values) + 2, 28))
    return max(12, min(max(values) + 2, 22))


def _apply_column_format(worksheet, column_index: int, sheet_name: str, original_column: str, raw_frame: pd.DataFrame) -> None:
    """Apply numeric formatting based on semantic column meaning."""
    series = raw_frame.get(original_column, pd.Series(dtype=object)) if raw_frame is not None else pd.Series(dtype=object)
    is_numeric = not series.empty and pd.to_numeric(series, errors="coerce").notna().any()
    if not is_numeric:
        return
    number_format = None
    if original_column in PERCENT_LIKE_COLUMNS or str(original_column).endswith("_pct"):
        number_format = PERCENT_LITERAL_FORMAT
    elif original_column in INTEGER_LIKE_COLUMNS or str(original_column).endswith("_count") or str(original_column).endswith("_rows"):
        number_format = INTEGER_FORMAT
    elif str(original_column).endswith("_ratio") or str(original_column).endswith("_score") or str(original_column) in {"grounding_fit_score", "final_example_score", "metric_value"}:
        number_format = RATIO_FORMAT if str(original_column) in {"grounding_fit_score", "final_example_score", "metric_value"} else DECIMAL_FORMAT
    if sheet_name == "source_diagnostics" and original_column == "metric_value" and "metric_type" in raw_frame.columns:
        for row_index, metric_type in enumerate(raw_frame["metric_type"].astype(str).tolist(), start=2):
            cell = worksheet.cell(row=row_index, column=column_index)
            if metric_type == "percentage":
                cell.number_format = PERCENT_LITERAL_FORMAT
            elif metric_type == "count":
                cell.number_format = INTEGER_FORMAT
            else:
                cell.number_format = RATIO_FORMAT
        return
    if number_format is None:
        return
    for row_index in range(2, worksheet.max_row + 1):
        worksheet.cell(row=row_index, column=column_index).number_format = number_format


def _write_readme_sheet(workbook) -> None:
    """Add a compact workbook guide and formula-backed provenance summary sheet."""
    if "readme" in workbook.sheetnames:
        del workbook["readme"]
    worksheet = workbook.create_sheet("readme", 0)
    rows = [
        ["Persona Workbook Guide", "Use this sheet first when reviewing denominators, grain, and grounding status."],
        ["", ""],
        ["Traceability Summary", "Formula-backed links to the workbook overview metrics."],
        ["All Labeled Rows", '=INDEX(overview!$B:$B,MATCH("total_labeled_records",overview!$A:$A,0))'],
        ["Persona Core Rows", '=INDEX(overview!$B:$B,MATCH("persona_core_labeled_records",overview!$A:$A,0))'],
        ["Promoted Persona Rows", '=INDEX(overview!$B:$B,MATCH("persona_count",overview!$A:$A,0))'],
        ["Approx Unknown Rows", '=ROUND(INDEX(overview!$B:$B,MATCH("overall_unknown_ratio",overview!$A:$A,0))*INDEX(overview!$B:$B,MATCH("total_labeled_records",overview!$A:$A,0)),0)'],
        ["", ""],
        ["How To Read Denominators", ""],
        ["share_of_persona_core_labeled_pct", "Percentage over persona_core_labeled_rows. Use this for persona clustering coverage."],
        ["share_of_all_labeled_pct", "Percentage over all labeled_episode_rows. Use this for whole-workbook context."],
        ["row_grain", "The entity counted by the row: post, episode, mixed_grain_bridge, or other."],
        ["denominator_type_key", "The semantic denominator family. Cross-check this against metric_glossary."],
        ["", ""],
        ["Review Tips", ""],
        ["Grounding states", "See persona_summary and cluster_stats for base_promotion_status, grounding_status, and promotion_grounding_status."],
        ["Mixed-grain diagnostics", "source_diagnostics rows with row_grain=mixed_grain_bridge are ratios, not funnel percentages."],
        ["Glossary", "See metric_glossary for metric definitions and denominator semantics."],
    ]
    for row in rows:
        worksheet.append(row)
    worksheet.freeze_panes = "A3"
    worksheet.column_dimensions["A"].width = 28
    worksheet.column_dimensions["B"].width = 92
    worksheet["A1"].font = TITLE_FONT
    worksheet["A3"].font = HEADER_FONT
    worksheet["B3"].font = HEADER_FONT
    for cell in worksheet[3]:
        cell.fill = HEADER_FILL
    for row_index in [9, 14]:
        for cell in worksheet[row_index]:
            cell.font = HEADER_FONT
            cell.fill = SUBTLE_FILL
    for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row, min_col=1, max_col=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)
    worksheet["B17"].hyperlink = "#metric_glossary!A1"
    worksheet["B17"].style = "Hyperlink"


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
