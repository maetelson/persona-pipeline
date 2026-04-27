"""Validate schema contracts and run fast schema-focused tests."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.pipeline_schema import (
    CORE_LABEL_COLUMNS,
    LABEL_CODE_COLUMNS,
    PIPELINE_STAGE_DEFINITIONS,
    PIPELINE_STAGE_METRIC_NAMES,
    WORKBOOK_COLUMN_ORDERS,
    WORKBOOK_RATIO_COLUMNS,
    WORKBOOK_SHEET_NAMES,
)


def main() -> None:
    """Validate static schema invariants and run schema regression tests."""
    workbook_sheets = set(WORKBOOK_SHEET_NAMES)
    ordered_sheets = set(WORKBOOK_COLUMN_ORDERS)
    if workbook_sheets != ordered_sheets:
        missing = sorted(workbook_sheets - ordered_sheets)
        extra = sorted(ordered_sheets - workbook_sheets)
        raise RuntimeError(
            "Workbook sheet contract mismatch. "
            f"Missing orders: {missing or 'none'}; extra orders: {extra or 'none'}"
        )

    for sheet_name, ratio_columns in WORKBOOK_RATIO_COLUMNS.items():
        order = set(WORKBOOK_COLUMN_ORDERS.get(sheet_name, []))
        missing_columns = sorted(set(ratio_columns) - order)
        if missing_columns:
            raise RuntimeError(f"Ratio columns missing from workbook order for {sheet_name}: {missing_columns}")

    if not set(CORE_LABEL_COLUMNS).issubset(set(LABEL_CODE_COLUMNS)):
        raise RuntimeError("CORE_LABEL_COLUMNS must be a subset of LABEL_CODE_COLUMNS.")

    if tuple(PIPELINE_STAGE_DEFINITIONS.keys()) != PIPELINE_STAGE_METRIC_NAMES:
        raise RuntimeError("PIPELINE_STAGE_METRIC_NAMES must match PIPELINE_STAGE_DEFINITIONS keys.")

    command = [
        sys.executable,
        "-m",
        "unittest",
        "tests.test_pipeline_schema",
        "tests.test_workbook_export",
    ]
    completed = subprocess.run(command, cwd=ROOT, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)

    print(f"Validated workbook sheets: {len(WORKBOOK_SHEET_NAMES)}")
    print(f"Validated label columns: {len(LABEL_CODE_COLUMNS)}")
    print(f"Validated pipeline stage metrics: {len(PIPELINE_STAGE_METRIC_NAMES)}")


if __name__ == "__main__":
    main()
