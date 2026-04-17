"""Export the final multi-sheet xlsx workbook."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import os

from src.analysis.stage_service import run_final_report_stage
from src.utils.logging import get_logger

LOGGER = get_logger("run.export_xlsx")


def main() -> None:
    """Run final analytics assembly and export the workbook."""
    write_debug_artifacts = os.getenv("WRITE_ANALYSIS_DEBUG_ARTIFACTS", "true").strip().lower() == "true"
    outputs = run_final_report_stage(ROOT, write_debug_artifacts=write_debug_artifacts)
    LOGGER.info("Wrote final workbook -> %s", outputs["final_workbook_path"])


if __name__ == "__main__":
    main()
