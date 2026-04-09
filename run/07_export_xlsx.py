"""Export the final multi-sheet xlsx workbook."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.workbook_bundle import read_workbook_bundle, workbook_bundle_exists
from src.exporters.xlsx_exporter import export_workbook_from_frames
from src.utils.io import read_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.export_xlsx")


def main() -> None:
    """Export the final workbook from parquet artifacts."""
    if workbook_bundle_exists(ROOT):
        frames = read_workbook_bundle(ROOT)
        output_path = export_workbook_from_frames(root_dir=ROOT, frames=frames)
        LOGGER.info("Wrote final workbook from canonical bundle -> %s", output_path)
        return

    frames = {
        "overview": _read_csv(ROOT / "data" / "analysis" / "overview.csv"),
        "counts": _read_csv(ROOT / "data" / "analysis" / "counts.csv"),
        "source_distribution": _read_csv(ROOT / "data" / "analysis" / "source_distribution.csv"),
        "taxonomy_summary": _read_csv(ROOT / "data" / "analysis" / "taxonomy_summary.csv"),
        "cluster_stats": _read_csv(ROOT / "data" / "analysis" / "cluster_stats.csv"),
        "persona_summary": _read_csv(ROOT / "data" / "analysis" / "persona_summary.csv"),
        "persona_axes": _read_csv(ROOT / "data" / "analysis" / "persona_axes.csv"),
        "persona_needs": _read_csv(ROOT / "data" / "analysis" / "persona_pains.csv"),
        "persona_cooccurrence": _read_csv(ROOT / "data" / "analysis" / "persona_cooccurrence.csv"),
        "persona_examples": _read_csv(ROOT / "data" / "analysis" / "persona_examples.csv"),
        "quality_checks": _read_csv(ROOT / "data" / "analysis" / "quality_checks.csv"),
    }
    output_path = export_workbook_from_frames(root_dir=ROOT, frames=frames)
    LOGGER.info("Wrote final workbook -> %s", output_path)


def _read_csv(path: Path):
    """Read one CSV artifact into a dataframe when present."""
    if not path.exists():
        return read_parquet(path.with_suffix(".parquet")) if path.with_suffix(".parquet").exists() else pd.DataFrame()
    return pd.read_csv(path)


if __name__ == "__main__":
    main()
