"""Run reusable source-collapse diagnostics for one or more sources."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.source_funnel_diagnostics import TARGET_SOURCES, build_source_collapse_diagnostics
from src.utils.logging import get_logger

LOGGER = get_logger("run.diagnose_source_collapse")


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for source-collapse diagnostics."""
    parser = argparse.ArgumentParser(description="Build stage-by-stage collapse diagnostics for one or more sources.")
    parser.add_argument(
        "sources",
        nargs="*",
        default=TARGET_SOURCES,
        help="Source ids to diagnose. Defaults to merchant_center_community and reddit.",
    )
    parser.add_argument(
        "--output-dir-name",
        default="source_collapse_diagnostics",
        help="Folder name under data/analysis/ where the diagnostic artifacts will be saved.",
    )
    return parser.parse_args()


def main() -> None:
    """Build and log reusable source-collapse diagnostics."""
    args = _parse_args()
    outputs = build_source_collapse_diagnostics(
        root_dir=ROOT,
        sources=list(args.sources),
        output_dir_name=str(args.output_dir_name),
    )
    LOGGER.info("Diagnosed sources: %s", ", ".join(args.sources))
    LOGGER.info("Source collapse diagnostics: %s", ", ".join(str(path) for path in outputs.values()))


if __name__ == "__main__":
    main()
