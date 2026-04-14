"""Write before/after scale metrics artifacts for the persona pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.scale_metrics import write_scale_metrics_outputs
from src.utils.logging import get_logger

LOGGER = get_logger("run.capture_scale_metrics")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(description="Write before/after scale metrics for the current pipeline state.")
    parser.add_argument(
        "--set-baseline",
        action="store_true",
        help="Replace the stored baseline snapshot with the current pipeline state before writing the report.",
    )
    return parser


def main() -> None:
    """Write scale metrics snapshot artifacts and markdown."""
    args = build_parser().parse_args()
    outputs = write_scale_metrics_outputs(ROOT, set_baseline=bool(args.set_baseline))
    LOGGER.info("Wrote scale metrics artifacts: %s", ", ".join(str(path) for path in outputs.values()))


if __name__ == "__main__":
    main()
