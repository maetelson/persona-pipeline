"""Run persona-stage source rebalancing experiments and write comparison artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.rebalancing import run_rebalance_experiments
from src.utils.logging import get_logger

LOGGER = get_logger("run.rebalance_personas")


def main() -> None:
    """Run baseline plus requested rebalancing modes."""
    parser = argparse.ArgumentParser(description="Run persona-stage source rebalancing experiments.")
    parser.add_argument(
        "--mode",
        action="append",
        dest="modes",
        help="Mode to run. Repeat for multiple modes. Defaults to all enabled modes in config/rebalancing.yaml.",
    )
    args = parser.parse_args()
    outputs = run_rebalance_experiments(ROOT, modes=args.modes)
    LOGGER.info("Recommended rebalance mode: %s", outputs["recommendation"])
    LOGGER.info("Rebalance artifacts: %s", ", ".join(str(path) for path in outputs["paths"].values()))


if __name__ == "__main__":
    main()
