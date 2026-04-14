"""Run the Reddit yield-failure analysis and write diagnostic artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.reddit_yield_analysis import analyze_reddit_yield
from src.utils.logging import get_logger

LOGGER = get_logger("run.analyze_reddit_yield")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for Reddit yield analysis."""
    parser = argparse.ArgumentParser(description="Analyze why Reddit keeps so few rows.")
    parser.add_argument(
        "--comparison-source",
        default="metabase_discussions",
        choices=["metabase_discussions", "hubspot_community", "google_ads_help_community"],
        help="A better-performing source used for contrast.",
    )
    return parser


def main() -> None:
    """Run the analysis and write CSV/markdown outputs."""
    args = build_parser().parse_args()
    outputs = analyze_reddit_yield(ROOT, comparison_source=args.comparison_source)
    LOGGER.info("Wrote Reddit yield analysis artifacts: %s", ", ".join(str(path) for path in outputs.values()))


if __name__ == "__main__":
    main()
