"""Write Reddit seed and subreddit retention diagnostics for collection tuning."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.reddit_retention import analyze_reddit_retention
from src.utils.logging import get_logger

LOGGER = get_logger("run.analyze_reddit_retention")


def build_parser() -> argparse.ArgumentParser:
    """Build the Reddit retention diagnostics CLI parser."""
    parser = argparse.ArgumentParser(description="Analyze Reddit seed and subreddit retention.")
    parser.add_argument("--min-raw-threshold", type=int, default=5, help="Minimum raw count used to flag low-yield seeds/subreddits.")
    return parser


def main() -> None:
    """Write Reddit retention artifacts for collection-policy tuning."""
    args = build_parser().parse_args()
    outputs = analyze_reddit_retention(ROOT, min_raw_threshold=args.min_raw_threshold)
    LOGGER.info("Wrote Reddit retention artifacts: %s", ", ".join(str(path) for path in outputs.values()))


if __name__ == "__main__":
    main()