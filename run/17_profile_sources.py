"""Profile source-stage timing from collection through labelability handoff."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.source_stage_profiler import profile_sources
from src.utils.logging import get_logger
from src.utils.run_helpers import load_dotenv

LOGGER = get_logger("run.profile_sources")


def build_parser() -> argparse.ArgumentParser:
    """Build the profiling CLI parser."""
    parser = argparse.ArgumentParser(description="Profile source stages for collection-to-labelability timing.")
    parser.add_argument(
        "--sources",
        nargs="+",
        default=["reddit", "stackoverflow", "github_discussions"],
        choices=["reddit", "stackoverflow", "github_discussions"],
        help="Sources to profile in sequence.",
    )
    return parser


def main() -> None:
    """Run source-stage profiling and write analysis artifacts."""
    load_dotenv(ROOT / ".env")
    args = build_parser().parse_args()
    outputs = profile_sources(ROOT, args.sources)
    LOGGER.info("Wrote profiling artifacts: %s", ", ".join(str(path) for path in outputs.values()))


if __name__ == "__main__":
    main()