"""Build reviewable second-pass query candidates from first-pass collected text."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.query_expander import save_query_expansion_outputs
from src.utils.logging import get_logger

LOGGER = get_logger("run.expand_queries_from_raw")


def main() -> None:
    """Extract exploratory query-expansion candidates from first-pass raw text."""
    frequency_path, candidates_path = save_query_expansion_outputs(ROOT)
    LOGGER.info("Wrote query term frequency table -> %s", frequency_path)
    LOGGER.info("Wrote query expansion candidates table -> %s", candidates_path)


if __name__ == "__main__":
    main()
