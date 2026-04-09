"""Run a fast source-coverage collection pass with limited queries and pages."""

from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.logging import get_logger

LOGGER = get_logger("run.collect_fast")


def main() -> None:
    """Collect a small subset to verify non-Reddit source coverage quickly."""
    defaults = {
        "COLLECT_SOURCE_FILTER": "stackoverflow,github_discussions",
        "COLLECT_MAX_QUERIES_PER_SOURCE": "3",
        "COLLECT_MAX_PAGES_PER_QUERY": "1",
        "GITHUB_ENABLE_DISCUSSIONS": "false",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)

    LOGGER.info(
        "Fast collect mode with sources=%s, max_queries=%s, max_pages=%s, github_discussions_enabled=%s",
        os.environ["COLLECT_SOURCE_FILTER"],
        os.environ["COLLECT_MAX_QUERIES_PER_SOURCE"],
        os.environ["COLLECT_MAX_PAGES_PER_QUERY"],
        os.environ["GITHUB_ENABLE_DISCUSSIONS"],
    )
    runpy.run_path(str(ROOT / "run" / "01_collect_all.py"), run_name="__main__")


if __name__ == "__main__":
    main()
