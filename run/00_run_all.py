"""Run the full local pipeline in sequence."""

from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.logging import get_logger
from src.utils.io import load_yaml
from src.utils.run_helpers import load_dotenv

LOGGER = get_logger("run.run_all")

STEPS = [
    "00_generate_time_slices.py",
    "01_collect_all.py",
    "01_5_expand_queries_from_raw.py",
    "02_normalize_all.py",
    "02.5_filter_time_window.py",
    "03_filter_valid.py",
    "03_5_prefilter_relevance.py",
    "04_build_episodes.py",
    "05_label_episodes.py",
    "06_1_discover_persona_axes.py",
    "07_export_xlsx.py",
]


def main() -> None:
    """Execute all pipeline steps in order and stop on first failure."""
    load_dotenv(ROOT / ".env")
    _validate_required_env()

    run_dir = ROOT / "run"
    for step in STEPS:
        step_path = run_dir / step
        LOGGER.info("Starting %s", step)
        runpy.run_path(str(step_path), run_name="__main__")
        LOGGER.info("Finished %s", step)

    LOGGER.info("Pipeline complete. Final output: %s", ROOT / "data" / "output" / "persona_pipeline_output.xlsx")

def _validate_required_env() -> None:
    """Fail early with a readable message when enabled live collectors need env vars."""
    missing: list[str] = []

    reddit_config = load_yaml(ROOT / "config" / "sources" / "reddit.yaml")
    if reddit_config.get("enabled", True) and not reddit_config.get("use_stub", False):
        if not os.getenv("REDDIT_USER_AGENT", "").strip():
            missing.append("REDDIT_USER_AGENT (required for live Reddit collection)")

    if missing:
        formatted = "\n".join(f"- {item}" for item in missing)
        raise RuntimeError(
            "Missing required environment variables for the enabled pipeline sources:\n"
            f"{formatted}\n\n"
            "Fix options:\n"
            "1. Set the variables in your shell before running\n"
            "2. Create a .env file in the repo root\n"
            "3. Or switch that source config to use_stub: true / enabled: false"
        )


if __name__ == "__main__":
    main()
