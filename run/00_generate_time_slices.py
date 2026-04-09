"""Generate reviewable time slices for each enabled source collector."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.utils.dates import build_time_slices
from src.utils.io import ensure_dir, load_yaml, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.generate_time_slices")


def main() -> None:
    """Generate source-level time slices from shared config."""
    window_config = load_yaml(ROOT / "config" / "time_window.yaml")
    source_config_dir = ROOT / "config" / "sources"

    rows: list[dict[str, str]] = []
    for config_path in sorted(source_config_dir.glob("*.yaml")):
        source_config = load_yaml(config_path)
        source_name = str(source_config.get("source", config_path.stem))
        if not source_config.get("enabled", True):
            continue
        for row in build_time_slices(window_config, source_name=source_name):
            rows.append(
                {
                    "source": source_name,
                    "window_id": row["window_id"],
                    "window_start": row["window_start"],
                    "window_end": row["window_end"],
                    "strategy": str(
                        ((window_config.get("source_overrides", {}) or {}).get(source_name, {}) or {}).get(
                            "strategy", window_config.get("default_strategy", "explicit")
                        )
                    ),
                }
            )

    df = pd.DataFrame(rows, columns=["source", "window_id", "window_start", "window_end", "strategy"])
    output_path = ROOT / "data" / "analysis" / "time_slices.parquet"
    ensure_dir(output_path.parent)
    write_parquet(df, output_path)
    LOGGER.info("Wrote time slice plan -> %s", output_path)
    LOGGER.info("Generated %s time slices across %s enabled sources", len(df), df['source'].nunique() if not df.empty else 0)


if __name__ == "__main__":
    main()
