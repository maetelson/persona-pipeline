"""Filter normalized posts by the shared collection time window."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.filters.time_window import apply_time_window_filter
from src.utils.io import load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.filter_time_window")


def main() -> None:
    """Apply the shared time window and separate out time-invalid rows."""
    normalized_df = read_parquet(ROOT / "data" / "normalized" / "normalized_posts.parquet")
    window_config = load_yaml(ROOT / "config" / "time_window.yaml")

    filtered_df, invalid_df, missing_df = apply_time_window_filter(normalized_df, window_config)
    write_parquet(filtered_df, ROOT / "data" / "normalized" / "time_filtered_posts.parquet")
    write_parquet(invalid_df, ROOT / "data" / "normalized" / "time_window_invalid.parquet")
    write_parquet(missing_df, ROOT / "data" / "normalized" / "missing_created_at.parquet")
    LOGGER.info("Wrote time-filtered posts: %s", len(filtered_df))
    LOGGER.info("Wrote time-window invalid rows: %s", len(invalid_df))
    LOGGER.info("Wrote missing created_at rows: %s", len(missing_df))


if __name__ == "__main__":
    main()
