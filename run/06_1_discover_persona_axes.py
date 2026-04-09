"""Discover grounded persona axes from labeled episodes and episode context."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.persona_axes import discover_persona_axes, write_persona_axis_outputs
from src.utils.io import read_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.discover_persona_axes")


def main() -> None:
    """Compute candidate persona axes before persona generation."""
    episodes_df = read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet")
    labeled_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    axis_candidates_df, final_axis_schema, implementation_note = discover_persona_axes(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
    )
    paths = write_persona_axis_outputs(
        root_dir=ROOT,
        axis_candidates_df=axis_candidates_df,
        final_axis_schema=final_axis_schema,
        implementation_note=implementation_note,
    )
    LOGGER.info(
        "Discovered persona axes: candidates=%s, kept=%s",
        len(axis_candidates_df),
        len(final_axis_schema),
    )
    LOGGER.info("Persona axis outputs: %s", ", ".join(str(path) for path in paths.values()))


if __name__ == "__main__":
    main()
