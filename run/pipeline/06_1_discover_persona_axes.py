"""Discover grounded persona axes from labeled episodes and episode context."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.axis_reduction import apply_axis_reduction, build_axis_quality_audit, recommend_axis_reduction, write_axis_reduction_outputs
from src.analysis.persona_axes import discover_persona_axes, write_persona_axis_outputs
from src.utils.io import load_yaml, read_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.discover_persona_axes")


def main() -> None:
    """Compute candidate persona axes before persona generation."""
    episodes_df = read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet")
    labeled_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    reduction_config = load_yaml(ROOT / "config" / "axis_reduction.yaml")
    axis_candidates_df, final_axis_schema, implementation_note = discover_persona_axes(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
    )
    audit_outputs = build_axis_quality_audit(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        candidate_df=axis_candidates_df,
        current_axis_schema=final_axis_schema,
        config=reduction_config,
    )
    recommendations_df = recommend_axis_reduction(audit_outputs["audit_df"], reduction_config)
    reduced_outputs = apply_axis_reduction(
        axis_wide_df=audit_outputs["axis_wide_df"],
        axis_long_df=audit_outputs["axis_long_df"],
        audit_df=audit_outputs["audit_df"],
        recommendations_df=recommendations_df,
        candidate_df=axis_candidates_df,
        current_axis_schema=final_axis_schema,
        config=reduction_config,
    )
    paths = write_persona_axis_outputs(
        root_dir=ROOT,
        axis_candidates_df=axis_candidates_df,
        final_axis_schema=reduced_outputs["reduced_axis_schema"],
        implementation_note=implementation_note,
    )
    reduction_paths = write_axis_reduction_outputs(
        root_dir=ROOT,
        audit_df=audit_outputs["audit_df"],
        recommendations_df=recommendations_df,
        reduced_outputs=reduced_outputs,
        apply_changes=True,
    )
    LOGGER.info(
        "Discovered persona axes: candidates=%s, kept=%s, reduced_core=%s",
        len(axis_candidates_df),
        len(final_axis_schema),
        sum(1 for row in reduced_outputs["reduced_axis_schema"] if str(row.get("axis_role", "core")) == "core"),
    )
    LOGGER.info("Persona axis outputs: %s", ", ".join(str(path) for path in paths.values()))
    LOGGER.info("Axis reduction outputs: %s", ", ".join(str(path) for path in reduction_paths.values()))


if __name__ == "__main__":
    main()
