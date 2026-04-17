"""Audit and reduce persona axes from labeled episodes."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.axis_reduction import (
    apply_axis_reduction,
    build_axis_quality_audit,
    export_axis_samples,
    recommend_axis_reduction,
    write_axis_reduction_outputs,
)
from src.analysis.persona_axes import discover_persona_axes
from src.utils.io import load_yaml, read_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.axis_cli")


def main() -> None:
    """Run axis audit/recommend/apply commands without notebooks."""
    parser = _build_parser()
    args = parser.parse_args()
    handlers = {
        "audit-axes": lambda: _run_audit(apply_changes=False),
        "recommend-axis-reduction": lambda: _run_reduction(apply_changes=False),
        "dry-run-axis-reduction": lambda: _run_reduction(apply_changes=False),
        "apply-axis-reduction": lambda: _run_reduction(apply_changes=True),
        "compare-axis-quality": lambda: _run_reduction(apply_changes=False),
        "export-axis-samples": lambda: _export_axis_samples(args.axis_name, args.limit),
    }
    handler = handlers.get(args.command)
    if handler is None:
        raise ValueError(f"Unsupported command: {args.command}")
    handler()


def _build_parser() -> argparse.ArgumentParser:
    """Create the axis reduction CLI parser."""
    parser = argparse.ArgumentParser(description="Audit and reduce persona axes.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in [
        "audit-axes",
        "recommend-axis-reduction",
        "dry-run-axis-reduction",
        "apply-axis-reduction",
        "compare-axis-quality",
    ]:
        subparsers.add_parser(command)
    export_parser = subparsers.add_parser("export-axis-samples")
    export_parser.add_argument("--axis", dest="axis_name", required=True)
    export_parser.add_argument("--limit", type=int, default=30)
    return parser


def _run_audit(apply_changes: bool) -> None:
    """Write the current axis audit without changing the schema."""
    episodes_df, labeled_df, candidate_df, current_axis_schema, config = _load_inputs()
    outputs = build_axis_quality_audit(episodes_df, labeled_df, candidate_df, current_axis_schema, config)
    recommendations_df = recommend_axis_reduction(outputs["audit_df"], config)
    reduced = apply_axis_reduction(
        outputs["axis_wide_df"],
        outputs["axis_long_df"],
        outputs["audit_df"],
        recommendations_df,
        candidate_df,
        current_axis_schema,
        config,
    )
    paths = write_axis_reduction_outputs(ROOT, outputs["audit_df"], recommendations_df, reduced, apply_changes=apply_changes)
    LOGGER.info("Axis audit complete -> %s", paths["axis_quality_audit_csv"])


def _run_reduction(apply_changes: bool) -> None:
    """Write audit + recommendation + before/after comparison artifacts."""
    episodes_df, labeled_df, candidate_df, current_axis_schema, config = _load_inputs()
    outputs = build_axis_quality_audit(episodes_df, labeled_df, candidate_df, current_axis_schema, config)
    recommendations_df = recommend_axis_reduction(outputs["audit_df"], config)
    reduced = apply_axis_reduction(
        outputs["axis_wide_df"],
        outputs["axis_long_df"],
        outputs["audit_df"],
        recommendations_df,
        candidate_df,
        current_axis_schema,
        config,
    )
    paths = write_axis_reduction_outputs(ROOT, outputs["audit_df"], recommendations_df, reduced, apply_changes=apply_changes)
    LOGGER.info(
        "Axis reduction complete (apply_changes=%s) -> %s, %s, %s",
        apply_changes,
        paths["axis_recommendations_csv"],
        paths["before_after_unknown_rates_csv"],
        paths["before_after_cluster_quality_csv"],
    )


def _export_axis_samples(axis_name: str, limit: int) -> None:
    """Write review samples for one axis."""
    episodes_df, labeled_df, candidate_df, current_axis_schema, config = _load_inputs()
    outputs = build_axis_quality_audit(episodes_df, labeled_df, candidate_df, current_axis_schema, config)
    recommendations_df = recommend_axis_reduction(outputs["audit_df"], config)
    output_path = ROOT / "data" / "analysis" / "axis_samples" / f"{axis_name}.csv"
    path = export_axis_samples(episodes_df, outputs["axis_wide_df"], recommendations_df, axis_name, output_path, limit=limit)
    LOGGER.info("Axis samples exported -> %s", path)


def _load_inputs():
    """Load shared inputs for axis reduction commands."""
    episodes_df = read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet")
    labeled_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    config = load_yaml(ROOT / "config" / "axis_reduction.yaml")
    candidate_df, current_axis_schema, _ = discover_persona_axes(episodes_df=episodes_df, labeled_df=labeled_df)
    return episodes_df, labeled_df, candidate_df, current_axis_schema, config


if __name__ == "__main__":
    main()
