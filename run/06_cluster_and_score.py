"""Run exploratory clustering, persona generation, and scoring."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.stage_service import run_analysis_stage
from src.utils.logging import get_logger

LOGGER = get_logger("run.cluster_and_score")


def main() -> None:
    """Generate exploratory analysis artifacts from labeled episodes."""
    write_debug_artifacts = os.getenv("WRITE_ANALYSIS_DEBUG_ARTIFACTS", "true").strip().lower() == "true"
    outputs = run_analysis_stage(ROOT, write_debug_artifacts=write_debug_artifacts)
    persisted = outputs["persisted"]
    cluster_meta = persisted["cluster_meta"]
    LOGGER.info(
        "Wrote analysis artifacts (clusters=%s, personas=%s, cluster_allowed=%s, exploratory_only=%s, cluster_reliability=%s, code_clusters=%s, quality_flag=%s, persona_axes=%s, workbook_bundle=%s, debug_artifacts=%s)",
        len(outputs["deterministic_outputs"]["cluster_summary_df"]),
        persisted["generated_persona_count"],
        cluster_meta["cluster_allowed"],
        cluster_meta["exploratory_only"],
        cluster_meta["cluster_reliability"],
        persisted["code_cluster_count"],
        persisted["quality_flag"],
        persisted["service_axis_count"],
        persisted["bundle_paths"]["manifest"],
        write_debug_artifacts,
    )
    LOGGER.info("Canonical workbook bundle: %s", ", ".join(str(path) for path in persisted["bundle_paths"].values()))
    if write_debug_artifacts:
        LOGGER.info("Persona report exports: %s", ", ".join(str(path) for path in persisted["export_paths"].values()))
        LOGGER.info("Persona debug outputs: %s", ", ".join(str(path) for path in persisted["debug_paths"].values()))
        LOGGER.info("Persona messaging outputs: %s", ", ".join(str(path) for path in persisted["messaging_paths"].values()))
    LOGGER.info("Persona axis discovery outputs: %s", ", ".join(str(path) for path in persisted["axis_paths"].values()))
    LOGGER.info("Axis reduction outputs: %s", ", ".join(str(path) for path in persisted["reduction_paths"].values()))
    if not cluster_meta["cluster_allowed"]:
        LOGGER.warning("Strict cluster gate skipped cluster/persona generation: %s", cluster_meta["reason"])


if __name__ == "__main__":
    main()
