"""Build the diagnostics-only review-ready gap analysis artifacts."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.review_ready_gap_analysis import (
    ROOT_GAP_ANALYSIS_ARTIFACT,
    ROOT_GAP_PLAN_DOC,
    ROOT_SOURCE_DECISION_ARTIFACT,
    build_review_ready_gap_analysis,
    write_review_ready_gap_artifacts,
)


def main() -> None:
    """Build and persist the readiness gap analysis artifacts."""
    report = build_review_ready_gap_analysis(ROOT_DIR)
    paths = write_review_ready_gap_artifacts(ROOT_DIR, report)
    print(f"Wrote {ROOT_GAP_ANALYSIS_ARTIFACT}")
    print(f"Wrote {ROOT_SOURCE_DECISION_ARTIFACT}")
    print(f"Wrote {ROOT_GAP_PLAN_DOC}")
    print(f"Recommended path: {report['recommended_next_implementation_path']}")
    print(
        "Reviewable achievable without weakening persona standards: "
        f"{report['reviewable_achievable_without_weakening_persona_standards']}"
    )
    for key, path in paths.items():
        print(f"{key}: {path}")


if __name__ == "__main__":
    main()
