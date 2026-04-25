"""Build the diagnostics-only source representativeness audit artifacts."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.source_representativeness_audit import (
    ROOT_SOURCE_REPRESENTATIVENESS_AUDIT_ARTIFACT,
    ROOT_SOURCE_REPRESENTATIVENESS_POLICY_DOC,
    ROOT_SOURCE_TIER_RECOMMENDATION_ARTIFACT,
    build_source_representativeness_audit,
    write_source_representativeness_artifacts,
)


def main() -> None:
    """Build and persist the source representativeness audit outputs."""
    report = build_source_representativeness_audit(ROOT_DIR)
    paths = write_source_representativeness_artifacts(ROOT_DIR, report)
    print(f"Wrote {ROOT_SOURCE_REPRESENTATIVENESS_AUDIT_ARTIFACT}")
    print(f"Wrote {ROOT_SOURCE_TIER_RECOMMENDATION_ARTIFACT}")
    print(f"Wrote {ROOT_SOURCE_REPRESENTATIVENESS_POLICY_DOC}")
    print(f"Recommended path: {report['recommended_next_implementation_path']}")
    print(
        "Pursue deck-ready by source exclusion instead of source fixing: "
        f"{report['pursue_deck_ready_by_source_exclusion_instead_of_source_fixing']}"
    )
    for key, path in paths.items():
        print(f"{key}: {path}")


if __name__ == "__main__":
    main()
