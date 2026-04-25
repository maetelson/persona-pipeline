"""Build the diagnostics-only deck-ready feasibility artifacts."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.deck_ready_feasibility_analysis import (
    ROOT_DECK_READY_FEASIBILITY_ARTIFACT,
    ROOT_DECK_READY_FEASIBILITY_DOC,
    ROOT_SOURCE_BALANCE_COVERAGE_GAP_ARTIFACT,
    build_deck_ready_feasibility_analysis,
    write_deck_ready_feasibility_artifacts,
)


def main() -> None:
    """Build and persist the current deck-ready feasibility diagnostics."""
    report = build_deck_ready_feasibility_analysis(ROOT_DIR)
    paths = write_deck_ready_feasibility_artifacts(ROOT_DIR, report)
    print(f"Wrote {ROOT_DECK_READY_FEASIBILITY_ARTIFACT}")
    print(f"Wrote {ROOT_SOURCE_BALANCE_COVERAGE_GAP_ARTIFACT}")
    print(f"Wrote {ROOT_DECK_READY_FEASIBILITY_DOC}")
    print(f"Decision: {report['deck_ready_feasibility_decision']}")
    print(f"Recommended path: {report['recommended_next_path']}")
    print(f"Continue toward deck-ready: {report['continue_toward_deck_ready']}")
    for key, path in paths.items():
        print(f"{key}: {path}")


if __name__ == "__main__":
    main()
