"""Generate Phase 1 diagnostics-only deck-ready denominator eligibility artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.deck_ready_denominator_eligibility import (  # noqa: E402
    build_deck_ready_denominator_eligibility_outputs,
    write_deck_ready_denominator_eligibility_artifacts,
)
from src.utils.io import read_parquet  # noqa: E402


def main() -> None:
    """Build diagnostics-only denominator eligibility artifacts from current labeled rows."""
    labeled_df = read_parquet(ROOT_DIR / "data" / "labeled" / "labeled_episodes.parquet")
    episodes_df = read_parquet(ROOT_DIR / "data" / "episodes" / "episode_table.parquet")
    persona_assignments_df = read_parquet(ROOT_DIR / "data" / "analysis" / "persona_assignments.parquet")
    source_balance_audit_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_balance_audit.csv")
    overview_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
    overview_metrics = dict(zip(overview_df["metric"].astype(str), overview_df["value"]))
    current_coverage = float(overview_metrics.get("persona_core_coverage_of_all_labeled_pct", 0.0) or 0.0)

    outputs = build_deck_ready_denominator_eligibility_outputs(
        labeled_df=labeled_df,
        episodes_df=episodes_df,
        persona_assignments_df=persona_assignments_df,
        source_balance_audit_df=source_balance_audit_df,
        current_persona_core_coverage_pct=current_coverage,
    )
    artifact_paths = write_deck_ready_denominator_eligibility_artifacts(
        ROOT_DIR,
        outputs["rows_df"],
        outputs["summary"],
    )
    print(json.dumps({key: str(value) for key, value in artifact_paths.items()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
