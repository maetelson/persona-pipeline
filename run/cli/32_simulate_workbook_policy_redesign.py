"""Run the simulation-only workbook promotion policy redesign audit."""

from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.workbook_policy_redesign import (
    ROOT_CANDIDATE_AUDIT,
    ROOT_POLICY_ARTIFACT,
    build_workbook_policy_redesign_report,
)


def main() -> None:
    """Build the workbook policy redesign report and candidate audit artifacts."""
    report = build_workbook_policy_redesign_report(ROOT_DIR)

    policy_path = ROOT_DIR / ROOT_POLICY_ARTIFACT
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        json.dumps(
            report,
            indent=2,
            ensure_ascii=False,
            default=lambda value: sorted(value) if isinstance(value, set) else str(value),
        ),
        encoding="utf-8",
    )

    audit_path = ROOT_DIR / ROOT_CANDIDATE_AUDIT
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    # mypy-safe enough because the report is built from a concrete DataFrame conversion.
    import pandas as pd

    pd.DataFrame(report["candidate_level_audit"]).to_csv(audit_path, index=False)

    print(f"Saved workbook policy redesign report to {policy_path}")
    print(f"Saved candidate policy audit to {audit_path}")


if __name__ == "__main__":
    main()
