"""Validate the manually anchored reconciliation/signoff seed set artifact."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.reconciliation_signoff_anchor_set import build_anchor_summary, validate_anchor_set_df


def main() -> None:
    """Validate the anchor set CSV and print a compact summary."""
    csv_path = ROOT_DIR / "artifacts" / "curation" / "reconciliation_signoff_anchor_set.csv"
    if not csv_path.exists():
        raise SystemExit(f"Missing anchor set artifact: {csv_path}")
    anchor_df = pd.read_csv(csv_path)
    errors = validate_anchor_set_df(anchor_df)
    if errors:
        raise SystemExit("Anchor-set validation failed:\n- " + "\n- ".join(errors))
    print(json.dumps(build_anchor_summary(anchor_df), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
