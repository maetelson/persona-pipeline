"""Validate the reconciliation/signoff curated evaluation set artifact."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.reconciliation_signoff_curation import validate_curation_df, validate_curation_splits


def main() -> None:
    """Validate the curated CSV plus dev/eval split artifacts and print a compact summary."""
    curation_dir = ROOT_DIR / "artifacts" / "curation"
    csv_path = curation_dir / "reconciliation_signoff_eval.csv"
    dev_path = curation_dir / "reconciliation_signoff_dev.csv"
    eval_locked_path = curation_dir / "reconciliation_signoff_eval_locked.csv"
    missing_paths = [str(path) for path in [csv_path, dev_path, eval_locked_path] if not path.exists()]
    if missing_paths:
        raise SystemExit("Missing curated evaluation files:\n- " + "\n- ".join(missing_paths))

    curation_df = pd.read_csv(csv_path)
    dev_df = pd.read_csv(dev_path)
    eval_locked_df = pd.read_csv(eval_locked_path)
    errors = validate_curation_df(curation_df)
    errors.extend(validate_curation_splits({"dev": dev_df, "eval_locked": eval_locked_df}))
    if errors:
        raise SystemExit("Curation validation failed:\n- " + "\n- ".join(errors))
    summary = {
        "rows": int(len(curation_df)),
        "label_counts": curation_df["curated_label"].astype(str).value_counts().to_dict(),
        "source_top_10": curation_df["source"].astype(str).value_counts().head(10).to_dict(),
        "dev_rows": int(len(dev_df)),
        "dev_label_counts": dev_df["curated_label"].astype(str).value_counts().to_dict(),
        "eval_locked_rows": int(len(eval_locked_df)),
        "eval_locked_label_counts": eval_locked_df["curated_label"].astype(str).value_counts().to_dict(),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
