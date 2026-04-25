"""Build the reconciliation/signoff curated evaluation set and summary artifacts."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.reconciliation_signoff_curation import (
    build_curation_artifacts,
    validate_curation_df,
    validate_curation_splits,
    write_curation_outputs,
)


def main() -> None:
    """Build curated evaluation artifacts and fail loudly when validation does not pass."""
    outputs = build_curation_artifacts(ROOT_DIR)
    errors = validate_curation_df(outputs["curation_df"])
    errors.extend(validate_curation_splits(outputs["split_frames"]))
    if errors:
        raise SystemExit("Curation validation failed:\n- " + "\n- ".join(errors))
    paths = write_curation_outputs(ROOT_DIR, outputs)
    print(paths["curation_csv"])
    print(paths["dev_csv"])
    print(paths["eval_locked_csv"])
    print(paths["summary_json"])


if __name__ == "__main__":
    main()
