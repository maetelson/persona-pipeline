"""Build the manually anchored reconciliation/signoff seed set artifact."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.reconciliation_signoff_anchor_set import main


if __name__ == "__main__":
    main()
