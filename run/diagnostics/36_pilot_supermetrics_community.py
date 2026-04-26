"""Run a bounded public-HTML pilot for Supermetrics Community."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.supermetrics_source_pilot import run_supermetrics_pilot


def main() -> None:
    """Run the Supermetrics public HTML pilot and print artifact paths."""
    outputs = run_supermetrics_pilot(ROOT)
    for name, path in outputs.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
