"""Run the bounded GitHub Discussions expansion pilot."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.github_discussions_expansion_pilot import run_github_discussions_expansion_pilot


def main() -> None:
    """Run the pilot and print artifact paths."""
    outputs = run_github_discussions_expansion_pilot(ROOT)
    for name, path in outputs.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
