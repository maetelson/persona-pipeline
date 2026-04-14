"""Write source-specific funnel diagnostics for merchant_center_community and reddit."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.source_funnel_diagnostics import build_source_funnel_diagnostics
from src.utils.logging import get_logger

LOGGER = get_logger("run.diagnose_source_funnels")


def main() -> None:
    """Build source-specific funnel diagnostics from current outputs."""
    outputs = build_source_funnel_diagnostics(ROOT)
    LOGGER.info("Source funnel diagnostics: %s", ", ".join(str(path) for path in outputs.values()))


if __name__ == "__main__":
    main()
