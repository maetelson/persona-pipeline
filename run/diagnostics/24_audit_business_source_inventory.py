"""Build business-community inventory diagnostics from current collection artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.business_source_inventory import build_business_source_inventory_audit
from src.utils.logging import get_logger

LOGGER = get_logger("run.audit_business_source_inventory")
DEFAULT_SOURCES = ["shopify_community", "klaviyo_community"]


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for business-source inventory diagnostics."""
    parser = argparse.ArgumentParser(description="Build discovery inventory diagnostics for selected business-community sources.")
    parser.add_argument(
        "sources",
        nargs="*",
        default=DEFAULT_SOURCES,
        help="Source ids to audit. Defaults to shopify_community and klaviyo_community.",
    )
    parser.add_argument(
        "--output-dir-name",
        default="business_source_inventory_audit",
        help="Folder name under data/analysis/ where the diagnostic artifacts will be saved.",
    )
    return parser.parse_args()


def main() -> None:
    """Build and log business-community inventory diagnostics."""
    args = _parse_args()
    outputs = build_business_source_inventory_audit(
        root_dir=ROOT,
        sources=list(args.sources),
        output_dir_name=str(args.output_dir_name),
    )
    LOGGER.info("Audited business-community sources: %s", ", ".join(args.sources))
    LOGGER.info("Business-community inventory diagnostics: %s", ", ".join(str(path) for path in outputs.values()))


if __name__ == "__main__":
    main()
