"""Validate before-vs-after seed banks against current pipeline artifacts."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.seed_validation import validate_seed_system


def main() -> None:
    """Run offline seed validation for the modified active source communities."""
    sources = [
        "reddit",
        "github_discussions",
        "stackoverflow",
        "shopify_community",
        "hubspot_community",
        "google_ads_help_community",
        "merchant_center_community",
        "klaviyo_community",
    ]
    outputs = validate_seed_system(ROOT, sources=sources)
    for name, path in outputs.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
