"""Tests for source-aware invalid filtering."""

from __future__ import annotations

from pathlib import Path
import unittest

import pandas as pd

from src.filters.invalid_filter import activate_rule_mode, apply_invalid_filter
from src.utils.io import load_yaml


class InvalidFilterTests(unittest.TestCase):
    """Verify source-specific signal overrides do not leak across sources."""

    ROOT = Path(__file__).resolve().parents[1]

    def setUp(self) -> None:
        rules = load_yaml(self.ROOT / "config" / "invalid_rules.yaml")
        self.rules = activate_rule_mode(rules, mode="analysis")

    def test_google_ads_overrides_rescue_relevant_rows(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "google_ads_community",
                    "title": "The conversion dosn't record",
                    "body": "Google Ads conversion tracking is set up but conversions still do not record.",
                    "comments_text": "",
                    "raw_text": "The conversion dosn't record Google Ads conversion tracking is set up but conversions still do not record.",
                    "language": "en",
                    "text_len": 110,
                },
                {
                    "source": "google_ads_community",
                    "title": "Our ads are eligible and approved, but not running",
                    "body": "Everything is approved, but the ads are still not running and not serving impressions.",
                    "comments_text": "",
                    "raw_text": "Our ads are eligible and approved, but not running. Everything is approved, but the ads are still not running and not serving impressions.",
                    "language": "en",
                    "text_len": 130,
                },
            ]
        )

        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 2)
        self.assertEqual(len(invalid_df), 0)

    def test_google_ads_overrides_do_not_leak_to_other_sources(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "hubspot_community",
                    "title": "Ads approved but not running",
                    "body": "Need help with ads approved but not running.",
                    "comments_text": "",
                    "raw_text": "Ads approved but not running. Need help with ads approved but not running.",
                    "language": "en",
                    "text_len": 80,
                }
            ]
        )

        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(invalid_df), 1)
        self.assertIn("missing_business_signal", invalid_df.iloc[0]["invalid_reason"])


if __name__ == "__main__":
    unittest.main()