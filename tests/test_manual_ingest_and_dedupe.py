"""Manual ingest and dedupe tests for the new source-group path."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.collectors.review_site_collector import ReviewSiteCollector
from src.filters.dedupe import split_duplicate_posts
from src.normalizers.review_site_normalizer import ReviewSiteNormalizer

FIXTURES = Path(__file__).resolve().parent / "fixtures"
ROOT = Path(__file__).resolve().parents[1]


class ManualIngestTests(unittest.TestCase):
    """Verify manual review-site snapshots flow through collection and normalize."""

    def test_manual_ingest_review_html_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fixture_target = temp_path / "review_page.html"
            fixture_target.write_text((FIXTURES / "review_page.html").read_text(encoding="utf-8"), encoding="utf-8")
            collector = ReviewSiteCollector(
                "g2",
                config={
                    "source_group": "review_sites",
                    "source_name": "g2",
                    "manual_input_dir": str(temp_path),
                    "direct_crawl_enabled": False,
                },
                data_dir=ROOT / "data",
            )
            records = collector.collect()
            self.assertTrue(records)
            normalized = ReviewSiteNormalizer().normalize_rows([record.to_dict() for record in records])
            self.assertEqual(len(normalized), 1)
            self.assertTrue(bool(normalized.iloc[0]["manual_import_flag"]))

    def test_dedupe_splits_duplicate_hashes(self) -> None:
        frame = pd.DataFrame(
            [
                {"dedupe_key": "same", "text_len": 100, "created_at": "2024-01-01T00:00:00+00:00"},
                {"dedupe_key": "same", "text_len": 90, "created_at": "2024-01-02T00:00:00+00:00"},
            ]
        )
        kept, duplicates = split_duplicate_posts(frame)
        self.assertEqual(len(kept), 1)
        self.assertEqual(len(duplicates), 1)
        self.assertEqual(duplicates.iloc[0]["invalid_reason"], "duplicate_candidate")


if __name__ == "__main__":
    unittest.main()
