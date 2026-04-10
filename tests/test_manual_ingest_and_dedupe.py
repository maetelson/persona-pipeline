"""Dedupe tests for source-aware candidate filtering."""

from __future__ import annotations

import unittest

import pandas as pd

from src.filters.dedupe import split_duplicate_posts


class DedupeTests(unittest.TestCase):
    """Verify source-aware duplicate splitting."""

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

    def test_dedupe_keeps_same_key_across_different_sources(self) -> None:
        frame = pd.DataFrame(
            [
                {"source": "reddit", "dedupe_key": "same", "text_len": 100, "created_at": "2024-01-01T00:00:00+00:00"},
                {"source": "stackoverflow", "dedupe_key": "same", "text_len": 90, "created_at": "2024-01-02T00:00:00+00:00"},
            ]
        )
        kept, duplicates = split_duplicate_posts(frame)
        self.assertEqual(len(kept), 2)
        self.assertEqual(len(duplicates), 0)


if __name__ == "__main__":
    unittest.main()
