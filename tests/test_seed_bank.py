"""Tests for compact source-specific seed bank helpers."""

from __future__ import annotations

from pathlib import Path
import unittest

from src.utils.seed_bank import load_seed_bank, validate_seed_bank

ROOT = Path(__file__).resolve().parents[1]


class SeedBankTests(unittest.TestCase):
    """Verify compact seed banks load and validate cleanly."""

    def test_load_reddit_excel_seed_bank(self) -> None:
        seed_bank = load_seed_bank(ROOT, "reddit", "reddit_r_excel")
        self.assertIsNotNone(seed_bank)
        assert seed_bank is not None
        self.assertEqual(len(seed_bank.core_seeds), 8)
        self.assertEqual(seed_bank.max_query_count, 8)
        self.assertIn("salary", seed_bank.all_negative_terms)

    def test_seed_banks_validate_without_errors(self) -> None:
        targets = [
            ("reddit", "reddit_r_excel"),
            ("reddit", "reddit_analytics"),
            ("reddit", "reddit_business_intelligence"),
            ("reddit", "reddit_marketing_analytics"),
        ]
        for source_group, source_id in targets:
            seed_bank = load_seed_bank(ROOT, source_group, source_id)
            assert seed_bank is not None
            findings = validate_seed_bank(seed_bank)
            self.assertFalse([item for item in findings if item["level"] == "error"], msg=f"{source_id}: {findings}")


if __name__ == "__main__":
    unittest.main()
