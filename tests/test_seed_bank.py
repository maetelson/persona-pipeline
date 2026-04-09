"""Tests for compact source-specific seed bank helpers."""

from __future__ import annotations

from pathlib import Path
import unittest

from src.utils.seed_bank import load_seed_bank, render_optional_queries, validate_seed_bank

ROOT = Path(__file__).resolve().parents[1]


class SeedBankTests(unittest.TestCase):
    """Verify compact seed banks load and validate cleanly."""

    def test_load_g2_seed_bank(self) -> None:
        seed_bank = load_seed_bank(ROOT, "review_sites", "g2")
        self.assertIsNotNone(seed_bank)
        assert seed_bank is not None
        self.assertEqual(len(seed_bank.core_seeds), 10)
        self.assertEqual(seed_bank.max_query_count, 10)
        self.assertIn("pricing", seed_bank.all_negative_terms)

    def test_optional_templates_render_product_queries(self) -> None:
        seed_bank = load_seed_bank(ROOT, "official_communities", "power_bi_community")
        assert seed_bank is not None
        rendered = render_optional_queries(seed_bank)
        self.assertIn("Power BI export to excel", rendered)
        self.assertIn("Power BI numbers don't match", rendered)

    def test_seed_banks_validate_without_errors(self) -> None:
        targets = [
            ("review_sites", "g2"),
            ("reddit", "reddit_r_excel"),
            ("official_communities", "power_bi_community"),
        ]
        for source_group, source_id in targets:
            seed_bank = load_seed_bank(ROOT, source_group, source_id)
            assert seed_bank is not None
            findings = validate_seed_bank(seed_bank)
            self.assertFalse([item for item in findings if item["level"] == "error"], msg=f"{source_id}: {findings}")


if __name__ == "__main__":
    unittest.main()
