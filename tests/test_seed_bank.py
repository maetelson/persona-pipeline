"""Tests for compact source-specific seed bank helpers."""

from __future__ import annotations

from pathlib import Path
import unittest

from src.utils.io import load_yaml
from src.utils.seed_bank import build_discovery_queries, load_seed_bank, validate_seed_bank

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
            ("business_communities", "shopify_community"),
            ("business_communities", "hubspot_community"),
            ("business_communities", "klaviyo_community"),
            ("business_communities", "google_ads_community"),
            ("business_communities", "google_ads_help_community"),
            ("business_communities", "merchant_center_community"),
            ("discourse", "metabase_discussions"),
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

    def test_business_community_candidate_pool_is_not_active_by_default(self) -> None:
        seed_bank = load_seed_bank(ROOT, "business_communities", "hubspot_community")
        self.assertIsNotNone(seed_bank)
        assert seed_bank is not None
        self.assertEqual(len(seed_bank.candidate_seed_pool), 0)
        self.assertEqual(len(seed_bank.core_seeds), 8)
        self.assertEqual(len(seed_bank.active_queries), 8)

    def test_build_discovery_queries_uses_source_specific_seed_bank(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "metabase_discussions.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="metabase_discussions",
            source_group="discourse",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("metabase dashboard filter", expanded)
        self.assertIn("cannot metabase dashboard filter", expanded)
        self.assertTrue(all("metabase" in query for query in expanded))

    def test_support_community_queries_keep_short_source_language(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "google_ads_community.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="google_ads_community",
            source_group="business_communities",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("google ads reporting wrong", expanded)
        self.assertIn("google ads metrics discrepancy", expanded)
        self.assertTrue(all(query.count("google ads") == 1 for query in expanded))


if __name__ == "__main__":
    unittest.main()
