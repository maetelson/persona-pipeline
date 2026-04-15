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
        self.assertEqual(len(seed_bank.core_seeds), 10)
        self.assertEqual(seed_bank.max_query_count, 10)
        self.assertIn("salary", seed_bank.all_negative_terms)

    def test_seed_banks_validate_without_errors(self) -> None:
        targets = [
            ("business_communities", "shopify_community"),
            ("business_communities", "hubspot_community"),
            ("business_communities", "klaviyo_community"),
            ("business_communities", "google_ads_community"),
            ("business_communities", "google_ads_help_community"),
            ("business_communities", "merchant_center_community"),
            ("business_communities", "mixpanel_community"),
            ("business_communities", "amplitude_community"),
            ("business_communities", "power_bi_community"),
            ("business_communities", "qlik_community"),
            ("business_communities", "sisense_community"),
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
        self.assertEqual(len(seed_bank.candidate_seed_pool), 5)
        self.assertEqual(len(seed_bank.core_seeds), 10)
        self.assertEqual(len(seed_bank.active_queries), 10)

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
        self.assertIn("google ads report mismatch", expanded)
        self.assertIn("google ads numbers don't match", expanded)
        self.assertIn("google ads campaign results wrong", expanded)
        self.assertTrue(all(query.count("google ads") == 1 for query in expanded))

    def test_shopify_queries_use_shopify_specific_seed_bank_only(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "shopify_community.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="shopify_community",
            source_group="business_communities",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("shopify reports not matching", expanded)
        self.assertIn("finance numbers different from shopify", expanded)
        self.assertIn("shopify what changed in store performance", expanded)
        self.assertNotIn("analytics not working", expanded)
        self.assertNotIn("wrong analytics data", expanded)
        self.assertTrue(all("shopify" in query for query in expanded))

    def test_google_ads_help_queries_use_help_specific_seed_bank(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "google_ads_help_community.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="google_ads_help_community",
            source_group="business_communities",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("google ads numbers don't match", expanded)
        self.assertIn("google ads report discrepancy", expanded)
        self.assertIn("google ads roas dropped but traffic looks normal", expanded)
        self.assertTrue(all(query.count("google ads") == 1 for query in expanded))

    def test_power_bi_queries_use_power_bi_specific_seed_bank(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "power_bi_community.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="power_bi_community",
            source_group="business_communities",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("power bi wrong totals", expanded)
        self.assertIn("power bi desktop and service show different numbers", expanded)
        self.assertIn("power bi export data doesn't match visual", expanded)
        self.assertTrue(all("power bi" in query for query in expanded))

    def test_mixpanel_queries_use_mixpanel_specific_seed_bank(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "mixpanel_community.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="mixpanel_community",
            source_group="business_communities",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("mixpanel event count doesn't match report", expanded)
        self.assertIn("mixpanel funnel numbers don't match insights", expanded)
        self.assertIn("mixpanel what should i do based on this report", expanded)

    def test_qlik_queries_use_qlik_specific_seed_bank(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "qlik_community.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="qlik_community",
            source_group="business_communities",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("wrong totals in qlik chart", expanded)
        self.assertIn("qlik set analysis result doesn't match total", expanded)
        self.assertIn("qlik dashboard and export don't match", expanded)

    def test_amplitude_queries_use_amplitude_specific_seed_bank(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "amplitude_community.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="amplitude_community",
            source_group="business_communities",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("amplitude event count doesn't match chart", expanded)
        self.assertIn("amplitude retention analysis numbers don't make sense", expanded)
        self.assertIn("amplitude what should i do based on this analysis", expanded)

    def test_sisense_queries_use_sisense_specific_seed_bank(self) -> None:
        config = load_yaml(ROOT / "config" / "sources" / "sisense_community.yaml")
        queries = build_discovery_queries(
            ROOT,
            config=config,
            source_id="sisense_community",
            source_group="business_communities",
        )
        expanded = [query.expanded_query for query in queries]
        self.assertIn("sisense dashboard numbers don't match", expanded)
        self.assertIn("wrong totals in sisense pivot table", expanded)
        self.assertIn("which sisense number should i trust", expanded)


if __name__ == "__main__":
    unittest.main()
