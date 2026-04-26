"""Regression tests for Phase 1 source-tier workbook annotations."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.source_tiers import SOURCE_TIER_COLUMNS, source_tier_payload


ROOT_DIR = Path(__file__).resolve().parents[1]


class SourceTierAnnotationTests(unittest.TestCase):
    """Verify source-tier annotations are deterministic and non-destructive."""

    def test_helper_assigns_expected_named_source_tiers(self) -> None:
        self.assertEqual(source_tier_payload("power_bi_community")["source_tier"], "core_representative_source")
        self.assertEqual(source_tier_payload("metabase_discussions")["source_tier"], "core_representative_source")
        self.assertEqual(source_tier_payload("adobe_analytics_community")["source_tier"], "supporting_validation_source")
        self.assertEqual(source_tier_payload("google_developer_forums")["source_tier"], "supporting_validation_source")
        self.assertEqual(source_tier_payload("domo_community_forum")["source_tier"], "exploratory_edge_source")
        self.assertEqual(source_tier_payload("klaviyo_community")["source_tier"], "excluded_from_deck_ready_core")
        self.assertFalse(source_tier_payload("adobe_analytics_community")["deck_ready_claim_anchor_allowed"])
        self.assertFalse(source_tier_payload("google_developer_forums")["keep_in_deck_ready_core_evidence"])

    def test_tiered_sources_remain_visible_in_workbook_facing_outputs(self) -> None:
        source_balance = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_balance_audit.csv")
        source_diagnostics = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_diagnostics.csv")
        source_distribution = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_distribution.csv")

        for frame in [source_balance, source_diagnostics, source_distribution]:
            for column in SOURCE_TIER_COLUMNS:
                self.assertIn(column, frame.columns)

        expected_tiers = {
            "power_bi_community": "core_representative_source",
            "metabase_discussions": "core_representative_source",
            "adobe_analytics_community": "supporting_validation_source",
            "google_developer_forums": "supporting_validation_source",
            "domo_community_forum": "exploratory_edge_source",
            "klaviyo_community": "excluded_from_deck_ready_core",
        }
        balance_tiers = dict(zip(source_balance["source"], source_balance["source_tier"]))
        diagnostics_sources = set(source_diagnostics["source"].astype(str).tolist())
        distribution_sources = set(source_distribution["source"].astype(str).tolist())
        for source, expected_tier in expected_tiers.items():
            self.assertEqual(balance_tiers[source], expected_tier)
            self.assertIn(source, diagnostics_sources)
            self.assertIn(source, distribution_sources)

    def test_overview_tier_counts_and_readiness_metrics_remain_stable(self) -> None:
        overview = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
        metrics = dict(zip(overview["metric"].astype(str), overview["value"]))
        quality_checks = pd.read_csv(ROOT_DIR / "data" / "analysis" / "quality_checks.csv")
        quality_metrics = dict(zip(quality_checks["metric"].astype(str), quality_checks["value"]))

        self.assertEqual(int(float(metrics["core_representative_source_count"])), 2)
        self.assertEqual(int(float(metrics["supporting_validation_source_count"])), 8)
        self.assertEqual(int(float(metrics["exploratory_edge_source_count"])), 3)
        self.assertEqual(int(float(metrics["excluded_from_deck_ready_core_source_count"])), 1)

        self.assertEqual(str(metrics["persona_readiness_state"]), "reviewable_but_not_deck_ready")
        self.assertEqual(str(metrics["overall_status"]), "WARN")
        self.assertEqual(str(metrics["quality_flag"]), "EXPLORATORY")
        self.assertEqual(int(float(metrics["final_usable_persona_count"])), 3)
        self.assertEqual(int(float(metrics["production_ready_persona_count"])), 3)
        self.assertEqual(int(float(metrics["review_ready_persona_count"])), 1)
        self.assertEqual(int(float(metrics["weak_source_cost_center_count"])), 4)
        self.assertEqual(int(float(quality_metrics["core_readiness_weak_source_cost_center_count"])), 3)

    def test_raw_source_directories_still_exist_and_weak_source_visibility_is_preserved(self) -> None:
        raw_root = ROOT_DIR / "data" / "raw"
        for source in [
            "adobe_analytics_community",
            "google_developer_forums",
            "domo_community_forum",
            "klaviyo_community",
        ]:
            self.assertTrue((raw_root / source).exists(), f"Expected raw source directory to remain present for {source}.")

        source_balance = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_balance_audit.csv")
        klaviyo_row = source_balance.loc[source_balance["source"].astype(str).eq("klaviyo_community")].iloc[0]
        self.assertIn("weak_source_cost_center", source_balance.columns)
        self.assertTrue(bool(klaviyo_row["weak_source_cost_center"]))
        self.assertEqual(str(klaviyo_row["source_tier"]), "excluded_from_deck_ready_core")


if __name__ == "__main__":
    unittest.main()
