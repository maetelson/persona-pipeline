"""Tests for diagnostics-only source representativeness audit outputs."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.source_representativeness_audit import build_source_representativeness_audit


ROOT_DIR = Path(__file__).resolve().parents[1]


class SourceRepresentativenessAuditTests(unittest.TestCase):
    """Validate the source representativeness audit against live artifacts."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.report = build_source_representativeness_audit(ROOT_DIR)

    def test_source_metrics_load_from_live_artifacts(self) -> None:
        fact_rows = self.report["source_fact_table"]
        source_balance = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_balance_audit.csv")
        self.assertEqual(len(fact_rows), len(source_balance))
        first = fact_rows[0]
        for key in [
            "source",
            "raw_rows",
            "valid_rows",
            "prefiltered_rows",
            "episode_rows",
            "labeled_rows",
            "persona_core_rows",
            "production_ready_persona_contribution",
            "review_ready_persona_contribution",
            "top_persona_contributions",
        ]:
            self.assertIn(key, first)

    def test_tier_assignment_is_deterministic_and_named_sources_are_present(self) -> None:
        recommendations = {row["source"]: row["recommended_tier"] for row in self.report["source_tier_recommendations"]}
        for source in [
            "google_developer_forums",
            "adobe_analytics_community",
            "domo_community_forum",
            "klaviyo_community",
        ]:
            self.assertIn(source, recommendations)
        self.assertNotEqual(recommendations["klaviyo_community"], "core_representative_source")

    def test_audit_distinguishes_reviewable_membership_from_deck_ready_core_membership(self) -> None:
        recommendations = {row["source"]: row for row in self.report["source_tier_recommendations"]}
        klaviyo = recommendations["klaviyo_community"]
        self.assertTrue(klaviyo["keep_in_reviewable"])
        self.assertFalse(klaviyo["keep_in_deck_ready_core"])
        self.assertTrue(klaviyo["keep_in_raw_archive"])

    def test_ablation_scenarios_expose_required_readiness_fields(self) -> None:
        scenarios = {row["scenario_id"]: row for row in self.report["ablation_scenarios"]}
        self.assertIn("A_current_baseline", scenarios)
        self.assertIn("H_keep_only_core_representative_sources", scenarios)
        baseline = scenarios["A_current_baseline"]
        for key in [
            "remaining_raw_rows",
            "remaining_labeled_rows",
            "remaining_source_count",
            "final_usable_persona_count",
            "production_ready_persona_count",
            "review_ready_persona_count",
            "effective_balanced_source_count",
            "persona_core_coverage_of_all_labeled_pct",
            "weak_source_cost_center_count",
            "core_readiness_weak_source_cost_center_count",
            "overall_status",
            "persona_readiness_state",
            "persona_evidence_loss_by_persona",
            "deck_ready_plausibility",
            "methodological_representativeness",
        ]:
            self.assertIn(key, baseline)

    def test_ablation_does_not_inflate_persona_counts(self) -> None:
        baseline = next(row for row in self.report["ablation_scenarios"] if row["scenario_id"] == "A_current_baseline")
        for row in self.report["ablation_scenarios"]:
            self.assertLessEqual(row["final_usable_persona_count"], baseline["final_usable_persona_count"])
            self.assertLessEqual(row["production_ready_persona_count"], baseline["production_ready_persona_count"])
            self.assertLessEqual(row["review_ready_persona_count"], baseline["review_ready_persona_count"])

    def test_questioned_source_assessments_are_explicit(self) -> None:
        assessments = {row["source"]: row for row in self.report["questioned_source_assessments"]}
        for source in [
            "google_developer_forums",
            "adobe_analytics_community",
            "domo_community_forum",
            "klaviyo_community",
        ]:
            self.assertIn(source, assessments)
            self.assertIn("recommended_tier", assessments[source])
            self.assertIn("keep_in_deck_ready_core", assessments[source])


if __name__ == "__main__":
    unittest.main()
