"""Tests for diagnostics-only review-ready gap analysis."""

from __future__ import annotations

import unittest
from pathlib import Path

from src.analysis.quality_status import QUALITY_STATUS_POLICY, READINESS_POLICY
from src.analysis.review_ready_gap_analysis import build_review_ready_gap_analysis


ROOT_DIR = Path(__file__).resolve().parents[1]


class ReviewReadyGapAnalysisTests(unittest.TestCase):
    """Validate the readiness gap analysis against current artifacts."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.report = build_review_ready_gap_analysis(ROOT_DIR)

    def test_blocker_thresholds_come_from_current_policy(self) -> None:
        blockers = {row["metric_name"]: row for row in self.report["readiness_blockers"]}
        self.assertEqual(
            blockers["effective_balanced_source_count"]["reviewable_threshold"],
            f">={QUALITY_STATUS_POLICY['effective_source_diversity']['fail_threshold']}",
        )
        self.assertEqual(
            blockers["persona_core_coverage_of_all_labeled_pct"]["reviewable_threshold"],
            str(READINESS_POLICY["reviewable_but_not_deck_ready"]["requirements"]["persona_core_coverage_of_all_labeled_pct"]["min"]),
        )

    def test_core_coverage_gap_matches_current_artifacts(self) -> None:
        gap = self.report["core_coverage_gap_analysis"]
        self.assertEqual(gap["current_labeled_rows"], 12669)
        self.assertEqual(gap["current_persona_core_rows"], 9442)
        self.assertEqual(gap["rows_needed_to_reach_75_0"], 60)
        self.assertEqual(gap["rows_needed_to_reach_80_0"], 694)

    def test_weak_source_decisions_follow_expected_defaults(self) -> None:
        decisions = {row["source"]: row["recommended_action"] for row in self.report["weak_source_decisions"]}
        self.assertEqual(decisions["google_developer_forums"], "fix_now_with_evidence")
        self.assertEqual(decisions["domo_community_forum"], "parser_or_episode_fidelity_audit_needed")
        self.assertEqual(decisions["adobe_analytics_community"], "parser_or_episode_fidelity_audit_needed")
        self.assertEqual(decisions["klaviyo_community"], "downgrade_to_exploratory_only")

    def test_baseline_scenario_remains_exploratory(self) -> None:
        baseline = next(row for row in self.report["scenario_simulation"] if row["scenario_id"] == "A_current_baseline")
        self.assertEqual(baseline["overall_status"], "FAIL")
        self.assertEqual(baseline["persona_readiness_state"], "exploratory_only")

    def test_cleanup_scenario_only_helps_when_fail_drops_to_warn(self) -> None:
        improved = [
            row for row in self.report["scenario_simulation"] if row["reviewable_achievable_without_weakening"]
        ]
        self.assertTrue(improved)
        self.assertTrue(all(row["overall_status"] == "WARN" for row in improved))
        self.assertTrue(all(row["persona_readiness_state"] == "reviewable_but_not_deck_ready" for row in improved))

    def test_scenarios_do_not_mutate_persona_counts(self) -> None:
        baseline = self.report["baseline"]
        for row in self.report["scenario_simulation"]:
            self.assertEqual(row["final_usable_persona_count"], baseline["final_usable_persona_count"])
            self.assertEqual(row["production_ready_persona_count"], baseline["production_ready_persona_count"])
            self.assertEqual(row["review_ready_persona_count"], baseline["review_ready_persona_count"])


if __name__ == "__main__":
    unittest.main()
