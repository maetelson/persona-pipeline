"""Tests for diagnostics-only deck-ready feasibility analysis."""

from __future__ import annotations

import unittest
from pathlib import Path

from src.analysis.quality_status import QUALITY_STATUS_POLICY, READINESS_POLICY
from src.analysis.deck_ready_feasibility_analysis import build_deck_ready_feasibility_analysis


ROOT_DIR = Path(__file__).resolve().parents[1]


class DeckReadyFeasibilityAnalysisTests(unittest.TestCase):
    """Validate the bounded deck-ready feasibility report against live artifacts."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.report = build_deck_ready_feasibility_analysis(ROOT_DIR)

    def test_baseline_matches_current_live_metrics(self) -> None:
        baseline = self.report["baseline"]
        self.assertEqual(baseline["overall_status"], "WARN")
        self.assertEqual(baseline["quality_flag"], "EXPLORATORY")
        self.assertEqual(baseline["persona_readiness_state"], "reviewable_but_not_deck_ready")
        self.assertEqual(baseline["final_usable_persona_count"], 3)
        self.assertEqual(baseline["production_ready_persona_count"], 3)
        self.assertEqual(baseline["review_ready_persona_count"], 1)
        self.assertEqual(baseline["weak_source_cost_center_count"], 4)
        self.assertEqual(baseline["core_readiness_weak_source_cost_center_count"], 3)

    def test_source_balance_gap_uses_current_policy_floor(self) -> None:
        gap = self.report["source_balance_gap_analysis"]
        self.assertEqual(
            gap["deck_ready_floor"],
            float(QUALITY_STATUS_POLICY["effective_source_diversity"]["warn_threshold"]),
        )
        self.assertAlmostEqual(gap["gap_to_deck_ready_floor"], 0.11, places=2)

    def test_core_coverage_gap_comes_from_live_artifacts(self) -> None:
        gap = self.report["core_coverage_gap_analysis"]
        self.assertEqual(gap["current_labeled_rows"], 12674)
        self.assertEqual(gap["current_persona_core_rows"], 9444)
        self.assertEqual(gap["rows_needed_to_reach_75_0"], 62)
        self.assertEqual(gap["rows_needed_to_reach_80_0"], 696)

    def test_current_source_balance_driver_is_healthy_high_volume_sources(self) -> None:
        gap = self.report["source_balance_gap_analysis"]
        self.assertEqual(gap["imbalance_driver"], "healthy_high_volume_sources")
        self.assertFalse(gap["single_source_remediation_realistically_moves_score"])

    def test_realistic_scenario_does_not_reach_deck_ready(self) -> None:
        scenario = next(
            row
            for row in self.report["scenario_simulation"]
            if row["scenario_id"] == "F_combined_realistic_one_source_win_plus_modest_balance_improvement"
        )
        self.assertFalse(scenario["deck_ready_candidate"])
        self.assertEqual(scenario["persona_readiness_state"], "reviewable_but_not_deck_ready")

    def test_aggressive_scenario_is_only_way_to_hit_deck_ready(self) -> None:
        scenario = next(
            row
            for row in self.report["scenario_simulation"]
            if row["scenario_id"] == "G_aggressive_hit_deck_ready_thresholds"
        )
        self.assertTrue(scenario["deck_ready_candidate"])
        self.assertEqual(scenario["persona_readiness_state"], "deck_ready")

    def test_scenarios_keep_persona_counts_constant(self) -> None:
        baseline = self.report["baseline"]
        for row in self.report["scenario_simulation"]:
            self.assertEqual(row["final_usable_persona_count"], baseline["final_usable_persona_count"])
            self.assertEqual(row["production_ready_persona_count"], baseline["production_ready_persona_count"])
            self.assertEqual(row["review_ready_persona_count"], baseline["review_ready_persona_count"])

    def test_decision_recommends_freeze_even_if_aggressive_path_exists(self) -> None:
        self.assertEqual(
            self.report["deck_ready_feasibility_decision"],
            "deck_ready_feasible_but_requires_large_data_quality_work",
        )
        self.assertEqual(self.report["recommended_next_path"], "stop and freeze as reviewable release")
        self.assertFalse(self.report["continue_toward_deck_ready"])
        self.assertTrue(self.report["freeze_at_reviewable_release"])

    def test_deck_ready_policy_target_remains_80_percent_core_coverage(self) -> None:
        self.assertEqual(
            READINESS_POLICY["deck_ready"]["requirements"]["persona_core_coverage_of_all_labeled_pct"]["min"],
            80.0,
        )


if __name__ == "__main__":
    unittest.main()
