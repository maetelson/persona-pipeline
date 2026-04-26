"""Tests for diagnostics-only review-ready gap analysis."""

from __future__ import annotations

import unittest
import math
from pathlib import Path

import pandas as pd

from src.analysis.quality_status import QUALITY_STATUS_POLICY, READINESS_POLICY
from src.analysis.review_ready_gap_analysis import build_review_ready_gap_analysis


ROOT_DIR = Path(__file__).resolve().parents[1]


class ReviewReadyGapAnalysisTests(unittest.TestCase):
    """Validate the readiness gap analysis against current artifacts."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.report = build_review_ready_gap_analysis(ROOT_DIR)

    def _as_bool(self, value: object) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"true", "1", "yes"}

    def test_blocker_thresholds_come_from_current_policy(self) -> None:
        blockers = {row["metric_name"]: row for row in self.report["readiness_blockers"]}
        self.assertEqual(
            blockers["effective_balanced_source_count"]["reviewable_threshold"],
            f">={QUALITY_STATUS_POLICY['effective_source_diversity']['fail_threshold']}",
        )
        self.assertEqual(
            blockers["core_readiness_weak_source_cost_center_count"]["reviewable_threshold"],
            "<4",
        )
        self.assertEqual(
            blockers["persona_core_coverage_of_all_labeled_pct"]["reviewable_threshold"],
            str(READINESS_POLICY["reviewable_but_not_deck_ready"]["requirements"]["persona_core_coverage_of_all_labeled_pct"]["min"]),
        )

    def test_core_coverage_gap_matches_current_artifacts(self) -> None:
        gap = self.report["core_coverage_gap_analysis"]
        quality_checks = pd.read_csv(ROOT_DIR / "data" / "analysis" / "quality_checks.csv")
        labeled_rows = int(
            float(quality_checks.loc[quality_checks["metric"] == "labeled_episode_rows", "value"].iloc[0])
        )
        persona_core_rows = int(
            float(quality_checks.loc[quality_checks["metric"] == "persona_core_labeled_rows", "value"].iloc[0])
        )
        expected_rows_to_75 = max(0, math.ceil(labeled_rows * 0.75) - persona_core_rows)
        expected_rows_to_80 = max(0, math.ceil(labeled_rows * 0.80) - persona_core_rows)

        self.assertEqual(gap["current_labeled_rows"], labeled_rows)
        self.assertEqual(gap["current_persona_core_rows"], persona_core_rows)
        self.assertEqual(gap["rows_needed_to_reach_75_0"], expected_rows_to_75)
        self.assertEqual(gap["rows_needed_to_reach_80_0"], expected_rows_to_80)

    def test_weak_source_decisions_follow_expected_defaults(self) -> None:
        decisions = {row["source"]: row["recommended_action"] for row in self.report["weak_source_decisions"]}
        self.assertEqual(decisions["google_developer_forums"], "fix_now_with_evidence")
        self.assertEqual(decisions["domo_community_forum"], "parser_or_episode_fidelity_audit_needed")
        self.assertEqual(decisions["adobe_analytics_community"], "parser_or_episode_fidelity_audit_needed")
        self.assertEqual(decisions["klaviyo_community"], "downgrade_to_exploratory_only")

    def test_current_baseline_is_reviewable_after_policy_cleanup(self) -> None:
        baseline = self.report["baseline"]
        self.assertEqual(baseline["overall_status"], "WARN")
        self.assertEqual(baseline["persona_readiness_state"], "reviewable_but_not_deck_ready")
        self.assertEqual(baseline["final_usable_persona_count"], 3)
        self.assertEqual(baseline["production_ready_persona_count"], 3)
        self.assertEqual(baseline["review_ready_persona_count"], 1)

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

    def test_source_balance_audit_flags_only_klaviyo_as_exploratory_only_debt(self) -> None:
        df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_balance_audit.csv")
        row = df.loc[df["source"] == "klaviyo_community"].iloc[0]
        self.assertTrue(self._as_bool(row["weak_source_cost_center"]))
        self.assertFalse(self._as_bool(row["core_readiness_weak_source_cost_center"]))
        self.assertTrue(self._as_bool(row["exploratory_only_weak_source_debt"]))

    def test_google_adobe_and_domo_remain_core_readiness_weak_sources(self) -> None:
        df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_balance_audit.csv")
        for source in ["google_developer_forums", "adobe_analytics_community", "domo_community_forum"]:
            row = df.loc[df["source"] == source].iloc[0]
            self.assertTrue(self._as_bool(row["weak_source_cost_center"]))
            self.assertTrue(self._as_bool(row["core_readiness_weak_source_cost_center"]))
            self.assertFalse(self._as_bool(row["exploratory_only_weak_source_debt"]))

    def test_persona_tiers_remain_unchanged(self) -> None:
        df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_summary.csv")
        persona_04 = df.loc[df["persona_id"] == "persona_04"].iloc[0]
        persona_05 = df.loc[df["persona_id"] == "persona_05"].iloc[0]
        self.assertFalse(self._as_bool(persona_04["final_usable_persona"]))
        self.assertTrue(self._as_bool(persona_04["review_ready_persona"]))
        self.assertEqual(str(persona_04["readiness_tier"]), "review_ready_persona")
        self.assertFalse(self._as_bool(persona_05["final_usable_persona"]))
        self.assertFalse(self._as_bool(persona_05["review_ready_persona"]))
        self.assertEqual(str(persona_05["readiness_tier"]), "blocked_or_constrained_candidate")


if __name__ == "__main__":
    unittest.main()
