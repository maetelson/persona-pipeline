"""Tests for simulation-only workbook policy redesign helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.persona_service import _review_ready_fields
from src.analysis.workbook_policy_redesign import _variant_result


class WorkbookPolicyRedesignTests(unittest.TestCase):
    """Validate bounded workbook policy redesign outcomes."""

    def _audit_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "persona_id": "persona_01",
                    "final_usable_persona": True,
                    "weak_source_link": False,
                    "thin_evidence_candidate": False,
                    "strategic_redundancy_status": "not_evaluated",
                    "semantic_review_candidate": False,
                },
                {
                    "persona_id": "persona_04",
                    "final_usable_persona": False,
                    "weak_source_link": False,
                    "thin_evidence_candidate": False,
                    "strategic_redundancy_status": "not_evaluated",
                    "semantic_review_candidate": True,
                },
                {
                    "persona_id": "persona_05",
                    "final_usable_persona": False,
                    "weak_source_link": False,
                    "thin_evidence_candidate": True,
                    "strategic_redundancy_status": "not_evaluated",
                    "semantic_review_candidate": False,
                },
            ]
        )

    def test_review_ready_variant_keeps_final_count_strict(self) -> None:
        result = _variant_result(
            variant_id="F",
            description="review-ready mode",
            audit_df=self._audit_frame(),
            context={"top_3_share": 83.3, "largest_source_influence_share_pct": 30.7},
            final_ids={"persona_01"},
            review_ready_ids={"persona_04"},
        )
        self.assertEqual(result["final_usable_persona_count"], 1)
        self.assertEqual(result["review_ready_persona_count"], 1)
        self.assertEqual(result["persona_04_status"], "review_ready_persona")
        self.assertFalse(result["thin_evidence_candidate_passes"])
        self.assertTrue(result["accepted"])

    def test_warning_only_variant_flags_thin_evidence_promotion(self) -> None:
        result = _variant_result(
            variant_id="D",
            description="warning only",
            audit_df=self._audit_frame(),
            context={"top_3_share": 83.3, "largest_source_influence_share_pct": 30.7},
            final_ids={"persona_01", "persona_04", "persona_05"},
            review_ready_ids=set(),
        )
        self.assertTrue(result["thin_evidence_candidate_passes"])
        self.assertEqual(result["persona_05_status"], "production_ready_persona")
        self.assertEqual(result["risk_level"], "high")
        self.assertFalse(result["accepted"])

    def test_review_ready_fields_marks_locally_strong_policy_constrained_persona(self) -> None:
        result = _review_ready_fields(
            {
                "status": "exploratory_bucket",
                "base_promotion_status": "promoted_candidate_persona",
                "structural_support_status": "structurally_supported",
                "grounding_status": "grounded_single",
                "promotion_grounding_status": "promotion_constrained_by_workbook_policy",
                "cross_source_robustness_score": 0.91,
                "distinctiveness_score": 0.78,
                "reason": "promotion constrained by workbook concentration and source-balance policy; top_3_cluster_share_of_core_labeled=83.3; weak_source_cost_centers_present",
                "selected_example_count": 5,
            },
            selected_example_count=5,
            evidence_confidence_tier="grounded",
        )
        self.assertEqual(result["readiness_tier"], "review_ready_persona")
        self.assertTrue(result["review_ready_persona"])
        self.assertFalse(result["production_ready_persona"])
        self.assertEqual(result["review_visibility_status"], "review_ready_visible")
        self.assertIn("top_3_cluster_share_of_core_labeled", result["workbook_policy_constraint"])

    def test_review_ready_fields_blocks_thin_candidate(self) -> None:
        result = _review_ready_fields(
            {
                "status": "exploratory_bucket",
                "base_promotion_status": "promoted_candidate_persona",
                "structural_support_status": "structurally_supported",
                "grounding_status": "grounded_single",
                "promotion_grounding_status": "promotion_constrained_by_workbook_policy",
                "cross_source_robustness_score": 0.78,
                "distinctiveness_score": 0.79,
                "reason": "promotion constrained by workbook concentration and source-balance policy; top_3_cluster_share_of_core_labeled=83.3; weak_source_cost_centers_present",
                "selected_example_count": 1,
            },
            selected_example_count=1,
            evidence_confidence_tier="thin",
        )
        self.assertEqual(result["readiness_tier"], "blocked_or_constrained_candidate")
        self.assertFalse(result["review_ready_persona"])
        self.assertIn("thin evidence", result["blocked_reason"])

    def test_review_ready_fields_blocks_near_duplicate(self) -> None:
        result = _review_ready_fields(
            {
                "status": "exploratory_bucket",
                "base_promotion_status": "promoted_candidate_persona",
                "structural_support_status": "structurally_supported",
                "grounding_status": "grounded_single",
                "promotion_grounding_status": "promotion_constrained_by_workbook_policy",
                "cross_source_robustness_score": 0.92,
                "distinctiveness_score": 0.82,
                "strategic_redundancy_status": "strategically_redundant",
                "reason": "promotion constrained by workbook concentration and source-balance policy; top_3_cluster_share_of_core_labeled=83.3",
                "selected_example_count": 4,
            },
            selected_example_count=4,
            evidence_confidence_tier="grounded",
        )
        self.assertFalse(result["review_ready_persona"])
        self.assertIn("near-duplicate", result["blocked_reason"])

    def test_review_ready_fields_blocks_weak_source_dominated_candidate(self) -> None:
        result = _review_ready_fields(
            {
                "status": "exploratory_bucket",
                "base_promotion_status": "promoted_candidate_persona",
                "structural_support_status": "structurally_supported",
                "grounding_status": "grounded_single",
                "promotion_grounding_status": "promotion_constrained_by_workbook_policy",
                "cross_source_robustness_score": 0.62,
                "distinctiveness_score": 0.81,
                "reason": "promotion constrained by workbook concentration and source-balance policy; largest_source_influence_share_pct=41.0",
                "selected_example_count": 4,
            },
            selected_example_count=4,
            evidence_confidence_tier="grounded",
        )
        self.assertFalse(result["review_ready_persona"])
        self.assertIn("weak-source dominated", result["blocked_reason"])


if __name__ == "__main__":
    unittest.main()
