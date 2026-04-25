"""Tests for simulation-only workbook policy redesign helpers."""

from __future__ import annotations

import unittest

import pandas as pd

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


if __name__ == "__main__":
    unittest.main()
