"""Regression tests for Phase 3A deck-ready claim eligibility annotations."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.deck_ready_claims import build_deck_ready_claim_outputs


ROOT_DIR = Path(__file__).resolve().parents[1]


class DeckReadyClaimEligibilityTests(unittest.TestCase):
    """Validate claim-eligibility annotations without changing readiness semantics."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.persona_summary_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_summary.csv")
        cls.cluster_stats_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "cluster_stats.csv")
        cls.promotion_path_debug_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_promotion_path_debug.csv")

    def test_required_phase3a_fields_exist_and_agree(self) -> None:
        required = [
            "deck_ready_claim_eligible_persona",
            "deck_ready_claim_evidence_status",
            "deck_ready_claim_reason",
            "core_anchor_policy_status",
            "supporting_validation_policy_status",
            "exploratory_dependency_policy_status",
            "excluded_source_dependency_policy_status",
        ]
        for frame in [self.persona_summary_df, self.cluster_stats_df, self.promotion_path_debug_df]:
            for column in required:
                self.assertIn(column, frame.columns)

        summary = self.persona_summary_df[["persona_id", *required]].sort_values("persona_id").reset_index(drop=True)
        cluster = self.cluster_stats_df[["persona_id", *required]].sort_values("persona_id").reset_index(drop=True)
        debug = self.promotion_path_debug_df[["persona_id", *required]].sort_values("persona_id").reset_index(drop=True)
        pd.testing.assert_frame_equal(summary, cluster, check_dtype=False)
        self.assertGreater(len(debug), 0)
        overlap_ids = debug["persona_id"].astype(str).tolist()
        summary_overlap = summary[summary["persona_id"].astype(str).isin(overlap_ids)].sort_values("persona_id").reset_index(drop=True)
        pd.testing.assert_frame_equal(summary_overlap, debug, check_dtype=False)

    def test_persona_01_to_03_are_claim_eligible(self) -> None:
        lookup = self.cluster_stats_df.set_index("persona_id")
        for persona_id in ["persona_01", "persona_02", "persona_03"]:
            row = lookup.loc[persona_id]
            self.assertTrue(bool(row["deck_ready_claim_eligible_persona"]))
            self.assertTrue(bool(row["production_ready_persona"]))
            self.assertEqual(str(row["core_anchor_policy_status"]), "pass")
            self.assertEqual(str(row["exploratory_dependency_policy_status"]), "pass")
            self.assertEqual(str(row["excluded_source_dependency_policy_status"]), "pass")

    def test_persona_04_remains_review_ready_non_production_with_explicit_reason(self) -> None:
        row = self.cluster_stats_df.set_index("persona_id").loc["persona_04"]
        self.assertFalse(bool(row["production_ready_persona"]))
        self.assertTrue(bool(row["review_ready_persona"]))
        self.assertEqual(str(row["readiness_tier"]), "review_ready_persona")
        self.assertTrue(bool(row["deck_ready_claim_eligible_persona"]))
        reason = str(row["deck_ready_claim_reason"])
        self.assertIn("review-ready persona", reason)
        self.assertIn("non-production", reason)
        self.assertIn("final usable count", reason)

    def test_persona_05_remains_blocked_and_not_claim_eligible(self) -> None:
        row = self.cluster_stats_df.set_index("persona_id").loc["persona_05"]
        self.assertFalse(bool(row["deck_ready_claim_eligible_persona"]))
        self.assertEqual(str(row["readiness_tier"]), "blocked_or_constrained_candidate")
        self.assertIn("Blocked or constrained persona", str(row["deck_ready_claim_reason"]))

    def test_phase3a_does_not_change_readiness_or_persona_counts(self) -> None:
        overview = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
        metrics = dict(zip(overview["metric"].astype(str), overview["value"]))
        self.assertEqual(str(metrics["persona_readiness_state"]), "reviewable_but_not_deck_ready")
        self.assertEqual(str(metrics["overall_status"]), "WARN")
        self.assertEqual(str(metrics["quality_flag"]), "EXPLORATORY")
        self.assertEqual(int(float(metrics["final_usable_persona_count"])), 3)
        self.assertEqual(int(float(metrics["production_ready_persona_count"])), 3)
        self.assertEqual(int(float(metrics["review_ready_persona_count"])), 1)
        self.assertEqual(int(float(metrics["deck_ready_claim_eligible_persona_count"])), 4)

    def test_supporting_validation_cannot_replace_missing_core_anchor(self) -> None:
        outputs = build_deck_ready_claim_outputs(
            persona_summary_df=pd.DataFrame(
                [
                    {
                        "persona_id": "persona_x",
                        "production_ready_persona": True,
                        "review_ready_persona": False,
                        "readiness_tier": "production_ready_persona",
                        "selected_example_count": 3,
                        "cluster_evidence_status": "sufficient",
                        "structural_support_status": "structurally_supported",
                        "has_core_representative_anchor": False,
                        "core_anchor_strength": "none",
                        "supporting_validation_strength": "strong",
                        "exploratory_dependency_risk": "low",
                        "excluded_source_dependency_risk": "low",
                        "deck_ready_claim_evidence_status": "supporting_validated",
                    }
                ]
            ),
            cluster_stats_df=pd.DataFrame(
                [
                    {
                        "persona_id": "persona_x",
                        "production_ready_persona": True,
                        "review_ready_persona": False,
                        "readiness_tier": "production_ready_persona",
                        "selected_example_count": 3,
                        "cluster_evidence_status": "sufficient",
                        "structural_support_status": "structurally_supported",
                        "has_core_representative_anchor": False,
                        "core_anchor_strength": "none",
                        "supporting_validation_strength": "strong",
                        "exploratory_dependency_risk": "low",
                        "excluded_source_dependency_risk": "low",
                        "deck_ready_claim_evidence_status": "supporting_validated",
                    }
                ]
            ),
            persona_promotion_path_debug_df=pd.DataFrame([{"persona_id": "persona_x"}]),
        )
        row = outputs["cluster_stats_df"].iloc[0]
        self.assertFalse(bool(row["deck_ready_claim_eligible_persona"]))
        self.assertEqual(str(row["core_anchor_policy_status"]), "blocked_missing_core_anchor")

    def test_dependency_risk_blocks_claim_eligibility(self) -> None:
        outputs = build_deck_ready_claim_outputs(
            persona_summary_df=pd.DataFrame(
                [
                    {
                        "persona_id": "persona_y",
                        "production_ready_persona": True,
                        "review_ready_persona": False,
                        "readiness_tier": "production_ready_persona",
                        "selected_example_count": 2,
                        "cluster_evidence_status": "sufficient",
                        "structural_support_status": "structurally_supported",
                        "has_core_representative_anchor": True,
                        "core_anchor_strength": "strong",
                        "supporting_validation_strength": "moderate",
                        "exploratory_dependency_risk": "medium",
                        "excluded_source_dependency_risk": "low",
                        "deck_ready_claim_evidence_status": "core_anchored",
                    }
                ]
            ),
            cluster_stats_df=pd.DataFrame(
                [
                    {
                        "persona_id": "persona_y",
                        "production_ready_persona": True,
                        "review_ready_persona": False,
                        "readiness_tier": "production_ready_persona",
                        "selected_example_count": 2,
                        "cluster_evidence_status": "sufficient",
                        "structural_support_status": "structurally_supported",
                        "has_core_representative_anchor": True,
                        "core_anchor_strength": "strong",
                        "supporting_validation_strength": "moderate",
                        "exploratory_dependency_risk": "medium",
                        "excluded_source_dependency_risk": "low",
                        "deck_ready_claim_evidence_status": "core_anchored",
                    }
                ]
            ),
            persona_promotion_path_debug_df=pd.DataFrame([{"persona_id": "persona_y"}]),
        )
        row = outputs["cluster_stats_df"].iloc[0]
        self.assertFalse(bool(row["deck_ready_claim_eligible_persona"]))
        self.assertEqual(str(row["exploratory_dependency_policy_status"]), "blocked_medium_exploratory_dependency")


if __name__ == "__main__":
    unittest.main()
