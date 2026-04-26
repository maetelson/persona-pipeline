"""Regression tests for persona_05 subtheme-preservation annotations."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.persona05_subtheme_preservation import build_persona05_subtheme_outputs


ROOT_DIR = Path(__file__).resolve().parents[1]


class Persona05SubthemePreservationTests(unittest.TestCase):
    """Validate persona_05 subtheme-preservation fields without changing persona semantics."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.persona_summary_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_summary.csv")
        cls.cluster_stats_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "cluster_stats.csv")
        cls.promotion_path_debug_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_promotion_path_debug.csv")

    def test_required_subtheme_fields_exist_and_align(self) -> None:
        required = [
            "subtheme_status",
            "parent_persona_id",
            "parent_persona_relation",
            "future_candidate_subtheme",
            "subtheme_reason",
            "standalone_persona_recommended",
            "claim_eligible_recommended",
            "related_subtheme_ids",
        ]
        for frame in [self.persona_summary_df, self.cluster_stats_df, self.promotion_path_debug_df]:
            for column in required:
                self.assertIn(column, frame.columns)

        summary = self.persona_summary_df[["persona_id", *required]].sort_values("persona_id").reset_index(drop=True)
        cluster = self.cluster_stats_df[["persona_id", *required]].sort_values("persona_id").reset_index(drop=True)
        debug = self.promotion_path_debug_df[["persona_id", *required]].sort_values("persona_id").reset_index(drop=True)
        pd.testing.assert_frame_equal(summary, cluster, check_dtype=False)
        overlap_ids = debug["persona_id"].astype(str).tolist()
        summary_overlap = summary[summary["persona_id"].astype(str).isin(overlap_ids)].sort_values("persona_id").reset_index(drop=True)
        pd.testing.assert_frame_equal(summary_overlap, debug, check_dtype=False)

    def test_persona05_expected_values_and_status_guards(self) -> None:
        row = self.cluster_stats_df.set_index("persona_id").loc["persona_05"]
        self.assertEqual(str(row["subtheme_status"]), "future_candidate_subtheme")
        self.assertEqual(str(row["parent_persona_id"]), "persona_03")
        self.assertEqual(str(row["parent_persona_relation"]), "delivery_specific_subtheme")
        self.assertTrue(bool(row["future_candidate_subtheme"]))
        self.assertFalse(bool(row["standalone_persona_recommended"]))
        self.assertFalse(bool(row["claim_eligible_recommended"]))
        self.assertFalse(bool(row["production_ready_persona"]))
        self.assertFalse(bool(row["review_ready_persona"]))
        self.assertFalse(bool(row["final_usable_persona"]))
        self.assertFalse(bool(row["deck_ready_claim_eligible_persona"]))
        self.assertEqual(str(row["readiness_tier"]), "blocked_or_constrained_candidate")

    def test_persona03_remains_production_ready_and_can_reference_subtheme(self) -> None:
        row = self.cluster_stats_df.set_index("persona_id").loc["persona_03"]
        self.assertEqual(str(row["subtheme_status"]), "not_applicable")
        self.assertFalse(bool(row["future_candidate_subtheme"]))
        self.assertTrue(bool(row["production_ready_persona"]))
        self.assertTrue(bool(row["final_usable_persona"]))
        self.assertTrue(bool(row["deck_ready_claim_eligible_persona"]))
        self.assertEqual(str(row["related_subtheme_ids"]), "persona_05")

    def test_no_other_persona_becomes_subtheme(self) -> None:
        non_target = self.cluster_stats_df[~self.cluster_stats_df["persona_id"].astype(str).isin(["persona_03", "persona_05"])].copy()
        self.assertTrue(non_target["future_candidate_subtheme"].fillna(False).astype(bool).eq(False).all())
        self.assertTrue(non_target["subtheme_status"].astype(str).eq("not_applicable").all())
        self.assertTrue(non_target["parent_persona_id"].fillna("").astype(str).eq("").all())

    def test_counts_and_readiness_state_remain_unchanged(self) -> None:
        overview = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
        metrics = dict(zip(overview["metric"].astype(str), overview["value"]))
        self.assertEqual(int(float(metrics["final_usable_persona_count"])), 3)
        self.assertEqual(int(float(metrics["production_ready_persona_count"])), 3)
        self.assertEqual(int(float(metrics["review_ready_persona_count"])), 1)
        self.assertEqual(int(float(metrics["deck_ready_claim_eligible_persona_count"])), 4)
        self.assertEqual(str(metrics["persona_readiness_state"]), "reviewable_but_not_deck_ready")
        self.assertEqual(str(metrics["overall_status"]), "WARN")
        self.assertEqual(str(metrics["quality_flag"]), "EXPLORATORY")

    def test_helper_keeps_non_target_personas_non_subthemes(self) -> None:
        outputs = build_persona05_subtheme_outputs(
            persona_summary_df=pd.DataFrame(
                [
                    {"persona_id": "persona_03", "production_ready_persona": True, "deck_ready_claim_eligible_persona": True},
                    {"persona_id": "persona_05", "production_ready_persona": False, "deck_ready_claim_eligible_persona": False},
                    {"persona_id": "persona_x", "production_ready_persona": False, "deck_ready_claim_eligible_persona": False},
                ]
            ),
            cluster_stats_df=pd.DataFrame(
                [
                    {"persona_id": "persona_03", "production_ready_persona": True, "deck_ready_claim_eligible_persona": True},
                    {"persona_id": "persona_05", "production_ready_persona": False, "deck_ready_claim_eligible_persona": False},
                    {"persona_id": "persona_x", "production_ready_persona": False, "deck_ready_claim_eligible_persona": False},
                ]
            ),
            persona_promotion_path_debug_df=pd.DataFrame(
                [
                    {"persona_id": "persona_03"},
                    {"persona_id": "persona_05"},
                    {"persona_id": "persona_x"},
                ]
            ),
        )
        lookup = outputs["cluster_stats_df"].set_index("persona_id")
        self.assertEqual(str(lookup.loc["persona_05", "subtheme_status"]), "future_candidate_subtheme")
        self.assertEqual(str(lookup.loc["persona_03", "related_subtheme_ids"]), "persona_05")
        self.assertEqual(str(lookup.loc["persona_x", "subtheme_status"]), "not_applicable")
        self.assertFalse(bool(lookup.loc["persona_x", "future_candidate_subtheme"]))


if __name__ == "__main__":
    unittest.main()
