"""Regression tests for Phase 2 deck-ready evidence tier accounting."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.source_tier_evidence import build_source_tier_evidence_outputs
from src.analysis.source_tiers import annotate_source_tiers


ROOT_DIR = Path(__file__).resolve().parents[1]


class SourceTierEvidenceTests(unittest.TestCase):
    """Validate tier-aware evidence counts without changing readiness semantics."""

    @classmethod
    def setUpClass(cls) -> None:
        episodes_df = pd.read_parquet(ROOT_DIR / "data" / "episodes" / "episode_table.parquet", columns=["episode_id", "source"])
        labeled_df = pd.read_parquet(ROOT_DIR / "data" / "labeled" / "labeled_episodes.parquet")
        persona_assignments_df = pd.read_parquet(ROOT_DIR / "data" / "analysis" / "persona_assignments.parquet", columns=["episode_id", "persona_id"])
        persona_summary_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_summary.csv")
        cluster_stats_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "cluster_stats.csv")
        cls.outputs = build_source_tier_evidence_outputs(
            episodes_df=episodes_df,
            labeled_df=labeled_df,
            persona_assignments_df=persona_assignments_df,
            persona_summary_df=persona_summary_df,
            cluster_stats_df=cluster_stats_df,
        )
        cls.episodes_df = episodes_df
        cls.labeled_df = labeled_df
        cls.persona_assignments_df = persona_assignments_df

    def test_global_tier_counts_match_live_artifacts_and_sum_back(self) -> None:
        report = self.outputs["report"]["global_tier_evidence_counts"]
        episode_sources = annotate_source_tiers(self.episodes_df[["episode_id", "source"]].drop_duplicates("episode_id"))
        labeled_with_source = self.labeled_df.merge(episode_sources, on="episode_id", how="left")
        labeled_with_source["persona_core_eligible"] = labeled_with_source["persona_core_eligible"].fillna(False).astype(bool)

        self.assertEqual(report["deck_ready_core_labeled_row_count"], int(labeled_with_source["source_tier"].eq("core_representative_source").sum()))
        self.assertEqual(report["supporting_validation_labeled_row_count"], int(labeled_with_source["source_tier"].eq("supporting_validation_source").sum()))
        self.assertEqual(report["exploratory_edge_labeled_row_count"], int(labeled_with_source["source_tier"].eq("exploratory_edge_source").sum()))
        self.assertEqual(report["excluded_from_deck_ready_core_labeled_row_count"], int(labeled_with_source["source_tier"].eq("excluded_from_deck_ready_core").sum()))

        persona_core = labeled_with_source[labeled_with_source["persona_core_eligible"]].copy()
        self.assertEqual(report["deck_ready_core_persona_core_row_count"], int(persona_core["source_tier"].eq("core_representative_source").sum()))
        self.assertEqual(report["supporting_validation_persona_core_row_count"], int(persona_core["source_tier"].eq("supporting_validation_source").sum()))
        self.assertEqual(report["exploratory_edge_persona_core_row_count"], int(persona_core["source_tier"].eq("exploratory_edge_source").sum()))
        self.assertEqual(report["excluded_from_deck_ready_core_persona_core_row_count"], int(persona_core["source_tier"].eq("excluded_from_deck_ready_core").sum()))

        self.assertEqual(
            report["deck_ready_core_labeled_row_count"]
            + report["supporting_validation_labeled_row_count"]
            + report["exploratory_edge_labeled_row_count"]
            + report["excluded_from_deck_ready_core_labeled_row_count"],
            int(len(labeled_with_source)),
        )
        self.assertEqual(
            report["deck_ready_core_persona_core_row_count"]
            + report["supporting_validation_persona_core_row_count"]
            + report["exploratory_edge_persona_core_row_count"]
            + report["excluded_from_deck_ready_core_persona_core_row_count"],
            int(persona_core["episode_id"].nunique()),
        )

    def test_per_persona_tier_counts_sum_to_total(self) -> None:
        breakdown = self.outputs["persona_breakdown_df"]
        totals = (
            breakdown["core_representative_persona_core_rows"].astype(int)
            + breakdown["supporting_validation_persona_core_rows"].astype(int)
            + breakdown["exploratory_edge_persona_core_rows"].astype(int)
            + breakdown["excluded_from_deck_ready_core_persona_core_rows"].astype(int)
        )
        self.assertTrue((totals == breakdown["total_persona_core_rows"].astype(int)).all())

    def test_named_source_evidence_flows_into_expected_tiers(self) -> None:
        episode_sources = annotate_source_tiers(self.episodes_df[["episode_id", "source"]].drop_duplicates("episode_id"))
        counts = (
            self.labeled_df.merge(episode_sources, on="episode_id", how="left")
            .groupby(["source", "source_tier"], dropna=False)["episode_id"]
            .nunique()
            .reset_index(name="rows")
        )
        lookup = {(row["source"], row["source_tier"]): int(row["rows"]) for _, row in counts.iterrows()}
        self.assertGreater(lookup[("power_bi_community", "core_representative_source")], 0)
        self.assertGreater(lookup[("metabase_discussions", "core_representative_source")], 0)
        self.assertGreater(lookup[("adobe_analytics_community", "supporting_validation_source")], 0)
        self.assertGreater(lookup[("google_developer_forums", "supporting_validation_source")], 0)
        self.assertGreater(lookup[("domo_community_forum", "exploratory_edge_source")], 0)
        self.assertGreater(lookup[("klaviyo_community", "excluded_from_deck_ready_core")], 0)

    def test_persona_outputs_receive_phase2_fields(self) -> None:
        for frame in [self.outputs["persona_summary_df"], self.outputs["cluster_stats_df"]]:
            for column in [
                "total_persona_core_rows",
                "core_representative_persona_core_rows",
                "supporting_validation_persona_core_rows",
                "exploratory_edge_persona_core_rows",
                "excluded_from_deck_ready_core_persona_core_rows",
                "core_representative_share_of_persona_core",
                "supporting_validation_share_of_persona_core",
                "exploratory_edge_share_of_persona_core",
                "excluded_share_of_persona_core",
                "has_core_representative_anchor",
                "core_anchor_strength",
                "supporting_validation_strength",
                "exploratory_dependency_risk",
                "excluded_source_dependency_risk",
                "deck_ready_claim_evidence_status",
            ]:
                self.assertIn(column, frame.columns)

    def test_readiness_and_persona_counts_remain_unchanged(self) -> None:
        overview = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
        quality_checks = pd.read_csv(ROOT_DIR / "data" / "analysis" / "quality_checks.csv")
        overview_metrics = dict(zip(overview["metric"].astype(str), overview["value"]))
        quality_metrics = dict(zip(quality_checks["metric"].astype(str), quality_checks["value"]))

        self.assertEqual(str(overview_metrics["persona_readiness_state"]), "reviewable_but_not_deck_ready")
        self.assertEqual(str(overview_metrics["overall_status"]), "WARN")
        self.assertEqual(str(overview_metrics["quality_flag"]), "EXPLORATORY")
        self.assertEqual(int(float(overview_metrics["final_usable_persona_count"])), 3)
        self.assertEqual(int(float(overview_metrics["production_ready_persona_count"])), 3)
        self.assertEqual(int(float(overview_metrics["review_ready_persona_count"])), 1)
        self.assertEqual(int(float(overview_metrics["weak_source_cost_center_count"])), 4)
        self.assertEqual(int(float(quality_metrics["core_readiness_weak_source_cost_center_count"])), 3)


if __name__ == "__main__":
    unittest.main()
