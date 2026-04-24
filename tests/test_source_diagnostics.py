"""Tests for source_diagnostics grain separation and invariants."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.analysis.diagnostics import build_source_balance_audit, build_source_diagnostics, build_source_stage_counts
from src.analysis.workbook_bundle import validate_workbook_frames
from src.utils.io import write_jsonl, write_parquet


class SourceDiagnosticsTests(unittest.TestCase):
    """Verify source diagnostics uses explicit grains and same-grain funnels only."""

    def _write_source_configs(self, root: Path, *sources: str, disabled_sources: set[str] | None = None) -> None:
        """Write minimal source configs for source-diagnostic tests."""
        disabled_sources = disabled_sources or set()
        config_dir = root / "config" / "sources"
        config_dir.mkdir(parents=True, exist_ok=True)
        for source in sources:
            source_group = "reddit" if source.startswith("reddit") else "business_communities"
            enabled = "false" if source in disabled_sources else "true"
            (config_dir / f"{source}.yaml").write_text(
                f"source_id: {source}\nsource_group: {source_group}\nenabled: {enabled}\n",
                encoding="utf-8",
            )

    def _write_source_fixture(
        self,
        root: Path,
        *,
        raw_counts: dict[str, int],
        prefiltered_sources: list[str],
        labelability_rows: list[dict[str, str]],
        relevance_drop_rows: list[dict[str, str]] | None = None,
        invalid_rows: list[dict[str, str]] | None = None,
    ) -> None:
        self._write_source_configs(root, *raw_counts.keys())
        for source, count in raw_counts.items():
            write_jsonl(
                root / "data" / "raw" / source / "page_001.jsonl",
                [{"id": f"{source}_{index}"} for index in range(count)],
            )
        write_parquet(pd.DataFrame({"source": prefiltered_sources}), root / "data" / "valid" / "valid_candidates_prefiltered.parquet")
        write_parquet(pd.DataFrame(labelability_rows), root / "data" / "labeled" / "labelability_audit.parquet")
        write_parquet(pd.DataFrame(relevance_drop_rows or []), root / "data" / "prefilter" / "relevance_drop.parquet")
        write_parquet(pd.DataFrame(invalid_rows or []), root / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")

    def test_build_source_diagnostics_separates_post_episode_and_bridge_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_configs(root, "reddit")
            write_jsonl(
                root / "data" / "raw" / "reddit" / "page_001.jsonl",
                [{"id": "r1"}, {"id": "r2"}],
            )
            write_parquet(pd.DataFrame({"source": ["reddit"]}), root / "data" / "valid" / "valid_candidates_prefiltered.parquet")
            write_parquet(
                pd.DataFrame(
                    {
                        "episode_id": ["e1", "e2"],
                        "labelability_status": ["labelable", "low_signal"],
                    }
                ),
                root / "data" / "labeled" / "labelability_audit.parquet",
            )
            write_parquet(pd.DataFrame(), root / "data" / "prefilter" / "relevance_drop.parquet")
            write_parquet(pd.DataFrame(), root / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")

            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit", "reddit"]}),
                valid_df=pd.DataFrame({"source": ["reddit"]}),
                episodes_df=pd.DataFrame({"episode_id": ["e1", "e2"], "source": ["reddit", "reddit"]}),
                labeled_df=pd.DataFrame({"episode_id": ["e1", "e2"]}),
                persona_assignments_df=pd.DataFrame({"episode_id": ["e1"], "persona_id": ["persona_01"]}),
                cluster_stats_df=pd.DataFrame({"persona_id": ["persona_01"], "promotion_status": ["promoted_persona"]}),
            )
            diagnostics_df = build_source_diagnostics(stage_counts_df)

            self.assertEqual(int(stage_counts_df.loc[0, "raw_record_count"]), 2)
            self.assertEqual(int(stage_counts_df.loc[0, "normalized_post_count"]), 2)
            self.assertEqual(int(stage_counts_df.loc[0, "valid_post_count"]), 1)
            self.assertEqual(int(stage_counts_df.loc[0, "prefiltered_valid_post_count"]), 1)
            self.assertEqual(int(stage_counts_df.loc[0, "episode_count"]), 2)
            self.assertEqual(int(stage_counts_df.loc[0, "labeled_episode_count"]), 2)
            self.assertEqual(int(stage_counts_df.loc[0, "labelable_episode_count"]), 1)

            metric_names = set(diagnostics_df["metric_name"].astype(str).tolist())
            self.assertIn("valid_posts_per_normalized_post_pct", metric_names)
            self.assertIn("labelable_episodes_per_labeled_episode_pct", metric_names)
            self.assertIn("episodes_per_prefiltered_valid_post", metric_names)
            self.assertNotIn("episode_survival_rate", metric_names)

            bridge_row = diagnostics_df[diagnostics_df["metric_name"] == "episodes_per_prefiltered_valid_post"].iloc[0]
            self.assertEqual(str(bridge_row["section"]), "cross_grain_bridge")
            self.assertEqual(str(bridge_row["row_kind"]), "metric")
            self.assertEqual(str(bridge_row["grain"]), "mixed_grain_bridge")
            self.assertEqual(float(bridge_row["metric_value"]), 2.0)

            top_reason_row = diagnostics_df[diagnostics_df["metric_name"] == "top_failure_reason"].iloc[0]
            self.assertEqual(str(top_reason_row["row_kind"]), "diagnostic")
            self.assertEqual(str(top_reason_row["section"]), "diagnostic_reasons")
            self.assertIn("priority_tier", diagnostics_df.columns)
            self.assertIn("primary_collapse_stage", diagnostics_df.columns)
            self.assertIn("recommended_action", diagnostics_df.columns)
            self.assertIn("false_negative_hint", diagnostics_df.columns)
            self.assertIn("source_specific_next_check", diagnostics_df.columns)
            self.assertIn("severity", diagnostics_df.columns)

            pct_rows = diagnostics_df[diagnostics_df["metric_type"] == "percentage"]
            self.assertTrue(((pct_rows["metric_value"].astype(float) >= 0.0) & (pct_rows["metric_value"].astype(float) <= 100.0)).all())

    def test_build_source_stage_counts_excludes_stale_or_disabled_sources_from_workbook_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_configs(root, "reddit", "ga4_help_community", "amplitude_community", disabled_sources={"amplitude_community"})
            self._write_source_fixture(
                root,
                raw_counts={"reddit": 2, "ga4_help_community": 2, "amplitude_community": 2},
                prefiltered_sources=["reddit"],
                labelability_rows=[{"episode_id": "e1", "labelability_status": "labelable"}],
            )

            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit"]}),
                valid_df=pd.DataFrame({"source": ["reddit"]}),
                episodes_df=pd.DataFrame({"episode_id": ["e1"], "source": ["reddit"]}),
                labeled_df=pd.DataFrame({"episode_id": ["e1"]}),
                persona_assignments_df=pd.DataFrame({"episode_id": ["e1"], "persona_id": ["persona_01"]}),
                cluster_stats_df=pd.DataFrame({"persona_id": ["persona_01"], "promotion_status": ["promoted_persona"]}),
            )

            self.assertEqual(stage_counts_df["source"].astype(str).tolist(), ["reddit"])

    def test_build_source_stage_counts_repairs_source_fidelity_gaps_before_validating_monotonicity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_configs(root, "reddit")
            write_jsonl(root / "data" / "raw" / "reddit" / "page_001.jsonl", [{"id": "r1"}])
            write_parquet(pd.DataFrame({"source": ["reddit", "reddit"]}), root / "data" / "valid" / "valid_candidates_prefiltered.parquet")
            write_parquet(pd.DataFrame({"episode_id": ["e1"], "labelability_status": ["labelable"]}), root / "data" / "labeled" / "labelability_audit.parquet")
            write_parquet(pd.DataFrame(), root / "data" / "prefilter" / "relevance_drop.parquet")
            write_parquet(pd.DataFrame(), root / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")
            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit"]}),
                valid_df=pd.DataFrame({"source": ["reddit"]}),
                episodes_df=pd.DataFrame({"episode_id": ["e1"], "source": ["reddit"]}),
                labeled_df=pd.DataFrame({"episode_id": ["e1"]}),
                persona_assignments_df=pd.DataFrame(),
                cluster_stats_df=pd.DataFrame(),
            )
            row = stage_counts_df.iloc[0]
            self.assertEqual(int(row["prefiltered_valid_post_count"]), 2)
            self.assertEqual(int(row["valid_post_count"]), 2)
            self.assertEqual(int(row["normalized_post_count"]), 2)
            self.assertEqual(int(row["raw_record_count"]), 2)

    def test_low_prefilter_retention_overrides_generic_source_reason_and_exposes_seed_intervention(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_fixture(
                root,
                raw_counts={"reddit": 10},
                prefiltered_sources=["reddit"],
                labelability_rows=[
                    {"episode_id": "e1", "labelability_status": "labelable"},
                    {"episode_id": "e2", "labelability_status": "labelable"},
                ],
                relevance_drop_rows=[{"source": "reddit", "prefilter_reason": "off_topic_query_match"}],
            )
            seed_dir = root / "config" / "seeds" / "reddit"
            seed_dir.mkdir(parents=True, exist_ok=True)
            (seed_dir / "reddit.yaml").write_text(
                "active_core_seeds:\n  - finance dashboard\n  - kpi reporting\n",
                encoding="utf-8",
            )

            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit"] * 10}),
                valid_df=pd.DataFrame({"source": ["reddit"] * 10}),
                episodes_df=pd.DataFrame({"episode_id": ["e1", "e2"], "source": ["reddit", "reddit"]}),
                labeled_df=pd.DataFrame({"episode_id": ["e1", "e2"]}),
                persona_assignments_df=pd.DataFrame({"episode_id": ["e1"], "persona_id": ["persona_01"]}),
                cluster_stats_df=pd.DataFrame({"persona_id": ["persona_01"], "promotion_status": ["promoted_persona"]}),
            )

            row = stage_counts_df.iloc[0]
            self.assertEqual(str(row["failure_reason_top"]), "low_prefilter_retention: off_topic_query_match")
            self.assertEqual(str(row["failure_level"]), "warning")
            self.assertIn("finance dashboard", str(row["recommended_seed_set"]))

            diagnostics_df = build_source_diagnostics(stage_counts_df)
            top_reason_row = diagnostics_df[diagnostics_df["metric_name"] == "top_failure_reason"].iloc[0]
            self.assertEqual(str(top_reason_row["metric_value"]), "low_prefilter_retention: off_topic_query_match")
            self.assertEqual(str(top_reason_row["diagnostic_level"]), "warning")
            self.assertFalse(diagnostics_df["metric_value"].astype(str).eq("labeled_output_present").any())
            intervention_row = diagnostics_df[diagnostics_df["metric_name"] == "recommended_seed_intervention"].iloc[0]
            self.assertIn("finance dashboard", str(intervention_row["metric_value"]))

    def test_recommended_seed_intervention_finds_discourse_seed_banks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_fixture(
                root,
                raw_counts={"google_developer_forums": 10},
                prefiltered_sources=["google_developer_forums"],
                labelability_rows=[{"episode_id": "e1", "labelability_status": "labelable"}],
                relevance_drop_rows=[{"source": "google_developer_forums", "prefilter_reason": "google_developer_forums:generic"}],
            )
            seed_dir = root / "config" / "seeds" / "discourse"
            seed_dir.mkdir(parents=True, exist_ok=True)
            (seed_dir / "google_developer_forums.yaml").write_text(
                "active_core_seeds:\n  - seed: scorecard total doesn't match\n  - seed: scheduled report failed\n",
                encoding="utf-8",
            )
            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["google_developer_forums"] * 10}),
                valid_df=pd.DataFrame({"source": ["google_developer_forums"] * 10}),
                episodes_df=pd.DataFrame({"episode_id": ["e1"], "source": ["google_developer_forums"]}),
                labeled_df=pd.DataFrame({"episode_id": ["e1"]}),
                persona_assignments_df=pd.DataFrame(),
                cluster_stats_df=pd.DataFrame(),
            )
            self.assertIn("scorecard total doesn't match", str(stage_counts_df.iloc[0]["recommended_seed_set"]))

    def test_low_episode_yield_overrides_generic_source_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_fixture(
                root,
                raw_counts={"reddit": 10},
                prefiltered_sources=["reddit"] * 10,
                labelability_rows=[{"episode_id": "e1", "labelability_status": "labelable"}],
            )

            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit"] * 10}),
                valid_df=pd.DataFrame({"source": ["reddit"] * 10}),
                episodes_df=pd.DataFrame({"episode_id": ["e1"], "source": ["reddit"]}),
                labeled_df=pd.DataFrame({"episode_id": ["e1"]}),
                persona_assignments_df=pd.DataFrame({"episode_id": ["e1"], "persona_id": ["persona_01"]}),
                cluster_stats_df=pd.DataFrame({"persona_id": ["persona_01"], "promotion_status": ["promoted_persona"]}),
            )

            row = stage_counts_df.iloc[0]
            self.assertEqual(str(row["failure_reason_top"]), "low_episode_yield")
            self.assertEqual(str(row["failure_level"]), "failure")

            diagnostics_df = build_source_diagnostics(stage_counts_df)
            episode_reason_row = diagnostics_df[diagnostics_df["metric_name"] == "episode_yield_reason"].iloc[0]
            self.assertEqual(str(episode_reason_row["metric_value"]), "low_episode_yield")
            self.assertEqual(str(episode_reason_row["diagnostic_level"]), "failure")

    def test_zero_promoted_contribution_beats_generic_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_fixture(
                root,
                raw_counts={"reddit": 6},
                prefiltered_sources=["reddit"] * 6,
                labelability_rows=[
                    {"episode_id": f"e{index}", "labelability_status": "labelable"}
                    for index in range(1, 7)
                ],
            )

            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit"] * 6}),
                valid_df=pd.DataFrame({"source": ["reddit"] * 6}),
                episodes_df=pd.DataFrame({"episode_id": [f"e{index}" for index in range(1, 7)], "source": ["reddit"] * 6}),
                labeled_df=pd.DataFrame({"episode_id": [f"e{index}" for index in range(1, 7)]}),
                persona_assignments_df=pd.DataFrame(columns=["episode_id", "persona_id"]),
                cluster_stats_df=pd.DataFrame(columns=["persona_id", "promotion_status"]),
            )

            row = stage_counts_df.iloc[0]
            self.assertEqual(str(row["failure_reason_top"]), "grounding_contribution_absent")
            self.assertEqual(str(row["failure_level"]), "warning")
            self.assertEqual(str(row["recommended_seed_set"]), "")

            diagnostics_df = build_source_diagnostics(stage_counts_df)
            grounding_row = diagnostics_df[diagnostics_df["metric_name"] == "grounding_contribution_reason"].iloc[0]
            self.assertEqual(str(grounding_row["metric_value"]), "grounding_contribution_absent")
            self.assertEqual(str(grounding_row["diagnostic_level"]), "warning")

    def test_review_visible_only_contribution_is_not_treated_as_grounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_fixture(
                root,
                raw_counts={"reddit": 4},
                prefiltered_sources=["reddit"] * 4,
                labelability_rows=[
                    {"episode_id": f"e{index}", "labelability_status": "labelable"}
                    for index in range(1, 5)
                ],
            )

            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit"] * 4}),
                valid_df=pd.DataFrame({"source": ["reddit"] * 4}),
                episodes_df=pd.DataFrame({"episode_id": [f"e{index}" for index in range(1, 5)], "source": ["reddit"] * 4}),
                labeled_df=pd.DataFrame({"episode_id": [f"e{index}" for index in range(1, 5)]}),
                persona_assignments_df=pd.DataFrame({"episode_id": ["e1", "e2"], "persona_id": ["persona_01", "persona_01"]}),
                cluster_stats_df=pd.DataFrame(
                    {
                        "persona_id": ["persona_01"],
                        "promotion_status": ["promoted_persona"],
                        "final_usable_persona": [False],
                    }
                ),
            )

            row = stage_counts_df.iloc[0]
            self.assertEqual(int(row["promoted_persona_episode_count"]), 2)
            self.assertEqual(int(row["grounded_promoted_persona_episode_count"]), 0)
            self.assertEqual(str(row["grounding_contribution_reason"]), "grounded_persona_contribution_absent")

    def test_source_balance_audit_flags_dominant_and_weak_sources(self) -> None:
        stage_counts_df = pd.DataFrame(
            [
                {
                    "source": "metabase_discussions",
                    "raw_record_count": 300,
                    "normalized_post_count": 300,
                    "valid_post_count": 210,
                    "prefiltered_valid_post_count": 120,
                    "episode_count": 60,
                    "labelable_episode_count": 50,
                    "labeled_episode_count": 40,
                    "promoted_persona_episode_count": 32,
                    "grounded_promoted_persona_episode_count": 18,
                    "failure_reason_top": "overconcentration_risk",
                    "failure_level": "warning",
                    "concentration_risk_reason": "overconcentration_risk",
                },
                {
                    "source": "reddit",
                    "raw_record_count": 180,
                    "normalized_post_count": 180,
                    "valid_post_count": 120,
                    "prefiltered_valid_post_count": 8,
                    "episode_count": 3,
                    "labelable_episode_count": 3,
                    "labeled_episode_count": 2,
                    "promoted_persona_episode_count": 1,
                    "grounded_promoted_persona_episode_count": 0,
                    "failure_reason_top": "low_prefilter_retention: off_topic_query_match",
                    "failure_level": "warning",
                    "concentration_risk_reason": "concentration_risk_clear",
                },
            ]
        )

        audit_df = build_source_balance_audit(stage_counts_df)

        dominant_row = audit_df.loc[audit_df["source"] == "metabase_discussions"].iloc[0]
        weak_row = audit_df.loc[audit_df["source"] == "reddit"].iloc[0]
        self.assertEqual(str(dominant_row["source_balance_status"]), "overdominant_source_risk")
        self.assertEqual(str(dominant_row["policy_action"]), "diversify_other_sources_before_scaling_this_source")
        self.assertEqual(str(weak_row["collapse_stage"]), "relevance_prefilter")
        self.assertTrue(bool(weak_row["weak_source_cost_center"]))
        self.assertEqual(str(weak_row["policy_action"]), "review_source_specific_prefilter_terms")

    def test_time_window_dominant_invalid_reason_maps_to_time_window_action(self) -> None:
        stage_counts_df = pd.DataFrame(
            [
                {
                    "source": "domo_community_forum",
                    "raw_record_count": 900,
                    "normalized_post_count": 900,
                    "valid_post_count": 120,
                    "prefiltered_valid_post_count": 100,
                    "episode_count": 80,
                    "labelable_episode_count": 80,
                    "labeled_episode_count": 80,
                    "promoted_persona_episode_count": 10,
                    "grounded_promoted_persona_episode_count": 4,
                    "dominant_invalid_reason": "outside_time_window",
                    "dominant_prefilter_reason": "domo_reporting_or_etl_workflow",
                    "valid_retention_reason": "low_valid_post_retention: outside_time_window",
                    "valid_retention_level": "warning",
                    "prefilter_retention_reason": "healthy_prefilter_retention",
                    "prefilter_retention_level": "pass",
                    "episode_yield_reason": "healthy_episode_yield",
                    "episode_yield_level": "pass",
                    "labelable_coverage_reason": "healthy_labelable_coverage",
                    "labelable_coverage_level": "pass",
                    "grounding_contribution_reason": "healthy_grounding_contribution",
                    "grounding_contribution_level": "pass",
                    "concentration_risk_reason": "concentration_risk_clear",
                    "concentration_risk_level": "pass",
                    "diversity_contribution_reason": "strong_diversity_contribution",
                    "diversity_contribution_level": "pass",
                    "failure_reason_top": "low_valid_post_retention: outside_time_window",
                    "failure_level": "warning",
                    "recommended_seed_set": "",
                }
            ]
        )

        audit_df = build_source_balance_audit(stage_counts_df)
        row = audit_df.iloc[0]
        self.assertEqual(str(row["collapse_stage"]), "time_window")
        self.assertEqual(str(row["policy_action"]), "review_time_window_policy")

    def test_source_diagnostics_surface_concentration_and_weak_diversity_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_fixture(
                root,
                raw_counts={"reddit": 8, "forums": 2},
                prefiltered_sources=["reddit"] * 8 + ["forums"] * 2,
                labelability_rows=[
                    *[{"episode_id": f"r{index}", "labelability_status": "labelable"} for index in range(1, 9)],
                    *[{"episode_id": f"f{index}", "labelability_status": "labelable"} for index in range(1, 3)],
                ],
            )

            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit"] * 8 + ["forums"] * 2}),
                valid_df=pd.DataFrame({"source": ["reddit"] * 8 + ["forums"] * 2}),
                episodes_df=pd.DataFrame(
                    {
                        "episode_id": [*[f"r{index}" for index in range(1, 9)], *[f"f{index}" for index in range(1, 3)]],
                        "source": ["reddit"] * 8 + ["forums"] * 2,
                    }
                ),
                labeled_df=pd.DataFrame({"episode_id": [*[f"r{index}" for index in range(1, 9)], *[f"f{index}" for index in range(1, 3)]]}),
                persona_assignments_df=pd.DataFrame(
                    {
                        "episode_id": [*[f"r{index}" for index in range(1, 7)], "f1"],
                        "persona_id": ["persona_01"] * 6 + ["persona_02"],
                    }
                ),
                cluster_stats_df=pd.DataFrame(
                    {
                        "persona_id": ["persona_01", "persona_02"],
                        "promotion_status": ["promoted_persona", "promoted_persona"],
                    }
                ),
            )

            reddit_row = stage_counts_df.loc[stage_counts_df["source"] == "reddit"].iloc[0]
            forums_row = stage_counts_df.loc[stage_counts_df["source"] == "forums"].iloc[0]
            self.assertEqual(str(reddit_row["failure_reason_top"]), "overconcentration_risk")
            self.assertEqual(str(reddit_row["failure_level"]), "failure")
            self.assertEqual(str(forums_row["failure_reason_top"]), "weak_diversity_contribution")
            self.assertEqual(str(forums_row["failure_level"]), "warning")

            diagnostics_df = build_source_diagnostics(stage_counts_df)
            reddit_concentration = diagnostics_df[
                (diagnostics_df["source"] == "reddit")
                & (diagnostics_df["metric_name"] == "concentration_risk_reason")
            ].iloc[0]
            self.assertEqual(str(reddit_concentration["metric_value"]), "overconcentration_risk")
            self.assertEqual(str(reddit_concentration["diagnostic_level"]), "failure")

    def test_source_diagnostics_adds_single_priority_action_layer_per_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_source_fixture(
                root,
                raw_counts={"reddit": 12},
                prefiltered_sources=["reddit"],
                labelability_rows=[{"episode_id": "r1", "labelability_status": "labelable"}],
            )
            stage_counts_df = build_source_stage_counts(
                root_dir=root,
                normalized_df=pd.DataFrame({"source": ["reddit"] * 12}),
                valid_df=pd.DataFrame({"source": ["reddit"] * 12}),
                episodes_df=pd.DataFrame({"episode_id": ["r1"], "source": ["reddit"]}),
                labeled_df=pd.DataFrame({"episode_id": ["r1"]}),
                persona_assignments_df=pd.DataFrame({"episode_id": ["r1"], "persona_id": ["persona_01"]}),
                cluster_stats_df=pd.DataFrame({"persona_id": ["persona_01"], "promotion_status": ["promoted_persona"]}),
            )
            diagnostics_df = build_source_diagnostics(stage_counts_df)

            for metric_name in ["priority_tier", "primary_collapse_stage", "recommended_action", "false_negative_hint", "source_specific_next_check", "severity"]:
                rows = diagnostics_df[diagnostics_df["metric_name"].astype(str).eq(metric_name)]
                self.assertEqual(len(rows), 1)
            self.assertEqual(
                str(diagnostics_df.loc[diagnostics_df["metric_name"].astype(str).eq("recommended_action"), "row_kind"].iloc[0]),
                "diagnostic",
            )

    def test_source_balance_audit_and_diagnostics_expose_source_specific_hints(self) -> None:
        stage_counts_df = pd.DataFrame(
            [
                {
                    "source": "google_developer_forums",
                    "raw_record_count": 1200,
                    "normalized_post_count": 1200,
                    "valid_post_count": 400,
                    "prefiltered_valid_post_count": 100,
                    "episode_count": 80,
                    "labelable_episode_count": 20,
                    "labeled_episode_count": 80,
                    "promoted_persona_episode_count": 10,
                    "grounded_promoted_persona_episode_count": 4,
                    "dominant_invalid_reason": "missing_pain_signal",
                    "dominant_prefilter_reason": "google_developer_forums:generic",
                    "valid_retention_reason": "low_valid_post_retention: missing_pain_signal",
                    "valid_retention_level": "warning",
                    "prefilter_retention_reason": "healthy_prefilter_retention",
                    "prefilter_retention_level": "pass",
                    "episode_yield_reason": "healthy_episode_yield",
                    "episode_yield_level": "pass",
                    "labelable_coverage_reason": "low_labelable_episode_ratio",
                    "labelable_coverage_level": "warning",
                    "grounding_contribution_reason": "healthy_grounding_contribution",
                    "grounding_contribution_level": "pass",
                    "concentration_risk_reason": "concentration_risk_clear",
                    "concentration_risk_level": "pass",
                    "diversity_contribution_reason": "weak_diversity_contribution",
                    "diversity_contribution_level": "warning",
                    "failure_reason_top": "low_valid_post_retention: missing_pain_signal",
                    "failure_level": "warning",
                    "recommended_seed_set": "",
                }
            ]
        )

        audit_df = build_source_balance_audit(stage_counts_df)
        row = audit_df.iloc[0]
        self.assertIn("scheduled report", str(row["false_negative_hint"]).lower())
        self.assertIn("invalid_candidates", str(row["source_specific_next_check"]).lower())

        diagnostics_df = build_source_diagnostics(stage_counts_df)
        hint_row = diagnostics_df[diagnostics_df["metric_name"].astype(str).eq("false_negative_hint")].iloc[0]
        next_check_row = diagnostics_df[diagnostics_df["metric_name"].astype(str).eq("source_specific_next_check")].iloc[0]
        self.assertIn("scheduled report", str(hint_row["metric_value"]).lower())
        self.assertIn("invalid_candidates", str(next_check_row["metric_value"]).lower())

    def test_validate_workbook_frames_rejects_mixed_grain_rate_names(self) -> None:
        frames = {
            "overview": pd.DataFrame({"metric": ["x"], "value": ["y"]}),
            "counts": pd.DataFrame({"metric": ["raw_record_rows"], "count": [1]}),
            "source_distribution": pd.DataFrame(),
            "taxonomy_summary": pd.DataFrame(),
            "cluster_stats": pd.DataFrame(),
            "persona_summary": pd.DataFrame(),
            "persona_axes": pd.DataFrame(),
            "persona_needs": pd.DataFrame(),
            "persona_cooccurrence": pd.DataFrame(),
            "persona_examples": pd.DataFrame(),
            "quality_checks": pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"]}),
            "source_diagnostics": pd.DataFrame(
                {
                    "source": ["reddit"],
                    "section": ["cross_grain_bridge"],
                    "row_kind": ["metric"],
                    "grain": ["mixed_grain_bridge"],
                    "metric_name": ["episode_survival_rate"],
                    "metric_value": [1.24],
                    "metric_type": ["ratio"],
                    "diagnostic_level": [""],
                    "metric_definition": ["bad legacy metric"],
                }
            ),
            "quality_failures": pd.DataFrame(),
            "metric_glossary": pd.DataFrame(),
        }
        messages = validate_workbook_frames(frames)
        self.assertIn("mixed-grain metric mislabeled as rate: source_diagnostics.episode_survival_rate", messages)


if __name__ == "__main__":
    unittest.main()
