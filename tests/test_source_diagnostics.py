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

            pct_rows = diagnostics_df[diagnostics_df["metric_type"] == "percentage"]
            self.assertTrue(((pct_rows["metric_value"].astype(float) >= 0.0) & (pct_rows["metric_value"].astype(float) <= 100.0)).all())

    def test_build_source_stage_counts_rejects_non_monotonic_post_or_episode_funnels(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_jsonl(root / "data" / "raw" / "reddit" / "page_001.jsonl", [{"id": "r1"}])
            write_parquet(pd.DataFrame({"source": ["reddit", "reddit"]}), root / "data" / "valid" / "valid_candidates_prefiltered.parquet")
            write_parquet(pd.DataFrame({"episode_id": ["e1"], "labelability_status": ["labelable"]}), root / "data" / "labeled" / "labelability_audit.parquet")
            write_parquet(pd.DataFrame(), root / "data" / "prefilter" / "relevance_drop.parquet")
            write_parquet(pd.DataFrame(), root / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")

            with self.assertRaisesRegex(ValueError, "prefiltered_valid_post_count cannot exceed valid_post_count"):
                build_source_stage_counts(
                    root_dir=root,
                    normalized_df=pd.DataFrame({"source": ["reddit"]}),
                    valid_df=pd.DataFrame({"source": ["reddit"]}),
                    episodes_df=pd.DataFrame({"episode_id": ["e1"], "source": ["reddit"]}),
                    labeled_df=pd.DataFrame({"episode_id": ["e1"]}),
                    persona_assignments_df=pd.DataFrame(),
                    cluster_stats_df=pd.DataFrame(),
                )

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
        self.assertEqual(str(weak_row["policy_action"]), "tune_source_seeds_and_prefilter_rules")

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
