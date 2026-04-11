"""Tests for source_diagnostics grain separation and invariants."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.analysis.diagnostics import build_source_diagnostics, build_source_stage_counts
from src.analysis.workbook_bundle import validate_workbook_frames
from src.utils.io import write_jsonl, write_parquet


class SourceDiagnosticsTests(unittest.TestCase):
    """Verify source diagnostics uses explicit grains and same-grain funnels only."""

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
            self.assertEqual(str(bridge_row["grain"]), "mixed_grain_bridge")
            self.assertEqual(float(bridge_row["metric_value"]), 2.0)

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

    def test_validate_workbook_frames_rejects_mixed_grain_rate_names(self) -> None:
        frames = {
            "overview": pd.DataFrame({"metric": ["x"], "value": ["y"]}),
            "counts": pd.DataFrame({"metric": ["raw_records"], "count": [1]}),
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
                    "grain": ["mixed_grain_bridge"],
                    "metric_name": ["episode_survival_rate"],
                    "metric_value": [1.24],
                    "metric_type": ["ratio"],
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
