"""Regression tests for the no-xlsx analysis validation snapshot CLI."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.utils.pipeline_schema import WORKBOOK_SHEET_NAMES


CLI_PATH = Path(__file__).resolve().parents[1] / "run" / "cli" / "17_analysis_snapshot.py"
CLI_SPEC = importlib.util.spec_from_file_location("analysis_snapshot_cli", CLI_PATH)
if CLI_SPEC is None or CLI_SPEC.loader is None:
    raise RuntimeError("Unable to load analysis snapshot CLI for tests.")
analysis_snapshot_cli = importlib.util.module_from_spec(CLI_SPEC)
CLI_SPEC.loader.exec_module(analysis_snapshot_cli)


class AnalysisSnapshotCliTests(unittest.TestCase):
    """Verify bundle-based validation snapshots work without xlsx export."""

    def test_build_validation_snapshot_reads_bundle_without_xlsx(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_minimal_fixture(root)

            snapshot = analysis_snapshot_cli.build_validation_snapshot(root)

            self.assertFalse(snapshot["uses_xlsx_export"])
            self.assertEqual(snapshot["overview_metrics"]["overall_status"], "WARN")
            self.assertEqual(snapshot["overview_metrics"]["final_usable_persona_count"], 3)
            self.assertEqual(len(snapshot["source_balance"]), 2)
            self.assertEqual(snapshot["source_balance"][0]["source"], "adobe_analytics_community")
            self.assertIn("root_cause_category", snapshot["source_balance"][0])
            self.assertIn("recommended_config_change", snapshot["source_balance"][0])
            self.assertEqual(len(snapshot["promoted_personas"]), 2)

    def test_build_validation_snapshot_requires_canonical_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            with self.assertRaises(FileNotFoundError):
                analysis_snapshot_cli.build_validation_snapshot(root)

    def test_validation_delta_flags_regressions_and_improvements(self) -> None:
        baseline = {
            "overview_metrics": {
                "weak_source_cost_center_count": 2,
                "final_usable_persona_count": 3,
                "top_3_cluster_share_of_core_labeled": 0.78,
                "largest_source_influence_share_pct": 24.0,
                "persona_core_coverage_of_all_labeled_pct": 88.0,
                "overall_unknown_ratio": 0.14,
            },
            "source_balance": [
                {
                    "source": "klaviyo_community",
                    "prefiltered_valid_post_count": 220,
                    "episode_count": 130,
                    "labelable_episode_count": 110,
                    "labeled_episode_count": 130,
                }
            ],
        }
        current = {
            "overview_metrics": {
                "weak_source_cost_center_count": 4,
                "final_usable_persona_count": 3,
                "top_3_cluster_share_of_core_labeled": 0.75,
                "largest_source_influence_share_pct": 26.0,
                "persona_core_coverage_of_all_labeled_pct": 90.0,
                "overall_unknown_ratio": 0.13,
            },
            "source_balance": [
                {
                    "source": "klaviyo_community",
                    "prefiltered_valid_post_count": 250,
                    "episode_count": 140,
                    "labelable_episode_count": 120,
                    "labeled_episode_count": 140,
                }
            ],
        }

        delta = analysis_snapshot_cli.build_validation_delta(current, baseline)

        self.assertTrue(delta["summary"]["regressed"])
        self.assertTrue(delta["summary"]["improved"])
        self.assertEqual(delta["overview_metric_deltas"]["weak_source_cost_center_count"]["classification"], "regressed")
        self.assertEqual(delta["overview_metric_deltas"]["top_3_cluster_share_of_core_labeled"]["classification"], "improved")
        self.assertEqual(delta["source_deltas"][0]["changes"]["prefiltered_valid_post_count"]["classification"], "improved")

    def _write_minimal_fixture(self, root: Path) -> None:
        analysis_dir = root / "data" / "analysis"
        workbook_dir = analysis_dir / "workbook_bundle"
        workbook_dir.mkdir(parents=True, exist_ok=True)
        (root / "data" / "labeled").mkdir(parents=True, exist_ok=True)

        overview_df = pd.DataFrame(
            {
                "metric": [
                    "persona_readiness_state",
                    "persona_readiness_label",
                    "overall_status",
                    "quality_flag",
                    "effective_balanced_source_count",
                    "weak_source_cost_center_count",
                    "weak_source_cost_centers",
                    "final_usable_persona_count",
                    "top_3_cluster_share_of_core_labeled",
                    "largest_source_influence_share_pct",
                    "persona_core_coverage_of_all_labeled_pct",
                    "overall_unknown_ratio",
                    "promoted_persona_example_coverage_pct",
                    "labeled_episode_rows",
                ],
                "value": [
                    "reviewable_but_not_deck_ready",
                    "Reviewable Draft",
                    "WARN",
                    "EXPLORATORY",
                    "6.2",
                    "2",
                    "adobe_analytics_community | klaviyo_community",
                    "3",
                    "0.79",
                    "24.5",
                    "88.4",
                    "0.12",
                    "100.0",
                    "120",
                ],
            }
        )
        quality_checks_df = pd.DataFrame(
            {
                "metric": ["persona_core_labeled_rows", "persona_core_unknown_ratio", "overall_unknown_ratio", "largest_cluster_share_of_core_labeled"],
                "value": [90, 0.05, 0.12, 48.0],
                "denominator_type": ["persona_core_labeled_rows", "persona_core_labeled_rows", "labeled_episode_rows", "persona_core_labeled_rows"],
                "denominator_value": [90, 90, 120, 90],
                "threshold": ["", "", "", ""],
                "status": ["pass", "pass", "pass", "warn"],
                "level": ["pass", "pass", "pass", "warning"],
                "notes": ["", "", "", ""],
            }
        )
        cluster_stats_df = pd.DataFrame(
            {
                "persona_id": ["persona_01", "persona_02"],
                "persona_size": [40, 24],
                "promotion_status": ["promoted_persona", "promoted_persona"],
            }
        )
        source_diagnostics_df = pd.DataFrame(
            {
                "source": ["adobe_analytics_community", "adobe_analytics_community", "klaviyo_community", "klaviyo_community"],
                "section": ["diagnostic_reasons"] * 4,
                "row_kind": ["diagnostic"] * 4,
                "grain": ["source"] * 4,
                "metric_name": ["priority_tier", "policy_action", "priority_tier", "policy_action"],
                "metric_value": ["fix_now", "tighten_episode_segmentation_for_source", "fix_now", "review_source_specific_prefilter_terms"],
                "metric_type": ["label"] * 4,
                "diagnostic_level": ["warning"] * 4,
                "metric_definition": ["", "", "", ""],
            }
        )
        persona_examples_df = pd.DataFrame({"persona_id": ["persona_01", "persona_02"]})

        empty = pd.DataFrame()
        bundle_frames = {
            "overview": overview_df,
            "counts": pd.DataFrame({"metric": ["raw_record_rows"], "count": [200]}),
            "source_distribution": empty,
            "taxonomy_summary": empty,
            "cluster_stats": cluster_stats_df,
            "persona_summary": empty,
            "persona_axes": empty,
            "persona_needs": empty,
            "persona_cooccurrence": empty,
            "persona_examples": persona_examples_df,
            "quality_checks": quality_checks_df,
            "source_diagnostics": source_diagnostics_df,
            "quality_failures": empty,
            "metric_glossary": empty,
        }
        for sheet in WORKBOOK_SHEET_NAMES:
            bundle_frames.get(sheet, empty).to_parquet(workbook_dir / f"{sheet}.parquet", index=False)

        pd.DataFrame(
            {
                "source": ["adobe_analytics_community", "klaviyo_community"],
                "valid_post_count": [2200, 6200],
                "prefiltered_valid_post_count": [980, 184],
                "episode_count": [680, 105],
                "labelable_episode_count": [230, 99],
                "labeled_episode_count": [680, 105],
                "collapse_stage": ["episode_yield", "relevance_prefilter"],
                "failure_reason_top": ["low_episode_yield", "low_prefilter_retention: klaviyo_community:generic"],
                "priority_tier": ["fix_now", "fix_now"],
                "policy_action": ["tighten_episode_segmentation_for_source", "review_source_specific_prefilter_terms"],
                "false_negative_hint": ["review adobe workspace multi-domain threads", "review reporting-trust false negatives"],
                "source_specific_next_check": ["sample episode splits", "sample generic drops"],
                "root_cause_category": ["episode_segmentation_under_split", "relevance_prefilter_generic_false_negative"],
                "evidence_to_inspect": ["data/episodes/episode_debug.parquet | data/episodes/episode_audit.parquet", "data/prefilter/relevance_drop.parquet"],
                "likely_false_negative_pattern": ["Adobe Workspace/CJA/report-builder threads may contain multiple analyst problems.", "Reporting-trust rows are being treated as generic product help in relevance prefiltering."],
                "recommended_config_change": ["Tune the Adobe source branch in src/episodes/builder.py.", "Update source-aware rescue terms in src/filters/relevance.py."],
                "required_regression_check": ["Add source-specific episode-builder regressions in tests/test_episode_builder.py.", "Add source-specific positive and negative relevance-prefilter regressions in tests/test_relevance_prefilter.py."],
                "owner_action_type": ["adjust_episode_builder", "adjust_relevance_prefilter_terms"],
                "can_auto_tune": [False, True],
                "must_manual_review": [True, False],
            }
        ).to_csv(analysis_dir / "source_balance_audit.csv", index=False)

        pd.DataFrame(
            {
                "persona_id": ["persona_01", "persona_02", "persona_03"],
                "persona_name": ["Reporting Packager", "Insight Translator", "Validation Reconciler"],
                "promotion_status": ["promoted_persona", "promoted_persona", "exploratory_bucket"],
                "promotion_action": ["remain_promoted", "remain_promoted", "downgraded_to_exploratory"],
                "share_of_core_labeled": [48.0, 24.0, 8.0],
                "grounding_status": ["grounded_single", "grounded_single", "not_applicable"],
                "selected_example_count": [2, 1, 0],
                "cross_source_robustness_score": [0.84, 0.79, 0.7],
                "final_usable_persona": [True, True, False],
            }
        ).to_csv(analysis_dir / "persona_summary.csv", index=False)

        pd.DataFrame(
            {
                "episode_id": ["e1", "e2", "e3"],
                "persona_core_eligible": [True, True, False],
            }
        ).to_parquet(root / "data" / "labeled" / "labeled_episodes.parquet", index=False)
        pd.DataFrame({"labelability_status": ["labelable", "borderline", "low_signal"]}).to_parquet(
            root / "data" / "labeled" / "labelability_audit.parquet",
            index=False,
        )


if __name__ == "__main__":
    unittest.main()
