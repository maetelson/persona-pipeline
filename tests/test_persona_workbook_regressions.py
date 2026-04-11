"""Regression tests for persona workbook generator contracts."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from src.analysis.diagnostics import build_source_diagnostics
from src.analysis.persona_service import _build_cluster_stats_df
from src.analysis.quality_status import build_quality_metrics, evaluate_quality_status
from src.analysis.stage_service import _build_final_overview_df
from src.analysis.workbook_bundle import assemble_workbook_frames, validate_workbook_frames
from src.exporters.xlsx_exporter import export_workbook_from_frames
from src.utils.pipeline_schema import DENOMINATOR_PERSONA_CORE_LABELED_ROWS, WORKBOOK_SHEET_NAMES


class PersonaWorkbookRegressionTests(unittest.TestCase):
    """Verify workbook generator policy contracts do not regress."""

    def test_share_columns_match_stated_denominator(self) -> None:
        frames = assemble_workbook_frames(
            overview_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"]}),
            counts_df=pd.DataFrame({"metric": ["raw_records"], "count": [1]}),
            source_distribution_df=pd.DataFrame({"source": ["reddit"], "share_of_labeled": [100.0]}),
            taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"]}),
            cluster_stats_df=pd.DataFrame(
                {
                    "persona_id": ["persona_01"],
                    "persona_size": [3],
                    "share_of_core_labeled": [75.0],
                    "share_of_all_labeled": [30.0],
                    "denominator_type": [DENOMINATOR_PERSONA_CORE_LABELED_ROWS],
                    "denominator_value": [4],
                    "base_promotion_status": ["promoted_persona"],
                    "promotion_status": ["promoted_persona"],
                    "grounding_status": ["grounded"],
                    "promotion_grounding_status": ["promoted_and_grounded"],
                    "promotion_reason": ["meets floor"],
                    "grounding_reason": ["grounded example exists"],
                    "grounded_candidate_count": [2],
                    "weak_candidate_count": [0],
                    "selected_example_count": [1],
                    "fallback_selected_count": [0],
                    "dominant_signature": ["workflow_stage=reporting"],
                    "dominant_bottleneck": ["manual_reporting"],
                    "dominant_analysis_goal": ["report_speed"],
                }
            ),
            persona_summary_df=pd.DataFrame(
                {
                    "persona_id": ["persona_01"],
                    "persona_name": ["Reporting Operator"],
                    "persona_size": [3],
                    "share_of_core_labeled": [75.0],
                    "share_of_all_labeled": [30.0],
                    "denominator_type": [DENOMINATOR_PERSONA_CORE_LABELED_ROWS],
                    "denominator_value": [4],
                    "min_cluster_size": [2],
                    "base_promotion_status": ["promoted_persona"],
                    "promotion_status": ["promoted_persona"],
                    "grounding_status": ["grounded"],
                    "promotion_grounding_status": ["promoted_and_grounded"],
                    "promotion_reason": ["meets floor"],
                    "grounding_reason": ["grounded example exists"],
                    "grounded_candidate_count": [2],
                    "weak_candidate_count": [0],
                    "selected_example_count": [1],
                    "fallback_selected_count": [0],
                    "one_line_summary": ["summary"],
                    "dominant_bottleneck": ["manual_reporting"],
                    "main_workflow_context": ["reporting"],
                    "analysis_behavior": ["report_speed"],
                    "trust_explanation_need": ["high"],
                    "current_tool_dependency": ["spreadsheet_heavy"],
                    "primary_output_expectation": ["xlsx"],
                    "top_pain_points": ["rework"],
                    "representative_examples": ["example"],
                    "why_this_persona_matters": ["matters"],
                }
            ),
            persona_axes_df=pd.DataFrame(),
            persona_needs_df=pd.DataFrame(),
            persona_cooccurrence_df=pd.DataFrame(),
            persona_examples_df=pd.DataFrame(),
            quality_checks_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"], "threshold": [""], "status": ["pass"], "level": ["pass"], "denominator_type": [""], "denominator_value": [""], "notes": [""]}),
            source_diagnostics_df=pd.DataFrame(),
            quality_failures_df=pd.DataFrame(),
            metric_glossary_df=pd.DataFrame(),
        )

        messages = validate_workbook_frames(frames)
        self.assertFalse(any(message.startswith("share denominator mismatch:") for message in messages))
        self.assertFalse(any(message.startswith("forbidden generic share column:") for message in messages))

    def test_largest_cluster_share_uses_persona_core_denominator(self) -> None:
        persona_source_df = pd.DataFrame(
            {
                "episode_id": ["e1", "e2", "e3", "e4"],
                "persona_id": ["persona_01", "persona_01", "persona_01", "persona_02"],
                "bottleneck_type": ["manual_reporting", "manual_reporting", "manual_reporting", "data_quality"],
                "analysis_goal": ["report_speed", "report_speed", "report_speed", "validate_numbers"],
            }
        )
        cluster_policy = {
            "min_cluster_size": 2,
            "status_by_persona": {
                "persona_01": {"status": "promoted_persona", "reason": "meets floor"},
                "persona_02": {"status": "exploratory_bucket", "reason": "below floor"},
            },
        }

        cluster_stats_df = _build_cluster_stats_df(
            persona_source_df=persona_source_df,
            axis_names=["bottleneck_type", "analysis_goal"],
            total_labeled_records=10,
            persona_core_labeled_records=4,
            cluster_policy=cluster_policy,
        )
        labeled_df = pd.DataFrame(
            {
                "episode_id": [f"l{i}" for i in range(10)],
                "persona_core_eligible": [True, True, True, True, False, False, False, False, False, False],
                "role_codes": ["analyst"] * 10,
                "question_codes": ["reporting"] * 10,
                "pain_codes": ["manual_reporting"] * 10,
                "output_codes": ["O_XLSX"] * 10,
            }
        )

        metrics = build_quality_metrics(
            total_raw_count=10,
            cleaned_count=10,
            labeled_df=labeled_df,
            source_stage_counts_df=pd.DataFrame({"source": ["reddit"], "raw_record_count": [10], "labeled_episode_count": [10]}),
            cluster_stats_df=cluster_stats_df,
            persona_examples_df=pd.DataFrame(),
            cluster_profiles=[],
        )

        self.assertEqual(float(metrics["largest_cluster_share_of_core_labeled"]), 75.0)

    def test_source_diagnostics_keep_same_grain_funnels_bounded_and_bridge_metrics_explicit(self) -> None:
        stage_counts_df = pd.DataFrame(
            {
                "source": ["reddit"],
                "raw_record_count": [5],
                "normalized_post_count": [5],
                "valid_post_count": [4],
                "prefiltered_valid_post_count": [2],
                "episode_count": [3],
                "labeled_episode_count": [3],
                "labelable_episode_count": [2],
                "effective_diversity_contribution": [0.6],
                "promoted_persona_episode_count": [2],
                "failure_reason_top": ["labeled_output_present"],
                "failure_level": ["pass"],
                "recommended_seed_set": [""],
            }
        )

        diagnostics_df = build_source_diagnostics(stage_counts_df)

        bounded_pct = diagnostics_df[diagnostics_df["metric_type"].astype(str).eq("percentage")]
        self.assertTrue(pd.to_numeric(bounded_pct["metric_value"], errors="coerce").between(0.0, 100.0).all())

        bridge_rows = diagnostics_df[diagnostics_df["grain"].astype(str).eq("mixed_grain_bridge")]
        self.assertFalse(bridge_rows["metric_name"].astype(str).str.contains("rate|share|survival", case=False, regex=True).any())
        self.assertTrue(bridge_rows["bounded_range"].astype(str).eq("unbounded_ratio").all())
        self.assertEqual(float(bridge_rows.loc[bridge_rows["metric_name"] == "episodes_per_prefiltered_valid_post", "metric_value"].iloc[0]), 1.5)

    def test_overview_cannot_hide_critical_status_reasons(self) -> None:
        flattened = {
            "overall_status": "FAIL",
            "quality_flag": "UNSTABLE",
            "quality_flag_rule": "UNSTABLE if any axis status is FAIL; EXPLORATORY if no FAIL and any axis status is WARN; otherwise OK.",
            "composite_reason_keys": "overall_unknown_critical | promoted_persona_examples_missing",
            "core_clustering_status": "WARN",
            "source_diversity_status": "OK",
            "example_grounding_status": "FAIL",
            "overall_unknown_status": "FAIL",
            "core_unknown_status": "OK",
            "core_coverage_status": "WARN",
            "effective_source_diversity_status": "OK",
            "source_concentration_status": "OK",
            "largest_cluster_dominance_status": "WARN",
            "grounding_coverage_status": "FAIL",
            "persona_core_coverage_of_all_labeled_pct": 61.2,
            "persona_core_unknown_ratio": 0.069204,
            "overall_unknown_ratio": 0.430085,
            "effective_labeled_source_count": 9.6,
            "largest_cluster_share_of_core_labeled": 55.0,
            "largest_labeled_source_share_pct": 48.7,
            "promoted_persona_example_coverage_pct": 66.7,
            "promoted_persona_grounded_count": 2,
            "promoted_persona_weakly_grounded_count": 0,
            "promoted_persona_ungrounded_count": 1,
            "promoted_personas_weakly_grounded": "",
            "promoted_personas_missing_examples": "persona_02",
            "min_cluster_size": 24,
        }
        overview_df = _build_final_overview_df(
            axis_names=[{"axis_name": "workflow_stage"}, {"axis_name": "analysis_goal"}],
            quality_checks=flattened,
            total_labeled_records=472,
            persona_core_labeled_records=289,
            cluster_stats_df=pd.DataFrame({"promotion_status": ["promoted_persona", "exploratory_bucket"]}),
        )
        lookup = dict(zip(overview_df["metric"], overview_df["value"]))

        self.assertEqual(str(lookup["quality_flag"]), "UNSTABLE")
        self.assertEqual(str(lookup["overall_status"]), "FAIL")
        self.assertIn("promoted_persona_examples_missing", str(lookup["composite_reason_keys"]))
        self.assertEqual(str(lookup["example_grounding_status"]), "FAIL")
        self.assertEqual(float(lookup["promoted_persona_ungrounded_count"]), 1.0)

    def test_promoted_personas_without_examples_are_explicitly_flagged(self) -> None:
        labeled_df = pd.DataFrame(
            {
                "episode_id": ["e1", "e2", "e3", "e4"],
                "persona_core_eligible": [True, True, True, True],
                "role_codes": ["analyst"] * 4,
                "question_codes": ["reporting"] * 4,
                "pain_codes": ["manual_reporting"] * 4,
                "output_codes": ["O_XLSX"] * 4,
            }
        )
        cluster_stats_df = pd.DataFrame(
            {
                "persona_id": ["persona_01", "persona_02"],
                "persona_size": [3, 1],
                "share_of_core_labeled": [75.0, 25.0],
                "share_of_all_labeled": [75.0, 25.0],
                "denominator_type": [DENOMINATOR_PERSONA_CORE_LABELED_ROWS, DENOMINATOR_PERSONA_CORE_LABELED_ROWS],
                "denominator_value": [4, 4],
                "promotion_status": ["promoted_persona", "promoted_persona"],
                "promotion_grounding_status": ["promoted_and_grounded", "promoted_but_ungrounded"],
            }
        )
        persona_examples_df = pd.DataFrame(
            {
                "persona_id": ["persona_01"],
                "example_rank": [1],
                "grounded_text": ["Strong example"],
            }
        )

        metrics = build_quality_metrics(
            total_raw_count=4,
            cleaned_count=4,
            labeled_df=labeled_df,
            source_stage_counts_df=pd.DataFrame({"source": ["reddit"], "raw_record_count": [4], "labeled_episode_count": [4]}),
            cluster_stats_df=cluster_stats_df,
            persona_examples_df=persona_examples_df,
            cluster_profiles=[],
        )
        flattened = evaluate_quality_status(metrics)
        overview_df = _build_final_overview_df(
            axis_names=[{"axis_name": "workflow_stage"}],
            quality_checks=flattened["metrics"] | {
                "overall_status": flattened["composite_status"],
                "quality_flag": flattened["quality_flag"],
                "quality_flag_rule": flattened["quality_flag_rule"],
                "composite_reason_keys": " | ".join(flattened["composite_reason_keys"]),
                "core_clustering_status": flattened["groups"]["core_clustering"]["status"],
                "source_diversity_status": flattened["groups"]["source_diversity"]["status"],
                "example_grounding_status": flattened["groups"]["example_grounding"]["status"],
                **{f"{axis}_status": payload["status"] for axis, payload in flattened["axes"].items()},
            },
            total_labeled_records=4,
            persona_core_labeled_records=4,
            cluster_stats_df=cluster_stats_df,
        )
        lookup = dict(zip(overview_df["metric"], overview_df["value"]))

        self.assertEqual(float(metrics["promoted_persona_example_coverage_pct"]), 50.0)
        self.assertEqual(str(metrics["promoted_personas_missing_examples"]), "persona_02")
        self.assertEqual(str(lookup["promoted_personas_missing_examples"]), "persona_02")
        self.assertNotEqual(str(lookup["example_grounding_status"]), "OK")

    def test_generation_keeps_required_sheets_and_workbook_headers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "export_schema.yaml").write_text("workbook_name: test.xlsx\n", encoding="utf-8")
            frames = assemble_workbook_frames(
                overview_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"]}),
                counts_df=pd.DataFrame({"metric": ["raw_records"], "count": [1], "denominator_type": ["raw_jsonl_rows"], "denominator_value": [1], "definition": ["rows"]}),
                source_distribution_df=pd.DataFrame({"source": ["reddit"], "raw_count": [1], "normalized_count": [1], "valid_count": [1], "prefiltered_valid_count": [1], "episode_count": [1], "labeled_count": [1], "share_of_labeled": [100.0], "denominator_type": ["labeled_episode_rows"], "denominator_value": [1]}),
                taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"], "why_it_matters": ["x"], "allowed_values_or_logic": ["y"], "evidence_fields": ["z"]}),
                cluster_stats_df=pd.DataFrame({"persona_id": ["persona_01"], "persona_size": [1], "share_of_core_labeled": [100.0], "share_of_all_labeled": [100.0], "denominator_type": [DENOMINATOR_PERSONA_CORE_LABELED_ROWS], "denominator_value": [1], "min_cluster_size": [1], "base_promotion_status": ["promoted_persona"], "promotion_status": ["promoted_persona"], "grounding_status": ["grounded"], "promotion_grounding_status": ["promoted_and_grounded"], "promotion_reason": ["meets floor"], "grounding_reason": ["grounded example exists"], "grounded_candidate_count": [1], "weak_candidate_count": [0], "selected_example_count": [1], "fallback_selected_count": [0], "dominant_signature": ["role=analyst"], "dominant_bottleneck": ["manual_reporting"], "dominant_analysis_goal": ["report_speed"]}),
                persona_summary_df=pd.DataFrame({"persona_id": ["persona_01"], "persona_name": ["Analyst"], "persona_size": [1], "share_of_core_labeled": [100.0], "share_of_all_labeled": [100.0], "denominator_type": [DENOMINATOR_PERSONA_CORE_LABELED_ROWS], "denominator_value": [1], "min_cluster_size": [1], "base_promotion_status": ["promoted_persona"], "promotion_status": ["promoted_persona"], "grounding_status": ["grounded"], "promotion_grounding_status": ["promoted_and_grounded"], "promotion_reason": ["meets floor"], "grounding_reason": ["grounded example exists"], "grounded_candidate_count": [1], "weak_candidate_count": [0], "selected_example_count": [1], "fallback_selected_count": [0], "one_line_summary": ["summary"], "dominant_bottleneck": ["manual_reporting"], "main_workflow_context": ["reporting"], "analysis_behavior": ["report_speed"], "trust_explanation_need": ["high"], "current_tool_dependency": ["spreadsheet_heavy"], "primary_output_expectation": ["xlsx"], "top_pain_points": ["rework"], "representative_examples": ["example"], "why_this_persona_matters": ["matters"]}),
                persona_axes_df=pd.DataFrame({"persona_id": ["persona_01"], "axis_name": ["role"], "axis_value": ["analyst"], "count": [1], "pct_of_persona": [100.0]}),
                persona_needs_df=pd.DataFrame({"persona_id": ["persona_01"], "pain_or_need": ["rework"], "count": [1], "pct_of_persona": [100.0], "rank": [1]}),
                persona_cooccurrence_df=pd.DataFrame({"persona_id": ["persona_01"], "theme_a": ["a"], "theme_b": ["b"], "pair_count": [1], "pct_of_persona": [100.0], "rank": [1]}),
                persona_examples_df=pd.DataFrame({"persona_id": ["persona_01"], "example_rank": [1], "grounded_text": ["example"], "selection_strength": ["grounded"], "grounding_strength": ["grounded"], "fallback_selected": [False], "coverage_selection_reason": ["score_plus_diversity_policy"], "grounding_reason": ["grounded"], "why_selected": ["because"], "matched_axes": ["role=analyst"], "reason_selected": ["fit"], "quote_quality": ["usable"], "grounding_fit_score": [1.8], "mismatch_count": [0], "critical_mismatch_count": [0], "matched_axis_count": [4], "final_example_score": [9.2]}),
                quality_checks_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"], "threshold": [""], "status": ["pass"], "level": ["pass"], "denominator_type": [""], "denominator_value": [""], "notes": [""]}),
                source_diagnostics_df=pd.DataFrame({"source": ["reddit"], "section": ["post_funnel"], "grain": ["post"], "metric_name": ["valid_posts_per_normalized_post_pct"], "metric_value": [100.0], "metric_type": ["percentage"], "denominator_metric": ["normalized_post_count"], "denominator_grain": ["post"], "denominator_value": [1], "bounded_range": ["0-100_pct"], "is_same_grain_funnel": [True], "metric_definition": ["definition"], "failure_reason_top": ["labeled_output_present"], "failure_level": ["pass"], "recommended_seed_set": [""]}),
                quality_failures_df=pd.DataFrame({"metric": ["denominator_consistency_check"], "level": ["pass"], "value": ["explicit"], "threshold": [""], "passed": [True]}),
                metric_glossary_df=pd.DataFrame({"metric": ["quality_flag"], "denominator_type": [""], "definition": ["definition"]}),
            )

            output = export_workbook_from_frames(root, frames)
            workbook = load_workbook(output, read_only=True)
            try:
                self.assertEqual(workbook.sheetnames[0], "readme")
                self.assertEqual(workbook.sheetnames[1:], WORKBOOK_SHEET_NAMES)
                overview_headers = [cell.value for cell in next(workbook["overview"].iter_rows(min_row=1, max_row=1))]
                source_headers = [cell.value for cell in next(workbook["source_diagnostics"].iter_rows(min_row=1, max_row=1))]
                persona_summary_headers = [cell.value for cell in next(workbook["persona_summary"].iter_rows(min_row=1, max_row=1))]
                persona_example_headers = [cell.value for cell in next(workbook["persona_examples"].iter_rows(min_row=1, max_row=1))]
            finally:
                workbook.close()

            self.assertIn("metric", overview_headers)
            self.assertIn("value", overview_headers)
            self.assertIn("metric_name", source_headers)
            self.assertIn("row_grain", source_headers)
            self.assertIn("promotion_grounding_status", persona_summary_headers)
            self.assertIn("grounding_status", persona_summary_headers)
            self.assertIn("selected_example_strength", persona_example_headers)
            self.assertIn("example_grounding_strength", persona_example_headers)
            self.assertIn("fallback_selected", persona_example_headers)


if __name__ == "__main__":
    unittest.main()