"""Tests for canonical workbook bundle and xlsx export."""

from __future__ import annotations

import tempfile
import unittest
import warnings
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from src.analysis.workbook_bundle import assemble_workbook_frames, validate_workbook_frames
from src.exporters.xlsx_exporter import export_workbook_from_frames


class WorkbookExportTests(unittest.TestCase):
    """Verify workbook creation remains robust for sparse inputs."""

    def test_export_creates_required_sheets_even_with_sparse_frames(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "export_schema.yaml").write_text("workbook_name: test.xlsx\n", encoding="utf-8")
            empty = pd.DataFrame()
            frames = assemble_workbook_frames(
                overview_df=pd.DataFrame({"metric": ["x"], "value": ["y"]}),
                counts_df=pd.DataFrame({"metric": ["raw_record_rows"], "count": [1]}),
                source_distribution_df=empty,
                taxonomy_summary_df=empty,
                cluster_stats_df=empty,
                persona_summary_df=empty,
                persona_axes_df=empty,
                persona_needs_df=empty,
                persona_cooccurrence_df=empty,
                persona_examples_df=empty,
                quality_checks_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"], "threshold": [""], "status": ["pass"], "notes": [""]}),
            )
            output = export_workbook_from_frames(root, frames)
            workbook = load_workbook(output, read_only=False)
            try:
                self.assertEqual(workbook.sheetnames[0], "readme")
                self.assertEqual(workbook.sheetnames[1:], [
                    "overview",
                    "counts",
                    "source_distribution",
                    "taxonomy_summary",
                    "cluster_stats",
                    "persona_summary",
                    "persona_axes",
                    "persona_needs",
                    "persona_cooccurrence",
                    "persona_examples",
                    "quality_checks",
                    "source_diagnostics",
                    "quality_failures",
                    "metric_glossary",
                ])
                self.assertEqual(workbook["overview"].freeze_panes, "A2")
                self.assertIsNotNone(workbook["overview"].auto_filter.ref)
                self.assertEqual(str(workbook["readme"]["B4"].value).startswith("=INDEX("), True)
                overview_headers = [cell.value for cell in next(workbook["overview"].iter_rows(min_row=1, max_row=1))]
                self.assertEqual(overview_headers[:3], ["metric_key", "display_label", "metric_value"])
            finally:
                workbook.close()

    def test_validate_workbook_frames_warns_on_sparse_optional_sheets(self) -> None:
        frames = assemble_workbook_frames(
            overview_df=pd.DataFrame({"metric": ["x"], "value": ["y"]}),
            counts_df=pd.DataFrame({"metric": ["raw_record_rows"], "count": [1]}),
            source_distribution_df=pd.DataFrame({"source": ["reddit"], "normalized_count": [1], "valid_count": [1], "episode_count": [1], "labeled_count": [1], "share_of_labeled": [100.0]}),
            taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"], "why_it_matters": ["x"], "allowed_values_or_logic": ["a"], "evidence_fields": ["b"]}),
            cluster_stats_df=pd.DataFrame(),
            persona_summary_df=pd.DataFrame(),
            persona_axes_df=pd.DataFrame(),
            persona_needs_df=pd.DataFrame(),
            persona_cooccurrence_df=pd.DataFrame(),
            persona_examples_df=pd.DataFrame(),
            quality_checks_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["LOW QUALITY"], "threshold": [""], "status": ["warn"], "notes": ["x"]}),
        )
        messages = validate_workbook_frames(frames)
        self.assertTrue(any("sparse data: empty cluster_stats sheet" == msg for msg in messages))

    def test_validate_workbook_frames_warns_on_missing_optional_columns(self) -> None:
        frames = {
            "overview": pd.DataFrame({"metric": ["x"]}),
            "counts": pd.DataFrame({"metric": ["raw_record_rows"], "count": [1]}),
            "source_distribution": pd.DataFrame({"source": ["reddit"]}),
            "taxonomy_summary": pd.DataFrame({"axis_name": ["role"]}),
            "cluster_stats": pd.DataFrame(),
            "persona_summary": pd.DataFrame(),
            "persona_axes": pd.DataFrame(),
            "persona_needs": pd.DataFrame(),
            "persona_cooccurrence": pd.DataFrame(),
            "persona_examples": pd.DataFrame(),
            "quality_checks": pd.DataFrame({"metric": ["quality_flag"]}),
            "source_diagnostics": pd.DataFrame(),
            "quality_failures": pd.DataFrame(),
            "metric_glossary": pd.DataFrame(),
        }
        messages = validate_workbook_frames(frames)
        self.assertTrue(any(msg == "missing optional column: overview.value" for msg in messages))

    def test_export_warns_for_invalid_ratio_but_still_writes_sparse_workbook(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "export_schema.yaml").write_text("workbook_name: test.xlsx\n", encoding="utf-8")
            frames = assemble_workbook_frames(
                overview_df=pd.DataFrame({"metric": ["x", "promoted_candidate_persona_count", "promotion_visibility_persona_count", "final_usable_persona_count", "deck_ready_persona_count"], "value": ["y", 1, 1, 1, 1]}),
                counts_df=pd.DataFrame({"metric": ["raw_record_rows"], "count": [1]}),
                source_distribution_df=pd.DataFrame({"source": ["reddit"], "normalized_count": [1], "valid_count": [1], "episode_count": [1], "labeled_count": [1], "share_of_labeled": [100.0]}),
                taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"], "why_it_matters": ["x"], "allowed_values_or_logic": ["a"], "evidence_fields": ["b"]}),
                cluster_stats_df=pd.DataFrame(
                    {
                        "persona_id": ["p1"],
                        "persona_size": [1],
                        "share_of_core_labeled": ["bad"],
                        "share_of_all_labeled": [100.0],
                        "base_promotion_status": ["promoted_candidate_persona"],
                        "promotion_status": ["promoted_persona"],
                        "promotion_grounding_status": ["promoted_and_grounded"],
                        "final_usable_persona": [True],
                        "denominator_type": ["persona_core_labeled_rows"],
                        "denominator_value": [1],
                        "dominant_signature": [""],
                        "dominant_bottleneck": [""],
                        "dominant_analysis_goal": [""],
                    }
                ),
                persona_summary_df=pd.DataFrame(),
                persona_axes_df=pd.DataFrame(),
                persona_needs_df=pd.DataFrame(),
                persona_cooccurrence_df=pd.DataFrame(),
                persona_examples_df=pd.DataFrame(),
                quality_checks_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"], "threshold": [""], "status": ["pass"], "notes": [""]}),
            )
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("always")
                output = export_workbook_from_frames(root, frames)
            self.assertTrue(output.exists())
            self.assertTrue(any("non-numeric ratio values: cluster_stats.share_of_core_labeled" in str(item.message) for item in captured))

    def test_export_applies_literal_percentage_format_to_pct_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "export_schema.yaml").write_text("workbook_name: test.xlsx\n", encoding="utf-8")
            frames = assemble_workbook_frames(
                overview_df=pd.DataFrame({"metric": ["quality_flag", "promoted_candidate_persona_count", "promotion_visibility_persona_count", "final_usable_persona_count", "deck_ready_persona_count"], "value": ["OK", 1, 1, 1, 1]}),
                counts_df=pd.DataFrame({"metric": ["raw_record_rows"], "count": [1]}),
                source_distribution_df=pd.DataFrame({"source": ["reddit"], "raw_count": [1], "normalized_count": [1], "valid_count": [1], "prefiltered_valid_count": [1], "episode_count": [1], "labeled_count": [1], "share_of_labeled": [100.0]}),
                taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"], "why_it_matters": ["x"], "allowed_values_or_logic": ["a"], "evidence_fields": ["b"]}),
                cluster_stats_df=pd.DataFrame({"persona_id": ["p1"], "persona_size": [1], "share_of_core_labeled": [75.0], "share_of_all_labeled": [50.0], "base_promotion_status": ["promoted_candidate_persona"], "promotion_status": ["promoted_persona"], "promotion_grounding_status": ["promoted_and_grounded"], "final_usable_persona": [True], "denominator_type": ["persona_core_labeled_rows"], "denominator_value": [1], "dominant_signature": [""], "dominant_bottleneck": [""], "dominant_analysis_goal": [""]}),
                persona_summary_df=pd.DataFrame(),
                persona_axes_df=pd.DataFrame(),
                persona_needs_df=pd.DataFrame(),
                persona_cooccurrence_df=pd.DataFrame(),
                persona_examples_df=pd.DataFrame(),
                quality_checks_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"], "threshold": [""], "status": ["pass"], "notes": [""]}),
            )
            output = export_workbook_from_frames(root, frames)
            workbook = load_workbook(output, read_only=False)
            try:
                worksheet = workbook["cluster_stats"]
                self.assertEqual(worksheet["C2"].number_format, '0.0"%"')
                self.assertEqual(worksheet["D2"].number_format, '0.0"%"')
            finally:
                workbook.close()

    def test_export_fails_when_share_name_and_denominator_do_not_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "export_schema.yaml").write_text("workbook_name: test.xlsx\n", encoding="utf-8")
            frames = assemble_workbook_frames(
                overview_df=pd.DataFrame({"metric": ["x", "promoted_candidate_persona_count", "promotion_visibility_persona_count", "final_usable_persona_count", "deck_ready_persona_count"], "value": ["y", 1, 1, 1, 1]}),
                counts_df=pd.DataFrame({"metric": ["raw_record_rows"], "count": [1]}),
                source_distribution_df=pd.DataFrame({"source": ["reddit"], "normalized_count": [1], "valid_count": [1], "episode_count": [1], "labeled_count": [1], "share_of_labeled": [100.0]}),
                taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"], "why_it_matters": ["x"], "allowed_values_or_logic": ["a"], "evidence_fields": ["b"]}),
                cluster_stats_df=pd.DataFrame(
                    {
                        "persona_id": ["p1"],
                        "persona_size": [1],
                        "share_of_core_labeled": [100.0],
                        "base_promotion_status": ["promoted_candidate_persona"],
                        "promotion_status": ["promoted_persona"],
                        "promotion_grounding_status": ["promoted_and_grounded"],
                        "final_usable_persona": [True],
                        "denominator_type": ["labeled_episode_rows"],
                        "denominator_value": [1],
                        "dominant_signature": [""],
                        "dominant_bottleneck": [""],
                        "dominant_analysis_goal": [""],
                    }
                ),
                persona_summary_df=pd.DataFrame(),
                persona_axes_df=pd.DataFrame(),
                persona_needs_df=pd.DataFrame(),
                persona_cooccurrence_df=pd.DataFrame(),
                persona_examples_df=pd.DataFrame(),
                quality_checks_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"], "threshold": [""], "status": ["pass"], "notes": [""]}),
            )
            with self.assertRaisesRegex(ValueError, "share denominator mismatch"):
                export_workbook_from_frames(root, frames)

    def test_export_adds_explicit_labels_and_keeps_glossary_definitions_aligned(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "export_schema.yaml").write_text("workbook_name: test.xlsx\n", encoding="utf-8")
            frames = assemble_workbook_frames(
                overview_df=pd.DataFrame(
                    {
                        "metric": ["raw_record_rows", "promotion_visibility_persona_count", "final_usable_persona_count"],
                        "value": [12, 3, 2],
                    }
                ),
                counts_df=pd.DataFrame({"metric": ["raw_record_rows"], "count": [12]}),
                source_distribution_df=pd.DataFrame(
                    {
                        "source": ["reddit"],
                        "raw_count": [12],
                        "normalized_count": [10],
                        "valid_count": [8],
                        "prefiltered_valid_count": [4],
                        "episode_count": [5],
                        "labeled_count": [5],
                        "share_of_labeled": [100.0],
                        "denominator_type": ["labeled_episode_rows"],
                        "denominator_value": [5],
                    }
                ),
                taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"], "why_it_matters": ["x"], "allowed_values_or_logic": ["a"], "evidence_fields": ["b"]}),
                cluster_stats_df=pd.DataFrame(),
                persona_summary_df=pd.DataFrame(),
                persona_axes_df=pd.DataFrame(),
                persona_needs_df=pd.DataFrame(),
                persona_cooccurrence_df=pd.DataFrame(),
                persona_examples_df=pd.DataFrame(),
                quality_checks_df=pd.DataFrame(
                    {
                        "metric": ["effective_labeled_source_count", "promoted_persona_grounding_failure_count"],
                        "value": [3.2, 1],
                        "threshold": ["fail<4.0", "warn<100.0; fail<80.0"],
                        "status": ["fail", "fail"],
                        "level": ["hard_fail", "hard_fail"],
                        "denominator_type": ["", "promoted_persona_rows"],
                        "denominator_value": ["", 3],
                        "notes": ["effective_source_diversity_low", "promoted_persona_examples_missing"],
                    }
                ),
                source_diagnostics_df=pd.DataFrame(),
                quality_failures_df=pd.DataFrame(),
                metric_glossary_df=pd.DataFrame(
                    {
                        "metric": ["effective_labeled_source_count", "final_usable_persona_count", "promotion_visibility_persona_count"],
                        "denominator_type": ["effective_labeled_source_count", "persona_cluster_rows", "persona_cluster_rows"],
                        "definition": [
                            "Effective count of contributing labeled sources after fractional down-weighting for very small labeled-source volumes. This is a source-count metric, not a row-count metric.",
                            "Count of final usable personas for downstream reporting. Under the current policy this includes only promoted_and_grounded personas, not weakly grounded or ungrounded review-only personas.",
                            "Count of promoted personas that remain visible in the workbook for reviewer inspection after grounding policy merge. Under the current flag policy this includes grounded, weakly grounded, and ungrounded promoted personas.",
                        ],
                    }
                ),
            )
            output = export_workbook_from_frames(root, frames)
            workbook = load_workbook(output, read_only=True)
            try:
                overview_rows = list(workbook["overview"].iter_rows(min_row=1, max_row=4, values_only=True))
                quality_rows = list(workbook["quality_checks"].iter_rows(min_row=1, max_row=3, values_only=True))
                glossary_rows = list(workbook["metric_glossary"].iter_rows(min_row=1, max_row=4, values_only=True))
                readme_persona_copy = workbook["readme"]["B26"].value
                readme_row_source_copy = workbook["readme"]["B27"].value
            finally:
                workbook.close()

            self.assertEqual(overview_rows[0][:3], ("metric_key", "display_label", "metric_value"))
            self.assertEqual(overview_rows[1][:3], ("raw_record_rows", "Raw record row count (JSONL lines, not source count)", 12))
            self.assertEqual(overview_rows[2][:3], ("promotion_visibility_persona_count", "Promotion-visibility persona count (review-visible promoted personas)", 3))
            self.assertEqual(overview_rows[3][:3], ("final_usable_persona_count", "Final usable persona count (grounded promoted personas only)", 2))

            self.assertEqual(quality_rows[0][:3], ("metric_key", "display_label", "metric_value"))
            self.assertEqual(quality_rows[1][1], "Effective labeled-source count (source diversity score, not row count)")
            self.assertEqual(quality_rows[2][1], "Promoted persona count failing grounded-usable policy")

            self.assertEqual(glossary_rows[0][:3], ("metric_key", "workbook_label", "denominator_type_key"))
            self.assertEqual(glossary_rows[1][1], "Effective labeled-source count (source diversity score, not row count)")
            self.assertIn("source-count metric, not a row-count metric", str(glossary_rows[1][3]))
            self.assertIn("Final Usable Persona Count", str(readme_persona_copy))
            self.assertIn("Raw Record Row Count is a count of JSONL rows", str(readme_row_source_copy))

    def test_export_uses_explicit_row_based_headers_for_counts_and_source_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "export_schema.yaml").write_text("workbook_name: test.xlsx\n", encoding="utf-8")
            frames = assemble_workbook_frames(
                overview_df=pd.DataFrame({"metric": ["raw_record_rows"], "value": [12]}),
                counts_df=pd.DataFrame(
                    {
                        "metric": ["raw_record_rows", "valid_candidate_rows", "labeled_episode_rows"],
                        "count": [12, 8, 5],
                        "denominator_type": ["raw_record_rows", "valid_candidate_rows", "labeled_episode_rows"],
                        "denominator_value": [12, 8, 5],
                        "definition": ["raw rows", "valid rows", "labeled rows"],
                    }
                ),
                source_distribution_df=pd.DataFrame(
                    {
                        "source": ["reddit"],
                        "raw_count": [12],
                        "normalized_count": [10],
                        "valid_count": [8],
                        "prefiltered_valid_count": [4],
                        "episode_count": [5],
                        "labeled_count": [5],
                        "share_of_labeled": [100.0],
                        "denominator_type": ["labeled_episode_rows"],
                        "denominator_value": [5],
                    }
                ),
                taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"], "why_it_matters": ["x"], "allowed_values_or_logic": ["a"], "evidence_fields": ["b"]}),
                cluster_stats_df=pd.DataFrame(),
                persona_summary_df=pd.DataFrame(),
                persona_axes_df=pd.DataFrame(),
                persona_needs_df=pd.DataFrame(),
                persona_cooccurrence_df=pd.DataFrame(),
                persona_examples_df=pd.DataFrame(),
                quality_checks_df=pd.DataFrame({"metric": ["quality_flag"], "value": ["OK"], "threshold": [""], "status": ["pass"], "level": ["pass"], "denominator_type": [""], "denominator_value": [""], "notes": [""]}),
                source_diagnostics_df=pd.DataFrame(),
                quality_failures_df=pd.DataFrame(),
                metric_glossary_df=pd.DataFrame(),
            )
            output = export_workbook_from_frames(root, frames)
            workbook = load_workbook(output, read_only=True)
            try:
                counts_headers = [cell.value for cell in next(workbook["counts"].iter_rows(min_row=1, max_row=1))]
                source_headers = [cell.value for cell in next(workbook["source_distribution"].iter_rows(min_row=1, max_row=1))]
            finally:
                workbook.close()

            self.assertIn("metric_key", counts_headers)
            self.assertIn("row_count", counts_headers)
            self.assertIn("raw_record_rows_for_source", source_headers)
            self.assertIn("valid_candidate_rows_for_source", source_headers)
            self.assertIn("labeled_episode_rows_for_source", source_headers)
            self.assertIn("share_of_labeled_episode_rows_pct", source_headers)


if __name__ == "__main__":
    unittest.main()
