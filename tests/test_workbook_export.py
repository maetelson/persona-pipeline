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
                counts_df=pd.DataFrame({"metric": ["raw_records"], "count": [1]}),
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
            workbook = load_workbook(output, read_only=True)
            try:
                self.assertEqual(
                    workbook.sheetnames,
                    [
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
                    ],
                )
            finally:
                workbook.close()

    def test_validate_workbook_frames_warns_on_sparse_optional_sheets(self) -> None:
        frames = assemble_workbook_frames(
            overview_df=pd.DataFrame({"metric": ["x"], "value": ["y"]}),
            counts_df=pd.DataFrame({"metric": ["raw_records"], "count": [1]}),
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
            "counts": pd.DataFrame({"metric": ["raw_records"], "count": [1]}),
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
                overview_df=pd.DataFrame({"metric": ["x"], "value": ["y"]}),
                counts_df=pd.DataFrame({"metric": ["raw_records"], "count": [1]}),
                source_distribution_df=pd.DataFrame({"source": ["reddit"], "normalized_count": [1], "valid_count": [1], "episode_count": [1], "labeled_count": [1], "share_of_labeled": [100.0]}),
                taxonomy_summary_df=pd.DataFrame({"axis_name": ["role"], "why_it_matters": ["x"], "allowed_values_or_logic": ["a"], "evidence_fields": ["b"]}),
                cluster_stats_df=pd.DataFrame({"persona_id": ["p1"], "persona_size": [1], "share_of_total": ["bad"], "dominant_signature": [""], "dominant_bottleneck": [""], "dominant_analysis_goal": [""]}),
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
            self.assertTrue(any("non-numeric ratio values: cluster_stats.share_of_total" in str(item.message) for item in captured))


if __name__ == "__main__":
    unittest.main()
