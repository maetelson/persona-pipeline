"""Tests for canonical workbook bundle and xlsx export."""

from __future__ import annotations

import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
