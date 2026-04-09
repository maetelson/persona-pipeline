"""Tests for shared ratio and quality helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.summary import build_quality_checks
from src.utils.pipeline_schema import compute_quality_flag, round_pct, round_ratio, round_frame_ratios


class PipelineSchemaTests(unittest.TestCase):
    """Verify deterministic rounding and quality flags."""

    def test_round_helpers_are_deterministic(self) -> None:
        self.assertEqual(round_ratio(0.1234567), 0.123457)
        self.assertEqual(round_pct(1, 3), 33.3)

    def test_round_frame_ratios_normalizes_ratio_columns(self) -> None:
        frame = pd.DataFrame({"share_of_total": [33.333, 66.666]})
        rounded = round_frame_ratios("cluster_stats", frame)
        self.assertEqual(list(rounded["share_of_total"]), [33.3, 66.7])

    def test_quality_flag_uses_unknown_ratio_threshold(self) -> None:
        self.assertEqual(compute_quality_flag(0.10), "OK")
        self.assertEqual(compute_quality_flag(0.31), "LOW QUALITY")

    def test_build_quality_checks_is_deterministic(self) -> None:
        labeled_df = pd.DataFrame(
            [
                {"role_codes": "R_ANALYST", "moment_codes": "M_REPORTING", "question_codes": "Q_REPORT_SPEED", "pain_codes": "P_MANUAL_REPORTING", "env_codes": "E_SPREADSHEET", "workaround_codes": "W_MANUAL", "output_codes": "O_XLSX", "fit_code": "F_STRONG"},
                {"role_codes": "unknown", "moment_codes": "M_REPORTING", "question_codes": "Q_REPORT_SPEED", "pain_codes": "P_MANUAL_REPORTING", "env_codes": "E_SPREADSHEET", "workaround_codes": "W_MANUAL", "output_codes": "O_XLSX", "fit_code": "F_STRONG"},
            ]
        )
        result = build_quality_checks(
            raw_audit_df=pd.DataFrame({"raw_record_count": [10]}),
            valid_df=pd.DataFrame({"id": [1, 2]}),
            labeled_df=labeled_df,
            cluster_profiles=[{"cluster_id": "c1", "size": 2, "share_of_total": 1.0}],
        )
        self.assertEqual(result["total_raw_count"], 10)
        self.assertEqual(result["quality_flag"], "LOW QUALITY")


if __name__ == "__main__":
    unittest.main()
