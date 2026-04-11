"""Tests for shared ratio and quality helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.summary import build_quality_checks, build_quality_checks_df
from src.analysis.diagnostics import build_quality_failures, finalize_quality_checks
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

    def test_finalize_quality_checks_uses_effective_source_diversity(self) -> None:
        base = {
            "labeled_count": 12,
            "quality_flag": "OK",
        }
        source_diagnostics_df = pd.DataFrame(
            [
                {"source": "a", "raw_count": 100, "labeled_count": 4},
                {"source": "b", "raw_count": 100, "labeled_count": 4},
                {"source": "c", "raw_count": 100, "labeled_count": 4},
                {"source": "d", "raw_count": 100, "labeled_count": 0},
            ]
        )
        cluster_stats_df = pd.DataFrame(
            [{"persona_id": "p1", "persona_size": 6, "share_of_total": 50.0, "promotion_status": "promoted_persona"}]
        )
        result = finalize_quality_checks(base, source_diagnostics_df, cluster_stats_df, pd.DataFrame())
        self.assertEqual(result["quality_flag"], "EXPLORATORY")
        self.assertLess(float(result["effective_labeled_source_count"]), 4.0)

    def test_build_quality_failures_marks_zero_labeled_source_failure(self) -> None:
        quality_checks = {"min_cluster_size": 5, "denominator_consistency": "explicit"}
        source_diagnostics_df = pd.DataFrame(
            [
                {"source": "hubspot_community", "raw_count": 100, "labeled_count": 0},
                {"source": "metabase_discussions", "raw_count": 100, "labeled_count": 8},
            ]
        )
        cluster_stats_df = pd.DataFrame(
            [{"persona_id": "p1", "persona_size": 8, "share_of_total": 65.0, "promotion_status": "promoted_persona"}]
        )
        failures_df = build_quality_failures(quality_checks, source_diagnostics_df, cluster_stats_df, pd.DataFrame())
        metrics = set(failures_df["metric"].astype(str).tolist())
        self.assertIn("source_failure:hubspot_community", metrics)

    def test_build_quality_checks_df_marks_source_failures_as_soft_fail(self) -> None:
        frame = build_quality_checks_df(
            {
                "quality_flag": "EXPLORATORY",
                "labeled_source_count": 9,
                "effective_labeled_source_count": 7.8,
                "source_failures": "shopify_community",
            }
        )
        row = frame.loc[frame["metric"] == "source_failures"].iloc[0]
        self.assertEqual(row["status"], "fail")
        self.assertEqual(row["level"], "soft_fail")


if __name__ == "__main__":
    unittest.main()
