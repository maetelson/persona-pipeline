"""Tests for shared ratio and quality helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.quality_status import QUALITY_STATUS_POLICY, evaluate_quality_status, quality_display_thresholds
from src.analysis.stage_service import _build_final_overview_df
from src.analysis.summary import build_quality_checks, build_quality_checks_df
from src.analysis.diagnostics import build_quality_failures, finalize_quality_checks
from src.utils.pipeline_schema import compute_quality_flag, round_pct, round_ratio, round_frame_ratios


class PipelineSchemaTests(unittest.TestCase):
    """Verify deterministic rounding and quality flags."""

    def test_round_helpers_are_deterministic(self) -> None:
        self.assertEqual(round_ratio(0.1234567), 0.123457)
        self.assertEqual(round_pct(1, 3), 33.3)

    def test_round_frame_ratios_normalizes_ratio_columns(self) -> None:
        frame = pd.DataFrame({"share_of_core_labeled": [33.333, 66.666]})
        rounded = round_frame_ratios("cluster_stats", frame)
        self.assertEqual(list(rounded["share_of_core_labeled"]), [33.3, 66.7])

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
        self.assertEqual(result["raw_record_rows"], 10)
        self.assertEqual(result["valid_candidate_rows"], 2)
        self.assertEqual(result["persona_core_unknown_ratio"], 0.5)
        self.assertEqual(result["overall_unknown_ratio"], 0.5)
        self.assertEqual(result["persona_core_coverage_of_all_labeled_pct"], 100.0)

    def test_finalize_quality_checks_uses_effective_source_diversity(self) -> None:
        base = {
            "labeled_episode_rows": 12,
            "persona_core_labeled_rows": 12,
            "persona_core_unknown_ratio": 0.05,
            "overall_unknown_ratio": 0.10,
            "persona_core_coverage_of_all_labeled_pct": 100.0,
            "effective_labeled_source_count": 2.4,
            "effective_balanced_source_count": 4.3,
            "largest_labeled_source_share_pct": 33.3,
            "largest_source_influence_share_pct": 40.1,
            "weak_source_cost_center_count": 4,
            "weak_source_cost_centers": "reddit | stackoverflow | github_discussions | hubspot_community",
            "largest_cluster_share_of_core_labeled": 50.0,
            "promoted_persona_example_coverage_pct": 100.0,
            "selected_example_grounding_issue_count": 0,
            "promoted_persona_grounding_failure_count": 0,
            "source_failures": "",
        }
        result = finalize_quality_checks(evaluate_quality_status(base))
        self.assertEqual(float(QUALITY_STATUS_POLICY["effective_source_diversity"]["fail_threshold"]), 5.0)
        self.assertEqual(result["source_diversity_status"], "FAIL")
        self.assertEqual(result["effective_source_diversity_status"], "FAIL")
        self.assertIn("effective_source_balance_critical", str(result["effective_source_diversity_reason_keys"]))
        self.assertEqual(result["overall_status"], "FAIL")
        self.assertEqual(result["quality_flag"], "UNSTABLE")
        self.assertLess(float(result["effective_balanced_source_count"]), 5.0)

    def test_finalize_quality_checks_escalates_when_overall_uncertainty_is_high(self) -> None:
        base = {
            "labeled_episode_rows": 472,
            "persona_core_labeled_rows": 289,
            "persona_core_unknown_ratio": 0.069204,
            "overall_unknown_ratio": 0.430085,
            "persona_core_coverage_of_all_labeled_pct": 61.2,
            "effective_labeled_source_count": 9.6,
            "effective_balanced_source_count": 8.1,
            "largest_labeled_source_share_pct": 42.4,
            "largest_source_influence_share_pct": 24.0,
            "weak_source_cost_center_count": 0,
            "weak_source_cost_centers": "",
            "largest_cluster_share_of_core_labeled": 55.0,
            "promoted_persona_example_coverage_pct": 80.0,
            "selected_example_grounding_issue_count": 0,
            "promoted_persona_grounding_failure_count": 1,
            "source_failures": "",
        }
        result = finalize_quality_checks(evaluate_quality_status(base))
        self.assertEqual(result["overall_unknown_status"], "FAIL")
        self.assertEqual(result["overall_status"], "FAIL")
        self.assertEqual(result["quality_flag"], "UNSTABLE")

    def test_build_quality_checks_df_marks_overall_status_and_unknown_ratio(self) -> None:
        frame = build_quality_checks_df(
            finalize_quality_checks(
                evaluate_quality_status(
                    {
                        "overall_unknown_ratio": 0.430085,
                        "persona_core_unknown_ratio": 0.05,
                        "persona_core_coverage_of_all_labeled_pct": 80.0,
                        "largest_labeled_source_share_pct": 40.0,
                        "largest_source_influence_share_pct": 24.0,
                        "largest_cluster_share_of_core_labeled": 50.0,
                        "promoted_persona_example_coverage_pct": 100.0,
                        "effective_labeled_source_count": 8.0,
                        "effective_balanced_source_count": 7.4,
                        "weak_source_cost_center_count": 0,
                        "weak_source_cost_centers": "",
                        "selected_example_grounding_issue_count": 0,
                        "promoted_persona_grounding_failure_count": 0,
                        "source_failures": "",
                        "labeled_episode_rows": 472,
                    }
                )
            )
        )
        overall_row = frame.loc[frame["metric"] == "overall_status"].iloc[0]
        unknown_row = frame.loc[frame["metric"] == "overall_unknown_ratio"].iloc[0]
        self.assertEqual(overall_row["status"], "fail")
        self.assertEqual(unknown_row["status"], "fail")

    def test_build_quality_failures_marks_zero_labeled_source_failure(self) -> None:
        quality_checks = {
            "min_cluster_size": 5,
            "denominator_consistency": "explicit",
            "labeled_episode_rows": 8,
            "overall_unknown_ratio": 0.10,
            "persona_core_coverage_of_all_labeled_pct": 100.0,
            "overall_unknown_status": "OK",
            "core_coverage_status": "OK",
            "source_diversity_status": "WARN",
            "source_diversity_reason_keys": "raw_covered_sources_missing_labels",
            "source_concentration_status": "OK",
            "largest_cluster_dominance_status": "OK",
            "grounding_coverage_status": "OK",
            "promoted_persona_grounding_failure_count": 0,
            "selected_example_grounding_issue_count": 0,
        }
        source_diagnostics_df = pd.DataFrame(
            [
                {"source": "hubspot_community", "raw_record_count": 100, "labeled_episode_count": 0},
                {"source": "metabase_discussions", "raw_record_count": 100, "labeled_episode_count": 8},
            ]
        )
        cluster_stats_df = pd.DataFrame(
            [{"persona_id": "p1", "persona_size": 8, "share_of_core_labeled": 65.0, "promotion_status": "promoted_persona"}]
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

    def test_overview_and_quality_checks_share_same_evaluated_status(self) -> None:
        evaluated = evaluate_quality_status(
            {
                "overall_unknown_ratio": 0.430085,
                "persona_core_unknown_ratio": 0.069204,
                "persona_core_coverage_of_all_labeled_pct": 61.2,
                "largest_labeled_source_share_pct": 42.4,
                "largest_source_influence_share_pct": 24.0,
                "largest_cluster_share_of_core_labeled": 55.0,
                "promoted_persona_example_coverage_pct": 80.0,
                "effective_labeled_source_count": 9.6,
                "effective_balanced_source_count": 7.8,
                "weak_source_cost_center_count": 0,
                "weak_source_cost_centers": "",
                "selected_example_grounding_issue_count": 0,
                "promoted_persona_grounding_failure_count": 1,
                "source_failures": "",
                "labeled_episode_rows": 472,
            }
        )
        flattened = finalize_quality_checks(evaluated)
        overview_df = _build_final_overview_df(
            axis_names=[{"axis_name": "role"}],
            quality_checks=flattened,
            stage_counts={
                "raw_record_rows": 7739,
                "normalized_post_rows": 7739,
                "valid_candidate_rows": 3438,
                "prefiltered_valid_rows": 783,
                "episode_rows": 472,
                "labeled_episode_rows": 472,
            },
            persona_core_labeled_rows=289,
            cluster_stats_df=pd.DataFrame({"promotion_status": ["promoted_persona"]}),
        )
        quality_df = build_quality_checks_df(flattened)
        overview_status = str(overview_df.loc[overview_df["metric"] == "overall_status", "value"].iloc[0])
        quality_status = str(quality_df.loc[quality_df["metric"] == "overall_status", "value"].iloc[0])
        self.assertEqual(overview_status, quality_status)
        self.assertEqual(overview_status, "FAIL")

    def test_overview_surfaces_worse_overall_uncertainty_even_when_core_is_good(self) -> None:
        evaluated = evaluate_quality_status(
            {
                "overall_unknown_ratio": 0.45,
                "persona_core_unknown_ratio": 0.02,
                "persona_core_coverage_of_all_labeled_pct": 88.0,
                "largest_labeled_source_share_pct": 20.0,
                "largest_source_influence_share_pct": 15.0,
                "largest_cluster_share_of_core_labeled": 40.0,
                "promoted_persona_example_coverage_pct": 100.0,
                "effective_labeled_source_count": 8.0,
                "effective_balanced_source_count": 7.2,
                "weak_source_cost_center_count": 0,
                "weak_source_cost_centers": "",
                "selected_example_grounding_issue_count": 0,
                "promoted_persona_grounding_failure_count": 0,
                "source_failures": "",
                "labeled_episode_rows": 100,
            }
        )
        flattened = finalize_quality_checks(evaluated)
        overview_df = _build_final_overview_df(
            axis_names=[{"axis_name": "role"}],
            quality_checks=flattened,
            stage_counts={
                "raw_record_rows": 100,
                "normalized_post_rows": 100,
                "valid_candidate_rows": 100,
                "prefiltered_valid_rows": 88,
                "episode_rows": 100,
                "labeled_episode_rows": 100,
            },
            persona_core_labeled_rows=88,
            cluster_stats_df=pd.DataFrame({"promotion_status": ["promoted_persona"]}),
        )
        overview_lookup = dict(zip(overview_df["metric"], overview_df["value"]))
        self.assertEqual(str(overview_lookup["core_unknown_status"]), "OK")
        self.assertEqual(str(overview_lookup["overall_unknown_status"]), "FAIL")
        self.assertEqual(str(overview_lookup["overall_status"]), "FAIL")
        self.assertEqual(str(overview_lookup["quality_flag"]), "UNSTABLE")

    def test_finalize_quality_checks_does_not_mutate_evaluated_status(self) -> None:
        evaluated = evaluate_quality_status(
            {
                "overall_unknown_ratio": 0.10,
                "persona_core_unknown_ratio": 0.05,
                "persona_core_coverage_of_all_labeled_pct": 90.0,
                "largest_labeled_source_share_pct": 20.0,
                "largest_source_influence_share_pct": 15.0,
                "largest_cluster_share_of_core_labeled": 40.0,
                "promoted_persona_example_coverage_pct": 100.0,
                "effective_labeled_source_count": 8.0,
                "effective_balanced_source_count": 7.2,
                "weak_source_cost_center_count": 0,
                "weak_source_cost_centers": "",
                "selected_example_grounding_issue_count": 0,
                "promoted_persona_grounding_failure_count": 0,
                "source_failures": "",
            }
        )
        flattened = finalize_quality_checks(evaluated)
        self.assertEqual(flattened["overall_status"], str(evaluated["composite_status"]))
        self.assertEqual(flattened["quality_flag"], str(evaluated["quality_flag"]))

    def test_threshold_display_is_derived_from_central_policy(self) -> None:
        thresholds = quality_display_thresholds()
        self.assertEqual(
            thresholds["effective_balanced_source_count"],
            str(QUALITY_STATUS_POLICY["effective_source_diversity"]["display_threshold"]),
        )
        quality_df = build_quality_checks_df(
            finalize_quality_checks(
                evaluate_quality_status(
                    {
                        "overall_unknown_ratio": 0.10,
                        "persona_core_unknown_ratio": 0.05,
                        "persona_core_coverage_of_all_labeled_pct": 90.0,
                        "effective_labeled_source_count": 3.5,
                        "effective_balanced_source_count": 3.5,
                        "largest_labeled_source_share_pct": 20.0,
                        "largest_source_influence_share_pct": 20.0,
                        "largest_cluster_share_of_core_labeled": 40.0,
                        "promoted_persona_example_coverage_pct": 100.0,
                        "weak_source_cost_center_count": 0,
                        "weak_source_cost_centers": "",
                        "selected_example_grounding_issue_count": 0,
                        "promoted_persona_grounding_failure_count": 0,
                        "source_failures": "",
                    }
                )
            )
        )
        row = quality_df.loc[quality_df["metric"] == "effective_balanced_source_count"].iloc[0]
        self.assertEqual(str(row["threshold"]), "warn<6.0; fail<5.0")
        self.assertEqual(str(row["status"]), "fail")

    def test_build_quality_failures_uses_persona_level_grounding_failure_count(self) -> None:
        quality_checks = {
            "min_cluster_size": 5,
            "denominator_consistency": "explicit",
            "grounding_coverage_status": "FAIL",
            "grounding_coverage_reason_keys": "promoted_persona_examples_missing",
            "promoted_persona_grounding_failure_count": 1,
            "selected_example_grounding_issue_count": 0,
            "promoted_persona_example_coverage_pct": 66.7,
        }
        failures_df = build_quality_failures(
            quality_checks,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        persona_row = failures_df.loc[failures_df["metric"] == "promoted_persona_grounding_gate"].iloc[0]
        example_row = failures_df.loc[failures_df["metric"] == "selected_example_grounding_issue_gate"].iloc[0]
        self.assertEqual(persona_row["value"], 1)
        self.assertEqual(example_row["value"], 0)
        self.assertEqual(persona_row["level"], "hard_fail")
        self.assertEqual(example_row["level"], "pass")

        quality_df = build_quality_checks_df(quality_checks)
        quality_row = quality_df.loc[quality_df["metric"] == "promoted_persona_grounding_failure_count"].iloc[0]
        self.assertEqual(str(quality_row["status"]), "fail")
        self.assertEqual(str(quality_row["level"]), "hard_fail")

    def test_central_policy_explicitly_describes_source_diversity_fail_rule(self) -> None:
        policy = QUALITY_STATUS_POLICY["effective_source_diversity"]
        self.assertEqual(policy["metric"], "effective_balanced_source_count")
        self.assertEqual(float(policy["fail_threshold"]), 5.0)
        self.assertEqual(float(policy["warn_threshold"]), 6.0)
        self.assertEqual(str(policy["display_threshold"]), "warn<6.0; fail<5.0")


if __name__ == "__main__":
    unittest.main()
