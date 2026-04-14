"""Tests for source-stage profiling report rendering."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.source_stage_profiler import render_profile_report


class SourceStageProfilerReportTest(unittest.TestCase):
    """Coverage for the markdown report emitted by source-stage profiling."""

    def test_render_profile_report_includes_reddit_path_and_diagnosis(self) -> None:
        """The markdown report should include the exact Reddit path and timing diagnosis."""
        summary_df = pd.DataFrame(
            [
                {
                    "source": "reddit",
                    "total_pipeline_seconds": 12.0,
                    "collect_seconds": 10.0,
                    "normalize_seconds": 0.4,
                    "validate_seconds": 0.3,
                    "dedupe_seconds": 0.1,
                    "prefilter_seconds": 0.5,
                    "episode_build_seconds": 0.4,
                    "labelability_handoff_seconds": 0.3,
                    "top_stage": "collect",
                    "top_stage_seconds": 10.0,
                    "raw_record_count": 100,
                    "valid_after_dedupe_count": 40,
                    "prefilter_keep_count": 8,
                    "episode_count": 9,
                    "labelable_episode_count": 5,
                    "borderline_episode_count": 2,
                    "prefilter_keep_ratio": 0.2,
                    "request_count": 140,
                    "search_request_count": 20,
                    "comments_request_count": 120,
                    "request_retry_count": 4,
                    "backoff_sleep_seconds": 3.5,
                    "total_sleep_seconds": 4.2,
                    "pagination_iterations": 8,
                    "average_items_per_page": 8.5,
                    "average_kept_items_per_page": 1.3,
                    "query_overlap_skip_count": 18,
                    "comment_fetch_count": 30,
                    "comment_skip_count": 70,
                    "rate_limit_header_seen_count": 6,
                    "pagination_low_yield_stop_count": 2,
                    "pagination_overlap_stop_count": 1,
                    "pagination_repeated_cursor_stop_count": 0,
                    "rate_limit_remaining_min": 4.0,
                    "collect_seconds_per_raw_record": 0.1,
                    "requests_per_raw_record": 1.4,
                },
                {
                    "source": "stackoverflow",
                    "total_pipeline_seconds": 5.0,
                    "collect_seconds": 3.2,
                    "normalize_seconds": 0.4,
                    "validate_seconds": 0.3,
                    "dedupe_seconds": 0.1,
                    "prefilter_seconds": 0.4,
                    "episode_build_seconds": 0.3,
                    "labelability_handoff_seconds": 0.3,
                    "top_stage": "collect",
                    "top_stage_seconds": 3.2,
                    "raw_record_count": 60,
                    "valid_after_dedupe_count": 45,
                    "prefilter_keep_count": 18,
                    "episode_count": 20,
                    "labelable_episode_count": 12,
                    "borderline_episode_count": 3,
                    "prefilter_keep_ratio": 0.4,
                    "request_count": 50,
                    "search_request_count": 10,
                    "comments_request_count": 20,
                    "request_retry_count": 1,
                    "backoff_sleep_seconds": 0.0,
                    "total_sleep_seconds": 0.0,
                    "pagination_iterations": 5,
                    "average_items_per_page": 12.0,
                    "average_kept_items_per_page": 3.6,
                    "query_overlap_skip_count": 0,
                    "comment_fetch_count": 20,
                    "comment_skip_count": 12,
                    "rate_limit_header_seen_count": 0,
                    "pagination_low_yield_stop_count": 0,
                    "pagination_overlap_stop_count": 0,
                    "pagination_repeated_cursor_stop_count": 0,
                    "rate_limit_remaining_min": 0.0,
                    "collect_seconds_per_raw_record": 0.053333,
                    "requests_per_raw_record": 0.833333,
                },
            ]
        )
        stage_df = pd.DataFrame(
            [
                {
                    "source": "reddit",
                    "stage": "collect",
                    "elapsed_seconds": 10.0,
                    "input_count": 0,
                    "output_count": 100,
                    "dropped_count": 0,
                    "notes": "requests=140",
                }
            ]
        )

        report = render_profile_report(summary_df, stage_df)

        self.assertIn("# Source Stage Profile Report", report)
        self.assertIn("run/01_collect_all.py", report)
        self.assertIn("RedditCollector.collect", report)
        self.assertIn("Reddit hotspot: collect", report)
        self.assertIn("avg_items_per_page", report)
        self.assertIn("Reddit pagination controls", report)
        self.assertIn("python run/diagnostics/17_profile_sources.py --sources reddit stackoverflow github_discussions", report)


if __name__ == "__main__":
    unittest.main()
