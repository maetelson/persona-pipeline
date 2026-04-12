"""Tests for Reddit collection policy diagnostics and low-intent filtering."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.analysis.reddit_retention import analyze_reddit_retention, render_reddit_retention_report
from src.collectors.reddit_collector import _build_reddit_search_text, _resolve_seed_page_cap, _should_collect_reddit_post, _should_fetch_reddit_comments
from src.utils.io import write_jsonl, write_parquet


class RedditRetentionDiagnosticsTests(unittest.TestCase):
    """Verify Reddit retention reporting and collector-side policy helpers."""

    def test_analyze_reddit_retention_writes_seed_and_subreddit_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config" / "sources").mkdir(parents=True, exist_ok=True)
            (root / "config" / "sources" / "reddit.yaml").write_text(
                "query_mode: source_config\ncomment_expansion_mode: conditional\ndefault_per_seed_page_cap: 3\nminimum_rolling_retention_threshold: 0.12\n",
                encoding="utf-8",
            )
            write_jsonl(
                root / "data" / "raw" / "reddit" / "raw.jsonl",
                [
                    {
                        "source": "reddit",
                        "raw_id": "r1",
                        "query_seed": "excel after dashboard",
                        "subreddit_or_forum": "r/excel",
                        "source_meta": {"subreddit_name_prefixed": "r/excel"},
                    },
                    {
                        "source": "reddit",
                        "raw_id": "r2",
                        "query_seed": "excel after dashboard",
                        "subreddit_or_forum": "r/excel",
                        "source_meta": {"subreddit_name_prefixed": "r/excel"},
                    },
                    {
                        "source": "reddit",
                        "raw_id": "r3",
                        "query_seed": "conversion drop",
                        "subreddit_or_forum": "r/ppcagencies",
                        "source_meta": {"subreddit_name_prefixed": "r/ppcagencies"},
                    },
                ],
            )
            write_parquet(
                pd.DataFrame(
                    [
                        {"source": "reddit", "raw_id": "r1", "query_seed": "excel after dashboard", "subreddit_or_forum": "r/excel"},
                        {"source": "reddit", "raw_id": "r2", "query_seed": "excel after dashboard", "subreddit_or_forum": "r/excel"},
                    ]
                ),
                root / "data" / "valid" / "valid_candidates.parquet",
            )
            write_parquet(
                pd.DataFrame(
                    [
                        {"source": "reddit", "raw_id": "r1", "query_seed": "excel after dashboard", "subreddit_or_forum": "r/excel"},
                    ]
                ),
                root / "data" / "prefilter" / "relevance_keep.parquet",
            )
            write_parquet(
                pd.DataFrame(
                    [
                        {"source": "reddit", "raw_id": "r2", "query_seed": "excel after dashboard", "subreddit_or_forum": "r/excel"},
                    ]
                ),
                root / "data" / "prefilter" / "relevance_borderline.parquet",
            )

            outputs = analyze_reddit_retention(root, min_raw_threshold=1)
            seed_df = pd.read_csv(outputs["seed_csv"])
            subreddit_df = pd.read_csv(outputs["subreddit_csv"])
            policy_df = pd.read_csv(outputs["policy_csv"])

            excel_seed = seed_df[seed_df["group_value"] == "excel after dashboard"].iloc[0]
            self.assertEqual(int(excel_seed["raw_count"]), 2)
            self.assertEqual(int(excel_seed["kept_count"]), 1)
            self.assertEqual(int(excel_seed["borderline_count"]), 1)
            self.assertAlmostEqual(float(excel_seed["raw_to_prefilter_retention"]), 1.0)

            low_yield_subreddit = subreddit_df[subreddit_df["group_value"] == "r/ppcagencies"].iloc[0]
            self.assertEqual(int(low_yield_subreddit["raw_count"]), 1)
            self.assertEqual(int(low_yield_subreddit["prefiltered_count"]), 0)
            self.assertIn("default_per_seed_page_cap", set(policy_df["policy_name"].astype(str)))

    def test_search_text_appends_negative_keywords(self) -> None:
        query = _build_reddit_search_text("excel after dashboard", ["job", "homework", "stock market news"])
        self.assertIn("-job", query)
        self.assertIn("-homework", query)
        self.assertIn('-"stock market news"', query)

    def test_should_collect_reddit_post_prefers_target_subreddit(self) -> None:
        should_collect, reason = _should_collect_reddit_post(
            {
                "subreddit_name_prefixed": "r/analytics",
                "title": "Dashboard numbers don't match export",
                "selftext": "I still export to Excel every week to reconcile campaign metrics.",
            },
            {
                "preferred_subreddits": ["analytics", "excel"],
                "deny_subreddit_patterns": [],
                "precollector_negative_keywords": ["remote job"],
                "precollector_required_signal_terms": ["dashboard", "excel"],
                "precollector_problem_terms": ["match", "reconcile"],
                "precollector_workflow_terms": ["dashboard", "excel"],
            },
        )
        self.assertTrue(should_collect)
        self.assertEqual(reason, "preferred_subreddit")

    def test_should_collect_reddit_post_skips_obvious_low_yield_candidate(self) -> None:
        should_collect, reason = _should_collect_reddit_post(
            {
                "subreddit_name_prefixed": "r/ppcagencies",
                "title": "Remote Job - Analytics Manager",
                "selftext": "Salary range and application details.",
            },
            {
                "preferred_subreddits": ["analytics", "excel"],
                "deny_subreddit_patterns": ["^r/ppcagencies$"],
                "precollector_negative_keywords": ["remote job", "salary"],
                "precollector_required_signal_terms": ["dashboard", "excel"],
                "precollector_problem_terms": ["match", "reconcile"],
                "precollector_workflow_terms": ["dashboard", "excel"],
            },
        )
        self.assertFalse(should_collect)
        self.assertEqual(reason, "deny_subreddit")

    def test_should_fetch_reddit_comments_skips_when_body_is_already_dense(self) -> None:
        should_fetch, reason = _should_fetch_reddit_comments(
            {
                "num_comments": 12,
                "is_self": True,
                "selftext": "x" * 260,
            },
            {
                "comment_expansion_mode": "conditional",
                "comment_expand_body_char_threshold": 220,
                "comment_expand_max_posts_per_page": 3,
                "comment_expand_max_posts_per_query": 8,
            },
            expanded_on_page=0,
            expanded_on_query=0,
        )
        self.assertFalse(should_fetch)
        self.assertEqual(reason, "sufficient_body")

    def test_should_fetch_reddit_comments_honors_page_limit(self) -> None:
        should_fetch, reason = _should_fetch_reddit_comments(
            {
                "num_comments": 12,
                "is_self": False,
                "selftext": "",
            },
            {
                "comment_expansion_mode": "conditional",
                "comment_expand_body_char_threshold": 220,
                "comment_expand_max_posts_per_page": 1,
                "comment_expand_max_posts_per_query": 8,
            },
            expanded_on_page=1,
            expanded_on_query=0,
        )
        self.assertFalse(should_fetch)
        self.assertEqual(reason, "page_limit")

    def test_resolve_seed_page_cap_prefers_seed_override(self) -> None:
        page_cap = _resolve_seed_page_cap(
            "manual reporting after dashboard",
            {
                "default_per_seed_page_cap": 3,
                "per_seed_page_caps": {
                    "manual reporting after dashboard": 2,
                    "excel after dashboard": 4,
                },
            },
        )
        self.assertEqual(page_cap, 2)

    def test_render_reddit_retention_report_includes_policy_and_runtime_sections(self) -> None:
        seed_df = pd.DataFrame(
            [
                {
                    "group_value": "excel after dashboard",
                    "raw_count": 2,
                    "valid_count": 2,
                    "kept_count": 1,
                    "borderline_count": 1,
                    "prefiltered_count": 2,
                    "raw_to_valid_retention": 1.0,
                    "raw_to_prefilter_retention": 1.0,
                    "valid_to_prefilter_retention": 1.0,
                }
            ]
        )
        subreddit_df = pd.DataFrame(
            [
                {
                    "group_value": "r/excel",
                    "raw_count": 2,
                    "valid_count": 2,
                    "kept_count": 1,
                    "borderline_count": 1,
                    "prefiltered_count": 2,
                    "raw_to_valid_retention": 1.0,
                    "raw_to_prefilter_retention": 1.0,
                    "valid_to_prefilter_retention": 1.0,
                }
            ]
        )
        seed_subreddit_df = pd.DataFrame(
            [
                {
                    "query_seed": "excel after dashboard",
                    "subreddit_or_forum": "r/excel",
                    "raw_count": 2,
                    "valid_count": 2,
                    "kept_count": 1,
                    "borderline_count": 1,
                    "prefiltered_count": 2,
                    "raw_to_valid_retention": 1.0,
                    "raw_to_prefilter_retention": 1.0,
                    "valid_to_prefilter_retention": 1.0,
                }
            ]
        )
        policy_df = pd.DataFrame(
            [
                {"policy_name": "comment_expansion_mode", "policy_value": "conditional"},
                {"policy_name": "minimum_rolling_retention_threshold", "policy_value": "0.12"},
            ]
        )
        runtime_df = pd.DataFrame(
            [
                {"metric_name": "pagination_seed_page_cap_stop_count", "metric_value": 2},
                {"metric_name": "pagination_rolling_retention_stop_count", "metric_value": 1},
            ]
        )

        report = render_reddit_retention_report(
            seed_df,
            subreddit_df,
            seed_subreddit_df,
            policy_df=policy_df,
            runtime_df=runtime_df,
            min_raw_threshold=1,
        )

        self.assertIn("## Policy Snapshot", report)
        self.assertIn("comment_expansion_mode", report)
        self.assertIn("## Latest Runtime Signals", report)
        self.assertIn("pagination_seed_page_cap_stop_count", report)


if __name__ == "__main__":
    unittest.main()