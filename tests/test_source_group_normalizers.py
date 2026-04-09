"""Schema mapping tests for new source-group normalizers."""

from __future__ import annotations

import unittest

from src.normalizers.official_community_normalizer import OfficialCommunityNormalizer
from src.normalizers.reddit_public_normalizer import RedditPublicNormalizer
from src.normalizers.review_site_normalizer import ReviewSiteNormalizer


class SourceGroupNormalizerTests(unittest.TestCase):
    """Verify unified schema mapping across new source groups."""

    def test_review_site_normalizer_maps_extended_fields(self) -> None:
        row = {
            "source": "g2",
            "source_group": "review_sites",
            "source_name": "g2",
            "source_type": "review",
            "raw_id": "r1",
            "raw_source_id": "r1",
            "canonical_url": "https://example.com/review",
            "url": "https://example.com/review",
            "query_seed": "manual_review_snapshot",
            "title": "Manual reporting in Excel",
            "body_text": "We export to Excel every week.",
            "author_name": "Jamie",
            "product_or_tool": "Power BI",
            "role_hint": "BI Analyst",
            "company_size_hint": "201-500",
            "industry_hint": "Retail",
            "workflow_hint": "board reporting",
            "pain_point_hint": "dashboard trust",
            "output_need_hint": "executive summary",
            "crawl_method": "manual_import",
            "crawl_status": "ok_manual_import",
            "manual_import_flag": True,
            "raw_file_path": "sample.html",
            "parse_version": "review_v1",
            "hash_id": "hash",
            "source_meta": {"rating": "4/5"},
        }
        normalized = ReviewSiteNormalizer().normalize_row(row)
        self.assertEqual(normalized.source_group, "review_sites")
        self.assertEqual(normalized.product_or_tool, "Power BI")
        self.assertTrue(normalized.manual_import_flag)

    def test_reddit_public_normalizer_preserves_thread_context(self) -> None:
        row = {
            "source": "reddit_r_excel",
            "source_group": "reddit",
            "source_name": "r/excel",
            "source_type": "comment",
            "raw_id": "c1",
            "raw_source_id": "c1",
            "url": "https://reddit.com/test",
            "canonical_url": "https://reddit.com/test",
            "query_seed": "r/excel",
            "title": "Comment on dashboard trust",
            "body_text": "We still reconcile source data.",
            "thread_title": "Monthly reporting takes too long",
            "parent_context": "Monthly reporting takes too long",
            "subreddit_or_forum": "r/excel",
            "crawl_method": "reddit_public_json",
            "crawl_status": "ok",
            "hash_id": "hash",
            "source_meta": {"score": 3},
        }
        normalized = RedditPublicNormalizer().normalize_row(row)
        self.assertEqual(normalized.thread_title, "Monthly reporting takes too long")
        self.assertEqual(normalized.subreddit_or_forum, "r/excel")

    def test_official_community_normalizer_maps_forum_context(self) -> None:
        row = {
            "source": "power_bi_community",
            "source_group": "official_communities",
            "source_name": "power_bi_community",
            "source_type": "thread",
            "raw_id": "t1",
            "raw_source_id": "t1",
            "url": "https://example.com/thread",
            "canonical_url": "https://example.com/thread",
            "query_seed": "Power BI Community",
            "title": "Can't explain KPI change",
            "body_text": "Finance exports to Excel every month.",
            "product_or_tool": "Power BI",
            "subreddit_or_forum": "Power BI Community Desktop",
            "thread_title": "Can't explain KPI change",
            "crawl_method": "public_html",
            "crawl_status": "ok",
            "hash_id": "hash",
            "source_meta": {"accepted_solution": "accepted_solution"},
        }
        normalized = OfficialCommunityNormalizer().normalize_row(row)
        self.assertEqual(normalized.product_or_tool, "Power BI")
        self.assertIn("Finance exports", normalized.body_text)


if __name__ == "__main__":
    unittest.main()
