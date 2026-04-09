"""Parser tests for new source-group ingestion paths."""

from __future__ import annotations

import json
from pathlib import Path
import unittest

from src.collectors.official_community_parser import parse_feed_entries, parse_official_community_html
from src.collectors.reddit_public_parser import parse_reddit_comment_payload, parse_reddit_listing_payload
from src.collectors.review_site_parser import parse_review_html

FIXTURES = Path(__file__).resolve().parent / "fixtures"


class SourceGroupParserTests(unittest.TestCase):
    """Verify synthetic fixtures parse into structured rows."""

    def test_review_html_parser_extracts_review_metadata(self) -> None:
        rows = parse_review_html(
            (FIXTURES / "review_page.html").read_text(encoding="utf-8"),
            source_name="g2",
            canonical_url="https://example.com/review",
            raw_file_path=str(FIXTURES / "review_page.html"),
            product_or_tool="Power BI",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["product_or_tool"], "Power BI")
        self.assertIn("dashboard trust", rows[0]["pain_point_hint"].lower())
        self.assertEqual(rows[0]["company_size_hint"], "201-500 employees")

    def test_reddit_json_parsers_extract_posts_and_comments(self) -> None:
        listing = json.loads((FIXTURES / "reddit_listing.json").read_text(encoding="utf-8"))
        comments = json.loads((FIXTURES / "reddit_comments.json").read_text(encoding="utf-8"))
        posts = parse_reddit_listing_payload(listing)
        comment_rows = parse_reddit_comment_payload(comments)
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]["subreddit_name_prefixed"], "r/excel")
        self.assertEqual(len(comment_rows), 1)
        self.assertIn("leadership review", comment_rows[0]["body"].lower())

    def test_official_community_parsers_extract_thread_and_feed(self) -> None:
        html_rows = parse_official_community_html(
            (FIXTURES / "official_thread.html").read_text(encoding="utf-8"),
            forum_name="Power BI Community",
            product_or_tool="Power BI",
            canonical_url="https://example.com/thread",
        )
        feed_rows = parse_feed_entries((FIXTURES / "official_feed.xml").read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(html_rows), 1)
        self.assertEqual(feed_rows[0]["title"], "Can't explain KPI change to stakeholders")
        self.assertEqual(html_rows[0]["product_or_tool"], "Power BI")


if __name__ == "__main__":
    unittest.main()
