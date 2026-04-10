"""Tests for public business community parsing and normalization."""

from __future__ import annotations

import unittest

from src.collectors.business_community_parser import (
    ThreadLink,
    canonicalize_business_url,
    discover_thread_links,
    parse_thread_page,
)
from src.normalizers.business_community_normalizer import BusinessCommunityNormalizer


class BusinessCommunitySourceTests(unittest.TestCase):
    """Verify Phase 1 business community helpers preserve source identity."""

    def test_discovery_dedupes_canonical_thread_urls(self) -> None:
        html = """
        <a href="/t/report-not-matching/123?utm_source=x">Report not matching</a>
        <a href="https://community.shopify.com/t/report-not-matching/123">Report not matching duplicate</a>
        <a href="/tag/reports">reports</a>
        """
        links = discover_thread_links(html, "https://community.shopify.com/c/data-analytics/293", "shopify")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].url, "https://community.shopify.com/t/report-not-matching/123")

    def test_hubspot_thread_parse_uses_json_ld_and_meta(self) -> None:
        html = """
        <html><head>
        <title>Custom report issue - HubSpot Community</title>
        <meta property="og:description" content="Our revenue report does not match the dashboard." />
        <script type="application/ld+json">
        {"@type":"DiscussionForumPosting","headline":"Custom report issue","datePublished":"2026-04-01T12:00:00Z","author":{"name":"ops_owner"},"commentCount":3}
        </script>
        </head><body><main>Our revenue report does not match the dashboard.</main></body></html>
        """
        parsed = parse_thread_page(
            html,
            url="https://community.hubspot.com/t5/Reporting-Dashboards/Custom-report-issue/td-p/123",
            platform="hubspot",
            fallback=ThreadLink(
                url="https://community.hubspot.com/t5/Reporting-Dashboards/Custom-report-issue/td-p/123",
                title="Custom report issue",
                board="Reporting & Analytics",
            ),
            product_or_tool="HubSpot",
        )
        self.assertEqual(parsed.title, "Custom report issue")
        self.assertEqual(parsed.reply_count, 3)
        self.assertEqual(parsed.author_name, "ops_owner")
        self.assertEqual(parsed.parse_status, "ok")

    def test_normalizer_preserves_business_source(self) -> None:
        normalized = BusinessCommunityNormalizer().normalize_row(
            {
                "source": "klaviyo_community",
                "source_group": "business_communities",
                "source_name": "Klaviyo Community",
                "source_type": "thread",
                "raw_id": "123",
                "canonical_url": "https://community.klaviyo.com/campaigns-77/campaign-report-issue-123",
                "url": "https://community.klaviyo.com/campaigns-77/campaign-report-issue-123",
                "query_seed": "campaign report issue",
                "title": "Campaign report issue",
                "body_text": "Campaign revenue is missing from the report.",
                "body": "Campaign revenue is missing from the report.",
                "comments_text": "",
                "created_at": "2026-04-01T12:00:00+00:00",
                "fetched_at": "2026-04-01T12:00:00+00:00",
                "product_or_tool": "Klaviyo",
                "subreddit_or_forum": "Campaigns",
                "source_meta": {"reply_count": 2},
            }
        )
        self.assertEqual(normalized.source, "klaviyo_community")
        self.assertEqual(normalized.source_group, "business_communities")
        self.assertEqual(normalized.product_or_tool, "Klaviyo")

    def test_klaviyo_canonical_url_drops_query(self) -> None:
        url = canonicalize_business_url(
            "/campaigns-77/campaign-report-issue-123?utm_source=x#reply",
            "https://community.klaviyo.com/marketing-30",
            "klaviyo",
        )
        self.assertEqual(url, "https://community.klaviyo.com/campaigns-77/campaign-report-issue-123")

    def test_google_support_thread_discovery_and_canonical_url(self) -> None:
        html = """
        <a href="/google-ads/thread/423847133/ads-not-run-issue?hl=en">Ads not run issue 0 Recommended Answers 0 Replies</a>
        <a href="/google-ads/thread/new?hl=en">Post a question</a>
        """
        links = discover_thread_links(html, "https://support.google.com/google-ads/threads?hl=en", "google_support")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].url, "https://support.google.com/google-ads/thread/423847133/ads-not-run-issue")
        self.assertEqual(links[0].title, "Ads not run issue")

    def test_google_support_parser_extracts_embedded_thread_body(self) -> None:
        html = r"""
        <html><head><title>Ad Group Disapproved - Google Ads Community</title></head>
        <body><script>
        [[123,null,null,null,null,null,210,null,\x22Ad Group Disapproved\x22,999,null,[0],
        \x22I am trying to advertise my arcade game, but my ad group got disapproved.\u003cbr\u003eCan anyone help me understand the policy issue?\x22,
        \x22en\x22]]
        </script></body></html>
        """
        parsed = parse_thread_page(
            html,
            url="https://support.google.com/google-ads/thread/423695610/ad-group-disapproved",
            platform="google_support",
            fallback=ThreadLink(
                url="https://support.google.com/google-ads/thread/423695610/ad-group-disapproved",
                title="Ad Group Disapproved",
                board="Google Ads Community",
            ),
            product_or_tool="Google Ads",
        )
        self.assertEqual(parsed.parse_status, "ok")
        self.assertIn("ad group got disapproved", parsed.body_text)


if __name__ == "__main__":
    unittest.main()
