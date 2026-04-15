"""Tests for public business community parsing and normalization."""

from __future__ import annotations

from pathlib import Path
import unittest

from src.collectors.business_community_parser import (
    ThreadLink,
    canonicalize_business_url,
    discover_rss_thread_links,
    discover_sitemap_thread_links,
    discover_thread_links,
    parse_thread_page,
)
from src.normalizers.business_community_normalizer import BusinessCommunityNormalizer

ROOT = Path(__file__).resolve().parents[1]


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

    def test_mixpanel_rss_discovery_supports_question_paths(self) -> None:
        xml = """
        <rss version="2.0"><channel>
        <item>
            <title><![CDATA[Why are funnel numbers different?]]></title>
            <link>https://community.mixpanel.com/x/questions/abcd1234/funnel-numbers-different</link>
            <description><![CDATA[The dashboard says one thing and the export says another.]]></description>
        </item>
        </channel></rss>
        """
        links = discover_rss_thread_links(xml, "https://community.mixpanel.com/x/questions/rss.xml", "mixpanel", "Questions")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].url, "https://community.mixpanel.com/x/questions/abcd1234/funnel-numbers-different")

    def test_mixpanel_sitemap_discovery_supports_question_paths(self) -> None:
        xml = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url>
            <loc>https://community.mixpanel.com/x/questions/abcd1234/funnel-numbers-different</loc>
            <lastmod>2026-04-13T17:00:50.385Z</lastmod>
          </url>
          <url>
            <loc>https://community.mixpanel.com/x/ask-ai/abcd1234/funnel-numbers-different</loc>
          </url>
        </urlset>
        """
        links = discover_sitemap_thread_links(xml, "https://community.mixpanel.com/sitemap-posts/0.xml", "mixpanel", "Questions")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].url, "https://community.mixpanel.com/x/questions/abcd1234/funnel-numbers-different")

    def test_qlik_thread_discovery_supports_td_p_urls(self) -> None:
        html = """
        <a href="/t5/Visualization-and-Usability/Wrong-total-in-chart/td-p/2546782">Wrong total in chart</a>
        <a href="/t5/user/viewprofilepage/user-id/340707">Profile</a>
        """
        links = discover_thread_links(html, "https://community.qlik.com/t5/Visualization-and-Usability/bd-p/new-to-qlik-sense", "qlik")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].url, "https://community.qlik.com/t5/Visualization-and-Usability/Wrong-total-in-chart/td-p/2546782")

    def test_amplitude_thread_discovery_supports_discussion_urls(self) -> None:
        html = """
        <a href="/discussion/1234/chart-does-not-match-expected-user-count">Chart does not match expected user count</a>
        <a href="/events/some-event">Event</a>
        """
        links = discover_thread_links(html, "https://community.amplitude.com/discussions", "amplitude")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].url, "https://community.amplitude.com/discussion/1234/chart-does-not-match-expected-user-count")

    def test_sisense_thread_discovery_supports_m_p_urls(self) -> None:
        html = """
        <a href="/t5/Help-and-How-To/widget-value-does-not-match-table/m-p/26688">widget value does not match table</a>
        <a href="/t5/s/profile">profile</a>
        """
        links = discover_thread_links(html, "https://community.sisense.com/", "sisense")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].url, "https://community.sisense.com/t5/Help-and-How-To/widget-value-does-not-match-table/m-p/26688")

    def test_sisense_thread_discovery_supports_next_data_listing_payload(self) -> None:
        html = """
        <script id="__NEXT_DATA__" type="application/json">
        {
          "props": {
            "pageProps": {
              "apolloState": {
                "Forum:board:help_and_how_to": {
                  "__typename": "Forum",
                  "title": "Help and How-To"
                },
                "ForumTopicMessage:message:29039": {
                  "__typename": "ForumTopicMessage",
                  "uid": 29039,
                  "subject": "Dashboard script: automatically reset filters to default when a dashboard is opened",
                  "board": {"__ref": "Forum:board:help_and_how_to"},
                  "repliesCount": 2,
                  "postTime": "2026-04-14T07:18:13.878-07:00"
                }
              }
            }
          }
        }
        </script>
        """
        links = discover_thread_links(html, "https://community.sisense.com/t5/Help-and-How-To/bg-p/help_and_how_to", "sisense")
        self.assertEqual(len(links), 1)
        self.assertEqual(
            links[0].url,
            "https://community.sisense.com/t5/Help-and-How-To/dashboard-script-automatically-reset-filters-to-default-when-a-dashboard-is-opened/m-p/29039",
        )
        self.assertEqual(links[0].reply_count, 2)

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
