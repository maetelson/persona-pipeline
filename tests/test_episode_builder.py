"""Tests for episode builder diagnostics and reply-schema handling."""

from __future__ import annotations

from pathlib import Path
import unittest

import pandas as pd

from src.episodes.builder import build_episode_outputs
from src.utils.io import load_yaml
from src.utils.record_access import serialize_source_meta

ROOT = Path(__file__).resolve().parents[1]


class EpisodeBuilderTests(unittest.TestCase):
    """Verify episode builder preserves title/body pain context for reply-like rows."""

    def test_hubspot_reply_row_uses_combined_title_body(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "hubspot_community",
                    "raw_id": "123",
                    "url": "https://example.com/thread/123",
                    "source_type": "community_message",
                    "title": "Exporting summarized data instead of unsummarized data",
                    "body": "I found a workaround. Exporting as CSV gives me one raw data file and one summarized file.",
                    "comments_text": "",
                    "thread_title": "Exporting summarized data instead of unsummarized data",
                    "parent_context": "",
                    "source_meta": serialize_source_meta(
                        {
                            "api_item": {
                                "depth": 1,
                                "subject": "Re: Exporting summarized data instead of unsummarized data",
                            }
                        }
                    ),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 0)
        self.assertEqual(int(debug_df.iloc[0]["episode_count"]), 0)
        self.assertTrue(bool(debug_df.iloc[0]["title_body_combined_used"]))

    def test_reply_without_context_gets_specific_drop_reason(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "hubspot_community",
                    "raw_id": "124",
                    "url": "https://example.com/thread/124",
                    "source_type": "community_message",
                    "title": "Anonymous & Revealed Contacts Reporting",
                    "body": "Short workaround reply only.",
                    "comments_text": "",
                    "thread_title": "Anonymous & Revealed Contacts Reporting",
                    "parent_context": "",
                    "source_meta": serialize_source_meta(
                        {
                            "api_item": {
                                "depth": 1,
                                "subject": "Re: Anonymous & Revealed Contacts Reporting",
                            }
                        }
                    ),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 0)
        self.assertEqual(str(debug_df.iloc[0]["drop_reason"]), "title_body_merge_failure")

    def test_shopify_funnel_drop_can_promote_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "shopify_community",
                    "raw_id": "shopify-1",
                    "url": "https://example.com/thread/shopify-1",
                    "source_type": "thread",
                    "title": "Sales Funnel Feedback",
                    "body": "My conversion rate dropped from 2% to 0.5% and checkout drop-off is high. I am analyzing sessions, sales, and funnel stages to figure out what changed.",
                    "comments_text": "",
                    "thread_title": "Sales Funnel Feedback",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "shopify"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 1)
        self.assertEqual(int(debug_df.iloc[0]["episode_count"]), 1)
        self.assertEqual(str(debug_df.iloc[0]["drop_reason"]), "")
        self.assertIn(str(episodes_df.iloc[0]["quality_bucket"]), {"hard_pass", "borderline"})

    def test_shopify_discussion_style_can_be_borderline(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "shopify_community",
                    "raw_id": "shopify-2",
                    "url": "https://example.com/thread/shopify-2",
                    "source_type": "thread",
                    "title": "How do you handle monthly sales comparison?",
                    "body": "I am curious how other store owners interpret weekly and monthly sales trends when analytics looks off.",
                    "comments_text": "",
                    "thread_title": "How do you handle monthly sales comparison?",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "shopify"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 1)
        self.assertIn(str(episodes_df.iloc[0]["quality_bucket"]), {"hard_pass", "borderline"})
        self.assertIn(str(debug_df.iloc[0]["quality_bucket"]), {"hard_pass", "borderline"})

    def test_shopify_reconciliation_context_rescues_weak_problem_phrasing(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "shopify_community",
                    "raw_id": "shopify-3",
                    "url": "https://example.com/thread/shopify-3",
                    "source_type": "thread",
                    "title": "Numbers for finance sign-off",
                    "body": "We validate Shopify payouts against bank deposits, GA4, and Google Ads before sending the weekly report because totals look off.",
                    "comments_text": "",
                    "thread_title": "Numbers for finance sign-off",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "shopify"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 1)
        self.assertIn(str(debug_df.iloc[0]["quality_bucket"]), {"hard_pass", "borderline"})

    def test_klaviyo_setup_tips_do_not_form_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-1",
                    "url": "https://example.com/thread/klaviyo-1",
                    "source_type": "thread",
                    "title": "Need subject line ideas",
                    "body": "What are the best practices for popup design and welcome series subject lines for a new list?",
                    "comments_text": "",
                    "thread_title": "Need subject line ideas",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "klaviyo"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 0)
        self.assertEqual(str(debug_df.iloc[0]["quality_bucket"]), "fail")

    def test_klaviyo_reporting_mismatch_forms_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-2",
                    "url": "https://example.com/thread/klaviyo-2",
                    "source_type": "thread",
                    "title": "Attributed revenue no longer matches benchmark report",
                    "body": "Our segment count and attributed revenue export are not matching the benchmark report, and we are trying to figure out what changed before sharing the weekly performance summary.",
                    "comments_text": "",
                    "thread_title": "Attributed revenue no longer matches benchmark report",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "klaviyo"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 1)
        self.assertIn(str(debug_df.iloc[0]["quality_bucket"]), {"hard_pass", "borderline"})

    def test_mixpanel_api_setup_noise_does_not_form_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "raw_id": "mixpanel-1",
                    "url": "https://example.com/thread/mixpanel-1",
                    "source_type": "thread",
                    "title": "Need help sending events with SDK",
                    "body": "How do I send events with the mobile SDK webhook API for a new implementation?",
                    "comments_text": "",
                    "thread_title": "Need help sending events with SDK",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "mixpanel"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 0)
        self.assertEqual(str(debug_df.iloc[0]["quality_bucket"]), "fail")

    def test_mixpanel_reporting_trust_forms_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "raw_id": "mixpanel-2",
                    "url": "https://example.com/thread/mixpanel-2",
                    "source_type": "thread",
                    "title": "Dashboard says one thing export says another",
                    "body": "Our funnel insights and CSV export are not matching, and we need a source of truth before sharing the report.",
                    "comments_text": "",
                    "thread_title": "Dashboard says one thing export says another",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "mixpanel"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 1)
        self.assertIn(str(debug_df.iloc[0]["quality_bucket"]), {"hard_pass", "borderline"})

    def test_sisense_infra_noise_does_not_form_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "sisense_community",
                    "raw_id": "sisense-1",
                    "url": "https://example.com/thread/sisense-1",
                    "source_type": "thread",
                    "title": "JWT token not working after upgrade",
                    "body": "Our Kubernetes deployment needs Auth0 and SSO troubleshooting after install.",
                    "comments_text": "",
                    "thread_title": "JWT token not working after upgrade",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "sisense"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 0)
        self.assertEqual(str(debug_df.iloc[0]["quality_bucket"]), "fail")

    def test_sisense_dashboard_trust_forms_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "sisense_community",
                    "raw_id": "sisense-2",
                    "url": "https://example.com/thread/sisense-2",
                    "source_type": "thread",
                    "title": "Widget value does not match table",
                    "body": "The dashboard export and widget totals are not matching the source data, and we are replacing SSRS with live detail reporting.",
                    "comments_text": "",
                    "thread_title": "Widget value does not match table",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "sisense"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 1)
        self.assertIn(str(debug_df.iloc[0]["quality_bucket"]), {"hard_pass", "borderline"})

    def test_stackoverflow_bi_reconciliation_forms_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "stackoverflow",
                    "raw_id": "so-episode-1",
                    "url": "https://example.com/thread/so-episode-1",
                    "source_type": "thread",
                    "title": "Power BI wrong total after date filter",
                    "body": "Our dashboard total is not matching the SQL Server query result, and we need a source of truth before sending the report to stakeholders.",
                    "comments_text": "",
                    "thread_title": "Power BI wrong total after date filter",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "stackoverflow"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 1)
        self.assertIn(str(debug_df.iloc[0]["quality_bucket"]), {"hard_pass", "borderline"})

    def test_stackoverflow_generic_export_help_does_not_form_episode(self) -> None:
        rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
        df = pd.DataFrame(
            [
                {
                    "source": "stackoverflow",
                    "raw_id": "so-episode-2",
                    "url": "https://example.com/thread/so-episode-2",
                    "source_type": "thread",
                    "title": "Strapi export csv with React frontend",
                    "body": "How do I export CSV from a React app with a Strapi API and JavaScript table component?",
                    "comments_text": "",
                    "thread_title": "Strapi export csv with React frontend",
                    "parent_context": "",
                    "source_meta": serialize_source_meta({"platform": "stackoverflow"}),
                }
            ]
        )
        episodes_df, debug_df, _ = build_episode_outputs(df, rules)
        self.assertEqual(len(episodes_df), 0)
        self.assertEqual(str(debug_df.iloc[0]["quality_bucket"]), "fail")


if __name__ == "__main__":
    unittest.main()
