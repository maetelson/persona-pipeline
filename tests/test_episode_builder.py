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
        self.assertEqual(len(episodes_df), 1)
        self.assertEqual(int(debug_df.iloc[0]["episode_count"]), 1)
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


if __name__ == "__main__":
    unittest.main()
