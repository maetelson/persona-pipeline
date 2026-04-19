"""Tests for source-aware time-window rescue."""

from __future__ import annotations

from pathlib import Path
import unittest

import pandas as pd

from src.filters.time_window import apply_time_window_filter
from src.utils.io import load_yaml

ROOT = Path(__file__).resolve().parents[1]


class TimeWindowFilterTests(unittest.TestCase):
    """Verify evergreen source-specific rescue does not over-open stale content."""

    def setUp(self) -> None:
        self.config = load_yaml(ROOT / "config" / "time_window.yaml")

    def test_qlik_evergreen_reporting_pain_can_survive_outside_window(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "qlik_community",
                    "title": "Wrong totals in straight table export",
                    "body": "Our set analysis totals are not matching the chart export and we still use this for manual reporting.",
                    "raw_text": "",
                    "comments_text": "",
                    "thread_title": "",
                    "created_at": "2020-10-01T00:00:00+00:00",
                }
            ]
        )
        valid_df, invalid_df, _ = apply_time_window_filter(frame, self.config)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_qlik_export_total_line_issue_can_survive_outside_window(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "qlik_community",
                    "title": "Export to Excel not exporting the total line",
                    "body": "Our board report export to Excel changes the total line and wrong values are exported in ad hoc reporting.",
                    "raw_text": "",
                    "comments_text": "",
                    "thread_title": "",
                    "created_at": "2020-08-01T00:00:00+00:00",
                }
            ]
        )
        valid_df, invalid_df, _ = apply_time_window_filter(frame, self.config)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_metabase_old_version_release_note_stays_outside_window(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "metabase_discussions",
                    "title": "Metabase v0.35 export issue after upgrade guide",
                    "body": "Release notes say the old version changed CSV export behavior.",
                    "raw_text": "",
                    "comments_text": "",
                    "thread_title": "",
                    "created_at": "2020-10-01T00:00:00+00:00",
                }
            ]
        )
        valid_df, invalid_df, _ = apply_time_window_filter(frame, self.config)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(invalid_df), 1)
        self.assertEqual(str(invalid_df.iloc[0]["invalid_reason"]), "outside_time_window")


if __name__ == "__main__":
    unittest.main()
