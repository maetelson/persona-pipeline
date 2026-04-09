"""Tests for centralized record access helpers."""

from __future__ import annotations

import unittest

from src.utils.record_access import (
    get_record_codes,
    get_record_id,
    get_record_source,
    get_record_text,
    is_valid_record,
)


class RecordAccessTests(unittest.TestCase):
    """Verify adapter helpers behave consistently across sparse records."""

    def test_accessors_read_basic_fields(self) -> None:
        record = {
            "episode_id": "ep-1",
            "source": "reddit",
            "normalized_episode": "Main text",
            "pain_codes": "P_MANUAL_REPORTING|P_DATA_QUALITY",
            "role_codes": "R_ANALYST",
        }
        self.assertEqual(get_record_id(record), "ep-1")
        self.assertEqual(get_record_source(record), "reddit")
        self.assertEqual(get_record_text(record), "Main text")
        self.assertEqual(
            get_record_codes(record),
            {
                "role_codes": ["R_ANALYST"],
                "moment_codes": [],
                "question_codes": [],
                "pain_codes": ["P_MANUAL_REPORTING", "P_DATA_QUALITY"],
                "env_codes": [],
                "workaround_codes": [],
                "output_codes": [],
                "fit_code": [],
            },
        )

    def test_is_valid_record_uses_required_fields(self) -> None:
        self.assertTrue(is_valid_record({"episode_id": "1", "normalized_episode": "x"}))
        self.assertFalse(is_valid_record({"episode_id": "", "normalized_episode": "x"}))


if __name__ == "__main__":
    unittest.main()
