"""Tests for robots-aware public fetch helpers."""

from __future__ import annotations

from email.message import Message
import unittest
from unittest.mock import patch

from src.utils.http_fetch import check_robots_allowed


class _FakeResponse:
    """Minimal urlopen-like response used for robots tests."""

    def __init__(self, body_text: str) -> None:
        self._body = body_text.encode("utf-8")
        self.headers = Message()
        self.headers.add_header("Content-Type", "text/plain; charset=utf-8")
        self.status = 200

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class HttpFetchTests(unittest.TestCase):
    """Verify robots parsing honors fetched robots.txt text."""

    def test_check_robots_allowed_parses_fetched_text(self) -> None:
        robots_text = "User-agent: *\nDisallow: /private\n"
        with patch("src.utils.http_fetch.urlopen", return_value=_FakeResponse(robots_text)):
            allowed, reason = check_robots_allowed(
                "https://community.sisense.com/t5/Help-and-How-To/bg-p/help_and_how_to",
                "Mozilla/5.0",
            )
        self.assertTrue(allowed)
        self.assertEqual(reason, "")


if __name__ == "__main__":
    unittest.main()
