"""Tests for subreddit-specific Reddit collection behavior."""

from __future__ import annotations

from src.collectors.reddit_public_collector import _should_skip_robots_check


def test_should_skip_robots_check_for_reddit_json_listing() -> None:
    """Reddit JSON listing endpoints should bypass the robots pre-check."""
    assert _should_skip_robots_check("https://www.reddit.com/r/analytics/new.json?limit=12&raw_json=1")


def test_should_skip_robots_check_for_reddit_json_comments() -> None:
    """Reddit JSON comment endpoints should bypass the robots pre-check."""
    assert _should_skip_robots_check("https://www.reddit.com/comments/abc123.json?limit=6&depth=1&raw_json=1")


def test_should_not_skip_robots_check_for_non_json_pages() -> None:
    """HTML Reddit pages should keep the normal robots behavior."""
    assert not _should_skip_robots_check("https://www.reddit.com/r/analytics/")
