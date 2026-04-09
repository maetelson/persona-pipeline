"""Tests for commit validation helpers."""

from __future__ import annotations

import unittest

from src.utils.git_sync import validate_commit_message


class GitSyncTests(unittest.TestCase):
    """Verify the tracked commit-message convention."""

    def test_accepts_valid_message(self) -> None:
        self.assertTrue(validate_commit_message("docs(repo): add sync flow")[0])

    def test_rejects_invalid_message(self) -> None:
        self.assertFalse(validate_commit_message("bad message")[0])
        self.assertFalse(validate_commit_message("")[0])


if __name__ == "__main__":
    unittest.main()
