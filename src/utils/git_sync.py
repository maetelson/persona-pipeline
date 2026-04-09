"""Helpers for commit-message validation and local git sync flows."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess

COMMIT_MESSAGE_PATTERN = re.compile(r"^(feat|fix|refactor|docs|test|chore)\(([a-z_]+)\): [a-z0-9].+$")
COMMIT_MESSAGE_EXAMPLE = "type(scope): short summary"


def validate_commit_message(message: str) -> tuple[bool, str]:
    """Validate the repository commit-message convention."""
    first_line = message.strip().splitlines()[0] if message.strip() else ""
    if not first_line:
        return False, f"Commit message is empty. Expected `{COMMIT_MESSAGE_EXAMPLE}`."
    if len(first_line) > 72:
        return False, "Commit summary is too long. Keep the first line under 72 characters."
    if not COMMIT_MESSAGE_PATTERN.match(first_line):
        return False, (
            "Commit message must match `type(scope): short summary` "
            "using one of feat|fix|refactor|docs|test|chore."
        )
    return True, ""


def run_git(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a git command and return the completed process."""
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
