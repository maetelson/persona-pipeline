"""Finalize one work cycle by staging, committing, and pushing changes."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.git_sync import run_git, validate_commit_message


def build_parser() -> argparse.ArgumentParser:
    """Build the task-finalizer CLI."""
    parser = argparse.ArgumentParser(description="Finalize one task by stage+commit+push.")
    parser.add_argument("message", help="Commit message following type(scope): short summary")
    parser.add_argument("--allow-empty", action="store_true", help="Exit successfully when there is nothing to commit.")
    return parser


def main() -> None:
    """Finalize the current task changes."""
    args = build_parser().parse_args()
    is_valid, error_message = validate_commit_message(args.message)
    if not is_valid:
        raise SystemExit(error_message)

    status = run_git(["status", "--short"], ROOT)
    if status.returncode != 0:
        raise SystemExit(status.stderr.strip() or "Unable to read git status.")
    if not status.stdout.strip():
        if args.allow_empty:
            print("No local changes to commit.")
            return
        raise SystemExit("No local changes to commit.")

    add = run_git(["add", "-A"], ROOT)
    if add.returncode != 0:
        raise SystemExit(add.stderr.strip() or "Failed to stage changes.")

    commit = run_git(["commit", "-m", args.message], ROOT)
    if commit.returncode != 0:
        raise SystemExit(commit.stderr.strip() or commit.stdout.strip() or "Failed to create commit.")

    push = run_git(["push"], ROOT)
    if push.returncode != 0:
        fallback = run_git(["push", "-u", "origin", "HEAD"], ROOT)
        if fallback.returncode != 0:
            raise SystemExit(fallback.stderr.strip() or fallback.stdout.strip() or "Failed to push commit.")
        print(commit.stdout.strip())
        print(fallback.stdout.strip() or fallback.stderr.strip())
        return

    print(commit.stdout.strip())
    print(push.stdout.strip() or push.stderr.strip())


if __name__ == "__main__":
    main()
