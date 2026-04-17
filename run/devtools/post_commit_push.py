"""Git post-commit hook entrypoint for automatic push to the configured remote."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.git_sync import run_git


def main() -> None:
    """Push the current branch after a successful commit without blocking the commit."""
    push = run_git(["push"], ROOT)
    if push.returncode == 0:
        return

    fallback = run_git(["push", "-u", "origin", "HEAD"], ROOT)
    if fallback.returncode != 0:
        message = fallback.stderr.strip() or fallback.stdout.strip() or "Automatic push failed."
        print(f"[post-commit] {message}", file=sys.stderr)


if __name__ == "__main__":
    main()
