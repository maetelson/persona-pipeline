"""Git commit-msg hook entrypoint for repository commit convention checks."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.git_sync import validate_commit_message


def main() -> None:
    """Validate the current commit message file."""
    if len(sys.argv) != 2:
        raise SystemExit("commit-msg hook requires a path to the commit message file.")
    message_path = Path(sys.argv[1])
    message = message_path.read_text(encoding="utf-8")
    is_valid, error_message = validate_commit_message(message)
    if not is_valid:
        raise SystemExit(error_message)


if __name__ == "__main__":
    main()
