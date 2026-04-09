"""Small helpers shared by rerunnable run scripts."""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: Path) -> None:
    """Load simple KEY=VALUE pairs from a local .env file when present."""
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def parse_csv_env_set(key: str) -> set[str]:
    """Return a normalized set from a comma-separated environment variable."""
    return {part.strip() for part in os.getenv(key, "").split(",") if part.strip()}
