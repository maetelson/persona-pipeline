"""Filesystem and dataframe IO helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def ensure_dir(path: Path) -> Path:
    """Create a directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file into a dictionary."""
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write a list of dictionaries as JSONL."""
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a JSONL file into a list of dictionaries."""
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write a dataframe to parquet."""
    ensure_dir(path.parent)
    df.to_parquet(path, index=False)


def read_parquet(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a parquet file or return an empty dataframe if absent."""
    if not path.exists():
        return pd.DataFrame(columns=columns or [])
    return pd.read_parquet(path, columns=columns)


def list_jsonl_files(path: Path) -> list[Path]:
    """List JSONL files under a directory in deterministic order."""
    if not path.exists():
        return []
    return sorted(path.glob("*.jsonl"))
