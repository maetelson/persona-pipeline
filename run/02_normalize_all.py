"""Normalize raw JSONL into the shared normalized post parquet."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.normalizers.base import NORMALIZED_POST_COLUMNS
from src.normalizers.discourse_normalizer import DiscourseNormalizer
from src.normalizers.github_discussions_normalizer import GitHubDiscussionsNormalizer
from src.normalizers.hackernews_normalizer import HackerNewsNormalizer
from src.normalizers.reddit_normalizer import RedditNormalizer
from src.normalizers.stackoverflow_normalizer import StackOverflowNormalizer
from src.normalizers.youtube_normalizer import YouTubeNormalizer
from src.utils.io import list_jsonl_files, read_jsonl, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.normalize_all")

NORMALIZER_REGISTRY = {
    "reddit": RedditNormalizer(),
    "stackoverflow": StackOverflowNormalizer(),
    "github_discussions": GitHubDiscussionsNormalizer(),
    "discourse": DiscourseNormalizer(),
    "hackernews": HackerNewsNormalizer(),
    "youtube": YouTubeNormalizer(),
}


def main() -> None:
    """Normalize raw source files into a single shared parquet file."""
    frames: list[pd.DataFrame] = []
    for source_name, normalizer in NORMALIZER_REGISTRY.items():
        raw_dir = ROOT / "data" / "raw" / source_name
        rows: list[dict[str, object]] = []
        for file_path in list_jsonl_files(raw_dir):
            rows.extend(read_jsonl(file_path))
        LOGGER.info("Loaded %s raw rows for %s", len(rows), source_name)
        frames.append(normalizer.normalize_rows(rows))

    normalized_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=NORMALIZED_POST_COLUMNS)
    write_parquet(normalized_df, ROOT / "data" / "normalized" / "normalized_posts.parquet")
    LOGGER.info("Wrote normalized posts: %s", len(normalized_df))


if __name__ == "__main__":
    main()
