"""Normalize raw JSONL into the shared normalized post parquet."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.normalizers.base import NORMALIZED_POST_COLUMNS
from src.normalizers.business_community_normalizer import BusinessCommunityNormalizer
from src.normalizers.discourse_normalizer import DiscourseNormalizer
from src.normalizers.github_discussions_normalizer import GitHubDiscussionsNormalizer
from src.normalizers.reddit_normalizer import RedditNormalizer
from src.normalizers.reddit_public_normalizer import RedditPublicNormalizer
from src.normalizers.stackoverflow_normalizer import StackOverflowNormalizer
from src.utils.io import list_jsonl_files, read_jsonl, write_parquet
from src.utils.logging import get_logger
from src.utils.source_registry import load_source_definitions

LOGGER = get_logger("run.normalize_all")

NORMALIZER_REGISTRY = {
    "reddit": RedditNormalizer(),
    "stackoverflow": StackOverflowNormalizer(),
    "github_discussions": GitHubDiscussionsNormalizer(),
    "discourse": DiscourseNormalizer(),
}


def _extend_registry_with_source_groups():
    """Attach config-driven source-group normalizers to the registry."""
    registry = dict(NORMALIZER_REGISTRY)
    definitions = load_source_definitions(ROOT, include_disabled=True)
    normalizer_map = {
        "business_communities": BusinessCommunityNormalizer,
        "discourse": DiscourseNormalizer,
        "reddit": RedditPublicNormalizer,
    }
    for definition in definitions:
        normalizer_cls = normalizer_map.get(definition.normalizer_kind)
        if normalizer_cls is None:
            continue
        registry[definition.source_id] = normalizer_cls()
    if any(definition.normalizer_kind == "discourse" and definition.source_id != "discourse" for definition in definitions):
        registry.pop("discourse", None)
    return registry


def main() -> None:
    """Normalize raw source files into a single shared parquet file."""
    frames: list[pd.DataFrame] = []
    for source_name, normalizer in _extend_registry_with_source_groups().items():
        raw_dir = ROOT / "data" / "raw" / source_name
        rows: list[dict[str, object]] = []
        for file_path in list_jsonl_files(raw_dir):
            rows.extend(read_jsonl(file_path))
        LOGGER.info("Loaded %s raw rows for %s", len(rows), source_name)
        normalized_df = normalizer.normalize_rows(rows)
        write_parquet(normalized_df, ROOT / "data" / "normalized" / f"{source_name}.parquet")
        frames.append(normalized_df)

    normalized_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=NORMALIZED_POST_COLUMNS)
    write_parquet(normalized_df, ROOT / "data" / "normalized" / "normalized_posts.parquet")
    LOGGER.info("Wrote normalized posts: %s", len(normalized_df))


if __name__ == "__main__":
    main()
