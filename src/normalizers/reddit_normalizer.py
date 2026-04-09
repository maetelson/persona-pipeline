"""Reddit raw-to-normalized transformation."""

from __future__ import annotations

import json
from typing import Any

from src.normalizers.base import NormalizedPost, PassThroughNormalizer
from src.utils.text import clean_text, combine_text, make_dedupe_key


class RedditNormalizer(PassThroughNormalizer):
    """Normalize Reddit raw records into the shared post schema."""

    def normalize_row(self, row: dict[str, Any]) -> NormalizedPost:
        source_meta = dict(row.get("source_meta", {}) or {})
        raw_post = dict(source_meta.get("raw_post", {}) or {})
        raw_comments = list(source_meta.get("raw_comments", []) or [])

        title = clean_text(raw_post.get("title") or row.get("title"))
        body = clean_text(raw_post.get("selftext") or row.get("body"))
        comments_text = clean_text(
            row.get("comments_text")
            or "\n\n".join(str(comment.get("body", "") or "") for comment in raw_comments if comment.get("body"))
        )
        raw_text = combine_text(title, body, comments_text)

        normalized_source_meta = {
            "subreddit": raw_post.get("subreddit") or source_meta.get("subreddit"),
            "subreddit_name_prefixed": raw_post.get("subreddit_name_prefixed")
            or source_meta.get("subreddit_name_prefixed"),
            "num_comments": raw_post.get("num_comments") or source_meta.get("num_comments"),
            "score": raw_post.get("score") or source_meta.get("score"),
            "search_url": source_meta.get("search_url", ""),
            "collector_mode": source_meta.get("collector_mode", "unknown"),
            "window_id": row.get("window_id", "") or source_meta.get("window_id", ""),
            "window_start": row.get("window_start", "") or source_meta.get("window_start", ""),
            "window_end": row.get("window_end", "") or source_meta.get("window_end", ""),
            "raw_post": raw_post,
            "raw_comments": raw_comments,
        }

        return NormalizedPost(
            source=row["source"],
            source_type=row.get("source_type", "forum"),
            raw_id=str(row["raw_id"]),
            url=row.get("url", ""),
            query_seed=row.get("query_seed", ""),
            title=title,
            body=body,
            comments_text=comments_text,
            raw_text=raw_text,
            created_at=row.get("created_at", ""),
            fetched_at=row.get("fetched_at", ""),
            language="en",
            author_hint=row.get("author_hint", "") or str(raw_post.get("author", "") or ""),
            source_meta={"json": json.dumps(normalized_source_meta, ensure_ascii=False, sort_keys=True)},
            dedupe_key=make_dedupe_key("reddit", raw_post.get("subreddit", ""), title, body),
            text_len=len(raw_text),
        )
