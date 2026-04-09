"""Public-safe subreddit collector for BI and analytics user communities."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote

from src.collectors.base import BaseCollector, RawRecord
from src.collectors.reddit_public_parser import (
    parse_reddit_comment_payload,
    parse_reddit_listing_payload,
    reddit_timestamp_to_iso,
)
from src.utils.http_fetch import check_robots_allowed, fetch_text
from src.utils.logging import get_logger
from src.utils.text import make_hash_id

LOGGER = get_logger("collectors.reddit_public")


class RedditPublicCollector(BaseCollector):
    """Collect public subreddit threads plus top-level comments."""

    source_type = "forum"

    def __init__(self, source_name: str, config: dict[str, Any], data_dir: Path) -> None:
        self.source_name = source_name
        super().__init__(config=config, data_dir=data_dir)

    def collect(self) -> list[RawRecord]:
        """Collect from a public subreddit JSON listing."""
        subreddit = str(self.config.get("subreddit", "") or "").strip()
        if not subreddit:
            raise ValueError(f"Missing subreddit config for {self.source_name}")
        user_agent = os.getenv("REDDIT_USER_AGENT", os.getenv("PUBLIC_WEB_USER_AGENT", "persona-pipeline/0.1"))
        listing_url = f"https://www.reddit.com/r/{quote(subreddit)}/new.json?limit={int(self.config.get('max_posts_per_run', 10))}&raw_json=1"
        allowed, reason = check_robots_allowed(listing_url, user_agent=user_agent)
        if not allowed:
            return [self._blocked_record(reason)]
        listing_response = fetch_text(listing_url, user_agent=user_agent)
        if not listing_response.ok:
            return [self._blocked_record(listing_response.error_message or listing_response.crawl_status)]
        payload = json.loads(listing_response.body_text)
        posts = parse_reddit_listing_payload(payload)
        records: list[RawRecord] = []
        for post in posts:
            records.append(self._build_thread_record(post))
            comments_url = f"https://www.reddit.com/comments/{post.get('id', '')}.json?limit={int(self.config.get('max_comments_per_thread', 10))}&depth=1&raw_json=1"
            comment_allowed, _ = check_robots_allowed(comments_url, user_agent=user_agent)
            if not comment_allowed:
                continue
            comments_response = fetch_text(comments_url, user_agent=user_agent)
            if not comments_response.ok:
                continue
            comments_payload = json.loads(comments_response.body_text)
            comments = parse_reddit_comment_payload(comments_payload)
            records.extend(self._build_comment_records(post, comments))
        LOGGER.info("Collected %s Reddit rows for %s", len(records), self.source_name)
        return records

    def _build_thread_record(self, post: dict[str, Any]) -> RawRecord:
        """Build the normalized raw thread record."""
        permalink = str(post.get("permalink", "") or "")
        canonical_url = f"https://www.reddit.com{permalink}" if permalink else ""
        title = str(post.get("title", "") or "")
        body = str(post.get("selftext", "") or "")
        subreddit_name = str(post.get("subreddit_name_prefixed", f"r/{self.config.get('subreddit', '')}") or "")
        created_at = reddit_timestamp_to_iso(post.get("created_utc"))
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "reddit")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="thread",
            raw_id=str(post.get("id", "") or ""),
            raw_source_id=str(post.get("id", "") or ""),
            url=canonical_url,
            canonical_url=canonical_url,
            title=title,
            body=body,
            body_text=body,
            comments_text="",
            created_at=created_at,
            fetched_at=created_at,
            retrieved_at=created_at,
            query_seed=subreddit_name,
            author_hint=str(post.get("author", "") or ""),
            author_name=str(post.get("author", "") or ""),
            product_or_tool=self._infer_tool(title, body),
            subreddit_or_forum=subreddit_name,
            thread_title=title,
            parent_context="",
            workflow_hint="",
            pain_point_hint="",
            output_need_hint="",
            crawl_method="reddit_public_json",
            crawl_status="ok",
            manual_import_flag=False,
            raw_file_path="",
            parse_version="reddit_public_v1",
            hash_id=make_hash_id(self.source_name, post.get("id", ""), canonical_url, title, body),
            source_meta={
                "score": post.get("score", 0),
                "comment_count": post.get("num_comments", 0),
                "flair": post.get("link_flair_text", ""),
                "raw_post": post,
            },
        )

    def _build_comment_records(self, post: dict[str, Any], comments: list[dict[str, Any]]) -> list[RawRecord]:
        """Build top-level comment records linked to their parent thread."""
        title = str(post.get("title", "") or "")
        permalink = str(post.get("permalink", "") or "")
        canonical_url = f"https://www.reddit.com{permalink}" if permalink else ""
        subreddit_name = str(post.get("subreddit_name_prefixed", f"r/{self.config.get('subreddit', '')}") or "")
        rows: list[RawRecord] = []
        for comment in comments:
            body = str(comment.get("body", "") or "")
            if not body:
                continue
            created_at = reddit_timestamp_to_iso(comment.get("created_utc"))
            rows.append(
                RawRecord(
                    source=self.source_name,
                    source_group=str(self.config.get("source_group", "reddit")),
                    source_name=str(self.config.get("source_name", self.source_name)),
                    source_type="comment",
                    raw_id=str(comment.get("id", "") or ""),
                    raw_source_id=str(comment.get("id", "") or ""),
                    url=canonical_url,
                    canonical_url=canonical_url,
                    title=f"Comment on {title}",
                    body=body,
                    body_text=body,
                    comments_text="",
                    created_at=created_at,
                    fetched_at=created_at,
                    retrieved_at=created_at,
                    query_seed=subreddit_name,
                    author_hint=str(comment.get("author", "") or ""),
                    author_name=str(comment.get("author", "") or ""),
                    product_or_tool=self._infer_tool(title, body),
                    subreddit_or_forum=subreddit_name,
                    thread_title=title,
                    parent_context=title,
                    crawl_method="reddit_public_json",
                    crawl_status="ok",
                    manual_import_flag=False,
                    raw_file_path="",
                    parse_version="reddit_public_v1",
                    hash_id=make_hash_id(self.source_name, comment.get("id", ""), canonical_url, title, body),
                    source_meta={
                        "parent_post_id": post.get("id", ""),
                        "score": comment.get("score", 0),
                        "raw_comment": comment,
                    },
                )
            )
        return rows

    def _infer_tool(self, title: str, body: str) -> str:
        """Infer a BI tool keyword from thread text."""
        combined = f"{title} {body}".lower()
        for tool in ["Power BI", "Tableau", "Looker Studio", "Sigma", "Excel", "Google Analytics"]:
            if tool.lower() in combined:
                return tool
        return ""

    def _blocked_record(self, reason: str) -> RawRecord:
        """Return a non-fatal blocked row."""
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "reddit")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="status",
            raw_id=f"{self.source_name}-blocked",
            raw_source_id=f"{self.source_name}-blocked",
            url="",
            canonical_url="",
            title=f"{self.source_name} collection blocked",
            body=str(reason or "public collection unavailable"),
            body_text=str(reason or "public collection unavailable"),
            comments_text="",
            created_at="",
            fetched_at="",
            retrieved_at="",
            query_seed=str(self.config.get("subreddit", "")),
            crawl_method="reddit_public_json",
            crawl_status="blocked_or_manual_required",
            manual_import_flag=False,
            raw_file_path="",
            parse_version="reddit_public_v1",
            hash_id=make_hash_id(self.source_name, "blocked"),
            source_meta={"blocked_reason": reason},
        )
