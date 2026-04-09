"""GitHub issues/discussions raw-to-normalized transformation."""

from __future__ import annotations

import json
from typing import Any

from src.normalizers.base import NormalizedPost, PassThroughNormalizer
from src.utils.text import clean_text, combine_text, make_dedupe_key


class GitHubDiscussionsNormalizer(PassThroughNormalizer):
    """Normalize GitHub issues and discussions into the shared post schema."""

    def normalize_row(self, row: dict[str, Any]) -> NormalizedPost:
        source_meta = dict(row.get("source_meta", {}) or {})
        content_kind = str(source_meta.get("content_kind", "unknown"))

        if content_kind == "issue":
            raw_issue = dict(source_meta.get("raw_issue", {}) or {})
            raw_issue_comments = list(source_meta.get("raw_issue_comments", []) or [])
            title = clean_text(raw_issue.get("title") or row.get("title"))
            body = clean_text(raw_issue.get("body") or row.get("body"))
            comments_text = clean_text(
                row.get("comments_text")
                or "\n\n".join(str(comment.get("body", "") or "") for comment in raw_issue_comments if comment.get("body"))
            )
            normalized_meta = {
                "collector_mode": source_meta.get("collector_mode", "unknown"),
                "content_kind": "issue",
                "repository": source_meta.get("repository", ""),
                "window_id": row.get("window_id", "") or source_meta.get("window_id", ""),
                "window_start": row.get("window_start", "") or source_meta.get("window_start", ""),
                "window_end": row.get("window_end", "") or source_meta.get("window_end", ""),
                "raw_issue": raw_issue,
                "raw_issue_comments": raw_issue_comments,
                "labels": raw_issue.get("labels", []),
                "state": raw_issue.get("state"),
                "comments_count": len(raw_issue_comments),
            }
        else:
            raw_discussion = dict(source_meta.get("raw_discussion", {}) or {})
            raw_discussion_comments = list(source_meta.get("raw_discussion_comments", []) or [])
            raw_discussion_replies = dict(source_meta.get("raw_discussion_replies", {}) or {})
            title = clean_text(raw_discussion.get("title") or row.get("title"))
            body = clean_text(raw_discussion.get("body") or row.get("body"))
            comments_text = clean_text(row.get("comments_text"))
            normalized_meta = {
                "collector_mode": source_meta.get("collector_mode", "unknown"),
                "content_kind": "discussion",
                "repository": source_meta.get("repository", ""),
                "window_id": row.get("window_id", "") or source_meta.get("window_id", ""),
                "window_start": row.get("window_start", "") or source_meta.get("window_start", ""),
                "window_end": row.get("window_end", "") or source_meta.get("window_end", ""),
                "raw_discussion": raw_discussion,
                "raw_discussion_comments": raw_discussion_comments,
                "raw_discussion_replies": raw_discussion_replies,
                "category": raw_discussion.get("category"),
                "comments_count": len(raw_discussion_comments),
                "reply_count": sum(len(replies) for replies in raw_discussion_replies.values()),
            }

        raw_text = combine_text(title, body, comments_text)
        return NormalizedPost(
            source=row["source"],
            source_group=row.get("source_group", ""),
            source_name=row.get("source_name", row.get("source", "")),
            source_type=row.get("source_type", "discussion"),
            raw_id=str(row["raw_id"]),
            raw_source_id=str(row.get("raw_source_id", row.get("raw_id", ""))),
            url=row.get("url", ""),
            canonical_url=row.get("canonical_url", row.get("url", "")),
            query_seed=row.get("query_seed", ""),
            title=title,
            body=body,
            body_text=body,
            comments_text=comments_text,
            raw_text=raw_text,
            created_at=row.get("created_at", ""),
            fetched_at=row.get("fetched_at", ""),
            retrieved_at=row.get("retrieved_at", row.get("fetched_at", "")),
            author_name=row.get("author_name", ""),
            product_or_tool=row.get("product_or_tool", ""),
            subreddit_or_forum=row.get("subreddit_or_forum", ""),
            thread_title=row.get("thread_title", "") or title,
            parent_context=row.get("parent_context", ""),
            role_hint=row.get("role_hint", ""),
            company_size_hint=row.get("company_size_hint", ""),
            industry_hint=row.get("industry_hint", ""),
            workflow_hint=row.get("workflow_hint", ""),
            pain_point_hint=row.get("pain_point_hint", ""),
            output_need_hint=row.get("output_need_hint", ""),
            dev_heavy_score=float(row.get("dev_heavy_score", 0.0) or 0.0),
            biz_user_score=float(row.get("biz_user_score", 0.0) or 0.0),
            relevance_score=float(row.get("relevance_score", 0.0) or 0.0),
            language="en",
            crawl_method=row.get("crawl_method", source_meta.get("crawl_method", "")),
            crawl_status=row.get("crawl_status", source_meta.get("crawl_status", "")),
            manual_import_flag=bool(row.get("manual_import_flag", False)),
            raw_file_path=row.get("raw_file_path", ""),
            parse_version=row.get("parse_version", "v1"),
            hash_id=row.get("hash_id", ""),
            author_hint=row.get("author_hint", ""),
            source_meta={"json": json.dumps(normalized_meta, ensure_ascii=False, sort_keys=True)},
            dedupe_key=make_dedupe_key("github", content_kind, source_meta.get("repository", ""), title, body),
            text_len=len(raw_text),
        )
