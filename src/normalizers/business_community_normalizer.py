"""Normalizer for public business community thread rows."""

from __future__ import annotations

import json
from typing import Any

from src.normalizers.base import NormalizedPost, PassThroughNormalizer
from src.utils.text import clean_text, combine_text, make_dedupe_key


class BusinessCommunityNormalizer(PassThroughNormalizer):
    """Normalize business community rows while preserving source identity."""

    def normalize_row(self, row: dict[str, Any]) -> NormalizedPost:
        title = clean_text(row.get("title"))
        body = clean_text(row.get("body_text") or row.get("body"))
        comments_text = clean_text(row.get("comments_text"))
        raw_text = combine_text(title, body, comments_text)
        source_meta = dict(row.get("source_meta", {}) or {})
        return NormalizedPost(
            source=row["source"],
            source_group=row.get("source_group", "business_communities"),
            source_name=row.get("source_name", row.get("source", "")),
            source_type=row.get("source_type", "thread"),
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
            author_name=row.get("author_name", row.get("author_hint", "")),
            product_or_tool=row.get("product_or_tool", ""),
            subreddit_or_forum=row.get("subreddit_or_forum", ""),
            thread_title=row.get("thread_title", title),
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
            language=row.get("language", "en"),
            crawl_method=row.get("crawl_method", "public_html"),
            crawl_status=row.get("crawl_status", ""),
            manual_import_flag=bool(row.get("manual_import_flag", False)),
            raw_file_path=row.get("raw_file_path", ""),
            parse_version=row.get("parse_version", "business_community_v1"),
            hash_id=row.get("hash_id", ""),
            author_hint=row.get("author_hint", ""),
            source_meta={"json": json.dumps(source_meta, ensure_ascii=False, sort_keys=True)},
            dedupe_key=make_dedupe_key(row.get("source", ""), row.get("canonical_url", row.get("url", ""))),
            text_len=len(raw_text),
        )
