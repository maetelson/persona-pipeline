"""Stack Overflow raw-to-normalized transformation."""

from __future__ import annotations

import json
from typing import Any

from src.normalizers.base import NormalizedPost, PassThroughNormalizer
from src.utils.text import clean_text, combine_text, make_dedupe_key


class StackOverflowNormalizer(PassThroughNormalizer):
    """Normalize Stack Overflow raw records into the shared post schema."""

    def normalize_row(self, row: dict[str, Any]) -> NormalizedPost:
        source_meta = dict(row.get("source_meta", {}) or {})
        raw_question = dict(source_meta.get("raw_question", {}) or {})
        raw_answers = list(source_meta.get("raw_answers", []) or [])
        raw_question_comments = list(source_meta.get("raw_question_comments", []) or [])
        raw_answer_comments = dict(source_meta.get("raw_answer_comments", {}) or {})

        title = clean_text(raw_question.get("title") or row.get("title"))
        body = clean_text(raw_question.get("body_markdown") or raw_question.get("body") or row.get("body"))
        comments_text = clean_text(row.get("comments_text"))

        answer_text = "\n\n".join(
            clean_text(answer.get("body_markdown") or answer.get("body", ""))
            for answer in raw_answers
            if answer.get("body_markdown") or answer.get("body")
        )
        raw_text = combine_text(title, body, answer_text, comments_text)

        normalized_source_meta = {
            "collector_mode": source_meta.get("collector_mode", "unknown"),
            "site": source_meta.get("site", "stackoverflow"),
            "window_id": row.get("window_id", "") or source_meta.get("window_id", ""),
            "window_start": row.get("window_start", "") or source_meta.get("window_start", ""),
            "window_end": row.get("window_end", "") or source_meta.get("window_end", ""),
            "raw_question": raw_question,
            "raw_question_comments": raw_question_comments,
            "raw_answers": raw_answers,
            "raw_answer_comments": raw_answer_comments,
            "answer_count": len(raw_answers),
            "question_comment_count": len(raw_question_comments),
            "answer_comment_count": sum(len(comments) for comments in raw_answer_comments.values()),
            "tags": raw_question.get("tags", []),
            "score": raw_question.get("score"),
            "is_answered": raw_question.get("is_answered"),
        }

        return NormalizedPost(
            source=row["source"],
            source_type=row.get("source_type", "qa"),
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
            author_hint=row.get("author_hint", "") or str(raw_question.get("owner", {}).get("display_name", "") or ""),
            source_meta={"json": json.dumps(normalized_source_meta, ensure_ascii=False, sort_keys=True)},
            dedupe_key=make_dedupe_key("stackoverflow", title, body),
            text_len=len(raw_text),
        )
