# Schema

## Normalized source-group schema

The normalized source-group outputs extend the existing shared schema without removing old columns.

Core compatibility fields kept from the old pipeline:

- `source`
- `source_type`
- `raw_id`
- `url`
- `query_seed`
- `title`
- `body`
- `comments_text`
- `raw_text`
- `created_at`
- `fetched_at`
- `language`
- `author_hint`
- `source_meta`
- `dedupe_key`
- `text_len`

Added source-group fields:

- `source_group`
- `source_name`
- `raw_source_id`
- `canonical_url`
- `body_text`
- `retrieved_at`
- `author_name`
- `product_or_tool`
- `subreddit_or_forum`
- `thread_title`
- `parent_context`
- `role_hint`
- `company_size_hint`
- `industry_hint`
- `workflow_hint`
- `pain_point_hint`
- `output_need_hint`
- `dev_heavy_score`
- `biz_user_score`
- `relevance_score`
- `crawl_method`
- `crawl_status`
- `manual_import_flag`
- `raw_file_path`
- `parse_version`
- `hash_id`

## Output locations

- raw: `data/raw/<source>/raw.jsonl`
- normalized per source: `data/normalized/<source>.parquet`
- combined source-group normalized: `data/normalized/normalized_source_groups.parquet`
- prefilter outputs: `data/prefilter/`
- QA/sample outputs: `data/analysis/`
