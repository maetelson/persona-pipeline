# Data Contract

## Storage rules

- raw: `jsonl`
- normalized / valid / episodes / labeled / analysis: `parquet`
- final: `xlsx`

## Core files

- `data/normalized/normalized_posts.parquet`
- `data/normalized/time_filtered_posts.parquet`
- `data/valid/valid_candidates.parquet`
- `data/valid/invalid_candidates.parquet`
- `data/episodes/episode_table.parquet`
- `data/labeled/labeled_episodes.parquet`
- `data/analysis/cluster_summary.parquet`
- `data/analysis/persona_candidates.parquet`
- `data/analysis/priority_scores.parquet`

## `normalized_posts`

Required columns:

- `source`
- `source_group`
- `source_name`
- `source_type`
- `raw_id`
- `raw_source_id`
- `url`
- `canonical_url`
- `query_seed`
- `title`
- `body`
- `body_text`
- `comments_text`
- `raw_text`
- `created_at`
- `fetched_at`
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
- `language`
- `crawl_method`
- `crawl_status`
- `manual_import_flag`
- `raw_file_path`
- `parse_version`
- `hash_id`
- `author_hint`
- `source_meta`
- `dedupe_key`
- `text_len`

Notes:

- `created_at` is required downstream for time window filtering
- `source_meta` is stored as JSON-compatible payload/string for parquet stability
- old consumers may continue using the legacy subset of columns
- source-group collectors are expected to fill the added fields when available

## `valid_candidates`

Key columns:

- all normalized columns
- `business_signal_score`
- `pain_signal_score`
- `business_signal_terms`
- `pain_signal_terms`

## `invalid_candidates`

Key columns:

- all normalized columns used in filtering
- `invalid_reason`
- optional signal columns if present

## `episode_table`

Required columns:

- `episode_id`
- `source`
- `raw_id`
- `url`
- `normalized_episode`
- `evidence_snippet`
- `role_clue`
- `work_moment`
- `business_question`
- `tool_env`
- `bottleneck_text`
- `workaround_text`
- `desired_output`
- `product_fit`
- `segmentation_note`

## `labeled_episodes`

Required columns:

- `episode_id`
- `role_codes`
- `moment_codes`
- `question_codes`
- `pain_codes`
- `env_codes`
- `workaround_codes`
- `output_codes`
- `fit_code`
- `label_confidence`
- `label_reason`

Notes:

- unknown is explicit and acceptable
- labeler should prefer `unknown` over guessing

## Analysis outputs

### `cluster_summary`

- `cluster_id`
- `cluster_key`
- `episode_count`
- `role_codes`
- `moment_codes`
- `question_codes`
- `pain_codes`
- `env_codes`
- `output_codes`
- `fit_code`
- `cluster_label`
- `cluster_note`

### `persona_candidates`

- `persona_id`
- `role_codes`
- `moment_codes`
- `question_codes`
- `pain_codes`
- `env_codes`
- `output_codes`
- `episode_count`
- `avg_priority_score`
- `high_priority_count`
- `persona_label`
- `persona_note`

### `priority_scores`

- `episode_id`
- `priority_score`
- `priority_band`
- `pain_score`
- `output_score`
- `role_score`
- `workaround_score`
- `fit_score`
- `unknown_penalty`
- `score_note`

## Source raw layouts

### Reddit

- `raw_post`
- `raw_comments`

### Review sites

- direct crawl may be blocked
- emit `crawl_status=blocked_or_manual_required` instead of crashing
- manual HTML / CSV / JSON / JSONL snapshots should map through the same parser + normalizer path

### Official communities

- feed items and public thread HTML may both be used
- thread/reply linkage should be preserved with `thread_title` and `parent_context`

### Stack Overflow

- `raw_question`
- `raw_question_comments`
- `raw_answers`
- `raw_answer_comments`

### GitHub issue

- `raw_issue`
- `raw_issue_comments`

### GitHub discussion

- `raw_discussion`
- `raw_discussion_comments`
- `raw_discussion_replies`
