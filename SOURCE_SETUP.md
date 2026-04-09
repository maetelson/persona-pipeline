# Source Setup

## Source groups

- `review_sites`
- `reddit`
- `official_communities`

## Config files

Each source is declared in `config/sources/<source>.yaml`.

Required fields for new source configs:

- `source_id`
- `source_name`
- `source_group`
- `collector_kind`
- `normalizer_kind`
- `enabled`

Helpful optional fields:

- `manual_input_dir`
- `direct_crawl_enabled`
- `direct_urls`
- `thread_urls`
- `feed_urls`
- `subreddit`
- `forum_name`
- `product_or_tool`
- `aliases`
- `seed_template_group`

## How to add a new review platform

1. Add `config/sources/<source>.yaml`
2. Set `source_group: review_sites`
3. Set `collector_kind: review_sites`
4. Set `normalizer_kind: review_sites`
5. Set `manual_input_dir`
6. Only enable `direct_crawl_enabled` if the target pages are public, stable, and robots-safe
7. Add parser fixtures and tests

## How to add a new subreddit

1. Add `config/sources/<source>.yaml`
2. Set `source_group: reddit`
3. Set the `subreddit` field
4. Add aliases if you want CLI-friendly names
5. Update fixtures/tests if parsing assumptions change

## How to add a new official community

1. Add `config/sources/<source>.yaml`
2. Set `source_group: official_communities`
3. Prefer `feed_urls` when public feeds exist
4. Add `thread_urls` for public HTML pages when feed coverage is weak
5. Keep `manual_input_dir` available as fallback

## Query banks

Source-group query banks live in `config/source_query_templates.yaml`.

Main non-developer BI themes:

- reporting pain
- dashboard trust
- manual Excel work
- ad hoc analysis
- metric inconsistency
- root cause analysis
- segmentation confusion

Developer-heavy noise is downweighted through `negative_downweight_lexicon` and `config/relevance_rules.yaml`.
