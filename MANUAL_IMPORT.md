# Manual Import

## When to use it

Use manual import when:

- a review site is blocked or not safely crawlable
- an official community needs HTML snapshots
- you have exported CSV or JSON from a compliant public workflow

## Supported snapshot formats

- HTML
- CSV
- JSON
- JSONL

## Directory layout

Place files under:

- `data/manual_ingest/g2`
- `data/manual_ingest/trustradius`
- `data/manual_ingest/capterra`
- `data/manual_ingest/gartner_peer_insights`
- or the matching source directory for any configured source

## Command

```bash
python run/10_source_cli.py ingest-manual --source g2 --input-dir data/manual_ingest/g2
```

That command:

1. collects from the manual snapshot directory
2. writes `data/raw/<source>/raw.jsonl`
3. normalizes to `data/normalized/<source>.parquet`
4. runs relevance prefilter outputs

## Recommended snapshot fields for CSV/JSON

- `raw_source_id`
- `canonical_url`
- `title`
- `body_text`
- `author_name`
- `product_or_tool`
- `role_hint`
- `company_size_hint`
- `industry_hint`
- `workflow_hint`
- `pain_point_hint`
- `output_need_hint`
- `rating`
- `verified_marker`

## Notes

- Manual import rows are marked with `manual_import_flag=true`
- Review sites emit `crawl_status=blocked_or_manual_required` instead of crashing collection
