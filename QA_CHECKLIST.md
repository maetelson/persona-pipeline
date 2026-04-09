# QA Checklist

## Collection

- Confirm the selected source or source group resolves correctly
- Confirm blocked review sites do not stop the run
- Confirm `blocked_manual_required_report` is written when expected
- Confirm raw outputs land in `data/raw/<source>/raw.jsonl`

## Manual import

- Confirm HTML / CSV / JSON snapshots parse without code changes
- Confirm `manual_import_flag=true`
- Confirm `raw_file_path` is populated

## Normalization

- Confirm required compatibility columns still exist
- Confirm new fields map to one unified schema across review, Reddit, and official community sources
- Confirm `hash_id` and `dedupe_key` are populated

## Relevance prefilter

- Confirm business-user-like content gets higher `biz_user_score`
- Confirm developer-heavy setup noise gets higher `dev_heavy_score`
- Confirm borderline rows export when requested
- Confirm `prefilter_summary_report` is written

## Samples

- Review a few rows from `data/analysis/source_sample_rows.csv`
- Confirm thread/comment linkage via `thread_title` and `parent_context`
- Confirm review metadata such as role/company size/industry survives normalization
