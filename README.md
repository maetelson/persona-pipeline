# Local Bottleneck Persona Pipeline

Local-only, file-based persona research pipeline for collecting public web data, processing it step by step, and exporting exactly one final Excel workbook.

## What this repo does

- Collects public discussion data from enabled sources into raw JSONL files
- Normalizes records into a common schema
- Filters valid persona candidates
- Builds episode-level units from posts
- Labels episodes with rule-based and optional LLM-assisted tagging
- Discovers persona axes and generates exploratory persona outputs
- Exports one final workbook to `data/output/persona_pipeline_output.xlsx`

## Non-goals

- No server components
- No hosted or local DB server
- No web app or API
- No orchestration platform
- No intermediate XLSX storage

## Storage contract

- Raw: `data/raw/{source}/*.jsonl`
- Intermediate: parquet
- Final output: `data/output/*.xlsx`

## Pipeline stages

1. `collectors`
2. `normalizers`
3. `filters`
4. `episodes`
5. `labeling`
6. `analysis`
7. `exporters`

The main entrypoint is:

```bash
python run/00_run_all.py
```

Expanded execution order:

```bash
python run/00_generate_time_slices.py
python run/01_collect_all.py
python run/01_5_expand_queries_from_raw.py
python run/02_normalize_all.py
python run/02.5_filter_time_window.py
python run/03_filter_valid.py
python run/03_5_prefilter_relevance.py
python run/04_build_episodes.py
python run/05_label_episodes.py
python run/06_1_discover_persona_axes.py
python run/06_cluster_and_score.py
python run/07_export_xlsx.py
```

Source-group CLI for the new BI-focused sources:

```bash
python run/10_source_cli.py collect --source-group review_sites
python run/10_source_cli.py collect --source-group reddit
python run/10_source_cli.py collect --source-group official_communities
python run/10_source_cli.py collect --source g2
python run/10_source_cli.py collect --source r/excel
python run/10_source_cli.py collect --source power_bi_community
python run/10_source_cli.py normalize --source-group review_sites
python run/10_source_cli.py prefilter --source-group reddit --export-borderline
python run/10_source_cli.py ingest-manual --source g2 --input-dir data/manual_ingest/g2
python run/10_source_cli.py dry-run --source-group official_communities
python run/10_source_cli.py prefilter --source reddit --export-borderline --limit 200
python run/10_source_cli.py prefilter --source stackoverflow --export-borderline --limit 200
python run/10_source_cli.py prefilter --source-group existing_forums --export-borderline --limit 200
python run/10_source_cli.py qa-relevance --source reddit --limit 200
python run/10_source_cli.py qa-relevance --source stackoverflow --limit 200
```

Important implementation note:

- `review_sites` and `official_communities` use shared collector implementations by source group
- This is code reuse only, not source merging
- Each configured source still runs independently and keeps its own outputs
- Raw outputs remain source-specific under `data/raw/{source_id}/`
- Normalized outputs remain source-specific under `data/normalized/{source_id}.parquet`
- Coverage, keep/drop, and QA reports can still be calculated per source because each row keeps its own `source` value

## Current source status

| Source | Status | Notes |
|---|---|---|
| Reddit | Implemented | Legacy Reddit collector remains available; live collection requires `REDDIT_USER_AGENT` |
| Stack Overflow | Implemented | Legacy Stack Overflow collector remains available |
| GitHub Issues | Implemented | REST search based |
| GitHub Discussions | Conditionally implemented | Requires `GITHUB_TOKEN` |
| Discourse | Stub | Placeholder collector/normalizer |
| Hacker News | Stub | Placeholder collector/normalizer |
| YouTube | Stub | Placeholder collector/normalizer |
| `g2` | Implemented | Uses shared `review_sites` collector code, but writes source-specific raw/normalized outputs |
| `trustradius` | Implemented | Uses shared `review_sites` collector code, but writes source-specific raw/normalized outputs |
| `capterra` | Implemented | Uses shared `review_sites` collector code, but writes source-specific raw/normalized outputs |
| `gartner_peer_insights` | Implemented | Uses shared `review_sites` collector code, but writes source-specific raw/normalized outputs |
| `r/excel` | Implemented | Uses source-specific config and keeps source-specific outputs |
| `r/analytics` | Implemented | Uses source-specific config and keeps source-specific outputs |
| `r/BusinessIntelligence` | Implemented | Uses source-specific config and keeps source-specific outputs |
| `r/MarketingAnalytics` | Implemented | Uses source-specific config and keeps source-specific outputs |
| `power_bi_community` | Implemented | Uses shared `official_communities` collector code, but writes source-specific raw/normalized outputs |
| `tableau_community` | Implemented | Uses shared `official_communities` collector code, but writes source-specific raw/normalized outputs |
| `looker_studio_community` | Implemented | Uses shared `official_communities` collector code, but writes source-specific raw/normalized outputs |
| `sigma_community` | Implemented | Uses shared `official_communities` collector code, but writes source-specific raw/normalized outputs |

## New BI-focused source groups

- `review_sites`
  - `g2`
  - `trustradius`
  - `capterra`
  - `gartner_peer_insights`
- `reddit`
  - `r/excel`
  - `r/analytics`
  - `r/BusinessIntelligence`
  - `r/MarketingAnalytics`
- `official_communities`
  - `power_bi_community`
  - `tableau_community`
  - `looker_studio_community`
  - `sigma_community`

Review sites use a safe two-lane ingestion design:

- Lane 1: direct crawl only when a public page is safely accessible and robots-safe
- Lane 2: manual import from saved HTML / CSV / JSON snapshots in `data/manual_ingest/<source>`

Blocked review sites do not fail the pipeline. They emit `crawl_status=blocked_or_manual_required` so you can continue with manual snapshots through the same parser and normalization layer.

Even though these sources share collector code by source group, they are still tracked per source:

- `g2`, `trustradius`, `capterra`, and `gartner_peer_insights` keep separate raw and normalized outputs
- `power_bi_community`, `tableau_community`, `looker_studio_community`, and `sigma_community` also keep separate raw and normalized outputs
- Combined outputs may be written for convenience, but per-source counting and filtering remain available from the source-specific files and the `source` column

## Requirements

- Python 3.11
- Minimal dependencies from `requirements.txt`

Install:

```bash
pip install -r requirements.txt
```

Current dependencies:

- `pandas`
- `pyarrow`
- `openpyxl`
- `PyYAML`

## Environment variables

Required for live Reddit collection:

- `REDDIT_USER_AGENT`

Optional depending on enabled sources and labeling mode:

- `STACKEXCHANGE_KEY`
- `GITHUB_TOKEN`
- `OPENAI_API_KEY`
- `LLM_MODEL` or `OPENAI_MODEL`
- collector and labeling limit flags used in the run scripts

Example PowerShell setup:

```powershell
$env:REDDIT_USER_AGENT="persona-pipeline/0.1 (by /u/your_reddit_username)"
$env:GITHUB_TOKEN=""
$env:ENABLE_LLM_LABELER="false"
```

You can also place these values in a repo-root `.env` file for local use.

## Repository layout

```text
config/      Pipeline configuration and source settings
run/         Runnable stage scripts
src/         Pipeline implementation modules
tests/       Tests
data/        Local runtime artifacts and final workbook output
```

## Outputs

Typical generated outputs include:

- Raw source files under `data/raw/`
- Normalized, valid, episode, labeled, and analysis parquet files under `data/`
- Final workbook at `data/output/persona_pipeline_output.xlsx`

Runtime data artifacts are local-only and should not be committed.

## Commit Messages

Use this repo convention for commits:

```text
type(scope): short summary
```

Recommended types:

- `feat`
- `fix`
- `refactor`
- `docs`
- `test`
- `chore`

Recommended scopes:

- `collect`
- `normalize`
- `filter`
- `episodes`
- `label`
- `analysis`
- `export`
- `config`
- `docs`
- `tests`
- `repo`

Examples:

- `feat(collect): add review site manual ingest flow`
- `fix(export): validate workbook sheet presence`
- `refactor(analysis): simplify final report assembly`

## Docs

- [ORCHESTRATION.md](./ORCHESTRATION.md): stage order and rerun behavior
- [DATA_CONTRACT.md](./DATA_CONTRACT.md): file and schema contracts
- [CODEBOOK.md](./CODEBOOK.md): labeling definitions
- [RUNBOOK.md](./RUNBOOK.md): execution notes and operational details
- [TASKS.md](./TASKS.md): current implementation checklist
- [COMMIT_CONVENTION.md](./COMMIT_CONVENTION.md): commit message format for this repo
- [SOURCE_SETUP.md](./SOURCE_SETUP.md): source-group setup and extension guide
- [MANUAL_IMPORT.md](./MANUAL_IMPORT.md): blocked-site manual ingest flow
- [SCHEMA.md](./SCHEMA.md): normalized source-group schema details
- [QA_CHECKLIST.md](./QA_CHECKLIST.md): parser and output QA checklist

## Current state

- The repo is a runnable local pipeline, not a polished product
- Analysis outputs are exploratory
- Persona-axis discovery runs before persona generation
- Query expansion is review-oriented and does not auto-write back into config
- Some collectors remain stubs by design until explicitly implemented
