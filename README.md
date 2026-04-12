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

Source-group CLI for active BI-focused sources:

```bash
python run/10_source_cli.py collect --source-group reddit
python run/10_source_cli.py collect --source r/excel
python run/10_source_cli.py prefilter --source-group reddit --export-borderline
python run/10_source_cli.py prefilter --source reddit --export-borderline --limit 200
python run/10_source_cli.py prefilter --source stackoverflow --export-borderline --limit 200
python run/10_source_cli.py prefilter --source-group existing_forums --export-borderline --limit 200
python run/10_source_cli.py qa-relevance --source reddit --limit 200
python run/10_source_cli.py qa-relevance --source stackoverflow --limit 200
python run/19_analyze_reddit_retention.py
```

Important implementation note:

- Active source configs now focus on Reddit, subreddit-specific Reddit, Stack Overflow, GitHub discussions, and existing lightweight forum stubs.
- Aggregate `reddit` now uses a curated source-config seed bank instead of the broad expanded query map.
- The Reddit collector applies source-specific negative keywords, per-seed page caps, rolling-retention stop rules, and optional comment expansion before raw persistence.
- Seed, subreddit, and seed-by-subreddit retention diagnostics are written under `data/analysis/` via `run/19_analyze_reddit_retention.py`.
- Policy audit artifacts are also written under `data/analysis/`, including `reddit_collection_policy_audit.csv`, `reddit_collection_policy_audit.json`, and `reddit_collection_policy_report.md`.
- Raw outputs remain source-specific under `data/raw/{source_id}/`.
- Normalized outputs remain source-specific under `data/normalized/{source_id}.parquet`.
- Coverage, keep/drop, and QA reports can still be calculated per source because each row keeps its own `source` value.

## Current source status

| Source | Status | Notes |
|---|---|---|
| Reddit | Implemented | Aggregate source now uses curated seeds plus collector-side pruning; live collection requires `REDDIT_USER_AGENT` |
| Stack Overflow | Implemented | Legacy Stack Overflow collector remains available |
| GitHub Issues | Implemented | REST search based |
| GitHub Discussions | Conditionally implemented | Requires `GITHUB_TOKEN` |
| Discourse | Stub | Placeholder collector/normalizer |
| Hacker News | Stub | Placeholder collector/normalizer |
| YouTube | Stub | Placeholder collector/normalizer |
| `r/excel` | Implemented | Uses source-specific config and keeps source-specific outputs |
| `r/analytics` | Implemented | Uses source-specific config and keeps source-specific outputs |
| `r/BusinessIntelligence` | Implemented | Uses source-specific config and keeps source-specific outputs |
| `r/MarketingAnalytics` | Implemented | Uses source-specific config and keeps source-specific outputs |

## New BI-focused source groups

- `reddit`
  - `r/excel`
  - `r/analytics`
  - `r/BusinessIntelligence`
  - `r/MarketingAnalytics`

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

## Reddit policy

Aggregate Reddit is treated as a source-specific low-yield/high-overhead collector rather than a generic forum source.

- Policy lives in [config/sources/reddit.yaml](config/sources/reddit.yaml).
- Curated aggregate seeds live in [config/seeds/existing_forums/reddit.yaml](config/seeds/existing_forums/reddit.yaml).
- Configurable controls include subreddit allow/deny rules, per-seed page caps, minimum rolling retention threshold, comment expansion mode, and early-stop thresholds.
- Auditable outputs include the latest policy snapshot, runtime stop counters, and seed/subreddit retention tables under `data/analysis/`.

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

## Git Sync Flow

Install the tracked hooks once:

```bash
python run/99_install_git_hooks.py
```

After that:

- each commit message is checked against the repo convention
- each successful commit is pushed to the configured remote automatically

If you want a single command for stage + commit + push:

```bash
python run/98_git_sync.py "type(scope): short summary"
```

If you want a task-finalize command that always stages, commits, and pushes at the end of one work cycle:

```bash
python run/97_finalize_task.py "type(scope): short summary"
```

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
