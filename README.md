# Local Bottleneck Persona Pipeline

Local-only, file-based persona research pipeline for collecting public web data, processing it step by step, and exporting exactly one final Excel workbook.

## Start Here

If you are new to this repo, use this order:

1. Read this file for the repo map and main entrypoints.
2. Use `run/pipeline/` for the main sequential pipeline.
3. Use `run/cli/` for targeted operating tasks.
4. Use [`docs/operational/ORCHESTRATION.md`](./docs/operational/ORCHESTRATION.md) for strict rerun order.
5. Use [`docs/`](./docs/) for policy and workbook guidance.

Quick navigation:

- Full pipeline: `python run/pipeline/00_run_all.py`
- Common rerun path: `03_filter_valid -> 03_5_prefilter_relevance -> 04_build_episodes -> 05_label_episodes -> 06_1_discover_persona_axes -> 06_cluster_and_score -> 07_export_xlsx`
- Source operations: `python run/cli/10_source_cli.py ...`
- Data/file contracts: [`docs/operational/DATA_CONTRACT.md`](./docs/operational/DATA_CONTRACT.md)

Ignore `__pycache__/`, `.venv/`, and generated artifacts under `data/` when judging the repo structure. They are runtime noise, not part of the intended folder design.

## What This Repo Does

- Collects public discussion data from enabled sources into raw JSONL files
- Normalizes records into a common schema
- Filters by time window and invalid signal rules
- Prefilters by relevance
- Builds episode-level units from posts
- Labels episodes with rule-based and optional LLM-assisted tagging
- Discovers persona axes, clusters episodes, and generates exploratory persona outputs
- Exports one final workbook to `data/output/persona_pipeline_output.xlsx`

## Non-goals

- No server components
- No hosted or local DB server
- No web app or API
- No orchestration platform
- No intermediate XLSX storage

## Storage Contract

| Layer | Format | Location |
|---|---|---|
| Raw | `jsonl` | `data/raw/{source}/` |
| Intermediate | `parquet` | `data/normalized/`, `data/valid/`, `data/prefilter/`, `data/episodes/`, `data/labeled/`, `data/analysis/` |
| Final output | `xlsx` | `data/output/` |

## Repository Map

```text
config/              Pipeline configuration and source settings
docs/                Policy docs and operational references
docs/operational/    Run order, contracts, QA, and maintenance docs
run/pipeline/        Main sequential pipeline scripts
run/cli/             Targeted operating and audit CLIs
run/diagnostics/     Optional diagnostics and tuning helpers
run/experiments/     One-off debug / experiment runners
run/devtools/        Git / maintenance helpers
src/                 Pipeline implementation modules
tests/               Tests
data/                Local runtime artifacts and final workbook output
```

Key layer split in `src/`:

1. `collectors`
2. `normalizers`
3. `filters`
4. `episodes`
5. `labeling`
6. `analysis`
7. `exporters`

## Running The Pipeline

Main entrypoint:

```bash
python run/pipeline/00_run_all.py
```

Expanded stage order:

```bash
python run/pipeline/00_generate_time_slices.py
python run/pipeline/01_collect_all.py
python run/pipeline/01_5_expand_queries_from_raw.py
python run/pipeline/02_normalize_all.py
python run/pipeline/02.5_filter_time_window.py
python run/pipeline/03_filter_valid.py
python run/pipeline/03_5_prefilter_relevance.py
python run/pipeline/04_build_episodes.py
python run/pipeline/05_label_episodes.py
python run/pipeline/06_1_discover_persona_axes.py
python run/pipeline/06_cluster_and_score.py
python run/pipeline/07_export_xlsx.py
```

Common sequential rerun path after filter, relevance, episode, or labeling changes:

```bash
python run/pipeline/03_filter_valid.py
python run/pipeline/03_5_prefilter_relevance.py
python run/pipeline/04_build_episodes.py
python run/pipeline/05_label_episodes.py
python run/pipeline/06_1_discover_persona_axes.py
python run/pipeline/06_cluster_and_score.py
python run/pipeline/07_export_xlsx.py
python run/cli/16_persona_workbook_audit.py
```

Smoke test:

```bash
python run/pipeline/08_smoke_pipeline.py
```

`run/pipeline/08_smoke_pipeline.py` is only for fast sanity checks. It is not the recommended refresh path after changing filters, episode logic, labeling, analysis, or export.

## Main CLIs

Primary source workflow:

```bash
python run/cli/10_source_cli.py collect --source-group reddit
python run/cli/10_source_cli.py prefilter --source-group reddit --export-borderline
python run/cli/10_source_cli.py qa-relevance --source reddit --limit 200
```

Other main CLIs:

| Script | Purpose |
|---|---|
| `run/cli/11_axis_cli.py` | Audit and reduce persona axes from labeled episodes |
| `run/cli/12_example_cli.py` | Select and audit representative persona examples |
| `run/cli/13_cluster_cli.py` | Bottleneck-first clustering audit and export |
| `run/cli/14_persona_cli.py` | Persona naming, insight, and solution-linkage artifacts |
| `run/cli/15_label_cli.py` | Label-quality audit, rerun, repair, and QA exports |
| `run/cli/16_persona_workbook_audit.py` | Workbook metric provenance and denominator/grain audit |

Diagnostics and experiments stay available, but are intentionally not the primary entrypoint surface:

| Script | Purpose |
|---|---|
| `run/diagnostics/17_profile_sources.py` | Source-stage timing from collection through labelability |
| `run/diagnostics/18_analyze_reddit_yield.py` | Reddit yield-failure analysis and diagnostic artifacts |
| `run/diagnostics/19_analyze_reddit_retention.py` | Seed and subreddit retention diagnostics for tuning |
| `run/pipeline/05_5_compare_labeling_coverage.py` | Compare labeling coverage across rule vs LLM paths |
| `run/diagnostics/20_capture_scale_metrics.py` | Before/after funnel and balance metrics snapshot |
| `run/diagnostics/20_rebalance_personas.py` | Persona rebalancing experiment runner |
| `run/diagnostics/21_diagnose_source_funnels.py` | Source-specific stage funnel diagnosis |
| `run/diagnostics/22_diagnose_source_collapse.py` | Reusable source-collapse diagnosis utility |
| `run/diagnostics/23_validate_seed_system.py` | Offline seed-system validation against current artifacts |
| `run/experiments/17_debug_openai_labeler_call.py` | Run one minimal live OpenAI call through the labeler |
| `run/experiments/18_prove_cache_vs_live_calls.py` | Controlled experiment: cache vs live OpenAI call behavior |

## Config Guide

Start in [`config/README.md`](./config/README.md).

Most frequently edited files:

- `config/sources/`
- `config/query_map.yaml`
- `config/relevance_rules.yaml`
- `config/invalid_rules.yaml`
- `config/segmentation_rules.yaml`
- `config/labeling_policy.yaml`
- `config/time_window.yaml`

## Docs Map

- [`docs/README.md`](./docs/README.md): policy and reference map
- [`docs/operational/ORCHESTRATION.md`](./docs/operational/ORCHESTRATION.md): stage order and rerun behavior
- [`docs/operational/DATA_CONTRACT.md`](./docs/operational/DATA_CONTRACT.md): file and schema contracts
- [`docs/operational/CODEBOOK.md`](./docs/operational/CODEBOOK.md): labeling definitions
- [`docs/operational/RUNBOOK.md`](./docs/operational/RUNBOOK.md): execution procedure
- [`docs/operational/TASKS.md`](./docs/operational/TASKS.md): current implementation checklist
- [`docs/operational/COMMIT_CONVENTION.md`](./docs/operational/COMMIT_CONVENTION.md): commit message format
- [`docs/operational/SCHEMA.md`](./docs/operational/SCHEMA.md): normalized schema details
- [`docs/operational/QA_CHECKLIST.md`](./docs/operational/QA_CHECKLIST.md): parser and output QA checklist

Policy docs remain in `docs/`:

- `persona_*`
- `source_*`
- `workbook_*`
- `cluster_robustness_policy.md`
- `quality_status_policy.md`

## Current Source Status

| Source | Status | Notes |
|---|---|---|
| Reddit (aggregate) | Implemented | Curated seeds + collector-side pruning; requires `REDDIT_USER_AGENT` |
| `r/excel` | Implemented | Source-specific config and outputs |
| `r/analytics` | Implemented | Source-specific config and outputs |
| `r/BusinessIntelligence` | Implemented | Source-specific config and outputs |
| `r/MarketingAnalytics` | Implemented | Source-specific config and outputs |
| Stack Overflow | Implemented | REST search based |
| GitHub Discussions | Conditionally implemented | Requires `GITHUB_TOKEN` |
| HubSpot Community | Implemented | Public business community collector |
| Klaviyo Community | Implemented | Public business community collector |
| Metabase Discussions | Implemented | Discourse-based discussion source |
| Mixpanel Community | Implemented | Public business community collector |
| Power BI Community | Implemented | Public Khoros search via Microsoft Fabric Community |
| Qlik Community | Implemented | Public business community collector |
| Shopify Community | Implemented | Public business community collector |
| Sisense Community | Implemented | Public business community collector |
| Amplitude Community | Config present, currently low/zero yield | Not a stable contributor in the latest local run |

## Requirements

- Python 3.11
- Install: `pip install -r requirements.txt`

Core dependencies: `pandas`, `pyarrow`, `openpyxl`, `PyYAML`

## Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `REDDIT_USER_AGENT` | For Reddit collection | Identifies the collector to Reddit |
| `STACKEXCHANGE_KEY` | Optional | Increases Stack Overflow rate limits |
| `GITHUB_TOKEN` | For GitHub Discussions | REST API auth |
| `OPENAI_API_KEY` | Optional | Enables LLM-assisted labeling |
| `LLM_MODEL` / `OPENAI_MODEL` | Optional | Override default model |
| `ENABLE_LLM_LABELER` | Optional | Set `true` to activate LLM labeling path |

Example PowerShell setup:

```powershell
$env:REDDIT_USER_AGENT="persona-pipeline/0.1 (by /u/your_reddit_username)"
$env:GITHUB_TOKEN=""
$env:ENABLE_LLM_LABELER="false"
```

Values can also be placed in a repo-root `.env` file for local use.

## Git Helpers

Install tracked hooks once:

```bash
python run/devtools/99_install_git_hooks.py
```

Single-command stage + commit + push:

```bash
python run/devtools/98_git_sync.py "type(scope): short summary"
```

Task-finalize helper:

```bash
python run/devtools/97_finalize_task.py "type(scope): short summary"
```

## Current State

- The repo is a runnable local pipeline, not a polished product
- Analysis outputs are exploratory
- Persona-axis discovery runs before persona generation
- Query expansion is review-oriented and does not auto-write back into config
