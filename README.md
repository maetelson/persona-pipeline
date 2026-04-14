# Local Bottleneck Persona Pipeline

Local-only, file-based persona research pipeline for collecting public web data, processing it step by step, and exporting exactly one final Excel workbook.

## What this repo does

- Collects public discussion data from enabled sources into raw JSONL files
- Normalizes records into a common schema
- Filters by time window and invalid signal rules
- Prefilters by relevance (keep / borderline / drop)
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

## Storage contract

| Layer | Format | Location |
|---|---|---|
| Raw | `jsonl` | `data/raw/{source}/` |
| Intermediate | `parquet` | `data/normalized/`, `data/valid/`, `data/prefilter/`, `data/episodes/`, `data/labeled/`, `data/analysis/` |
| Final output | `xlsx` | `data/output/` |

## Pipeline stages

```
raw → normalized → time-filtered → valid → relevance-prefiltered
  → episodes → labeled episodes → analysis → final xlsx
```

1. `collectors`
2. `normalizers`
3. `filters` (time window + invalid rules + relevance prefilter)
4. `episodes`
5. `labeling`
6. `analysis` (axis discovery + clustering + scoring)
7. `exporters`

## Running the pipeline

Main entrypoint (runs all stages in order):

```bash
python run/00_run_all.py
```

Expanded stage-by-stage execution:

```bash
python run/00_generate_time_slices.py      # build time window slices
python run/01_collect_all.py               # collect raw JSONL per source
python run/01_5_expand_queries_from_raw.py # surface query expansion candidates
python run/02_normalize_all.py             # normalize to common schema
python run/02.5_filter_time_window.py      # enforce date window
python run/03_filter_valid.py              # invalid signal filter
python run/03_5_prefilter_relevance.py     # relevance keep/borderline/drop
python run/04_build_episodes.py            # episode segmentation
python run/05_label_episodes.py            # rule + optional LLM labeling
python run/06_1_discover_persona_axes.py   # persona axis discovery
python run/06_cluster_and_score.py         # bottleneck clustering and scoring
python run/07_export_xlsx.py               # export final workbook
```

Smoke test (fast end-to-end sanity check):

```bash
python run/08_smoke_pipeline.py
```

## CLI tools

### Source workflow (`10_source_cli.py`)

```bash
python run/10_source_cli.py collect --source-group reddit
python run/10_source_cli.py collect --source r/excel
python run/10_source_cli.py prefilter --source-group reddit --export-borderline
python run/10_source_cli.py prefilter --source reddit --export-borderline --limit 200
python run/10_source_cli.py prefilter --source stackoverflow --export-borderline --limit 200
python run/10_source_cli.py qa-relevance --source reddit --limit 200
python run/10_source_cli.py qa-relevance --source stackoverflow --limit 200
```

### Axis / cluster / persona / label CLIs

| Script | Purpose |
|---|---|
| `run/11_axis_cli.py` | Audit and reduce persona axes from labeled episodes |
| `run/12_example_cli.py` | Select and audit representative persona examples |
| `run/13_cluster_cli.py` | Bottleneck-first clustering audit and export |
| `run/14_persona_cli.py` | Persona naming, insight, and solution-linkage artifacts |
| `run/15_label_cli.py` | Label-quality audit, rerun, repair, and QA exports |
| `run/16_persona_workbook_audit.py` | Workbook metric provenance and denominator/grain audit |

### Diagnostics and analysis

| Script | Purpose |
|---|---|
| `run/diagnostics/17_profile_sources.py` | Source-stage timing from collection through labelability |
| `run/diagnostics/18_analyze_reddit_yield.py` | Reddit yield-failure analysis and diagnostic artifacts |
| `run/diagnostics/19_analyze_reddit_retention.py` | Seed and subreddit retention diagnostics for tuning |
| `run/05_5_compare_labeling_coverage.py` | Compare labeling coverage across rule vs LLM paths |
| `run/diagnostics/20_capture_scale_metrics.py` | Before/after funnel and balance metrics snapshot |
| `run/diagnostics/20_rebalance_personas.py` | Persona rebalancing experiment runner |
| `run/diagnostics/21_diagnose_source_funnels.py` | Source-specific stage funnel diagnosis |
| `run/diagnostics/22_diagnose_source_collapse.py` | Reusable source-collapse diagnosis utility |
| `run/diagnostics/23_validate_seed_system.py` | Offline seed-system validation against current artifacts |
| `run/experiments/17_debug_openai_labeler_call.py` | Run one minimal live OpenAI call through the labeler |
| `run/experiments/18_prove_cache_vs_live_calls.py` | Controlled experiment: cache vs live OpenAI call behavior |

## Current source status

| Source | Status | Notes |
|---|---|---|
| Reddit (aggregate) | Implemented | Curated seeds + collector-side pruning; requires `REDDIT_USER_AGENT` |
| `r/excel` | Implemented | Source-specific config and outputs |
| `r/analytics` | Implemented | Source-specific config and outputs |
| `r/BusinessIntelligence` | Implemented | Source-specific config and outputs |
| `r/MarketingAnalytics` | Implemented | Source-specific config and outputs |
| Stack Overflow | Implemented | REST search based |
| GitHub Issues | Implemented | REST search based |
| GitHub Discussions | Conditionally implemented | Requires `GITHUB_TOKEN` |
| Discourse | Stub | Placeholder collector/normalizer |
| Hacker News | Stub | Placeholder collector/normalizer |
| YouTube | Stub | Placeholder collector/normalizer |

Active BI-focused `reddit` source group:
- `r/excel`
- `r/analytics`
- `r/BusinessIntelligence`
- `r/MarketingAnalytics`

## Reddit collection policy

Aggregate Reddit is treated as a low-yield/high-overhead source with explicit pruning controls.

- Config: [config/sources/reddit.yaml](config/sources/reddit.yaml)
- Curated seeds: [config/seeds/existing_forums/reddit.yaml](config/seeds/existing_forums/reddit.yaml)
- Controls: subreddit allow/deny rules, per-seed page caps, minimum rolling-retention threshold, comment expansion mode, early-stop thresholds
- Audit outputs under `data/analysis/`: policy snapshot, runtime stop counters, seed/subreddit retention tables, `reddit_collection_policy_audit.csv/json`, `reddit_collection_policy_report.md`

## Policy documents

Governance policies and workbook audit specs live in `docs/`:

| Document | Purpose |
|---|---|
| `docs/persona_core_taxonomy.md` | Canonical persona axis taxonomy |
| `docs/persona_core_coverage_policy.md` | Minimum corpus coverage requirements per persona |
| `docs/persona_promotion_policy.md` | Criteria for promoting a candidate to a final persona |
| `docs/persona_promotion_grounding_policy.md` | Evidence grounding rules for promotion |
| `docs/persona_readiness_policy.md` | Readiness gate before workbook export |
| `docs/persona_example_grounding_policy.md` | Example selection and grounding requirements |
| `docs/cluster_robustness_policy.md` | Cluster quality and robustness thresholds |
| `docs/quality_status_policy.md` | Quality status label definitions |
| `docs/source_balance_policy.md` | Source diversity and balance requirements |
| `docs/source_diagnostics_metric_contract.md` | Metric definitions for source diagnostics |
| `docs/source_diagnostics_grain_policy.md` | Grain and aggregation rules for source metrics |
| `docs/source_diagnostics_reason_policy.md` | Reason code definitions for source diagnostics |
| `docs/workbook_metric_truth_table.md` | Authoritative truth table for workbook metrics |
| `docs/workbook_semantic_changelog.md` | Semantic changelog for workbook schema changes |
| `docs/persona_workbook_audit.md` | Workbook audit procedure and metric provenance |
| `docs/persona_workbook_denominator_policy.md` | Denominator selection and grain rules |

## Repository layout

```text
config/      Pipeline configuration, source settings, and scoring rules
docs/        Policy documents, strategy notes, and archived scratch docs
run/         Runnable stage scripts and CLI tools
run/diagnostics/  Optional diagnostics and tuning helpers
run/experiments/  One-off debug / experiment runners
src/         Pipeline implementation modules
tests/       Tests
data/        Local runtime artifacts and final workbook output
```

Key config files:

```text
config/query_map.yaml              Active query seed inventory (54 seeds across 3 sources)
config/query_seed_taxonomy.yaml    Taxonomy for query design
config/query_expansion_rules.yaml  Rules for raw-driven query expansion
config/segmentation_rules.yaml     Episode segmentation thresholds
config/labeling_policy.yaml        Labeling rules and codebook reference
config/codebook.yaml               Label definitions
config/scoring.yaml                Persona priority scoring weights
config/pipeline_thresholds.yaml    Stage robustness thresholds
config/export_schema.yaml          Workbook sheet and column contract
```

## Outputs

Typical generated artifacts:

- Raw source files: `data/raw/{source_id}/`
- Stage parquets: `data/normalized/`, `data/valid/`, `data/prefilter/`, `data/episodes/`, `data/labeled/`
- Analysis artifacts: `data/analysis/` (axis candidates, cluster summaries, diagnostics, audits)
- Final workbook: `data/output/persona_pipeline_output.xlsx`

Runtime data artifacts are local-only and should not be committed.

## Requirements

- Python 3.11
- Install: `pip install -r requirements.txt`

Core dependencies: `pandas`, `pyarrow`, `openpyxl`, `PyYAML`

## Environment variables

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
