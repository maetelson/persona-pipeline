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
python run/04_build_episodes.py
python run/05_label_episodes.py
python run/06_1_discover_persona_axes.py
python run/06_cluster_and_score.py
python run/07_export_xlsx.py
```

## Current source status

| Source | Status | Notes |
|---|---|---|
| Reddit | Implemented | Live collection requires `REDDIT_USER_AGENT` |
| Stack Overflow | Implemented | Live collection supported |
| GitHub Issues | Implemented | REST search based |
| GitHub Discussions | Conditionally implemented | Requires `GITHUB_TOKEN` |
| Discourse | Stub | Placeholder collector/normalizer |
| Hacker News | Stub | Placeholder collector/normalizer |
| YouTube | Stub | Placeholder collector/normalizer |

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

## Docs

- [ORCHESTRATION.md](./ORCHESTRATION.md): stage order and rerun behavior
- [DATA_CONTRACT.md](./DATA_CONTRACT.md): file and schema contracts
- [CODEBOOK.md](./CODEBOOK.md): labeling definitions
- [RUNBOOK.md](./RUNBOOK.md): execution notes and operational details
- [TASKS.md](./TASKS.md): current implementation checklist

## Current state

- The repo is a runnable local pipeline, not a polished product
- Analysis outputs are exploratory
- Persona-axis discovery runs before persona generation
- Query expansion is review-oriented and does not auto-write back into config
- Some collectors remain stubs by design until explicitly implemented
