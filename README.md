# Persona Mining Pipeline

> Reproducible qualitative data mining for evidence-based persona discovery from public community data.

![Python 3.11](https://img.shields.io/badge/python-3.11-blue)
![Local Only](https://img.shields.io/badge/architecture-local--only-informational)
![Pipeline](https://img.shields.io/badge/output-reviewable%20workbook-success)
![Status](https://img.shields.io/badge/status-reviewable%20release%20%2F%20research%20pipeline-orange)

This repository turns messy public discussion data into auditable persona evidence. It collects multi-source community data, normalizes it into a common schema, segments pain episodes, labels them with rules plus optional LLM assistance, clusters persona candidates, scores readiness, and exports a reviewable workbook.

It is not a SaaS product, a dashboard template, or a one-shot LLM summarizer. It is a local, file-based research pipeline built for product discovery, UX research, market pain-point mining, and evidence review.

Current workbook artifact: [`data/output/persona_pipeline_output.xlsx`](./data/output/persona_pipeline_output.xlsx)  
Current release label: `Reviewable release / research pipeline`

## Problem

Persona research often falls into one of three traps:

- too small: a handful of interviews becomes the whole market narrative
- too manual: tagging, clustering, and evidence review do not scale
- too vague: LLM summaries sound polished but hide weak grounding, source bias, and duplicate claims

Public communities contain real pain signals, but they are noisy, duplicated, source-biased, and structurally inconsistent. A Reddit thread, a forum post, a GitHub discussion, and a Stack Overflow question do not arrive in the same shape, and they do not deserve the same evidentiary weight.

This project exists to turn that mess into something reviewable: auditable persona candidates with source-aware diagnostics, explicit readiness tiers, and workbook artifacts that can be inspected instead of merely trusted.

## Why This Matters

Teams make product, UX, and GTM decisions from personas all the time. If the persona pipeline cannot show where a claim came from, how balanced the evidence is, and what remains weak, the result is usually presentation-ready fiction.

This repository aims for a better standard: quality gates, not vibes.

## What This Project Does

The pipeline processes public/community data through explicit, rerunnable stages:

1. collect public/community data into raw JSONL
2. normalize heterogeneous sources into a shared schema
3. filter invalid and noisy rows
4. prefilter for pain/relevance
5. build episode-level units from posts
6. label episodes with rule-based and optional LLM-assisted workflows
7. discover persona axes from the corpus
8. cluster and score persona candidates
9. evaluate source balance, weak-source debt, and evidence quality
10. export a reviewable workbook and supporting artifacts

This keeps raw collection separate from filtering, episode segmentation, labeling, clustering, and export. Each stage is local, file-based, and rerunnable.

## Why It Is Different

Most "persona generation" projects stop at scraping plus summarization. This one does not.

- Evidence-first persona generation: personas are backed by episode-level evidence, not just generated prose.
- Source-aware diagnostics: source balance, source-tier behavior, and weak-source debt are first-class outputs.
- Weak-source handling: low-quality or weakly contributing sources are diagnosed, not silently blended away.
- Readiness distinctions: review-ready, production-ready, claim-eligible, and blocked candidates are deliberately separated.
- Claim controls: deck-ready claim eligibility is tracked independently from workbook readiness.
- No silent source deletion: raw data is preserved close to source and downstream decisions remain inspectable.
- Workbook-quality gates: the export is treated as a deliverable with validation, not an afterthought.
- Reproducible stages: the pipeline is scriptable end to end and emits artifacts suitable for audit and regression testing.

## Pipeline Overview

```mermaid
flowchart LR
    A["Raw Sources"] --> B["Normalization"]
    B --> C["Valid Filter"]
    C --> D["Relevance Prefilter"]
    D --> E["Episode Builder"]
    E --> F["Labeling"]
    F --> G["Axis Discovery"]
    G --> H["Clustering + Scoring"]
    H --> I["Quality Gates"]
    I --> J["Workbook Export"]
```

ASCII view:

```text
Raw Sources
  -> Normalization
  -> Valid Filter
  -> Relevance Prefilter
  -> Episode Builder
  -> Labeling
  -> Axis Discovery
  -> Clustering + Scoring
  -> Quality Gates
  -> Workbook Export
```

Canonical pipeline order:

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

## Current Output Status

The current release is intentionally described as `reviewable_but_not_deck_ready`.

- Production-ready personas: `3`
- Review-ready personas: `1`
- Deck-ready claim-eligible personas: `4`
- Blocked/constrained candidates: `1`

Why the distinction matters:

- `production-ready persona`: strong enough to count as final usable output in the current workbook.
- `review-ready persona`: ready for analyst/research discussion, but not counted as final usable output.
- `deck-ready claim-eligible persona`: supported strongly enough that claim wording may be used in a deck, but that does not automatically make the persona production-ready or upgrade workbook readiness.
- `blocked/constrained candidate`: materially limited by evidence, source policy, or readiness constraints and should not be promoted.

In the current accepted release:

- the workbook itself remains `reviewable_but_not_deck_ready`
- the `3` production-ready personas are also the `3` final usable personas
- the extra claim-eligible persona is discussion-ready, not production-ready
- one candidate remains blocked and is not claim-eligible

This is deliberate. The project tracks overclaim risk explicitly rather than pretending every cluster deserves promotion.

## Key Artifacts

Primary outputs:

- [`data/output/persona_pipeline_output.xlsx`](./data/output/persona_pipeline_output.xlsx)
- [`data/analysis/overview.csv`](./data/analysis/overview.csv)
- [`data/analysis/persona_summary.csv`](./data/analysis/persona_summary.csv)
- [`data/analysis/cluster_stats.csv`](./data/analysis/cluster_stats.csv)
- [`data/analysis/source_balance_audit.csv`](./data/analysis/source_balance_audit.csv)
- [`data/analysis/source_diagnostics.csv`](./data/analysis/source_diagnostics.csv)
- [`data/analysis/persona_promotion_path_debug.csv`](./data/analysis/persona_promotion_path_debug.csv)

Readiness and policy artifacts:

- [`artifacts/readiness/`](./artifacts/readiness/)
- [`artifacts/policy/`](./artifacts/policy/)
- [`artifacts/release/`](./artifacts/release/)

These artifacts are part of the value proposition. They make it possible to inspect how a persona was promoted, why a source is weak, and why a workbook is reviewable instead of deck-ready.

## Quality And Trust Model

### Quality Gates, Not Vibes

The pipeline tries to earn trust through explicit controls:

- Source balance checks: personas should not be driven by one dominant or weakly representative source mix.
- Weak-source cost center handling: weak sources are surfaced as debt instead of being silently averaged away.
- Source-tier policy: evidence is treated differently depending on whether it comes from core representative, supporting validation, or exploratory edge sources.
- Persona evidence tiers: readiness depends on evidence shape, not just cluster existence.
- Claim eligibility controls: deck-ready claim wording is evaluated separately from persona production readiness.
- Workbook policy checks: the exported workbook is validated against expected count and policy invariants.

Source evidence is interpreted through tiers:

- `core representative`: the anchor layer for claims that must hold up beyond anecdote
- `supporting validation`: strengthens a claim but does not substitute for a missing core anchor
- `exploratory edge`: useful for discovery, hypothesis generation, and edge-case signal, but not enough to carry a production-ready persona alone

This is why the README does not say "five finished personas" even though multiple persona candidates exist. The system is designed to avoid that kind of inflation.

## Quickstart

### 1. Install

```bash
pip install -r requirements.txt
```

Requirements:

- Python `3.11`
- Core libraries include `pandas`, `pyarrow`, `openpyxl`, and `PyYAML`

### 2. Configure Environment

Required for some live sources:

- `REDDIT_USER_AGENT`

Optional depending on enabled workflows:

- `STACKEXCHANGE_KEY`
- `GITHUB_TOKEN`
- `OPENAI_API_KEY`
- `LLM_MODEL` or `OPENAI_MODEL`
- `ENABLE_LLM_LABELER`

Example PowerShell setup:

```powershell
$env:REDDIT_USER_AGENT="persona-pipeline/0.1 (by /u/your_reddit_username)"
$env:GITHUB_TOKEN=""
$env:ENABLE_LLM_LABELER="false"
```

You can also use a repo-root `.env` file for local development.

### 3. Run The Pipeline

Full pipeline:

```bash
python run/pipeline/00_run_all.py
```

Common analysis-quality rerun loop:

```bash
python run/pipeline/03_filter_valid.py
python run/pipeline/03_5_prefilter_relevance.py
python run/pipeline/04_build_episodes.py
python run/pipeline/05_label_episodes.py
python run/pipeline/06_1_discover_persona_axes.py
python run/pipeline/06_cluster_and_score.py
python run/cli/17_analysis_snapshot.py --compare-latest
```

Workbook export:

```bash
python run/pipeline/07_export_xlsx.py
```

Useful examples:

```bash
python run/pipeline/06_cluster_and_score.py
python run/pipeline/07_export_xlsx.py
python run/cli/17_analysis_snapshot.py --compare-latest
python run/cli/16_persona_workbook_audit.py
```

Important: dependent stages should be run sequentially, not in parallel. This repository treats the pipeline as dependency-sensitive by design.

### 4. Run Tests

Example test commands:

```bash
python -m unittest tests.test_workbook_export
python -m unittest tests.test_analysis_snapshot_cli
python -m unittest tests.test_persona_workbook_regressions
```

You can also run a broader suite, for example:

```bash
python -m unittest
```

## Repository Structure

```text
config/          source configs, filters, labeling policy, query maps, time windows
run/pipeline/    main sequential pipeline stages
run/cli/         targeted audit, analysis, and operating commands
src/             collectors, normalizers, filters, episodes, labeling, analysis, exporters
tests/           unit and regression tests
docs/            operational docs, contracts, codebooks, and repo guidance
artifacts/       readiness, policy, curation, and release evidence
data/            local raw, intermediate parquet, analysis outputs, final xlsx
```

Storage contract:

- raw: `data/raw/{source}/*.jsonl`
- intermediate stages: parquet
- final output: `data/output/*.xlsx`

## Example Use Cases

- Product discovery from public user pain signals
- UX research synthesis across community sources
- Market pain-point mining for positioning or roadmap work
- Community-based VOC analysis with source-aware filtering
- Persona evidence workbook generation for review workflows
- Source quality auditing before socializing persona claims

## Limitations

- This is not a fully automated truth engine.
- Public community data is noisy and source bias still matters.
- LLM labeling is optional and requires audit safeguards.
- Deck-ready claims require passing quality gates, not just finding an interesting cluster.
- Some outputs are reviewable rather than final production-ready deliverables.
- The current accepted workbook is explicitly `reviewable_but_not_deck_ready`.

That honesty is part of the design, not a defect in the README.

## Roadmap

- Better source adapters and source-specific normalizers
- Stronger gold-set and benchmark-style evaluation
- Improved source balance and weak-source remediation
- Interactive review UI for workbook and evidence inspection
- Human-in-the-loop labeling and adjudication workflows
- Better persona merge/split controls
- Expanded workbook export and validation surfaces

## Contributing

Contributions are especially useful in these areas:

- new collectors for relevant public/community sources
- better source adapters and normalizers
- improved quality gates and policy checks
- labeling and audit improvements
- evaluation datasets and gold-set construction
- documentation and operational clarity

If you are extending the pipeline, keep the stage boundaries explicit. Avoid collapsing collection, filtering, labeling, and export into one opaque step.

## License, Citation, Acknowledgement

License: `TBD`

If you want to cite or reuse the approach before a formal license is chosen, treat this repository as a research-engineering reference implementation and confirm usage expectations with the maintainer.
