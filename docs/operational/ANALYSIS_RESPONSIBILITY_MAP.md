# Analysis Responsibility Map

This document maps the current `src/analysis/` modules to their main responsibilities.

It is intentionally descriptive, not a refactor proposal. The goal is to make current boundaries explicit before any future module split.

## Responsibility groups

### Clustering

- `bottleneck_clustering.py`
- `cluster.py`
- `clustering.py`
- `cooccurrence.py`
- `profiling.py`

Primary role:

- build bottleneck features
- assign clusters/personas
- merge overlapping personas
- emit overlap/merge audit signals

### Axis discovery

- `persona_axes.py`
- `axis_reduction.py`

Primary role:

- discover axes from labeled episodes
- reduce noisy axes
- emit axis candidate and reduction artifacts

### Persona scoring and promotion

- `score.py`
- `persona.py`
- `persona_service.py`
- `pipeline_thresholds.py`

Primary role:

- build persona candidates
- score readiness and promotion state
- compute persona-level summary frames

### Source diagnostics

- `diagnostics.py`
- `source_funnel_diagnostics.py`
- `source_stage_profiler.py`
- `stage_counts.py`

Primary role:

- compute source-stage counts
- build source diagnostics and source balance audit
- emit weak-source and funnel diagnostics

### Workbook / report assembly

- `workbook_bundle.py`
- `report_export.py`
- `summary.py`
- `persona_messaging.py`
- `persona_gen.py`
- `example_selection.py`

Primary role:

- assemble workbook-facing tables
- export report-oriented frames
- select representative examples and persona messaging

### Quality / readiness

- `quality_status.py`
- `pipeline_thresholds.py`
- `scale_metrics.py`

Primary role:

- compute readiness, coverage, and threshold status
- summarize quality and scale signals

### Seed / source analysis

- `query_expander.py`
- `seed_validation.py`
- `business_source_inventory.py`
- `reddit_retention.py`
- `reddit_yield_analysis.py`
- `raw_audit.py`
- `rebalancing.py`

Primary role:

- analyze seeds, source inventory, raw collection behavior, and rebalancing experiments

### Experimental diagnostics

- `reconciliation_signoff_curation.py`

Primary role:

- build and validate curation/evaluation artifacts used to audit future clustering changes

## Overloaded files

### `stage_service.py`

Current responsibilities:

- orchestrates analysis outputs
- applies workbook-level promotion constraints
- writes many final frames used by snapshot and workbook consumers

Why it is overloaded:

- it mixes orchestration, policy overlay, and final-frame assembly
- it is the main place where local persona state becomes workbook-facing final state

### `persona_service.py`

Current responsibilities:

- persona-level assembly
- cluster/persona summary shaping
- dominant signature and workbook-facing persona tables

Why it is overloaded:

- it mixes semantic persona assembly with workbook presentation concerns

### `bottleneck_clustering.py`

Current responsibilities:

- feature building
- signature building
- cluster assignment
- merge logic
- overlap logic

Why it is overloaded:

- it owns both representation and clustering policy
- it is the highest-risk file for semantic behavior changes

### `diagnostics.py`

Current responsibilities:

- source-stage aggregation
- source failure reason normalization
- remediation shaping
- source diagnostic frame building

Why it is overloaded:

- it mixes metric calculation, root-cause normalization, and human-facing remediation output

### `quality_status.py`

Current responsibilities:

- compute readiness and quality labels
- interpret multiple threshold families

Why it is overloaded:

- quality semantics touch multiple downstream consumers and are easy to duplicate conceptually

### `workbook_bundle.py`

Current responsibilities:

- define workbook bundle members
- emit workbook-facing parquet bundle artifacts

Why it is overloaded:

- it sits at the boundary between analysis truth and workbook packaging
- any schema drift here can confuse both export and audit consumers

## Recommended future split directions

Not for this pass, but the clean future grouping would likely be:

- `src/analysis/clustering/`
- `src/analysis/source_diagnostics/`
- `src/analysis/persona/`
- `src/analysis/workbook/`
- `src/analysis/quality/`
- `src/analysis/experiments/`

This document is meant to reduce confusion now, without moving code yet.
