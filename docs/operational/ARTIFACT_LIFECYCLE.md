# Artifact Lifecycle

This document is the source of truth for how runtime artifacts should be interpreted, regenerated, and cleaned up.

The repository intentionally keeps pipeline data local and file-based, but `data/analysis/` currently contains a mix of canonical outputs, diagnostics, experiments, snapshots, and historical comparison files. This document makes that mixed state explicit so we can avoid treating stale artifacts as current truth.

## Current mixed state

`data/analysis/` is currently a shared runtime workspace, not a cleanly partitioned storage tree.

The latest structure audit found these practical categories:

- `canonical_output`: current stage-owned outputs that represent the latest analysis state
- `workbook_bundle`: parquet bundle written for workbook-facing validation and export
- `snapshots`: frozen comparison points such as `validation_snapshot.*` and `before_rerun_snapshot/`
- `before_after_comparison`: explicit before/after comparison files and rerun deltas
- `diagnostics`: audit/debug/sample outputs used to understand failures
- `experiments`: alternate clustering, rebalancing, cache/live-call, or seed-discovery experiments
- `temporary_or_stale`: ad hoc one-off folders named after a fix pass or interim investigation
- `unknown`: files that may still be useful, but do not currently have a documented lifecycle classification

Representative examples:

- canonical:
  - `data/analysis/overview.csv`
  - `data/analysis/counts.csv`
  - `data/analysis/cluster_stats.csv`
  - `data/analysis/persona_summary.csv`
  - `data/analysis/source_balance_audit.csv`
  - `data/analysis/source_diagnostics.csv`
  - `data/analysis/persona_assignments.parquet`
  - `data/analysis/persona_axis_assignments.parquet`
  - `data/analysis/validation_snapshot.json`
- workbook bundle:
  - `data/analysis/workbook_bundle/`
- snapshots:
  - `data/analysis/before_rerun_snapshot/`
  - `data/analysis/validation_snapshot.json`
  - `data/analysis/validation_snapshot.md`
- diagnostics:
  - `data/analysis/source_funnel_diagnostics/`
  - `data/analysis/weak_source_diagnostics/`
  - `data/analysis/business_source_inventory_audit/`
  - `data/analysis/*audit*`
  - `data/analysis/*debug*`
- experiments:
  - `data/analysis/llm_experiments/`
  - `data/analysis/rebalanced/`
  - `data/analysis/axis_samples/`
  - `data/analysis/new_seed_configs/`
- temporary or stale:
  - `data/analysis/klaviyo_after_prefilter_fix/`
  - `data/analysis/klaviyo_after_valid_episode_fix/`
  - `data/analysis/merchant_center_after_episode_fix/`
  - `data/analysis/mixpanel_qlik_after_builder_fix/`
  - `data/analysis/mixpanel_qlik_after_todos/`

## Target structure

We are not moving files in this pass, but this is the intended target structure for future cleanup:

```text
data/analysis/
  canonical/
  diagnostics/
  experiments/
  snapshots/
  workbook_bundle/
artifacts/
  curation/
  evaluation/
  archive/
```

Interpretation:

- `data/analysis/canonical/`
  - latest stage-owned outputs from `06_1_discover_persona_axes.py`, `06_cluster_and_score.py`, and `17_analysis_snapshot.py`
- `data/analysis/diagnostics/`
  - reproducible audits and read-only diagnosis outputs
- `data/analysis/experiments/`
  - alternate clustering, rebalancing, cache/live-call, or seed experiments
- `data/analysis/snapshots/`
  - intentionally frozen baselines for comparison
- `data/analysis/workbook_bundle/`
  - workbook-facing parquet bundle used by validation/export
- `artifacts/curation/`
  - manually reviewed curation/evaluation CSVs plus summaries
- `artifacts/evaluation/`
  - future evaluation-only reports that are not part of canonical analysis
- `artifacts/archive/`
  - retired ad hoc outputs that should not be treated as current truth

## Stage ownership

| Artifact family | Owning stage/script | Regenerate by |
|---|---|---|
| `time_slices.parquet` | `00_generate_time_slices.py` | rerun stage `00` |
| query expansion outputs | `01_5_expand_queries_from_raw.py` | rerun stage `01.5` |
| axis discovery outputs | `06_1_discover_persona_axes.py` | rerun stage `06.1` |
| clustering/persona outputs | `06_cluster_and_score.py` | rerun stage `06` |
| workbook bundle | `06_cluster_and_score.py` | rerun stage `06` |
| validation snapshot | `run/cli/17_analysis_snapshot.py` | rerun `17_analysis_snapshot.py --compare-latest` |
| workbook xlsx | `07_export_xlsx.py` | rerun stage `07` |
| curation/evaluation CSVs | `run/cli/19_build_reconciliation_signoff_curation.py` | rerun curation builder |
| curation validation | `run/cli/20_validate_reconciliation_signoff_curation.py` | rerun curation validator |

## Canonical artifacts

These are the files that downstream analysis should treat as the latest truth unless a caller explicitly asks for historical comparison.

- `data/analysis/overview.csv`
- `data/analysis/counts.csv`
- `data/analysis/cluster_stats.csv`
- `data/analysis/clusters.csv`
- `data/analysis/cluster_summary.parquet`
- `data/analysis/persona_candidates.parquet`
- `data/analysis/priority_scores.parquet`
- `data/analysis/persona_summary.csv`
- `data/analysis/persona_assignments.parquet`
- `data/analysis/persona_axis_assignments.parquet`
- `data/analysis/persona_axis_values.parquet`
- `data/analysis/source_distribution.csv`
- `data/analysis/source_balance_audit.csv`
- `data/analysis/source_diagnostics.csv`
- `data/analysis/quality_checks.csv`
- `data/analysis/quality_failures.csv`
- `data/analysis/metric_glossary.csv`
- `data/analysis/final_axis_schema.json`
- `data/analysis/axis_candidates.csv`
- `data/analysis/axis_candidates.parquet`
- `data/analysis/validation_snapshot.json`
- `data/analysis/validation_snapshot.md`

When in doubt:

- use the latest root-level canonical files
- use `data/analysis/workbook_bundle/` only for workbook-facing validation/export
- do not use ad hoc `*_after_*`, `before_after_*`, or experiment folders as current truth

## Workbook bundle artifacts

`data/analysis/workbook_bundle/` is canonical for workbook assembly, but not a replacement for the root-level human-readable CSV outputs.

These files are:

- owned by `06_cluster_and_score.py`
- safe to overwrite on every rerun
- not intended for manual editing

## Curation and evaluation artifacts

Current curation artifacts live under `artifacts/curation/` and are reproducible.

Current owner scripts:

- `run/cli/19_build_reconciliation_signoff_curation.py`
- `run/cli/20_validate_reconciliation_signoff_curation.py`
- implementation: `src/analysis/reconciliation_signoff_curation.py`

Current generated files:

- `artifacts/curation/reconciliation_signoff_eval.csv`
- `artifacts/curation/reconciliation_signoff_dev.csv`
- `artifacts/curation/reconciliation_signoff_eval_locked.csv`
- `artifacts/curation/reconciliation_signoff_eval_summary.json`

These artifacts are evaluation assets, not canonical pipeline outputs.

## Safe-to-delete rules

Safe to delete when not actively used:

- ad hoc `before_after_*` comparison files
- `*_after_*` fix-pass folders
- experiment directories under `data/analysis/experiments/` once findings are documented
- intermediate diagnosis samples such as `*_sample.csv`, `*_sample.txt`, `*_debug.csv`, `*_debug.txt`
- frozen local snapshots that have been superseded and are not referenced by docs

Do not delete casually:

- root-level canonical outputs listed above
- `data/analysis/workbook_bundle/`
- `data/output/persona_pipeline_output.xlsx` when it is the current verified deliverable
- `artifacts/curation/` files when they are being used as evaluation rails

## Regeneration rules

- canonical analysis outputs are regenerated by rerunning the appropriate downstream path from the changed stage
- workbook-facing changes should include `run/cli/16_persona_workbook_audit.py` when workbook denominators or policy overlays may change
- curation/evaluation artifacts are regenerated by their owner scripts, not by the main pipeline

## Low-risk cleanup rule for now

Until the physical directory migration happens:

- treat `data/analysis/` as a mixed runtime zone
- treat root-level canonical CSV/parquet/json plus `workbook_bundle/` as current truth
- treat ad hoc subfolders and `before_after_*` files as non-canonical unless a task explicitly asks for them
