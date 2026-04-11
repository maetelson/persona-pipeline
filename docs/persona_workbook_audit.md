# Persona Workbook Audit

## Scope

This audit traces the source-of-truth code that generates the workbook sheets:

- `overview`
- `cluster_stats`
- `persona_summary`
- `quality_checks`
- `source_diagnostics`
- `persona_examples`

It documents current inconsistencies before any business-logic fixes.

## Workbook Generation Map

Primary execution path:

1. `python run/07_export_xlsx.py`
2. `src.analysis.stage_service.run_final_report_stage`
3. `src.analysis.stage_service.run_analysis_stage`
4. `src.analysis.stage_service.build_deterministic_analysis_outputs`
5. `src.analysis.workbook_bundle.assemble_workbook_frames`
6. `src.exporters.xlsx_exporter.export_workbook_from_frames`

Sheet source-of-truth functions:

- `overview`
  - `src.analysis.persona_service._build_overview_df`
  - `src.analysis.stage_service._update_overview_quality`
- `cluster_stats`
  - `src.analysis.persona_service._build_cluster_stats_df`
- `persona_summary`
  - `src.analysis.persona_service._build_persona_summary_df`
- `quality_checks`
  - `src.analysis.summary.build_quality_checks`
  - `src.analysis.diagnostics.finalize_quality_checks`
  - `src.analysis.summary.build_quality_checks_df`
- `source_diagnostics`
  - `src.analysis.diagnostics.build_source_diagnostics`
- `persona_examples`
  - `src.analysis.example_selection.select_persona_representative_examples`
  - `src.analysis.bottleneck_clustering.build_bottleneck_cluster_outputs`
  - `src.analysis.persona_service._build_persona_examples_df`

Supporting data-shape decisions:

- persona-core clustering subset:
  - `src.analysis.stage_service._persona_core_subset`
- promoted/exploratory cluster classification:
  - `src.analysis.persona_service._cluster_promotion_policy`
- workbook frame persistence:
  - `src.analysis.workbook_bundle.write_workbook_bundle`

## Inconsistency Audit

### 1. `overview.total_labeled_records = 472`

- Source function:
  - `src.analysis.persona_service._build_overview_df`
- Current logic:
  - `total_labeled_records` is set from `total_labeled_records = int(quality_checks.get("labeled_count", len(labeled_df)))`
  - In `build_persona_outputs`, `quality_checks["labeled_count"]` comes from all labeled episode rows, not the persona-core subset.
- Intended semantic definition:
  - If the label says `total_labeled_records`, it is semantically correct for all labeled rows.
  - If the same sheet is expected to summarize persona clustering inputs, it also needs an explicit `persona_core_labeled_records`.
- Audit classification:
  - Sheet presentation mismatch.
  - Missing companion metric for the actual clustering denominator.

### 2. `quality_checks.persona_core_labeled_count = 289`

- Source function:
  - `src.analysis.summary.build_quality_checks`
- Current logic:
  - `core_labeled_df = _persona_core_subset(labeled_df)`
  - count is `len(core_labeled_df)`
- Intended semantic definition:
  - Number of labeled rows eligible for persona clustering after excluding `persona_core_eligible = false`.
- Audit classification:
  - Metric is internally consistent.
  - Presentation mismatch because this denominator is not surfaced as the denominator for cluster share sheets.

### 3. `quality_checks.unknown_ratio = 0.069204` with denominator `persona_core_labeled_rows`

- Source function:
  - `src.analysis.summary.build_quality_checks`
  - helper: `src.analysis.summary._row_unknown_ratio`
- Current logic:
  - `unknown_ratio = round(_row_unknown_ratio(core_labeled_df), 6)`
  - denominator is persona-core rows only.
- Intended semantic definition:
  - Unknown-label rate over the persona-clustering subset.
- Audit classification:
  - Formula is internally consistent.
  - Naming mismatch: `unknown_ratio` is ambiguous because there is also an overall unknown ratio.
  - Better explicit name would be `persona_core_unknown_ratio`.

### 4. `quality_checks.overall_unknown_ratio = 0.430085` with denominator `labeled_episode_rows`

- Source function:
  - `src.analysis.summary.build_quality_checks`
- Current logic:
  - `overall_unknown_ratio = round(_row_unknown_ratio(labeled_df), 6)`
- Intended semantic definition:
  - Unknown-label rate over all labeled episodes, including non-core rows.
- Audit classification:
  - Formula is internally consistent.
  - The caveat is not hidden in the sheet, but downstream quality flagging mostly ignores it.

### 5. `cluster_stats` and `persona_summary` use denominator 472 even though persona clustering uses 289 rows

- Source functions:
  - `src.analysis.persona_service._build_cluster_stats_df`
  - `src.analysis.persona_service._build_persona_summary_df`
  - denominator selection comes from `src.analysis.persona_service.build_persona_outputs`
- Current logic:
  - `total_labeled_records = int(quality_checks.get("labeled_count", len(labeled_df)))`
  - `share_of_total = round_pct(persona_size, total_labeled_records)`
  - `denominator_type = "labeled_episode_rows"`
  - `denominator_value = total_labeled_records`
- Intended semantic definition:
  - The rows in `cluster_stats` and `persona_summary` describe persona clusters built from the persona-core subset, not all labeled rows.
  - Their denominator should either be the core subset or the metric label should explicitly say `share_of_all_labeled`.
- Audit classification:
  - Denominator mismatch.
  - Sheet presentation mismatch.

### 6. `quality_checks.largest_cluster_share = 33.7` while persona_01 size is 159

- Source function:
  - `src.analysis.diagnostics.finalize_quality_checks`
- Current logic:
  - `largest_cluster_share` is taken from `cluster_stats_df["share_of_total"].max()`
  - `cluster_stats_df["share_of_total"]` is already computed against 472 in `_build_cluster_stats_df`
  - `quality_checks_df` then labels that value with denominator type `persona_core_labeled_rows` and denominator value `289`
- Intended semantic definition:
  - If denominator type is `persona_core_labeled_rows`, 159 should be compared to 289, which is about `55.0%`.
- Audit classification:
  - Formula + denominator mismatch.
  - The value is inherited from the wrong upstream denominator and then relabeled as if it were core-based.

### 7. `source_diagnostics` has suspicious funnel values

- Source function:
  - `src.analysis.diagnostics.build_source_diagnostics`
- Current logic:
  - `prefilter_survival_rate = prefiltered_count / valid_count`
  - `episode_survival_rate = episode_count / prefiltered_count`
  - `labeling_survival_rate = labeled_count / episode_count`
  - `labelable_count` comes from `labelability_audit.parquet` rows where status is in `{"labelable", "borderline"}`
  - `labeled_count` comes from labeled episode rows joined back to source
- Intended semantic definition:
  - A funnel sheet should compare like-for-like grains and monotonic stages.
- Observed problems:
  - `reddit`: `episode_count = 21` from `prefiltered_valid_count = 17`, so `episode_survival_rate = 123.5`
  - `metabase_discussions`: `labelable_count = 58`, `labeled_count = 230`
  - `github_discussions`: `labelable_count = 9`, `labeled_count = 13`
- Audit classification:
  - Naming mismatch and grain mismatch, not necessarily arithmetic bug.
  - `episode_count > prefiltered_valid_count` can happen because one post can yield multiple episodes, so the label `survival_rate` is misleading.
  - `labeled_count > labelable_count` happens because `labelable_count` excludes `low_signal`, while `labeled_count` counts all labeled outputs. This is not a same-grain funnel.

### 8. `persona_examples` has no selected example for promoted persona_02

- Source functions:
  - `src.analysis.example_selection.select_persona_representative_examples`
  - helper: `src.analysis.example_selection._select_diverse_examples`
  - workbook shaping: `src.analysis.persona_service._build_persona_examples_df`
- Current logic:
  - Only rows with `quote_quality in {"strong_representative", "usable"}` are eligible for normal selection.
  - Borderline fallback is only used if no usable quote exists and the borderline quote passes `_allow_borderline_fallback`.
- Current observed state:
  - promoted personas are `persona_01`, `persona_02`, `persona_04`
  - selected examples exist only for `persona_01` and `persona_04`
  - `persona_02` top candidates are only `borderline` or `reject`, so none are selected
- Intended semantic definition:
  - If a persona is promoted into the workbook, either:
    - it should have at least one grounded selected example, or
    - the workbook should explicitly mark that representative evidence is missing.
- Audit classification:
  - Sheet presentation mismatch and missing quality gate.
  - Not necessarily a scoring bug, but the promoted persona contract is incomplete.

### 9. `overview.quality_flag = OK` is too optimistic given `overall_unknown_ratio = 0.430085`

- Source functions:
  - `src.analysis.summary.build_quality_checks`
  - `src.analysis.diagnostics.finalize_quality_checks`
  - `src.analysis.stage_service._update_overview_quality`
- Current logic:
  - `build_quality_checks` starts with `compute_quality_flag(unknown_ratio)` where `unknown_ratio` uses the persona-core subset only
  - `finalize_quality_checks` upgrades to `UNSTABLE` or `EXPLORATORY` only for:
    - low effective labeled source count
    - example grounding failures
    - raw-to-labeled collapse
    - failed source coverage
    - cluster dominance / small promoted personas
  - `overall_unknown_ratio` is not part of the final quality flag decision
- Intended semantic definition:
  - A workbook-level `quality_flag` should either:
    - reflect whole-workbook reliability, including overall unknown burden, or
    - be renamed to a narrower label such as `persona_core_quality_flag`.
- Audit classification:
  - Semantic mismatch.
  - The flag is currently optimistic because it is effectively a persona-core quality flag presented as a workbook-wide quality flag.

## Reproducible Commands

Rebuild analysis artifacts and workbook bundle:

```powershell
python run/06_cluster_and_score.py
```

Inspect workbook-facing intermediate metrics without touching the xlsx:

```powershell
python run/16_persona_workbook_audit.py
```

Machine-readable audit dump:

```powershell
python run/16_persona_workbook_audit.py --json
```

Export the final workbook after generator fixes and after closing Excel:

```powershell
python run/07_export_xlsx.py
```

## Current Audit Summary

- The workbook is generated from code, not hand-authored, and the main source-of-truth path is `run/07_export_xlsx.py -> stage_service -> workbook bundle -> xlsx exporter`.
- The biggest consistency problem is denominator mixing between:
  - all labeled rows: `472`
  - persona-core labeled rows: `289`
- `cluster_stats`, `persona_summary`, and `largest_cluster_share` currently present persona-core cluster outputs using all-labeled denominators.
- `source_diagnostics` is mixing row grains while presenting the result as a simple stage funnel.
- `persona_examples` allows a promoted persona to appear without any selected example.
- `quality_flag` currently behaves like a persona-core quality flag, not a whole-workbook quality flag.
