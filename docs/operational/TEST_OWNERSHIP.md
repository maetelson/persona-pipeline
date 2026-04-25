# Test Ownership

This file maps policies and pipeline subsystems to the tests or validation scripts that should fail when those contracts regress.

It is not a guarantee that coverage is complete. It is a navigation aid for making safe changes.

## Policy to test map

| Area | Primary tests / validators |
|---|---|
| Invalid filtering policy | `tests.test_invalid_filter`, `tests.test_source_cli_smoke`, `tests.test_prefilter_stage_runner` |
| Relevance prefilter policy | `tests.test_relevance_prefilter`, `tests.test_prefilter_stage_runner`, `tests.test_source_cli_smoke` |
| Time-window filtering | `tests.test_time_window_filter`, `tests.test_pipeline_schema` |
| Episode builder / segmentation | `tests.test_episode_builder`, `tests.test_pipeline_schema` |
| Labeler runtime / low-signal discrepancy rescue | `tests.test_llm_labeler_runtime`, `tests.test_label_quality` |
| Label quality and schema consistency | `tests.test_label_quality`, `tests.test_pipeline_schema` |
| Axis discovery / reduction | `tests.test_axis_reduction`, `tests.test_axis_cli_smoke` |
| Bottleneck clustering | `tests.test_bottleneck_clustering`, `tests.test_cluster_cli_smoke` |
| Persona example selection / grounding | `tests.test_example_selection`, `tests.test_example_cli_smoke` |
| Persona taxonomy / messaging | `tests.test_persona_core_taxonomy`, `tests.test_persona_messaging`, `tests.test_persona_cli_smoke` |
| Persona promotion / workbook policy overlay | `tests.test_persona_workbook_regressions`, `tests.test_analysis_snapshot_cli` |
| Source diagnostics / source balance audit | `tests.test_source_diagnostics`, `tests.test_source_stage_profiler`, `tests.test_source_aggregation`, `tests.test_source_cli_smoke` |
| Workbook export | `tests.test_workbook_export`, `tests.test_persona_workbook_regressions` |
| Pipeline schema and cross-stage contracts | `tests.test_pipeline_schema`, `tests.test_analysis_snapshot_cli` |
| Source collectors / source configs | `tests.test_discourse_collector`, `tests.test_stackoverflow_collector`, `tests.test_reddit_public_collector`, `tests.test_business_community_sources` |
| Seed bank and source inventory | `tests.test_seed_bank`, `tests.test_business_community_sources`, `tests.test_reddit_retention` |
| Curation/evaluation artifacts | `run/cli/19_build_reconciliation_signoff_curation.py`, `run/cli/20_validate_reconciliation_signoff_curation.py` |

## Hotspot ownership

These files are especially important because they encode cross-cutting policy behavior.

- filtering:
  - `src/filters/invalid_filter.py`
  - `src/filters/relevance.py`
- episode build:
  - `src/episodes/builder.py`
- labeling:
  - `src/labeling/llm_labeler.py`
  - `run/pipeline/05_label_episodes.py`
- analysis:
  - `src/analysis/bottleneck_clustering.py`
  - `src/analysis/persona_service.py`
  - `src/analysis/stage_service.py`
  - `src/analysis/diagnostics.py`
- export:
  - `src/exporters/xlsx_exporter.py`

When these files change, do not rely on a single unit test. Use the mapped cluster of tests.

## Known coverage gaps

- There is no dedicated unit test for artifact lifecycle classification yet.
- Curation artifacts currently rely on build/validate scripts rather than a dedicated unittest module.
- `data/analysis/` cleanup semantics are documented, but not enforced by a repository-wide linter or checker.
