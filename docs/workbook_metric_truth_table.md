# Workbook Metric Truth Table

This document audits the summary-layer metrics in `persona_pipeline_output.reviewable.xlsx` before any business-logic changes.

It answers three questions:

1. Which function produces each workbook summary metric?
2. Which artifact or file actually feeds that metric?
3. Where do workbook labels collide, alias, or mislead?

## File / Function Map

Summary-sheet production path:

1. `src/analysis/stage_service.py::build_deterministic_analysis_outputs()` loads stage artifacts and builds in-memory frames.
2. `src/analysis/summary.py::build_counts_table()` builds the `counts` sheet.
3. `src/analysis/stage_service.py::_build_final_overview_df()` builds the `overview` sheet from the flattened quality result plus `cluster_stats_df`.
4. `src/analysis/quality_status.py::build_quality_metrics()` computes raw quality metrics.
5. `src/analysis/quality_status.py::evaluate_quality_status()` applies the centralized status policy.
6. `src/analysis/diagnostics.py::finalize_quality_checks()` calls `flatten_quality_status_result()` and returns the workbook-facing quality metric dictionary.
7. `src/analysis/summary.py::build_quality_checks_df()` renders that dictionary into `quality_checks` rows.
8. `src/analysis/summary.py::append_source_survival_rows()` appends the per-source `metric:source` rows in `quality_checks` from `source_stage_counts_df`.
9. `src/analysis/diagnostics.py::build_quality_failures()` builds the `quality_failures` sheet.
10. `src/exporters/xlsx_exporter.py::_write_readme_sheet()` builds the `readme` sheet as Excel formulas over the `overview` sheet.

## Artifact Legend

- `A1`: `data/raw/{source}/*.jsonl`
- `A2`: `data/analysis/raw_audit.parquet`
- `A3`: `data/normalized/normalized_posts.parquet`
- `A4`: `data/valid/valid_candidates.parquet`
- `A5`: `data/valid/valid_candidates_prefiltered.parquet`
- `A6`: `data/episodes/episode_table.parquet`
- `A7`: `data/labeled/labeled_episodes.parquet`
- `A8`: `data/labeled/labelability_audit.parquet`
- `A9`: `data/prefilter/relevance_drop.parquet`
- `A10`: `data/valid/invalid_candidates_with_prefilter.parquet`
- `A11`: `data/analysis/cluster_stats.csv` and in-memory `cluster_stats_df`
- `A12`: `data/analysis/persona_examples.csv` and in-memory `persona_examples_df`
- `A13`: in-memory `source_stage_counts_df`, persisted downstream as `data/analysis/source_diagnostics.csv`
- `A14`: `data/analysis/overview.csv` and workbook sheet `overview`
- `A15`: workbook sheet `readme` formulas authored in `src/exporters/xlsx_exporter.py::_write_readme_sheet()`

## Highest-Severity Semantic Collisions

1. `counts.raw_records = 7739` and `quality_checks.total_raw_count = 208` are both presented as raw-like totals, but they come from different producers and different artifacts.
   `raw_records` comes from a direct filesystem scan of `A1` via `count_raw_jsonl_by_source()`. `total_raw_count` comes from `A2` via `raw_audit_df["raw_record_count"].sum()` in `build_deterministic_analysis_outputs()`. The sheet labels do not tell the reviewer these are different concepts.
2. `counts.valid_records = 3438` and `quality_checks.cleaned_count = 3438` are the same `len(valid_df)` from `A4`, under two different names.
3. `quality_checks.example_grounding_failure_count = 0` does not mean grounding is healthy. It only counts weak or bad *selected example rows* in `A12`. It does not count promoted personas with no selected example at all. That is why `promoted_persona_ungrounded_count = 1`, `promoted_persona_example_coverage_pct = 66.7`, and `example_grounding_status = FAIL` can coexist with `example_grounding_failure_count = 0`.
4. `overview.persona_count = 3`, `quality_checks.promoted_persona_count = 3`, and `readme[Promoted Persona Rows] = 3` all describe the same promoted persona count, but under different names. The readme label is especially misleading because it says `Rows` while the value is actually a persona count.
5. `overview.total_labeled_records = 472`, `quality_checks.labeled_count = 472`, and `readme[All Labeled Rows] = 472` are the same number under different labels.
6. `overview.persona_core_labeled_records = 289`, `quality_checks.persona_core_labeled_count = 289`, `quality_checks.persona_core_labeled_records = 289`, and `readme[Persona Core Rows] = 289` are the same concept repeated under multiple names.
7. `quality_checks.grounding_coverage_status` and `quality_checks.example_grounding_status` are currently duplicates in practice because the grouped `example_grounding` status is composed from the single `grounding_coverage` axis.
8. `quality_failures.example_grounding_gate` shows `value = 0`, but the gate fails for `promoted_persona_examples_coverage_critical | promoted_persona_examples_missing`. The number shown is not the number that drove the failure.

## Truth Table

### counts

| workbook_sheet | workbook_metric_name | actual_source_function | actual_source_artifact/file | entity_grain | intended semantic definition | current displayed definition | collision_or_mismatch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| counts | raw_records | `src/analysis/summary.py::build_counts_table()` using `src/analysis/diagnostics.py::count_raw_jsonl_by_source()` | `A1` | raw JSONL row | Total non-empty raw JSONL rows on disk | `Non-empty JSONL rows under data/raw/{source}/*.jsonl.` | Collides with `quality_checks.total_raw_count`, which is not built from the same artifact. |
| counts | normalized_records | `src/analysis/summary.py::build_counts_table()` | `A3` | normalized post row | Total normalized rows after source normalizers | `Rows in normalized_posts.parquet.` | In this workbook it equals `raw_records`, but that is a data coincidence, not a naming collision. |
| counts | valid_records | `src/analysis/summary.py::build_counts_table()` | `A4` | valid candidate row | Total valid candidates before relevance prefilter | `Rows in valid_candidates.parquet before relevance prefiltering.` | Same number and same concept as `quality_checks.cleaned_count`. |
| counts | prefiltered_valid_records | `src/analysis/summary.py::build_counts_table()` | `A5` | prefiltered valid candidate row | Total candidates kept by relevance prefilter | `Rows passed into episode building when the prefilter output exists.` | No direct naming collision in summary sheets. |
| counts | episodes | `src/analysis/summary.py::build_counts_table()` | `A6` | episode row | Total episode rows | `Rows in episode_table.parquet.` | Distinct concept. |
| counts | labeled_records | `src/analysis/summary.py::build_counts_table()` | `A7` | labeled episode row | Total labeled episode rows | `Rows in labeled_episodes.parquet.` | Same concept as `overview.total_labeled_records`, `quality_checks.labeled_count`, and `readme[All Labeled Rows]`. |

### overview

| workbook_sheet | workbook_metric_name | actual_source_function | actual_source_artifact/file | entity_grain | intended semantic definition | current displayed definition | collision_or_mismatch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| overview | overall_status | `src/analysis/stage_service.py::_build_final_overview_df()` from `flatten_quality_status_result(evaluate_quality_status(build_quality_metrics(...)))` | `A7 + A11 + A12 + A13` | workbook-wide status | Composite quality status across axes | No explicit definition on sheet | Same value as `quality_checks.overall_status`. |
| overview | quality_flag | same as above | `A7 + A11 + A12 + A13` | workbook-wide flag | Display flag derived from `overall_status` | No explicit definition on sheet | Same value as `quality_checks.quality_flag`. |
| overview | quality_flag_rule | `src/analysis/quality_status.py::flatten_quality_status_result()` rendered by `_build_final_overview_df()` | policy constant in code | workbook-wide rule | Rule for deriving `quality_flag` from `overall_status` | No explicit definition on sheet | Same text as `quality_checks.quality_flag_rule`. |
| overview | composite_reason_keys | `flatten_quality_status_result()` | evaluated quality result from `A7 + A11 + A12 + A13` | workbook-wide reason list | Aggregated reason keys behind `overall_status` | No explicit definition on sheet | Same concept as `quality_checks.composite_reason_keys`. |
| overview | core_clustering_status | `flatten_quality_status_result()` | `A7 + A11` | grouped status | Grouped status over core clustering axes | No explicit definition on sheet | Same as `quality_checks.core_clustering_status`. |
| overview | source_diversity_status | `flatten_quality_status_result()` | `A13` | grouped status | Grouped status over source diversity axes | No explicit definition on sheet | Same as `quality_checks.source_diversity_status`. |
| overview | example_grounding_status | `flatten_quality_status_result()` | `A11 + A12` | grouped status | Grouped status over example grounding axes | No explicit definition on sheet | In practice duplicates `quality_checks.grounding_coverage_status` because group has one axis. |
| overview | overall_unknown_status | `flatten_quality_status_result()` | `A7` | status over labeled rows | Status for overall unknown-label ratio | No explicit definition on sheet | Same as `quality_checks.overall_unknown_status`. |
| overview | core_unknown_status | `flatten_quality_status_result()` | `A7` | status over persona-core labeled rows | Status for core unknown-label ratio | No explicit definition on sheet | Same as `quality_checks.core_unknown_status`. |
| overview | core_coverage_status | `flatten_quality_status_result()` | `A7` | status over labeled rows | Status for persona-core coverage of all labeled | No explicit definition on sheet | Same as `quality_checks.core_coverage_status`. |
| overview | effective_source_diversity_status | `flatten_quality_status_result()` | `A13` | status over source contributions | Status for effective labeled source count | No explicit definition on sheet | Same as `quality_checks.effective_source_diversity_status`. |
| overview | source_concentration_status | `flatten_quality_status_result()` | `A13` | status over labeled-source shares | Status for largest labeled source share | No explicit definition on sheet | Same as `quality_checks.source_concentration_status`. |
| overview | largest_cluster_dominance_status | `flatten_quality_status_result()` | `A11` | status over cluster shares | Status for largest cluster share of persona-core labeled rows | No explicit definition on sheet | Same as `quality_checks.largest_cluster_dominance_status`. |
| overview | grounding_coverage_status | `flatten_quality_status_result()` | `A11 + A12` | status over promoted personas | Axis status for promoted persona grounding coverage | No explicit definition on sheet | Duplicates `quality_checks.grounding_coverage_status`; semantically overlaps `example_grounding_status`. |
| overview | total_labeled_records | `_build_final_overview_df()` | `A7` | labeled episode row | Total labeled rows | No explicit definition on sheet | Same concept as `counts.labeled_records`, `quality_checks.labeled_count`, and `readme[All Labeled Rows]`. |
| overview | persona_core_labeled_records | `_build_final_overview_df()` | `A7` | persona-core labeled episode row | Total labeled rows with `persona_core_eligible=true` | No explicit definition on sheet | Same concept as `quality_checks.persona_core_labeled_count`, `quality_checks.persona_core_labeled_records`, and `readme[Persona Core Rows]`. |
| overview | persona_core_coverage_of_all_labeled_pct | `_build_final_overview_df()` via `build_quality_metrics()` | `A7` | labeled episode row | Share of all labeled rows that are persona-core eligible | No explicit definition on sheet | Same metric as `quality_checks.persona_core_coverage_of_all_labeled_pct`. |
| overview | persona_core_unknown_ratio | `_build_final_overview_df()` via `build_quality_metrics()` | `A7` | persona-core labeled episode row | Unknown-label ratio within persona-core subset | No explicit definition on sheet | Same metric as `quality_checks.persona_core_unknown_ratio`. |
| overview | overall_unknown_ratio | `_build_final_overview_df()` via `build_quality_metrics()` | `A7` | labeled episode row | Unknown-label ratio across all labeled rows | No explicit definition on sheet | Same metric as `quality_checks.overall_unknown_ratio`; used by readme `Approx Unknown Rows`. |
| overview | effective_labeled_source_count | `_build_final_overview_df()` via `build_quality_metrics()` | `A13` | source contribution score | Effective labeled source count with weak-contribution weighting | No explicit definition on sheet | Same metric as `quality_checks.effective_labeled_source_count`. |
| overview | largest_cluster_share_of_core_labeled | `_build_final_overview_df()` via `build_quality_metrics()` | `A11` | persona-core labeled row share | Largest cluster share over persona-core labeled denominator | No explicit definition on sheet | Same metric as `quality_checks.largest_cluster_share_of_core_labeled`. |
| overview | largest_labeled_source_share_pct | `_build_final_overview_df()` via `build_quality_metrics()` | `A13` | labeled episode row share | Largest source share over labeled rows | No explicit definition on sheet | Same metric as `quality_checks.largest_labeled_source_share_pct`. |
| overview | promoted_persona_example_coverage_pct | `_build_final_overview_df()` via `build_quality_metrics()` | `A11 + A12` | promoted persona | Share of promoted personas with accepted grounding state | No explicit definition on sheet | Same metric as `quality_checks.promoted_persona_example_coverage_pct` and `quality_failures.promoted_example_coverage_gate.value`. |
| overview | promoted_persona_grounded_count | `_build_final_overview_df()` via `build_quality_metrics()` | `A11 + A12` | promoted persona | Count of promoted personas with grounded example coverage | No explicit definition on sheet | Same metric as `quality_checks.promoted_persona_grounded_count`. |
| overview | promoted_persona_weakly_grounded_count | `_build_final_overview_df()` via `build_quality_metrics()` | `A11 + A12` | promoted persona | Count of promoted personas covered only by weak fallback | No explicit definition on sheet | Same metric as `quality_checks.promoted_persona_weakly_grounded_count`. |
| overview | promoted_persona_ungrounded_count | `_build_final_overview_df()` via `build_quality_metrics()` | `A11 + A12` | promoted persona | Count of promoted personas with no accepted grounding evidence | No explicit definition on sheet | Same metric as `quality_checks.promoted_persona_ungrounded_count`. |
| overview | promoted_personas_weakly_grounded | `_build_final_overview_df()` via `build_quality_metrics()` | `A11 + A12` | promoted persona id list | Pipe-delimited weakly grounded persona ids | No explicit definition on sheet | Same concept as `quality_checks.promoted_personas_weakly_grounded`. |
| overview | promoted_personas_missing_examples | `_build_final_overview_df()` via `build_quality_metrics()` | `A11 + A12` | promoted persona id list | Pipe-delimited promoted persona ids with no accepted selected example | No explicit definition on sheet | Same concept as `quality_checks.promoted_personas_missing_examples`. |
| overview | persona_count | `_build_final_overview_df()` from `cluster_stats_df[promotion_status == "promoted_persona"]` | `A11` | promoted persona | Count of promoted personas | No explicit definition on sheet | Same concept as `quality_checks.promoted_persona_count`; readme calls it `Promoted Persona Rows`, which is misleading. |
| overview | exploratory_bucket_count | `_build_final_overview_df()` from non-promoted `cluster_stats_df` rows | `A11` | exploratory cluster | Count of non-promoted buckets | No explicit definition on sheet | Distinct concept. |
| overview | min_cluster_size | `_build_final_overview_df()` via `build_quality_metrics()` | derived from `len(A7)` | threshold count | Promotion size floor | No explicit definition on sheet | Same metric as `quality_checks.min_cluster_size`. |
| overview | selected_axes | `_build_final_overview_df()` | reduced axis schema in memory, persisted through analysis outputs | axis name list | Final selected persona axes | No explicit definition on sheet | Distinct concept. |
| overview | clustering_mode | `_build_final_overview_df()` hardcoded literal | code constant | mode label | Descriptive label for clustering mode | No explicit definition on sheet | Distinct concept. |

### quality_checks

Note: the table below covers the non-source-suffixed summary metrics. Rows like `valid_posts_per_normalized_post_pct:github_discussions` are appended later by `src/analysis/summary.py::append_source_survival_rows()` from `A13` and are not top-line summary metrics.

| workbook_sheet | workbook_metric_name | actual_source_function | actual_source_artifact/file | entity_grain | intended semantic definition | current displayed definition | collision_or_mismatch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| quality_checks | total_raw_count | `src/analysis/quality_status.py::build_quality_metrics()` with `total_raw_count` argument supplied by `src/analysis/stage_service.py::build_deterministic_analysis_outputs()` | `A2` | raw audit source row sum | Intended to be total raw count for quality policy input | No explicit definition; denominator shows `raw_jsonl_rows` | Semantic bug: labeled as raw JSONL total, but actually summed from `raw_audit.parquet`, not from `A1`. This is why `208` coexists with `counts.raw_records = 7739`. |
| quality_checks | cleaned_count | `build_quality_metrics()` | `A4` | valid candidate row | Count of cleaned / valid rows feeding later stages | No explicit definition; denominator shows `valid_candidate_rows` | Same number and concept as `counts.valid_records`; alias collision. |
| quality_checks | labeled_count | `build_quality_metrics()` | `A7` | labeled episode row | Count of all labeled rows | No explicit definition | Same concept as `counts.labeled_records`, `overview.total_labeled_records`, and readme `All Labeled Rows`. |
| quality_checks | persona_core_labeled_count | `build_quality_metrics()` | `A7` | persona-core labeled episode row | Count of persona-core labeled rows | No explicit definition | Same concept as `overview.persona_core_labeled_records`; duplicated again by `persona_core_labeled_records`. |
| quality_checks | persona_core_labeled_records | `build_quality_metrics()` | `A7` | persona-core labeled episode row | Same as above, preserved as alternate key | No explicit definition | Duplicate of `persona_core_labeled_count`. |
| quality_checks | persona_core_unknown_ratio | `build_quality_metrics()` | `A7` | persona-core labeled episode row | Unknown-label ratio inside persona-core subset | Threshold shown | Same metric as overview. |
| quality_checks | overall_unknown_ratio | `build_quality_metrics()` | `A7` | labeled episode row | Unknown-label ratio across all labeled rows | Threshold shown | Same metric as overview. |
| quality_checks | persona_core_coverage_of_all_labeled_pct | `build_quality_metrics()` | `A7` | labeled episode row | Share of all labeled rows retained in persona-core subset | Threshold shown | Same metric as overview. |
| quality_checks | cluster_count | `build_quality_metrics()` | cluster profiles in memory | cluster row | Number of cluster profiles | No explicit definition | Distinct concept. |
| quality_checks | cluster_distribution | `build_quality_metrics()` | cluster profiles in memory | cluster row list | Distribution payload for cluster sizes and shares | Notes cell contains serialized list | Distinct concept; not echoed in overview. |
| quality_checks | labeled_source_count | `build_quality_metrics()` | `A13` | source | Count of sources with at least one labeled episode | No explicit definition | Related to, but not same as, `effective_labeled_source_count`. |
| quality_checks | effective_labeled_source_count | `build_quality_metrics()` | `A13` | weighted source contribution | Fractionally weighted labeled source count | Threshold shown | Same metric as overview. |
| quality_checks | raw_source_count | `build_quality_metrics()` | `A13` | source | Count of sources with raw coverage | No explicit definition | Distinct concept. |
| quality_checks | min_cluster_size | `build_quality_metrics()` | derived from `len(A7)` | threshold count | Minimum size floor for promoted personas | No explicit definition | Same metric as overview. |
| quality_checks | largest_cluster_share_of_core_labeled | `build_quality_metrics()` | `A11` | persona-core labeled row share | Largest cluster share over persona-core denominator | Threshold shown | Same metric as overview. |
| quality_checks | largest_labeled_source_share_pct | `build_quality_metrics()` | `A13` | labeled episode row share | Largest source share over labeled denominator | Threshold shown | Same metric as overview. |
| quality_checks | single_cluster_dominance | `build_quality_metrics()` | `A11` | boolean flag | Whether largest cluster exceeds dominance threshold | No explicit definition | Related to `largest_cluster_dominance_status`, not same concept. |
| quality_checks | small_promoted_persona_count | `build_quality_metrics()` | `A11` | promoted persona | Count of promoted personas below min size floor | No explicit definition | Used by `quality_failures.persona_promotion_gate`. |
| quality_checks | example_grounding_failure_count | `build_quality_metrics()` via `src/analysis/quality_status.py::_example_failure_count()` | `A12` | selected example row | Count selected examples with weak grounding evidence | No explicit definition | Misleading name: does not count missing examples, so `0` can coexist with ungrounded promoted personas and failing grounding coverage. |
| quality_checks | promoted_persona_count | `build_quality_metrics()` via `_promoted_persona_example_counts()` | `A11` | promoted persona | Count of promoted personas | No explicit definition | Same concept as `overview.persona_count` and readme `Promoted Persona Rows`. |
| quality_checks | promoted_personas_with_examples | `build_quality_metrics()` via `_promoted_persona_example_counts()` | `A11 + A12` | promoted persona | Count of promoted personas in accepted grounded or weak-grounded states | No explicit definition | Feeds `promoted_persona_example_coverage_pct`. |
| quality_checks | promoted_personas_missing_examples | `build_quality_metrics()` via `_promoted_persona_example_counts()` | `A11 + A12` | promoted persona id list | Promoted persona ids without accepted selected examples | No explicit definition | Same concept as overview. |
| quality_checks | promoted_persona_example_coverage_pct | `build_quality_metrics()` via `_promoted_persona_example_counts()` | `A11 + A12` | promoted persona | Coverage share of promoted personas with accepted grounding | Threshold shown | Same metric as overview; also drives `quality_failures.promoted_example_coverage_gate`. |
| quality_checks | promoted_persona_grounded_count | `build_quality_metrics()` via `_promoted_persona_example_counts()` | `A11 + A12` | promoted persona | Count of promoted personas in `promoted_and_grounded` state | No explicit definition | Same metric as overview. |
| quality_checks | promoted_persona_weakly_grounded_count | `build_quality_metrics()` via `_promoted_persona_example_counts()` | `A11 + A12` | promoted persona | Count of promoted personas in `promoted_but_weakly_grounded` state | No explicit definition | Same metric as overview. |
| quality_checks | promoted_persona_ungrounded_count | `build_quality_metrics()` via `_promoted_persona_example_counts()` | `A11 + A12` | promoted persona | Count of promoted personas in `promoted_but_ungrounded` state | No explicit definition | Same metric as overview. |
| quality_checks | promoted_personas_weakly_grounded | `build_quality_metrics()` via `_promoted_persona_example_counts()` | `A11 + A12` | promoted persona id list | Weakly grounded persona ids | No explicit definition | Same concept as overview. |
| quality_checks | source_failures | `build_quality_metrics()` via `_failed_sources()` | `A13` | source id list | Sources with raw coverage but zero labeled output | No explicit definition | Distinct concept. |
| quality_checks | denominator_consistency | `build_quality_metrics()` hardcoded literal | code literal | workbook check flag | Marker that denominator semantics are explicit | Notes say rendered from centralized evaluated status result | Distinct concept. |
| quality_checks | largest_cluster_share_denominator_type | `build_quality_metrics()` hardcoded literal | code literal | denominator label | Declares denominator family for largest cluster share | No explicit definition | Distinct concept. |
| quality_checks | core_unknown_status | `flatten_quality_status_result()` after `evaluate_quality_status()` | policy over `A7` | status | Status for `persona_core_unknown_ratio` | No explicit definition | Same status family as overview. |
| quality_checks | core_unknown_reason_keys | `flatten_quality_status_result()` | policy over `A7` | reason key list | Reason keys behind `core_unknown_status` | No explicit definition | Same as overview family, not shown there directly. |
| quality_checks | core_unknown_threshold_rule | `flatten_quality_status_result()` | policy constant | threshold text | Display threshold rule for core unknown | No explicit definition in sheet except value cell | Distinct but policy-derived. |
| quality_checks | overall_unknown_status | `flatten_quality_status_result()` | policy over `A7` | status | Status for `overall_unknown_ratio` | No explicit definition | Same as overview. |
| quality_checks | overall_unknown_reason_keys | `flatten_quality_status_result()` | policy over `A7` | reason key list | Reason keys behind `overall_unknown_status` | No explicit definition | Supports `quality_failures.overall_uncertainty_gate`. |
| quality_checks | overall_unknown_threshold_rule | `flatten_quality_status_result()` | policy constant | threshold text | Display threshold rule for overall unknown | No explicit definition | Distinct but policy-derived. |
| quality_checks | core_coverage_status | `flatten_quality_status_result()` | policy over `A7` | status | Status for persona-core coverage metric | No explicit definition | Same as overview. |
| quality_checks | core_coverage_reason_keys | `flatten_quality_status_result()` | policy over `A7` | reason key list | Reason keys behind `core_coverage_status` | No explicit definition | Supports `quality_failures.core_coverage_gate`. |
| quality_checks | core_coverage_threshold_rule | `flatten_quality_status_result()` | policy constant | threshold text | Display threshold rule for core coverage | No explicit definition | Distinct but policy-derived. |
| quality_checks | effective_source_diversity_status | `flatten_quality_status_result()` | policy over `A13` | status | Status for effective source diversity | No explicit definition | Same as overview. |
| quality_checks | effective_source_diversity_reason_keys | `flatten_quality_status_result()` | policy over `A13` | reason key list | Reason keys behind effective source diversity status | No explicit definition | Supports grouped `source_diversity_status`. |
| quality_checks | effective_source_diversity_threshold_rule | `flatten_quality_status_result()` | policy constant | threshold text | Display threshold rule for effective source diversity | Current value appears blank/NaN in workbook for this build | Display defect: threshold text is not surfacing cleanly. |
| quality_checks | source_concentration_status | `flatten_quality_status_result()` | policy over `A13` | status | Status for largest labeled source share | No explicit definition | Same as overview. |
| quality_checks | source_concentration_reason_keys | `flatten_quality_status_result()` | policy over `A13` | reason key list | Reason keys behind source concentration status | No explicit definition | Supports `quality_failures.source_concentration_gate`. |
| quality_checks | source_concentration_threshold_rule | `flatten_quality_status_result()` | policy constant | threshold text | Display threshold rule for source concentration | No explicit definition | Distinct but policy-derived. |
| quality_checks | largest_cluster_dominance_status | `flatten_quality_status_result()` | policy over `A11` | status | Status for cluster dominance | No explicit definition | Same as overview. |
| quality_checks | largest_cluster_dominance_reason_keys | `flatten_quality_status_result()` | policy over `A11` | reason key list | Reason keys behind cluster dominance status | No explicit definition | Supports `quality_failures.cluster_dominance_gate`. |
| quality_checks | largest_cluster_dominance_threshold_rule | `flatten_quality_status_result()` | policy constant | threshold text | Display threshold rule for cluster dominance | No explicit definition | Distinct but policy-derived. |
| quality_checks | grounding_coverage_status | `flatten_quality_status_result()` | policy over `A11 + A12` | status | Axis status for promoted persona coverage | No explicit definition | In practice duplicates `example_grounding_status` because group has one axis. |
| quality_checks | grounding_coverage_reason_keys | `flatten_quality_status_result()` | policy over `A11 + A12` | reason key list | Reason keys behind grounding coverage status | No explicit definition | Same reasons reappear in `example_grounding_status` and failure gates. |
| quality_checks | grounding_coverage_threshold_rule | `flatten_quality_status_result()` | policy constant | threshold text | Display threshold rule for grounding coverage | Current value appears blank/NaN in workbook for this build | Display defect or export coercion issue. |
| quality_checks | core_clustering_status | `flatten_quality_status_result()` | grouped policy over `A7 + A11` | grouped status | Grouped status for clustering quality | Notes show grouped reasons | Same as overview. |
| quality_checks | core_clustering_reason_keys | `flatten_quality_status_result()` | grouped policy over `A7 + A11` | grouped reason key list | Grouped reasons behind `core_clustering_status` | No explicit definition | Same concept as overview `composite_reason_keys` subset. |
| quality_checks | source_diversity_status | `flatten_quality_status_result()` | grouped policy over `A13` | grouped status | Grouped status for source diversity quality | No explicit definition | Same as overview. |
| quality_checks | source_diversity_reason_keys | `flatten_quality_status_result()` | grouped policy over `A13` | grouped reason key list | Grouped reasons behind `source_diversity_status` | No explicit definition | Distinct grouped view. |
| quality_checks | example_grounding_status | `flatten_quality_status_result()` | grouped policy over `A11 + A12` | grouped status | Grouped status for example grounding quality | Notes show grouped reasons | Duplicates `grounding_coverage_status` in this implementation. |
| quality_checks | example_grounding_reason_keys | `flatten_quality_status_result()` | grouped policy over `A11 + A12` | grouped reason key list | Grouped reasons behind `example_grounding_status` | No explicit definition | Duplicates `grounding_coverage_reason_keys` in this implementation. |
| quality_checks | overall_status | `flatten_quality_status_result()` | composite policy over `A7 + A11 + A12 + A13` | composite status | Final status across all axes | Threshold column explains composite rule | Same as overview. |
| quality_checks | composite_reason_keys | `flatten_quality_status_result()` | composite policy over `A7 + A11 + A12 + A13` | composite reason key list | Combined reason keys behind `overall_status` | No explicit definition | Same as overview. |
| quality_checks | quality_flag | `flatten_quality_status_result()` | composite policy result | composite display flag | Display quality flag derived from `overall_status` | Threshold column explains mapping | Same as overview. |
| quality_checks | quality_flag_rule | `flatten_quality_status_result()` | code literal | display rule | Human-readable rule for `quality_flag` | No explicit definition | Same as overview. |

### quality_failures

| workbook_sheet | workbook_metric_name | actual_source_function | actual_source_artifact/file | entity_grain | intended semantic definition | current displayed definition | collision_or_mismatch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| quality_failures | overall_uncertainty_gate | `src/analysis/diagnostics.py::build_quality_failures()` | `quality_checks[overall_unknown_*]`, ultimately `A7` | gate | Gate row for overall unknown ratio | No explicit definition beyond threshold/reason | Consistent with `quality_checks.overall_unknown_status`. |
| quality_failures | core_coverage_gate | `build_quality_failures()` | `quality_checks[core_coverage_*]`, ultimately `A7` | gate | Gate row for persona-core coverage | No explicit definition beyond threshold/reason | Consistent with `quality_checks.core_coverage_status`. |
| quality_failures | source_diversity_gate | `build_quality_failures()` | `quality_checks[effective_source_diversity_*]`, ultimately `A13` | gate | Gate row for effective source diversity | No explicit definition beyond threshold/reason | Consistent with `quality_checks.effective_source_diversity_status`. |
| quality_failures | source_concentration_gate | `build_quality_failures()` | `quality_checks[source_concentration_*]`, ultimately `A13` | gate | Gate row for source concentration | No explicit definition beyond threshold/reason | Consistent with `quality_checks.source_concentration_status`. |
| quality_failures | cluster_dominance_gate | `build_quality_failures()` | `quality_checks[largest_cluster_dominance_*]`, ultimately `A11` | gate | Gate row for largest cluster dominance | No explicit definition beyond threshold/reason | Consistent with `quality_checks.largest_cluster_dominance_status`. |
| quality_failures | persona_promotion_gate | `build_quality_failures()` using `small_promoted_persona_count` and `min_cluster_size` | `A11` | gate | Gate row for promoted personas below minimum size | No explicit definition beyond threshold/reason | Distinct concept. |
| quality_failures | raw_to_labeled_source_gate | `build_quality_failures()` using `raw_source_count` and labeled-source count | `A13` | gate | Gate row for raw source coverage collapsing into very few labeled sources | No explicit definition beyond threshold/reason | Distinct concept. |
| quality_failures | example_grounding_gate | `build_quality_failures()` | `quality_checks[grounding_coverage_*]` plus `quality_checks[example_grounding_failure_count]`, ultimately `A11 + A12` | gate | Intended as gate for example grounding failures | No explicit definition beyond threshold/reason | Mismatch: displayed numeric value is `example_grounding_failure_count`, but failure threshold/reason is copied from grounding coverage status, not from that count. |
| quality_failures | promoted_example_coverage_gate | `build_quality_failures()` | `quality_checks[promoted_persona_example_coverage_pct]`, ultimately `A11 + A12` | gate | Gate row for promoted persona example coverage | No explicit definition beyond threshold/reason | Consistent with policy, but semantically overlaps `example_grounding_gate`. |
| quality_failures | denominator_consistency_check | `build_quality_failures()` | code literal plus workbook rendering contract | workbook check flag | Confirms denominator metadata is present | `all summary rows expose denominator_type/value` | Distinct concept. |

### readme

| workbook_sheet | workbook_metric_name | actual_source_function | actual_source_artifact/file | entity_grain | intended semantic definition | current displayed definition | collision_or_mismatch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| readme | All Labeled Rows | `src/exporters/xlsx_exporter.py::_write_readme_sheet()` formula `INDEX/MATCH` into `overview.total_labeled_records` | `A15` formula over `A14` | labeled episode row | Reviewer shortcut to total labeled rows | `Formula-backed links to the workbook overview metrics.` | Same concept as `counts.labeled_records` and `quality_checks.labeled_count`; label is clear. |
| readme | Persona Core Rows | `_write_readme_sheet()` formula into `overview.persona_core_labeled_records` | `A15` formula over `A14` | persona-core labeled episode row | Reviewer shortcut to persona-core labeled rows | same as above | Same concept as `quality_checks.persona_core_labeled_count` and `persona_core_labeled_records`. |
| readme | Promoted Persona Rows | `_write_readme_sheet()` formula into `overview.persona_count` | `A15` formula over `A14` | promoted persona | Reviewer shortcut to promoted persona count | same as above | Misleading label: says `Rows`, but source metric is persona count, not row count. |
| readme | Approx Unknown Rows | `_write_readme_sheet()` formula `ROUND(overall_unknown_ratio * total_labeled_records)` | `A15` formula over `A14` | approximated labeled episode row count | Approximate count of unknown-labeled rows | same as above | Derived convenience metric, not a stored pipeline artifact. |
| readme | share_of_persona_core_labeled_pct | `_write_readme_sheet()` static guidance row | code literal | denominator guidance | Explain cluster-share denominator | `Percentage over persona_core_labeled_rows. Use this for persona clustering coverage.` | Informational only. |
| readme | share_of_all_labeled_pct | `_write_readme_sheet()` static guidance row | code literal | denominator guidance | Explain all-labeled denominator | `Percentage over all labeled_episode_rows. Use this for whole-workbook context.` | Informational only. |
| readme | row_grain | `_write_readme_sheet()` static guidance row | code literal | grain guidance | Explain `source_diagnostics` row grain | `The entity counted by the row: post, episode, mixed_grain_bridge, or other.` | Informational only. |
| readme | denominator_type_key | `_write_readme_sheet()` static guidance row | code literal | denominator guidance | Explain semantic denominator family label | `The semantic denominator family. Cross-check this against metric_glossary.` | Informational only. |
| readme | Grounding states | `_write_readme_sheet()` static guidance row | code literal | reviewer guidance | Tell reviewer where grounding semantics live | `See persona_summary and cluster_stats for base_promotion_status, grounding_status, and promotion_grounding_status.` | Informational only. |
| readme | Mixed-grain diagnostics | `_write_readme_sheet()` static guidance row | code literal | reviewer guidance | Warn that mixed-grain bridge rows are ratios, not percentages | `source_diagnostics rows with row_grain=mixed_grain_bridge are ratios, not funnel percentages.` | Informational only. |

## Direct Same-Number / Same-Concept Aliases

- `counts.valid_records` and `quality_checks.cleaned_count` are both `len(valid_df)` from `A4`.
- `overview.persona_count`, `quality_checks.promoted_persona_count`, and `readme[Promoted Persona Rows]` are the same promoted persona count from `A11`.
- `counts.labeled_records`, `overview.total_labeled_records`, `quality_checks.labeled_count`, and `readme[All Labeled Rows]` are the same labeled row count from `A7`.
- `overview.persona_core_labeled_records`, `quality_checks.persona_core_labeled_count`, `quality_checks.persona_core_labeled_records`, and `readme[Persona Core Rows]` are the same persona-core labeled row count from `A7`.
- `overview.promoted_persona_example_coverage_pct`, `quality_checks.promoted_persona_example_coverage_pct`, and `quality_failures.promoted_example_coverage_gate.value` all come from the same promoted-persona coverage calculation over `A11 + A12`.

## One Name Used For Multiple Concepts

- `raw_count` / `raw_records` / `total_raw_count` language is currently overloaded.
  In practice the workbook uses one phrase family for two distinct producers:
  filesystem raw JSONL totals from `A1`, and collector audit totals from `A2`.
- `grounding failure` language is currently overloaded.
  `example_grounding_failure_count` means weak or bad *selected example rows* in `A12`, while `example_grounding_status` and `grounding_coverage_status` are driven by promoted-persona coverage over `A11 + A12`, including missing examples.
- `Promoted Persona Rows` in `readme` uses row language for what is actually a promoted persona count.

## Audit Conclusions For The Known Contradictions

- `7739` and `208` both appear as raw-like counts because they are not the same metric. `7739` is a direct count of non-empty raw JSONL lines on disk from `A1`; `208` is the sum of `raw_record_count` rows in `A2`.
- `cleaned_count` is currently the same thing as `valid_records`: both are `len(data/valid/valid_candidates.parquet)`.
- Grounding failure metrics are not numerically inconsistent, but they are semantically mislabeled for review use. `example_grounding_failure_count = 0` only says there are no weak or rejected *selected examples* in `A12`. It does not say every promoted persona is grounded, which is why `promoted_persona_ungrounded_count = 1` and `example_grounding_status = FAIL` are both still correct under the current implementation.