# Source Diagnostics Metric Contract

This document is the source-facing contract for the workbook tabs that summarize per-source health.

## Source of truth path

| Output | Builder | File | Notes |
| --- | --- | --- | --- |
| `source_stage_counts_df` | `build_source_stage_counts` | `src/analysis/diagnostics.py` | Canonical per-source aggregate table used by diagnostics, quality checks, and survival summaries. |
| `source_distribution_df` | `build_final_source_distribution` | `src/analysis/summary.py` | Wide by-source distribution sheet used for labeled-share context, not diagnostics. |
| `source_diagnostics_df` | `build_source_diagnostics` | `src/analysis/diagnostics.py` | Row-based source diagnostics sheet with explicit metric vs diagnostic rows. |
| Workbook assembly | `build_deterministic_analysis_outputs` | `src/analysis/stage_service.py` | Calls the source builders and passes frames into workbook assembly/export. |
| Workbook export | `export_workbook_from_frames` | `src/exporters/xlsx_exporter.py` | Applies display headers and Excel formatting only. No metric math happens here. |

## Intermediate per-source aggregate contract

These fields live in `source_stage_counts_df` and are the only source-of-truth inputs for `source_diagnostics`.

| metric_name | source function/file | entity grain | numerator | denominator | bounded vs unbounded | intended interpretation |
| --- | --- | --- | --- | --- | --- | --- |
| `source` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | source | n/a | n/a | categorical | Canonical source key after `canonical_source_name`. |
| `raw_record_count` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | raw record row | raw JSONL row count | none | unbounded count | Ingest volume from `data/raw/{source}/*.jsonl`. |
| `normalized_post_count` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | normalized post row | normalized rows for source | none | unbounded count | Post-level volume after normalizers. |
| `valid_post_count` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | normalized post row | valid rows for source | none | unbounded count | Post rows surviving invalid filtering. |
| `prefiltered_valid_post_count` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | valid post row | retained prefilter rows for source | none | unbounded count | Valid post rows kept for episode building. |
| `episode_count` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | episode row | episode rows for source | none | unbounded count | Episode output volume. One post can yield zero or more episodes. |
| `labeled_episode_count` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | episode row | labeled episode rows for source | none | unbounded count | Episodes present in `labeled_episodes.parquet`. |
| `labelable_episode_count` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | labeled episode row | labeled rows with `labelable` or `borderline` status | none | unbounded count | Labelability subset of labeled episodes. Must be `<= labeled_episode_count`. |
| `promoted_persona_episode_count` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | promoted persona assignment row | promoted/review-visible persona assignments for source | none | unbounded count | Source contribution into promoted persona evidence. |
| `effective_diversity_contribution` | `build_source_stage_counts` / `src/analysis/diagnostics.py` | source contribution score | `min(1.0, labeled_episode_count / 5.0)` | implicit labeled episode volume floor | bounded `0-1` ratio | Fractional source-diversity contribution used for weak-source detection. |
| `dominant_invalid_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | most frequent `invalid_reason` | none | categorical | Context for low valid retention. Not a funnel metric. |
| `dominant_prefilter_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | most frequent `prefilter_reason` | none | categorical | Context for low prefilter retention. Not a funnel metric. |
| `valid_retention_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | post-funnel diagnosis | normalized-post retention threshold | categorical | Whether normalized posts are converting into valid posts. |
| `prefilter_retention_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | post-funnel diagnosis | valid-post retention threshold | categorical | Whether valid posts are surviving relevance prefiltering. |
| `episode_yield_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | cross-grain diagnosis | episode yield threshold | categorical | Whether retained prefiltered posts are producing enough episodes. |
| `labelable_coverage_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | episode-funnel diagnosis | labelability threshold | categorical | Whether labeled episodes remain labelable/borderline often enough. |
| `grounding_contribution_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | promoted persona contribution diagnosis | promoted contribution expectation | categorical | Whether the source contributes grounded/promoted persona evidence. |
| `concentration_risk_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | labeled or promoted source share diagnosis | concentration threshold | categorical | Whether one source is becoming too dominant. |
| `diversity_contribution_reason` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | effective diversity diagnosis | diversity floor | categorical | Whether the source contributes meaningful diversity. |
| `failure_reason_top` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | highest-ranked active issue | ranked reason order | categorical | Top actionable reason used for quick triage. |
| `failure_level` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source diagnostic | severity for `failure_reason_top` | none | categorical | `failure`, `warning`, or `pass`. |
| `recommended_seed_set` | `_diagnose_source_row` / `src/analysis/diagnostics.py` | source intervention | local seed list payload | none | categorical text | Only populated when top issue is `low_prefilter_retention:*` and a local seed file exists. |
| `root_cause_category` | `_build_source_remediation` / `src/analysis/diagnostics.py` | source remediation | normalized remediation category | none | categorical | Root-cause normalization used to prevent repeated generic TODO output. |
| `evidence_to_inspect` | `_build_source_remediation` / `src/analysis/diagnostics.py` | source intervention | canonical artifact path(s) | none | categorical text | Primary local artifact to inspect before changing rules. |
| `likely_false_negative_pattern` | `_build_source_remediation` / `src/analysis/diagnostics.py` | source remediation | source-aware pattern summary | none | categorical text | Concrete hypothesis for what kind of rows are being dropped or under-segmented. |
| `recommended_config_change` | `_build_source_remediation` / `src/analysis/diagnostics.py` | source intervention | next config/code path to edit | none | categorical text | Concrete remediation target, not just a review instruction. |
| `required_regression_check` | `_build_source_remediation` / `src/analysis/diagnostics.py` | source intervention | test or rerun assertion | none | categorical text | Regression work required after changing the source-specific logic. |
| `owner_action_type` | `_build_source_remediation` / `src/analysis/diagnostics.py` | source remediation | remediation ownership class | none | categorical | Groups work into invalid-filter, prefilter, episode-builder, time-window, or monitor-only actions. |
| `can_auto_tune` | `_build_source_remediation` / `src/analysis/diagnostics.py` | source remediation | auto-tune eligibility | none | boolean | True only when source-aware tuning can be attempted without manual sample inspection first. |
| `must_manual_review` | `_build_source_remediation` / `src/analysis/diagnostics.py` | source remediation | manual review requirement | none | boolean | True when sample inspection is required before any rule change. |

## Source distribution contract

`source_distribution` is a wide context sheet. It is not a diagnostic funnel and should not be used to infer source bottlenecks by itself.

| metric_name | source function/file | entity grain | numerator | denominator | bounded vs unbounded | intended interpretation |
| --- | --- | --- | --- | --- | --- | --- |
| `source` | `build_final_source_distribution` / `src/analysis/summary.py` | source | n/a | n/a | categorical | Canonical source key. |
| `raw_count` | `build_final_source_distribution` / `src/analysis/summary.py` | raw record row | raw JSONL row count | none | unbounded count | Same meaning as `raw_record_count`, displayed in the distribution sheet. |
| `normalized_count` | `build_final_source_distribution` / `src/analysis/summary.py` | normalized post row | normalized rows for source | none | unbounded count | Source volume after normalization. |
| `valid_count` | `build_final_source_distribution` / `src/analysis/summary.py` | normalized post row | valid rows for source | none | unbounded count | Source volume after invalid filtering. |
| `prefiltered_valid_count` | `build_final_source_distribution` / `src/analysis/summary.py` | valid post row | retained prefilter rows for source | none | unbounded count | Source volume passed into episode building. |
| `episode_count` | `build_final_source_distribution` / `src/analysis/summary.py` | episode row | episode rows for source | none | unbounded count | Episode output volume. |
| `labeled_count` | `build_final_source_distribution` / `src/analysis/summary.py` | episode row | labeled episode rows for source | none | unbounded count | Labeled evidence volume for the source. |
| `share_of_labeled` | `build_final_source_distribution` / `src/analysis/summary.py` | episode row share | `labeled_count` | total labeled episode rows | bounded `0-100` pct | Source share of the full labeled evidence base. |
| `denominator_type` | `build_final_source_distribution` / `src/analysis/summary.py` | workbook metadata | `labeled_episode_rows` | none | categorical | Declares the share denominator family. |
| `denominator_value` | `build_final_source_distribution` / `src/analysis/summary.py` | episode row | total labeled episode rows | none | unbounded count | Explicit denominator for `share_of_labeled`. |

## Source diagnostics contract

`source_diagnostics` is row-based and split into explicit sections. `row_kind=metric` rows carry funnel or bridge math. `row_kind=diagnostic` rows carry reason strings and diagnostic severity. They must never be blended.

### Numeric metric rows

| metric_name | source function/file | entity grain | numerator | denominator | bounded vs unbounded | intended interpretation |
| --- | --- | --- | --- | --- | --- | --- |
| `raw_record_count` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | raw record row | raw JSONL rows | none | unbounded count | Raw ingest volume for the source. |
| `normalized_post_count` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | normalized post row | normalized posts | none | unbounded count | Post-funnel starting volume. |
| `valid_post_count` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | normalized post row | valid posts | none | unbounded count | Post rows surviving invalid filtering. |
| `prefiltered_valid_post_count` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | valid post row | retained prefilter rows | none | unbounded count | Post rows surviving relevance prefiltering. |
| `valid_posts_per_normalized_post_pct` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | post | `valid_post_count` | `normalized_post_count` | bounded `0-100` pct | Same-grain post-funnel retention. |
| `prefiltered_valid_posts_per_valid_post_pct` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | post | `prefiltered_valid_post_count` | `valid_post_count` | bounded `0-100` pct | Same-grain prefilter retention. |
| `episode_count` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | episode row | episode rows | none | unbounded count | Episode-funnel starting volume. |
| `labeled_episode_count` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | episode row | labeled episode rows | none | unbounded count | Episode rows reaching labeled output. |
| `labelable_episode_count` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | labeled episode row | labelable or borderline labeled rows | none | unbounded count | Labelability subset of labeled episodes. |
| `labeled_episodes_per_episode_pct` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | episode | `labeled_episode_count` | `episode_count` | bounded `0-100` pct | Same-grain episode retention. |
| `labelable_episodes_per_labeled_episode_pct` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | episode | `labelable_episode_count` | `labeled_episode_count` | bounded `0-100` pct | Same-grain labelability coverage. |
| `episodes_per_prefiltered_valid_post` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | mixed-grain bridge | `episode_count` | `prefiltered_valid_post_count` | unbounded ratio | Cross-grain bridge from retained posts to episodes. Can exceed `1.0`. |
| `labeled_episodes_per_prefiltered_valid_post` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | mixed-grain bridge | `labeled_episode_count` | `prefiltered_valid_post_count` | unbounded ratio | Cross-grain bridge from retained posts to labeled episodes. Can exceed `1.0`. |
| `labelable_episodes_per_prefiltered_valid_post` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | mixed-grain bridge | `labelable_episode_count` | `prefiltered_valid_post_count` | unbounded ratio | Cross-grain bridge from retained posts to labelable labeled episodes. Can exceed `1.0`. |
| `effective_diversity_contribution` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source contribution score | `min(1.0, labeled_episode_count / 5.0)` | implicit labeled-volume floor | bounded `0-1` ratio | Fractional diversity contribution, not a funnel ratio. |
| `promoted_persona_episode_count` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | promoted persona assignment row | promoted persona assignments | none | unbounded count | Source contribution to promoted personas. |

### Diagnostic rows

| metric_name | source function/file | entity grain | numerator | denominator | bounded vs unbounded | intended interpretation |
| --- | --- | --- | --- | --- | --- | --- |
| `top_failure_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | ranked top issue string | none | categorical | One per source. Primary bottleneck or `healthy_source_contribution`. |
| `dominant_invalid_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | dominant invalid reason string | none | categorical | Root-cause context for valid retention. |
| `dominant_prefilter_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | dominant prefilter reason string | none | categorical | Root-cause context for prefilter retention. |
| `valid_retention_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | diagnostic reason string | `normalized_post_count` threshold context | categorical | Explicit post-funnel health diagnosis. |
| `prefilter_retention_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | diagnostic reason string | `valid_post_count` threshold context | categorical | Explicit prefilter health diagnosis. |
| `episode_yield_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | diagnostic reason string | `prefiltered_valid_post_count` threshold context | categorical | Explicit bridge/yield diagnosis. |
| `labelable_coverage_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | diagnostic reason string | `labeled_episode_count` threshold context | categorical | Explicit labelability coverage diagnosis. |
| `grounding_contribution_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | diagnostic reason string | `labeled_episode_count` contribution context | categorical | Expresses whether grounding/persona contribution is absent. |
| `concentration_risk_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | diagnostic reason string | labeled/promoted share thresholds | categorical | Expresses whether overconcentration risk exists. |
| `diversity_contribution_reason` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | diagnostic reason string | diversity contribution floor | categorical | Expresses whether the source is a weak diversity contributor. |
| `recommended_action` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source intervention | short action label | none | categorical | Backward-compatible action label derived from the richer remediation model. |
| `false_negative_hint` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source diagnostic | source-aware likely pattern | none | categorical | Short source-specific hint about the current false-negative or under-segmentation risk. |
| `source_specific_next_check` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source intervention | next artifact/inspection target | none | categorical | Short next inspection step to perform before a source-scoped rerun. |
| `root_cause_category` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source remediation | normalized remediation category | none | categorical | Distinguishes symptom-level failure from reusable remediation class. |
| `evidence_to_inspect` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source intervention | canonical artifact path(s) | none | categorical text | Exact artifact to open before editing rules. |
| `likely_false_negative_pattern` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source remediation | source-aware drop pattern | none | categorical text | Durable source-aware description of the likely false-negative pattern. |
| `recommended_config_change` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source intervention | next config/code path | none | categorical text | Concrete file or rule family to modify next. |
| `required_regression_check` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source intervention | required test/rerun gate | none | categorical text | Regression check required after remediation. |
| `owner_action_type` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source remediation | remediation ownership class | none | categorical | Indicates whether the work belongs in invalid filtering, prefiltering, episode building, or monitoring. |
| `can_auto_tune` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source remediation | auto-tune eligibility | none | boolean | True when source-aware tuning can be attempted directly. |
| `must_manual_review` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source remediation | manual review requirement | none | boolean | True when sample inspection is required before tuning. |
| `recommended_seed_intervention` | `build_source_diagnostics` / `src/analysis/diagnostics.py` | source intervention | local seed payload | none | categorical text | Only present when top issue is low prefilter retention and a real seed file exists. |

## Invariants

- Same-grain monotonicity must hold in `source_stage_counts_df`:
  - `normalized_post_count <= raw_record_count`
  - `valid_post_count <= normalized_post_count`
  - `prefiltered_valid_post_count <= valid_post_count`
  - `labeled_episode_count <= episode_count`
  - `labelable_episode_count <= labeled_episode_count`
- Mixed-grain bridge metrics must use `*_per_*` names and must not use `rate`, `share`, or `survival` naming.
- `source_diagnostics` must contain explicit `diagnostic_reasons` rows with exactly one `top_failure_reason` per source.
- Generic placeholders such as `labeled_output_present` are invalid and should fail workbook validation.
- `fix_now` sources must expose either a non-empty `recommended_config_change` or an explicit manual review requirement with a non-empty `evidence_to_inspect`.
- `can_auto_tune=true` is allowed only when the remediation model explicitly marks the source as safe for source-aware tuning without manual evidence review first.
