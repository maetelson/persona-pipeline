# Quality Status Policy

## Why this refactor was necessary

The old workbook pipeline let quality interpretation drift because status logic was split across multiple places:

- `build_quality_checks()` computed early summary-like signals
- `finalize_quality_checks()` could reinterpret those signals later
- `overview` had its own summary rendering path

That made it possible for the same metric snapshot to look healthier in `overview` than in `quality_checks`.

## Single source of truth

There is now one authoritative policy entry point:

- [`evaluate_quality_status()`](/Users/hands/OneDrive/Desktop/persona/src/analysis/quality_status.py)

The flow is now:

1. `build_quality_metrics()` computes raw metrics only
2. `evaluate_quality_status()` evaluates axis status, reason keys, and composite status
3. `finalize_quality_checks()` only flattens the evaluated result for workbook rendering
4. `overview` and `quality_checks` both render from that same flattened evaluated result

`finalize_quality_checks()` is formatting-only and must not recalculate thresholds or severities.

## Raw metrics used by policy

The evaluator consumes these raw metrics:

- `persona_core_unknown_ratio`
- `overall_unknown_ratio`
- `persona_core_coverage_of_all_labeled_pct`
- `effective_labeled_source_count`
- `largest_labeled_source_share_pct`
- `largest_cluster_share_of_core_labeled`
- `promoted_persona_example_coverage_pct`
- `example_grounding_failure_count`
- `source_failures`
- `promoted_personas_missing_examples`

These are metric inputs only. They do not carry workbook status labels.

## Axis policy

The threshold and severity rules live in one place only:

- [`QUALITY_STATUS_POLICY`](/Users/hands/OneDrive/Desktop/persona/src/analysis/quality_status.py)

Current axis rules:

- `core_unknown`
  - metric: `persona_core_unknown_ratio`
  - `WARN` at `>= 0.15`
  - `FAIL` at `>= 0.30`
- `overall_unknown`
  - metric: `overall_unknown_ratio`
  - `WARN` at `>= 0.25`
  - `FAIL` at `>= 0.40`
- `core_coverage`
  - metric: `persona_core_coverage_of_all_labeled_pct`
  - `WARN` at `< 75.0`
  - `FAIL` at `< 60.0`
- `effective_source_diversity`
  - metric: `effective_labeled_source_count`
  - `FAIL` at `< 4.0`
  - explicit threshold phrase: `FAIL at < 4.0`
  - workbook-test reference: FAIL at `< 4.0`
  - reason: `effective_source_diversity_low`
  - note: this is intentionally a fail-level axis, not a warning-level axis
- `source_concentration`
  - metric: `largest_labeled_source_share_pct`
  - `WARN` at `>= 50.0`
  - `FAIL` at `>= 70.0`
- `largest_cluster_dominance`
  - metric: `largest_cluster_share_of_core_labeled`
  - `WARN` at `>= 55.0`
  - `FAIL` at `>= 70.0`
- `grounding_coverage`
  - metric: `promoted_persona_example_coverage_pct`
  - `WARN` at `< 100.0`
  - `FAIL` at `< 80.0`

Additional reason keys can be appended without changing the numeric threshold outcome:

- `raw_covered_sources_missing_labels`
- `selected_example_grounding_weak`
- `promoted_persona_examples_missing`

## Why low effective source diversity is a failure

The source-diversity severity change was intentional and is now policy-backed.

When `effective_labeled_source_count < 4.0`, the workbook is not just slightly noisy. It means the labeled evidence base is too narrow for the workbook to present a stable top-line summary without a strong caveat. Under the centralized policy, that condition is therefore a `FAIL` axis:

- axis: `effective_source_diversity`
- reason key: `effective_source_diversity_low`
- implementation: [`QUALITY_STATUS_POLICY`](/Users/hands/OneDrive/Desktop/persona/src/analysis/quality_status.py)

Any test expectation update to treat this as `FAIL` is only valid because this rule is now explicitly defined here and consumed by both `overview` and `quality_checks`.

## Grouped and composite statuses

Workbook-facing grouped statuses are composed from axis results:

- `core_clustering_status`
  - axes: `core_unknown`, `core_coverage`, `largest_cluster_dominance`
- `source_diversity_status`
  - axes: `effective_source_diversity`, `source_concentration`
- `example_grounding_status`
  - axes: `grounding_coverage`

Composite rule:

- `FAIL` if any axis is `FAIL`
- `WARN` if no axis is `FAIL` and at least one axis is `WARN`
- `OK` otherwise

Workbook `quality_flag` is derived only from `overall_status`:

- `FAIL -> UNSTABLE`
- `WARN -> EXPLORATORY`
- `OK -> OK`

## Rendering rules

These files render the centralized result and must not reinterpret policy:

- [`finalize_quality_checks()`](/Users/hands/OneDrive/Desktop/persona/src/analysis/diagnostics.py)
- [`build_quality_checks_df()`](/Users/hands/OneDrive/Desktop/persona/src/analysis/summary.py)
- [`_build_final_overview_df()`](/Users/hands/OneDrive/Desktop/persona/src/analysis/stage_service.py)

Rendering may format:

- row shape
- threshold display text
- denominator text
- notes/reason display

Rendering must not:

- recompute statuses
- apply alternate thresholds
- suppress worse axis results to keep the top line clean
- downgrade `FAIL` to `WARN` or `OK`

## Test policy

If a test expectation changes because severity changed, that change is only valid when:

1. the rule is defined in `QUALITY_STATUS_POLICY`
2. this document describes the rule
3. both `overview` and `quality_checks` consume the same evaluator result
4. the test asserts policy semantics, not just a string snapshot
