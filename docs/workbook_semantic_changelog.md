# Workbook Semantic Changelog

## Before -> After

### Metrics renamed
- `example_grounding_failure_count` -> `selected_example_grounding_issue_count`
  - Before: ambiguous failure count that looked persona-level but was actually example-level.
  - After: explicit example-row grounding issue count.
- Workbook-facing helper labels were expanded from terse metric keys to explicit reviewer labels such as:
  - `raw_record_rows` -> `Raw record row count (JSONL lines, not source count)`
  - `promotion_visibility_persona_count` -> `Promotion-visibility persona count (review-visible promoted personas)`
  - `final_usable_persona_count` -> `Final usable persona count (grounded promoted personas only)`

### Metrics removed
- Ambiguous workbook metric `persona_count` was removed from workbook semantics.
- Legacy stage aliases such as `raw_records` and other pre-canonical stage labels are rejected by validation.
- Generic source diagnostic placeholder `labeled_output_present` is no longer the default outcome when a stronger source-specific bottleneck exists.

### Denominator and grain semantics changed
- Stage metrics now use one canonical stage dictionary across `counts`, `overview`, and `quality_checks`.
- `source_diagnostics` no longer implies one linear funnel across mixed entities.
  - Same-grain post and episode percentages remain bounded.
  - Cross-grain bridge metrics are explicit `*_per_*` ratios, not survival rates.
- `effective_labeled_source_count` is now documented as a source-count metric, not a row-count metric.
- Workbook headers in `counts` and `source_distribution` now explicitly say `row_count` and `*_rows_for_source` to avoid source-vs-row confusion.

### Promotion and usable persona policy changes
- Promoted candidate personas, review-visible promoted personas, final usable personas, and deck-ready personas are now separate concepts.
- Review-visible promoted personas can include weakly grounded or ungrounded promoted personas.
- Final usable and deck-ready persona counts now exclude weakly grounded and ungrounded review-only personas.

### Grounding failure metric fixes
- Persona-level grounding failure is now tracked explicitly as `promoted_persona_grounding_failure_count`.
- Example-level and persona-level grounding issues are no longer conflated.
- `quality_checks` and `quality_failures` now use the same grounding story:
  - persona-level grounding failures can fail while example-level issue count remains zero
  - promoted persona coverage failure is shown separately from selected-example issue counts

### Source diagnostics rule changes
- `source_diagnostics` now ranks source bottlenecks using explicit rules:
  - `low_valid_post_retention`
  - `low_prefilter_retention`
  - `low_episode_yield`
  - `low_labelable_episode_ratio`
  - `zero_promoted_persona_contribution`
  - `concentration_risk_contribution`
  - `weak_diversity_contribution`
- `recommended_seed_set` is only populated when policy supports a real prefilter intervention and a local source seed file exists.
- The sheet now explains why a source is weak or risky instead of merely confirming that output exists.
