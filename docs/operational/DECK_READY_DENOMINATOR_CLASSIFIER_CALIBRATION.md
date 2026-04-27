# Deck-Ready Denominator Classifier Calibration

## Scope

This pass recalibrates the diagnostics-only row classifier used for deck-ready
denominator eligibility review. It does not run denominator ablation, does not
compute an adjusted coverage metric, and does not change readiness,
persona-promotion, or workbook semantics.

## What Changed

- Expanded the classifier text blob to use all available episode and labeled-row
  context, including `evidence_snippet`, `work_moment`, `tool_env`,
  `segmentation_note`, `labelability_reason`, and code-family fields.
- Added broader business-context signals around reporting cadence, stakeholder
  delivery, reconciliation, export, metric review, and manual reporting burden.
- Added broader technical/support noise signals around auth, API, deployment,
  connector, syntax, formula, and runtime troubleshooting.
- Added source-specific noise patterns for Adobe Analytics, Domo, Google
  Developer Forums, GitHub Discussions, and Stack Overflow.
- Tightened ambiguity routing so mixed business + technical rows are classified
  as `ambiguous_review_bucket` unless technical/support evidence clearly
  dominates.
- Removed the old near-hard fallback where `labelability_status = low_signal`
  effectively collapsed the non-core slice into `generic_low_signal`.

## Before / After

Before calibration:

- `generic_low_signal = 3229`
- `ambiguous_review_bucket = 0`
- `denominator_eligible_business_non_core = 0`
- explicit technical/support noise categories combined = `0`
- denominator-eligible rows = `9445`
- denominator-ineligible rows = `3229`

After calibration:

- `generic_low_signal = 0`
- `ambiguous_review_bucket = 1082`
- `denominator_eligible_business_non_core = 223`
- explicit technical/support noise categories combined = `1924`
- denominator-eligible rows = `10750`
- denominator-ineligible rows = `1924`

## Important Guardrail

These results are diagnostics-only.

- Denominator ablation has not run.
- Adjusted coverage is still not official.
- `persona_core_coverage_of_all_labeled_pct` is unchanged.
- Readiness state is unchanged.
- Persona counts are unchanged.

## Next Safe Step

Another live precision review is still required before any denominator ablation
or adjusted denominator metric is considered. The current calibration is much
more useful for auditability, but it still needs measured precision validation
against reviewed samples before it can influence any readiness interpretation.
