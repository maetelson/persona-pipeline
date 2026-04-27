# Deck-Ready Denominator Eligibility Policy Spec

## Policy Problem

The current workbook reports `persona_core_coverage_of_all_labeled_pct` using
`all_labeled_rows` as the denominator. That denominator is simple and auditable,
but it is also broad: labeled non-core rows currently mix business-relevant
non-core evidence with technical, support, debug, and other low-value rows that
do not safely represent deck-ready persona evidence.

Recent recovery work showed that many non-core labeled rows are not safe
persona-core evidence. Those rows often still belong in raw and labeled
diagnostics, but they should not automatically define the denominator used for
future deck-ready evidence sufficiency decisions.

At the same time, excluding rows without a formal policy would create a
cherry-picking risk. Coverage could appear to improve simply because
inconvenient denominator rows were hidden, especially from weak sources or
technical surfaces. The goal of this policy is therefore not to hide data. The
goal is to distinguish row-level deck-ready denominator eligibility from
retained diagnostic visibility, with explicit categories and an auditable trail.

## Denominator Eligibility Taxonomy

The row-level denominator taxonomy for future audited implementation is:

- `persona_core_evidence`
- `denominator_eligible_business_non_core`
- `technical_support_debug_noise`
- `source_specific_support_noise`
- `setup_auth_permission_noise`
- `api_sdk_debug_noise`
- `server_deploy_config_noise`
- `syntax_formula_debug_noise`
- `vendor_announcement_or_feature_request_only`
- `career_training_certification_noise`
- `generic_low_signal`
- `ambiguous_review_bucket`

Defaults:

- `persona_core_evidence`: included
- `denominator_eligible_business_non_core`: included
- all explicit noise categories: excluded
- `ambiguous_review_bucket`: included by default, but flagged and separately counted

## Category Definitions

### `persona_core_evidence`

- Definition: rows already accepted as persona-core evidence under the current
  pipeline and workbook policy.
- `deck_ready_denominator_eligible = True`
- Example signals: clear recurring reporting burden, reliable persona mapping,
  strong workflow context, accepted grounding.
- Non-example signals: non-core rows, thin snippets, unresolved technical noise.
- Default handling: always retained in the adjusted denominator.
- Risk notes: any later implementation must guarantee that persona-core rows can
  never be excluded by denominator eligibility logic.

### `denominator_eligible_business_non_core`

- Definition: non-core rows that still show clear business workflow pain
  relevant to reporting, dashboards, metrics, KPI explanation, stakeholder
  decision-making, analysis handoff, reconciliation, report delivery, manual
  reporting burden, or trust and validation of numbers.
- `deck_ready_denominator_eligible = True`
- Example signals: recurring report assembly pain, dashboard-to-spreadsheet
  fallback, stakeholder-facing metric mismatch, analysis handoff friction,
  validation workload after dashboard use.
- Non-example signals: rows focused mainly on syntax, setup, API behavior, or
  generic tool support without business context.
- Default handling: retained in the adjusted denominator even if not safe for
  persona-core promotion.
- Risk notes: this bucket prevents denominator ablation from unfairly rewarding
  only already-promoted evidence.

### `technical_support_debug_noise`

- Definition: rows whose primary value is technical troubleshooting, support,
  or implementation debugging without sufficient business workflow context.
- `deck_ready_denominator_eligible = False`
- Example signals: stack traces, code-level debugging, implementation-specific
  failures, technical troubleshooting threads with no reporting use case.
- Non-example signals: technical detail embedded in a broader reporting or
  stakeholder workflow pain story.
- Default handling: excluded from the adjusted denominator, but retained in
  diagnostics.
- Risk notes: over-classifying rows here could hide business-adjacent evidence,
  so confidence and explanation fields are required.

### `source_specific_support_noise`

- Definition: rows dominated by product support, vendor forum guidance,
  troubleshooting etiquette, or source-native support flows rather than user
  workflow pain.
- `deck_ready_denominator_eligible = False`
- Example signals: support-ticket style threads, admin guidance, source-native
  help desk exchanges, forum escalation flows.
- Non-example signals: vendor-specific business pain that still clearly
  describes reporting burden or trust failure.
- Default handling: excluded from the adjusted denominator, preserved in source
  diagnostics.
- Risk notes: this category must not be used to suppress whole sources; it is a
  row-level classification only.

### `setup_auth_permission_noise`

- Definition: rows focused on setup, installation, permissions, access control,
  or authentication without business reporting context.
- `deck_ready_denominator_eligible = False`
- Example signals: login failures, permission grants, connector access setup,
  token issues, account provisioning.
- Non-example signals: access issues described as part of recurring reporting
  delay or stakeholder delivery failure, if the business context is dominant.
- Default handling: excluded from the adjusted denominator.
- Risk notes: setup friction can matter operationally, but it should not define
  deck-ready persona evidence sufficiency on its own.

### `api_sdk_debug_noise`

- Definition: rows centered on API, SDK, webhook, or library integration
  debugging without enough business workflow context.
- `deck_ready_denominator_eligible = False`
- Example signals: request formatting bugs, SDK method usage, auth headers,
  pagination handling, library exceptions.
- Non-example signals: API use discussed mainly as part of reporting output
  construction or analytics handoff burden.
- Default handling: excluded from the adjusted denominator.
- Risk notes: many developer-heavy sources will generate this category at high
  volume, so exclusion counts must be source-audited.

### `server_deploy_config_noise`

- Definition: rows dominated by deployment, infrastructure, hosting, runtime,
  environment, or configuration troubleshooting without business reporting
  relevance.
- `deck_ready_denominator_eligible = False`
- Example signals: server startup failure, deployment rollback, env var
  problems, container/runtime config issues.
- Non-example signals: delivery or rendering issues that directly affect report
  output fidelity for end users.
- Default handling: excluded from the adjusted denominator.
- Risk notes: deployment noise can swamp technical sources; exclusion must stay
  explicit and visible.

### `syntax_formula_debug_noise`

- Definition: rows focused mainly on syntax, formula correctness, expression
  debugging, or code-level fixes without clear business context.
- `deck_ready_denominator_eligible = False`
- Example signals: DAX syntax error, SQL typo, formula parser issue, regular
  expression correction, expression-scoping bug with no workflow pain context.
- Non-example signals: measure or total-mismatch rows where the dominant issue
  is business trust, reconciliation, or stakeholder-facing interpretation.
- Default handling: excluded from the adjusted denominator.
- Risk notes: this category needs extra care because some formula-level rows are
  business-relevant; ambiguity handling must catch those mixed cases.

### `vendor_announcement_or_feature_request_only`

- Definition: rows that are mainly announcements, roadmap notes, release posts,
  or feature requests without concrete user pain.
- `deck_ready_denominator_eligible = False`
- Example signals: release announcements, wishlist posts, broad feature asks
  with no workflow evidence, changelog discussion.
- Non-example signals: feature-limitation rows that clearly describe recurring
  reporting burden or blocked workflow outcomes.
- Default handling: excluded from the adjusted denominator.
- Risk notes: feature requests with real workflow pain should be classified by
  the pain, not suppressed by format alone.

### `career_training_certification_noise`

- Definition: rows focused on career development, interviews, salary, resume
  help, certifications, training, or tutorial-style learning rather than live
  workflow evidence.
- `deck_ready_denominator_eligible = False`
- Example signals: certification advice, job search threads, beginner course
  guidance, interview prep, resume review.
- Non-example signals: practitioners describing current reporting pain in a work
  context, even if they are early career.
- Default handling: excluded from the adjusted denominator.
- Risk notes: this category should be high-confidence only, because career
  discussions sometimes contain real workflow evidence.

### `generic_low_signal`

- Definition: rows too vague, too thin, or too context-poor to safely support
  deck-ready denominator reasoning.
- `deck_ready_denominator_eligible = False`
- Example signals: generic complaints, one-line fragments, context-free tool
  chatter, non-specific “how do I do this” text with no business framing.
- Non-example signals: short rows that still clearly state a business reporting
  problem or trust failure.
- Default handling: excluded from the adjusted denominator.
- Risk notes: this category is useful but dangerous; it must not become a
  dumping ground for inconvenient rows.

### `ambiguous_review_bucket`

- Definition: rows with mixed business and technical signals where exclusion
  confidence is not high enough to classify them as clear noise.
- `deck_ready_denominator_eligible = True`
- Example signals: formula or implementation details embedded in a stakeholder
  reporting problem, technical-looking rows with real business reconciliation
  context, mixed admin and workflow issues.
- Non-example signals: clearly technical-only troubleshooting or clearly
  business-relevant non-core evidence.
- Default handling: retained in the adjusted denominator and reported
  separately.
- Risk notes: this bucket is the main conservative safeguard against
  denominator cherry-picking.

## Eligibility Rules

A row should be `deck_ready_denominator_eligible = True` when:

- it is already `persona_core_evidence`, or
- it is non-core but clearly business-relevant and grounded in workflow pain
  around reporting, dashboards, metrics, KPI explanation, stakeholder
  decision-making, analysis handoff, reconciliation, report delivery, manual
  reporting burden, or trust and validation of numbers.

A row should be `deck_ready_denominator_eligible = False` when it is primarily:

- syntax, debug, support, or setup noise
- API, SDK, deployment, auth, or permission troubleshooting without business
  workflow context
- vendor announcement or feature request without concrete user pain
- career, training, certification, or tutorial content
- generic low-signal text
- context-poor text that cannot support persona reasoning

## Ambiguity Handling

Ambiguous rows are included by default, not excluded by default.

The `ambiguous_review_bucket` is reserved for rows with mixed business and
technical signals where exclusion confidence is not high enough. Those rows
count in the adjusted denominator until later manual or validated rule review
says otherwise.

Ambiguity must be reported as diagnostics, not hidden. Future outputs should
show ambiguous counts explicitly so denominator pressure remains transparent even
when the row is not confidently classed as clean business evidence.

## Source-Tier Interaction

Row-level denominator eligibility is driven first by row evidence quality, not
by source tier alone.

- `core_representative_source`
  - rows may be denominator-eligible or denominator-ineligible depending on
    row-level evidence quality
  - high source quality does not override row-level technical noise
- `supporting_validation_source`
  - rows can be denominator-eligible if they contain business-relevant non-core
    evidence
  - source-tier policy still prevents them from silently becoming claim anchors
- `exploratory_edge_source`
  - rows can be denominator-eligible only when the row itself is business
    relevant
  - they remain explicitly visible as exploratory in diagnostics
- `excluded_from_deck_ready_core`
  - rows remain visible in raw and labeled diagnostics
  - rows should default to denominator-ineligible unless a later explicit
    exception policy is written

Source tier modifies confidence, reporting, and diagnostics interpretation. It
must not silently hide rows. Row-level technical noise overrides source tier. A
technical-support row from a high-quality source is still
denominator-ineligible.

## Anti-Cherry-Picking Guardrails

Any future implementation of this policy must preserve these safeguards:

- no raw deletion
- no labeled-row deletion
- excluded rows remain present in diagnostics outputs
- every excluded row has a controlled exclusion category and human-readable
  reason
- exclusion counts are reported by source, source tier, and category
- ambiguous rows are reported separately
- source-specific exclusions cannot be silently introduced to improve one metric
- `final_usable_persona_count`, `production_ready_persona_count`,
  `review_ready_persona_count`, and
  `deck_ready_claim_eligible_persona_count` do not change from denominator
  eligibility alone
- original and adjusted coverage metrics coexist
- denominator changes are never presented as deck-ready achievement without
  diagnostic transparency

These guardrails exist to prevent:

- deleting inconvenient rows
- excluding rows only because they hurt metrics
- excluding rows from only one source to improve coverage
- hiding weak-source evidence
- silently changing denominator without row-level reasons
- making the workbook look deck-ready without preserving evidence transparency

## Required Future Fields

### Row-level fields

- `deck_ready_denominator_eligible`
- `denominator_eligibility_category`
- `denominator_exclusion_reason`
- `technical_noise_confidence`
- `business_context_signal_count`
- `technical_noise_signal_count`
- `source_specific_noise_signal_count`
- `ambiguity_flag`

### Aggregate fields

- `deck_ready_denominator_row_count`
- `deck_ready_denominator_excluded_row_count`
- `denominator_exclusion_count_by_category`
- `denominator_exclusion_count_by_source`
- `denominator_exclusion_count_by_source_tier`
- `adjusted_persona_core_coverage_pct`
- `original_persona_core_coverage_pct`

## Metric Reporting Rules

The current `persona_core_coverage_of_all_labeled_pct` remains the legacy
audited metric and must stay visible.

Any future denominator implementation must add a second audited metric, named
consistently as `adjusted_deck_ready_denominator_core_coverage_pct` or an
equivalent explicitly adjusted name.

The adjusted metric must never silently replace the original metric. Readiness
artifacts should show both metrics side-by-side and explain denominator
differences explicitly.

## Implementation Phases

Rollout must follow this sequence:

1. Phase 1: add row-level classification fields in diagnostics only
2. Phase 2: run denominator ablation with no readiness semantic change
3. Phase 3: validate sample precision
4. Phase 4: add adjusted denominator metric alongside the original metric
5. Phase 5: update workbook or README presentation only if accepted

This sequence is mandatory because denominator policy is audit-sensitive and
must not be allowed to change release interpretation before field-level evidence
quality is proven.

## Test Plan

Future implementation must add tests that verify:

- persona-core rows are always denominator-eligible
- pure syntax and debug rows are excluded
- setup, auth, API, and deploy noise is excluded
- business reporting pain is retained
- stakeholder-facing metric mismatch is retained
- ambiguous rows are handled conservatively
- exclusion reason is populated whenever a row is excluded
- excluded rows still appear in diagnostics
- raw and labeled data is not deleted
- `final_usable_persona_count` does not change
- `production_ready_persona_count` does not change
- `review_ready_persona_count` does not change
- `deck_ready_claim_eligible_persona_count` does not change
- both original and adjusted coverage metrics remain visible

## Risks Before Implementation

- false-positive exclusions can artificially improve coverage
- source-specific exclusion drift can hide weak evidence
- ambiguous rows are the main pressure point and must default conservative
- denominator ablation should not proceed until fields, diagnostics, and tests
  exist

## Relationship To Current Policy

This policy does not replace current source-tier policy, weak-source denominator
split policy, deck-ready claim policy, review-ready or production-ready
semantics, persona_05 preservation, or final release wording. It fills one
missing layer only: row-level deck-ready denominator eligibility.

Until implementation is completed and audited, the workbook should continue to
treat `persona_core_coverage_of_all_labeled_pct` as the official coverage
metric and should continue to preserve all current diagnostic visibility.
