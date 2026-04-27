# Slice-Aware Source Policy Spec

## Policy Problem

The current workbook source policy operates mostly at the source level. That
works when a source is either broadly representative or broadly weak, but the
recent root-cause audit showed that several weak sources are not failing in one
uniform way.

Some weak sources contain useful evidence-producing slices alongside
technical/support-heavy slices that create debt:

- `google_developer_forums` is a good-source-bad-slice mix
- `adobe_analytics_community` is a good-source-bad-slice mix with
  source-specific language and codebook-boundary issues
- `domo_community_forum` is currently debt-heavy with little strong
  evidence-producing surface
- `klaviyo_community` is mostly low-value / bad-fit and is already excluded from
  deck-ready core

Past whole-source actions were not enough:

- deleting weak sources regressed source balance
- keeping every slice equally counted preserves weak-source debt
- denominator policy solved a coverage-design problem, but not the
  source-balance and weak-source-debt structure

Therefore a slice-aware source policy is needed. The goal is not to delete
sources, remove rows, or hide weak evidence. The goal is to classify
source-internal slices in a way that preserves diagnostics visibility while
making future balance and weak-source interpretation more methodologically
coherent.

## Source-Slice Taxonomy

The source-slice taxonomy for future audited implementation is:

- `evidence_producing_slice`
- `mixed_evidence_slice`
- `debt_producing_slice`
- `exploratory_slice`
- `diagnostics_only_slice`
- `insufficient_evidence_slice`

## Category Definitions

### `evidence_producing_slice`

- Definition: a source-internal slice that contributes meaningful persona-core
  evidence or denominator-eligible business evidence, with technical/support
  noise not dominating the slice.
- Can contribute to deck-ready source balance: yes
- Can contribute to weak-source debt: conditional
- Can anchor deck-ready claims: conditional
- Remains visible in diagnostics: yes
- Default handling: keep visible and eligible for future quality-adjusted
  source-balance analysis.
- Risk notes: must not be promoted only because it improves metrics; it still
  has to clear source-tier and claim-anchor policy.

### `mixed_evidence_slice`

- Definition: a slice that contains meaningful business evidence but also a
  substantial technical/support/noise component that makes its deck-ready role
  ambiguous.
- Can contribute to deck-ready source balance: conditional
- Can contribute to weak-source debt: conditional
- Can anchor deck-ready claims: no
- Remains visible in diagnostics: yes
- Default handling: keep visible, do not treat as automatically quarantined or
  automatically healthy.
- Risk notes: this is the main boundary bucket where premature quarantine would
  look like cherry-picking.

### `debt_producing_slice`

- Definition: a slice where technical/support/debug/setup noise dominates,
  persona-core contribution is very low, and excluded or low-value rows inflate
  source volume without enough usable evidence.
- Can contribute to deck-ready source balance: no
- Can contribute to weak-source debt: yes
- Can anchor deck-ready claims: no
- Remains visible in diagnostics: yes
- Default handling: keep visible and count as debt in future secondary
  slice-aware diagnostics unless later policy changes explicitly approve a
  quarantine path.
- Risk notes: debt-producing slices cannot simply disappear; they must remain
  visible by source and slice.

### `exploratory_slice`

- Definition: a slice that may contain some useful signal, but is too small,
  too unstable, too product-specific, or too weakly evidenced to influence
  deck-ready balance interpretation safely.
- Can contribute to deck-ready source balance: conditional
- Can contribute to weak-source debt: conditional
- Can anchor deck-ready claims: no
- Remains visible in diagnostics: yes
- Default handling: keep visible as exploratory-only signal.
- Risk notes: this bucket is not a shortcut to ignore a weak slice; it is a
  diagnostics bucket for plausible but not yet reliable evidence.

### `diagnostics_only_slice`

- Definition: a slice that should remain visible for reproducibility and source
  auditing, but should not count toward future deck-ready balance interpretation
  or weak-source relief.
- Can contribute to deck-ready source balance: no
- Can contribute to weak-source debt: yes
- Can anchor deck-ready claims: no
- Remains visible in diagnostics: yes
- Default handling: preserve in diagnostics only.
- Risk notes: this is the strongest non-deletion quarantine state and should be
  used only with explicit controlled reasons.

### `insufficient_evidence_slice`

- Definition: a slice where the observed rows are too few, too noisy, or too
  weakly patterned to classify confidently as either evidence-producing or
  debt-producing.
- Can contribute to deck-ready source balance: conditional
- Can contribute to weak-source debt: conditional
- Can anchor deck-ready claims: no
- Remains visible in diagnostics: yes
- Default handling: keep visible and conservative until precision improves.
- Risk notes: this bucket prevents forced classification when the evidence base
  is thin.

## Slice Eligibility Rules

A slice may be treated as `evidence_producing_slice` only when all of the
following are broadly true:

- it contributes persona-core rows or denominator-eligible business evidence
- technical/support noise is not dominant
- it helps `persona_01` through `persona_04`
- it is not only vendor-support, setup, or implementation chatter
- it has stable enough volume to matter for source interpretation

A slice may be treated as `debt_producing_slice` when one or more of the
following dominate:

- technical/support/debug/setup noise
- very low persona-core contribution
- denominator-excluded rows dominate
- the slice inflates source volume without enough usable evidence

Mixed cases should remain `mixed_evidence_slice` or
`insufficient_evidence_slice` until slice-level precision is good enough to
justify stronger handling.

## Current Weak-Source Slice Classifications

### `google_developer_forums`

- `evidence_producing_slice`
  - `sharing_permissions_delivery`
  - `report_logic_and_filters`
- `mixed_evidence_slice`
  - `report_delivery_ui`
- `debt_producing_slice`
  - `other_operational`
- Notes:
  - this is a strong good-source-bad-slice-mix case
  - auth/setup/API slices can still carry business context, so they should not
    be force-quarantined without precision validation

### `adobe_analytics_community`

- `evidence_producing_slice`
  - `metric_reconciliation`
- `mixed_evidence_slice`
  - `workspace_reporting`
  - `implementation_tracking`
  - `api_admin_config`
  - `other_operational`
- Notes:
  - source-specific language modeling matters here
  - codebook boundary leakage can make technical rows look more business-heavy
    than they really are

### `domo_community_forum`

- `debt_producing_slice`
  - `card_report_delivery`
  - `etl_dataset_config`
  - `beast_mode_formula`
  - `filtering_logic`
  - `other_operational`
- Notes:
  - currently no strong evidence-producing slice was found
  - Domo looks less like a mixed source and more like a debt-heavy slice set
    under the current audit

### `klaviyo_community`

- `evidence_producing_slice`
  - `revenue_reporting`
  - `segment_flow_logic`
- `debt_producing_slice`
  - `other_operational`
- Notes:
  - the source remains `excluded_from_deck_ready_core` unless a later policy
    explicitly changes that
  - even its useful slices are too small to resolve source-balance problems by
    themselves

## Slice-Aware Source Balance Interpretation

The policy questions for source balance should be answered this way:

- Official source balance should remain visible and unchanged.
- Quality-adjusted source balance should be added as a separate audited metric
  first, not as a silent replacement.
- Future quality-adjusted balance should prefer slice-qualified evidence over
  all active rows when evaluating deck-ready balance interpretation.
- Debt-producing slices should not help future quality-adjusted source balance.
- Official and quality-adjusted balance must coexist side by side.
- Weak or debt-heavy slices must remain visible so the balance metric does not
  hide weak sources.

Recommended interpretation order:

1. Keep `official_effective_balanced_source_count` as the current official
   metric.
2. Add `quality_adjusted_effective_balanced_source_count` as a secondary audited
   diagnostic.
3. Do not let the adjusted metric participate in readiness gating until
   slice-classification precision and diagnostics visibility are validated.

## Slice-Aware Weak-Source Debt Interpretation

The policy questions for weak-source debt should be answered this way:

- Source-level weak-source debt should remain visible.
- Slice-aware weak-source debt should be introduced as a secondary diagnostic,
  not an immediate replacement.
- Evidence-producing slices inside weak sources should remain usable for future
  audited interpretation.
- Debt-producing slices must remain diagnostics-visible even if later
  quarantine-like handling is added.
- A source should not stop counting as officially weak only because a slice-aware
  simulation looks better; that change should come only after precision
  validation and explicit approval.

Recommended interpretation order:

1. Keep `official_weak_source_cost_center_count` and
   `official_core_readiness_weak_source_count` visible.
2. Add `slice_aware_weak_source_cost_center_count` and
   `slice_aware_core_readiness_weak_source_count` as separate audited
   diagnostics.
3. Only after validation should policy decide whether slice-aware weak-source
   debt can change official readiness interpretation.

## Anti-Cherry-Picking Guardrails

Any future implementation must require all of the following:

- no raw deletion
- no source deletion
- no slice deletion
- all quarantined or downgraded slices remain diagnostics-visible
- every slice classification has a controlled reason
- slice counts are reported by source and category
- source-level and slice-level metrics are shown side by side
- evidence-producing slices cannot be selected only because they improve metrics
- debt-producing slices cannot be hidden
- official metrics cannot be silently replaced

These guardrails are the minimum requirement for making slice-aware handling
auditable rather than cosmetic.

## Required Future Fields

Future source/slice-level fields:

- `source_slice_id`
- `source_slice_name`
- `source_slice_category`
- `source_slice_reason`
- `source_slice_confidence`
- `source_slice_deck_ready_balance_eligible`
- `source_slice_weak_debt_eligible`
- `source_slice_quarantine_status`
- `source_slice_quarantine_reason`

Future aggregate fields:

- `quality_adjusted_effective_balanced_source_count`
- `official_effective_balanced_source_count`
- `slice_aware_weak_source_cost_center_count`
- `official_weak_source_cost_center_count`
- `slice_aware_core_readiness_weak_source_count`
- `official_core_readiness_weak_source_count`
- `debt_producing_slice_count`
- `evidence_producing_slice_count`

## Implementation Phases

Suggested rollout order:

1. Phase 1: add slice classification diagnostics only
2. Phase 2: run quality-adjusted source balance simulation
3. Phase 3: run slice-aware weak-source debt simulation
4. Phase 4: validate slice classification precision
5. Phase 5: add secondary audited source balance / weak debt metrics
6. Phase 6: consider readiness gate participation only after approval

## Test Plan

Before any implementation, tests should verify:

- source-level diagnostics remain visible
- debt-producing slices remain visible
- official source balance remains visible
- quality-adjusted balance is separate
- source-level weak-source count remains visible
- slice-aware weak-source count is separate
- Google evidence-producing slices are not lost
- Domo debt-producing slices do not silently disappear
- Klaviyo remains excluded from deck-ready core unless explicitly changed
- persona counts do not change from slice classification alone

## Risks Before Implementation

- Google and Adobe both contain mixed slices, so premature quarantine would be
  easy to overfit.
- Domo is debt-heavy enough that a slice-aware policy could become a disguised
  source-removal policy if diagnostics visibility is not enforced.
- Klaviyo contains some useful slices, but they are too small to justify
  relaxing its current deck-ready-core exclusion without a broader source-fit
  decision.
- Quality-adjusted source balance can become more methodologically meaningful
  than the current official balance, but that does not mean it is ready to
  replace the official metric.
