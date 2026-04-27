# Adjusted Denominator Readiness Policy Decision

## Scope

This pass evaluates whether the audited conservative adjusted denominator metric
may participate in workbook readiness gating. It does not change readiness
behavior, does not replace the original coverage metric, and does not hide
excluded rows.

## Current Denominator State

- Original coverage metric remains `persona_core_coverage_of_all_labeled_pct = 74.5`
- Adjusted conservative metric is `adjusted_deck_ready_denominator_core_coverage_pct = 83.25`
- Adjusted denominator row count is `11345`
- Adjusted denominator excluded row count is `1329`
- Policy mode is `conservative_high_confidence_noise_only`
- Policy version is `v1`
- Adjusted denominator metric status is `audited`

Rows excluded by the conservative policy remain visible in diagnostics and in
the dedicated exclusion artifact. `ambiguous_review_bucket` rows remain
included. `denominator_eligible_business_non_core` rows remain included.
`persona_core_evidence` rows are never excluded.

## Exclusion Distribution

By category:

- `server_deploy_config_noise = 981`
- `syntax_formula_debug_noise = 144`
- `source_specific_support_noise = 77`
- `api_sdk_debug_noise = 71`
- `setup_auth_permission_noise = 55`
- `career_training_certification_noise = 1`

By source:

- `domo_community_forum = 519`
- `stackoverflow = 440`
- `shopify_community = 140`
- `google_developer_forums = 81`
- `github_discussions = 66`
- `adobe_analytics_community = 64`
- `hubspot_community = 10`
- `klaviyo_community = 5`
- `power_bi_community = 3`
- `reddit = 1`

By source tier:

- `supporting_validation_source = 802`
- `exploratory_edge_source = 519`
- `excluded_from_deck_ready_core = 5`
- `core_representative_source = 3`

## Policy Safety Assessment

### Narrowness

The implemented exclusion rule is narrow enough for audited use. It excludes
only non-core rows that are already denominator-ineligible under the row-level
policy, belong to an explicit technical/support noise category, and meet the
high-confidence threshold `technical_noise_confidence >= 0.9`.

### Precision

The prior sample audit found `100%` precision for high-confidence exclusion
candidates and only `20%` precision for medium-confidence candidates. That
supports use of the current conservative exclusion rule, but does not justify a
broader denominator policy.

### Ambiguity Handling

Ambiguous rows are handled conservatively. `ambiguous_review_bucket` remains
included in the denominator and visible in diagnostics. This is the main guard
against overstating denominator cleanup.

### Concentration

Exclusions are somewhat concentrated:

- `domo_community_forum + stackoverflow = 959 / 1329 = 72.2%`

That concentration is acceptable for an audited secondary metric only because
row-level reasons and source/source-tier summaries remain visible. It would be
riskier if the adjusted metric were used as a silent replacement.

### Weak-Source Visibility

The policy does not hide weak-source evidence. Weak sources remain visible in
the workbook diagnostics, source diagnostics, denominator row artifact, and the
dedicated exclusion CSV.

### Cherry-Picking Risk

Cherry-picking risk is reduced but not eliminated. The risk is controlled
because:

- the rule is explicit and category-limited
- ambiguous rows are not excluded
- business non-core rows are not excluded
- the original metric remains visible
- excluded rows remain auditable by source and source tier

### Reviewer Explainability

The adjusted metric is explainable to reviewers as an audited denominator
variant that removes only explicit high-confidence technical/support noise while
retaining all rows in diagnostics. It is not explainable as a silent primary
replacement without the original metric.

## Readiness Options

### A. Keep adjusted metric diagnostic-only

- Methodological rigor: highest
- Cherry-picking risk: lowest
- Reviewer clarity: high
- Implementation complexity: none
- Workbook readiness could change: no
- Original metric remains visible: yes
- Persona counts change: no
- `persona_04` later production-ready impact: none
- `persona_05` blocked/subtheme impact: unchanged

### B. Use adjusted metric as secondary deck-ready coverage check

- Methodological rigor: high
- Cherry-picking risk: low to moderate
- Reviewer clarity: high if both metrics are shown side by side
- Implementation complexity: low to moderate
- Workbook readiness could change: not currently, because other blockers remain
- Original metric remains visible: yes
- Persona counts change: no
- `persona_04` later production-ready impact: possible only if all other gates clear
- `persona_05` blocked/subtheme impact: unchanged

### C. Replace original metric with adjusted metric

- Methodological rigor: low
- Cherry-picking risk: high
- Reviewer clarity: low
- Implementation complexity: low
- Workbook readiness could change: yes
- Original metric remains visible: no
- Persona counts change: no
- `persona_04` later production-ready impact: overstated
- `persona_05` blocked/subtheme impact: unchanged

This option is not recommended.

### D. Require both original and adjusted metrics to pass

- Methodological rigor: high
- Cherry-picking risk: very low
- Reviewer clarity: medium
- Implementation complexity: low
- Workbook readiness could change: no under current data
- Original metric remains visible: yes
- Persona counts change: no
- `persona_04` later production-ready impact: none until original coverage also passes
- `persona_05` blocked/subtheme impact: unchanged

This option preserves the current bottleneck and does not make practical use of
the audited denominator policy.

### E. Use adjusted metric only when conservative precision and diagnostics visibility pass

- Methodological rigor: high
- Cherry-picking risk: low
- Reviewer clarity: high
- Implementation complexity: moderate
- Workbook readiness could change: not currently, because other blockers remain
- Original metric remains visible: yes
- Persona counts change: no
- `persona_04` later production-ready impact: possible only with other blockers cleared
- `persona_05` blocked/subtheme impact: unchanged

This is effectively a guarded form of option B.

## Blocker Table Under the Adjusted Metric

- Adjusted coverage status: passes coverage floor at `83.25`
- Effective balanced source count: `5.89` and still below the `6.0` floor
- Weak source cost center count: `4`
- Core readiness weak source cost center count: `3`
- Source balance status: still `WARN`
- Selected-example grounding issue count: `4`
- Production-ready/final-usable counts: unchanged at `3`
- Review-ready count: unchanged at `1`
- Deck-ready claim-eligible count: unchanged at `4`

Remaining blockers after adjusted coverage passes:

- `effective_balanced_source_count < 6.0`
- `core_readiness_weak_source_cost_center_count = 3`
- selected-example grounding remains `WARN`
- overall workbook `overall_status = WARN`
- workbook `quality_flag = EXPLORATORY`

## Decision

Passing the adjusted conservative coverage metric alone is **not** enough to
make the workbook deck-ready. The adjusted metric resolves the coverage blocker
only. Source balance, weak-source debt, and example-grounding warnings still
prevent deck-ready readiness.

Recommended policy path:

- `allow_adjusted_metric_as_secondary_gate`

## Proposed Gate Logic For A Future Implementation

This pass does not implement the gate change. If adopted later, the exact logic
should be:

1. Keep `persona_core_coverage_of_all_labeled_pct` visible and unchanged.
2. Keep `original_persona_core_coverage_pct` and
   `adjusted_deck_ready_denominator_core_coverage_pct` both visible.
3. Allow the coverage component of deck-ready gating to pass when all are true:
   - `adjusted_denominator_metric_status = audited`
   - `denominator_policy_mode = conservative_high_confidence_noise_only`
   - `denominator_policy_version = v1`
   - adjusted conservative coverage `>= 80.0`
   - excluded rows remain visible in denominator diagnostics
   - `ambiguous_review_bucket` remains included
4. Keep all non-coverage deck-ready gates unchanged.
5. Keep the overall quality cap unchanged, so `overall_status` must still be
   compatible with deck-ready before the workbook state upgrades.

## Expected State If That Gate Change Were Implemented Today

Expected readiness after a future gate change would remain:

- `persona_readiness_state = reviewable_but_not_deck_ready`

Reason:

- source-balance and weak-source blockers still keep `overall_status = WARN`
- example-grounding warnings still remain

So this would be a **coverage-only policy improvement**, not a full readiness
upgrade.

## Expected Files Affected In A Future Gate Change

- `src/analysis/quality_status.py`
- `src/analysis/stage_service.py`
- `src/analysis/summary.py`
- readiness regression tests
- readiness documentation

## Tests Required Before Any Gate Change

- original metric remains visible and unchanged
- adjusted metric remains separately visible
- adjusted coverage can satisfy only the coverage gate, not all readiness
- ambiguous rows remain included
- business non-core rows remain included
- excluded rows remain visible in diagnostics
- persona counts do not change
- `persona_04` and `persona_05` semantics do not change
- workbook does not upgrade when source-balance or weak-source blockers remain

## Rollback Criteria

Rollback the future gate change if any of the following happens:

- adjusted coverage becomes materially dependent on a broadened exclusion rule
- excluded row concentration becomes materially worse without transparent diagnostics
- ambiguous rows begin to be excluded
- the original metric stops being shown alongside the adjusted metric
- workbook readiness upgrades while non-coverage blockers still remain

## README / Workbook Wording Requirements

If a later gate change is approved, README and workbook wording must say:

- the original all-labeled coverage metric remains the audited baseline
- the adjusted denominator metric is a conservative audited readiness aid
- adjusted coverage is not a silent replacement
- excluded rows remain visible in denominator diagnostics
- deck-ready still requires source-balance and weak-source conditions beyond coverage
