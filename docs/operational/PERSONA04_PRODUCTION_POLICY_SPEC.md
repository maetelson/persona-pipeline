# PERSONA04_PRODUCTION_POLICY_SPEC

## Purpose

This document defines how `persona_04` should be interpreted when local persona quality is strong but workbook-global production policy still blocks promotion.

This is a policy and interpretation document only.

It does not:
- promote `persona_04`
- change `final_usable_persona` semantics
- change clustering, labeling, source rules, source tiers, workbook thresholds, workbook readiness, review-ready logic, deck-ready claim logic, or XLSX presentation

## Current production-ready semantics

### What `production_ready_persona` currently means

`production_ready_persona` is the strict persona tier used for final production outputs.

A `production_ready_persona` is expected to:
- clear local structural support and grounding requirements
- avoid thin-evidence or duplicate-persona risks
- remain coherent under workbook-global concentration and weak-source policy
- be safe to count toward the final production persona asset

### What `final_usable_persona` currently means

`final_usable_persona` is the count-facing production output used in the workbook and downstream release narrative.

Today it implies:
- the persona is production-ready
- the persona is safe to include in the final usable count
- the workbook-level interpretation stays coherent when the persona is counted

### Why `persona_04` does not currently qualify

`persona_04` is locally strong:
- semantically distinct
- structurally supported
- grounded
- `selected_example_count = 5`
- `core_anchor_strength = strong`
- `supporting_validation_strength = strong`
- `exploratory_dependency_risk = low`
- `excluded_source_dependency_risk = low`

But it is still blocked by:
- primary: `workbook_global_concentration_policy`
- secondary: `weak_source_debt`

### Why `review_ready_persona` plus `deck_ready_claim_eligible_persona` is not the same as `production_ready_persona`

`review_ready_persona` means the persona is strong enough for analyst review and workbook discussion.

`deck_ready_claim_eligible_persona` means the persona can support deck or analyst discussion under the two-layer evidence policy.

Neither label means:
- the persona is safe to count in `final_usable_persona_count`
- the persona is safe to strengthen production semantics while workbook readiness remains `reviewable_but_not_deck_ready`

## Policy problem

The current problem is not local persona quality.

The current problem is semantic contradiction:
- `persona_04` is locally strong
- workbook-global warnings still remain
- promoting `persona_04` would increase `production_ready_persona_count` from `3` to `4`
- promoting `persona_04` would increase `final_usable_persona_count` from `3` to `4`
- workbook state would still remain `reviewable_but_not_deck_ready`

That is risky because it would make persona-level semantics stronger than workbook-level readiness.

In practice, that would create a release that says:
- the workbook is still not deck-ready
- the workbook still carries concentration and weak-source warnings
- but a fourth persona is now counted as final usable anyway

This weakens the meaning of `production_ready_persona` and `final_usable_persona`.

It also opens a policy hole that nearby personas could try to exploit later, especially if they are locally strong but still globally constrained.

## Option comparison

### Option A. Keep current strict policy

Definition:
- `persona_04` remains `review_ready_persona`
- `persona_04` remains `deck_ready_claim_eligible_persona`
- `persona_04` remains non-production and outside `final_usable_persona_count`

Evaluation:
- methodological rigor: high
- standards weakening risk: low
- reviewer confusion risk: low
- implementation complexity: none
- `final_usable_persona_count` change: no
- workbook readiness contradiction: no
- risk `persona_05` could exploit the same path: low
- recommendation: yes

### Option B. Allow one constrained-production persona

Definition:
- `persona_04` becomes production-ready while still marked as policy-constrained

Evaluation:
- methodological rigor: medium
- standards weakening risk: high
- reviewer confusion risk: high
- implementation complexity: medium
- `final_usable_persona_count` change: yes
- workbook readiness contradiction: yes
- risk `persona_05` could exploit the same path: medium
- recommendation: no

Rationale:
- this creates a production persona that still lives inside a workbook that is not production-ready at the workbook level

### Option C. Add a new non-counting tier

Definition:
- introduce `constrained_production_candidate` as an interpretive tier only
- keep `production_ready_persona` and `final_usable_persona` unchanged

Evaluation:
- methodological rigor: high
- standards weakening risk: low
- reviewer confusion risk: medium
- implementation complexity: medium
- `final_usable_persona_count` change: no
- workbook readiness contradiction: no
- risk `persona_05` could exploit the same path: low if rules are strict
- recommendation: yes, optional

Rationale:
- this preserves strict production semantics while giving reviewers a precise label for a locally strong but globally constrained persona

### Option D. Redefine `final_usable_persona`

Definition:
- allow locally strong review-ready personas to count as final usable

Evaluation:
- methodological rigor: low
- standards weakening risk: very high
- reviewer confusion risk: high
- implementation complexity: high
- `final_usable_persona_count` change: yes
- workbook readiness contradiction: yes
- risk `persona_05` could exploit the same path: high
- recommendation: no

### Option E. Keep production semantics strict and use claim eligibility for `persona_04`

Definition:
- maintain the current production/final-usable boundary
- rely on `deck_ready_claim_eligible_persona` for discussion-safe usage

Evaluation:
- methodological rigor: high
- standards weakening risk: low
- reviewer confusion risk: low
- implementation complexity: none
- `final_usable_persona_count` change: no
- workbook readiness contradiction: no
- risk `persona_05` could exploit the same path: low
- recommendation: yes

## Recommended policy path

Recommended path:
- keep current `production_ready_persona` and `final_usable_persona` semantics strict
- keep `persona_04` as `review_ready_persona`
- keep `persona_04` as `deck_ready_claim_eligible_persona`
- do not directly promote `persona_04`
- optionally introduce `constrained_production_candidate` later as a non-counting interpretive tier if reviewer communication needs it

This means the safe default remains Option A plus Option E, with Option C available only if the team needs a clearer label for policy-constrained strength.

## Optional `constrained_production_candidate` tier

This tier should only be introduced if the team needs extra interpretive precision.

It should not act as a hidden promotion path.

### Exact eligibility rules

A persona may qualify as `constrained_production_candidate` only if all of the following are true:
- `production_ready_persona = False`
- `review_ready_persona = True`
- `deck_ready_claim_eligible_persona = True`
- `structural_support_status = structurally_supported`
- `grounding_status` is grounded
- selected examples are sufficient for reviewer interpretation
- semantic distinctiveness is strong enough to avoid near-duplicate risk
- `core_anchor_strength` is `moderate` or `strong`
- `supporting_validation_strength` is `moderate` or `strong`
- `exploratory_dependency_risk = low`
- `excluded_source_dependency_risk = low`
- the persona is blocked by workbook-global policy, not by local evidence weakness

### Count behavior

`constrained_production_candidate` must not change:
- `final_usable_persona_count`
- `production_ready_persona_count`
- `review_ready_persona_count`

It is interpretive only.

### Workbook and readme behavior

If ever implemented, this tier may appear in workbook or readme presentation only if the wording is explicit that:
- it is not production-ready
- it is not final usable
- it is a locally strong but globally constrained persona

### Why `persona_04` qualifies

`persona_04` qualifies for this optional interpretive tier because:
- it is review-ready
- it is claim-eligible
- local evidence is strong
- the blocker is workbook-global policy rather than thin evidence, duplication, or weak grounding

### Why `persona_05` does not qualify

`persona_05` does not qualify because:
- it is not review-ready
- it is not claim-eligible
- it remains blocked/constrained for local evidence reasons
- its boundary clarity and evidence pool are not strong enough

## Tests needed before any implementation

If this policy is implemented later, tests must verify:
- `persona_04` does not become `final_usable_persona` unless explicitly approved
- `persona_04` does not become `production_ready_persona` through interpretive tiering alone
- `persona_05` cannot use the same path
- `final_usable_persona_count` stays strict unless policy explicitly changes
- `production_ready_persona_count` changes only if policy explicitly approves it
- workbook readiness wording remains coherent
- `review_ready_persona` and `deck_ready_claim_eligible_persona` remain separate from production semantics
- any optional `constrained_production_candidate` field is non-counting
- no hidden coupling lets the new tier influence readiness calculations

## Implementation stance

This document does not approve direct promotion of `persona_04`.

The current default interpretation remains:
- `persona_04` stays `review_ready_persona`
- `persona_04` stays `deck_ready_claim_eligible_persona`
- `persona_04` stays non-production
- `persona_04` stays outside `final_usable_persona_count`

Only a later, explicit implementation pass may add a non-counting interpretive tier if the team decides that extra communication precision is worth the added complexity.
