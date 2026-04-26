# Deck-Ready Readiness Interpretation Spec

## Status

- Spec only.
- No production policy change is implemented by this document.
- No thresholds, source configs, clustering rules, labeling rules, promotion logic, workbook readiness policy, XLSX presentation, or `final_usable_persona` semantics are changed here.
- Phase 3 defines interpretation rules only. It does not yet change workbook readiness calculation.

## Purpose

Define how deck-ready interpretation should work after:

1. Phase 1 source-tier annotation
2. Phase 2 tier-aware evidence accounting

This spec separates:

- strict persona production semantics
- review-ready visibility semantics
- deck-ready claim eligibility semantics

The goal is to allow stronger claim interpretation where evidence supports it without:

- bypassing thresholds
- weakening persona standards
- hiding weak-source diagnostics
- collapsing production-ready and review-ready meanings into one label

## Current Evidence-Tier State

### Stable workbook baseline

- `persona_readiness_state = reviewable_but_not_deck_ready`
- `overall_status = WARN`
- `quality_flag = EXPLORATORY`
- `final_usable_persona_count = 3`
- `production_ready_persona_count = 3`
- `review_ready_persona_count = 1`

These values remain the current source of truth until a later approved implementation explicitly changes readiness interpretation.

### Global tier evidence counts

- `deck_ready_core_labeled_row_count = 5169`
- `deck_ready_core_persona_core_row_count = 5154`
- `supporting_validation_labeled_row_count = 6276`
- `supporting_validation_persona_core_row_count = 4001`
- `exploratory_edge_labeled_row_count = 1119`
- `exploratory_edge_persona_core_row_count = 187`
- `excluded_from_deck_ready_core_labeled_row_count = 110`
- `excluded_from_deck_ready_core_persona_core_row_count = 103`

Interpretation:

- core representative evidence is substantial, not token-level
- supporting validation evidence is also substantial
- exploratory and excluded persona-core dependence is present, but comparatively small

### Current persona-level evidence summary

#### `persona_01`

- production-ready persona
- core representative rows: `1893 / 4490` (`42.16%`)
- supporting validation rows: `2419 / 4490` (`53.88%`)
- exploratory dependency risk: `low`
- excluded dependency risk: `low`
- current evidence status: `core_anchored`

#### `persona_02`

- production-ready persona
- core representative rows: `1901 / 2570` (`73.97%`)
- supporting validation rows: `629 / 2570` (`24.47%`)
- exploratory dependency risk: `low`
- excluded dependency risk: `low`
- current evidence status: `core_anchored`

#### `persona_03`

- production-ready persona
- core representative rows: `591 / 807` (`73.23%`)
- supporting validation rows: `206 / 807` (`25.53%`)
- exploratory dependency risk: `low`
- excluded dependency risk: `low`
- current evidence status: `core_anchored`

#### `persona_04`

- review-ready persona
- core representative rows: `174 / 601` (`28.95%`)
- supporting validation rows: `387 / 601` (`64.39%`)
- exploratory dependency risk: `low`
- excluded dependency risk: `low`
- current evidence status: `core_anchored`

#### `persona_05`

- blocked or constrained candidate
- core representative rows: `377 / 539` (`69.94%`)
- supporting validation rows: `153 / 539` (`28.39%`)
- exploratory dependency risk: `low`
- excluded dependency risk: `low`
- current evidence status: `core_anchored`

### Current anchor summary

Under the Phase 2 descriptive diagnostics:

- `persona_01`, `persona_02`, `persona_03`, and `persona_04` are all `core_anchored`
- no current production-ready persona is heavily dependent on exploratory or excluded evidence
- `persona_04` has meaningful core representative evidence, but it is still not production-ready under current workbook policy
- `persona_05` is blocked for reasons other than lack of core representative evidence alone

## Problem To Solve In Phase 3

The current workbook can already say:

- which personas are production-ready
- which personas are review-ready
- which sources are core representative, supporting, exploratory, or excluded from deck-ready core
- which personas have core representative anchors

But it cannot yet say, in a policy-backed way:

- whether a persona is eligible to be described as deck-ready claim material
- how supporting validation helps without replacing core representative evidence
- how to keep exploratory and excluded evidence visible without letting them anchor claims

The Phase 3 question is therefore not:

- "should we change readiness thresholds?"

It is:

- "should we add an interpretation layer for deck-ready claim eligibility before touching workbook readiness itself?"

## Proposed Interpretation Vocabulary

Phase 3 should distinguish the following labels:

### `production_ready_persona`

Existing meaning. A persona that already clears the current strict production/final-usable standard. This remains unchanged.

### `review_ready_persona`

Existing meaning. A persona suitable for analyst review and deck discussion, but not yet production-ready. This remains unchanged.

### `deck_ready_claim_eligible_persona`

A new interpretation label. This would mean the persona has sufficient core representative anchor evidence to support deck-ready-style claim wording, without changing whether it is production-ready or final usable.

This label is not equal to:

- `production_ready_persona`
- `final_usable_persona`
- `review_ready_persona`

It is a separate claim-eligibility layer.

### `supporting_validated_persona`

A persona with useful supporting-validation evidence, but not enough core representative anchoring to support stronger deck-ready claims on its own.

### `exploratory_dependent_persona`

A persona whose evidence posture still relies too heavily on exploratory or excluded sources for deck-ready claim wording.

### `blocked_or_constrained_candidate`

Existing meaning. A candidate that remains blocked by current workbook policy. This remains unchanged.

## Proposed Minimum Requirements For `deck_ready_claim_eligible_persona`

The safest Phase 3 interpretation rule is:

1. the persona must already be `production_ready_persona`, or be an explicitly approved `review_ready_persona`
2. `has_core_representative_anchor = True`
3. `core_anchor_strength` must be `moderate` or `strong`
4. `exploratory_dependency_risk` must be `low`
5. `excluded_source_dependency_risk` must be `low`
6. supporting validation may strengthen confidence, but cannot replace the core representative anchor
7. the persona must not be dominated by weak-source dependence in a way that contradicts the claim
8. any thin-evidence or structural warning that blocks strong claims must remain visible

Interpretation note:

- `supporting_validation_strength` is additive, not substitutive
- `core_anchor_strength = weak` is not enough by itself for deck-ready claim eligibility
- exploratory or excluded dependence should block claim-eligibility if the risk rises above `low`

## Proposed Rule For Weak-Source Use In Deck-Ready Claim Interpretation

Phase 3 should not convert all weak-source diagnostics into automatic blockers.

Instead:

- weak-source diagnostics stay visible
- weak-source dependence becomes interpretive context for claim wording
- a persona should not be treated as deck-ready-claim-eligible when its core anchor or major support is weak-source-dominated in a way that undermines methodological trust

This keeps Phase 3 narrower than a full readiness redesign.

## Persona 04 Treatment Recommendation

### Recommendation

- `persona_04` should remain `review_ready_persona`
- `persona_04` may become a `deck_ready_claim_candidate` or `deck_ready_claim_eligible_persona` only if Phase 3 implementation is approved and the explicit claim-eligibility rule is satisfied
- `persona_04` should remain excluded from `final_usable_persona_count`
- workbook wording must continue to avoid implying `persona_04` is production-ready

### Why

Current evidence says:

- `persona_04` has a real core representative anchor
- `persona_04` has strong supporting validation
- dependency on exploratory and excluded evidence is low

But current policy also says:

- it is review-ready only
- it is not production-ready
- it is not part of the final usable count

Therefore the safest Phase 3 move is:

- allow explicit claim-candidate wording if approved
- do not silently upgrade it into production-ready or final-usable semantics

### Wording requirement

If implemented later, workbook wording should say something like:

- "deck-ready claim candidate"
- "core-anchored review-ready persona"

and must not imply:

- "production-ready persona"
- "final usable persona"
- "included in headline final persona count"

## Workbook-Level Blockers Under The Two-Layer Policy

Current workbook blockers and warnings:

- `core_readiness_weak_source_cost_center_count = 3`
- `weak_source_cost_center_count = 4`
- `effective_balanced_source_count = 5.89`
- `persona_core_coverage_of_all_labeled_pct = 74.5`

Phase 3 should interpret these as follows:

- the two-layer policy does not automatically erase them
- the two-layer policy may change claim interpretation at the persona level
- the two-layer policy does not, by itself, change workbook readiness state

Therefore the safest Phase 3 stance is:

- change deck-ready claim eligibility first
- do not change workbook deck-ready readiness calculation yet

This means the two-layer policy should initially:

- change only deck-ready claim eligibility
- keep workbook readiness unchanged
- use evidence-tier counts as interpretive context, not as a replacement readiness engine

## Phase 3 Policy Options

### A. Descriptive only

Keep readiness unchanged and use the evidence-tier diagnostics only as explanation.

- Methodological rigor: medium
- Threshold bypass risk: low
- Cherry-picking risk: low
- Implementation complexity: low
- Reviewer clarity: medium
- Moves toward deck-ready: low
- Preserves persona semantics: high

Why not choose it:

- it adds very little operational value beyond Phase 2
- it does not let the workbook express which personas are safe for stronger claim framing

### B. Deck-ready claim eligibility layer

Keep workbook readiness unchanged, but add a new persona-level claim-eligibility interpretation.

- Methodological rigor: high
- Threshold bypass risk: low-medium
- Cherry-picking risk: medium
- Implementation complexity: medium
- Reviewer clarity: high
- Moves toward deck-ready: medium-high
- Preserves persona semantics: high

Why choose it:

- it adds useful interpretation without rewriting readiness policy
- it preserves current production-ready and review-ready meanings
- it gives reviewers a clearer way to talk about core-anchored personas

### C. Full readiness reinterpretation

Use source-tier evidence to modify workbook deck-ready readiness calculation.

- Methodological rigor: potentially high
- Threshold bypass risk: high
- Cherry-picking risk: high
- Implementation complexity: high
- Reviewer clarity: medium
- Moves toward deck-ready: high
- Preserves persona semantics: medium-low

Why not choose it now:

- it is too easy to use as an indirect threshold bypass
- it would mix descriptive diagnostics, source-tier policy, and workbook readiness in one step
- the current evidence does not yet justify that larger policy move safely

## Recommended Phase 3 Path

Recommended path:

- **Option B: deck-ready claim eligibility layer first**

This means:

- keep `persona_readiness_state`, `overall_status`, and `quality_flag` unchanged
- keep `final_usable_persona_count`, `production_ready_persona_count`, and `review_ready_persona_count` unchanged
- add an explicit persona-level label for deck-ready claim eligibility
- keep this separate from production-ready and final-usable semantics

This is the narrowest path that:

- uses the new tier evidence meaningfully
- preserves current strict persona semantics
- avoids a silent workbook readiness rewrite

## Affected Outputs If Approved Later

Phase 3 implementation would likely affect:

- `overview.csv`
- `persona_summary.csv`
- `cluster_stats.csv`
- `persona_promotion_path_debug.csv` if reviewer traceability is needed
- `quality_checks.csv` only if explicit interpretation rows are added
- XLSX readme and presentation in a later presentation phase

## Proposed Fields For Future Implementation

Possible fields:

- `deck_ready_claim_eligible_persona`
- `deck_ready_claim_evidence_status`
- `deck_ready_claim_reason`
- `core_anchor_policy_status`
- `supporting_validation_policy_status`
- `exploratory_dependency_policy_status`
- `excluded_source_dependency_policy_status`

Suggested semantics:

- `deck_ready_claim_eligible_persona`: boolean interpretation layer, not a production-ready flag
- `deck_ready_claim_evidence_status`: one of `core_anchored`, `supporting_validated`, `exploratory_dependent`, `insufficient_core_evidence`
- `deck_ready_claim_reason`: short reviewer-facing explanation
- `*_policy_status`: explicit pass/warn/block style interpretation rows for each evidence dimension

## Required Tests Before Implementation

Phase 3 implementation should prove:

1. evidence-tier diagnostics do not change `final_usable_persona`
2. production-ready count remains unchanged unless explicitly changed in a later approved pass
3. `persona_01`, `persona_02`, and `persona_03` become `deck_ready_claim_eligible_persona` if criteria pass
4. `persona_04` treatment is explicit and remains non-production
5. `persona_05` remains blocked
6. exploratory or excluded dependency risk can block deck-ready claim eligibility
7. supporting validation cannot substitute for a missing core representative anchor
8. workbook readiness state does not silently change unless a later phase explicitly chooses full reinterpretation

## Risks Before Implementation

### Risk 1: semantic drift

Reviewers may confuse `deck_ready_claim_eligible_persona` with:

- `production_ready_persona`
- `final_usable_persona`

This must be prevented with explicit wording.

### Risk 2: cherry-picking perception

If core-anchored personas are highlighted without keeping supporting, exploratory, and weak-source context visible, the workbook may appear curated for a preferred answer.

### Risk 3: soft threshold bypass

If claim-eligibility is implemented carelessly, it could function as a shadow readiness gate that bypasses the existing workbook policy.

### Risk 4: persona_04 ambiguity

`persona_04` is the most sensitive case because it is review-ready and core-anchored, but still not production-ready. The implementation must preserve that distinction cleanly.

### Risk 5: implementation creep

Phase 3 should not silently become Phase 4. Claim-eligibility labeling should land before any XLSX presentation redesign or workbook readiness reinterpretation.

## Final Recommendation

Proceed with:

- Phase 3 as a **deck-ready claim eligibility interpretation layer**

Do not proceed yet with:

- full workbook readiness reinterpretation
- threshold changes
- source deletion
- source-tier reclassification

The current workbook should remain:

- `reviewable_but_not_deck_ready`

until a later approved pass explicitly changes readiness policy.
