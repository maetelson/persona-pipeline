## Workbook Policy Redesign Spec

### Scope

This document defines a simulation-backed redesign for exposing `review_ready_persona` rows without weakening current `production_ready_persona` standards.

This is a specification only.

It does not change:

- production clustering
- axis weights
- source rules
- persona promotion logic
- workbook thresholds
- final export behavior

### Why This Spec Exists

Current evidence shows:

- the current workbook policy is correctly strict for production-ready persona claims
- `persona_04` has strong semantic, grounding, identity, and source-diversity support
- `persona_04` should still not count as production-ready because workbook-global concentration remains too high
- `persona_05` remains too thin to expose as anything stronger than blocked / constrained

So the design goal is not to relax production standards.

The design goal is to separate:

- personas safe for production-ready reporting
- personas strong enough for analyst review but still blocked from final headline use

### New Tier Semantics

The workbook should distinguish four row-level states.

#### `production_ready_persona`

- Meets the existing final usable persona standard.
- Safe for headline persona counts and downstream production-ready reporting.
- Continues to count toward `final_usable_persona_count`.

#### `review_ready_persona`

- Strong candidate for analyst review.
- Locally grounded and structurally supported.
- Semantically distinct enough to deserve visibility.
- Still blocked from headline production-ready persona counts.
- Must not be described as final validated persona output.
- Does not count toward `final_usable_persona_count`.

#### `exploratory_bucket`

- Retained for analysis context only.
- Not strong enough for production-ready or review-ready treatment.
- Includes residual or small exploratory clusters that remain useful as research material.

#### `blocked_or_constrained_candidate`

- A promoted or near-promoted candidate that remains blocked by workbook policy or local evidence weakness.
- May still carry strong signals, but should not be elevated into review-ready unless it clears the stricter review-ready rules below.

### Review-Ready Eligibility Rules

A persona may become `review_ready_persona` only if all of the following are true:

1. It passes local structural support.
   - `structural_support_status` must indicate structurally supported behavior.

2. It passes grounding.
   - `grounding_status` must indicate acceptable grounding.
   - It must not rely on weak fallback or missing examples.

3. It is not weak-source dominated.
   - It must not be linked to a weak-source cost center as its primary source.

4. It is not a near-duplicate.
   - Any redundancy or identity collision signal must remain clear enough that the persona is still semantically distinct.

5. It is not thin-evidence.
   - It must have enough selected examples and bundle evidence to support analyst review.

6. It is blocked mainly by workbook-global concentration policy.
   - The persona would otherwise look locally valid.
   - The main blocker should be workbook-wide concentration or related global caution, not local weakness.

7. It has clear semantic distinctiveness.
   - Its semantic identity should remain stable and interpretable.
   - For current evidence, this means the persona behaves like a real reconciliation/signoff persona rather than a generic reporting tail.

### Review-Ready Exclusion Rules

`review_ready_persona` must not include:

- weak-source dominated candidates
- near-duplicates
- thin-evidence candidates
- candidates with selected-example coverage too low for analyst review
- candidates that pass only because a threshold was relaxed
- `persona_05`-like thin or tool-limitation-heavy candidates
- candidates still failing local grounding or structural support

### Current Persona Examples

Under the current evidence, the expected treatment is:

#### `persona_01`

- `production_ready_persona`
- broad reporting-packager parent with sufficient local support and grounding

#### `persona_02`

- `production_ready_persona`
- locally supported and grounded under the current strict standard

#### `persona_03`

- `production_ready_persona`
- remains production-ready under current semantics
- should carry a reviewer note that representative examples are thinner than ideal
- this is a caution note, not a demotion rule in this spec

#### `persona_04`

- `review_ready_persona`
- semantically distinct and locally strong
- blocked from production-ready only because workbook-global concentration remains too high
- should be described as a strong review candidate, not a final validated persona

#### `persona_05`

- `blocked_or_constrained_candidate`
- remains blocked
- not eligible for `review_ready_persona`
- current evidence still points to thin example support and weaker semantic confidence than `persona_04`

### Output Surface Changes

The redesign should affect workbook-facing outputs, but only after implementation.

#### `overview.csv`

Add or clarify:

- `production_ready_persona_count`
- `review_ready_persona_count`
- `exploratory_persona_count`
- `final_usable_persona_count`

Rules:

- `final_usable_persona_count` must remain the strict production-ready count.
- `review_ready_persona_count` must be separate and clearly labeled as not headline-ready.
- `headline_persona_count` must not silently absorb review-ready personas.

#### `persona_summary.csv`

Add row-level fields for tier visibility and constraint explanation.

#### `cluster_stats.csv`

Add the same row-level tier fields so reviewers can filter and audit both production-ready and review-ready personas consistently.

#### `persona_promotion_path_debug.csv`

Add explicit branch visibility for:

- locally valid but workbook-constrained
- blocked for thin evidence
- blocked for weak-source or duplication risk

This file should become the clearest debug table for “why not production-ready?”

#### Workbook README / notes sheet

Add user-facing language that distinguishes:

- production-ready personas
- review-ready personas
- exploratory buckets

The notes must explicitly say that review-ready personas are not final validated persona claims and are not safe for deck-ready use without human review.

#### Final xlsx export

No implementation change in this pass.

When implemented later, the export should surface review-ready personas in a separate clearly labeled section, filter, or note path rather than folding them into production-ready totals.

### Proposed Fields

These fields should be added or updated during implementation:

- `readiness_tier`
- `production_ready_persona`
- `review_ready_persona`
- `review_ready_reason`
- `blocked_reason`
- `workbook_policy_constraint`
- `review_visibility_status`

Recommended semantics:

- `readiness_tier`
  - one of:
    - `production_ready_persona`
    - `review_ready_persona`
    - `blocked_or_constrained_candidate`
    - `exploratory_bucket`

- `production_ready_persona`
  - strict boolean
  - matches current `final_usable_persona` semantics unless policy is explicitly changed later

- `review_ready_persona`
  - boolean flag for locally strong but workbook-constrained personas

- `review_ready_reason`
  - short analyst-facing explanation of why the row is review-ready

- `blocked_reason`
  - explicit reason when the row remains blocked

- `workbook_policy_constraint`
  - machine-readable summary of global workbook blocker(s)

- `review_visibility_status`
  - explicit row-level display state for workbook views

### Metric Reporting Rules

These counts must remain separate:

- `final_usable_persona_count`
- `production_ready_persona_count`
- `review_ready_persona_count`
- `exploratory_persona_count`

Rules:

- `final_usable_persona_count` and `production_ready_persona_count` should stay aligned unless the implementation later distinguishes them intentionally.
- `review_ready_persona_count` must not inflate production-ready totals.
- Any workbook headline metric must say whether it is counting only production-ready personas or both production-ready and review-ready personas.

### User-Facing Language

`review_ready_persona` must never be described as:

- final validated persona
- deck-ready persona
- production-ready persona
- final headline persona

Preferred wording:

- strong candidate for review
- locally grounded but workbook-constrained
- needs human review before deck-ready use
- visible for analyst review, not final validated output

### Implementation Plan

This is the intended sequence for a later implementation pass.

1. Add row-level tier fields in the analysis layer.
   - start in workbook-facing persona frames, not in clustering logic

2. Keep current production-ready semantics unchanged.
   - `final_usable_persona` remains strict
   - current thresholds remain unchanged

3. Add review-ready classification as a separate overlay.
   - derive it after current production-ready classification
   - do not replace existing promotion logic

4. Update workbook-facing tables.
   - `overview.csv`
   - `persona_summary.csv`
   - `cluster_stats.csv`
   - `persona_promotion_path_debug.csv`

5. Update workbook notes / readme semantics.
   - clearly explain the two-tier interpretation

6. Update export rendering only after the workbook-facing frames are stable.
   - review-ready personas must be visibly separate from production-ready personas

### Test Plan Before Implementation

Implementation should not proceed without tests that verify:

1. Production-ready count does not increase just by adding the review-ready tier.

2. `persona_04` becomes `review_ready_persona`.

3. `persona_05` remains blocked and does not become review-ready.

4. Weak-source dominated candidates do not become review-ready.

5. Near-duplicates do not become review-ready.

6. Thin-evidence candidates do not become review-ready.

7. `final_usable_persona` semantics remain strict and unchanged.

8. Workbook export and workbook-facing CSVs clearly separate:
   - production-ready personas
   - review-ready personas
   - exploratory or blocked personas

9. Headline metrics do not silently absorb review-ready personas.

10. Readme / notes wording does not imply review-ready personas are final validated personas.

### Risks Before Implementation

1. `persona_03` already carries thin-example caution while remaining production-ready.
   - implementation must avoid accidentally treating all thin-example warnings as immediate demotion rules.

2. Review-ready visibility can easily confuse downstream readers if the workbook wording is not explicit.

3. If row-level fields are added inconsistently across `overview`, `cluster_stats`, and `persona_summary`, reviewers will lose trust in the tier semantics.

4. Any implementation that changes headline counts without very explicit naming will recreate the same ambiguity this redesign is trying to remove.

5. This spec is based on current evidence and simulation artifacts, not a gold acceptance regime.
   - implementation should preserve that caution in reviewer-facing language.

