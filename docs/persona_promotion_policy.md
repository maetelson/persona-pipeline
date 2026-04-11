## Persona Promotion Policy

This policy defines how persona promotion, grounding visibility, and downstream reporting counts must be interpreted in the workbook.

### Core Terms

- `promoted_candidate_persona`
  - A cluster that passed the base promotion gate based on size and dominance rules before grounding review.
  - This is a candidate for promotion, not yet a final usable persona.

- `promoted_and_grounded`
  - A promoted candidate with at least one acceptable grounded representative example.
  - This is the only promoted state that counts as a final usable persona for downstream reporting.

- `promoted_but_weakly_grounded`
  - A promoted candidate that stays visible only because weak fallback evidence is allowed by policy.
  - It remains review-visible in the workbook but is not treated as final usable or deck-ready.

- `promoted_but_ungrounded`
  - A promoted candidate that remains visible under `ungrounded_action: flag` even though it has no acceptable grounding evidence.
  - It is review-visible only and must not contaminate final persona totals.

- `final_usable_persona`
  - A promoted candidate whose combined policy state is `promoted_and_grounded`.
  - This is the headline persona count for downstream reporting and the deck-ready persona count under the current policy.

### Count Policy

Under the current `ungrounded_action: flag` policy:

- Ungrounded promoted personas count toward workbook review visibility: yes.
- Ungrounded promoted personas count toward headline persona count: no.
- Ungrounded promoted personas count toward deck-ready persona count: no.
- Weakly grounded promoted personas count toward workbook review visibility: yes.
- Weakly grounded promoted personas count toward headline persona count: no.
- Weakly grounded promoted personas count toward deck-ready persona count: no.

### Workbook Metrics

The workbook must expose these top-level counts explicitly:

- `promoted_candidate_persona_count`
  - Base promotion candidates before grounding review.

- `promotion_visibility_persona_count`
  - Personas still visible in the workbook's promoted set after grounding merge.
  - Under the current flag policy this can include grounded, weakly grounded, and ungrounded promoted personas.

- `final_usable_persona_count`
  - Promoted personas that are grounded enough for downstream reporting.
  - Under the current policy this includes only `promoted_and_grounded` personas.

- `deck_ready_persona_count`
  - Personas safe to present as deck-ready headline personas.
  - Under the current policy this equals `final_usable_persona_count`.

### Workbook Row Semantics

`cluster_stats` and `persona_summary` must expose row-level fields so reviewers can filter visible promoted personas separately from final usable personas:

- `base_promotion_status`
- `promoted_candidate_persona`
- `promotion_status`
- `grounding_status`
- `promotion_grounding_status`
- `workbook_review_visible`
- `final_usable_persona`
- `deck_ready_persona`
- `reporting_readiness_status`

### Reviewer Rule

If one persona is explicitly `promoted_but_ungrounded`, the workbook must not present the promoted visibility total as if all promoted personas are usable. A reviewer must be able to see both:

- how many promoted personas remain visible for audit or review
- how many personas are actually usable for downstream reporting

### Current Example

If the workbook has:

- `promotion_visibility_persona_count = 3`
- `promoted_persona_grounded_count = 2`
- `promoted_persona_ungrounded_count = 1`

then the correct headline interpretation is:

- visible promoted personas for review: 3
- final usable personas: 2
- deck-ready personas: 2

The workbook must never collapse that back into an ambiguous `persona_count = 3` headline.