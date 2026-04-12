## Persona Promotion And Grounding Policy

This policy separates promotion candidacy, workbook visibility, downstream usability, and deck-readiness so the workbook cannot imply that unsupported personas are production-ready.

It also separates structural support from grounding so a grounded but weakly separated cluster can remain review-visible without being counted as a mature persona.

### State Model

- `promoted_candidate_persona`
  - Passed the base size and dominance gate before grounding review.
  - This is a candidate state, not a downstream-usable persona state.

- `review_visible_persona`
  - Remains visible in workbook review tables after grounding merge.
  - Used when a promoted candidate is still worth reviewer inspection but is not final usable.

- `promoted_but_ungrounded`
  - A promoted candidate with no accepted grounded example.
  - Must remain `review_visible_persona` or be downgraded; it must never count as headline, final usable, or deck-ready.

- `promoted_but_weakly_grounded`
  - A promoted candidate retained only by weak fallback grounding.
  - Must remain `review_visible_persona`; it is not final usable or deck-ready.

- `grounded_but_structurally_weak`
  - A promoted candidate with acceptable grounding evidence but weak structural support.
  - Must remain `review_visible_persona`; it is grounded enough to inspect, but still too weakly separated or fragile to count as final usable or deck-ready.

- `final_usable_persona`
  - A promoted candidate that is grounded strongly enough for downstream reporting.
  - This is the canonical usable-persona state.

- `deck_ready_persona`
  - A final usable persona that is safe for deck-ready headline presentation.
  - Under the current policy this matches `final_usable_persona`.

### Workbook Count Contract

The workbook must expose these counts separately:

- `promoted_candidate_persona_count`
  - Base promotion candidates before grounding review.

- `promotion_visibility_persona_count`
  - Personas still visible for reviewer inspection after grounding merge.
  - This can include grounded, weakly grounded, and ungrounded promoted candidates.

- `headline_persona_count`
  - Headline persona total shown to workbook readers.
  - This must equal `final_usable_persona_count`.

- `final_usable_persona_count`
  - Personas safe for downstream reporting.
  - Unsupported review-visible personas must not count here.

- `deck_ready_persona_count`
  - Personas safe for deck-ready presentation.
  - Under the current policy this equals `final_usable_persona_count`.

### Row-Level Contract

`cluster_stats` and `persona_summary` must expose enough row-level state to explain why a persona is visible, usable, or blocked:

- `base_promotion_status`
- `promotion_status`
- `promotion_grounding_status`
- `structural_support_status`
- `structural_support_reason`
- `grounding_status`
- `visibility_state`
- `usability_state`
- `deck_readiness_state`
- `promotion_action`
- `workbook_review_visible`
- `final_usable_persona`
- `deck_ready_persona`
- `reporting_readiness_status`

### Reviewer Interpretation

If a persona is promoted but lacks acceptable grounding, or if it is grounded but still structurally weak, it must be explicitly represented as review-visible-only or downgraded. The workbook must never collapse these review-visible personas into the headline persona total.

Example:

- `promotion_visibility_persona_count = 4`
- `headline_persona_count = 2`
- `final_usable_persona_count = 2`
- `promoted_persona_ungrounded_count = 2`

Correct interpretation:

- Four personas remain visible for audit.
- Two personas are actually usable downstream.
- Two promoted candidates remain unsupported and must not be presented as usable personas.