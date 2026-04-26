## Persona 05 Merge / Suppression Review

### Summary

`persona_05` should not move toward deck-ready claim eligibility in its current standalone form.

The evidence does not support:

- promoting it
- marking it claim-eligible
- treating it as a clean fifth persona

But the evidence also does not support throwing it away as pure noise.

Recommended decision:

- `keep_as_future_candidate_subtheme`

The cleanest interpretation is that `persona_05` captures a real delivery-facing nuance:

- last-mile reporting output construction blocked by tool limitations

That nuance is real enough to preserve, but not clean enough to defend as a standalone persona.

### Decision Table

- total rows: `539`
- clean evidence: `15`
- ambiguous: `269`
- persona_03 overlap: `130`
- persona_01 overlap: `1`
- support noise: `88`
- weak generic: `36`
- selected examples: `5`
- deck-ready claim eligible: `False`
- readiness tier: `blocked_or_constrained_candidate`
- core representative rows: `377`
- supporting validation rows: `153`
- exploratory/excluded rows: `9`
- production-ready: `False`
- review-ready: `False`
- final usable: `False`

### Persona 05 vs Persona 03

#### Shared signals

- `tool_limitation_workaround`
- dashboard and filter behavior friction
- feature or interaction limits inside the BI layer
- rows framed as making dashboards or views behave the way users expect

#### Differentiating signals

- `persona_05` is more delivery-facing and more often tied to a stakeholder-visible output artifact
- `persona_05` more often keeps `manual_reporting` as the lead bottleneck
- `persona_05` examples are more often about trend tables, target-facing views, shared dashboard interpretation, or presentation-output construction

#### Assessment

`persona_05` adds a real nuance beyond `persona_03`, but most of its broader evidence pool still behaves like a delivery-specific submode of `persona_03`.

This is why a direct merge into `persona_03` is understandable, but still a little too aggressive for this decision-only pass.

### Persona 05 vs Persona 01

#### Shared signals

- `manual_reporting`
- analyst-owned recurring reporting work
- desire to make reporting workflows more reliable and repeatable

#### Differentiating signals

- `persona_05` is about the BI or report layer failing to express the final stakeholder-ready artifact
- `persona_01` is broader export churn, spreadsheet rework, and reconciliation burden
- `persona_05` is more layout, filter, interaction, and presentation-output specific

#### Assessment

`persona_01` can absorb some spillover rows, but it should not absorb the whole pattern. Doing so would flatten the specific last-mile output-construction nuance that makes the surviving clean `persona_05` examples worthwhile.

### Ambiguous-Row Review

Sampled `50` ambiguous rows.

Classification:

- `should_merge_into_persona03 = 9`
- `should_merge_into_persona01 = 0`
- `should_remain_persona05_future_candidate = 0`
- `should_suppress_as_noise = 6`
- `ambiguous_keep_unresolved = 35`

Interpretation:

The ambiguous pool is not secretly a strong reserve of clean `persona_05` evidence. It is mostly unresolved, with a noticeable drift toward `persona_03` and some straightforward support/platform noise.

### Persona03-Overlap Review

Sampled `30` overlap rows.

Classification:

- `true_persona03 = 13`
- `persona05_submode_of_persona03 = 0`
- `clean_persona05_misclassified = 1`
- `support_noise = 2`
- `ambiguous = 14`

Interpretation:

The overlap pool looks much more like `persona_03` plus some noise than like a missed bank of clean `persona_05` evidence.

### Clean Persona 05 Evidence

The `15` clean rows are enough to justify preserving the idea, but not enough to justify preserving the persona as a standalone deck-ready candidate.

The best way to preserve that signal is:

- keep the five good representative examples
- preserve the narrative as a named future subtheme
- stop treating it as if it is on the verge of becoming a fifth claim-eligible persona

### Option Simulation

#### A. Keep persona_05 blocked as-is

- counts unchanged
- evidence preserved
- reviewer clarity remains weak
- duplicate risk remains high

#### B. Merge persona_05 into persona_03

- counts unchanged
- reviewer clarity improves
- duplicate risk drops
- some delivery-specific nuance is lost

#### C. Merge persona_05 into persona_01

- counts unchanged
- reviewer clarity only modestly improves
- important output-construction nuance is lost
- manual-reporting framing becomes too broad

#### D. Suppress persona_05 as noisy tail

- counts unchanged
- highest clarity
- highest information loss
- overcorrects by discarding the clean examples

#### E. Keep persona_05 as future_candidate_subtheme

- counts unchanged
- clarity improves without pretending it is a fifth persona
- preserves the clean examples and narrative
- keeps overclaiming risk low

### Recommendation

Recommended decision:

- `keep_as_future_candidate_subtheme`

Why this is the best fit:

- the clean examples are real
- the standalone persona is still too muddy
- most confusion is with `persona_03`, not `persona_01`
- a future subtheme preserves the insight without overstating the persona structure

### Practical Interpretation

If this decision is approved later, `persona_05` should be treated as:

- a named subtheme closely related to `persona_03`
- not claim-eligible
- not production-ready
- not review-ready
- not part of the final usable count

This keeps the clean last-mile reporting-output pattern visible without asking the workbook to defend it as a standalone fifth persona.
