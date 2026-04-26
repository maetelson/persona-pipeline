## Persona 05 Subtheme Preservation Spec

### Purpose

This spec defines how `persona_05` should be preserved after the merge / suppression review without continuing to treat it as a standalone deck-ready persona candidate.

This is a documentation-only specification.

It does not:

- change clustering
- change persona assignment
- change promotion logic
- change review-ready logic
- change deck-ready claim eligibility logic
- change workbook thresholds
- change readiness calculation
- change XLSX presentation
- change `final_usable_persona` semantics

### Current Decision Context

Current decision outcome:

- `persona_05` should not remain on a standalone persona path
- `persona_05` should not be discarded as pure noise
- recommended decision: `keep_as_future_candidate_subtheme`

Current evidence posture:

- total rows: `539`
- clean `persona_05` evidence: `15`
- ambiguous: `269`
- `persona03_overlap`: `130`
- `persona01_overlap`: `1`
- support noise: `88`
- weak generic: `36`
- selected examples: `5`

Interpretation:

- the clean examples are real
- the standalone persona is too muddy
- most confusion is with `persona_03`, not `persona_01`

### Persona 05 Subtheme Interpretation

Recommended interpretation:

- `persona_05` is a `future_candidate_subtheme`
- it captures **last-mile reporting output construction blocked by tool limitations**
- it is most closely related to `persona_03`
- it should remain:
  - named
  - blocked
  - non-claim-eligible
  - non-production
  - outside final usable count

This preserves the best `persona_05` insight without asking the workbook to defend it as a fifth standalone persona.

### Relationship To Persona 03

#### Persona 03

`persona_03` remains the broader generic tool-limitation workaround persona.

It captures:

- BI or analytics tools failing to support the needed behavior
- workaround-oriented frustration
- feature, filter, interaction, and tool behavior limits

#### Persona 05

`persona_05` is narrower and more delivery-specific.

It captures:

- users already have the data
- they are blocked at the last mile of reporting delivery
- the blocked artifact is stakeholder-facing:
  - report
  - dashboard
  - table
  - filter behavior
  - visual layout
  - presentation-ready output

#### Why Persona 05 Should Not Stand Alone Yet

`persona_05` is not strong enough to stand alone because:

- clean evidence is too small
- overlap with `persona_03` is still large
- support and ambiguity remain too high
- current evidence is better represented as a named subtheme than as a separate claimable persona

### Relationship To Persona 01

#### Persona 01

`persona_01` remains the broad manual reporting / export / spreadsheet rework burden persona.

It captures:

- recurring export work
- spreadsheet and Excel rework
- reconciliation burden
- broad manual analyst labor

#### Why Persona 05 Should Not Be Flattened Into Persona 01

`persona_05` should not be absorbed into `persona_01` because that would erase the delivery/output nuance:

- the core pain is not only manual work
- the tool layer cannot express the final stakeholder-ready artifact cleanly
- the pain is more about final output construction than about raw export churn

### Output Semantics

Future implementation should add subtheme-preservation fields rather than changing readiness or promotion fields.

Recommended fields:

- `subtheme_status`
- `parent_persona_id`
- `parent_persona_relation`
- `future_candidate_subtheme`
- `subtheme_reason`
- `standalone_persona_recommended`
- `claim_eligible_recommended`
- `related_subtheme_ids`

Field intent:

- `subtheme_status`: declares whether a row is a preserved subtheme, not a standalone persona candidate
- `parent_persona_id`: identifies the closest parent persona
- `parent_persona_relation`: explains the relationship in plain language
- `future_candidate_subtheme`: simple boolean for reviewer-facing interpretation
- `subtheme_reason`: concise explanation of why the subtheme is preserved
- `standalone_persona_recommended`: indicates whether a standalone persona path is still recommended
- `claim_eligible_recommended`: indicates whether the subtheme should be considered for claim wording
- `related_subtheme_ids`: optional parent-side visibility for linked subthemes

### Expected Values For Persona 05

Recommended values:

- `subtheme_status = future_candidate_subtheme`
- `parent_persona_id = persona_03`
- `parent_persona_relation = delivery_specific_subtheme`
- `future_candidate_subtheme = True`
- `subtheme_reason = Last-mile reporting output construction blocked by tool limitations is real, but current evidence is too overlap-heavy and too thin for standalone persona treatment.`
- `standalone_persona_recommended = False`
- `claim_eligible_recommended = False`

Current persona state should remain unchanged:

- `production_ready_persona = False`
- `review_ready_persona = False`
- `final_usable_persona = False`
- `deck_ready_claim_eligible_persona = False`
- `readiness_tier = blocked_or_constrained_candidate`

### Expected Values For Other Personas

The preservation spec must not accidentally reclassify current stable personas.

Expected values:

- `persona_01`: not a subtheme
- `persona_02`: not a subtheme
- `persona_03`: not a subtheme
- `persona_04`: not a subtheme

Optional parent-side linkage:

- `persona_03.related_subtheme_ids` may include `persona_05`

Guardrails:

- no other persona should become a subtheme by default
- `persona_03` remains production-ready and claim-eligible
- `persona_05` remains visible but clearly non-final

### Affected Outputs For Future Implementation

If approved, future implementation should touch only interpretation-facing outputs first:

- `persona_summary.csv`
- `cluster_stats.csv`
- `persona_promotion_path_debug.csv`
- `persona_examples.csv` if parent/subtheme relationship is helpful there
- final XLSX `persona_summary` sheet
- workbook `readme` sheet wording

Implementation should avoid changing:

- clustering outputs
- assignment outputs
- readiness calculations
- production-ready counts
- final usable counts
- claim-eligible counts

### Implementation Plan

Recommended implementation sequence:

1. Add subtheme-preservation fields to analysis outputs only.
2. Populate `persona_05` with the expected preserved-subtheme values.
3. Optionally add `related_subtheme_ids` to `persona_03`.
4. Keep all existing readiness, promotion, and claim fields unchanged.
5. Update workbook presentation wording later so reviewers can see that `persona_05` is preserved as a subtheme, not a fifth final persona.

### Required Tests Before Implementation

Tests should verify:

- `persona_05` is marked `future_candidate_subtheme`
- `persona_05.parent_persona_id = persona_03`
- `persona_05.parent_persona_relation = delivery_specific_subtheme`
- `persona_05.future_candidate_subtheme = True`
- `persona_05.standalone_persona_recommended = False`
- `persona_05.claim_eligible_recommended = False`
- `persona_05` remains not production-ready
- `persona_05` remains not review-ready
- `persona_05` remains not final usable
- `persona_05` remains not deck-ready claim-eligible
- `persona_03` remains production-ready and claim-eligible
- no other persona is accidentally turned into a subtheme
- persona counts do not change
- `final_usable_persona_count` stays `3`
- `deck_ready_claim_eligible_persona_count` stays `4`

### Risks Before Implementation

Main risks:

- reviewers may misread `future_candidate_subtheme` as a hidden promotion path
- parent/subtheme wording could blur the distinction between `persona_03` and `persona_05` if phrased loosely
- subtheme visibility could accidentally create a fifth-persona impression in presentation surfaces
- adding parent linkage in the wrong place could look like a silent merge even when no merge has happened

### Recommended Outcome

Recommended policy outcome:

- preserve `persona_05` as a named future-candidate subtheme
- link it explicitly to `persona_03`
- keep it blocked and non-claim-eligible
- do not treat it as a standalone fifth deck-ready persona unless future evidence materially improves
