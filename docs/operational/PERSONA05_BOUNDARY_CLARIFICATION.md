## Persona 05 Boundary Clarification

### Summary

`persona_05` still looks like a real pattern, but its boundary is only moderately clear.

The strongest current interpretation remains:

- `persona_05` = last-mile reporting output construction blocked by tool limitations

This means the user already has the data and often already has a workable question or metric model, but the BI or reporting layer cannot cleanly produce the final stakeholder-ready artifact. The artifact can be a pivoted table, dashboard interaction, filter behavior, report layout, visual configuration, or other presentation-ready output.

The persona is therefore not blocked by missing source support or missing core anchors. It is blocked by boundary clarity.

### Current Working Hypothesis

- Core job / situation: an analyst or operations owner is trying to produce a stakeholder-facing report, dashboard, table, filter interaction, or shareable view.
- Pain: the last-mile output cannot be expressed cleanly because the BI tool cannot support the required layout, interaction, visual behavior, or report configuration.
- Desired output: a presentation-ready, stakeholder-usable deliverable without awkward workarounds.
- Different from `persona_01`: `persona_01` is broader recurring manual reporting, export churn, spreadsheet rework, and reconciliation burden. `persona_05` is narrower and more output-construction specific.
- Different from `persona_03`: `persona_03` is a cleaner generic tool-limitation workaround persona. `persona_05` keeps the limitation element but ties it to reporting delivery and stakeholder-facing output.

### Boundary Matrix

#### Persona 05 vs Persona 01

- Shared signals:
  - `manual_reporting`
  - report delivery pressure
  - analyst-led workflow ownership
  - desire for repeatable stakeholder reporting
- Differentiating signals:
  - final output shape, layout, or interaction is blocked inside the BI tool
  - `presentation_ready_output_need` is explicit
  - dashboard, filter, pivot, or visual behavior is part of the pain
  - stakeholder-facing usability matters more than export volume
- Overlapping rows:
  - `metabase_discussions::99555::01`
  - `hubspot_community::823169::01`
  - `sisense_community::21785::01`
- High-risk confusion cases:
  - `power_bi_community::4755354::01`
  - `stackoverflow::68635577::01`
  - `shopify_community::416748::01`
- Clean separation examples:
  - `github_discussions::4223807485::01`
  - `metabase_discussions::153908::01`
  - `github_discussions::866442257::01`
- Boundary strength: `moderate`

Interpretation:

`persona_05` is not just a tail of `persona_01`, but rows that talk about making reporting more reliable without clearly naming the blocked final output still slide back toward `persona_01`.

#### Persona 05 vs Persona 03

- Shared signals:
  - `tool_limitation_workaround`
  - BI-layer behavior blocks task completion
  - feature or interaction limitations inside the reporting tool
- Differentiating signals:
  - `manual_reporting` remains the dominant lead signal
  - the pain is tied to a stakeholder-facing report, dashboard, or shareable artifact
  - `presentation_ready_output_need` and `reporting_deadline_pressure` recur
  - the desired state is a deliverable output, not just feature parity
- Overlapping rows:
  - `github_discussions::1810542734::01`
  - `github_discussions::3517021486::01`
  - `metabase_discussions::109422::01`
- High-risk confusion cases:
  - `metabase_discussions::20967::01`
  - `metabase_discussions::20909::01`
  - `github_discussions::1199913352::01`
- Clean separation examples:
  - `metabase_discussions::99555::01`
  - `hubspot_community::823169::01`
  - `github_discussions::4223807485::01`
- Boundary strength: `moderate`

Interpretation:

`persona_05` is not just a tail of `persona_03`, but rows that are mostly generic feature limitation, API help, or configuration friction without clear delivery context still drift back toward `persona_03`.

### Reviewed Row Summary

Reviewed scope:

- 5 selected examples
- 20 additional central rows
- 15 boundary rows against `persona_01`
- 15 boundary rows against `persona_03`
- 10 weak or noisy rows

Classification summary:

- `clean_persona05 = 27`
- `persona01_overlap = 13`
- `persona03_overlap = 11`
- `support_troubleshooting_noise = 8`
- `weak_generic = 4`
- `ambiguous = 2`

Takeaway:

The reviewed set contains enough clean `persona_05` evidence to keep the persona alive, but not enough boundary clarity to make claim wording safe without an explicit rule.

### Positive Boundary Rules

Rows should count as `persona_05` evidence when they show:

- stakeholder-facing output construction
- final table, view, dashboard, or report layout pain
- filter or interaction behavior needed for delivery
- visual or report configuration blocking the final artifact
- presentation-ready or shareable dashboard or report need
- visible last-mile delivery pressure

### Negative Boundary Rules

Rows should not support `persona_05` claim wording when they are mainly:

- recurring export burden without final-output specificity
- spreadsheet rework alone
- generic tool limitation without reporting delivery context
- auth, connector, refresh, permission, setup, or support troubleshooting
- formula, API, or automation debugging without stakeholder-facing output context
- feature requests without concrete reporting delivery pain

### Scores

- `boundary_score_vs_persona_01 = 3 / 5`
- `boundary_score_vs_persona_03 = 3 / 5`
- `representative_example_strength = 4 / 5`
- `support_noise_risk = 3 / 5`
- `duplicate_risk = 3 / 5`
- `overall_boundary_readiness = borderline`

### Decision

Recommended next action:

- `needs_boundary_rule_implementation`

Smallest safe next step:

- Add a diagnostic boundary rule that requires both:
  - reporting-delivery context
  - a concrete last-mile output-construction blocker

Do not move directly to claim eligibility recheck yet.

### Why Not Recheck Now

The selected examples are now much better, which meaningfully improves reviewer understanding. But the remaining issue is no longer example quality alone. It is the rule for what should and should not count as `persona_05` evidence.

Until that rule is explicit, `persona_05` remains too easy to blur with:

- `persona_01` when the row is really broad manual reporting burden
- `persona_03` when the row is really generic tool-limitation friction
