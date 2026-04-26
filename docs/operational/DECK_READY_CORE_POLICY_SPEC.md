# Deck-Ready Core Policy Spec

## Status

- Spec only.
- No production policy change is implemented by this document.
- No thresholds, source configs, clustering rules, labeling rules, promotion logic, workbook semantics, or `final_usable_persona` semantics are changed here.

## Phase 1 Implementation Note

Phase 1 of this policy is limited to workbook-facing source-tier annotation only.

- `source_balance_audit.csv`, `source_diagnostics.csv`, and `source_distribution.csv` may carry deterministic source-tier fields.
- `overview.csv` may carry aggregate source-tier counts.
- No readiness state, persona count, weak-source denominator behavior, or XLSX presentation semantics change in Phase 1.

## Purpose

Define a transparent deck-ready core policy that separates:

1. the broader `reviewable_release_corpus`
2. `deck_ready_core_evidence`
3. `supporting_validation_sources`
4. `exploratory_edge_sources`
5. `raw_archive_only_sources`

The goal is to make deck-ready claims methodologically honest without deleting raw data, hiding diagnostics, or pretending that weak or supporting sources are core-representative.

## Current Problem Definition

The current workbook is stable and valid as a reviewable release, but not as a deck-ready artifact.

Current stable baseline:

- `persona_readiness_state = reviewable_but_not_deck_ready`
- `overall_status = WARN`
- `quality_flag = EXPLORATORY`
- `final_usable_persona_count = 3`
- `production_ready_persona_count = 3`
- `review_ready_persona_count = 1`
- `effective_balanced_source_count = 5.89`
- `persona_core_coverage_of_all_labeled_pct = 74.5`
- `core_readiness_weak_source_cost_center_count = 3`

What we learned from the last deck-ready attempts:

- The reviewable release can legitimately use the broad corpus because it is an analyst-facing draft state with visible caveats.
- Deck-ready claims need stricter source interpretation because those claims are stronger than reviewable workbook claims.
- Broad deletion of `adobe_analytics_community`, `google_developer_forums`, and `domo_community_forum` made the corpus cleaner but dropped `effective_balanced_source_count` to `4.86` and regressed readiness back to `exploratory_only`.
- Adobe remediation ended as a no-op because no safe Adobe-only parser or episode-fidelity fix was found.
- Google remediation ended as a no-op because the remaining false-negative tail was too mixed with noise for another safe source-specific rescue.
- Domo is currently best understood as exploratory edge, but still contributes to source balance in the active reviewable corpus.

Conclusion:

- The next step should not be another blind remediation loop.
- The next step should not be broad source deletion.
- The missing layer is a source-tier policy that distinguishes deck-ready core evidence from the broader reviewable corpus.

## Source Membership Layers

### `reviewable_release_corpus`

The full active corpus used to produce the reviewable workbook. This layer prioritizes breadth, analyst visibility, and transparency over deck-ready methodological strictness.

### `deck_ready_core_evidence`

The subset of sources allowed to anchor deck-ready persona claims. This layer is stricter about representativeness and should define headline deck-ready evidence.

### `supporting_validation_sources`

Sources that may strengthen, triangulate, or pressure-test deck-ready claims, but should not anchor those claims by themselves.

### `exploratory_edge_sources`

Sources that remain useful for discovery, edge-case pattern finding, or corpus breadth, but should not determine deck-ready headline interpretation.

### `raw_archive_only_sources`

Sources retained only for reproducibility or historical reconstruction. They do not participate in current deck-ready or reviewable interpretation unless explicitly reactivated by policy.

## Source Tier Definitions

This spec uses the following tier names:

- `core_representative_source`
- `supporting_validation_source`
- `exploratory_edge_source`
- `excluded_from_deck_ready_core`
- `raw_archive_only`

### `core_representative_source`

Strongly aligned with target users, recurring BI/analytics interpretation pain, and post-dashboard decision workflows. These sources may anchor deck-ready persona claims.

### `supporting_validation_source`

Useful, often high-value, but too vendor-specific, support-heavy, or platform-bound to anchor deck-ready claims alone. These sources may support or triangulate deck-ready claims.

### `exploratory_edge_source`

Methodologically weaker for deck-ready claims, but still useful for exploration and reviewable breadth. These sources stay visible and may remain active in the reviewable corpus.

### `excluded_from_deck_ready_core`

Visible for reviewable interpretation or diagnostics, but not valid inputs for deck-ready core evidence. These sources should not anchor deck-ready source-balance or deck-ready core-coverage arguments.

### `raw_archive_only`

Retained only for reproducibility. Not active in deck-ready core, not active in reviewable interpretation, and not active in the normal pipeline unless deliberately reinstated.

## What Each Tier Can And Cannot Do

| Tier | Can anchor deck-ready persona claims | Can provide supporting evidence | Can appear in reviewable workbook | Can appear in source diagnostics | Can affect deck-ready source balance | Can affect reviewable readiness | Should be retained in raw archive | Requires remediation before core use |
|---|---|---|---|---|---|---|---|---|
| `core_representative_source` | Yes | Yes | Yes | Yes | Yes | Yes | Yes | No |
| `supporting_validation_source` | No | Yes | Yes | Yes | Conditional | Yes | Yes | Yes |
| `exploratory_edge_source` | No | Conditional | Yes | Yes | No | Conditional | Yes | Yes |
| `excluded_from_deck_ready_core` | No | Conditional | Yes | Yes | No | Conditional | Yes | Yes |
| `raw_archive_only` | No | No | No | Conditional | No | No | Yes | Yes |

Interpretation notes:

- `Conditional` under deck-ready source balance means the source can remain visible and counted for reviewable breadth, but should not be allowed to artificially rescue deck-ready core balance if it is not core-representative.
- `Conditional` under reviewable readiness means the source can remain visible and active in the broader workbook, but the policy should state clearly whether it contributes to reviewable readiness or only to reviewable diagnostics.
- `Requires remediation before core use = Yes` means the source cannot be treated as deck-ready core evidence without either source-quality improvement or an explicit policy decision that its current methodological fit is sufficient.

## Current Source Classification Using Live Artifacts

| Source | Recommended tier | Keep in reviewable release corpus | Keep in deck-ready core evidence | Keep in supporting validation | Keep in exploratory | Keep in raw archive | Remediation required before core claim | Reason |
|---|---|---|---|---|---|---|---|---|
| `power_bi_community` | `core_representative_source` | Yes | Yes | No | No | Yes | No | Strong target fit, strong workflow fit, high-quality evidence, and high removal risk to persona structure. |
| `metabase_discussions` | `core_representative_source` | Yes | Yes | No | No | Yes | No | Strong target fit, strong workflow fit, low noise, and high structural importance. |
| `github_discussions` | `supporting_validation_source` | Yes | No | Yes | No | Yes | No | Strong evidence value, but too developer-support-heavy to anchor deck-ready persona claims alone. |
| `hubspot_community` | `supporting_validation_source` | Yes | No | Yes | No | Yes | No | Strong workflow fit, but highly vendor-specific and better used for triangulation than anchoring. |
| `reddit` | `supporting_validation_source` | Yes | No | Yes | No | Yes | No | Useful cross-context evidence with low vendor bias, but not stable enough to anchor deck-ready claims by itself. |
| `stackoverflow` | `supporting_validation_source` | Yes | No | Yes | No | Yes | No | Valuable supporting signal, but high developer-support bias makes it a poor deck-ready anchor. |
| `shopify_community` | `supporting_validation_source` | Yes | No | Yes | No | Yes | No | Strong workflow fit with vendor specificity; better as supporting validation. |
| `sisense_community` | `supporting_validation_source` | Yes | No | Yes | No | Yes | No | Good evidence quality and strong workflow fit, but too platform-specific for core anchoring. |
| `mixpanel_community` | `exploratory_edge_source` | Yes | No | No | Yes | Yes | Yes | Small-volume platform-specific source with limited core necessity. |
| `qlik_community` | `exploratory_edge_source` | Yes | No | No | Yes | Yes | Yes | Platform-specific, moderate semantic fit, but low necessity and weaker core value. |
| `klaviyo_community` | `excluded_from_deck_ready_core` | Yes | No | No | Yes | Yes | Yes | Already behaves like exploratory-only weak-source debt and should not anchor deck-ready interpretation. |
| `adobe_analytics_community` | `supporting_validation_source` | Yes | No | Yes | No | Yes | Yes | High workflow fit, high uniqueness, and high removal risk, but not core-representative enough to anchor deck-ready claims. |
| `google_developer_forums` | `supporting_validation_source` | Yes | No | Yes | No | Yes | Yes | Meaningful unique evidence remains, but strong developer-support bias makes it unsuitable as a core deck-ready anchor. |
| `domo_community_forum` | `exploratory_edge_source` | Yes | No | No | Yes | Yes | Yes | Useful for breadth and balance, but not strong enough methodologically to anchor deck-ready claims. |

## Explicit Judgments For The Questioned Sources

### Adobe

- Adobe is not core-representative.
- Adobe has real supporting-validation value.
- Adobe has high uniqueness and high removal risk.
- Adobe may remain active in the reviewable corpus and visible in diagnostics.
- Adobe should not anchor deck-ready headline claims without a future evidence-based policy change.

### Google Developer Forums

- Google is not core-representative.
- Google has supporting-validation value and some unique evidence.
- Google has strong developer-support bias.
- Google may remain active in the reviewable corpus and visible in diagnostics.
- Google should not anchor deck-ready headline claims.

### Domo

- Domo is exploratory edge.
- Domo should not anchor deck-ready claims.
- Domo may still help the broader reviewable corpus maintain breadth and source balance.
- Domo should remain visible rather than silently excluded.

### Klaviyo

- Klaviyo should not be part of deck-ready core evidence.
- Klaviyo may remain visible in reviewable, exploratory, and diagnostic views.
- Klaviyo should remain retained in raw/archive form for reproducibility.

## Deck-Ready Readiness Interpretation Under The New Policy

### Core principle

Deck-ready should not be judged on the undifferentiated all-active-source corpus.

### Proposed rule

Deck-ready should be judged on:

1. `deck_ready_core_evidence` as the anchor layer for headline persona claims
2. `supporting_validation_sources` as corroborating evidence that can strengthen confidence without redefining the deck-ready core
3. `exploratory_edge_sources` as visible but non-anchoring context

### Implications

- Supporting-validation sources can strengthen claims, stress-test claims, or provide additional examples.
- Supporting-validation sources should not by themselves rescue deck-ready source balance or be used as the primary denominator for deck-ready core coverage.
- Exploratory-edge sources remain visible in diagnostics and reviewable views, but should not block deck-ready core claims if they are not part of deck-ready core evidence.
- Weak-source diagnostics must stay visible in all cases. The policy cannot hide them or erase them from audit outputs.
- Raw data remains retained even when a source is not part of deck-ready core evidence.

### Source balance interpretation

Under this policy, source balance should be interpreted in two layers:

1. `reviewable_release_corpus` balance
2. `deck_ready_core_evidence` balance

That avoids the current trap where non-core-representative sources are simultaneously:

- too weak to support deck-ready claims honestly
- but still necessary to keep all-active-source balance from collapsing

### Weak-source visibility rule

Weak-source diagnostics should continue to show:

- all active weak sources
- which sources are core-readiness weak sources
- which sources are supporting validation only
- which sources are exploratory or excluded from deck-ready core

The policy must not let deck-ready interpretation look cleaner by hiding those weak-source facts.

## Policy Options Comparison

### A. Strict all-active-source deck-ready policy

- Methodological strength: medium
- Source balance risk: low
- Implementation complexity: low
- Transparency: high
- Cherry-picking risk: low
- Plausibly reaches deck-ready: low
- Why not choose it: it keeps deck-ready hostage to non-core-representative active sources.

### B. Deck-ready core-only policy

- Methodological strength: high
- Source balance risk: high
- Implementation complexity: medium
- Transparency: medium
- Cherry-picking risk: high
- Plausibly reaches deck-ready: medium
- Why not choose it: too easy to look like cherry-picking if supporting and exploratory evidence are completely sidelined.

### C. Two-layer policy: core representative evidence plus supporting validation

- Methodological strength: high
- Source balance risk: medium
- Implementation complexity: medium-high
- Transparency: high
- Cherry-picking risk: medium
- Plausibly reaches deck-ready: highest among current options
- Why choose it: preserves transparency, retains breadth, keeps weak sources visible, and separates core anchoring from supporting triangulation.

### D. Freeze at reviewable release

- Methodological strength: medium
- Source balance risk: none
- Implementation complexity: low
- Transparency: high
- Cherry-picking risk: low
- Plausibly reaches deck-ready: no
- Why not choose it: safe operationally, but does not advance the deck-ready goal.

## Recommended Policy Path

Recommended path:

- **two-layer deck-ready policy**

Design summary:

- `core_representative_source` anchors deck-ready persona claims
- `supporting_validation_source` strengthens or triangulates claims but does not anchor them
- `exploratory_edge_source` remains visible but does not anchor deck-ready claims
- raw data is retained for reproducibility
- weak-source diagnostics remain visible

Why this path:

- It is methodologically stronger than all-active-source interpretation.
- It is more transparent than a pure core-only policy.
- It avoids repeating the failed pattern of broad deletion.
- It gives future deck-ready work a stable interpretation target before more source additions or source-quality work.

## Implementation Sequence If This Spec Is Approved

### Phase 1. Add source-tier fields to workbook-facing source outputs

- Add tier and layer fields to source-facing analysis outputs.
- Keep raw and diagnostics visibility intact.

### Phase 2. Add deck-ready core vs supporting evidence counts

- Separate deck-ready core evidence counts from reviewable corpus counts.
- Keep current reviewable metrics available side by side.

### Phase 3. Update readiness interpretation without changing persona quality thresholds

- Apply the new layer interpretation to deck-ready readiness evaluation.
- Do not change promotion thresholds or persona-quality standards.

### Phase 4. Update XLSX and README presentation

- Make the core-vs-supporting distinction explicit in workbook-facing explanations.
- Preserve full weak-source visibility.

### Phase 5. Final validation

- Re-run the full workbook validation path.
- Confirm no persona counts or readiness semantics changed unexpectedly.

## Required Test Plan Before Implementation

Tests should verify:

- no raw data deletion occurs
- source diagnostics still include all active or archived sources as appropriate
- source tier fields are deterministic
- `adobe_analytics_community` and `google_developer_forums` are supporting validation, not core representative
- `domo_community_forum` is exploratory edge
- `klaviyo_community` is excluded from deck-ready core
- production-ready and review-ready persona counts do not change due to tier labeling alone
- deck-ready core counts are separate from reviewable corpus counts
- weak-source diagnostics remain visible
- supporting-validation sources can be shown as corroborating evidence without anchoring headline deck-ready claims

## Risks Before Implementation

- If the policy is implemented carelessly, it could look like cherry-picking rather than transparent methodological narrowing.
- If deck-ready core balance is computed incorrectly, the workbook could hide genuine concentration risk.
- If supporting-validation sources are allowed to count as core implicitly, the new layer would be semantic only and would not solve the current mismatch.
- If exploratory-edge sources are suppressed from diagnostics, transparency would regress.
- If deck-ready core is made too narrow, persona structure could become fragile even if methodological purity improves.

## Non-Goals

- This spec does not approve source deletion.
- This spec does not relax thresholds.
- This spec does not redefine `final_usable_persona`.
- This spec does not change current reviewable release semantics.
