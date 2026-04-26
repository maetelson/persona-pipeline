"""Rerank Stack Overflow tag bundles with explicit anti-duplication pressure against existing core source families."""

# Stack Overflow Tag Bundle Rerank

## Purpose
This document reranks bounded Stack Overflow tag bundles for pilot planning with an explicit anti-duplication constraint.

The key change from the earlier Stack Overflow pilot spec is that bundle ranking now penalizes heavy overlap with already-dominant core representative source families, especially:

- `power_bi_community`
- `metabase_discussions`

The goal of Stack Overflow expansion is not just to find a collectible high-yield bundle. It is to find a bundle that can improve source balance and persona-core coverage without reinforcing already-concentrated evidence families.

## Existing Source-Family Overlap Analysis

Current strong source families:

- `power_bi_community`
  - `source_tier = core_representative_source`
  - `labeled_share_pct = 24.6`
  - `blended_influence_share_pct = 30.7`
  - dominant production-ready contribution across `persona_01`, `persona_02`, and `persona_03`
- `metabase_discussions`
  - `source_tier = core_representative_source`
  - `labeled_share_pct = 16.2`
  - `blended_influence_share_pct = 20.0`
  - strong recurring contribution across `persona_01`, `persona_02`, and `persona_03`

Implication:

- new Stack Overflow bundles should be penalized if they mostly extend Power BI or Metabase evidence rather than adding a new reporting-workflow surface
- supporting-validation expansion should improve diversity, not simply restate an already-strong BI platform from a second channel

Additional context:

- `stackoverflow` is already a `supporting_validation_source`
- it has stable access and meaningful volume
- but it also carries high developer-support bias and should be expanded only where the bundle is likely to improve representative coverage rather than generic debugging density

## Anti-Duplication Scoring Rule

A Stack Overflow tag bundle receives an overlap penalty when it:

- overlaps heavily with an existing core representative source family
- is likely to reinforce the same persona slices already dominated by `power_bi_community` or `metabase_discussions`
- is more likely to increase concentration than improve balance
- adds mostly syntax, formula, or implementation variants of an already-covered workflow

Recommended qualitative penalty levels:

- `high_overlap_penalty`
  - direct platform duplication of an existing core source family
  - likely to reinforce already-dominant persona evidence
- `medium_overlap_penalty`
  - partial workflow overlap, but with some meaningful new output or reporting nuance
- `low_overlap_penalty`
  - materially different reporting surface, stakeholder output mode, or workflow pain not already covered by the dominant core families

Recommended bundle evaluation formula:

- `total_adjusted_score`
  - start with:
    - reporting/workflow fit
    - expected yield
    - persona contribution across `persona_01` through `persona_04`
    - reverse-scored technical-debug noise
    - reverse-scored maintenance complexity
  - then apply:
    - overlap penalty with `power_bi_community`
    - overlap penalty with `metabase_discussions`
    - source-balance improvement bonus

Interpretation:

- a high-yield bundle should still be demoted if it mostly duplicates an already-dominant core source family
- a medium-yield bundle may rank higher if it broadens reporting-workflow coverage without deep duplication

## Reranked Bundle Table

### 1. `ssrs_reporting_services`
- reporting/workflow fit: `5`
- expected yield: `3`
- expected persona_01 contribution: `5`
- expected persona_02 contribution: `2`
- expected persona_03 contribution: `3`
- expected persona_04 contribution: `3`
- technical-debug noise risk: `3`
- overlap with existing core sources: `1`
- source-balance improvement potential: `5`
- maintenance complexity: `4`
- total adjusted score: `33`

Why it moved up:

- lower overlap with the two dominant core families
- stronger contribution to recurring reporting, export, pagination, layout, and scheduled-output burden
- more likely to add `persona_01` and output-construction-adjacent evidence that is not simply another BI dashboard thread

### 2. `tableau_dashboard_calculation`
- reporting/workflow fit: `5`
- expected yield: `3`
- expected persona_01 contribution: `3`
- expected persona_02 contribution: `2`
- expected persona_03 contribution: `5`
- expected persona_04 contribution: `4`
- technical-debug noise risk: `3`
- overlap with existing core sources: `2`
- source-balance improvement potential: `4`
- maintenance complexity: `4`
- total adjusted score: `31`

Why it ranks well:

- strong dashboard/reporting fit
- meaningful chance to broaden BI workflow evidence outside Power BI and Metabase
- still some overlap with existing generic BI/workaround evidence, but less concentration risk than another Power BI-heavy bundle

### 3. `business_intelligence_reporting`
- reporting/workflow fit: `4`
- expected yield: `3`
- expected persona_01 contribution: `3`
- expected persona_02 contribution: `3`
- expected persona_03 contribution: `3`
- expected persona_04 contribution: `3`
- technical-debug noise risk: `2`
- overlap with existing core sources: `2`
- source-balance improvement potential: `4`
- maintenance complexity: `3`
- total adjusted score: `28`

Why it stays in the middle:

- broader diversity potential than Power BI or Metabase-specific bundles
- but more likely to leak generic programming, weakly contextualized BI chatter, or thin recommendation content

### 4. `powerbi_dax_reporting`
- reporting/workflow fit: `5`
- expected yield: `5`
- expected persona_01 contribution: `3`
- expected persona_02 contribution: `2`
- expected persona_03 contribution: `5`
- expected persona_04 contribution: `4`
- technical-debug noise risk: `2`
- overlap with existing core sources: `5`
- source-balance improvement potential: `1`
- maintenance complexity: `4`
- total adjusted score: `26`

Why it was demoted:

- strongest direct overlap with `power_bi_community`
- high probability of reinforcing an already-dominant source/topic family
- likely to add DAX, measure, and filter-context debugging variants rather than new cross-source diversity

### 5. `data_visualization_reporting`
- reporting/workflow fit: `3`
- expected yield: `2`
- expected persona_01 contribution: `2`
- expected persona_02 contribution: `2`
- expected persona_03 contribution: `2`
- expected persona_04 contribution: `3`
- technical-debug noise risk: `2`
- overlap with existing core sources: `2`
- source-balance improvement potential: `3`
- maintenance complexity: `3`
- total adjusted score: `22`

Why it stays low:

- potentially broad enough to diversify
- but likely too noisy and too weakly tied to recurring reporting pain unless paired with very strict query logic

### 6. `metabase_reporting`
- reporting/workflow fit: `4`
- expected yield: `2`
- expected persona_01 contribution: `2`
- expected persona_02 contribution: `2`
- expected persona_03 contribution: `4`
- expected persona_04 contribution: `2`
- technical-debug noise risk: `3`
- overlap with existing core sources: `5`
- source-balance improvement potential: `1`
- maintenance complexity: `4`
- total adjusted score: `19`

Why it ranks near the bottom:

- direct duplication pressure against `metabase_discussions`
- weak diversity benefit
- risk of spending expansion effort to restate an already-strong core family

### 7. `looker_studio_reporting`
- reporting/workflow fit: `3`
- expected yield: `2`
- expected persona_01 contribution: `2`
- expected persona_02 contribution: `2`
- expected persona_03 contribution: `3`
- expected persona_04 contribution: `3`
- technical-debug noise risk: `1`
- overlap with existing core sources: `2`
- source-balance improvement potential: `3`
- maintenance complexity: `3`
- total adjusted score: `19`

Why it remains low:

- lower duplication risk than Power BI or Metabase
- but high risk of recreating the same Google-style operational/support noise already seen elsewhere

## Decision On `powerbi_dax_reporting`

### Does it add genuinely new evidence beyond `power_bi_community`?
Only partially.

It may add some new phrasing around:

- DAX totals logic
- measure behavior
- filter-context edge cases
- export or table-visual behavior

But most of that still sits very close to the same platform/problem family already strongly represented by `power_bi_community`.

### Would it improve or worsen source balance?
It would likely **worsen practical concentration pressure**.

Even if the rows come from Stack Overflow rather than the Power BI community, they would still reinforce the same dominant tool family rather than broadening the representative source mix.

### Is it likely to overproduce DAX syntax/debug noise?
Yes.

This is one of the highest-risk bundles for:

- formula debugging
- measure syntax
- filter-context implementation issues
- technical workaround threads with weak stakeholder/reporting context

### Should it be demoted from first pilot priority?
Yes.

It remains a plausible later bundle if broader Stack Overflow expansion proves valuable, but it should no longer be the first pilot because it does not satisfy the anti-duplication goal strongly enough.

## Selected Revised First Pilot Bundle
The revised first pilot bundle should be:

- `ssrs_reporting_services`

Why this bundle is the best next pilot:

- strongest balance-improvement potential among the current options
- relatively low overlap with `power_bi_community` and `metabase_discussions`
- likely to surface recurring reporting, scheduled delivery, export fidelity, layout, matrix, and stakeholder-output problems
- more likely to add `persona_01` and reporting-output evidence that complements the existing corpus rather than intensifying current concentration
- still collectible through the existing Stack Overflow collector family without opening a new source path

## Recommendation
Updated recommendation for the next implementation pass:

- implement a bounded Stack Overflow pilot runner
- first bundle:
  - `ssrs_reporting_services`
- reserve secondary follow-up bundles:
  - `tableau_dashboard_calculation`
  - `business_intelligence_reporting`

Do not lead with:

- `powerbi_dax_reporting`
- `metabase_reporting`

unless a later pass explicitly decides that within-family reinforcement is acceptable despite concentration pressure.

## Risks And Assumptions

### Risks
- `ssrs_reporting_services` may underperform on yield even if it is methodologically cleaner
- `tableau_dashboard_calculation` may eventually prove stronger if SSRS volume is too thin
- `business_intelligence_reporting` may be too broad and leak generic programming noise
- Stack Overflow as a whole still carries high technical-debug bias, so even the reranked first bundle could fail the quality gate

### Assumptions
- source-balance improvement is more important than maximizing raw Stack Overflow volume
- a medium-yield but less duplicative bundle is preferable to a high-yield Power BI bundle that increases concentration
- existing `power_bi_community` and `metabase_discussions` already provide strong enough evidence that same-family reinforcement should be penalized in pilot selection
