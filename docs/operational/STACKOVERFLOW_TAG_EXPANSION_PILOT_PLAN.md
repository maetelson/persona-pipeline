"""Bounded Stack Overflow tag expansion pilot plan for representative-source testing."""

# Stack Overflow Tag Expansion Pilot Plan

## Purpose
This document defines a bounded pilot for expanding Stack Overflow / Stack Exchange collection with narrowly targeted BI, dashboard, and reporting-oriented tag bundles.

The pilot is intended to test whether carefully selected Stack Overflow tag bundles can produce representative persona-core evidence for `persona_01` through `persona_04` without overwhelming the corpus with pure technical debugging, SDK, deployment, or generic programming noise.

This is a planning-only document. It does not activate any production source, change any production config, or alter any workbook, persona, threshold, or source-tier semantics.

## Candidate Tag Strategy
The pilot should not rely on broad generic tags alone. Instead, it should combine:

1. BI / reporting tool tags
2. reporting-workflow query terms
3. strict negative-term scoring

Preferred strategy:

- use tool-specific tags with strong reporting/workflow adjacency
- prefer bundles that naturally surface dashboard, metric, reporting, export, layout, totals, and filter-context pain
- avoid broad engineering tags unless they are paired with reporting/dashboard/business-intelligence query terms

Preferred tag families:

- `powerbi`, `powerbi-desktop`, `dax`, `powerquery`
- `tableau-desktop`
- `reporting-services`, `ssrs`
- `business-intelligence`
- `dashboard`
- `data-visualization`
- `metabase` if stable tag yield exists
- `data-analysis` only when combined with reporting/dashboard/business-intelligence terms

## Inclusion Logic
Questions should be prioritized when they mention one or more of:

- `dashboard`
- `report`
- `reporting`
- `metric`
- `measure`
- `kpi`
- `visualization`
- `filter`
- `slicer`
- `pivot`
- `export`
- `spreadsheet`
- `table layout`
- `matrix`
- `calculated field`
- `total mismatch`
- `data mismatch`
- `aggregation`
- `stakeholder`
- `business reporting`
- `recurring report`
- `monthly report`
- `weekly report`

High-value combinations include:

- BI tool tag + report/dashboard/metric language
- BI tool tag + totals/filter/measure mismatch language
- BI tool tag + export/layout/stakeholder-output language
- reporting-services/ssrs tag + export/layout/pagination language

## Exclusion Logic
Questions should be excluded or heavily downweighted when dominated by:

- pure syntax errors
- package installation
- auth / permission setup
- API / SDK integration
- server deployment
- CI / CD
- database connection errors
- driver setup
- generic programming questions
- beginner homework
- career or interview topics
- course or tutorial requests
- thin feature recommendations with no reporting pain

Examples of likely negative markers:

- `syntax error`
- `nullpointerexception`
- `module not found`
- `oauth`
- `jwt`
- `api`
- `sdk`
- `docker`
- `kubernetes`
- `deployment`
- `connection string`
- `ssl`
- `interview`
- `resume`
- `homework`

## Proposed Pilot Tag Bundles

### A. `powerbi_dax_reporting`
- tags:
  - `powerbi`
  - `powerbi-desktop`
  - `dax`
  - `powerquery`
- query terms:
  - `dashboard`
  - `report`
  - `reporting`
  - `measure`
  - `metric`
  - `filter`
  - `slicer`
  - `total mismatch`
  - `aggregation`
  - `export`
- expected persona contribution:
  - strongest: `persona_03`
  - strong secondary: `persona_01`, `persona_04`
  - possible: `persona_02`
- expected noise risk:
  - medium
- expected yield:
  - high
- implementation complexity:
  - low

### B. `tableau_dashboard_calculation`
- tags:
  - `tableau-desktop`
- query terms:
  - `dashboard`
  - `calculated field`
  - `filter`
  - `totals`
  - `layout`
  - `export`
  - `table`
  - `measure`
- expected persona contribution:
  - strongest: `persona_03`
  - strong secondary: `persona_04`
  - possible: `persona_01`
- expected noise risk:
  - medium-low
- expected yield:
  - medium
- implementation complexity:
  - low

### C. `ssrs_reporting_services`
- tags:
  - `reporting-services`
  - `ssrs`
- query terms:
  - `report`
  - `layout`
  - `pagination`
  - `export`
  - `matrix`
  - `tablix`
  - `subscription`
  - `recurring report`
- expected persona contribution:
  - strongest: `persona_01`
  - strong secondary: `persona_05`
  - possible: `persona_03`
- expected noise risk:
  - medium
- expected yield:
  - medium
- implementation complexity:
  - low

### D. `business_intelligence_reporting`
- tags:
  - `business-intelligence`
  - `dashboard`
  - `data-visualization`
- query terms:
  - `reporting`
  - `dashboard`
  - `metric`
  - `kpi`
  - `stakeholder`
  - `business reporting`
  - `data mismatch`
- expected persona contribution:
  - broad but lower-confidence support across `persona_01` to `persona_04`
- expected noise risk:
  - medium-high
- expected yield:
  - medium
- implementation complexity:
  - medium

### E. `metabase_reporting`
- tags:
  - `metabase`
- query terms:
  - `dashboard`
  - `report`
  - `metric`
  - `filter`
  - `totals`
  - `export`
- expected persona contribution:
  - strongest: `persona_03`
  - possible: `persona_01`, `persona_04`
- expected noise risk:
  - medium
- expected yield:
  - unknown to low-medium
- implementation complexity:
  - low

## Selected First Pilot Bundle
The first pilot bundle should be:

- `powerbi_dax_reporting`

Why this bundle is the best first pilot:

- highest likely yield among bounded BI/reporting bundles
- strong overlap with reporting workflow, measures, filters, totals, and dashboard pain
- best expected contribution to `persona_01`, `persona_03`, and `persona_04`
- compatible with the existing Stack Overflow collector family and existing seed-bank patterns
- easier to score tightly than broad `business-intelligence` or `dashboard` tags

## Pilot Scope

- target question count:
  - `500` to `1000` questions if available
- time window:
  - recent multi-year window consistent with current Stack Overflow corpus strategy
- answer policy:
  - do not include all answers/comments by default
  - include accepted answer or top answer only if already supported cleanly by the collector path and only if it materially improves context
- fetch stability:
  - prefer existing Stack Exchange API path already used in the repository
- output location:
  - pilot-only artifacts under `artifacts/source_pilots/`

## Output Artifacts

- `artifacts/source_pilots/stackoverflow_tag_expansion_raw.jsonl`
- `artifacts/source_pilots/stackoverflow_tag_expansion_sample.csv`
- `artifacts/source_pilots/stackoverflow_tag_expansion_summary.json`
- `artifacts/readiness/stackoverflow_tag_expansion_pilot_spec.json`

## Row Schema
Each pilot row should include:

- `source = stackoverflow_tag_expansion`
- `raw_id`
- `url`
- `title`
- `body_or_excerpt`
- `tags`
- `score`
- `answer_count`
- `accepted_answer_flag`
- `created_at`
- `updated_at`
- `bundle_name`
- `fetch_method`
- `fetched_at`

## Automated Scoring Signals

### Positive signals
- `reporting_workflow_signal`
- `dashboard_metric_signal`
- `bi_tool_signal`
- `calculation_measure_signal`
- `filter_slicer_signal`
- `export_spreadsheet_signal`
- `validation_reconciliation_signal`
- `visualization_layout_signal`
- `stakeholder_output_signal`

### Negative signals
- `pure_syntax_debug_noise`
- `install_setup_noise`
- `auth_permission_noise`
- `api_sdk_noise`
- `database_connection_noise`
- `server_deploy_noise`
- `homework_beginner_noise`
- `generic_programming_noise`

## Persona Fit Estimation
Each row should receive pilot-only weak/medium/strong fit estimates for:

- `persona_01`
  - manual reporting / export / spreadsheet / recurring reporting burden
- `persona_02`
  - KPI explanation / root-cause / insight handoff
- `persona_03`
  - tool limitation / workflow workaround
- `persona_04`
  - metric validation / dashboard mistrust / number reconciliation
- `persona_05`
  - last-mile reporting output construction

## Onboarding Gate
The Stack Overflow tag expansion pilot should pass only if:

- `valid_candidate_ratio >= 0.35`
- `estimated_labelable_ratio >= 0.60`
  - or clearly better than prior failed pilots with strong evidence and low noise concentration
- `estimated_persona_core_candidate_count >= 120`
- at least two of `persona_01` through `persona_04` show meaningful signal
- pure technical debug noise is not dominant
- `estimated_weak_source_risk` is not high
- access is stable and reproducible

Recommended interpretation notes:

- Stack Overflow must outperform the failed Reddit and GitHub Discussions routes on quality ratio, not only on volume
- yield alone is not enough if the bundle behaves like another technical-support cost center

## Decision Outcomes
The pilot should choose exactly one of:

- `implement_stackoverflow_pilot_runner`
- `narrow_tag_bundle`
- `try_next_tag_bundle`
- `reject_stackoverflow_expansion`
- `stop_source_expansion`

## Implementation Recommendation
Recommendation for the next pass:

- `Go` for implementing a bounded Stack Overflow pilot runner

Recommended next-pass scope:

- reuse the existing Stack Overflow collector family
- add a pilot-only tag bundle config
- add a bounded pilot runner under `run/diagnostics/`
- write outputs only to `artifacts/source_pilots/`
- do not alter production source configs or active source activation

## Risks And Assumptions

### Risks
- Stack Overflow may still overproduce DAX or formula-debug noise even inside a narrow BI bundle
- high-yield tags can look promising on volume while still failing persona-core quality
- `powerbi_dax_reporting` may bias toward `persona_03` unless scoring strongly rewards reporting output and validation pain
- broad dashboard/business-intelligence tags can leak generic programming or recommendation chatter

### Assumptions
- existing Stack Overflow collector access remains stable and reproducible
- the repository already has enough Stack Overflow collection scaffolding to support a bounded pilot without introducing a new collector family
- a tightly constrained tag/query bundle is more likely to succeed than broad source-family expansion
