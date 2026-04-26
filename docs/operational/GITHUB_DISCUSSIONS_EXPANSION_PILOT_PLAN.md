# GITHUB_DISCUSSIONS_EXPANSION_PILOT_PLAN

## Purpose

Design a bounded GitHub Discussions expansion pilot that reuses the existing `github_discussions` collector family and tests whether a narrow repository bundle can improve:
- source balance
- persona-core coverage
- evidence for `persona_01` through `persona_04`

This pass is spec-only. It does not:
- activate new production sources
- mutate production configs
- change clustering, labeling, thresholds, workbook policy, source tiers, or persona semantics

## Candidate repository criteria

Prioritize repositories that are:
- analytics, BI, dashboard, reporting, semantic-layer, or product-analytics tools
- used by analysts, operators, product teams, data teams, or RevOps-like workflow owners
- likely to contain discussion about dashboards, reports, metrics, drill-down, metric trust, exports, reporting delivery, or explanation friction
- public and accessible through GitHub Discussions
- active enough to yield meaningful public discussion volume

High-value repository traits:
- user-facing discussion categories like `Q&A`, `Ideas`, `General`, or `Help`
- evidence of workflow complaints rather than only code patches
- enough thread volume to support a bounded `500` to `1000` discussion pilot

## Repositories to avoid or downweight

Avoid or sharply downweight repositories dominated by:
- pure library debugging
- installation or deployment support only
- generic feature requests with no user pain context
- CI/CD or infrastructure issues
- authentication or permission troubleshooting
- beginner coding help
- low-volume discussions

## Candidate repo shortlist

### `apache/superset`

- `discussions_enabled`: yes
- `estimated_discussion_volume`: high
- `likely_persona_01_contribution`: medium
- `likely_persona_02_contribution`: medium
- `likely_persona_03_contribution`: strong
- `likely_persona_04_contribution`: medium
- `developer_support_noise_risk`: medium
- `implementation_difficulty`: low
- `expected_source_tier`: strong_supporting_validation_source_candidate
- `pilot_priority`: P1

Why it belongs:
- strong BI/reporting tool fit
- public Discussions confirmed
- likely to surface drill-down limits, totals confusion, semantic pain, and reporting workflow friction

### `lightdash/lightdash`

- `discussions_enabled`: yes
- `estimated_discussion_volume`: medium-high
- `likely_persona_01_contribution`: medium
- `likely_persona_02_contribution`: medium
- `likely_persona_03_contribution`: strong
- `likely_persona_04_contribution`: medium
- `developer_support_noise_risk`: medium
- `implementation_difficulty`: low
- `expected_source_tier`: strong_supporting_validation_source_candidate
- `pilot_priority`: P1

Why it belongs:
- semantic-layer and dashboard workflow fit
- product discussions are likely to include report usability, modeling, and stakeholder follow-up pain

### `evidence-dev/evidence`

- `discussions_enabled`: yes
- `estimated_discussion_volume`: medium
- `likely_persona_01_contribution`: medium
- `likely_persona_02_contribution`: medium
- `likely_persona_03_contribution`: medium-strong
- `likely_persona_04_contribution`: low-medium
- `developer_support_noise_risk`: medium
- `implementation_difficulty`: low
- `expected_source_tier`: supporting_validation_source_candidate
- `pilot_priority`: P1

Why it belongs:
- report and presentation workflow fit
- likely to surface stakeholder-facing reporting needs and workflow limitations around analytics communication

### `plausible/analytics`

- `discussions_enabled`: yes
- `estimated_discussion_volume`: medium
- `likely_persona_01_contribution`: low-medium
- `likely_persona_02_contribution`: medium
- `likely_persona_03_contribution`: medium
- `likely_persona_04_contribution`: strong
- `developer_support_noise_risk`: medium-low
- `implementation_difficulty`: low
- `expected_source_tier`: supporting_validation_source_candidate
- `pilot_priority`: P1

Why it belongs:
- strong fit for metric trust, attribution interpretation, and dashboard confidence pain
- likely to contribute to `persona_04` more directly than broader engineering repos

### `getredash/redash`

- `discussions_enabled`: yes
- `estimated_discussion_volume`: medium-low
- `likely_persona_01_contribution`: medium
- `likely_persona_02_contribution`: low-medium
- `likely_persona_03_contribution`: medium
- `likely_persona_04_contribution`: medium
- `developer_support_noise_risk`: medium
- `implementation_difficulty`: low
- `expected_source_tier`: supporting_validation_source_candidate
- `pilot_priority`: P2

Why it is backup rather than core bundle:
- good reporting-tool fit, but likely lower recent activity and higher maintenance-style support mix

### `grafana/grafana`

- `discussions_enabled`: yes
- `estimated_discussion_volume`: high
- `likely_persona_01_contribution`: low
- `likely_persona_02_contribution`: low-medium
- `likely_persona_03_contribution`: medium
- `likely_persona_04_contribution`: low-medium
- `developer_support_noise_risk`: high
- `implementation_difficulty`: low
- `expected_source_tier`: low_value_supporting_source_candidate
- `pilot_priority`: P3

Why it is not in the first pilot bundle:
- likely too much platform, permissions, observability, and operational support noise relative to BI/reporting persona goals

### `dbt-labs/dbt-core`

- `discussions_enabled`: yes
- `estimated_discussion_volume`: high
- `likely_persona_01_contribution`: low
- `likely_persona_02_contribution`: medium
- `likely_persona_03_contribution`: medium
- `likely_persona_04_contribution`: low-medium
- `developer_support_noise_risk`: high
- `implementation_difficulty`: low
- `expected_source_tier`: low_value_supporting_source_candidate
- `pilot_priority`: P3

Why it is not in the first pilot bundle:
- too much modeling and implementation detail
- higher chance of library and engineering support noise than reporting workflow pain

### `metabase/metabase`

- `discussions_enabled`: no
- `estimated_discussion_volume`: n/a
- `likely_persona_01_contribution`: n/a
- `likely_persona_02_contribution`: n/a
- `likely_persona_03_contribution`: n/a
- `likely_persona_04_contribution`: n/a
- `developer_support_noise_risk`: n/a
- `implementation_difficulty`: n/a
- `expected_source_tier`: n/a
- `pilot_priority`: excluded

Why excluded:
- GitHub Discussions path was not publicly available at review time
- Metabase remains relevant as a community source elsewhere, but not for this GitHub Discussions pilot

## Recommended 3–5 repo pilot bundle

Recommended first-pass bundle:
- `apache/superset`
- `lightdash/lightdash`
- `evidence-dev/evidence`
- `plausible/analytics`

Reserve backup:
- `getredash/redash`

Why this bundle:
- all four primary repos have public Discussions available
- each has user-facing product/workflow relevance
- together they cover BI dashboards, reporting workflow, semantic friction, and metric trust better than a single repo
- they avoid the heaviest developer-noise profiles in `grafana` and `dbt-core`

## Expected persona contribution

Expected by repo:

- `apache/superset`
  - strongest for `persona_03`
  - secondary for `persona_02` and `persona_04`

- `lightdash/lightdash`
  - strongest for `persona_03`
  - secondary for `persona_01`, `persona_02`, and `persona_04`

- `evidence-dev/evidence`
  - strongest for `persona_03`
  - secondary for `persona_01` and `persona_02`
  - modest `persona_05` spillover possible, but that is not a pilot objective

- `plausible/analytics`
  - strongest for `persona_04`
  - secondary for `persona_02` and `persona_03`

Bundle-level expectation:
- meaningful signal should appear for at least `persona_02`, `persona_03`, and `persona_04`
- `persona_01` signal may be weaker than the failed Reddit bundle, but should be higher quality if present

## Expected noise risks

Primary risks:
- install or setup support noise
- auth or permission troubleshooting
- implementation or API/library issues
- generic feature requests with thin workflow context
- maintainer/internal discussion without end-user pain

Repo-specific caution:
- `superset` and `lightdash` may still overproduce product-support threads
- `evidence` may over-index on documentation or build workflow issues
- `plausible` may drift into self-hosting and setup support

## Pilot scope

Recommended bounded scope:
- target `500` to `1000` discussion threads across the bundle if available
- include:
  - title
  - body
  - category
  - labels if available
  - bounded comments if available
- cap comments per discussion:
  - recommended `10`
- keep outputs pilot-only under `artifacts/source_pilots/`

Suggested collection posture:
- prefer latest activity order
- bounded first pass across selected repos
- no production source activation

## Pilot artifacts

Pilot outputs:
- [github_discussions_expansion_raw.jsonl](C:\Users\hands\OneDrive\Desktop\persona\artifacts\source_pilots\github_discussions_expansion_raw.jsonl)
- [github_discussions_expansion_sample.csv](C:\Users\hands\OneDrive\Desktop\persona\artifacts\source_pilots\github_discussions_expansion_sample.csv)
- [github_discussions_expansion_summary.json](C:\Users\hands\OneDrive\Desktop\persona\artifacts\source_pilots\github_discussions_expansion_summary.json)
- [GITHUB_DISCUSSIONS_EXPANSION_PILOT_PLAN.md](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\GITHUB_DISCUSSIONS_EXPANSION_PILOT_PLAN.md)

## Automated scoring plan

Positive signals:
- dashboard or reporting pain
- metric definition confusion
- data model or semantic-layer confusion tied to reporting
- stakeholder-facing reporting need
- export or spreadsheet workaround
- data trust or reconciliation issue
- workflow limitation in BI/reporting tool
- root-cause or explanation handoff

Negative signals:
- install, deploy, or debug only
- auth, permission, or setup only
- generic feature request with no user pain
- API or library coding issue
- CI/CD or infrastructure issue
- beginner tutorial/help
- release announcement
- maintainer or internal discussion without user pain

Recommended summary metrics:
- `total_fetched_threads`
- `total_fetched_comments`
- `usable_rows`
- `valid_candidate_count`
- `valid_candidate_ratio`
- `estimated_persona_core_candidate_count`
- `estimated_labelable_ratio`
- `developer_support_noise_count`
- `setup_auth_noise_count`
- `generic_feature_request_noise_count`
- `persona_01_fit_count`
- `persona_02_fit_count`
- `persona_03_fit_count`
- `persona_04_fit_count`
- `persona_05_fit_count`
- `expected_source_tier`
- `estimated_weak_source_risk`

## Onboarding gate

Pass only if:
- enough public discussions are accessible from the selected repos
- `valid_candidate_ratio` is materially better than the failed Reddit pilot
- `estimated_labelable_ratio` is plausibly `>= 0.60`
- estimated persona-core candidates are meaningful
- at least two of `persona_01` through `persona_04` show strong signal
- developer-support noise is not dominant
- expected weak-source risk is not high

Recommended numeric interpretation:
- target `valid_candidate_ratio >= 0.35`
- target `estimated_labelable_ratio >= 0.60`
- target `estimated_persona_core_candidate_count >= 120` in a `500` to `1000` thread bundle
- target at least two personas among `persona_01` to `persona_04` with `>= 40` meaningful-fit rows

## Decision outcomes

Allowed outcomes:
- `promote_to_active_github_discussions_bundle`
- `expand_repo_sample`
- `narrow_repo_scope`
- `reject_github_discussions_expansion`
- `fallback_to_stackoverflow_tag_expansion`
- `stop_source_expansion`

Recommended interpretation:
- promote only if the bundle clears both quality and weak-source-risk gates
- narrow repo scope if one or two repos are clearly high-signal and the rest mainly add support noise
- reject if developer-support and setup noise dominate

## Go or no-go recommendation

Recommendation for implementation:
- **Go**

Why:
- existing collector family already exists
- public access path is clearer than failed vendor-community pilots
- repo shortlist is feasible and bounded
- this is the best next chance to find higher-quality product-workflow evidence without opening a new collector family

Important guardrail:
- even if the pilot passes, do not activate in production until the bundle clears a separate production activation review
