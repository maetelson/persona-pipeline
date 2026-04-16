"""Logue seed strategy proposal for broad quantitative interpretation pain."""

# Logue Seed Strategy Proposal

## Canonical Taxonomy

Logue should organize seeds around broad quantitative interpretation pain, not around conversion as the default center of gravity.

### Canonical Problem Axes

| Axis | Definition | High-signal operator wording |
|---|---|---|
| `numbers_mismatch` | Two or more tools, reports, exports, or dashboards disagree. | `numbers don't match`, `report is wrong`, `totals are off`, `dashboard says one thing export says another` |
| `metric_definition_confusion` | The user cannot tell how a metric is defined or calculated. | `metric definition`, `what is counted`, `why is this KPI different`, `unclear formula` |
| `trend_interpretation_confusion` | The user sees a spike/drop/change but cannot interpret it confidently. | `what changed`, `why did this spike`, `is this normal`, `how to explain this drop` |
| `segmentation_breakdown_confusion` | The user cannot break numbers down correctly by channel, audience, device, product, lifecycle stage, or time grain. | `breakdown by channel`, `segment comparison`, `device split`, `which segment caused this` |
| `dashboard_confusion` | The dashboard is hard to interpret, too summarized, or not sufficient for follow-up analysis. | `dashboard not enough`, `can't drill down`, `dashboard is confusing`, `need more detail` |
| `aggregation_filter_issue` | Wrong rollups, date basis, filtering, duplicate counting, denominator, or grouping logic. | `wrong aggregation`, `date range issue`, `double counted`, `wrong filter`, `created_at vs event_date` |
| `source_of_truth_conflict` | Different systems claim different truths, and the user cannot resolve which is authoritative. | `source of truth`, `CRM says one thing analytics says another`, `backend doesn't match dashboard` |
| `business_question_translation_failure` | The user struggles to turn a vague business question into the right analysis question. | `what should I even look at`, `how do I frame this analysis`, `which report answers this` |
| `stakeholder_explanation_pressure` | The user needs to explain numbers to managers, execs, or clients under time pressure. | `stakeholder asks why`, `need to explain this`, `leadership wants an answer`, `weekly report pressure` |
| `insight_to_action_failure` | The user sees the numbers but cannot decide what action follows. | `what should I do next`, `what action should I take`, `how do I act on this` |
| `attribution_tracking_issue` | The user cannot trust event, attribution, or tracking logic. | `tracking is off`, `attribution mismatch`, `conversion path unclear`, `not recording properly` |
| `funnel_conversion_drop` | The user is investigating a conversion, revenue, or funnel decline. | `conversion drop`, `revenue down`, `checkout drop`, `funnel drop-off` |

### Normalized Seed Tiers

Every source should use the same three-tier structure:

1. Core high-signal pain phrases
   - Short, operator-native, complaint-like, and directly tied to one canonical axis.
   - These should be the main active seeds.
2. Medium-signal expansion phrases
   - Longer workflow phrasing, tool-specific variants, and adjacent follow-up wording.
   - Good for expansion, review buckets, or source-specific enrichment.
3. Risky / broad phrases to avoid
   - Generic BI, marketing, dashboard, or “best practices” language that pulls low-signal noise.

### Canonical Seed Families By Axis

| Axis | Core high-signal pain phrases | Medium-signal expansion phrases | Risky / broad phrases to avoid |
|---|---|---|---|
| `numbers_mismatch` | `numbers don't match`, `wrong totals`, `report mismatch`, `dashboard numbers wrong` | `export doesn't match dashboard`, `CRM and analytics don't match`, `SQL result doesn't match report` | `analytics issue`, `reporting problem`, `wrong data` |
| `metric_definition_confusion` | `metric definition`, `what counts as`, `KPI definition`, `formula confusion` | `how is this calculated`, `why is this KPI different across reports`, `semantic layer confusion` | `metrics`, `KPI`, `analytics metrics` |
| `trend_interpretation_confusion` | `why did this change`, `spike in numbers`, `drop in numbers`, `what changed and why` | `week over week looks wrong`, `is this trend real`, `unexpected jump in report` | `trend analysis`, `performance trend` |
| `segmentation_breakdown_confusion` | `breakdown by channel`, `segment comparison`, `device split`, `which segment caused this` | `new vs returning confusion`, `campaign breakdown doesn't add up`, `breakdown by lifecycle stage` | `segmentation`, `segments`, `channel report` |
| `dashboard_confusion` | `dashboard not enough`, `can't drill down`, `dashboard is confusing`, `can't interpret dashboard` | `summary chart but no explanation`, `exporting because dashboard is too limited`, `need more detail in dashboard` | `dashboard`, `dashboard tool`, `data viz` |
| `aggregation_filter_issue` | `wrong aggregation`, `wrong date field`, `double counted`, `filter logic issue` | `created_at vs event_date`, `rolling window confusion`, `duplicate counting problem`, `wrong denominator` | `SQL filter`, `aggregation`, `date range` |
| `source_of_truth_conflict` | `source of truth`, `which number is right`, `systems don't agree`, `backend doesn't match dashboard` | `finance vs analytics mismatch`, `CRM vs dashboard mismatch`, `dashboard vs export mismatch` | `data governance`, `single source of truth` |
| `business_question_translation_failure` | `what should I analyze`, `which report answers this`, `how do I frame this`, `don't know what to look at` | `business question too vague`, `how to turn this into an analysis question`, `what dimension should I break it down by` | `analytics question`, `analysis`, `business question` |
| `stakeholder_explanation_pressure` | `need to explain numbers`, `stakeholder asks why`, `leadership wants explanation`, `weekly report pressure` | `need an executive summary`, `can't explain KPI movement`, `client asking why numbers changed` | `stakeholder report`, `leadership dashboard` |
| `insight_to_action_failure` | `what should I do next`, `what action should I take`, `can't turn this into action`, `what does this mean for the business` | `which lever changed`, `what should we optimize`, `how to act on this metric` | `business insight`, `decision making` |
| `attribution_tracking_issue` | `attribution mismatch`, `tracking issue`, `not recording properly`, `conversion path unclear` | `UTM not matching report`, `event tracking off`, `platform says click but analytics says none` | `attribution`, `tracking`, `conversion tracking` |
| `funnel_conversion_drop` | `conversion drop`, `revenue down`, `checkout drop`, `drop-off increased` | `why did conversion rate fall`, `what caused lead drop`, `which step of funnel changed` | `conversion`, `revenue`, `funnel` |

## Problem Axes By Community

The table below is a normalized language map. It is designed to be portable across sources, but still respects how each community tends to phrase pain.

Important uncertainty:
- `reddit`, `stackoverflow`, `shopify_community`, `hubspot_community`, and `klaviyo_community` are current repo concepts.
- `power_bi_community`, `mixpanel_community`, and `qlik_community` now have current source configs in this repository.
- So the rows for those four communities are language proposals, not wired runtime behavior.

| Community | Strongest natural language patterns | Best-fitting axes | Conversion role |
|---|---|---|---|
| `reddit` | `numbers don't match`, `can't explain KPI`, `weekly report pain`, `which number do you trust`, `what should I look at` | `numbers_mismatch`, `trend_interpretation_confusion`, `stakeholder_explanation_pressure`, `business_question_translation_failure`, `source_of_truth_conflict` | One subset among many; should not dominate aggregate Reddit seeds |
| `stack_overflow` | `how to compare`, `why report doesn't match`, `created_at vs event_date`, `wrong aggregation`, `segment breakdown`, `how to explain KPI change` | `aggregation_filter_issue`, `metric_definition_confusion`, `segmentation_breakdown_confusion`, `numbers_mismatch`, `trend_interpretation_confusion` | Present, but secondary to analysis/debugging phrasing |
| `shopify_community` | `sales don't match`, `orders not matching`, `dashboard not enough`, `channel breakdown`, `traffic not matching`, `which report is right` | `numbers_mismatch`, `source_of_truth_conflict`, `segmentation_breakdown_confusion`, `dashboard_confusion`, `attribution_tracking_issue` | Useful, but currently overemphasized relative to other quantitative pain |
| `hubspot_community` | `report wrong`, `attribution report wrong`, `source of truth`, `weekly report`, `dashboard export`, `need to explain this` | `numbers_mismatch`, `source_of_truth_conflict`, `stakeholder_explanation_pressure`, `dashboard_confusion`, `metric_definition_confusion` | One valid branch, but not the main identity |
| `klaviyo_community` | `revenue mismatch`, `segment not updating`, `campaign results missing`, `what changed`, `which number is right`, `how should I interpret this` | `numbers_mismatch`, `trend_interpretation_confusion`, `segmentation_breakdown_confusion`, `attribution_tracking_issue`, `insight_to_action_failure` | Relevant, but the seed mix should expand beyond revenue and attribution |
| `power_bi_community` | `totals are wrong`, `measure definition`, `filter context issue`, `drill-down confusion`, `can't explain dashboard` | `metric_definition_confusion`, `aggregation_filter_issue`, `dashboard_confusion`, `numbers_mismatch`, `business_question_translation_failure` | Usually minor; should not dominate the community framing |
| `mixpanel_community` | `event count doesn't match`, `funnel report looks wrong`, `which number should I trust`, `what changed in this trend`, `what should I do from this report` | `numbers_mismatch`, `trend_interpretation_confusion`, `source_of_truth_conflict`, `insight_to_action_failure`, `metric_definition_confusion` | Present, but only one branch under wider product-analytics trust and interpretation pain |
| `amplitude_community` | `event count doesn't match chart`, `retention numbers don't make sense`, `which amplitude metric should I trust`, `breakdown by property does not add up`, `what changed in this trend` | `numbers_mismatch`, `metric_definition_confusion`, `trend_interpretation_confusion`, `segmentation_breakdown_confusion`, `source_of_truth_conflict` | Present, but secondary to broader product-analytics trust and interpretation pain |
| `qlik_community` | `wrong totals`, `set analysis doesn't match`, `filter pane changed the numbers`, `straight table total is wrong`, `dashboard and export don't match` | `numbers_mismatch`, `aggregation_filter_issue`, `metric_definition_confusion`, `dashboard_confusion`, `segmentation_breakdown_confusion` | Usually secondary to totals, set-analysis, and dashboard trust issues |
| `sisense_community` | `dashboard numbers don't match`, `wrong totals in pivot table`, `widget value does not match table`, `dashboard filter changes totals unexpectedly`, `source data and report don't match` | `numbers_mismatch`, `aggregation_filter_issue`, `dashboard_confusion`, `source_of_truth_conflict`, `segmentation_breakdown_confusion` | Present, but dashboard trust and filter logic should dominate |

## Conversion Is A Subset, Not The Center

`funnel_conversion_drop` should be treated as one of twelve canonical axes, not as the umbrella under which other pains are interpreted.

That means:

- Do not use `conversion`, `revenue drop`, `campaign drop`, or `attribution` as the main style check for all business-community seed banks.
- Do not define broad quantitative pain through only marketing-performance language.
- Do not assume “numbers pain” means “conversion investigation.”

Instead, conversion should sit inside a broader structure:

1. First ask whether the user is struggling with trust, interpretation, comparison, explanation, or actionability of numbers.
2. Then classify whether the concrete case is about:
   - mismatch
   - definition
   - trend interpretation
   - segmentation
   - dashboard limits
   - aggregation/filtering
   - source-of-truth conflict
   - question framing
   - stakeholder pressure
   - actionability
   - attribution/tracking
   - or funnel/conversion

Practical implication for seeds:

- A source should not be considered “well covered” just because it has:
  - `conversion drop`
  - `attribution mismatch`
  - `campaign performance drop`
- It also needs seeds for:
  - `which number is right`
  - `what changed and why`
  - `wrong total`
  - `dashboard says one thing export says another`
  - `how do I explain this to stakeholders`
  - `what analysis should I run`
  - `what action should I take`

## Bad Seed Patterns To Avoid

### Risky / broad patterns

- `analytics`
- `dashboard`
- `reporting`
- `metrics`
- `KPI`
- `business intelligence`
- `segment`
- `trend`
- `attribution`
- `conversion`

These are too broad when used alone.

### High-noise educational / generic patterns

- `tutorial`
- `best practices`
- `getting started`
- `how to learn`
- `course`
- `certification`
- `examples`
- `template`
- `sample dashboard`

### Developer-heavy patterns that drift away from Logue

- `API auth`
- `SDK`
- `OAuth`
- `npm`
- `docker`
- `stack trace`
- `plugin development`
- `deployment`
- `warehouse architecture`

### Bad pattern rule of thumb

Avoid seeds that are:

- broad noun-only phrases with no pain signal
- tool names with no interpretation problem attached
- generic education or inspiration requests
- setup-only or syntax-only issues
- pure optimization phrasing with no confusion, trust, interpretation, or actionability burden

## Rollout Recommendation

### 1. Normalize the taxonomy first

Update the repository’s seed framing so every active source can be mapped back to the same 12 canonical axes.

Most likely files:
- [config/query_seed_taxonomy.yaml](C:/Users/hands/OneDrive/Desktop/persona/config/query_seed_taxonomy.yaml)
- [config/query_map.yaml](C:/Users/hands/OneDrive/Desktop/persona/config/query_map.yaml)
- [config/query_expansion_rules.yaml](C:/Users/hands/OneDrive/Desktop/persona/config/query_expansion_rules.yaml)

### 2. Rebuild source-local seed banks around the same structure

For each community, define:
- 6 to 10 core high-signal pain phrases
- 8 to 16 medium-signal expansion phrases
- an explicit avoid list for broad or misleading variants

Most likely files:
- [config/seeds/business_communities](C:/Users/hands/OneDrive/Desktop/persona/config/seeds/business_communities)
- [config/seeds/existing_forums](C:/Users/hands/OneDrive/Desktop/persona/config/seeds/existing_forums)
- [config/seeds/reddit](C:/Users/hands/OneDrive/Desktop/persona/config/seeds/reddit)

### 3. Update seed-bank validation so it no longer rewards conversion-heavy phrasing disproportionately

Most likely file:
- [src/utils/seed_bank.py](C:/Users/hands/OneDrive/Desktop/persona/src/utils/seed_bank.py)

### 4. Retune downstream filters after seed reframing

Broader seeds will not matter if relevance and invalid filters still mainly reward conversion/reporting-drop language.

Most likely files:
- [config/relevance_rules.yaml](C:/Users/hands/OneDrive/Desktop/persona/config/relevance_rules.yaml)
- [config/invalid_rules.yaml](C:/Users/hands/OneDrive/Desktop/persona/config/invalid_rules.yaml)

### 5. Validate source coverage with diagnostics, not intuition

Use source diagnostics to confirm that newly introduced axes actually survive:
- raw collection
- valid filtering
- relevance prefilter
- episode build
- labeling
- persona promotion

Current repo artifacts that can support this:
- [data/analysis/source_diagnostics.csv](C:/Users/hands/OneDrive/Desktop/persona/data/analysis/source_diagnostics.csv)
- [data/analysis/weak_source_triage.csv](C:/Users/hands/OneDrive/Desktop/persona/data/analysis/weak_source_triage.csv)
- [data/analysis/business_community_seed_coverage.csv](C:/Users/hands/OneDrive/Desktop/persona/data/analysis/business_community_seed_coverage.csv)

## Proposed Next Step

Translate this document into:

1. a repo-level canonical seed taxonomy update
2. per-community seed bank rewrites
3. downstream filter alignment

That sequence keeps the repository reviewable and avoids changing collectors before the seed model itself is correct.
