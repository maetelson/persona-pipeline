# Pipeline Metrics Before vs After

- Baseline snapshot: 2026-04-14T01:37:46.131203+00:00
- Current snapshot: 2026-04-14T01:37:46.131203+00:00
- Scope: raw, valid, prefiltered valid, episodes, labeled, persona-core, and source-balance risk indicators.
- Current snapshot still equals the baseline. Rerun collection and downstream stages, then rerun `python run/20_capture_scale_metrics.py` to populate the after delta.

## Overall Funnel

| Metric | Before | Current | Delta |
|---|---:|---:|---:|
| raw | 6804 | 6804 | 0 |
| valid | 3102 | 3102 | 0 |
| prefiltered_valid | 788 | 788 | 0 |
| episodes | 464 | 464 | 0 |
| labeled | 464 | 464 | 0 |
| labelable | 285 | 285 | 0 |
| persona_core | 285 | 285 | 0 |

## Source Balance

| Metric | Before | Current | Delta |
|---|---:|---:|---:|
| largest_raw_source_share_pct | 28.6 | 28.6 | 0.0 |
| largest_labeled_source_share_pct | 49.1 | 49.1 | 0.0 |
| largest_blended_influence_share_pct | 43.8 | 43.8 | 0.0 |
| effective_balanced_source_count | 4.0 | 4.0 | 0 |
| source_count_with_labelable | 10 | 10 | 0 |

## Current Source Snapshot

| Source | Raw | Valid | Prefiltered valid | Episodes | Labeled | Persona-core | Labeled share % | Blended influence % | Balance status | Top failure reason |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| hubspot_community | 750 | 266 | 142 | 78 | 78 | 78 | 16.8 | 18.7 | watchlist | low_valid_post_retention: missing_pain_signal |
| metabase_discussions | 800 | 443 | 355 | 228 | 228 | 58 | 49.1 | 43.8 | overdominant_source_risk | low_episode_yield |
| shopify_community | 800 | 167 | 38 | 35 | 35 | 35 | 7.5 | 8.1 | watchlist | low_valid_post_retention: missing_pain_signal |
| stackoverflow | 1947 | 1183 | 71 | 34 | 34 | 34 | 7.3 | 8.4 | weak_source_cost_center | low_prefilter_retention: stackoverflow:generic |
| google_ads_help_community | 151 | 84 | 38 | 31 | 31 | 30 | 6.7 | 7.2 | balanced_or_healthy | healthy_source_contribution |
| google_ads_community | 160 | 98 | 45 | 24 | 24 | 23 | 5.2 | 6.0 | watchlist | low_episode_yield |
| github_discussions | 985 | 539 | 23 | 20 | 20 | 13 | 4.3 | 4.6 | weak_source_cost_center | low_prefilter_retention: github_discussions_workflow_context |
| klaviyo_community | 800 | 128 | 71 | 12 | 12 | 12 | 2.6 | 2.7 | weak_source_cost_center | low_valid_post_retention: missing_pain_signal |
| merchant_center_community | 115 | 66 | 3 | 1 | 1 | 1 | 0.2 | 0.3 | weak_source_cost_center | low_prefilter_retention: merchant_center_community:generic |
| reddit | 296 | 102 | 1 | 1 | 1 | 1 | 0.2 | 0.3 | weak_source_cost_center | low_valid_post_retention: low_relevance_prefilter |
| reddit_business_intelligence | 0 | 10 | 1 | 0 | 0 | 0 | 0.0 | 0.0 |  |  |
| reddit_analytics | 0 | 6 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |  |  |
| reddit_marketing_analytics | 0 | 3 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |  |  |
| reddit_r_excel | 0 | 7 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |  |  |

## Readiness Context

- Dominant labeled source: metabase_discussions
- Dominant influence source: metabase_discussions
- Persona readiness state: reviewable_but_not_deck_ready
- Quality flag: UNSTABLE
