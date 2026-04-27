# Existing Source Expansion Feasibility

## Baseline

- `persona_readiness_state` = `reviewable_but_not_deck_ready`
- `overall_status` = `WARN`
- `quality_flag` = `EXPLORATORY`
- `persona_core_coverage_of_all_labeled_pct` = `74.5`
- `effective_balanced_source_count` = `5.89`
- `weak_source_cost_center_count` = `4`
- `core_readiness_weak_source_cost_center_count` = `3`
- `core_recovery_decision` = `no_safe_core_recovery_available`

## Existing-Source Expansion Table

- `hubspot_community`: tier=`supporting_validation_source`, raw=`6581`, labeled=`796`, core=`712`, labelable_ratio=`98.4`, core_ratio=`0.894`, influence=`8.2`, weak=`False`, core_weak=`False`, under_expanded=`True`, expansion_worthy=`True`, score=`13.0`
- `reddit`: tier=`supporting_validation_source`, raw=`1068`, labeled=`214`, core=`168`, labelable_ratio=`90.2`, core_ratio=`0.785`, influence=`2.0`, weak=`False`, core_weak=`False`, under_expanded=`True`, expansion_worthy=`True`, score=`12.0`
- `sisense_community`: tier=`supporting_validation_source`, raw=`586`, labeled=`189`, core=`119`, labelable_ratio=`100.0`, core_ratio=`0.63`, influence=`1.5`, weak=`False`, core_weak=`False`, under_expanded=`True`, expansion_worthy=`True`, score=`11.0`
- `shopify_community`: tier=`supporting_validation_source`, raw=`5691`, labeled=`624`, core=`328`, labelable_ratio=`71.3`, core_ratio=`0.526`, influence=`4.4`, weak=`False`, core_weak=`False`, under_expanded=`True`, expansion_worthy=`False`, score=`10.0`
- `mixpanel_community`: tier=`exploratory_edge_source`, raw=`632`, labeled=`27`, core=`24`, labelable_ratio=`100.0`, core_ratio=`0.889`, influence=`0.3`, weak=`False`, core_weak=`False`, under_expanded=`False`, expansion_worthy=`False`, score=`9.0`
- `qlik_community`: tier=`exploratory_edge_source`, raw=`328`, labeled=`48`, core=`38`, labelable_ratio=`100.0`, core_ratio=`0.792`, influence=`0.5`, weak=`False`, core_weak=`False`, under_expanded=`False`, expansion_worthy=`False`, score=`9.0`
- `github_discussions`: tier=`supporting_validation_source`, raw=`18119`, labeled=`1313`, core=`891`, labelable_ratio=`83.2`, core_ratio=`0.679`, influence=`11.0`, weak=`False`, core_weak=`False`, under_expanded=`False`, expansion_worthy=`False`, score=`8.0`
- `klaviyo_community`: tier=`excluded_from_deck_ready_core`, raw=`6686`, labeled=`110`, core=`73`, labelable_ratio=`93.6`, core_ratio=`0.664`, influence=`0.9`, weak=`True`, core_weak=`False`, under_expanded=`False`, expansion_worthy=`False`, score=`5.0`
- `metabase_discussions`: tier=`core_representative_source`, raw=`14239`, labeled=`2055`, core=`1724`, labelable_ratio=`100.0`, core_ratio=`0.839`, influence=`20.0`, weak=`False`, core_weak=`False`, under_expanded=`False`, expansion_worthy=`False`, score=`4.0`
- `adobe_analytics_community`: tier=`supporting_validation_source`, raw=`4357`, labeled=`645`, core=`172`, labelable_ratio=`28.4`, core_ratio=`0.267`, influence=`3.2`, weak=`True`, core_weak=`True`, under_expanded=`False`, expansion_worthy=`False`, score=`3.0`
- `power_bi_community`: tier=`core_representative_source`, raw=`32088`, labeled=`3114`, core=`2661`, labelable_ratio=`99.5`, core_ratio=`0.855`, influence=`30.7`, weak=`False`, core_weak=`False`, under_expanded=`False`, expansion_worthy=`False`, score=`3.0`
- `stackoverflow`: tier=`supporting_validation_source`, raw=`21544`, labeled=`1683`, core=`678`, labelable_ratio=`48.2`, core_ratio=`0.403`, influence=`10.2`, weak=`False`, core_weak=`False`, under_expanded=`False`, expansion_worthy=`False`, score=`2.0`
- `google_developer_forums`: tier=`supporting_validation_source`, raw=`10984`, labeled=`812`, core=`186`, labelable_ratio=`27.6`, core_ratio=`0.229`, influence=`3.7`, weak=`True`, core_weak=`True`, under_expanded=`False`, expansion_worthy=`False`, score=`1.0`
- `domo_community_forum`: tier=`exploratory_edge_source`, raw=`9354`, labeled=`1044`, core=`93`, labelable_ratio=`8.9`, core_ratio=`0.089`, influence=`3.5`, weak=`True`, core_weak=`True`, under_expanded=`False`, expansion_worthy=`False`, score=`-2.0`

## Sources That Should Not Be Expanded

- `adobe_analytics_community`: current core-readiness weak-source cost center
- `google_developer_forums`: current core-readiness weak-source cost center and developer-support bias
- `domo_community_forum`: exploratory edge and current weak-source cost center
- `klaviyo_community`: excluded from deck-ready core
- `mixpanel_community`: exploratory edge and too small to move metrics materially
- `qlik_community`: exploratory edge and too small to move metrics materially
- `power_bi_community`: already dominant; more rows would worsen concentration
- `metabase_discussions`: already dominant core source; expansion would not improve balance enough
- `stackoverflow`: supporting_validation but low persona-core ratio and high technical-debug dilution
- `github_discussions`: prior expansion pilots failed quality gates and support/developer noise remains high

## Scenario Simulation Results

### A_expand_one_best_existing_source_plus_500_raw
- `projected_labeled_rows` = `12734`
- `projected_persona_core_rows` = `9499`
- `projected_persona_core_coverage_of_all_labeled_pct` = `74.59`
- `projected_effective_balanced_source_count` = `5.94`
- `projected_weak_source_cost_center_count` = `4`
- `projected_core_readiness_weak_source_cost_center_count` = `3`
- `projected_contribution_to_persona_01` = `48.0`
- `projected_contribution_to_persona_02` = `4.2`
- `projected_contribution_to_persona_03` = `1.9`
- `projected_contribution_to_persona_04` = `2.8`
- `risk_of_worsening_source_concentration` = `low`
- `risk_of_creating_new_weak_source_debt` = `low`
- `whether_deck_ready_becomes_materially_closer` = `no`
- `sources` = `[('hubspot_community', 500)]`
### B_expand_one_best_existing_source_plus_1000_raw
- `projected_labeled_rows` = `12795`
- `projected_persona_core_rows` = `9553`
- `projected_persona_core_coverage_of_all_labeled_pct` = `74.66`
- `projected_effective_balanced_source_count` = `5.97`
- `projected_weak_source_cost_center_count` = `4`
- `projected_core_readiness_weak_source_cost_center_count` = `3`
- `projected_contribution_to_persona_01` = `96.0`
- `projected_contribution_to_persona_02` = `8.4`
- `projected_contribution_to_persona_03` = `3.8`
- `projected_contribution_to_persona_04` = `5.6`
- `risk_of_worsening_source_concentration` = `low`
- `risk_of_creating_new_weak_source_debt` = `low`
- `whether_deck_ready_becomes_materially_closer` = `no`
- `sources` = `[('hubspot_community', 1000)]`
### C_expand_two_balanced_existing_sources_plus_500_each
- `projected_labeled_rows` = `12835`
- `projected_persona_core_rows` = `9578`
- `projected_persona_core_coverage_of_all_labeled_pct` = `74.62`
- `projected_effective_balanced_source_count` = `6.02`
- `projected_weak_source_cost_center_count` = `4`
- `projected_core_readiness_weak_source_cost_center_count` = `3`
- `projected_contribution_to_persona_01` = `122.9`
- `projected_contribution_to_persona_02` = `7.9`
- `projected_contribution_to_persona_03` = `1.9`
- `projected_contribution_to_persona_04` = `12.6`
- `risk_of_worsening_source_concentration` = `low`
- `risk_of_creating_new_weak_source_debt` = `low`
- `whether_deck_ready_becomes_materially_closer` = `yes_for_balance_only`
- `sources` = `[('hubspot_community', 500), ('reddit', 500)]`
### D_expand_one_core_representative_source_and_one_supporting_validation_source
- `projected_labeled_rows` = `12807`
- `projected_persona_core_rows` = `9560`
- `projected_persona_core_coverage_of_all_labeled_pct` = `74.65`
- `projected_effective_balanced_source_count` = `5.92`
- `projected_weak_source_cost_center_count` = `4`
- `projected_core_readiness_weak_source_cost_center_count` = `3`
- `projected_contribution_to_persona_01` = `66.7`
- `projected_contribution_to_persona_02` = `36.1`
- `projected_contribution_to_persona_03` = `11.9`
- `projected_contribution_to_persona_04` = `5.7`
- `risk_of_worsening_source_concentration` = `high`
- `risk_of_creating_new_weak_source_debt` = `low`
- `whether_deck_ready_becomes_materially_closer` = `no`
- `sources` = `[('metabase_discussions', 500), ('hubspot_community', 500)]`
### E_expand_only_sources_that_improve_source_balance
- `projected_labeled_rows` = `12835`
- `projected_persona_core_rows` = `9578`
- `projected_persona_core_coverage_of_all_labeled_pct` = `74.62`
- `projected_effective_balanced_source_count` = `6.02`
- `projected_weak_source_cost_center_count` = `4`
- `projected_core_readiness_weak_source_cost_center_count` = `3`
- `projected_contribution_to_persona_01` = `122.9`
- `projected_contribution_to_persona_02` = `7.9`
- `projected_contribution_to_persona_03` = `1.9`
- `projected_contribution_to_persona_04` = `12.6`
- `risk_of_worsening_source_concentration` = `low`
- `risk_of_creating_new_weak_source_debt` = `low`
- `whether_deck_ready_becomes_materially_closer` = `yes_for_balance_only`
- `sources` = `[('hubspot_community', 500), ('reddit', 500)]`
### F_no_op_baseline
- `projected_labeled_rows` = `12674`
- `projected_persona_core_rows` = `9445`
- `projected_persona_core_coverage_of_all_labeled_pct` = `74.5`
- `projected_effective_balanced_source_count` = `5.89`
- `projected_weak_source_cost_center_count` = `4`
- `projected_core_readiness_weak_source_cost_center_count` = `3`
- `projected_contribution_to_persona_01` = `0.0`
- `projected_contribution_to_persona_02` = `0.0`
- `projected_contribution_to_persona_03` = `0.0`
- `projected_contribution_to_persona_04` = `0.0`
- `risk_of_worsening_source_concentration` = `none`
- `risk_of_creating_new_weak_source_debt` = `none`
- `whether_deck_ready_becomes_materially_closer` = `no`
- `sources` = `[]`

## Recommendation

- `strategy` = `expand_two_balanced_existing_sources`
- `why` = `hubspot_community and reddit are the strongest under-expanded non-weak supporting_validation sources with persona_core_ratio above the current workbook baseline. Expanding them together is the only existing-source scenario that plausibly clears the effective_balanced_source_count floor while still nudging core coverage upward instead of diluting it.`
- `sources_to_expand_first` = `['hubspot_community', 'reddit']`
- `expected_row_target` = `{'hubspot_community_raw': 500, 'reddit_raw': 500}`
- `exact_collector_or_seed_config_to_adjust_later` = `['C:/Users/hands/OneDrive/Desktop/persona/config/seeds/business_communities/hubspot_community.yaml', 'C:/Users/hands/OneDrive/Desktop/persona/config/seeds/existing_forums/reddit.yaml', 'C:/Users/hands/OneDrive/Desktop/persona/config/sources/hubspot_community.yaml', 'C:/Users/hands/OneDrive/Desktop/persona/config/sources/reddit.yaml']`
- `pilot_size` = `250 raw rows per source before any full crawl increase`
- `success_metrics` = `{'combined_effective_balanced_source_count_target': '>=6.0', 'hubspot_incremental_persona_core_ratio_target': '>=0.80', 'reddit_incremental_persona_core_ratio_target': '>=0.75', 'projected_persona_core_coverage_must_not_drop': True, 'no_new_weak_source_cost_centers': True}`
- `rollback_criteria` = `['rollback if expanded reddit rows resemble the failed RevOps bundle and incremental persona_core_ratio falls below 0.75', 'rollback if hubspot expansion is dominated by support/config noise and incremental persona_core_ratio falls below 0.80', 'rollback if concentration rises without maintaining effective_balanced_source_count >= 6.0']`
- `why_better_than_adding_a_new_source_family` = `It reuses already-active collectors and vetted source families, avoids the recent instability of new vendor-community pilots, and offers the best shot at clearing the source-balance blocker without opening another weak-source family.`
- `expected_deck_ready_impact` = `materially helps source balance but only marginally moves persona_core_coverage_of_all_labeled_pct from 74.5 to about 74.62, so deck-ready would still remain blocked on core coverage even after a successful bounded expansion.`
