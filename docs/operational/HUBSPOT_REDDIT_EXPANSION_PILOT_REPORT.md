# HubSpot + Reddit Expansion Pilot Report

## Baseline
- `persona_readiness_state` = `reviewable_but_not_deck_ready`
- `overall_status` = `WARN`
- `quality_flag` = `EXPLORATORY`
- `total_raw_rows` = `132257`
- `total_labeled_rows` = `12674`
- `total_persona_core_rows` = `9445`
- `effective_balanced_source_count` = `5.89`
- `persona_core_coverage_of_all_labeled_pct` = `74.5`
- `weak_source_cost_center_count` = `4`
- `core_readiness_weak_source_cost_center_count` = `3`
- `final_usable_persona_count` = `3`
- `production_ready_persona_count` = `3`
- `review_ready_persona_count` = `1`
- `deck_ready_claim_eligible_persona_count` = `4`

## Dedupe Index Summary
- `hubspot_community` = `{'source': 'hubspot_community', 'existing_raw_count': 6581, 'existing_normalized_count': 6581, 'existing_labeled_count': 796, 'existing_known_raw_id_count': 6581, 'existing_known_url_count': 1570, 'existing_known_canonical_url_count': 1570, 'existing_known_title_count': 2440, 'existing_known_content_hash_count': 7370, 'dedupe_key_coverage': {'raw_id': True, 'url': True, 'canonical_url': True, 'title': True, 'content_hash': True}}`
- `reddit` = `{'source': 'reddit', 'existing_raw_count': 629, 'existing_normalized_count': 629, 'existing_labeled_count': 214, 'existing_known_raw_id_count': 629, 'existing_known_url_count': 629, 'existing_known_canonical_url_count': 0, 'existing_known_title_count': 787, 'existing_known_content_hash_count': 1387, 'dedupe_key_coverage': {'raw_id': True, 'url': True, 'canonical_url': False, 'title': True, 'content_hash': True}}`

## Source Summaries
### hubspot_community
- `fetched_rows` = `303`
- `duplicate_rows` = `302`
- `new_unique_rows` = `1`
- `duplicate_rate` = `0.997`
- `duplicate_reason_counts` = `{'duplicate_raw_id': 299, 'duplicate_url': 3}`
- `valid_candidate_count` = `1`
- `estimated_persona_core_candidate_count` = `1`
- `incremental_core_candidate_ratio` = `1.0`
- `career_training_noise_count` = `0`
- `generic_chatter_noise_count` = `0`
- `setup_support_noise_count` = `0`
- `vendor_marketing_noise_count` = `0`
- `persona_fit_counts` = `{'persona_01': 1, 'persona_02': 1, 'persona_03': 1, 'persona_04': 0, 'persona_05': 1}`
- `seed_window_exhausted` = `True`
### reddit
- `fetched_rows` = `0`
- `duplicate_rows` = `0`
- `new_unique_rows` = `0`
- `duplicate_rate` = `0.0`
- `duplicate_reason_counts` = `{}`
- `valid_candidate_count` = `0`
- `estimated_persona_core_candidate_count` = `0`
- `incremental_core_candidate_ratio` = `0.0`
- `career_training_noise_count` = `0`
- `generic_chatter_noise_count` = `0`
- `setup_support_noise_count` = `0`
- `vendor_marketing_noise_count` = `0`
- `persona_fit_counts` = `{'persona_01': 0, 'persona_02': 0, 'persona_03': 0, 'persona_04': 0, 'persona_05': 0}`
- `seed_window_exhausted` = `True`

## Projected Metric Movement
- `projected_labeled_rows` = `12675`
- `projected_persona_core_rows` = `9446`
- `projected_effective_balanced_source_count` = `5.9`
- `projected_persona_core_coverage_of_all_labeled_pct` = `74.52`
- `projected_weak_source_cost_center_count` = `4`
- `projected_core_readiness_weak_source_cost_center_count` = `3`
- `projected_final_usable_persona_count` = `3`
- `projected_production_ready_persona_count` = `3`
- `projected_review_ready_persona_count` = `1`
- `projected_deck_ready_claim_eligible_persona_count` = `4`
- `projected_source_balance_impact` = `insufficient`
- `confidence_level` = `low`

## Gate Result
- `hubspot_pass` = `False`
- `reddit_pass` = `False`
- `combined_pass` = `False`
- `decision` = `adjust_seed_window`
