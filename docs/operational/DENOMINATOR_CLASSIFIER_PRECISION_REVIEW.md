# Denominator Classifier Precision Review

## Sampled Row Classification Summary

- `sample_size = 300`
- `reviewed_category_counts = {'ambiguous_review_bucket': 79, 'server_deploy_config_noise': 71, 'technical_support_debug_noise': 68, 'syntax_formula_debug_noise': 53, 'setup_auth_permission_noise': 13, 'api_sdk_debug_noise': 6, 'denominator_eligible_business_non_core': 3, 'generic_low_signal': 3, 'vendor_announcement_or_feature_request_only': 2, 'career_training_certification_noise': 1, 'source_specific_support_noise': 1}`
- `percentage_of_generic_low_signal_truly_generic_low_signal = 1.0`
- `percentage_that_should_be_denominator_eligible_business_non_core = 1.0`
- `percentage_that_should_be_explicit_technical_support_noise = 71.7`
- `percentage_that_should_be_ambiguous_review_bucket = 26.3`
- `percentage_that_may_be_persona_core_candidate = 0.0`

## Source-by-Source Category Distribution

- `adobe_analytics_community` / `ambiguous_review_bucket`: 36
- `adobe_analytics_community` / `api_sdk_debug_noise`: 1
- `adobe_analytics_community` / `server_deploy_config_noise`: 8
- `adobe_analytics_community` / `technical_support_debug_noise`: 10
- `domo_community_forum` / `ambiguous_review_bucket`: 10
- `domo_community_forum` / `api_sdk_debug_noise`: 3
- `domo_community_forum` / `server_deploy_config_noise`: 42
- `domo_community_forum` / `setup_auth_permission_noise`: 1
- `domo_community_forum` / `technical_support_debug_noise`: 14
- `domo_community_forum` / `vendor_announcement_or_feature_request_only`: 1
- `github_discussions` / `ambiguous_review_bucket`: 9
- `github_discussions` / `api_sdk_debug_noise`: 1
- `github_discussions` / `server_deploy_config_noise`: 8
- `github_discussions` / `setup_auth_permission_noise`: 11
- `github_discussions` / `technical_support_debug_noise`: 21
- `github_discussions` / `vendor_announcement_or_feature_request_only`: 1
- `google_developer_forums` / `ambiguous_review_bucket`: 17
- `google_developer_forums` / `api_sdk_debug_noise`: 1
- `google_developer_forums` / `career_training_certification_noise`: 1
- `google_developer_forums` / `server_deploy_config_noise`: 13
- `google_developer_forums` / `setup_auth_permission_noise`: 1
- `google_developer_forums` / `technical_support_debug_noise`: 23
- `shopify_community` / `source_specific_support_noise`: 1
- `stackoverflow` / `ambiguous_review_bucket`: 7
- `stackoverflow` / `denominator_eligible_business_non_core`: 3
- `stackoverflow` / `generic_low_signal`: 3
- `stackoverflow` / `syntax_formula_debug_noise`: 53

## Root Cause of generic_low_signal Collapse

- The current Phase 1 classifier effectively hard-defaults every non-core row with labelability_status=low_signal into generic_low_signal; all 3229 non-core rows have that status.
- The current text scoring is too shallow for Stack Overflow and vendor-community language, so business-relevant reporting rows and explicit technical-noise rows are not being separated.
- Episode inputs do not include richer workflow metadata such as workflow_stage, analysis_goal, bottleneck_type, or trust_validation_need, so classifier decisions rely on compressed text only.
- Source-specific business and support vocabularies are under-modeled, especially for Adobe, Domo, Google Developer Forums, and GitHub Discussions.

## Proposed Classifier Calibration

- `additional_business_context_terms`: ['workspace', 'report suite', 'board deck', 'executive summary', 'page views', 'quarterly reporting', 'weekly reporting', 'forecast review', 'stakeholder delivery', 'validated export', 'leadership update']
- `additional_technical_noise_terms`: ['parser exception', 'permission denied', 'oauth', 'token', 'connector', 'runtime', 'endpoint', 'sdk', 'deployment', 'server call', 'calculated column', 'measure', 'matrix visual']
- `source_specific_noise_patterns`: {'adobe_analytics_community': ['server calls', 'evar', 'tracking rule'], 'domo_community_forum': ['beast mode', 'dataset view', 'connector config'], 'google_developer_forums': ['api quota', 'oauth consent', 'property setup'], 'github_discussions': ['self-hosted', 'docker', 'installation', 'upgrade'], 'stackoverflow': ['dax syntax', 'measure', 'calculated column', 'slicer']}
- `ambiguity_rules`: ['If business context >= 2 and technical/support signals >= 1, prefer ambiguous_review_bucket over generic_low_signal.', 'If stakeholder, mismatch, reconciliation, export, or delivery terms appear, do not fall back to generic_low_signal without a stronger explicit noise bucket.']
- `text_field_enrichment_needed`: ['Always include evidence_snippet, work_moment, and segmentation_note in the classifier text blob.', 'If available later, join workflow_stage, analysis_goal, bottleneck_type, and trust_validation_need before any denominator ablation.']
- `confidence_thresholds`: ['Only emit explicit denominator-ineligible noise buckets when technical/support evidence clearly dominates business context.', 'Keep mixed rows denominator-eligible and route them to ambiguous_review_bucket.']

## Full-Corpus Projection

- `reviewed_category_counts`: {'server_deploy_config_noise': 817, 'technical_support_debug_noise': 727, 'ambiguous_review_bucket': 711, 'syntax_formula_debug_noise': 710, 'setup_auth_permission_noise': 98, 'api_sdk_debug_noise': 75, 'generic_low_signal': 32, 'career_training_certification_noise': 28, 'denominator_eligible_business_non_core': 22, 'vendor_announcement_or_feature_request_only': 5, 'source_specific_support_noise': 4}
- `possible_denominator_ineligible_rows`: 2496
- `possible_adjusted_denominator_row_count`: 10178
- `possible_adjusted_core_coverage_pct`: 92.8

## Decision

- `recommended_next_action = implement_classifier_calibration_diagnostics`
- `whether_denominator_ablation_is_safe_to_run_next = False`