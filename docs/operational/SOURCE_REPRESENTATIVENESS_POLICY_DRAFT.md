# Source Representativeness Policy Draft

## Summary

- Current workbook readiness: `reviewable_but_not_deck_ready` / `WARN`
- Recommended next implementation path: `source-specific remediation`
- Deck-ready by source exclusion instead of source fixing: `False`
- Weak-source remediation still worth doing: `True`

## Tier Definitions

- `core_representative_source`: strongly aligned with target users and recurring BI or analytics interpretation pain.
- `supporting_validation_source`: useful for evidence and triangulation, but too product-specific or support-heavy to anchor deck-ready claims alone.
- `exploratory_edge_source`: useful for discovery, but not required for deck-ready core readiness.
- `exclude_from_deck_ready_core`: keep visible for reviewable analysis, but do not rely on it for deck-ready core claims.
- `archive_only`: keep raw data for reproducibility only.

## Tier Recommendations

- `metabase_discussions` -> `core_representative_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `False`
- `power_bi_community` -> `core_representative_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `False`
- `klaviyo_community` -> `exclude_from_deck_ready_core` | keep in reviewable `True` | keep in deck-ready core `False` | remediation needed `False`
- `domo_community_forum` -> `exploratory_edge_source` | keep in reviewable `True` | keep in deck-ready core `False` | remediation needed `False`
- `mixpanel_community` -> `exploratory_edge_source` | keep in reviewable `True` | keep in deck-ready core `False` | remediation needed `False`
- `qlik_community` -> `exploratory_edge_source` | keep in reviewable `True` | keep in deck-ready core `False` | remediation needed `False`
- `adobe_analytics_community` -> `supporting_validation_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `True`
- `github_discussions` -> `supporting_validation_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `False`
- `google_developer_forums` -> `supporting_validation_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `True`
- `hubspot_community` -> `supporting_validation_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `False`
- `reddit` -> `supporting_validation_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `False`
- `shopify_community` -> `supporting_validation_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `False`
- `sisense_community` -> `supporting_validation_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `False`
- `stackoverflow` -> `supporting_validation_source` | keep in reviewable `True` | keep in deck-ready core `True` | remediation needed `False`

## Questioned Sources

- `google_developer_forums`: representative `False`, mostly vendor/developer support `True`, unique evidence `True`, damage if removed `False`, recommended tier `supporting_validation_source`
- `adobe_analytics_community`: representative `False`, mostly vendor/developer support `True`, unique evidence `True`, damage if removed `True`, recommended tier `supporting_validation_source`
- `domo_community_forum`: representative `False`, mostly vendor/developer support `True`, unique evidence `True`, damage if removed `False`, recommended tier `exploratory_edge_source`
- `klaviyo_community`: representative `False`, mostly vendor/developer support `True`, unique evidence `True`, damage if removed `False`, recommended tier `exclude_from_deck_ready_core`

## Ablation Summary

- `A_current_baseline`: readiness `reviewable_but_not_deck_ready`, deck-ready plausibility `unchanged`, methodological representativeness `unchanged`, remaining source count `14`
- `B_exclude_klaviyo_from_core_only`: readiness `reviewable_but_not_deck_ready`, deck-ready plausibility `unchanged`, methodological representativeness `unchanged`, remaining source count `13`
- `C_exclude_google_from_deck_ready_core`: readiness `reviewable_but_not_deck_ready`, deck-ready plausibility `unchanged`, methodological representativeness `unchanged`, remaining source count `13`
- `D_exclude_adobe_from_deck_ready_core`: readiness `reviewable_but_not_deck_ready`, deck-ready plausibility `unchanged`, methodological representativeness `unchanged`, remaining source count `13`
- `E_exclude_domo_from_deck_ready_core`: readiness `reviewable_but_not_deck_ready`, deck-ready plausibility `more_plausible`, methodological representativeness `unchanged`, remaining source count `13`
- `F_exclude_google_adobe_domo`: readiness `exploratory_only`, deck-ready plausibility `more_plausible`, methodological representativeness `improves`, remaining source count `11`
- `G_exclude_all_non_core_representative_sources`: readiness `reviewable_but_not_deck_ready`, deck-ready plausibility `more_plausible`, methodological representativeness `unchanged`, remaining source count `10`
- `H_keep_only_core_representative_sources`: readiness `exploratory_only`, deck-ready plausibility `more_plausible`, methodological representativeness `improves`, remaining source count `2`

## Policy Direction

- Reviewable release and deck-ready core may use different source membership if the exclusion improves methodological representativeness without damaging persona structure.
- Raw source archives remain intact even when a source is excluded from deck-ready core claims.
- Excluding weak sources from deck-ready core is not threshold relaxation. It is a corpus-representativeness decision and must stay visible in diagnostics.
