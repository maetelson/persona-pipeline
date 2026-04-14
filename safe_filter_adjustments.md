# Safe Filter Adjustments

## Implemented in This Pass

### Invalid Filter Vocabulary

- Added `discrepancy` to `pain_signal_keywords`.
- Added `not showing` to `pain_signal_keywords`.

Why this is safe:

- Both phrases already appear across the query inventory, source-specific seeds, and rejection diagnostics.
- They capture real reporting and measurement failure language without lowering any thresholds.
- They are especially relevant for Shopify, Google Ads Community, and Google Ads Help workflows.

## Explicitly Not Changed

- No threshold changes in the invalid filter.
- No threshold changes in the relevance prefilter.
- No changes to source-balance protection.
- No broad whitelist expansion.
- No persona-core admission changes.

## Medium-Risk Filter Changes To Test Later

### Shopify and HubSpot source-specific pain vocabulary

- Possible future additions: phrases around under-reporting, missing report fields, stopped syncing, and report-building blockers.
- Why not now: invalid samples still include support replies and benign how-to discussion, so the precision gain needs a review pass first.

### GitHub Discussions workflow rescue tuning

- Possible future change: modest relief for highly specific workflow-context posts.
- Why not now: current prefilter weakness is seed precision, and a filter change before seed cleanup would blur diagnosis.

### Merchant Center source-specific rules

- Possible future change: stronger handling for product-feed mismatch and local-feed sync language.
- Why not now: the source currently needs seed cleanup more than filter relaxation.

## Decision Rule

Any future filter change should ship only if the after snapshot shows downstream lift in prefiltered valid, episodes, labeled, and persona-core without increasing source concentration or junk dominance.