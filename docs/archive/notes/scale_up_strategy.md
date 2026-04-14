# Scale-Up Strategy

## Objective

Increase total collection volume toward 30,000 raw rows while preserving downstream lift, persona interpretability, source diversity, and pipeline stability.

## Baseline

- Raw: 6,804
- Valid: 3,102
- Prefiltered valid: 788
- Episodes: 464
- Labeled: 464
- Persona-core: 285
- Current instability driver: Metabase dominates downstream influence while several other sources under-contribute after filtering.

## What This Pass Changes

- Raises collection caps only for sources that are visibly cap-bound and already produce useful downstream signal.
- Promotes only scored source-specific seeds in Shopify and Google Ads Community.
- Enables Google Ads Help Community with a realistic raw-volume floor because prior artifacts show healthy downstream yield.
- Adds a standalone before/after metrics snapshot utility instead of changing core pipeline orchestration.
- Keeps Metabase, Reddit aggregate, Stack Overflow, GitHub Discussions, Klaviyo, and Merchant Center out of the first expansion pass unless diagnostics improve.

## Safe Leverage Points

### Low-risk / high-confidence

- Shopify Community: discovered 1,534 threads and fetched only 800, with strong episode and persona-core conversion. Raising the fetch cap should lift both raw and downstream volume without changing filtering behavior.
- HubSpot Community: discovered 750/fetched 750 with strong downstream lift and perfect labelability. Increasing the API board cap and fetch cap should widen volume while staying in the same source family.
- Google Ads Community: downstream retention is strong and discovery depth is artificially shallow at one page. More discovery pages plus a few vetted seeds should increase useful volume.
- Google Ads Help Community: prior recovery artifacts show a healthy 151 -> 84 -> 38 -> 31 progression. Enabling it adds a non-Metabase source with good downstream efficiency.
- Instrumentation: snapshotting raw, valid, prefiltered valid, episodes, labeled, persona-core, and source-balance metrics is low-risk and necessary for staged rollout.

### Medium-risk staged expansion

- GitHub Discussions seed refresh: replace zero-yield complaint seeds with stronger workflow-limited seeds before increasing query count or page depth.
- Stack Overflow query tightening: use tag and prefilter diagnostics to prune generic queries, then deepen only the surviving subset.
- Source-specific invalid filter vocabulary expansion for Shopify and HubSpot: review invalid samples first, then add narrow source-specific pain phrases if the rejection pattern remains consistent after the cap increase.
- Merchant Center Community rescue: only after seed precision improves, because current prefilter retention is too low to justify raw growth.

### Risky and deferred

- Scaling Metabase now: it already dominates downstream influence, so expanding it would worsen source concentration.
- Broad Reddit expansion: current raw-to-prefilter retention is too weak and would mostly add junk.
- Blanket filter relaxation: this would increase vanity counts faster than persona signal.
- Klaviyo cap expansion: it is cap-bound, but current episode yield is too weak to treat raw growth there as useful.

## Why These Changes Should Increase Useful Volume

- Shopify and HubSpot are bottlenecked by collection caps, not by post-collection quality collapse.
- Google Ads Community and Google Ads Help show good downstream retention and contribute diversity outside the dominant Metabase source.
- Seed additions remain source-specific and pain-oriented, which preserves the current relevance discipline.
- No threshold was loosened globally; the only filter change in this pass is recognizing two high-signal pain phrases already central to this corpus.

## Rollout Order

1. Capture baseline metrics with `python run/diagnostics/20_capture_scale_metrics.py --set-baseline`.
2. Run collection for the changed sources only.
3. Rerun normalize -> filter -> prefilter -> episodes -> labeling -> analysis.
4. Re-run `python run/diagnostics/20_capture_scale_metrics.py`.
5. Compare `data/analysis/pipeline_metrics_before_after.md` and `data/analysis/source_balance_audit.csv` before promoting any medium-risk changes.

## Success Gates

- Raw rises materially.
- Prefiltered valid, episodes, labeled, and persona-core all rise.
- Largest blended source influence share does not worsen materially.
- Effective balanced source count rises or stays flat.
- New volume comes mainly from Shopify, HubSpot, Google Ads Community, and Google Ads Help rather than from Metabase or Reddit.
