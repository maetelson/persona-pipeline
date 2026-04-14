# Source Expansion Plan

## Expand Now

### Shopify Community

- Change: raise `max_threads_per_run` from 800 to 1,400 and `max_discovered_threads_per_url` from 100 to 150.
- Why: the source discovered 1,534 threads and only fetched 800, so collection is cap-bound.
- Why it is safe: downstream efficiency is already acceptable and labelability is strong.
- Expected effect: more raw plus a real increase in prefiltered valid, episodes, labeled, and persona-core without leaning harder on Metabase.

### HubSpot Community

- Change: raise `max_threads_per_run` from 800 to 1,200, raise `max_discovered_threads_per_url` from 100 to 150, raise `discovery_page_count` from 8 to 12, and raise Khoros `max_items_per_board` from 250 to 350.
- Why: current volume is discovery-limited and fetch-limited, but downstream retention is strong enough to justify controlled expansion.
- Why it is safe: the same collector path and boards remain in use; only depth changes.
- Expected effect: more non-Metabase labeled and persona-core rows from a source that already survives prefilter and labeling well.

### Google Ads Community

- Change: raise `discovery_page_count` from 1 to 4 and `max_discovered_threads_per_url` from 100 to 150.
- Why: this source has good post-filter survival but shallow discovery.
- Why it is safe: we are not expanding to new boards or relaxing filters, only reading deeper into existing discovery pages.
- Expected effect: moderate raw lift with a relatively efficient valid-to-labelable path.

### Google Ads Help Community

- Change: enable the source and lower its minimum raw warning floor from 1,000 to 100.
- Why: prior recovery diagnostics already show healthy downstream lift, and the collector is implemented and registry-backed.
- Why it is staged: it was previously disabled, so it should be monitored separately on the first rerun.
- Expected effect: a new high-signal non-Metabase source that improves both volume and source diversity.

## Hold for Now

### Metabase Discussions

- Reason to hold: it already owns about half of labeled volume and the highest blended influence share.
- Trigger to revisit: only after other sources gain enough downstream volume to reduce concentration risk.

### Stack Overflow

- Reason to hold: raw volume is already high, but prefilter retention is only about 6%.
- Trigger to revisit: after query pruning and workflow-context tuning improve precision.

### GitHub Discussions

- Reason to hold: raw volume is strong, but prefilter retention remains weak.
- Trigger to revisit: after zero-yield seeds are replaced with stronger workflow-limited phrases.

### Klaviyo Community

- Reason to hold: it is cap-bound but collapses at episode yield.
- Trigger to revisit: only after seed and invalid-filter diagnostics show better persona signal.

### Merchant Center Community

- Reason to hold: prefilter retention is currently too low.
- Trigger to revisit: after seed precision improves and generic support cases are reduced.

### Reddit Aggregate

- Reason to hold: current retention indicates low-yield ingestion, not lack of raw depth.
- Trigger to revisit: only after targeted subreddit/seed evidence shows sustained downstream lift.