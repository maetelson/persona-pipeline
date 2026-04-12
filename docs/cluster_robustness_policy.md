## Cluster Robustness Policy

This policy keeps bottleneck-first persona clustering interpretable while making sure weak tail clusters and near-duplicate splits do not read like mature personas.

### What Counts As Structurally Supported

A final cluster is structurally supported only when both conditions hold:

- `stability_status = stable`
- `evidence_status = sufficient`

In practice this means the cluster is large enough to survive the stability gate and still clears the cohesion and separation floors after robustness merging.

Clusters that fail either condition can stay workbook-visible for analyst review, but they must not be counted as final usable or deck-ready personas.

### Merge Policy

The robustness pass now uses three layers:

1. Sparse signature attachment
   - Small signatures attach to the nearest anchor when similarity is high enough.

2. Fragile adjacent-cluster absorption
   - Small final anchors merge into a stronger parent when they share a primary bottleneck, base name, exact name, or strong secondary overlap and their centroid similarity is high enough.

3. Low-separation duplicate cleanup
   - Even if a cluster is large enough to survive the size gate, it can still merge into a larger adjacent cluster when centroid similarity is extremely high and nearest-neighbor separation is too low to justify a standalone mature persona.
   - This is the main protection against under-merged pairs such as two diagnose-change clusters that differ in wording but not in analyst-meaningful boundary.

Only the smallest unmatched fragments collapse into residual exploratory families.

### Review-Visible But Not Mature

These cluster states must stay review-visible instead of being counted as mature personas:

- grounded but structurally weak
- weakly grounded
- ungrounded
- fragile or micro tail clusters that still survive for audit visibility

The workbook must keep these clusters auditable, but headline persona counts must reflect only structurally supported and grounded personas.

### Workbook Metrics

The workbook should surface these metrics together so the story remains coherent:

- Separability:
  - `avg_cluster_separation`
  - `min_cluster_separation`
  - `weak_separation_cluster_count`

- Evidence sufficiency:
  - `thin_evidence_cluster_count`
  - `structurally_supported_cluster_count`

- Dominance:
  - `largest_cluster_share_of_core_labeled`
  - `top_3_cluster_share_of_core_labeled`

- Tail fragility:
  - `fragile_tail_cluster_count`
  - `fragile_tail_share_of_core_labeled`

### Interpretation Rule

If cluster robustness says the tail is fragile or that adjacent clusters are weakly separated, persona readiness and final usable persona counts must reflect that reality. Robustness metrics are not just audit decoration; they are part of the persona maturity contract.