# Reconciliation/Signoff Anchor Structure

This document defines the two-layer anchor evaluation structure for reconciliation/signoff experiments.

## Layer 1: Identity Anchor Set

Artifact:
- `artifacts/curation/reconciliation_signoff_anchor_set.csv`

Purpose:
- preserve baseline `persona_04` identity
- validate purity of the reconciliation/signoff persona
- block hard-negative/helpdesk/docs rows from becoming anchors
- keep reporting-packager parent rows in `persona_01`
- keep ambiguous rows out of anchor construction

This set is an identity and safety rail, not a coverage-gain rail.

## Layer 2: Expansion Anchor Set

Artifact:
- `artifacts/curation/reconciliation_signoff_expansion_anchor_set.csv`

Purpose:
- evaluate whether high-quality reconciliation/signoff rows outside baseline `persona_04` should safely join `persona_04`
- measure coverage gain separately from identity preservation
- keep parent / hard-negative / ambiguous rows explicit in the evaluation set

Current expansion counts:
- positive expansion rows: 50
- parent retention rows: 25
- hard-negative block rows: 25
- ambiguous do-not-anchor rows: 15

## Intended Use

- use the identity anchor set to test continuity, purity, and safety
- use the expansion anchor set to test controlled coverage gain
- do not treat either set as a gold final acceptance set
- do not use these sets alone to justify production changes without separate holdout evidence
