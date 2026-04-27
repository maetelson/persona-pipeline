# Refined Mixed Slice Classification

## Scope

This pass adds diagnostics-only refined slice fields for Google Developer Forums and Adobe Analytics Community mixed slices.
It preserves the existing source_slice_* fields and does not change official source balance, weak-source counts, readiness, or persona semantics.

## Refined Summary

- refined evidence category counts: `{'debt_producing_slice': 2325, 'evidence_producing_slice': 9435, 'mixed_evidence_slice': 914}`
- Google refined split: `{}`
- Adobe refined split: `{}`

## Review Alignment

- Google report_delivery_ui now splits into business-leaning delivery mismatch, clear auth/query/formula support, and mixed uncertain cases.
- Adobe mixed slices now split into workspace, tracking, API/admin, and operational business/debt/ambiguous diagnostics.
- Official metrics remain unchanged; these fields are for future simulation and precision review only.
