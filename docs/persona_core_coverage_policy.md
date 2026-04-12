## Purpose

Persona-core coverage should improve by recovering rows with stable persona signal, not by hiding uncertainty behind vague fallback buckets.

## Explicit Coverage Categories

- `missing_taxonomy_value`
  - the row is interpretable, but the current role, goal, pain, or output taxonomy does not capture it cleanly enough
- `overly_strict_axis_requirement`
  - the row still expresses a stable persona signature, but current low-signal gating is stricter than the policy-backed evidence warrants
- `parser_schema_mismatch`
  - the row has strong evidence, but the parser or label contract failed to preserve that evidence in the final labels
- `weak_workflow_context`
  - the row is relevant, but workflow-stage evidence is too implicit for the current deterministic mapping
- `insufficient_bottleneck_signal`
  - the row is relevant, but the current bottleneck mapping does not recover the blocking friction clearly enough
- `output_expectation_not_captured`
  - the row implies a dashboard, export, validation artifact, or automation output that the current output mapping missed
- `multi_axis_conflict`
  - the row contains conflicting directional signals that should stay out of persona-core until disambiguated
- `generic_chatter_not_persona_usable`
  - the row is too weak, generic, or announcement-like to support persona clustering

## Admission Policy

A low-signal row can still contribute to persona-core only when both conditions hold:

1. the primary persona axes are complete
2. the root cause category is one of:
   - `missing_taxonomy_value`
   - `overly_strict_axis_requirement`
   - `weak_workflow_context`
   - `insufficient_bottleneck_signal`
   - `output_expectation_not_captured`

These categories are supportable because they indicate recoverable mapping or gating issues, not generic noise.

## Exclusions

The following categories remain excluded from persona-core:

- `generic_chatter_not_persona_usable`
- `parser_schema_mismatch`
- `multi_axis_conflict`

These rows can inform QA and future repair work, but they should not define persona structure.

## Repair Rule

Supportable low-signal rows should not stay permanently blank. Deterministic repair is allowed to recover core labels for those rows when text evidence supports a stable role, goal, bottleneck, or output expectation.

## Transparency Requirement

Every unknown or non-core row should expose:

- the detailed row-level reason
- the explicit coverage category
- whether the category is supportable for low-signal persona-core admission

This keeps coverage gains auditable and prevents silent bucket inflation.