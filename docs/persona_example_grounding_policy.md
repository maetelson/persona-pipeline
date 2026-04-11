## Persona Example Grounding Policy

This policy links persona promotion, representative-example selection, fallback handling, and workbook display.

### Core Rule

Promotion must not outrun evidence.

- A promoted persona is healthy only when it has at least one selected example with grounded evidence.
- If only weak evidence exists, the persona may remain promoted, but the workbook must label it as weakly grounded.
- If no acceptable evidence exists, the persona must be surfaced as ungrounded or downgraded according to config.
- The system must never silently fill a promoted persona with a weak example and present it as a normal representative example.

### Candidate Quality Levels

- `strong_representative`: strong bottleneck evidence, workflow context, and no critical axis mismatch.
- `usable`: acceptable representative evidence with enough workflow and bottleneck specificity to ground the persona.
- `borderline`: partial evidence that may only be used as a documented fallback when policy permits.
- `reject`: not acceptable as persona grounding.

### Grounding Strength Levels

- `strong`: selected example is `strong_representative` and stays within strong mismatch limits.
- `grounded`: selected example is `strong_representative` or `usable` and stays within grounded mismatch limits.
- `weak`: example exceeds grounded mismatch limits or is only borderline, but still passes documented fallback eligibility.
- `unacceptable`: example cannot be used for grounding.

### Scoring Components

Representative-example ranking must include all of the following as real scoring inputs, not prose-only labels:

- workflow pain clarity
- repeated manual workaround evidence
- output or stakeholder need clarity
- tool-dependency and bottleneck fit
- cluster or persona axis fit
- mismatch penalties
- grounding-fit score
- diversity-aware tie-breaking across sources and subpatterns

### Fallback Rules

- Weak fallback is allowed only for promoted personas.
- Weak fallback may only use examples classified as `weak` by policy.
- Fallback may not be shown as a normal grounded representative example.
- Fallback rows must expose:
  - `selection_strength`
  - `grounding_strength`
  - `fallback_selected`
  - `coverage_selection_reason`
  - mismatch metadata

### Promotion and Grounding Relationship

Promotion is evaluated in two stages:

1. Base promotion checks cluster size and dominance rules.
2. Grounding policy then classifies promoted personas into one of these states:
   - `promoted_and_grounded`
   - `promoted_but_weakly_grounded`
   - `promoted_but_ungrounded`
   - `downgraded_due_to_no_grounding`

The workbook must expose both the final promotion status and the grounding state so reviewers can see whether a promoted persona is fully supported.

### Ungrounded Promotion Handling

The current config uses `ungrounded_action: flag`.

- If a promoted persona has no grounded example and no eligible weak fallback, it remains promoted only as `promoted_but_ungrounded`.
- If config changes to `ungrounded_action: downgrade`, the same case becomes `downgraded_due_to_no_grounding` and the final promotion status is downgraded to exploratory.

### Workbook Transparency Requirements

The workbook must make all of the following visible:

- final promotion status
- base promotion status
- grounding status
- combined promotion-grounding state
- grounding reason
- whether a selected example is grounded or weak fallback
- why a fallback was selected
- mismatch counts and grounding-fit metadata

This policy is implemented through `config/example_selection.yaml` and enforced in selection, promotion integration, workbook rendering, and tests.