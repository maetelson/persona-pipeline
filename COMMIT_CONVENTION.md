# Commit Message Convention

Use this repository format for commits:

```text
type(scope): short summary
```

## Rules

- Write the summary in imperative mood.
- Keep the first line concise and preferably under 72 characters.
- Match the scope to the pipeline stage or repo area being changed.
- Use one commit for one clear unit of change when practical.

## Types

- `feat`: new pipeline capability
- `fix`: bug fix or data-contract correction
- `refactor`: structure or data-flow cleanup without intended behavior change
- `docs`: documentation only
- `test`: tests only
- `chore`: maintenance, config, or dependency updates

## Recommended Scopes

- `collect`
- `normalize`
- `filter`
- `episodes`
- `label`
- `analysis`
- `export`
- `config`
- `docs`
- `tests`
- `repo`

## Examples

- `feat(collect): add review site manual ingest flow`
- `fix(export): validate workbook sheet presence`
- `refactor(analysis): simplify final report assembly`
- `docs(repo): add commit message convention`
