# Codebook

`config/codebook.yaml` drives the rule-based prelabeler.

## Current code families

### Role

- `R_ANALYST`
- `R_MANAGER`
- `R_MARKETER`

### Moment

- `M_REPORTING`
- `M_VALIDATION`
- `M_TRIAGE`
- `M_AUTOMATION`

### Question

- `Q_REPORT_SPEED`
- `Q_VALIDATE_NUMBERS`
- `Q_DIAGNOSE_ISSUE`
- `Q_AUTOMATE_WORKFLOW`

### Pain

- `P_MANUAL_REPORTING`
- `P_DATA_QUALITY`
- `P_TOOL_LIMITATION`
- `P_HANDOFF`

### Environment

- `E_SPREADSHEET`
- `E_SQL_BI`
- `E_PYTHON`
- `E_WAREHOUSE`

### Workaround

- `W_MANUAL`
- `W_SPREADSHEET`
- `W_SCRIPT`

### Output

- `O_XLSX`
- `O_DASHBOARD`
- `O_VALIDATED_DATASET`
- `O_AUTOMATION_JOB`

### Fit

- `F_STRONG`
- `F_REVIEW`

## Labeling policy

- rule labeler fills only clear matches
- uncertain cases remain `unknown`
- LLM hook is only for `unknown` or low-confidence rows
- allowed output must stay within defined codes

## Current limitations

- code granularity is still coarse
- multiple codes may be assigned in one family
- no strict hierarchical ontology yet
- unknown-heavy episodes are expected
