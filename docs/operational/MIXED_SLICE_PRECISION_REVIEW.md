# Mixed Slice Precision Review

## Scope

- reviewed rows: `200`
- Google reviewed rows: `100`
- Adobe reviewed rows: `100`

## Google

- `report_delivery_ui`: sample `100`, evidence `49`, mixed `31`, debt `12`, ambiguous `8`, evidence_or_mixed_precision `0.8`, debt_risk `0.12`
- `sharing_permissions_delivery` and `report_logic_and_filters` remain credible evidence slices under current audit assumptions.
- Google could stop being weak under refined policy: `True`

## Adobe

- `workspace_reporting`: sample `40`, evidence `27`, mixed `4`, debt `5`, ambiguous `1`, evidence_or_mixed_precision `0.775`, debt_risk `0.2`
- `implementation_tracking`: sample `10`, evidence `1`, mixed `0`, debt `4`, ambiguous `5`, evidence_or_mixed_precision `0.1`, debt_risk `0.4`
- `api_admin_config`: sample `15`, evidence `7`, mixed `0`, debt `3`, ambiguous `5`, evidence_or_mixed_precision `0.4667`, debt_risk `0.2`
- `other_operational`: sample `35`, evidence `19`, mixed `1`, debt `12`, ambiguous `1`, evidence_or_mixed_precision `0.5714`, debt_risk `0.4`
- Adobe could stop being weak under refined policy: `True`

## Refined Scenario Simulation

- `A_current_mixed_retained`: weak `2`, core-readiness weak `2`, balance `6.04`, still weak `adobe_analytics_community | google_developer_forums`, google `slice_aware_mixed`, adobe `slice_aware_mixed`
- `B_keep_reviewed_evidence_plus_mixed`: weak `0`, core-readiness weak `0`, balance `5.8`, still weak `none`, google `evidence_rescued`, adobe `evidence_rescued`
- `C_move_reviewed_debt_into_debt`: weak `0`, core-readiness weak `0`, balance `5.87`, still weak `none`, google `evidence_rescued`, adobe `evidence_rescued`
- `D_split_adobe_mixed`: weak `1`, core-readiness weak `1`, balance `5.89`, still weak `google_developer_forums`, google `slice_aware_mixed`, adobe `evidence_rescued`
- `E_split_google_mixed`: weak `1`, core-readiness weak `1`, balance `6.01`, still weak `adobe_analytics_community`, google `evidence_rescued`, adobe `slice_aware_mixed`
- `F_conservative_high_conf_debt_only`: weak `0`, core-readiness weak `0`, balance `5.9`, still weak `none`, google `evidence_rescued`, adobe `evidence_rescued`

## Recommendation

- next action: `implement_refined_slice_classification_diagnostics`
- rationale: Google mixed rows now look refinable toward evidence-leaning delivery sub-slices, while Adobe needs explicit workspace vs implementation/admin splits. That is enough to justify a diagnostics-only refinement pass, but not yet a secondary weak-source metric.
