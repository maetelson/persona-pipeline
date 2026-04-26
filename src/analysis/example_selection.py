"""Interpretable representative-example scoring and selection."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.source_tiers import source_tier_payload
from src.utils.io import ensure_dir, load_yaml
from src.utils.record_access import get_episode_id, get_record_source, get_record_text, get_record_value
from src.utils.text import clean_text, make_dedupe_key

HTML_TAG_RE = re.compile(r"<[^>]+>")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
TOKEN_RE = re.compile(r"[a-z0-9]+")


def load_example_selection_config(root_dir: Path) -> dict[str, Any]:
    """Load example-selection config."""
    return load_yaml(root_dir / "config" / "example_selection.yaml")


def select_persona_representative_examples(
    persona_source_df: pd.DataFrame,
    axis_names: list[str],
    config: dict[str, Any],
    max_items: int = 8,
) -> dict[str, pd.DataFrame | dict[str, list[str]] | str]:
    """Select strong representative examples per persona and write audit-friendly tables."""
    selected_rows: list[dict[str, Any]] = []
    borderline_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    summary_lookup: dict[str, list[str]] = {}

    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        dominant_axes = {axis: _dominant_axis_value(group, axis) for axis in axis_names}
        candidates = [_score_candidate(row, dominant_axes, axis_names, config) for _, row in group.iterrows()]
        ranked = sorted(candidates, key=lambda item: (-float(item["final_example_score"]), str(item["episode_id"])))
        selected = _select_diverse_examples(ranked, max_items=max_items, config=config)
        selected = _apply_curated_persona_example_policy(
            persona_id=str(persona_id),
            ranked=ranked,
            selected=selected,
            config=config,
            max_items=max_items,
        )
        selected_ids = {str(item["episode_id"]) for item in selected}
        for rank, item in enumerate(selected, start=1):
            item["persona_id"] = str(persona_id)
            item["example_rank"] = rank
            item["selection_decision"] = "selected"
            item["selection_strength"] = str(item.get("selection_strength", "grounded") or "grounded")
            item["fallback_selected"] = bool(item.get("fallback_selected", False))
            item["coverage_selection_reason"] = str(item.get("coverage_selection_reason", "score_plus_diversity_policy") or "score_plus_diversity_policy")
            selected_rows.append(item)
        for item in ranked:
            item["persona_id"] = str(persona_id)
            if str(item["episode_id"]) in selected_ids:
                audit_rows.append(item)
                continue
            if item["quote_quality"] == "borderline":
                item["selection_decision"] = "borderline"
                borderline_rows.append(item)
            else:
                item["selection_decision"] = "rejected"
                rejected_rows.append(item)
            audit_rows.append(item)
        summary_lookup[str(persona_id)] = [str(item["grounded_text"]) for item in selected[:2]]

    selected_df = pd.DataFrame(selected_rows)
    borderline_df = pd.DataFrame(borderline_rows)
    rejected_df = pd.DataFrame(rejected_rows)
    audit_df = pd.DataFrame(audit_rows)
    markdown = _build_examples_markdown(selected_df)
    return {
        "selected_df": selected_df,
        "borderline_df": borderline_df,
        "rejected_df": rejected_df,
        "audit_df": audit_df,
        "summary_lookup": summary_lookup,
        "markdown": markdown,
    }


def ensure_promoted_persona_grounding(
    selected_df: pd.DataFrame,
    audit_df: pd.DataFrame,
    promoted_persona_ids: list[str],
    config: dict[str, Any],
    max_items_per_persona: int = 6,
) -> dict[str, Any]:
    """Backward-compatible wrapper for explicit promotion-grounding policy."""
    return apply_promotion_grounding_policy(
        selected_df=selected_df,
        audit_df=audit_df,
        promoted_persona_ids=promoted_persona_ids,
        config=config,
        max_items_per_persona=max_items_per_persona,
    )


def apply_promotion_grounding_policy(
    selected_df: pd.DataFrame,
    audit_df: pd.DataFrame,
    promoted_persona_ids: list[str],
    config: dict[str, Any],
    max_items_per_persona: int = 6,
) -> dict[str, Any]:
    """Link promoted-persona coverage to explicit grounding states."""
    selected = selected_df.copy() if selected_df is not None else pd.DataFrame()
    audit = audit_df.copy() if audit_df is not None else pd.DataFrame()
    if selected.empty and audit.empty:
        return {
            "selected_df": selected,
            "audit_df": audit,
            "missing_persona_ids": promoted_persona_ids,
            "persona_grounding_df": pd.DataFrame(),
        }
    weak_label = str(config.get("policy", {}).get("fallback", {}).get("weak_selection_strength_label", "weak_grounding_fallback"))
    coverage_reason = str(config.get("policy", {}).get("fallback", {}).get("coverage_selection_reason", "minimum_coverage_policy"))
    ungrounded_action = str(config.get("policy", {}).get("promotion_grounding", {}).get("ungrounded_action", "flag") or "flag").strip().lower()
    selected_ids = set(selected.get("episode_id", pd.Series(dtype=str)).astype(str).tolist()) if not selected.empty else set()
    fallback_rows: list[dict[str, Any]] = []
    missing_persona_ids: list[str] = []
    grounding_rows: list[dict[str, Any]] = []
    grounding_debug_rows: list[dict[str, Any]] = []
    for persona_id in promoted_persona_ids:
        persona_key = str(persona_id)
        persona_selected = selected[selected.get("persona_id", pd.Series(dtype=str)).astype(str).eq(persona_key)].copy() if not selected.empty else pd.DataFrame()
        grounded_selected = persona_selected[
            persona_selected.get("grounding_strength", pd.Series(dtype=str)).astype(str).isin({"strong", "grounded"})
        ] if not persona_selected.empty else pd.DataFrame()
        weak_selected = persona_selected[
            persona_selected.get("selection_strength", pd.Series(dtype=str)).astype(str).eq(weak_label)
        ] if not persona_selected.empty else pd.DataFrame()
        persona_candidates = audit[audit.get("persona_id", pd.Series(dtype=str)).astype(str).eq(persona_key)].copy()
        grounded_candidate_count = int(
            persona_candidates.get("grounding_strength", pd.Series(dtype=str)).astype(str).isin({"strong", "grounded"}).sum()
        ) if not persona_candidates.empty else 0
        weak_candidate_count = int(
            persona_candidates.get("grounding_strength", pd.Series(dtype=str)).astype(str).eq("weak").sum()
        ) if not persona_candidates.empty else 0
        rejected_candidate_count = int(
            persona_candidates.get("grounding_strength", pd.Series(dtype=str)).astype(str).eq("unacceptable").sum()
        ) if not persona_candidates.empty else 0
        bundle = _bundle_grounding_evidence(persona_candidates, config)
        rejected_by_diversity_count = _grounding_diversity_rejection_count(persona_candidates)

        final_status = _policy_status(config, "promoted_and_grounded_status", "promoted_and_grounded")
        grounding_status = "grounded_single"
        grounding_reason = "has at least one grounded representative example selected by score and diversity policy"

        if grounded_selected.empty and bundle["bundle_grounding_status"] == "grounded_bundle":
            bundle_example = _pick_bundle_materialized_example(
                persona_candidates=persona_candidates,
                selected_df=selected,
                selected_ids=selected_ids,
                bundle=bundle,
                config=config,
            )
            if bundle_example is not None:
                next_rank = (
                    int(persona_selected["example_rank"].max()) + 1
                    if not persona_selected.empty and "example_rank" in persona_selected.columns
                    else 1
                )
                standalone_grounding_strength = str(bundle_example.get("grounding_strength", "") or "")
                standalone_quote_quality = str(bundle_example.get("quote_quality", "") or "")
                bundle_example["persona_id"] = persona_key
                bundle_example["example_rank"] = next_rank
                bundle_example["selection_decision"] = "policy_bundle_support_selected"
                bundle_example["fallback_selected"] = True
                bundle_example["fallback_mode"] = "bundle_materialization"
                bundle_example["coverage_selection_reason"] = "bundle_grounding_materialization"
                bundle_example["bundle_grounded_example"] = True
                bundle_example["standalone_grounding_strength"] = standalone_grounding_strength
                bundle_example["standalone_quote_quality"] = standalone_quote_quality
                bundle_example["selection_strength"] = "grounded"
                bundle_example["grounding_strength"] = "grounded"
                bundle_example["grounding_reason"] = (
                    "grounding_strength=grounded_via_bundle; "
                    f"standalone_grounding_strength={standalone_grounding_strength}; "
                    f"standalone_quote_quality={standalone_quote_quality}; "
                    f"bundle_grounding_status={bundle['bundle_grounding_status']}"
                )
                bundle_example["why_selected"] = (
                    "Bundle grounding materialized this representative example because the promoted persona "
                    "was grounded by multi-episode evidence but had no selected representative row. "
                    + str(bundle_example.get("why_selected", "") or "")
                ).strip()
                fallback_rows.append(bundle_example)
                selected_ids.add(str(bundle_example.get("episode_id", "")))
                grounded_selected = pd.DataFrame([bundle_example])

        if grounded_selected.empty:
            if bundle["bundle_grounding_status"] == "grounded_bundle":
                grounding_status = "grounded_bundle"
                grounding_reason = str(bundle["bundle_grounding_reason"])
                final_status = _policy_status(config, "promoted_and_grounded_status", "promoted_and_grounded")
            else:
                if weak_selected.empty:
                    fallback = _pick_promoted_fallback(persona_candidates, selected, selected_ids, config)
                    if fallback is not None:
                        next_rank = (
                            int(persona_selected["example_rank"].max()) + 1
                            if not persona_selected.empty and "example_rank" in persona_selected.columns
                            else 1
                        )
                        fallback["persona_id"] = persona_key
                        fallback["example_rank"] = next_rank
                        fallback["selection_decision"] = "policy_fallback_selected"
                        fallback["fallback_selected"] = True
                        fallback["fallback_mode"] = "standard"
                        fallback["coverage_selection_reason"] = coverage_reason
                        fallback["grounding_warning"] = _fallback_grounding_warning(fallback)
                        if str(fallback.get("grounding_strength", "") or "") in {"strong", "grounded"}:
                            fallback["selection_strength"] = "grounded"
                            final_status = _policy_status(config, "promoted_and_grounded_status", "promoted_and_grounded")
                            grounding_status = "grounded_single"
                            grounding_reason = "coverage policy selected an additional grounded example because the promoted persona had no selected grounding row"
                        else:
                            fallback["selection_strength"] = weak_label
                            final_status = _policy_status(config, "promoted_but_weakly_grounded_status", "promoted_but_weakly_grounded")
                            grounding_status = "weak_bundle"
                            grounding_reason = "only weak fallback evidence met policy; workbook must label the persona as weakly grounded"
                        fallback["why_selected"] = (
                            "Coverage policy selected this example because the promoted persona lacked grounded coverage. "
                            + str(fallback.get("why_selected", "") or "")
                            + (f" Weakness: {fallback['grounding_warning']}." if fallback["grounding_warning"] else "")
                        ).strip()
                        fallback_rows.append(fallback)
                        selected_ids.add(str(fallback.get("episode_id", "")))
                        if str(fallback.get("selection_strength", "") or "") == weak_label:
                            weak_selected = pd.DataFrame([fallback])
                        else:
                            grounded_selected = pd.DataFrame([fallback])
                    else:
                        salvage_fallback = _pick_promoted_salvage_fallback(persona_candidates, selected, selected_ids, config)
                        if salvage_fallback is not None:
                            next_rank = (
                                int(persona_selected["example_rank"].max()) + 1
                                if not persona_selected.empty and "example_rank" in persona_selected.columns
                                else 1
                            )
                            salvage_fallback["persona_id"] = persona_key
                            salvage_fallback["example_rank"] = next_rank
                            salvage_fallback["selection_decision"] = "policy_salvage_fallback_selected"
                            salvage_fallback["fallback_selected"] = True
                            salvage_fallback["fallback_mode"] = "salvage"
                            salvage_fallback["coverage_selection_reason"] = coverage_reason
                            salvage_fallback["selection_strength"] = weak_label
                            salvage_fallback["grounding_strength"] = "weak"
                            salvage_fallback["grounding_warning"] = _fallback_grounding_warning(salvage_fallback)
                            salvage_fallback["why_selected"] = (
                                "Coverage salvage selected this example because the promoted persona had no grounded or weak "
                                "candidate under strict policy and this row still has strong bottleneck evidence. "
                                + str(salvage_fallback.get("why_selected", "") or "")
                                + (f" Weakness: {salvage_fallback['grounding_warning']}." if salvage_fallback["grounding_warning"] else "")
                            ).strip()
                            fallback_rows.append(salvage_fallback)
                            selected_ids.add(str(salvage_fallback.get("episode_id", "")))
                            weak_selected = pd.DataFrame([salvage_fallback])
                if grounded_selected.empty:
                    if bundle["bundle_grounding_status"] == "weak_bundle":
                        final_status = _policy_status(config, "promoted_but_weakly_grounded_status", "promoted_but_weakly_grounded")
                        grounding_status = "weak_bundle"
                        grounding_reason = str(bundle["bundle_grounding_reason"])
                    elif not weak_selected.empty:
                        final_status = _policy_status(config, "promoted_but_weakly_grounded_status", "promoted_but_weakly_grounded")
                        grounding_status = "weak_bundle"
                        grounding_reason = "only weak fallback evidence is selected for this promoted persona"
                    else:
                        missing_persona_ids.append(persona_key)
                        grounding_status = "ungrounded"
                        grounding_reason = str(bundle["bundle_grounding_reason"] or "no grounded or weak fallback candidate met policy thresholds")
                        if ungrounded_action == "downgrade":
                            final_status = _policy_status(config, "downgraded_due_to_no_grounding_status", "downgraded_due_to_no_grounding")
                        else:
                            final_status = _policy_status(config, "promoted_but_ungrounded_status", "promoted_but_ungrounded")

        grounding_rows.append(
            {
                "persona_id": persona_key,
                "grounding_status": grounding_status,
                "promotion_grounding_status": final_status,
                "grounding_reason": grounding_reason,
                "grounded_candidate_count": grounded_candidate_count,
                "weak_candidate_count": weak_candidate_count,
                "rejected_candidate_count": rejected_candidate_count,
                "context_evidence_count": int(bundle["context_evidence_count"]),
                "workaround_evidence_count": int(bundle["workaround_evidence_count"]),
                "trust_validation_evidence_count": int(bundle["trust_validation_evidence_count"]),
                "bundle_episode_count": int(bundle["bundle_episode_count"]),
                "bundle_dimension_hits": int(bundle["bundle_dimension_hits"]),
                "total_bundle_strength": int(bundle["total_bundle_strength"]),
                "bundle_grounding_status": str(bundle["bundle_grounding_status"]),
                "bundle_grounding_reason": str(bundle["bundle_grounding_reason"]),
                "context_evidence_episode_ids": str(bundle["context_evidence_episode_ids"]),
                "workaround_evidence_episode_ids": str(bundle["workaround_evidence_episode_ids"]),
                "trust_validation_evidence_episode_ids": str(bundle["trust_validation_evidence_episode_ids"]),
                "bundle_support_examples": str(bundle["bundle_support_examples"]),
                "selected_example_count": int(len(persona_selected)) + (1 if fallback_rows and str(fallback_rows[-1].get("persona_id", "")) == persona_key else 0),
                "fallback_selected_count": int(len(weak_selected)),
            }
        )
        grounding_debug_rows.append(
            {
                "persona_id": persona_key,
                "candidate_count_before_filter": int(bundle.get("candidate_count_before_filter", 0) or 0),
                "candidate_count_after_filter": int(bundle.get("candidate_count_after_filter", 0) or 0),
                "context_evidence_count": int(bundle.get("context_evidence_count", 0) or 0),
                "workaround_evidence_count": int(bundle.get("workaround_evidence_count", 0) or 0),
                "trust_validation_evidence_count": int(bundle.get("trust_validation_evidence_count", 0) or 0),
                "rejected_by_threshold_count": int(bundle.get("rejected_by_threshold_count", 0) or 0),
                "rejected_by_diversity_count": int(rejected_by_diversity_count),
                "rejected_by_mismatch_count": int(bundle.get("rejected_by_mismatch_count", 0) or 0),
                "grounding_status": grounding_status,
                "promotion_grounding_status": final_status,
                "bundle_grounding_status": str(bundle.get("bundle_grounding_status", "") or ""),
                "bundle_episode_count": int(bundle.get("bundle_episode_count", 0) or 0),
                "bundle_support_examples": str(bundle.get("bundle_support_examples", "") or ""),
                "context_evidence_episode_ids": str(bundle.get("context_evidence_episode_ids", "") or ""),
                "workaround_evidence_episode_ids": str(bundle.get("workaround_evidence_episode_ids", "") or ""),
                "trust_validation_evidence_episode_ids": str(bundle.get("trust_validation_evidence_episode_ids", "") or ""),
                "selected_example_count": int(len(persona_selected)) + (1 if fallback_rows and str(fallback_rows[-1].get("persona_id", "")) == persona_key else 0),
                "final_grounding_fail_reason": "" if final_status == _policy_status(config, "promoted_and_grounded_status", "promoted_and_grounded") else grounding_reason,
            }
        )
    if fallback_rows:
        fallback_df = pd.DataFrame(fallback_rows)
        selected = pd.concat([selected, fallback_df], ignore_index=True) if not selected.empty else fallback_df
        if not audit.empty:
            audit = audit.copy()
            for _, row in fallback_df.iterrows():
                mask = audit.get("episode_id", pd.Series(dtype=str)).astype(str).eq(str(row.get("episode_id", "")))
                audit.loc[mask, "selection_decision"] = str(row.get("selection_decision", "promoted_fallback_selected"))
                audit.loc[mask, "selection_strength"] = str(row.get("selection_strength", weak_label))
                audit.loc[mask, "grounding_warning"] = str(row.get("grounding_warning", ""))
                audit.loc[mask, "example_rank"] = int(row.get("example_rank", 1) or 1)
                audit.loc[mask, "why_selected"] = str(row.get("why_selected", "") or "")
                audit.loc[mask, "fallback_selected"] = bool(row.get("fallback_selected", False))
                audit.loc[mask, "coverage_selection_reason"] = str(row.get("coverage_selection_reason", "") or "")
                audit.loc[mask, "fallback_mode"] = str(row.get("fallback_mode", "standard") or "standard")
    if not selected.empty:
        sort_columns = [column for column in ["persona_id", "example_rank", "final_example_score"] if column in selected.columns]
        ascending = [value for column, value in zip(["persona_id", "example_rank", "final_example_score"], [True, True, False]) if column in sort_columns]
        if sort_columns:
            selected = selected.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)
        selected["example_rank"] = (
            selected.groupby("persona_id", dropna=False).cumcount() + 1
        )
    return {
        "selected_df": selected,
        "audit_df": audit,
        "missing_persona_ids": missing_persona_ids,
        "persona_grounding_df": pd.DataFrame(grounding_rows),
        "grounding_debug_df": pd.DataFrame(grounding_debug_rows),
    }


def build_legacy_representative_examples(persona_source_df: pd.DataFrame, max_items: int = 8) -> pd.DataFrame:
    """Reproduce the old representative-example selection for before/after comparison."""
    rows: list[dict[str, Any]] = []
    for persona_id, group in persona_source_df.groupby("persona_id", dropna=False):
        ranked = group.sort_values(["label_confidence", "episode_id"], ascending=[False, True])
        seen: set[str] = set()
        rank = 1
        for _, row in ranked.iterrows():
            text = clean_text(str(row.get("normalized_episode", "") or ""))[:500]
            if not text or text in seen:
                continue
            seen.add(text)
            rows.append(
                {
                    "persona_id": str(persona_id),
                    "example_rank": rank,
                    "grounded_text": text,
                    "reason_selected": "high confidence + dominant axis match",
                }
            )
            rank += 1
            if rank > max_items:
                break
    return pd.DataFrame(rows)


def select_cluster_representative_texts(cluster_rows: pd.DataFrame, config: dict[str, Any], max_items: int = 8) -> list[str]:
    """Return strong representative texts for cluster-level profiling."""
    candidates = [_score_candidate(row, dominant_axes={}, axis_names=[], config=config) for _, row in cluster_rows.iterrows()]
    selected = _select_diverse_examples(sorted(candidates, key=lambda item: -float(item["final_example_score"])), max_items=max_items, config=config)
    return [str(item["grounded_text"]) for item in selected]


def write_example_selection_outputs(root_dir: Path, outputs: dict[str, Any]) -> dict[str, Path]:
    """Persist example-selection artifacts."""
    output_dir = ensure_dir(root_dir / "data" / "analysis")
    paths = {
        "representative_examples_v2_csv": output_dir / "representative_examples_v2.csv",
        "representative_examples_by_persona_md": output_dir / "representative_examples_by_persona.md",
        "rejected_example_samples_csv": output_dir / "rejected_example_samples.csv",
        "borderline_example_samples_csv": output_dir / "borderline_example_samples.csv",
        "example_selection_audit_csv": output_dir / "example_selection_audit.csv",
    }
    outputs["selected_df"].to_csv(paths["representative_examples_v2_csv"], index=False)
    outputs["borderline_df"].head(200).to_csv(paths["borderline_example_samples_csv"], index=False)
    outputs["rejected_df"].head(200).to_csv(paths["rejected_example_samples_csv"], index=False)
    outputs["audit_df"].to_csv(paths["example_selection_audit_csv"], index=False)
    paths["representative_examples_by_persona_md"].write_text(str(outputs["markdown"]), encoding="utf-8")
    return paths


def compare_example_selection(before_df: pd.DataFrame, after_df: pd.DataFrame) -> pd.DataFrame:
    """Compare old vs new representative example quality at a coarse level."""
    before = _selection_quality_summary(before_df, text_column="grounded_text")
    after = _selection_quality_summary(after_df, text_column="grounded_text")
    rows = []
    for metric in sorted(set(before) | set(after)):
        rows.append(
            {
                "metric_name": metric,
                "before_value": before.get(metric, 0.0),
                "after_value": after.get(metric, 0.0),
                "delta": round(float(after.get(metric, 0.0)) - float(before.get(metric, 0.0)), 4),
            }
        )
    return pd.DataFrame(rows)


def _score_candidate(row: pd.Series, dominant_axes: dict[str, str], axis_names: list[str], config: dict[str, Any]) -> dict[str, Any]:
    """Score one candidate example with interpretable dimensions."""
    full_text = _source_text(row)
    snippet = _extract_best_snippet(full_text, config)
    text = snippet.lower()
    weights = config.get("weights", {})
    positive_patterns = config.get("positive_patterns", {})
    negative_patterns = config.get("negative_patterns", {})
    alignment = _persona_axis_alignment(row, dominant_axes, axis_names)
    output_stakeholder_alignment = _output_stakeholder_alignment_score(
        dominant_axes=dominant_axes,
        score_breakdown_seed={
            "output_need_score": _pattern_score(text, positive_patterns.get("output_need", [])),
            "stakeholder_pressure_score": _pattern_score(text, ["stakeholder", "leadership", "executive", "board report", "business review"]),
            "validation_pressure_score": _pattern_score(text, positive_patterns.get("validation_pressure", [])),
        },
    )

    score_breakdown = {
        "explicit_workflow_pain_score": _pattern_score(text, positive_patterns.get("explicit_workflow_pain", [])),
        "repeated_manual_workaround_score": _pattern_score(text, positive_patterns.get("repeated_manual_workaround", [])),
        "explanation_burden_score": _pattern_score(text, positive_patterns.get("explanation_burden", [])),
        "validation_pressure_score": _pattern_score(text, positive_patterns.get("validation_pressure", [])),
        "non_genericness_score": _non_genericness_score(text),
        "bottleneck_specificity_score": _pattern_score(text, positive_patterns.get("bottleneck_specificity", [])),
        "workflow_context_score": _pattern_score(text, positive_patterns.get("workflow_context", [])),
        "business_context_score": _pattern_score(text, positive_patterns.get("business_context", [])),
        "stakeholder_pressure_score": _pattern_score(text, ["stakeholder", "leadership", "executive", "board report", "business review"]),
        "reporting_pain_score": _pattern_score(text, ["report", "reporting", "weekly report", "monthly report", "deadline"]),
        "dashboard_trust_score": _pattern_score(text, ["dashboard", "don't trust", "reconcile", "numbers don't match"]),
        "excel_rework_score": _pattern_score(text, ["excel", "xlsx", "spreadsheet", "csv", "manual spreadsheet", "copy paste"]),
        "adhoc_analysis_score": _pattern_score(text, ["ad hoc", "follow-up", "stakeholder asks", "one-off request"]),
        "root_cause_score": _pattern_score(text, ["why did", "can't explain", "root cause", "drove it"]),
        "metric_definition_score": _pattern_score(text, ["metric definition", "finance definition", "source of truth", "numbers don't match"]),
        "output_need_score": _pattern_score(text, positive_patterns.get("output_need", [])),
        "persona_fit_score": float(alignment["persona_fit_score"]),
        "defining_axis_match_score": float(alignment["defining_axis_match_score"]),
        "grounding_fit_score": float(_grounding_fit_score(alignment, output_stakeholder_alignment)),
        "output_stakeholder_alignment_score": float(output_stakeholder_alignment),
        "genericness_penalty": _pattern_score(text, negative_patterns.get("genericness", [])),
        "technical_noise_penalty": _pattern_score(text, negative_patterns.get("technical_noise", [])),
        "generic_product_statement_penalty": _pattern_score(text, negative_patterns.get("generic_product_statement", [])),
        "source_specific_example_penalty": _source_specific_example_penalty(source=str(get_record_source(row) or ""), text=text),
        "no_user_pain_context_penalty": _no_user_pain_context_penalty(text),
        "short_no_workflow_evidence_penalty": _short_no_workflow_penalty(text),
        "critical_axis_mismatch_penalty": float(alignment["critical_mismatch_count"]),
        "major_axis_mismatch_penalty": float(1.0 if alignment["mismatch_count"] > int(config.get("thresholds", {}).get("max_selected_mismatch_axes", 2)) else 0.0),
        "self_reported_mismatch_penalty": float(1.0 if alignment["critical_mismatch_count"] > 0 or alignment["mismatch_count"] > 0 else 0.0),
        "duplicate_penalty": 0.0,
    }
    final_example_score = (
        score_breakdown["explicit_workflow_pain_score"] * float(weights.get("explicit_workflow_pain", 1.8))
        + score_breakdown["repeated_manual_workaround_score"] * float(weights.get("repeated_manual_workaround", 1.5))
        + score_breakdown["explanation_burden_score"] * float(weights.get("explanation_burden", 1.4))
        + score_breakdown["validation_pressure_score"] * float(weights.get("validation_pressure", 1.4))
        + score_breakdown["non_genericness_score"] * float(weights.get("non_genericness", 0.8))
        + score_breakdown["bottleneck_specificity_score"] * float(weights.get("bottleneck_specificity", 1.0))
        + score_breakdown["workflow_context_score"] * float(weights.get("workflow_context", 1.0))
        + score_breakdown["business_context_score"] * float(weights.get("business_context", 1.0))
        + score_breakdown["stakeholder_pressure_score"] * float(weights.get("stakeholder_pressure", 1.0))
        + score_breakdown["reporting_pain_score"] * float(weights.get("reporting_pain", 1.0))
        + score_breakdown["dashboard_trust_score"] * float(weights.get("dashboard_trust", 1.0))
        + score_breakdown["excel_rework_score"] * float(weights.get("excel_rework", 1.0))
        + score_breakdown["adhoc_analysis_score"] * float(weights.get("adhoc_analysis", 1.0))
        + score_breakdown["root_cause_score"] * float(weights.get("root_cause", 1.0))
        + score_breakdown["metric_definition_score"] * float(weights.get("metric_definition", 1.0))
        + score_breakdown["output_need_score"] * float(weights.get("output_need", 1.0))
        + score_breakdown["persona_fit_score"] * float(weights.get("persona_fit", 1.0))
        + score_breakdown["defining_axis_match_score"] * float(weights.get("defining_axis_match", 1.0))
        + score_breakdown["grounding_fit_score"] * float(weights.get("grounding_fit", 1.0))
        + score_breakdown["output_stakeholder_alignment_score"] * float(weights.get("output_stakeholder_alignment", 1.0))
        - score_breakdown["genericness_penalty"] * float(weights.get("genericness_penalty", 1.0))
        - score_breakdown["technical_noise_penalty"] * float(weights.get("technical_noise_penalty", 1.0))
        - score_breakdown["generic_product_statement_penalty"] * float(weights.get("generic_product_statement_penalty", 2.2))
        - score_breakdown["source_specific_example_penalty"] * float(weights.get("source_specific_example_penalty", 2.0))
        - score_breakdown["no_user_pain_context_penalty"] * float(weights.get("no_user_pain_context_penalty", 2.0))
        - score_breakdown["short_no_workflow_evidence_penalty"] * float(weights.get("short_no_workflow_evidence_penalty", 2.0))
        - score_breakdown["critical_axis_mismatch_penalty"] * float(weights.get("critical_axis_mismatch_penalty", 1.0))
        - score_breakdown["major_axis_mismatch_penalty"] * float(weights.get("major_axis_mismatch_penalty", 1.0))
        - score_breakdown["self_reported_mismatch_penalty"] * float(weights.get("self_reported_mismatch_penalty", 1.0))
    )
    quote_quality = _quote_quality(score_breakdown, final_example_score, config)
    cluster_fit_reason = _cluster_fit_reason(row, dominant_axes, axis_names)
    top_positive_signals = _top_signals(score_breakdown, positive=True)
    top_negative_signals = _top_signals(score_breakdown, positive=False)
    rejection_reason = _rejection_reason(score_breakdown, quote_quality)
    source = str(get_record_source(row) or "")
    tier_payload = source_tier_payload(source)
    candidate = {
        "episode_id": get_episode_id(row),
        "source": source,
        "source_tier": str(tier_payload["source_tier"]),
        "grounded_text": snippet,
        "quote_quality": quote_quality,
        "top_positive_signals": json.dumps(top_positive_signals, ensure_ascii=False),
        "top_negative_signals": json.dumps(top_negative_signals, ensure_ascii=False),
        "score_breakdown": json.dumps(score_breakdown, ensure_ascii=False),
        "cluster_fit_reason": cluster_fit_reason,
        "rejection_reason": rejection_reason,
        "subpattern_label": _subpattern_label(text, config),
        "final_example_score": round(float(final_example_score), 4),
        "mismatch_count": int(alignment["mismatch_count"]),
        "critical_mismatch_count": int(alignment["critical_mismatch_count"]),
        "matched_axis_count": int(alignment["matched_count"]),
        "grounding_fit_score": round(float(score_breakdown["grounding_fit_score"]), 4),
        "selection_strength": "not_selected",
        "label_confidence": float(get_record_value(row, "label_confidence", 0.0) or 0.0),
        "source_text_length": len(snippet),
        "reason_selected": " | ".join(top_positive_signals[:3]) if top_positive_signals else "borderline or weak evidence",
        "why_selected": _why_selected(top_positive_signals, cluster_fit_reason),
        "matched_axes": _matched_axes(cluster_fit_reason),
        "fallback_selected": False,
        "coverage_selection_reason": "",
    }
    candidate["grounding_strength"] = _grounding_strength(candidate, config)
    candidate["fallback_eligible"] = bool(candidate["grounding_strength"] == "weak" and _allow_borderline_fallback(candidate, config))
    candidate["grounding_reason"] = _grounding_reason(candidate)
    return candidate


def _apply_curated_persona_example_policy(
    *,
    persona_id: str,
    ranked: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    config: dict[str, Any],
    max_items: int,
) -> list[dict[str, Any]]:
    """Apply narrow persona-specific curated example overrides without changing generic selection behavior."""
    curated_cfg = (
        config.get("curated_persona_examples", {}).get(str(persona_id), {})
        if isinstance(config.get("curated_persona_examples", {}), dict)
        else {}
    )
    if not curated_cfg:
        return selected

    preferred_ids = [str(value).strip() for value in list(curated_cfg.get("preferred_episode_ids", []) or []) if str(value).strip()]
    if not preferred_ids:
        return selected

    min_selected_examples = max(1, int(curated_cfg.get("min_selected_examples", 1) or 1))
    max_selected_examples = max(min_selected_examples, int(curated_cfg.get("max_selected_examples", max_items) or max_items))
    max_selected_examples = min(max_selected_examples, max_items)
    allowed_tiers = {
        str(value).strip()
        for value in list(
            curated_cfg.get(
                "allowed_source_tiers",
                ["core_representative_source", "supporting_validation_source"],
            )
            or []
        )
        if str(value).strip()
    }
    override_reason = str(
        curated_cfg.get(
            "override_reason",
            "Curated example override preserves a clearer persona boundary than the generic selector for this persona.",
        )
        or ""
    ).strip()

    ranked_lookup = {str(item.get("episode_id", "") or ""): item for item in ranked}
    curated: list[dict[str, Any]] = []
    for episode_id in preferred_ids:
        candidate = ranked_lookup.get(episode_id)
        if candidate is None:
            continue
        source_tier = str(candidate.get("source_tier", "") or "")
        if allowed_tiers and source_tier not in allowed_tiers:
            continue
        curated_candidate = dict(candidate)
        curated_candidate["selection_strength"] = "curated_override"
        curated_candidate["coverage_selection_reason"] = "persona_curated_example_override"
        curated_candidate["fallback_selected"] = False
        curated_candidate["curated_example_override"] = True
        curated_candidate["curated_example_override_reason"] = override_reason
        curated_candidate["why_selected"] = (
            f"{override_reason} "
            + str(curated_candidate.get("why_selected", "") or "")
        ).strip()
        curated.append(curated_candidate)
        if len(curated) >= max_selected_examples:
            break

    if len(curated) < min_selected_examples:
        return selected
    return curated


def _select_diverse_examples(candidates: list[dict[str, Any]], max_items: int, config: dict[str, Any]) -> list[dict[str, Any]]:
    """Select top examples with diversity and near-duplicate suppression."""
    selected: list[dict[str, Any]] = []
    selected_texts: list[str] = []
    selected_sources: set[str] = set()
    used_subpatterns: set[str] = set()
    duplicate_threshold = float(config.get("thresholds", {}).get("duplicate_similarity_threshold", 0.72))
    max_mismatch_axes = int(config.get("thresholds", {}).get("max_selected_mismatch_axes", 2))
    max_critical_mismatch_axes = int(config.get("thresholds", {}).get("max_selected_critical_mismatch_axes", 1))
    source_diversity_margin = float(config.get("policy", {}).get("diversity", {}).get("prefer_new_source_within_score_margin", config.get("thresholds", {}).get("source_diversity_score_margin", 1.25)))
    diversify_fraction = float(config.get("policy", {}).get("diversity", {}).get("diversify_subpatterns_until_slot_fraction", 0.5))
    for candidate in candidates:
        if candidate.get("grounding_strength") not in {"strong", "grounded"}:
            candidate["rejection_reason"] = str(candidate.get("rejection_reason", "") or "grounding strength below grounded selection floor")
            continue
        if int(candidate.get("mismatch_count", 0) or 0) > max_mismatch_axes:
            candidate["rejection_reason"] = "rejected by mismatch ceiling"
            continue
        if int(candidate.get("critical_mismatch_count", 0) or 0) > max_critical_mismatch_axes:
            candidate["rejection_reason"] = "rejected by critical mismatch ceiling"
            continue
        text = str(candidate["grounded_text"])
        duplicate_penalty = 0.0
        for prior in selected_texts:
            sim = _text_similarity(text, prior)
            if sim >= duplicate_threshold:
                duplicate_penalty = max(duplicate_penalty, sim)
        if duplicate_penalty > 0:
            candidate["duplicate_penalty"] = round(duplicate_penalty, 4)
            candidate["rejection_reason"] = "near-duplicate of a stronger selected example"
            continue
        subpattern = str(candidate.get("subpattern_label", "general_bottleneck"))
        source = str(candidate.get("source", "") or "")
        if source and source in selected_sources:
            better_source_option = any(
                str(other.get("source", "") or "") not in selected_sources
                and float(other.get("final_example_score", 0.0) or 0.0) >= float(candidate.get("final_example_score", 0.0) or 0.0) - source_diversity_margin
                and str(other.get("quote_quality", "")) in {"strong_representative", "usable"}
                and int(other.get("mismatch_count", 0) or 0) <= max_mismatch_axes
                and int(other.get("critical_mismatch_count", 0) or 0) <= max_critical_mismatch_axes
                for other in candidates
            )
            if better_source_option:
                candidate["rejection_reason"] = "held out for source diversity"
                continue
        if subpattern in used_subpatterns and len(selected) < max(1, int(max_items * diversify_fraction)):
            candidate["rejection_reason"] = "held out for subpattern diversity"
            continue
        selected.append(candidate)
        selected_texts.append(text)
        if source:
            selected_sources.add(source)
        used_subpatterns.add(subpattern)
        if len(selected) >= max_items:
            break
    return selected


def _extract_best_snippet(text: str, config: dict[str, Any]) -> str:
    """Extract the strongest local snippet instead of using the whole post verbatim."""
    cleaned = _strip_markup(text)
    sentences = [clean_text(part) for part in SENTENCE_SPLIT_RE.split(cleaned) if clean_text(part)]
    if not sentences:
        return cleaned[: int(config.get("snippet", {}).get("max_chars", 420))]
    scored: list[tuple[float, str]] = []
    for index, sentence in enumerate(sentences):
        snippet = sentence
        if index + 1 < len(sentences):
            snippet = f"{sentence} {sentences[index + 1]}"
        score = _snippet_strength(snippet)
        scored.append((score, snippet))
    scored.sort(key=lambda item: item[0], reverse=True)
    max_chars = int(config.get("snippet", {}).get("max_chars", 420))
    min_chars = int(config.get("snippet", {}).get("min_chars", 45))
    for _, snippet in scored:
        trimmed = snippet[:max_chars].strip()
        if len(trimmed) >= min_chars:
            return trimmed
    return clean_text(cleaned)[:max_chars]


def _source_text(row: pd.Series) -> str:
    """Build combined text for snippet extraction."""
    return get_record_text(row, fields=["normalized_episode", "evidence_snippet"])


def _cluster_fit_reason(row: pd.Series, dominant_axes: dict[str, str], axis_names: list[str]) -> str:
    """Explain why the example fits or misses the persona signature."""
    alignment = _persona_axis_alignment(row, dominant_axes, axis_names)
    matches: list[str] = []
    misses: list[str] = []
    for axis in axis_names[:6]:
        raw_value = str(row.get(axis, "") or "").strip().lower()
        dominant = str(dominant_axes.get(axis, "") or "").strip().lower()
        if not dominant or dominant == "unassigned":
            continue
        if raw_value == dominant:
            matches.append(f"{axis}={dominant}")
        else:
            misses.append(f"{axis}={raw_value or 'unassigned'} vs {dominant}")
    if matches:
        qualifier = ""
        if int(alignment["critical_mismatch_count"]) > 0:
            qualifier = " with major mismatch risk"
        return f"Matches persona on {', '.join(matches[:3])}{qualifier}" + (f"; misses {', '.join(misses[:2])}" if misses else "")
    return "Low direct axis match; keep only as weak grounding fallback when no stronger example exists."


def _quote_quality(score_breakdown: dict[str, float], final_score: float, config: dict[str, Any]) -> str:
    """Classify example quality."""
    thresholds = config.get("thresholds", {})
    has_bottleneck = score_breakdown["bottleneck_specificity_score"] > 0
    has_context = (
        score_breakdown["workflow_context_score"] > 0
        or score_breakdown["business_context_score"] > 0
        or score_breakdown["explicit_workflow_pain_score"] > 0
    )
    strong_work_context = (
        score_breakdown["explicit_workflow_pain_score"]
        + score_breakdown["repeated_manual_workaround_score"]
        + score_breakdown["explanation_burden_score"]
        + score_breakdown["validation_pressure_score"]
        + score_breakdown["workflow_context_score"]
        + score_breakdown["business_context_score"]
        + score_breakdown["output_need_score"]
        + score_breakdown["excel_rework_score"]
    ) >= 3
    hard_penalty = (
        score_breakdown["generic_product_statement_penalty"] >= 1
        or score_breakdown["no_user_pain_context_penalty"] >= 1
        or score_breakdown["short_no_workflow_evidence_penalty"] >= 1
    )
    if score_breakdown["technical_noise_penalty"] >= 2 and not has_bottleneck:
        return "reject"
    if score_breakdown["genericness_penalty"] >= 2 and not has_context:
        return "reject"
    if hard_penalty and not (has_bottleneck and strong_work_context):
        return "reject"
    if score_breakdown["source_specific_example_penalty"] >= 1 and not strong_work_context:
        return "reject"
    if final_score >= float(thresholds.get("strong_representative_min_score", 8.0)) and has_bottleneck and has_context:
        return "strong_representative"
    if final_score >= float(thresholds.get("usable_min_score", 5.5)) and (has_bottleneck or strong_work_context):
        return "usable"
    if final_score >= float(thresholds.get("borderline_min_score", 3.0)):
        return "borderline"
    return "reject"


def _rejection_reason(score_breakdown: dict[str, float], quality: str) -> str:
    """Return a short rejection reason when needed."""
    if quality in {"strong_representative", "usable"}:
        return ""
    if score_breakdown["technical_noise_penalty"] >= 2:
        return "technical implementation/debugging dominates the text"
    if score_breakdown["generic_product_statement_penalty"] >= 1:
        return "generic product statement, not a grounded user pain example"
    if score_breakdown["source_specific_example_penalty"] >= 1:
        return "support answer or personal workflow snippet is too far from the target analyst pain"
    if score_breakdown["no_user_pain_context_penalty"] >= 1:
        return "no clear user pain or workflow pressure"
    if score_breakdown["short_no_workflow_evidence_penalty"] >= 1:
        return "short answer with no workflow evidence"
    if score_breakdown["genericness_penalty"] >= 2:
        return "too generic or promotional to explain the workflow bottleneck"
    if score_breakdown["bottleneck_specificity_score"] <= 0:
        return "does not clearly expose the repeated work bottleneck"
    return "context is too weak or ambiguous for a strong representative example"


def _pattern_score(text: str, patterns: list[str]) -> float:
    """Count distinct matched phrases in a capped way."""
    hits = 0
    for pattern in patterns:
        if str(pattern).lower() in text:
            hits += 1
    return min(float(hits), 3.0)


def _non_genericness_score(text: str) -> float:
    """Reward concrete workflow evidence."""
    concrete_terms = ["because", "before", "after", "every", "weekly", "monthly", "stakeholder", "client", "manager", "leadership", "report", "dashboard", "spreadsheet", "reconcile", "export"]
    return min(float(sum(1 for term in concrete_terms if term in text)), 3.0)


def _source_specific_example_penalty(source: str, text: str) -> float:
    """Down-rank support-answer snippets and personal automation anecdotes that pollute grounding."""
    lowered = str(text or "").lower()
    penalty = 0.0
    support_answer_terms = [
        "if the issue persists",
        "for your reference",
        "accept it as a solution",
        "give a kudos",
        "best practices",
        "go through the below documentation",
        "you may manually export",
    ]
    personal_learning_terms = [
        "sharpen my python skills",
        "personal project",
        "learning exercise",
        "copy it manually",
        "paste it into a new workbook",
    ]
    if any(term in lowered for term in support_answer_terms):
        penalty += 1.0
    if source == "stackoverflow" and any(term in lowered for term in personal_learning_terms):
        penalty += 1.2
    if source == "github_discussions" and "minor release" in lowered and "downloads" in lowered and "workaround" in lowered:
        penalty += 0.8
    return round(min(penalty, 2.0), 4)


def _no_user_pain_context_penalty(text: str) -> float:
    """Penalize snippets without a user pain or pressure cue."""
    pain_terms = [
        "can't",
        "cannot",
        "need",
        "problem",
        "issue",
        "wrong",
        "mismatch",
        "manual",
        "reconcile",
        "blocked",
        "not enough",
        "how do i",
        "why",
        "what changed",
        "explain what changed",
        "before sending",
        "figure out",
        "source of truth",
    ]
    return 0.0 if any(term in text for term in pain_terms) else 1.0


def _short_no_workflow_penalty(text: str) -> float:
    """Penalize short implementation answers with no workflow context."""
    workflow_terms = ["report", "dashboard", "stakeholder", "weekly", "monthly", "excel", "spreadsheet", "campaign", "revenue", "conversion", "business"]
    return 1.0 if len(text) < 120 and not any(term in text for term in workflow_terms) else 0.0


def _why_selected(top_positive_signals: list[str], cluster_fit_reason: str) -> str:
    """Explain representative example selection in human terms."""
    signals = ", ".join(signal.replace("_score", "").replace("_", " ") for signal in top_positive_signals[:4])
    if not signals:
        signals = "weak but best available grounded evidence"
    return f"Selected for {signals}. {cluster_fit_reason}"


def _grounding_fit_score(alignment: dict[str, Any], output_stakeholder_alignment: float) -> float:
    """Score whether an example is grounded enough to stand in for the persona."""
    matched = float(alignment.get("persona_fit_score", 0.0) or 0.0)
    defining = float(alignment.get("defining_axis_match_score", 0.0) or 0.0)
    mismatches = float(alignment.get("mismatch_count", 0) or 0)
    critical = float(alignment.get("critical_mismatch_count", 0) or 0)
    return round(max(0.0, matched * 1.5 + defining * 1.5 + min(float(output_stakeholder_alignment), 2.0) * 0.25 - mismatches * 0.35 - critical * 1.0), 4)


def _persona_axis_alignment(row: pd.Series, dominant_axes: dict[str, str], axis_names: list[str]) -> dict[str, Any]:
    """Return detailed axis alignment between one row and the persona signature."""
    if not axis_names or not dominant_axes:
        return {
            "matched_count": 0,
            "mismatch_count": 0,
            "critical_mismatch_count": 0,
            "persona_fit_score": 0.0,
            "defining_axis_match_score": 0.0,
        }
    critical_axes = {"bottleneck_type", "workflow_stage", "analysis_goal", "tool_dependency_mode", "output_expectation"}
    matched = 0
    mismatches = 0
    considered = 0
    critical_matches = 0
    critical_mismatches = 0
    critical_considered = 0
    for axis in axis_names:
        dominant = str(dominant_axes.get(axis, "") or "").strip().lower()
        if not dominant or dominant == "unassigned":
            continue
        raw_value = str(row.get(axis, "") or "").strip().lower()
        considered += 1
        is_match = raw_value == dominant
        if is_match:
            matched += 1
        else:
            mismatches += 1
        if axis in critical_axes:
            critical_considered += 1
            if is_match:
                critical_matches += 1
            else:
                critical_mismatches += 1
    return {
        "matched_count": matched,
        "mismatch_count": mismatches,
        "critical_mismatch_count": critical_mismatches,
        "persona_fit_score": round(matched / max(considered, 1), 4),
        "defining_axis_match_score": round(critical_matches / max(critical_considered, 1), 4) if critical_considered else 0.0,
    }


def _output_stakeholder_alignment_score(dominant_axes: dict[str, str], score_breakdown_seed: dict[str, float]) -> float:
    """Boost examples that surface output/stakeholder pressure when the persona signature suggests it matters."""
    output_expected = str(dominant_axes.get("output_expectation", "") or "").strip().lower() not in {"", "unassigned"}
    trust_expected = str(dominant_axes.get("trust_validation_need", "") or "").strip().lower() not in {"", "unassigned"}
    score = 0.0
    if output_expected:
        score += float(score_breakdown_seed.get("output_need_score", 0.0))
    if trust_expected:
        score += float(score_breakdown_seed.get("stakeholder_pressure_score", 0.0))
        score += float(score_breakdown_seed.get("validation_pressure_score", 0.0))
    return min(score, 3.0)


def _matched_axes(cluster_fit_reason: str) -> str:
    """Extract matched axis text from the cluster fit reason."""
    if "Matches persona on " not in cluster_fit_reason:
        return ""
    return cluster_fit_reason.split("Matches persona on ", 1)[1].split("; misses", 1)[0]


def _top_signals(score_breakdown: dict[str, float], positive: bool) -> list[str]:
    """Return top positive or negative scoring dimensions."""
    keys = [key for key in score_breakdown if key.endswith("_penalty")] if not positive else [key for key in score_breakdown if not key.endswith("_penalty")]
    ranked = sorted(((key, float(score_breakdown[key])) for key in keys), key=lambda item: item[1], reverse=True)
    return [key for key, value in ranked if value > 0][:4]


def _grounding_strength(candidate: dict[str, Any], config: dict[str, Any]) -> str:
    """Classify whether a candidate can ground a persona strongly, weakly, or not at all."""
    quality = str(candidate.get("quote_quality", "") or "")
    mismatch_count = int(candidate.get("mismatch_count", 0) or 0)
    critical_mismatch_count = int(candidate.get("critical_mismatch_count", 0) or 0)
    for level in ("strong", "grounded", "weak"):
        rule = dict(config.get("policy", {}).get("grounding_strength", {}).get(level, {}) or {})
        allowed = {str(value) for value in list(rule.get("allowed_quote_qualities", []))}
        if quality not in allowed:
            continue
        if mismatch_count > int(rule.get("max_mismatch_axes", 99)):
            continue
        if critical_mismatch_count > int(rule.get("max_critical_mismatch_axes", 99)):
            continue
        if level == "weak" and not _allow_borderline_fallback(candidate, config):
            continue
        return level
    return "unacceptable"


def _grounding_reason(candidate: dict[str, Any]) -> str:
    """Explain how a candidate landed in its grounding bucket."""
    return (
        f"grounding_strength={candidate.get('grounding_strength', '')}; "
        f"quote_quality={candidate.get('quote_quality', '')}; "
        f"mismatch_count={int(candidate.get('mismatch_count', 0) or 0)}; "
        f"critical_mismatch_count={int(candidate.get('critical_mismatch_count', 0) or 0)}"
    )


def _policy_status(config: dict[str, Any], key: str, default: str) -> str:
    """Read a configured promotion-grounding status name."""
    return str(config.get("policy", {}).get("promotion_grounding", {}).get(key, default) or default)


def _subpattern_label(text: str, config: dict[str, Any]) -> str:
    """Assign a coarse subpattern for diversity selection."""
    for label, patterns in (config.get("subpatterns", {}) or {}).items():
        if any(str(pattern).lower() in text for pattern in patterns):
            return str(label)
    return "general_bottleneck"


def _dominant_axis_value(group: pd.DataFrame, axis: str) -> str:
    """Return dominant axis value inside a persona group."""
    if axis not in group.columns:
        return "unassigned"
    series = group[axis].fillna("unassigned").astype(str).str.lower()
    series = series[series != "unassigned"]
    if series.empty:
        return "unassigned"
    return str(series.value_counts().idxmax())


def _selection_quality_summary(df: pd.DataFrame, text_column: str) -> dict[str, float]:
    """Build coarse quality summary for before/after comparison."""
    if df is None or df.empty or text_column not in df.columns:
        return {"selected_count": 0.0, "avg_text_len": 0.0, "workflow_signal_density": 0.0, "bottleneck_signal_density": 0.0}
    text_series = df[text_column].fillna("").astype(str)
    workflow = text_series.str.lower().str.contains("report|excel|dashboard|stakeholder|weekly|monthly", regex=True).mean()
    bottleneck = text_series.str.lower().str.contains("manual|reconcile|match|break down|can't explain|ad hoc|export", regex=True).mean()
    return {
        "selected_count": float(len(df)),
        "avg_text_len": round(float(text_series.str.len().mean()), 2),
        "workflow_signal_density": round(float(workflow), 4),
        "bottleneck_signal_density": round(float(bottleneck), 4),
    }


def _strip_markup(text: str) -> str:
    """Remove simple HTML/markdown noise."""
    value = clean_text(text)
    value = HTML_TAG_RE.sub(" ", value)
    value = re.sub(r"`{1,3}.*?`{1,3}", " ", value)
    value = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", value)
    value = re.sub(r"[*_#>\-]{1,}", " ", value)
    return clean_text(value)


def _snippet_strength(text: str) -> float:
    """Quick heuristic for snippet preselection."""
    lowered = text.lower()
    positives = _pattern_score(lowered, ["report", "dashboard", "excel", "reconcile", "stakeholder", "manual", "break down", "why did", "ad hoc"])
    negatives = _pattern_score(lowered, ["docker", "kubernetes", "npm", "sdk", "oauth", "javascript", "python script", "introduce"])
    return positives - negatives + min(len(lowered) / 120.0, 2.0)


def _text_similarity(text_a: str, text_b: str) -> float:
    """Compute simple Jaccard similarity for duplicate suppression."""
    tokens_a = set(TOKEN_RE.findall(text_a.lower()))
    tokens_b = set(TOKEN_RE.findall(text_b.lower()))
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / max(len(tokens_a | tokens_b), 1)


def _build_examples_markdown(selected_df: pd.DataFrame) -> str:
    """Render selected examples in markdown grouped by persona."""
    if selected_df.empty:
        return "# Representative Examples\n\nNo examples selected.\n"
    sections = ["# Representative Examples", ""]
    grouped = selected_df.sort_values(["persona_id", "example_rank"]).groupby("persona_id", dropna=False)
    for persona_id, group in grouped:
        sections.append(f"## {persona_id}")
        for _, row in group.iterrows():
            sections.append(f"- [{row['quote_quality']}] {row['grounded_text']}")
        sections.append("")
    return "\n".join(sections).strip() + "\n"


def _allow_borderline_fallback(candidate: dict[str, Any], config: dict[str, Any]) -> bool:
    """Allow only stronger borderline examples when no usable quote exists."""
    if not bool(config.get("policy", {}).get("fallback", {}).get("allow_weak_grounding_fallback", True)):
        return False
    try:
        breakdown = json.loads(str(candidate.get("score_breakdown", "{}")))
    except json.JSONDecodeError:
        breakdown = {}
    workflow_business_strength = (
        float(breakdown.get("workflow_context_score", 0.0))
        + float(breakdown.get("business_context_score", 0.0))
        + float(breakdown.get("stakeholder_pressure_score", 0.0))
    )
    output_context = float(breakdown.get("output_need_score", 0.0)) + float(breakdown.get("excel_rework_score", 0.0))
    problem_strength = (
        float(breakdown.get("bottleneck_specificity_score", 0.0))
        + float(breakdown.get("dashboard_trust_score", 0.0))
        + float(breakdown.get("metric_definition_score", 0.0))
        + float(breakdown.get("root_cause_score", 0.0))
    )
    return float(candidate.get("final_example_score", 0.0)) >= float(config.get("thresholds", {}).get("promoted_fallback_min_score", 4.0)) and (
        problem_strength >= 2.0 or (workflow_business_strength >= 2.0 and output_context >= 1.0)
    )


def _pick_promoted_fallback(
    persona_candidates: pd.DataFrame,
    selected_df: pd.DataFrame,
    selected_ids: set[str],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    """Pick the best weak fallback for a promoted persona that lacks a selected example."""
    if persona_candidates.empty:
        return None
    thresholds = config.get("thresholds", {})
    min_score = float(thresholds.get("promoted_fallback_min_score", 2.5))
    max_mismatch_axes = int(thresholds.get("max_selected_mismatch_axes", 2)) + 1
    max_critical_mismatch_axes = int(thresholds.get("max_selected_critical_mismatch_axes", 1))
    used_sources = set(selected_df.get("source", pd.Series(dtype=str)).astype(str).tolist()) if not selected_df.empty else set()
    rows: list[dict[str, Any]] = []
    for _, row in persona_candidates.iterrows():
        candidate = row.to_dict()
        if str(candidate.get("episode_id", "")) in selected_ids:
            continue
        grounding_strength = str(candidate.get("grounding_strength", "") or "")
        if grounding_strength not in {"strong", "grounded", "weak"}:
            continue
        if grounding_strength == "weak" and not bool(config.get("policy", {}).get("fallback", {}).get("allow_weak_grounding_fallback", True)):
            continue
        if float(candidate.get("final_example_score", 0.0) or 0.0) < min_score:
            continue
        if int(candidate.get("critical_mismatch_count", 0) or 0) > max_critical_mismatch_axes:
            continue
        if int(candidate.get("mismatch_count", 0) or 0) > max_mismatch_axes:
            continue
        quality = str(candidate.get("quote_quality", "") or "")
        quality_bonus = {"borderline": 1.0, "usable": 2.0, "strong_representative": 3.0, "reject": 0.0}.get(quality, 0.0)
        grounding_bonus = {"weak": 1.0, "grounded": 2.5, "strong": 3.0}.get(grounding_strength, 0.0)
        source_bonus = 0.75 if str(candidate.get("source", "") or "") not in used_sources else 0.0
        candidate["_fallback_priority"] = (
            float(candidate.get("final_example_score", 0.0) or 0.0)
            + quality_bonus
            + grounding_bonus
            + source_bonus
            - float(candidate.get("mismatch_count", 0) or 0) * 0.5
            - float(candidate.get("critical_mismatch_count", 0) or 0) * 1.0
        )
        rows.append(candidate)
    if not rows:
        return None
    rows.sort(key=lambda item: (-float(item["_fallback_priority"]), -float(item.get("final_example_score", 0.0) or 0.0), str(item.get("episode_id", ""))))
    best = dict(rows[0])
    best.pop("_fallback_priority", None)
    return best


def _pick_bundle_materialized_example(
    persona_candidates: pd.DataFrame,
    selected_df: pd.DataFrame,
    selected_ids: set[str],
    bundle: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    """Pick one real support row to represent a persona that is grounded only via bundle evidence."""
    if persona_candidates.empty:
        return None
    bundle_cfg = dict(config.get("policy", {}).get("bundle_grounding", {}) or {})
    min_candidate_score = float(bundle_cfg.get("min_candidate_score", 3.0))
    max_mismatch_axes = int(bundle_cfg.get("max_mismatch_axes", 3))
    max_critical_mismatch_axes = int(bundle_cfg.get("max_critical_mismatch_axes", 1))
    used_sources = set(selected_df.get("source", pd.Series(dtype=str)).astype(str).tolist()) if not selected_df.empty else set()
    context_ids = set(_split_episode_ids(bundle.get("context_evidence_episode_ids", "")))
    workaround_ids = set(_split_episode_ids(bundle.get("workaround_evidence_episode_ids", "")))
    trust_ids = set(_split_episode_ids(bundle.get("trust_validation_evidence_episode_ids", "")))
    preferred_ids = context_ids | workaround_ids | trust_ids
    if not preferred_ids:
        return None

    rows: list[dict[str, Any]] = []
    for _, row in persona_candidates.iterrows():
        candidate = row.to_dict()
        episode_id = str(candidate.get("episode_id", "") or "")
        if not episode_id or episode_id in selected_ids or episode_id not in preferred_ids:
            continue
        if float(candidate.get("final_example_score", 0.0) or 0.0) < min_candidate_score:
            continue
        if int(candidate.get("mismatch_count", 0) or 0) > max_mismatch_axes:
            continue
        if int(candidate.get("critical_mismatch_count", 0) or 0) > max_critical_mismatch_axes:
            continue
        quality = str(candidate.get("quote_quality", "") or "")
        grounding_strength = str(candidate.get("grounding_strength", "") or "")
        if quality == "reject" and grounding_strength == "unacceptable":
            continue
        dimension_hits = int(episode_id in context_ids) + int(episode_id in workaround_ids) + int(episode_id in trust_ids)
        quality_bonus = {"strong_representative": 3.0, "usable": 2.0, "borderline": 1.0, "reject": 0.0}.get(quality, 0.0)
        source_bonus = 0.5 if str(candidate.get("source", "") or "") not in used_sources else 0.0
        candidate["_bundle_priority"] = (
            dimension_hits * 10.0
            + float(candidate.get("final_example_score", 0.0) or 0.0)
            + float(candidate.get("grounding_fit_score", 0.0) or 0.0)
            + quality_bonus
            + source_bonus
            - float(candidate.get("mismatch_count", 0) or 0) * 0.5
            - float(candidate.get("critical_mismatch_count", 0) or 0) * 1.5
        )
        rows.append(candidate)
    if not rows:
        return None
    rows.sort(
        key=lambda item: (
            -float(item.get("_bundle_priority", 0.0) or 0.0),
            -float(item.get("final_example_score", 0.0) or 0.0),
            str(item.get("episode_id", "")),
        )
    )
    best = dict(rows[0])
    best.pop("_bundle_priority", None)
    return best


def _bundle_grounding_evidence(persona_candidates: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
    """Compute multi-episode grounding evidence for one promoted persona."""
    bundle_cfg = dict(config.get("policy", {}).get("bundle_grounding", {}) or {})
    if not bool(bundle_cfg.get("enabled", True)) or persona_candidates.empty:
        return {
            "candidate_count_before_filter": int(len(persona_candidates)),
            "candidate_count_after_filter": 0,
            "rejected_by_threshold_count": int(len(persona_candidates)),
            "rejected_by_mismatch_count": 0,
            "context_evidence_count": 0,
            "workaround_evidence_count": 0,
            "trust_validation_evidence_count": 0,
            "bundle_episode_count": 0,
            "bundle_dimension_hits": 0,
            "total_bundle_strength": 0,
            "bundle_grounding_status": "ungrounded",
            "bundle_grounding_reason": "bundle grounding disabled or no candidate episodes available",
            "context_evidence_episode_ids": "",
            "workaround_evidence_episode_ids": "",
            "trust_validation_evidence_episode_ids": "",
            "bundle_support_examples": "",
        }

    min_candidate_score = float(bundle_cfg.get("min_candidate_score", 3.0))
    max_mismatch_axes = int(bundle_cfg.get("max_mismatch_axes", 3))
    max_critical_mismatch_axes = int(bundle_cfg.get("max_critical_mismatch_axes", 1))
    candidate_count_before_filter = int(len(persona_candidates))
    rejected_by_threshold_count = 0
    rejected_by_mismatch_count = 0
    pool_rows: list[dict[str, Any]] = []
    for _, row in persona_candidates.iterrows():
        candidate = row.to_dict()
        if float(candidate.get("final_example_score", 0.0) or 0.0) < min_candidate_score:
            rejected_by_threshold_count += 1
            continue
        if int(candidate.get("mismatch_count", 0) or 0) > max_mismatch_axes:
            rejected_by_mismatch_count += 1
            continue
        if int(candidate.get("critical_mismatch_count", 0) or 0) > max_critical_mismatch_axes:
            rejected_by_mismatch_count += 1
            continue
        if str(candidate.get("quote_quality", "") or "") == "reject":
            rejected_by_threshold_count += 1
            continue
        if str(candidate.get("grounding_strength", "") or "") == "unacceptable" and str(candidate.get("quote_quality", "") or "") not in {"borderline", "usable", "strong_representative"}:
            rejected_by_threshold_count += 1
            continue
        pool_rows.append(candidate)

    if not pool_rows:
        return {
            "candidate_count_before_filter": candidate_count_before_filter,
            "candidate_count_after_filter": 0,
            "rejected_by_threshold_count": rejected_by_threshold_count,
            "rejected_by_mismatch_count": rejected_by_mismatch_count,
            "context_evidence_count": 0,
            "workaround_evidence_count": 0,
            "trust_validation_evidence_count": 0,
            "bundle_episode_count": 0,
            "bundle_dimension_hits": 0,
            "total_bundle_strength": 0,
            "bundle_grounding_status": "ungrounded",
            "bundle_grounding_reason": "no multi-episode evidence rows cleared bundle grounding thresholds",
            "context_evidence_episode_ids": "",
            "workaround_evidence_episode_ids": "",
            "trust_validation_evidence_episode_ids": "",
            "bundle_support_examples": "",
        }

    context_rows: list[dict[str, Any]] = []
    workaround_rows: list[dict[str, Any]] = []
    trust_rows: list[dict[str, Any]] = []
    for candidate in pool_rows:
        breakdown = _score_breakdown_dict(candidate)
        if _candidate_has_context_evidence(candidate, breakdown):
            context_rows.append(candidate)
        if _candidate_has_workaround_evidence(candidate, breakdown):
            workaround_rows.append(candidate)
        if _candidate_has_trust_validation_evidence(candidate, breakdown):
            trust_rows.append(candidate)

    context_count = len({str(item.get("episode_id", "")) for item in context_rows if str(item.get("episode_id", ""))})
    workaround_count = len({str(item.get("episode_id", "")) for item in workaround_rows if str(item.get("episode_id", ""))})
    trust_count = len({str(item.get("episode_id", "")) for item in trust_rows if str(item.get("episode_id", ""))})
    bundle_episode_ids = sorted(
        {
            str(item.get("episode_id", ""))
            for item in [*context_rows, *workaround_rows, *trust_rows]
            if str(item.get("episode_id", ""))
        }
    )
    dimension_hits = sum(1 for count in [context_count, workaround_count, trust_count] if count > 0)
    total_bundle_strength = context_count + workaround_count + trust_count
    grounded_bundle = (
        context_count >= int(bundle_cfg.get("min_context_evidence_count", 2))
        and workaround_count >= int(bundle_cfg.get("min_workaround_evidence_count", 2))
        and trust_count >= int(bundle_cfg.get("min_trust_validation_evidence_count", 1))
        and len(bundle_episode_ids) >= int(bundle_cfg.get("min_bundle_episode_count", 3))
    )
    weak_bundle = (
        not grounded_bundle
        and dimension_hits >= int(bundle_cfg.get("weak_bundle_min_dimension_hits", 2))
        and total_bundle_strength >= int(bundle_cfg.get("weak_bundle_min_total_strength", 3))
    )
    if grounded_bundle:
        bundle_status = "grounded_bundle"
        bundle_reason = (
            f"bundle grounding satisfied by {context_count} context episodes, {workaround_count} workaround episodes, "
            f"and {trust_count} trust/output episodes across {len(bundle_episode_ids)} distinct episodes"
        )
    elif weak_bundle:
        bundle_status = "weak_bundle"
        bundle_reason = (
            f"bundle evidence is directionally present but incomplete: context={context_count}, workaround={workaround_count}, "
            f"trust_or_output={trust_count}, episodes={len(bundle_episode_ids)}"
        )
    else:
        bundle_status = "ungrounded"
        bundle_reason = (
            f"bundle evidence is insufficient: context={context_count}, workaround={workaround_count}, "
            f"trust_or_output={trust_count}, episodes={len(bundle_episode_ids)}"
        )
    max_supporting_examples = int(bundle_cfg.get("max_supporting_examples", 3))
    support_examples = _bundle_support_examples(pool_rows, max_items=max_supporting_examples)
    return {
        "candidate_count_before_filter": candidate_count_before_filter,
        "candidate_count_after_filter": int(len(pool_rows)),
        "rejected_by_threshold_count": rejected_by_threshold_count,
        "rejected_by_mismatch_count": rejected_by_mismatch_count,
        "context_evidence_count": context_count,
        "workaround_evidence_count": workaround_count,
        "trust_validation_evidence_count": trust_count,
        "bundle_episode_count": len(bundle_episode_ids),
        "bundle_dimension_hits": dimension_hits,
        "total_bundle_strength": total_bundle_strength,
        "bundle_grounding_status": bundle_status,
        "bundle_grounding_reason": bundle_reason,
        "context_evidence_episode_ids": " | ".join(_episode_ids(context_rows, max_supporting_examples)),
        "workaround_evidence_episode_ids": " | ".join(_episode_ids(workaround_rows, max_supporting_examples)),
        "trust_validation_evidence_episode_ids": " | ".join(_episode_ids(trust_rows, max_supporting_examples)),
        "bundle_support_examples": " | ".join(support_examples),
    }


def _score_breakdown_dict(candidate: dict[str, Any]) -> dict[str, Any]:
    """Parse one candidate's score_breakdown JSON safely."""
    try:
        parsed = json.loads(str(candidate.get("score_breakdown", "{}") or "{}"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _candidate_has_context_evidence(candidate: dict[str, Any], breakdown: dict[str, Any]) -> bool:
    """Return whether a row demonstrates the recurring job context."""
    return (
        float(breakdown.get("workflow_context_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("business_context_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("explicit_workflow_pain_score", 0.0) or 0.0) > 0.0
        or int(candidate.get("matched_axis_count", 0) or 0) >= 2
    )


def _candidate_has_workaround_evidence(candidate: dict[str, Any], breakdown: dict[str, Any]) -> bool:
    """Return whether a row shows the bottleneck/workaround repeating in practice."""
    return (
        float(breakdown.get("repeated_manual_workaround_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("bottleneck_specificity_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("excel_rework_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("root_cause_score", 0.0) or 0.0) > 0.0
        or _bundle_workaround_text_evidence(candidate, breakdown)
    )


def _candidate_has_trust_validation_evidence(candidate: dict[str, Any], breakdown: dict[str, Any]) -> bool:
    """Return whether a row exposes trust, validation, or output-delivery pressure."""
    return (
        float(breakdown.get("validation_pressure_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("stakeholder_pressure_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("output_need_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("dashboard_trust_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("metric_definition_score", 0.0) or 0.0) > 0.0
    )


def _episode_ids(rows: list[dict[str, Any]], max_items: int) -> list[str]:
    """Return unique episode ids in stable order."""
    seen: set[str] = set()
    results: list[str] = []
    for row in sorted(rows, key=lambda item: (-float(item.get("final_example_score", 0.0) or 0.0), str(item.get("episode_id", "")))):
        episode_id = str(row.get("episode_id", "") or "")
        if not episode_id or episode_id in seen:
            continue
        seen.add(episode_id)
        results.append(episode_id)
        if len(results) >= max_items:
            break
    return results


def _split_episode_ids(value: Any) -> list[str]:
    """Split pipe-delimited bundle episode ids into a stable list."""
    parts = [part.strip() for part in str(value or "").split("|")]
    return [part for part in parts if part]


def _bundle_support_examples(rows: list[dict[str, Any]], max_items: int) -> list[str]:
    """Return short support snippets for bundle-grounding diagnostics."""
    examples: list[str] = []
    seen: set[str] = set()
    ordered = sorted(rows, key=lambda item: (-float(item.get("final_example_score", 0.0) or 0.0), str(item.get("episode_id", ""))))
    for row in ordered:
        text = clean_text(str(row.get("grounded_text", "") or ""))
        if not text or text in seen:
            continue
        seen.add(text)
        examples.append(text[:160])
        if len(examples) >= max_items:
            break
    return examples


def _pick_promoted_salvage_fallback(
    persona_candidates: pd.DataFrame,
    selected_df: pd.DataFrame,
    selected_ids: set[str],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    """Pick a strict-but-salvageable fallback when all candidates were marked unacceptable."""
    if persona_candidates.empty:
        return None
    fallback_cfg = dict(config.get("policy", {}).get("fallback", {}) or {})
    promoted_statuses = {
        str(config.get("policy", {}).get("promotion_grounding", {}).get("base_promoted_status", "promoted_persona")),
        "promoted_candidate_persona",
    }
    if str(fallback_cfg.get("promoted_personas_only", True)).lower() == "true":
        persona_statuses = set(persona_candidates.get("base_promotion_status", pd.Series(dtype=str)).astype(str).tolist())
        if persona_statuses and persona_statuses.isdisjoint(promoted_statuses):
            return None
    min_score = float(fallback_cfg.get("salvage_min_score", 5.0))
    max_mismatch_axes = int(fallback_cfg.get("salvage_max_mismatch_axes", 3))
    max_critical_mismatch_axes = int(fallback_cfg.get("salvage_max_critical_mismatch_axes", 1))
    allowed_reasons = {
        "no clear user pain or workflow pressure",
        "does not clearly expose the repeated work bottleneck",
        "context is too weak or ambiguous for a strong representative example",
    }
    used_sources = set(selected_df.get("source", pd.Series(dtype=str)).astype(str).tolist()) if not selected_df.empty else set()
    rows: list[dict[str, Any]] = []
    for _, row in persona_candidates.iterrows():
        candidate = row.to_dict()
        if str(candidate.get("episode_id", "")) in selected_ids:
            continue
        if str(candidate.get("grounding_strength", "") or "") != "unacceptable":
            continue
        if float(candidate.get("final_example_score", 0.0) or 0.0) < min_score:
            continue
        if int(candidate.get("critical_mismatch_count", 0) or 0) > max_critical_mismatch_axes:
            continue
        if int(candidate.get("mismatch_count", 0) or 0) > max_mismatch_axes:
            continue
        rejection_reason = str(candidate.get("rejection_reason", "") or "").strip().lower()
        if rejection_reason not in allowed_reasons:
            continue
        if str(candidate.get("quote_quality", "") or "") == "reject":
            continue
        source_bonus = 0.5 if str(candidate.get("source", "") or "") not in used_sources else 0.0
        candidate["_salvage_priority"] = (
            float(candidate.get("final_example_score", 0.0) or 0.0)
            + source_bonus
            - float(candidate.get("mismatch_count", 0) or 0) * 0.35
            - float(candidate.get("critical_mismatch_count", 0) or 0) * 0.75
        )
        rows.append(candidate)
    if not rows:
        return None
    rows.sort(
        key=lambda item: (
            -float(item["_salvage_priority"]),
            -float(item.get("final_example_score", 0.0) or 0.0),
            str(item.get("episode_id", "")),
        )
    )
    best = dict(rows[0])
    best.pop("_salvage_priority", None)
    return best


def _fallback_grounding_warning(candidate: dict[str, Any]) -> str:
    """Explain why a fallback-selected example is weaker than a normal representative example."""
    reasons: list[str] = []
    if str(candidate.get("grounding_strength", "") or "") == "weak":
        reasons.append("grounding_strength=weak")
    if str(candidate.get("quote_quality", "") or "") not in {"usable", "strong_representative"}:
        reasons.append(f"quote_quality={candidate.get('quote_quality', '')}")
    if int(candidate.get("critical_mismatch_count", 0) or 0) > 0:
        reasons.append(f"critical_mismatch_count={int(candidate.get('critical_mismatch_count', 0) or 0)}")
    if int(candidate.get("mismatch_count", 0) or 0) > 0:
        reasons.append(f"mismatch_count={int(candidate.get('mismatch_count', 0) or 0)}")
    rejection_reason = str(candidate.get("rejection_reason", "") or "").strip()
    if rejection_reason:
        reasons.append(rejection_reason)
    return " | ".join(reasons)


def _grounding_diversity_rejection_count(persona_candidates: pd.DataFrame) -> int:
    """Count candidates held out by diversity or duplicate suppression after clearing base quality."""
    if persona_candidates.empty or "rejection_reason" not in persona_candidates.columns:
        return 0
    reasons = persona_candidates["rejection_reason"].fillna("").astype(str).str.strip().str.lower()
    return int(
        reasons.isin(
            {
                "near-duplicate of a stronger selected example",
                "held out for source diversity",
                "held out for subpattern diversity",
            }
        ).sum()
    )


def _bundle_workaround_text_evidence(candidate: dict[str, Any], breakdown: dict[str, Any]) -> bool:
    """Detect workaround evidence for bundle grounding when explicit workaround scoring misses tool-limitation language."""
    text = str(candidate.get("grounded_text", "") or "").lower()
    workaround_markers = [
        "manual",
        "manually",
        "workaround",
        "duplicate",
        "replace",
        "rebuild",
        "alter",
        "update all",
        "tedious",
        "save the chart as a duplicate",
    ]
    tool_block_markers = [
        "dashboard",
        "table",
        "filter",
        "query",
        "field",
        "column",
        "chart",
    ]
    if not any(marker in text for marker in workaround_markers):
        return False
    if any(marker in text for marker in tool_block_markers):
        return True
    return (
        float(breakdown.get("dashboard_trust_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("explicit_workflow_pain_score", 0.0) or 0.0) > 0.0
        or float(breakdown.get("output_need_score", 0.0) or 0.0) > 0.0
    )
