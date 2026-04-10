"""Interpretable representative-example scoring and selection."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

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
        selected_ids = {str(item["episode_id"]) for item in selected}
        for rank, item in enumerate(selected, start=1):
            item["persona_id"] = str(persona_id)
            item["example_rank"] = rank
            item["selection_decision"] = "selected"
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
        "persona_fit_score": _persona_fit_score(row, dominant_axes, axis_names),
        "genericness_penalty": _pattern_score(text, negative_patterns.get("genericness", [])),
        "technical_noise_penalty": _pattern_score(text, negative_patterns.get("technical_noise", [])),
        "generic_product_statement_penalty": _pattern_score(text, negative_patterns.get("generic_product_statement", [])),
        "no_user_pain_context_penalty": _no_user_pain_context_penalty(text),
        "short_no_workflow_evidence_penalty": _short_no_workflow_penalty(text),
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
        - score_breakdown["genericness_penalty"] * float(weights.get("genericness_penalty", 1.0))
        - score_breakdown["technical_noise_penalty"] * float(weights.get("technical_noise_penalty", 1.0))
        - score_breakdown["generic_product_statement_penalty"] * float(weights.get("generic_product_statement_penalty", 2.2))
        - score_breakdown["no_user_pain_context_penalty"] * float(weights.get("no_user_pain_context_penalty", 2.0))
        - score_breakdown["short_no_workflow_evidence_penalty"] * float(weights.get("short_no_workflow_evidence_penalty", 2.0))
    )
    quote_quality = _quote_quality(score_breakdown, final_example_score, config)
    cluster_fit_reason = _cluster_fit_reason(row, dominant_axes, axis_names)
    top_positive_signals = _top_signals(score_breakdown, positive=True)
    top_negative_signals = _top_signals(score_breakdown, positive=False)
    rejection_reason = _rejection_reason(score_breakdown, quote_quality)
    return {
        "episode_id": get_episode_id(row),
        "source": get_record_source(row),
        "grounded_text": snippet,
        "quote_quality": quote_quality,
        "top_positive_signals": json.dumps(top_positive_signals, ensure_ascii=False),
        "top_negative_signals": json.dumps(top_negative_signals, ensure_ascii=False),
        "score_breakdown": json.dumps(score_breakdown, ensure_ascii=False),
        "cluster_fit_reason": cluster_fit_reason,
        "rejection_reason": rejection_reason,
        "subpattern_label": _subpattern_label(text, config),
        "final_example_score": round(float(final_example_score), 4),
        "label_confidence": float(get_record_value(row, "label_confidence", 0.0) or 0.0),
        "source_text_length": len(snippet),
        "reason_selected": " | ".join(top_positive_signals[:3]) if top_positive_signals else "borderline or weak evidence",
        "why_selected": _why_selected(top_positive_signals, cluster_fit_reason),
        "matched_axes": _matched_axes(cluster_fit_reason),
    }


def _select_diverse_examples(candidates: list[dict[str, Any]], max_items: int, config: dict[str, Any]) -> list[dict[str, Any]]:
    """Select top examples with diversity and near-duplicate suppression."""
    selected: list[dict[str, Any]] = []
    selected_texts: list[str] = []
    used_subpatterns: set[str] = set()
    duplicate_threshold = float(config.get("thresholds", {}).get("duplicate_similarity_threshold", 0.72))
    for candidate in candidates:
        if candidate["quote_quality"] not in {"strong_representative", "usable"}:
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
        if subpattern in used_subpatterns and len(selected) < max_items // 2:
            continue
        selected.append(candidate)
        selected_texts.append(text)
        used_subpatterns.add(subpattern)
        if len(selected) >= max_items:
            break
    if not selected:
        for candidate in candidates:
            if candidate["quote_quality"] == "borderline" and _allow_borderline_fallback(candidate):
                selected.append(candidate)
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


def _persona_fit_score(row: pd.Series, dominant_axes: dict[str, str], axis_names: list[str]) -> float:
    """Score how well the row matches the persona's dominant axis values."""
    if not axis_names or not dominant_axes:
        return 0.0
    matched = 0
    considered = 0
    for axis in axis_names:
        raw_value = str(row.get(axis, "") or "").strip().lower()
        dominant = str(dominant_axes.get(axis, "") or "").strip().lower()
        if not dominant or dominant == "unassigned":
            continue
        considered += 1
        if raw_value == dominant:
            matched += 1
    return round(matched / max(considered, 1), 4)


def _cluster_fit_reason(row: pd.Series, dominant_axes: dict[str, str], axis_names: list[str]) -> str:
    """Explain why the example fits or misses the persona signature."""
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
        return f"Matches persona on {', '.join(matches[:3])}" + (f"; misses {', '.join(misses[:2])}" if misses else "")
    return "Low direct axis match; kept only if bottleneck detail is unusually strong."


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


def _no_user_pain_context_penalty(text: str) -> float:
    """Penalize snippets without a user pain or pressure cue."""
    pain_terms = ["can't", "cannot", "need", "problem", "issue", "wrong", "mismatch", "manual", "reconcile", "blocked", "not enough", "how do i", "why"]
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


def _allow_borderline_fallback(candidate: dict[str, Any]) -> bool:
    """Allow only stronger borderline examples when no usable quote exists."""
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
    return float(candidate.get("final_example_score", 0.0)) >= 4.0 and (
        problem_strength >= 2.0 or (workflow_business_strength >= 2.0 and output_context >= 1.0)
    )
