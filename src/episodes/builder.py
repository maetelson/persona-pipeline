"""Episode building logic from valid normalized posts."""

from __future__ import annotations

from collections import Counter
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.episodes.schema import EPISODE_COLUMNS, EpisodeRecord
from src.utils.record_access import get_record_source_meta
from src.utils.text import clean_text, combine_text


QUESTION_TYPE_PATTERNS = {
    "reporting": ["report", "reporting", "export", "deliverable", "stakeholder update"],
    "reconciliation": ["reconcile", "mismatch", "validation", "qa", "compare", "audit"],
    "investigation": ["investigate", "debug", "root cause", "why is", "broken", "issue"],
    "automation": ["automate", "automation", "script", "pipeline", "recurring"],
}

BOTTLENECK_PATTERNS = {
    "manual_reporting": ["manual", "copy paste", "spreadsheet", "excel export", "weekly report"],
    "data_quality": ["mismatch", "validation", "wrong number", "reconcile", "qa"],
    "tool_limitation": ["cannot", "can't", "limitation", "missing feature", "sorting", "broken"],
    "handoff_friction": ["stakeholder", "request", "handoff", "wait for", "review"],
}

TOOL_PATTERNS = {
    "excel": ["excel", "spreadsheet", "csv"],
    "sql_bi": ["sql", "dashboard", "qlik", "tableau", "power bi", "superset", "metabase", "looker"],
    "python": ["python", "pandas", "notebook", "script"],
    "warehouse": ["snowflake", "bigquery", "redshift", "database"],
}

COLLAB_PATTERNS = {
    "stakeholder": ["stakeholder", "leadership", "exec", "manager"],
    "analyst_peer": ["analyst", "teammate", "peer", "co-worker"],
    "engineer": ["engineer", "developer", "data engineer", "backend"],
    "client": ["client", "customer", "vendor"],
}

OUTPUT_PATTERNS = {
    "xlsx_report": ["excel", "spreadsheet", "xlsx", "report"],
    "dashboard_update": ["dashboard", "chart", "visualization"],
    "validated_dataset": ["validated data", "reconciled data", "clean dataset"],
    "automation_job": ["automation", "pipeline", "scheduled"],
}

ROLE_PATTERNS = {
    "analyst": ["analyst", "dashboard", "report", "sql"],
    "manager": ["manager", "stakeholder", "leadership"],
    "marketer": ["ads", "campaign", "media buyer", "marketing"],
}

WORK_MOMENT_PATTERNS = {
    "reporting": ["report", "export", "stakeholder update"],
    "validation": ["qa", "validation", "reconcile", "compare"],
    "triage": ["debug", "issue", "bug", "broken", "investigate"],
    "automation_design": ["automate", "workflow", "pipeline", "script"],
}


@dataclass(slots=True)
class SegmentState:
    """Intermediate segment with derived episode fields."""

    text: str
    question_type: str
    bottleneck_text: str
    tool_env: str
    collaborator: str
    desired_output: str
    role_clue: str
    work_moment: str

    @property
    def signature(self) -> tuple[str, str, str, str, str]:
        """Episode boundary signature for splitting decisions."""
        return (
            self.question_type,
            self.bottleneck_text,
            self.tool_env,
            self.collaborator,
            self.desired_output,
        )


@dataclass(slots=True)
class EpisodeBuildDebug:
    """Per-row diagnostics for episode promotion and drop analysis."""

    source: str
    raw_id: str
    url: str
    source_schema_type: str
    source_type: str
    episode_count: int
    drop_reason: str
    drop_detail: str
    title_len: int
    body_len: int
    comments_len: int
    parent_context_len: int
    thread_title_len: int
    combined_text_len: int
    candidate_unit_count_raw: int
    candidate_unit_count_cleaned: int
    duplicate_collapse_count: int
    title_body_combined_used: bool
    missing_required_fields: bool
    reply_like_schema: bool
    passes_combined_quality: bool
    quality_score: float
    quality_bucket: str
    quality_fail_reason: str
    rescue_reason: str
    top_level_meta_keys: str
    nested_meta_keys: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize one diagnostic row."""
        return {
            "source": self.source,
            "raw_id": self.raw_id,
            "url": self.url,
            "source_schema_type": self.source_schema_type,
            "source_type": self.source_type,
            "episode_count": self.episode_count,
            "drop_reason": self.drop_reason,
            "drop_detail": self.drop_detail,
            "title_len": self.title_len,
            "body_len": self.body_len,
            "comments_len": self.comments_len,
            "parent_context_len": self.parent_context_len,
            "thread_title_len": self.thread_title_len,
            "combined_text_len": self.combined_text_len,
            "candidate_unit_count_raw": self.candidate_unit_count_raw,
            "candidate_unit_count_cleaned": self.candidate_unit_count_cleaned,
            "duplicate_collapse_count": self.duplicate_collapse_count,
            "title_body_combined_used": self.title_body_combined_used,
            "missing_required_fields": self.missing_required_fields,
            "reply_like_schema": self.reply_like_schema,
            "passes_combined_quality": self.passes_combined_quality,
            "quality_score": self.quality_score,
            "quality_bucket": self.quality_bucket,
            "quality_fail_reason": self.quality_fail_reason,
            "rescue_reason": self.rescue_reason,
            "top_level_meta_keys": self.top_level_meta_keys,
            "nested_meta_keys": self.nested_meta_keys,
        }


@dataclass(slots=True)
class QualityAssessment:
    """Deterministic episode quality scoring outcome."""

    score: float
    bucket: str
    fail_reason: str
    rescue_reason: str
    passes: bool


def build_episode_table(valid_df: pd.DataFrame, rules: dict[str, Any]) -> pd.DataFrame:
    """Convert valid candidates into episode-level rows."""
    episodes_df, _, _ = build_episode_outputs(valid_df, rules)
    return episodes_df


def build_episode_outputs(valid_df: pd.DataFrame, rules: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Convert valid candidates into episodes plus debug diagnostics."""
    if valid_df.empty:
        return (
            pd.DataFrame(columns=EPISODE_COLUMNS),
            pd.DataFrame(),
            pd.DataFrame(),
        )

    episode_rows: list[dict[str, str]] = []
    debug_rows: list[dict[str, Any]] = []
    for _, row in valid_df.iterrows():
        episodes, debug = build_post_episodes(row, rules)
        episode_rows.extend(episode.to_dict() for episode in episodes)
        debug_rows.append(debug.to_dict())
    debug_df = pd.DataFrame(debug_rows)
    schema_df = build_parser_schema_diff(valid_df)
    return (
        pd.DataFrame(episode_rows, columns=EPISODE_COLUMNS),
        debug_df,
        schema_df,
    )


def build_post_episodes(row: pd.Series, rules: dict[str, Any]) -> tuple[list[EpisodeRecord], EpisodeBuildDebug]:
    """Build one or more conservative episodes from a single valid post."""
    source = str(row.get("source", "") or "")
    diagnostics = _build_episode_row_diagnostics(row, rules, source=source)
    candidate_units = _build_units_from_row(row, rules, diagnostics)
    min_episode_len = int(rules.get("min_episode_len", 120))
    if not candidate_units:
        candidate_units = [diagnostics["combined_text"]]

    segments = [_derive_segment_state(unit) for unit in candidate_units if len(clean_text(unit)) >= min_episode_len // 2]
    segments = [segment for segment in segments if not _is_non_boundary_segment(segment.text, rules)]
    if not segments:
        fallback = _derive_segment_state(diagnostics["combined_text"])
        segments = [fallback] if len(fallback.text) >= min_episode_len else []
    if not segments:
        return [], _build_debug_record(row, diagnostics, episode_count=0, drop_reason=_derive_drop_reason(diagnostics))

    grouped = _group_segments(segments, rules)
    if not grouped:
        grouped = [_derive_segment_state(diagnostics["combined_text"])]

    episodes: list[EpisodeRecord] = []
    quality_assessments: list[QualityAssessment] = []
    for index, segment in enumerate(grouped, start=1):
        normalized_episode = clean_text(segment.text)
        quality = _assess_episode_quality(normalized_episode, rules, source=source)
        quality_assessments.append(quality)
        if not quality.passes:
            continue
        episodes.append(
            EpisodeRecord(
                episode_id=f"{row['source']}::{row['raw_id']}::{index:02d}",
                source=str(row["source"]),
                raw_id=str(row["raw_id"]),
                url=str(row.get("url", "")),
                normalized_episode=normalized_episode,
                evidence_snippet=normalized_episode[:220],
                role_clue=segment.role_clue,
                work_moment=segment.work_moment,
                business_question=_business_question(segment),
                tool_env=segment.tool_env,
                bottleneck_text=segment.bottleneck_text,
                workaround_text=_extract_workaround(normalized_episode),
                desired_output=segment.desired_output,
                product_fit=_score_product_fit(segment, normalized_episode),
                quality_score=quality.score,
                quality_bucket=quality.bucket,
                quality_fail_reason=quality.fail_reason,
                rescue_reason=quality.rescue_reason,
                segmentation_note=_segmentation_note(segment, rules),
            )
        )
    chosen_quality = quality_assessments[0] if quality_assessments else _assess_episode_quality(diagnostics["combined_text"], rules, source=source)
    drop_reason = "" if episodes else _derive_drop_reason(diagnostics, grouped_segments=grouped, quality=chosen_quality)
    return episodes, _build_debug_record(
        row,
        diagnostics,
        episode_count=len(episodes),
        drop_reason=drop_reason,
        quality=chosen_quality,
    )


def _build_units_from_row(row: pd.Series, rules: dict[str, Any], diagnostics: dict[str, Any] | None = None) -> list[str]:
    """Build conservative segmentation units from title, body, and comment blocks."""
    units: list[str] = []
    title = clean_text(str(row.get("title", "") or ""))
    body = _normalize_bullets(str(row.get("body", "") or ""))
    comments_text = str(row.get("comments_text", "") or "")
    parent_context = clean_text(str(row.get("parent_context", "") or ""))
    if diagnostics is None:
        diagnostics = _build_episode_row_diagnostics(row, rules)

    combined_primary = diagnostics["combined_primary"]
    if combined_primary:
        units.append(combined_primary)
    elif title:
        units.append(title)
    if parent_context and parent_context not in units:
        units.append(parent_context)
    units.extend(_split_body_into_units(body, rules))
    units.extend(_comment_blocks(comments_text, rules))
    diagnostics["candidate_unit_count_raw"] = len([unit for unit in units if clean_text(unit)])

    cleaned_units: list[str] = []
    duplicate_collapse_count = 0
    for unit in units:
        unit = clean_text(unit)
        if not unit:
            continue
        if cleaned_units and _text_similarity(cleaned_units[-1], unit) >= float(rules.get("similarity_merge_threshold", 0.62)):
            cleaned_units[-1] = combine_text(cleaned_units[-1], unit)
            duplicate_collapse_count += 1
        else:
            cleaned_units.append(unit)
    diagnostics["candidate_unit_count_cleaned"] = len(cleaned_units)
    diagnostics["duplicate_collapse_count"] = duplicate_collapse_count
    return cleaned_units


def _split_body_into_units(text: str, rules: dict[str, Any]) -> list[str]:
    """Split body into a few coarse chunks instead of sentence-heavy fragments."""
    max_units = int(rules.get("max_units_from_body", 6))
    min_unit_len = int(rules.get("min_unit_len", 80))
    paragraphs = [clean_text(part) for part in re.split(r"\n\s*\n+", text) if clean_text(part)]
    if not paragraphs:
        return []

    units: list[str] = []
    buffer: list[str] = []
    current_len = 0
    for paragraph in paragraphs:
        if _is_boilerplate(paragraph, rules):
            continue
        if buffer and (current_len + len(paragraph) > 900 or len(buffer) >= 3):
            merged = clean_text(" ".join(buffer))
            if len(merged) >= min_unit_len:
                units.append(merged)
            buffer = [paragraph]
            current_len = len(paragraph)
        else:
            buffer.append(paragraph)
            current_len += len(paragraph)
    if buffer:
        merged = clean_text(" ".join(buffer))
        if len(merged) >= min_unit_len:
            units.append(merged)
    return units[:max_units]


def _normalize_bullets(text: str) -> str:
    """Turn bullet-like markers into paragraph boundaries before splitting."""
    normalized = text.replace("\\*", "\n").replace("* ", "\n").replace("- ", "\n").replace("â€¢", "\n")
    normalized = re.sub(r"(?m)^\d+[.)]\s+", "\n", normalized)
    return normalized


def _comment_blocks(comments_text: str, rules: dict[str, Any]) -> list[str]:
    """Group comments into a few large blocks to avoid per-comment splitting."""
    comments = [clean_text(part) for part in re.split(r"\n\s*\n+", comments_text) if clean_text(part)]
    max_blocks = int(rules.get("max_comment_blocks", 2))
    block_max_chars = int(rules.get("comment_block_max_chars", 1000))

    blocks: list[str] = []
    buffer: list[str] = []
    current_len = 0
    for comment in comments:
        if _is_boilerplate(comment, rules):
            continue
        if buffer and (current_len + len(comment) > block_max_chars or len(buffer) >= 5):
            blocks.append(" ".join(buffer))
            buffer = [comment]
            current_len = len(comment)
        else:
            buffer.append(comment)
            current_len += len(comment)
    if buffer:
        blocks.append(" ".join(buffer))
    return [clean_text(block) for block in blocks[:max_blocks] if clean_text(block)]


def _group_segments(segments: list[SegmentState], rules: dict[str, Any]) -> list[SegmentState]:
    """Merge candidate units into conservative final episodes.

    Over-segmentation guard:
    - do not split on promotional blurbs, duplicate notices, or support boilerplate
    - require meaningful signature change, not just a weak keyword flip
    - merge highly similar adjacent units
    - keep short or repetitive follow-up text attached

    Under-segmentation guard:
    - still split when question type, bottleneck, tool, collaborator, and output
      meaningfully shift and the current block has enough substance
    """
    if not segments:
        return []

    min_episode_len = int(rules.get("min_episode_len", 120))
    max_episode_len = int(rules.get("max_episode_len", 1800))
    min_boundary_segment_len = int(rules.get("min_boundary_segment_len", 180))
    similarity_threshold = float(rules.get("similarity_merge_threshold", 0.62))
    min_signature_change_count = int(rules.get("min_signature_change_count", 2))

    grouped: list[SegmentState] = [segments[0]]
    for segment in segments[1:]:
        current = grouped[-1]
        signature_change_count = _signature_change_count(current, segment, rules)
        similarity = _text_similarity(current.text, segment.text)
        weak_new_signal = _is_non_boundary_segment(segment.text, rules)
        explicit_shift = _has_explicit_shift_marker(segment.text)
        strong_pair = _has_strong_boundary_pair(current, segment)
        boundary_allowed = (
            signature_change_count >= min_signature_change_count
            and len(current.text) >= min_boundary_segment_len
            and len(segment.text) >= min_boundary_segment_len
            and similarity < similarity_threshold
            and not weak_new_signal
            and (explicit_shift or strong_pair)
        )
        would_be_too_long = len(current.text) + len(segment.text) > max_episode_len

        if boundary_allowed:
            grouped.append(segment)
            continue
        if would_be_too_long and len(current.text) >= min_episode_len and signature_change_count >= min_signature_change_count:
            grouped.append(segment)
            continue
        grouped[-1] = _merge_segment_states(current, segment)

    stabilized: list[SegmentState] = []
    for segment in grouped:
        if stabilized and (
            len(segment.text) < min_episode_len or _text_similarity(stabilized[-1].text, segment.text) >= similarity_threshold
        ):
            stabilized[-1] = _merge_segment_states(stabilized[-1], segment)
        else:
            stabilized.append(segment)
    return stabilized


def _merge_segment_states(left: SegmentState, right: SegmentState) -> SegmentState:
    """Merge adjacent segment states while preserving the strongest signals."""
    merged_text = combine_text(left.text, right.text)
    return SegmentState(
        text=merged_text,
        question_type=_prefer_signal(left.question_type, right.question_type),
        bottleneck_text=_prefer_signal(left.bottleneck_text, right.bottleneck_text),
        tool_env=_prefer_signal(left.tool_env, right.tool_env),
        collaborator=_prefer_signal(left.collaborator, right.collaborator),
        desired_output=_prefer_signal(left.desired_output, right.desired_output),
        role_clue=_prefer_signal(left.role_clue, right.role_clue),
        work_moment=_prefer_signal(left.work_moment, right.work_moment),
    )


def _derive_segment_state(text: str) -> SegmentState:
    """Extract segment-level signals used for episode boundaries."""
    normalized = clean_text(text)
    return SegmentState(
        text=normalized,
        question_type=_classify(normalized, QUESTION_TYPE_PATTERNS, fallback="workflow_help"),
        bottleneck_text=_extract_bottleneck(normalized),
        tool_env=_classify(normalized, TOOL_PATTERNS, fallback="unknown"),
        collaborator=_classify(normalized, COLLAB_PATTERNS, fallback="solo"),
        desired_output=_classify(normalized, OUTPUT_PATTERNS, fallback="unspecified_output"),
        role_clue=_classify(normalized, ROLE_PATTERNS, fallback=""),
        work_moment=_classify(normalized, WORK_MOMENT_PATTERNS, fallback="unspecified"),
    )


def _classify(text: str, patterns: dict[str, list[str]], fallback: str) -> str:
    """Return the best matching label from a keyword map."""
    lowered = text.lower()
    best_label = fallback
    best_score = 0
    for label, keywords in patterns.items():
        score = sum(1 for keyword in keywords if keyword in lowered)
        if score > best_score:
            best_label = label
            best_score = score
    return best_label


def _extract_bottleneck(text: str) -> str:
    """Return the dominant bottleneck category for the segment."""
    return _classify(text, BOTTLENECK_PATTERNS, fallback="general_friction")


def _business_question(segment: SegmentState) -> str:
    """Map segment signature to a compact business question."""
    question_map = {
        "reporting": "How can we deliver recurring reporting output faster and with fewer manual steps?",
        "reconciliation": "How can we validate and reconcile reported numbers with less manual QA?",
        "investigation": "How can we diagnose and resolve analytics issues faster?",
        "automation": "What part of this workflow should be automated first?",
    }
    return question_map.get(segment.question_type, "How can this workflow be made more reliable and repeatable?")


def _extract_workaround(text: str) -> str:
    """Extract a coarse workaround signal from the episode text."""
    lowered = text.lower()
    if "copy paste" in lowered or "manual" in lowered:
        return "manual workaround"
    if "excel" in lowered or "spreadsheet" in lowered:
        return "spreadsheet workaround"
    if "script" in lowered or "python" in lowered:
        return "ad hoc script workaround"
    return ""


def _score_product_fit(segment: SegmentState, text: str) -> str:
    """Assign a lightweight fit label for downstream prioritization."""
    lowered = text.lower()
    if segment.bottleneck_text != "general_friction" and (
        "manual" in lowered or "repetitive" in lowered or "recurring" in lowered
    ):
        return "strong_fit"
    if segment.desired_output != "unspecified_output":
        return "review"
    return "weak_fit"


def _segmentation_note(segment: SegmentState, rules: dict[str, Any]) -> str:
    """Build a compact trace note for auditing why this became an episode."""
    return (
        f"strategy={rules.get('default_strategy', 'conservative_signature_split')};"
        f"quality_gate=passed;"
        f"q={segment.question_type};"
        f"b={segment.bottleneck_text};"
        f"tool={segment.tool_env};"
        f"collab={segment.collaborator};"
        f"out={segment.desired_output}"
    )


def _passes_episode_quality_filter(text: str, rules: dict[str, Any], source: str = "") -> bool:
    """Return whether text has enough workflow and metric pain to become an episode."""
    return _assess_episode_quality(text, rules, source=source).passes


def _assess_episode_quality(text: str, rules: dict[str, Any], source: str = "") -> QualityAssessment:
    """Return hard-pass, borderline, or fail quality decision with source-aware reasons."""
    quality_cfg = rules.get("quality_filter", {}) or {}
    if not bool(quality_cfg.get("enabled", False)):
        return QualityAssessment(score=1.0, bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
    lowered = clean_text(text).lower()
    if not lowered:
        return QualityAssessment(score=0.0, bucket="fail", fail_reason="short_text_no_workflow_signal", rescue_reason="", passes=False)
    workflow_terms = [str(term).lower() for term in quality_cfg.get("workflow_pain_terms", [])]
    metric_terms = [str(term).lower() for term in quality_cfg.get("metric_problem_terms", [])]
    required_terms = [str(term).lower() for term in quality_cfg.get("required_problem_terms", [])]
    if source == "shopify_community":
        workflow_terms = [
            *workflow_terms,
            "troubleshoot",
            "troubleshooting",
            "debugging session",
            "figure out",
            "feedback",
            "escalation",
            "analysis",
            "funnel",
            "price mismatch",
            "showing nothing",
        ]
        metric_terms = [
            *metric_terms,
            "sessions",
            "orders",
            "sales",
            "checkout",
            "ga4",
            "inventory",
            "merchant center",
            "feed",
        ]
        required_terms = [
            *required_terms,
            "drop",
            "dropped",
            "plummeting",
            "low",
            "off by",
            "showing nothing",
            "disappeared",
            "missing",
        ]
    usage_patterns = [str(term).lower() for term in quality_cfg.get("usage_only_patterns", [])]
    has_workflow_pain = any(term in lowered for term in workflow_terms)
    has_metric_problem = any(term in lowered for term in metric_terms)
    has_required_problem = any(term in lowered for term in required_terms)
    structural_pain = _has_structural_reporting_pain(lowered)
    if not has_workflow_pain and structural_pain:
        has_workflow_pain = True
    if not has_required_problem and structural_pain:
        has_required_problem = True
    usage_only = any(lowered.startswith(pattern) for pattern in usage_patterns) and not has_required_problem
    if source not in {"shopify_community", "google_ads_help_community"}:
        passed = has_workflow_pain and has_metric_problem and has_required_problem and not usage_only
        return QualityAssessment(
            score=1.0 if passed else 0.0,
            bucket="hard_pass" if passed else "fail",
            fail_reason="" if passed else "quality_filter_failed",
            rescue_reason="",
            passes=passed,
        )

    if source == "google_ads_help_community":
        metric_presence = any(
            term in lowered
            for term in [
                "conversion",
                "conversions",
                "conversion action",
                "click",
                "clicks",
                "impressions",
                "report",
                "reporting",
                "metrics",
                "campaign",
                "performance",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "mismatch",
                "discrepancy",
                "not matching",
                "not showing",
                "wrong",
                "delay",
                "zero impressions",
                "not generating impressions",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "campaign",
                "performance",
                "reporting",
                "conversion action",
                "merchant center",
                "attribution",
                "ad preview",
                "diagnosis",
                "search campaign",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "what could be the issue",
                "preventing",
                "help identify",
                "why",
                "cannot see",
                "not showing",
                "could this be related",
            ]
        )
        discussion_style = any(
            term in lowered
            for term in [
                "hi everyone",
                "could you please review",
                "i would really appreciate it",
                "thanks in advance",
                "can anyone help",
            ]
        )
        account_support_only = any(
            term in lowered
            for term in [
                "identity verification",
                "payment verification",
                "suspended account",
                "billing issue",
                "under review",
                "wrong google account",
            ]
        ) and not metric_presence
        low_signal = not any([metric_presence, discrepancy_presence, analysis_context, explanation_burden])

        score = 0.0
        score += 1.2 if metric_presence else 0.0
        score += 1.1 if discrepancy_presence or has_required_problem else 0.0
        score += 0.9 if analysis_context else 0.0
        score += 0.8 if explanation_burden else 0.0
        score += 0.7 if has_workflow_pain else 0.0
        score += 0.5 if discussion_style and (metric_presence or analysis_context) else 0.0
        score -= 1.2 if usage_only else 0.0

        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                discussion_style and (metric_presence or analysis_context),
            ]
        )
        if account_support_only:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="support_case_without_analysis_context",
                rescue_reason="",
                passes=False,
            )
        if len(lowered) < 140 and not discrepancy_presence and not explanation_burden:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="short_text_help_stub",
                rescue_reason="",
                passes=False,
            )
        if low_signal:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="complaint_without_operational_context",
                rescue_reason="",
                passes=False,
            )
        if signal_count >= 3 and (discrepancy_presence or has_required_problem or explanation_burden):
            return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
        if signal_count >= 2:
            if metric_presence and not (discrepancy_presence or has_required_problem):
                return QualityAssessment(
                    score=round(score, 3),
                    bucket="borderline",
                    fail_reason="metric_present_but_no_explicit_blocker",
                    rescue_reason="google_ads_help_metric_context_rescue",
                    passes=True,
                )
            if not has_workflow_pain and (metric_presence or analysis_context):
                return QualityAssessment(
                    score=round(score, 3),
                    bucket="borderline",
                    fail_reason="weak_workflow_signal",
                    rescue_reason="google_ads_help_reporting_interpretation_rescue",
                    passes=True,
                )
            if discussion_style and (metric_presence or analysis_context):
                return QualityAssessment(
                    score=round(score, 3),
                    bucket="borderline",
                    fail_reason="discussion_style_but_relevant",
                    rescue_reason="google_ads_help_support_style_rescue",
                    passes=True,
                )
            return QualityAssessment(
                score=round(score, 3),
                bucket="borderline",
                fail_reason="weak_problem_phrasing",
                rescue_reason="google_ads_help_problem_phrasing_rescue",
                passes=True,
            )
        return QualityAssessment(
            score=round(score, 3),
            bucket="fail",
            fail_reason="impression_problem_without_reporting_context" if metric_presence else "complaint_without_operational_context",
            rescue_reason="",
            passes=False,
        )

    metric_presence = any(term in lowered for term in ["metric", "metrics", "report", "reporting", "analytics", "dashboard", "export", "csv"])
    discrepancy_presence = any(term in lowered for term in ["discrepancy", "mismatch", "not matching", "wrong", "confusion", "off by"])
    business_metric_presence = any(term in lowered for term in ["sales", "revenue", "orders", "sessions", "conversion", "aov", "roas", "checkout"])
    analysis_context = any(term in lowered for term in ["compare", "comparison", "trend", "weekly", "monthly", "performance", "ga4", "merchant center"])
    explanation_burden = any(term in lowered for term in ["figure out", "cannot explain", "explain", "why", "interpret", "understand", "what changed"])
    discussion_style = any(term in lowered for term in ["feedback", "curious", "anyone else", "what do you check first", "how do you handle", "looking for advice"])
    operational_context = any(term in lowered for term in ["store", "campaign", "product", "inventory", "checkout", "merchant center", "report", "dashboard"])
    pure_feature_request = any(term in lowered for term in ["feature request", "would be nice", "idea:"]) and not metric_presence and not business_metric_presence
    generic_tips = any(term in lowered for term in ["tips", "best apps", "inspiration", "advice"]) and not metric_presence and not discrepancy_presence
    low_signal = not any([metric_presence, discrepancy_presence, business_metric_presence, analysis_context, explanation_burden])

    score = 0.0
    score += 1.3 if metric_presence else 0.0
    score += 1.1 if discrepancy_presence else 0.0
    score += 1.0 if business_metric_presence else 0.0
    score += 0.9 if analysis_context else 0.0
    score += 0.8 if explanation_burden else 0.0
    score += 0.7 if has_workflow_pain else 0.0
    score += 0.6 if discussion_style and (metric_presence or business_metric_presence or analysis_context) else 0.0
    score += 0.5 if operational_context else 0.0
    score -= 1.5 if pure_feature_request else 0.0
    score -= 1.2 if generic_tips else 0.0
    score -= 0.8 if usage_only else 0.0

    signal_count = sum(
        int(flag)
        for flag in [
            metric_presence,
            discrepancy_presence or has_required_problem,
            business_metric_presence,
            analysis_context,
            explanation_burden,
            discussion_style and (metric_presence or business_metric_presence or analysis_context),
        ]
    )
    if pure_feature_request:
        return QualityAssessment(score=round(score, 3), bucket="fail", fail_reason="complaint_without_operational_context", rescue_reason="", passes=False)
    if generic_tips or low_signal:
        return QualityAssessment(score=round(score, 3), bucket="fail", fail_reason="truly_weak_low_signal", rescue_reason="", passes=False)
    if signal_count >= 3 and (discrepancy_presence or has_required_problem or explanation_burden):
        return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
    if signal_count >= 2:
        fail_reason = ""
        rescue_reason = "shopify_quality_rescue"
        if discussion_style and (metric_presence or business_metric_presence or analysis_context):
            fail_reason = "discussion_style_but_relevant"
            rescue_reason = "shopify_discussion_style_rescue"
        elif metric_presence and not (discrepancy_presence or has_required_problem):
            fail_reason = "metric_present_but_no_explicit_blocker"
            rescue_reason = "shopify_metric_context_rescue"
        elif not has_workflow_pain and (metric_presence or analysis_context):
            fail_reason = "weak_workflow_signal"
            rescue_reason = "shopify_workflow_weak_rescue"
        else:
            fail_reason = "weak_problem_phrasing"
            rescue_reason = "shopify_problem_phrasing_rescue"
        return QualityAssessment(score=round(score, 3), bucket="borderline", fail_reason=fail_reason, rescue_reason=rescue_reason, passes=True)
    fail_reason = "weak_problem_phrasing" if business_metric_presence or metric_presence else "complaint_without_operational_context"
    return QualityAssessment(score=round(score, 3), bucket="fail", fail_reason=fail_reason, rescue_reason="", passes=False)


def build_parser_schema_diff(valid_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize source parser output schemas visible to episode building."""
    rows: list[dict[str, Any]] = []
    if valid_df.empty:
        return pd.DataFrame(rows)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for _, row in valid_df.iterrows():
        schema = _schema_flags_from_row(row)
        grouped.setdefault((str(row.get("source", "")), schema["source_schema_type"]), []).append(schema)
    for (source, schema_type), items in grouped.items():
        total = max(len(items), 1)
        rows.append(
            {
                "source": source,
                "source_schema_type": schema_type,
                "row_count": len(items),
                "reply_like_ratio": round(sum(1 for item in items if item["reply_like_schema"]) / total, 4),
                "missing_body_ratio": round(sum(1 for item in items if item["body_len"] == 0) / total, 4),
                "comments_present_ratio": round(sum(1 for item in items if item["comments_len"] > 0) / total, 4),
                "parent_context_ratio": round(sum(1 for item in items if item["parent_context_len"] > 0) / total, 4),
                "thread_title_ratio": round(sum(1 for item in items if item["thread_title_len"] > 0) / total, 4),
                "top_level_meta_keys": items[0]["top_level_meta_keys"],
                "nested_meta_keys": items[0]["nested_meta_keys"],
            }
        )
    return pd.DataFrame(rows).sort_values(["source", "row_count"], ascending=[True, False]).reset_index(drop=True)


def _prefer_signal(left: str, right: str) -> str:
    """Prefer the more specific non-empty signal when merging segments."""
    generic = {"", "unknown", "solo", "workflow_help", "general_friction", "unspecified", "unspecified_output"}
    if left in generic and right not in generic:
        return right
    return left


def _signature_change_count(left: SegmentState, right: SegmentState, rules: dict[str, Any]) -> int:
    """Count meaningful signature changes between adjacent segments."""
    generic_values = set(str(value) for value in rules.get("generic_values", []))
    changes = 0
    for left_value, right_value in zip(left.signature, right.signature, strict=True):
        if left_value == right_value:
            continue
        if left_value in generic_values and right_value in generic_values:
            continue
        changes += 1
    return changes


def _is_non_boundary_segment(text: str, rules: dict[str, Any]) -> bool:
    """Return True when a unit is mostly boilerplate, promo, or repetitive filler."""
    lowered = clean_text(text).lower()
    if not lowered:
        return True
    for pattern in rules.get("non_boundary_patterns", []):
        if str(pattern).lower() in lowered:
            return True
    if len(lowered) < int(rules.get("min_unit_len", 80)):
        return True
    return False


def _has_explicit_shift_marker(text: str) -> bool:
    """Return True when the text contains a real transition cue."""
    lowered = clean_text(text).lower()
    markers = [
        "on the other hand",
        "separately",
        "another issue",
        "different problem",
        "meanwhile",
        "however",
        "but then",
        "next problem",
        "also need",
        "in addition",
    ]
    return any(marker in lowered for marker in markers)


def _has_strong_boundary_pair(left: SegmentState, right: SegmentState) -> bool:
    """Allow a split only when multiple non-generic families truly shift."""
    left_values = set(left.signature)
    right_values = set(right.signature)
    if "unknown" in left_values and "unknown" in right_values:
        return False

    different_families = 0
    for left_value, right_value in zip(left.signature, right.signature, strict=True):
        if left_value == right_value:
            continue
        if left_value in {"unknown", "solo", "workflow_help", "general_friction", "unspecified", "unspecified_output"}:
            continue
        if right_value in {"unknown", "solo", "workflow_help", "general_friction", "unspecified", "unspecified_output"}:
            continue
        different_families += 1
    return different_families >= 2


def _is_boilerplate(text: str, rules: dict[str, Any]) -> bool:
    """Return True when text looks like platform boilerplate or support filler."""
    lowered = clean_text(text).lower()
    if not lowered:
        return True
    for pattern in rules.get("boilerplate_patterns", []):
        if str(pattern).lower() in lowered:
            return True
    return False


def _text_similarity(left: str, right: str) -> float:
    """Compute a simple Jaccard similarity over normalized token sets."""
    left_tokens = set(re.findall(r"[a-zA-Z][a-zA-Z0-9]+", left.lower()))
    right_tokens = set(re.findall(r"[a-zA-Z][a-zA-Z0-9]+", right.lower()))
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    return overlap / union if union else 0.0


def _build_episode_row_diagnostics(row: pd.Series, rules: dict[str, Any], source: str = "") -> dict[str, Any]:
    """Derive schema and quality context used for episode diagnostics."""
    title = clean_text(str(row.get("title", "") or ""))
    body = clean_text(str(row.get("body", "") or ""))
    comments_text = clean_text(str(row.get("comments_text", "") or ""))
    thread_title = clean_text(str(row.get("thread_title", "") or ""))
    parent_context = clean_text(str(row.get("parent_context", "") or ""))
    combined_text = combine_text(title, body, comments_text, parent_context)
    schema_flags = _schema_flags_from_row(row)
    combined_primary = combine_text(title, body, parent_context) if title and (body or parent_context) else combined_text
    quality = _assess_episode_quality(combined_text, rules, source=source)
    return {
        **schema_flags,
        "title": title,
        "body": body,
        "comments_text": comments_text,
        "thread_title": thread_title,
        "parent_context": parent_context,
        "combined_text": combined_text,
        "combined_primary": combined_primary,
        "candidate_unit_count_raw": 0,
        "candidate_unit_count_cleaned": 0,
        "duplicate_collapse_count": 0,
        "passes_combined_quality": quality.passes,
        "quality_score": quality.score,
        "quality_bucket": quality.bucket,
        "quality_fail_reason": quality.fail_reason,
        "rescue_reason": quality.rescue_reason,
    }


def _schema_flags_from_row(row: pd.Series) -> dict[str, Any]:
    """Return parser/source schema hints visible in the normalized row."""
    meta = get_record_source_meta(row)
    nested_keys = sorted(meta.keys())
    title = clean_text(str(row.get("title", "") or ""))
    body = clean_text(str(row.get("body", "") or ""))
    comments_text = clean_text(str(row.get("comments_text", "") or ""))
    parent_context = clean_text(str(row.get("parent_context", "") or ""))
    thread_title = clean_text(str(row.get("thread_title", "") or ""))
    source_type = str(row.get("source_type", "") or "")
    api_item = meta.get("api_item", {}) if isinstance(meta.get("api_item"), dict) else {}
    raw_topic = meta.get("raw_topic", {}) if isinstance(meta.get("raw_topic"), dict) else {}
    discovery_ref = meta.get("discovery_ref", {}) if isinstance(meta.get("discovery_ref"), dict) else {}
    reply_like_schema = False
    source_schema_type = source_type or "unknown"
    if api_item:
        depth = int(api_item.get("depth", 0) or 0)
        reply_like_schema = depth > 0 or str(api_item.get("subject", "")).strip().lower().startswith("re:")
        source_schema_type = "khoros_reply_message" if reply_like_schema else "khoros_thread_message"
    elif raw_topic:
        source_schema_type = "discourse_topic"
    elif discovery_ref:
        source_schema_type = "discourse_listing_fallback"
    elif source_type == "thread":
        source_schema_type = "html_thread"
    missing_required_fields = not (str(row.get("source", "") or "").strip() and str(row.get("raw_id", "") or "").strip() and (title or body or comments_text))
    return {
        "source_schema_type": source_schema_type,
        "source_type": source_type,
        "reply_like_schema": reply_like_schema,
        "missing_required_fields": missing_required_fields,
        "title_len": len(title),
        "body_len": len(body),
        "comments_len": len(comments_text),
        "parent_context_len": len(parent_context),
        "thread_title_len": len(thread_title),
        "combined_text_len": len(combine_text(title, body, comments_text, parent_context)),
        "top_level_meta_keys": "json",
        "nested_meta_keys": "|".join(nested_keys),
    }


def _derive_drop_reason(
    diagnostics: dict[str, Any],
    grouped_segments: list[SegmentState] | None = None,
    quality: QualityAssessment | None = None,
) -> str:
    """Return a specific reason when a row fails episode promotion."""
    if diagnostics["missing_required_fields"]:
        return "missing_required_fields_for_episode_creation"
    if diagnostics["source_schema_type"] == "khoros_reply_message" and diagnostics["parent_context_len"] == 0:
        if diagnostics["passes_combined_quality"]:
            return "title_body_merge_failure"
            return "unsupported_page_schema"
    if diagnostics["combined_text_len"] < 120:
        return "text_too_short"
    if grouped_segments:
        lowered = clean_text(grouped_segments[0].text).lower() if grouped_segments else ""
        if lowered and not _passes_episode_quality_filter(lowered, {"quality_filter": {"enabled": True}}):
            pass
    effective_quality = quality or QualityAssessment(
        score=float(diagnostics.get("quality_score", 0.0) or 0.0),
        bucket=str(diagnostics.get("quality_bucket", "fail") or "fail"),
        fail_reason=str(diagnostics.get("quality_fail_reason", "") or ""),
        rescue_reason=str(diagnostics.get("rescue_reason", "") or ""),
        passes=bool(diagnostics.get("passes_combined_quality", False)),
    )
    if diagnostics["passes_combined_quality"] and diagnostics["candidate_unit_count_cleaned"] > 0:
        return "title_body_merge_failure"
    if diagnostics["duplicate_collapse_count"] > 0 and diagnostics["candidate_unit_count_cleaned"] <= 1 and diagnostics["passes_combined_quality"]:
        return "duplicate_collapse_issue"
    return effective_quality.fail_reason or "quality_filter_failed"


def _build_debug_record(
    row: pd.Series,
    diagnostics: dict[str, Any],
    episode_count: int,
    drop_reason: str,
    quality: QualityAssessment | None = None,
) -> EpisodeBuildDebug:
    """Create one stable debug row for a source post."""
    drop_detail_map = {
        "text_too_short": "combined_title_body_comments_below_min_episode_len",
        "unsupported_page_schema": "reply_message_schema_without_root_problem_context",
        "title_body_merge_failure": "problem_signal_in_title_not_carried_into_episode_unit",
        "duplicate_collapse_issue": "duplicate_unit_merge_left_no_distinct_episode_candidate",
        "missing_required_fields_for_episode_creation": "missing_source_raw_id_or_text_fields",
        "quality_filter_failed": "quality_filter_removed_all_grouped_segments",
    }
    effective_quality = quality or QualityAssessment(
        score=float(diagnostics.get("quality_score", 0.0) or 0.0),
        bucket=str(diagnostics.get("quality_bucket", "fail") or "fail"),
        fail_reason=str(diagnostics.get("quality_fail_reason", "") or ""),
        rescue_reason=str(diagnostics.get("rescue_reason", "") or ""),
        passes=bool(diagnostics.get("passes_combined_quality", False)),
    )
    return EpisodeBuildDebug(
        source=str(row.get("source", "") or ""),
        raw_id=str(row.get("raw_id", "") or ""),
        url=str(row.get("url", "") or ""),
        source_schema_type=str(diagnostics["source_schema_type"]),
        source_type=str(diagnostics["source_type"]),
        episode_count=int(episode_count),
        drop_reason="" if episode_count else drop_reason,
        drop_detail="" if episode_count else drop_detail_map.get(drop_reason, ""),
        title_len=int(diagnostics["title_len"]),
        body_len=int(diagnostics["body_len"]),
        comments_len=int(diagnostics["comments_len"]),
        parent_context_len=int(diagnostics["parent_context_len"]),
        thread_title_len=int(diagnostics["thread_title_len"]),
        combined_text_len=int(diagnostics["combined_text_len"]),
        candidate_unit_count_raw=int(diagnostics["candidate_unit_count_raw"]),
        candidate_unit_count_cleaned=int(diagnostics["candidate_unit_count_cleaned"]),
        duplicate_collapse_count=int(diagnostics["duplicate_collapse_count"]),
        title_body_combined_used=bool(diagnostics["combined_primary"]),
        missing_required_fields=bool(diagnostics["missing_required_fields"]),
        reply_like_schema=bool(diagnostics["reply_like_schema"]),
        passes_combined_quality=bool(diagnostics["passes_combined_quality"]),
        quality_score=float(effective_quality.score),
        quality_bucket=str(effective_quality.bucket),
        quality_fail_reason=str(effective_quality.fail_reason),
        rescue_reason=str(effective_quality.rescue_reason),
        top_level_meta_keys=str(diagnostics["top_level_meta_keys"]),
        nested_meta_keys=str(diagnostics["nested_meta_keys"]),
    )


def _has_structural_reporting_pain(lowered: str) -> bool:
    """Recognize structural reporting pain that lacks classic why/wrong phrasing."""
    structural_terms = [
        "instead of",
        "offline source",
        "not able",
        "unable to",
        "doesn't show",
        "not showing",
        "cannot use",
        "can t use",
        "summary",
        "summarized",
        "workaround",
    ]
    return any(term in lowered for term in structural_terms)
