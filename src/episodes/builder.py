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
    segments = [segment for segment in segments if not _is_non_boundary_segment(segment.text, rules, source=source)]
    if not segments:
        fallback = _derive_segment_state(diagnostics["combined_text"])
        segments = [fallback] if len(fallback.text) >= min_episode_len else []
    if not segments:
        return [], _build_debug_record(row, diagnostics, episode_count=0, drop_reason=_derive_drop_reason(diagnostics))

    grouped = _group_segments(segments, rules, source=source)
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
    source = str(row.get("source", "") or "")
    body_has_multi_paragraphs = bool(re.search(r"\n\s*\n+", body))
    source_split_units = _split_source_shift_units(body, source, rules)
    use_title_only_primary = bool(source_split_units)

    if source == "adobe_analytics_community" and body_has_multi_paragraphs and title:
        units.append(title)
    elif use_title_only_primary and title:
        units.append(title)
    elif combined_primary:
        units.append(combined_primary)
    elif title:
        units.append(title)
    if parent_context and parent_context not in units:
        units.append(parent_context)
    if source == "adobe_analytics_community" and body_has_multi_paragraphs:
        adobe_paragraphs = [clean_text(part) for part in re.split(r"\n\s*\n+", body) if clean_text(part)]
        for paragraph in adobe_paragraphs:
            split_units = _split_source_shift_units(paragraph, source, rules)
            if split_units:
                units.extend(split_units)
            elif len(paragraph) >= int(rules.get("min_unit_len", 80)):
                units.append(paragraph)
    else:
        if source_split_units:
            units.extend(source_split_units)
        else:
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


def _group_segments(segments: list[SegmentState], rules: dict[str, Any], source: str = "") -> list[SegmentState]:
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
    if source == "adobe_analytics_community":
        min_boundary_segment_len = max(140, min_boundary_segment_len - 100)
        min_signature_change_count = max(2, min_signature_change_count - 1)
    elif source == "domo_community_forum":
        min_boundary_segment_len = max(100, min_boundary_segment_len - 160)

    grouped: list[SegmentState] = [segments[0]]
    for segment in segments[1:]:
        current = grouped[-1]
        signature_change_count = _signature_change_count(current, segment, rules)
        adobe_domain_shift = source == "adobe_analytics_community" and _adobe_domain_shift(current.text, segment.text)
        domo_domain_shift = source == "domo_community_forum" and _domo_domain_shift(current.text, segment.text)
        if adobe_domain_shift:
            signature_change_count = max(signature_change_count, min_signature_change_count)
        if domo_domain_shift:
            signature_change_count = max(signature_change_count, min_signature_change_count)
        similarity = _text_similarity(current.text, segment.text)
        weak_new_signal = _is_non_boundary_segment(segment.text, rules, source=source)
        explicit_shift = _has_explicit_shift_marker(segment.text)
        strong_pair = _has_strong_boundary_pair(current, segment, source=source)
        boundary_allowed = (
            signature_change_count >= min_signature_change_count
            and len(current.text) >= min_boundary_segment_len
            and len(segment.text) >= min_boundary_segment_len
            and similarity < similarity_threshold
            and (not weak_new_signal or adobe_domain_shift or domo_domain_shift)
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
            "inventory",
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
    if source == "hubspot_community":
        # HubSpot threads often describe report-builder, attribution, lifecycle, and CRM
        # workflow limitations without using explicit mismatch wording.
        workflow_terms = [
            *workflow_terms,
            "hubspot",
            "crm",
            "report",
            "reporting",
            "dashboard",
            "custom report",
            "journey report",
            "attribution",
            "utm",
            "campaign",
            "campaigns",
            "lifecycle stage",
            "deal",
            "deals",
            "contact",
            "contacts",
            "segment",
            "segments",
            "marketing email",
            "filter",
            "filters",
            "data studio",
        ]
        metric_terms = [
            *metric_terms,
            "hubspot",
            "report",
            "reporting",
            "dashboard",
            "conversion rate",
            "attribution",
            "utm",
            "revenue",
            "campaign",
            "campaigns",
            "marketing influences",
            "health score",
            "page views",
            "contacts",
            "custom property",
            "segment",
            "email performance",
            "open",
            "click",
            "click-through",
        ]
        required_terms = [
            *required_terms,
            "can't use",
            "cannot use",
            "can't see",
            "cannot see",
            "at a loss",
            "struggling",
            "unreliable",
            "doesn't show",
            "doesnt show",
            "stops applying",
            "prevents",
            "replicate",
            "is there a way",
            "best way",
            "walk me through",
            "trying to build",
            "trying to create",
            "missing data",
            "not available",
        ]
    if source == "klaviyo_community":
        # Klaviyo posts often describe CRM/email workflow failures in operational language
        # like lists, flows, templates, integrations, or rate drops rather than BI-style
        # reporting mismatch phrasing.
        workflow_terms = [
            *workflow_terms,
            "flow",
            "flows",
            "campaign",
            "campaigns",
            "segment",
            "segments",
            "popup",
            "pop-up",
            "signup form",
            "integration",
            "deliverability",
            "template",
            "templates",
            "tracking",
            "discount code",
            "coupon",
        ]
        metric_terms = [
            *metric_terms,
            "email",
            "emails",
            "open rate",
            "click rate",
            "revenue",
            "deliverability",
            "list",
            "lists",
            "subscriber",
            "subscribers",
            "profile",
            "tracking",
            "page view",
            "browse abandonment",
            "welcome flow",
            "post-purchase flow",
        ]
        required_terms = [
            *required_terms,
            "not added",
            "not being added",
            "decrease",
            "dropped",
            "not working",
            "not available",
            "error occurred",
            "try again later",
            "challenge",
            "cumbersome",
            "manual process",
            "unclear",
            "disrupted",
            "how can i",
            "best way",
            "please help",
        ]
    if source == "adobe_analytics_community":
        workflow_terms = [
            *workflow_terms,
            "adobe analytics",
            "workspace",
            "debugger",
            "report suite",
            "segment",
            "calculated metric",
            "evar",
            "prop",
            "classification",
            "attribution",
            "data feed",
            "alert",
            "anomaly alert",
            "direct traffic",
            "seo",
        ]
        metric_terms = [
            *metric_terms,
            "workspace",
            "debugger",
            "report suite",
            "segment",
            "calculated metric",
            "evar",
            "prop",
            "classification",
            "attribution",
            "direct traffic",
            "seo",
            "customers metric",
            "traffic",
        ]
        required_terms = [
            *required_terms,
            "not in workspace",
            "does not appear",
            "doesn't appear",
            "visible in debugger",
            "spike",
            "drop",
            "time delay",
            "way earlier",
            "sent after",
            "is there a way",
        ]
    if source == "domo_community_forum":
        workflow_terms = [
            *workflow_terms,
            "domo",
            "card",
            "chart",
            "graph by",
            "hourly chart",
            "time scale",
            "filter card",
            "filter view",
            "data table",
            "analyzer",
            "beast mode",
            "dataset",
        ]
        metric_terms = [
            *metric_terms,
            "domo",
            "card",
            "chart",
            "graph by",
            "hourly chart",
            "time scale",
            "filter card",
            "data table",
            "dataset",
            "month end date",
        ]
        required_terms = [
            *required_terms,
            "forcing",
            "hard to get",
            "default selection",
            "not allow user to clear filter",
            "always have something selected",
            "is it possible",
            "why is",
        ]
    if source == "power_bi_community":
        # Power BI threads often describe interpretation and diagnosis pain with BI-native
        # language like measures, visuals, refresh, service/desktop differences, and filter
        # context rather than generic business reporting mismatch phrasing.
        workflow_terms = [
            *workflow_terms,
            "power bi",
            "dax",
            "measure",
            "measures",
            "matrix",
            "slicer",
            "visual",
            "visuals",
            "line chart",
            "legend",
            "filter context",
            "row context",
            "gateway",
            "refresh",
            "desktop",
            "service",
            "drill through",
            "relationship",
            "relationships",
            "rankx",
        ]
        metric_terms = [
            *metric_terms,
            "power bi",
            "report",
            "reports",
            "reporting",
            "dashboard",
            "visual",
            "visuals",
            "matrix",
            "table",
            "export",
            "csv",
            "measure",
            "measures",
            "dax",
            "count",
            "distinct count",
            "total",
            "totals",
            "forecast",
            "actual",
            "refresh",
            "gateway",
        ]
        required_terms = [
            *required_terms,
            "why does",
            "why is",
            "not in power bi service",
            "gone missing",
            "failed refresh",
            "timed out",
            "wrong total",
            "wrong totals",
            "not matching",
            "different numbers",
            "incorrect data",
            "doesn't match",
            "doesnt match",
            "doesn't work",
            "doesnt work",
            "limitation",
            "workaround",
            "issue",
            "problem",
        ]
    if source == "qlik_community":
        # Qlik threads often describe reporting pain with Qlik-native chart, totals,
        # expression, and export language rather than generic dashboard mismatch terms.
        workflow_terms = [
            *workflow_terms,
            "qlik",
            "qlik sense",
            "qlikview",
            "nprinting",
            "set analysis",
            "pivot table",
            "straight table",
            "combo chart",
            "cross tab",
            "gauge chart",
            "filter pane",
            "dimension",
            "measure",
            "expression",
            "totals",
            "pixel perfect",
            "aggr",
            "fractile",
        ]
        metric_terms = [
            *metric_terms,
            "qlik",
            "report",
            "reports",
            "reporting",
            "dashboard",
            "chart",
            "table",
            "excel",
            "export",
            "measure",
            "expression",
            "count",
            "total",
            "totals",
            "kpi",
            "nprinting",
        ]
        required_terms = [
            *required_terms,
            "wrong total",
            "wrong totals",
            "different totals",
            "not aggregating",
            "not matching",
            "mismatch",
            "issue",
            "problem",
            "error",
            "limitation",
            "workaround",
            "stuck",
            "desired level",
            "collapsed or expanded",
            "not available",
        ]
    if source == "sisense_community":
        # Sisense threads often describe dashboard interpretation and filter behavior
        # through widget, break-by, and scripting language rather than generic mismatch terms.
        workflow_terms = [
            *workflow_terms,
            "sisense",
            "dashboard",
            "dashboards",
            "widget",
            "widgets",
            "pivot table",
            "column chart",
            "bar chart",
            "filter",
            "filters",
            "break by",
            "data model",
            "drill dashboard",
            "jump to dashboard",
            "scientific units",
            "javascript",
        ]
        metric_terms = [
            *metric_terms,
            "sisense",
            "dashboard",
            "widget",
            "pivot table",
            "table",
            "chart",
            "column chart",
            "bar chart",
            "filter",
            "break by",
            "data model",
            "scientific units",
            "column width",
            "list selection",
        ]
        required_terms = [
            *required_terms,
            "reset",
            "limits",
            "dynamically change",
            "hide",
            "empty legend",
            "same month or not",
            "column width",
            "calculate",
            "multiple columns",
            "left-click",
            "background filter",
            "better way",
            "is it possible",
        ]
    if source == "mixpanel_community":
        # Mixpanel threads often describe event identity, funnel, and reporting trust
        # pain in product-analytics language rather than generic BI/reporting phrasing.
        workflow_terms = [
            *workflow_terms,
            "mixpanel",
            "funnel",
            "funnels",
            "retention",
            "insights",
            "event",
            "events",
            "distinct id",
            "identify",
            "mirror",
            "project",
            "cohort",
            "breakdown",
            "session duration",
            "query",
        ]
        metric_terms = [
            *metric_terms,
            "mixpanel",
            "report",
            "reports",
            "dashboard",
            "funnel",
            "retention",
            "event",
            "events",
            "count",
            "conversion time",
            "country",
            "city",
            "breakdown",
            "query",
        ]
        required_terms = [
            *required_terms,
            "wrong",
            "missing",
            "not linking",
            "not showing",
            "discrepancy",
            "limitation",
            "hurdle",
            "issue",
            "problem",
            "why does",
            "why is",
            "missing mirror sync",
        ]
    if source == "metabase_discussions":
        # Metabase threads often phrase dashboard and query pain as operational forum
        # questions about filters, models, sync, and exports rather than generic mismatch wording.
        workflow_terms = [
            *workflow_terms,
            "metabase",
            "dashboard",
            "dashboards",
            "question",
            "questions",
            "model",
            "models",
            "query",
            "queries",
            "native query",
            "native sql",
            "sql editor",
            "filter",
            "filters",
            "dropdown",
            "sync",
            "metadata",
            "migration",
            "connection",
            "schema changes",
            "drill through",
            "legend",
            "export",
        ]
        metric_terms = [
            *metric_terms,
            "metabase",
            "dashboard",
            "report",
            "reporting",
            "chart",
            "charts",
            "table",
            "pivot",
            "pivot table",
            "filter",
            "dropdown",
            "model",
            "question",
            "query",
            "database",
            "views",
            "metadata sync",
            "csv",
            "xlsx",
            "export",
        ]
        required_terms = [
            *required_terms,
            "failed",
            "fails",
            "failing",
            "can't",
            "cannot",
            "unable",
            "not syncing",
            "not showing",
            "breaking dashboards",
            "wrong options",
            "incorrect",
            "issue",
            "problem",
            "workaround",
            "is there a way",
            "trying to",
            "how do you handle",
            "harder to read",
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
    if source not in {
        "shopify_community",
        "hubspot_community",
        "klaviyo_community",
        "adobe_analytics_community",
        "power_bi_community",
        "qlik_community",
        "sisense_community",
        "mixpanel_community",
        "metabase_discussions",
    }:
        passed = has_workflow_pain and has_metric_problem and has_required_problem and not usage_only
        return QualityAssessment(
            score=1.0 if passed else 0.0,
            bucket="hard_pass" if passed else "fail",
            fail_reason="" if passed else "quality_filter_failed",
            rescue_reason="",
            passes=passed,
        )

    if source == "metabase_discussions":
        metric_presence = any(
            term in lowered
            for term in [
                "dashboard",
                "dashboards",
                "question",
                "questions",
                "query",
                "queries",
                "model",
                "models",
                "chart",
                "table",
                "filter",
                "dropdown",
                "metadata",
                "sync",
                "export",
                "xlsx",
                "csv",
                "metabase",
            ]
        )
        problem_presence = any(
            term in lowered
            for term in [
                "issue",
                "problem",
                "failed",
                "fails",
                "failing",
                "can't",
                "cannot",
                "unable",
                "wrong",
                "incorrect",
                "not working",
                "not showing",
                "not syncing",
                "breaking",
                "workaround",
                "how do you handle",
                "is there a way",
                "trying to",
            ]
        )
        explanation_presence = any(
            term in lowered
            for term in [
                "for my work",
                "our team",
                "customer",
                "we have",
                "i have",
                "i am using",
                "i'm using",
                "trying to",
                "need to",
                "want to",
            ]
        )
        if has_workflow_pain and metric_presence and (has_required_problem or problem_presence):
            score = 4.0 if explanation_presence else 3.0
            return QualityAssessment(
                score=score,
                bucket="hard_pass" if explanation_presence else "borderline",
                fail_reason="",
                rescue_reason="metabase_reporting_problem_rescue",
                passes=True,
            )
        return QualityAssessment(
            score=0.0,
            bucket="fail",
            fail_reason="quality_filter_failed",
            rescue_reason="",
            passes=False,
        )

    if source == "hubspot_community":
        metric_presence = any(
            term in lowered
            for term in [
                "hubspot",
                "report",
                "reporting",
                "dashboard",
                "conversion rate",
                "attribution",
                "utm",
                "revenue",
                "campaign",
                "campaigns",
                "marketing influences",
                "health score",
                "page views",
                "contacts",
                "segment",
                "email performance",
                "custom report",
                "journey report",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "can't use",
                "cannot use",
                "can't see",
                "cannot see",
                "unreliable",
                "missing data",
                "doesn't show",
                "doesnt show",
                "not showing",
                "stops applying",
                "prevents",
                "at a loss",
                "struggling",
                "not available",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "custom report",
                "journey report",
                "attribution",
                "utm",
                "lifecycle stage",
                "campaign",
                "segment",
                "marketing email",
                "health score",
                "dashboard",
                "custom property",
                "page views",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "trying to build",
                "trying to create",
                "walk me through",
                "best way",
                "is there a way",
                "where i'm stuck",
                "where im stuck",
                "i don't get it",
                "i dont get it",
                "could someone",
            ]
        )
        discussion_style = any(
            term in lowered
            for term in [
                "hi all",
                "thanks for your help in advance",
                "could someone walk me through",
                "any advice",
                "best way to understand",
            ]
        )
        support_boilerplate = any(
            term in lowered
            for term in [
                "senior community moderator",
                "thanks for bringing this to the community",
                "i'd like to tag",
                "top contributors",
                "thanks so much for coming back",
            ]
        ) and not discrepancy_presence
        low_signal = not any([metric_presence, discrepancy_presence, analysis_context, explanation_burden])

        score = 0.0
        score += 1.2 if metric_presence else 0.0
        score += 1.1 if discrepancy_presence or has_required_problem else 0.0
        score += 0.9 if analysis_context else 0.0
        score += 0.8 if explanation_burden else 0.0
        score += 0.7 if has_workflow_pain else 0.0
        score += 0.4 if discussion_style and (metric_presence or analysis_context) else 0.0
        score -= 1.2 if support_boilerplate else 0.0
        score -= 0.8 if usage_only and not analysis_context else 0.0

        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if support_boilerplate:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="support_reply_without_analysis_context",
                rescue_reason="",
                passes=False,
            )
        if low_signal:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="hubspot_low_signal",
                rescue_reason="",
                passes=False,
            )
        if signal_count >= 3 and (discrepancy_presence or has_required_problem or explanation_burden or analysis_context):
            return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
        if signal_count >= 2:
            rescue_reason = "hubspot_reporting_builder_rescue"
            fail_reason = "weak_problem_phrasing"
            if metric_presence and not (discrepancy_presence or has_required_problem):
                fail_reason = "metric_present_but_no_explicit_blocker"
                rescue_reason = "hubspot_metric_context_rescue"
            elif not has_workflow_pain and (metric_presence or analysis_context):
                fail_reason = "weak_workflow_signal"
                rescue_reason = "hubspot_workflow_weak_rescue"
            elif discussion_style and (metric_presence or discrepancy_presence or analysis_context):
                fail_reason = "discussion_style_but_relevant"
                rescue_reason = "hubspot_discussion_style_rescue"
            return QualityAssessment(score=round(score, 3), bucket="borderline", fail_reason=fail_reason, rescue_reason=rescue_reason, passes=True)
        return QualityAssessment(
            score=round(score, 3),
            bucket="fail",
            fail_reason="hubspot_problem_without_reporting_context",
            rescue_reason="",
            passes=False,
        )

    if source == "klaviyo_community":
        metric_presence = any(
            term in lowered
            for term in [
                "klaviyo",
                "campaign",
                "campaigns",
                "flow",
                "flows",
                "segment",
                "segments",
                "segment count",
                "list count",
                "profile count",
                "revenue",
                "attributed revenue",
                "attribution",
                "reporting",
                "analytics",
                "export",
                "csv",
                "benchmark",
                "benchmark report",
                "conversion",
                "open rate",
                "click rate",
                "deliverability",
                "tracking",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "mismatch",
                "not matching",
                "discrepancy",
                "decrease",
                "dropped",
                "not added",
                "not being added",
                "not syncing",
                "not working",
                "error occurred",
                "try again later",
                "disrupted",
                "not available",
                "challenge",
                "cumbersome",
                "manual process",
                "unclear",
                "bug",
                "issue",
                "reporting lag",
                "what changed",
                "wrong revenue",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "shopify",
                "integration",
                "revenue report",
                "campaign report",
                "flow report",
                "segment report",
                "attribution report",
                "benchmark",
                "page view",
                "tracking",
                "source of truth",
                "compare",
                "comparison",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "how can i",
                "what is the best way",
                "what should i do",
                "trying to",
                "don't know how",
                "dont know how",
                "unclear",
                "please help",
                "appreciate your guidance",
                "point me in the right direction",
                "is this possible",
                "what changed",
            ]
        )
        discussion_style = any(
            term in lowered
            for term in [
                "hi there",
                "hi everyone",
                "dear klaviyo support team",
                "can someone help",
                "thoughts on",
            ]
        )
        generic_tips = any(
            term in lowered
            for term in [
                "best practices",
                "join the community today",
                "product feedback",
                "thoughts on",
                "welcome series",
                "signup form design",
                "email ideas",
                "subject line",
            ]
        ) and not discrepancy_presence and not explanation_burden
        low_signal = not any([metric_presence, discrepancy_presence, analysis_context, explanation_burden])

        score = 0.0
        score += 1.2 if metric_presence else 0.0
        score += 1.2 if discrepancy_presence or has_required_problem else 0.0
        score += 0.9 if analysis_context else 0.0
        score += 0.8 if explanation_burden else 0.0
        score += 0.7 if has_workflow_pain else 0.0
        score += 0.5 if discussion_style and (metric_presence or discrepancy_presence or analysis_context) else 0.0
        score -= 1.0 if generic_tips else 0.0
        score -= 0.8 if usage_only else 0.0

        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if low_signal:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="klaviyo_low_signal",
                rescue_reason="",
                passes=False,
            )

    if source == "adobe_analytics_community":
        metric_presence = any(
            term in lowered
            for term in [
                "adobe analytics",
                "workspace",
                "analysis workspace",
                "freeform table",
                "debugger",
                "report suite",
                "report builder",
                "excel",
                "pdf",
                "segment",
                "calculated metric",
                "evar",
                "prop",
                "classification",
                "attribution",
                "pageurl",
                "page view",
                "expirydate",
                "date range",
                "direct traffic",
                "seo",
                "customers metric",
                "alert",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "not in workspace",
                "does not appear",
                "doesn't appear",
                "visible in debugger",
                "spike",
                "drop in seo",
                "time delay",
                "way earlier",
                "sent after",
                "updated after",
                "impact ios",
                "wrong channel",
                "slow",
                "very slow",
                "loading issues",
                "font is broken",
                "broken in a pdf",
                "broken in pdf",
                "can't see the data",
                "cannot see the data",
                "unspecified pageurl",
                "what could be the cause",
                "dropping one of the dimensions",
                "missing dimension",
                "export missing",
                "export is dropping",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "banking mobile app",
                "uat environment",
                "anomaly alert",
                "alert",
                "debugger",
                "workspace",
                "traffic",
                "ios",
                "bookmarked",
                "weekly review",
                "team members",
                "pdf format",
                "freeform table",
                "stakeholder workbook",
                "sign-off",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "is there a way",
                "is it possible",
                "currently validating",
                "we have noticed",
                "we investigated",
                "could find",
                "i want to set up",
                "wondering anyone is experiencing",
                "what could be the cause",
                "i'd like to create a report",
                "i've tried",
                "what do i need to set",
                "pull out all the segments",
                "for a particular report suite",
            ]
        )
        operational_question = any(
            term in lowered
            for term in [
                "what causes",
                "is there anything i need to set",
                "is it possible to pull out",
                "what could be the cause",
                "wondering anyone is experiencing",
                "i'd like to create a report",
                "i've tried using",
                "can i create",
            ]
        )
        adobe_noise = any(
            term in lowered
            for term in [
                "in previous posts",
                "we covered migrating",
                "we'll discuss",
                "migration tutorial",
                "complete web sdk migration tutorial",
                "release notes",
                "community member of the year",
                "certification",
                "exam",
                "summit",
            ]
        )
        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if adobe_noise and not discrepancy_presence:
            return QualityAssessment(score=0.0, bucket="fail", fail_reason="adobe_reference_or_training_content", rescue_reason="", passes=False)
        if signal_count >= 3 and (discrepancy_presence or analysis_context):
            return QualityAssessment(score=0.92, bucket="hard_pass", fail_reason="", rescue_reason="adobe_analytics_context_rescue", passes=True)
        if metric_presence and (discrepancy_presence or explanation_burden or operational_question) and not adobe_noise:
            return QualityAssessment(
                score=0.78,
                bucket="borderline",
                fail_reason="weak_problem_phrasing",
                rescue_reason="adobe_operational_question_rescue",
                passes=True,
            )
        if signal_count >= 2 and (metric_presence or analysis_context):
            return QualityAssessment(score=0.72, bucket="borderline", fail_reason="weak_problem_phrasing", rescue_reason="adobe_analytics_domain_rescue", passes=True)
        return QualityAssessment(score=0.0, bucket="fail", fail_reason="adobe_low_signal", rescue_reason="", passes=False)

    if source == "domo_community_forum":
        filter_default_issue = any(
            term in lowered
            for term in [
                "drop down filter",
                "filter card",
                "default selection",
                "latest month selected",
                "always keep",
                "clear filter",
            ]
        )
        summary_scope_issue = any(
            term in lowered
            for term in [
                "beast mode",
                "summary number",
                "whole dataset",
                "filtered rows",
                "reporting total is wrong",
            ]
        )
        metric_presence = any(
            term in lowered
            for term in [
                "domo",
                "card",
                "chart",
                "graph by",
                "hourly chart",
                "time scale",
                "filter card",
                "filter view",
                "data table",
                "dataset",
                "month end date",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "forcing",
                "hard to get",
                "default selection",
                "not allow user to clear filter",
                "always keep the latest month selected",
                "always have something selected",
                "pre-select",
                "pre selected",
                "always have something selected",
                "wrong",
                "broken",
                "not possible",
                "is it possible",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "graph by",
                "hourly chart",
                "time scale",
                "filter view",
                "drop down filter",
                "month end date",
                "data table",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "why is",
                "is it possible",
                "i want",
                "however, i want",
                "recommend",
                "how do you",
                "any idea",
            ]
        )
        ideas_exchange_noise = "ideas exchange" in lowered and not (discrepancy_presence or analysis_context)
        app_framework_noise = any(term in lowered for term in ["custom app", "app framework", "react", "vue", "ddx"]) and not discrepancy_presence
        if ideas_exchange_noise or app_framework_noise:
            return QualityAssessment(score=0.0, bucket="fail", fail_reason="domo_platform_discussion_without_workflow_pain", rescue_reason="", passes=False)
        if filter_default_issue or summary_scope_issue:
            return QualityAssessment(score=0.88, bucket="hard_pass", fail_reason="", rescue_reason="domo_filter_summary_rescue", passes=True)
        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if signal_count >= 3 and (discrepancy_presence or analysis_context):
            return QualityAssessment(score=0.9, bucket="hard_pass", fail_reason="", rescue_reason="domo_chart_filter_rescue", passes=True)
        if signal_count >= 2 and metric_presence and (discrepancy_presence or analysis_context):
            return QualityAssessment(score=0.7, bucket="borderline", fail_reason="weak_problem_phrasing", rescue_reason="domo_domain_rescue", passes=True)
        return QualityAssessment(score=0.0, bucket="fail", fail_reason="domo_low_signal", rescue_reason="", passes=False)
        if signal_count >= 3 and (discrepancy_presence or has_required_problem or explanation_burden):
            return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
        if signal_count >= 2:
            rescue_reason = "klaviyo_operational_workflow_rescue"
            fail_reason = "weak_problem_phrasing"
            if metric_presence and not (discrepancy_presence or has_required_problem):
                fail_reason = "metric_present_but_no_explicit_blocker"
                rescue_reason = "klaviyo_metric_context_rescue"
            elif not has_workflow_pain and (metric_presence or analysis_context):
                fail_reason = "weak_workflow_signal"
                rescue_reason = "klaviyo_workflow_weak_rescue"
            elif discussion_style and (metric_presence or discrepancy_presence or analysis_context):
                fail_reason = "discussion_style_but_relevant"
                rescue_reason = "klaviyo_discussion_style_rescue"
            return QualityAssessment(score=round(score, 3), bucket="borderline", fail_reason=fail_reason, rescue_reason=rescue_reason, passes=True)
        return QualityAssessment(
            score=round(score, 3),
            bucket="fail",
            fail_reason="klaviyo_problem_without_operational_context",
            rescue_reason="",
            passes=False,
        )

    if source == "power_bi_community":
        metric_presence = any(
            term in lowered
            for term in [
                "power bi",
                "dax",
                "measure",
                "measures",
                "matrix",
                "slicer",
                "visual",
                "visuals",
                "dashboard",
                "report",
                "reports",
                "table",
                "line chart",
                "legend",
                "count",
                "distinct count",
                "total",
                "totals",
                "forecast",
                "actual",
                "gateway",
                "refresh",
                "desktop",
                "service",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "wrong",
                "incorrect",
                "not matching",
                "mismatch",
                "different",
                "gone missing",
                "missing",
                "failed",
                "timed out",
                "doesn't work",
                "doesnt work",
                "disabled",
                "cannot",
                "can't",
                "cant",
                "limitation",
                "workaround",
                "not visible",
                "not showing",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "desktop",
                "service",
                "gateway",
                "refresh",
                "filter context",
                "row context",
                "drill through",
                "relationship",
                "relationships",
                "rankx",
                "top n",
                "forecast version",
                "export",
                "power query",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "why",
                "how can i",
                "how do i",
                "is there any way",
                "is there an option",
                "trying to",
                "need to",
                "requirement",
                "workaround",
                "client wants",
                "expected outcome",
            ]
        )
        discussion_style = any(
            term in lowered
            for term in [
                "hi all",
                "hi everyone",
                "looking for guidance",
                "can someone help",
                "has anyone seen",
                "any suggestion",
            ]
        )
        support_boilerplate = any(
            term in lowered
            for term in [
                "accepted solution",
                "has your issue been resolved",
                "thank you for the response provided",
                "please feel free to contact us",
                "mark this as the accepted solution",
                "community member addressed your query",
            ]
        ) and not discrepancy_presence
        low_signal = not any([metric_presence, discrepancy_presence, analysis_context, explanation_burden])

        score = 0.0
        score += 1.2 if metric_presence else 0.0
        score += 1.2 if discrepancy_presence or has_required_problem else 0.0
        score += 0.9 if analysis_context else 0.0
        score += 0.8 if explanation_burden else 0.0
        score += 0.7 if has_workflow_pain else 0.0
        score += 0.5 if discussion_style and (metric_presence or discrepancy_presence or analysis_context) else 0.0
        score -= 1.2 if support_boilerplate else 0.0
        score -= 0.8 if usage_only else 0.0

        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if support_boilerplate:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="support_reply_without_operator_context",
                rescue_reason="",
                passes=False,
            )
        if low_signal:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="power_bi_low_signal",
                rescue_reason="",
                passes=False,
            )
        if signal_count >= 3 and (discrepancy_presence or has_required_problem or explanation_burden):
            return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
        if signal_count >= 2:
            rescue_reason = "power_bi_measure_reporting_rescue"
            fail_reason = "weak_problem_phrasing"
            if metric_presence and not (discrepancy_presence or has_required_problem):
                fail_reason = "metric_present_but_no_explicit_blocker"
                rescue_reason = "power_bi_metric_context_rescue"
            elif not has_workflow_pain and (metric_presence or analysis_context):
                fail_reason = "weak_workflow_signal"
                rescue_reason = "power_bi_workflow_weak_rescue"
            elif discussion_style and (metric_presence or discrepancy_presence or analysis_context):
                fail_reason = "discussion_style_but_relevant"
                rescue_reason = "power_bi_discussion_style_rescue"
            return QualityAssessment(score=round(score, 3), bucket="borderline", fail_reason=fail_reason, rescue_reason=rescue_reason, passes=True)
        return QualityAssessment(
            score=round(score, 3),
            bucket="fail",
            fail_reason="power_bi_problem_without_reporting_context",
            rescue_reason="",
            passes=False,
        )

    if source == "qlik_community":
        metric_presence = any(
            term in lowered
            for term in [
                "qlik",
                "qlik sense",
                "qlikview",
                "nprinting",
                "set analysis",
                "pivot table",
                "straight table",
                "combo chart",
                "cross tab",
                "gauge chart",
                "filter pane",
                "expression",
                "dimension",
                "measure",
                "total",
                "totals",
                "kpi",
                "report",
                "dashboard",
                "excel",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "wrong",
                "incorrect",
                "not aggregating",
                "not matching",
                "mismatch",
                "different totals",
                "collapsed or expanded",
                "desired level",
                "error",
                "issue",
                "problem",
                "limitation",
                "unavailable",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "aggr",
                "fractile",
                "total inside",
                "pixel perfect",
                "reporting service",
                "visualization",
                "usability",
                "filter",
                "expression",
                "axis",
                "chart",
                "table",
                "export",
                "email",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "how to",
                "trying to",
                "need to",
                "stuck",
                "is there a feature",
                "would like to know",
                "the goal is",
                "i'm stuck",
                "can someone help",
            ]
        )
        discussion_style = any(
            term in lowered
            for term in [
                "hi all",
                "hi everyone",
                "thanks for your reply",
                "hello everyone",
                "any suggestion",
                "can someone help",
            ]
        )
        support_boilerplate = any(
            term in lowered
            for term in [
                "accepted solution",
                "mark as accepted solution",
                "please close the case",
                "community manager",
            ]
        ) and not discrepancy_presence
        low_signal = not any([metric_presence, discrepancy_presence, analysis_context, explanation_burden])

        score = 0.0
        score += 1.2 if metric_presence else 0.0
        score += 1.2 if discrepancy_presence or has_required_problem else 0.0
        score += 0.9 if analysis_context else 0.0
        score += 0.8 if explanation_burden else 0.0
        score += 0.7 if has_workflow_pain else 0.0
        score += 0.5 if discussion_style and (metric_presence or discrepancy_presence or analysis_context) else 0.0
        score -= 1.2 if support_boilerplate else 0.0
        score -= 0.8 if usage_only else 0.0

        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if support_boilerplate:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="support_reply_without_analysis_context",
                rescue_reason="",
                passes=False,
            )
        if low_signal:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="qlik_low_signal",
                rescue_reason="",
                passes=False,
            )
        if signal_count >= 3 and (discrepancy_presence or has_required_problem or explanation_burden):
            return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
        if signal_count >= 2:
            rescue_reason = "qlik_reporting_expression_rescue"
            fail_reason = "weak_problem_phrasing"
            if metric_presence and not (discrepancy_presence or has_required_problem):
                fail_reason = "metric_present_but_no_explicit_blocker"
                rescue_reason = "qlik_metric_context_rescue"
            elif not has_workflow_pain and (metric_presence or analysis_context):
                fail_reason = "weak_workflow_signal"
                rescue_reason = "qlik_workflow_weak_rescue"
            elif discussion_style and (metric_presence or discrepancy_presence or analysis_context):
                fail_reason = "discussion_style_but_relevant"
                rescue_reason = "qlik_discussion_style_rescue"
            return QualityAssessment(score=round(score, 3), bucket="borderline", fail_reason=fail_reason, rescue_reason=rescue_reason, passes=True)
        return QualityAssessment(
            score=round(score, 3),
            bucket="fail",
            fail_reason="qlik_problem_without_reporting_context",
            rescue_reason="",
            passes=False,
        )

    if source == "sisense_community":
        metric_presence = any(
            term in lowered
            for term in [
                "sisense",
                "dashboard",
                "dashboards",
                "widget",
                "widgets",
                "pivot table",
                "table",
                "chart",
                "column chart",
                "bar chart",
                "filter",
                "filters",
                "break by",
                "data model",
                "drill dashboard",
                "jump to dashboard",
                "scientific units",
                "javascript",
                "export",
                "exports",
                "xlsx",
                "csv",
                "pdf",
                "report",
                "reporting",
                "wrong totals",
                "source data",
                "ssrs",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "reset",
                "limits",
                "limited",
                "dynamically change",
                "hide",
                "empty legend",
                "same month or not",
                "column width",
                "calculate",
                "multiple columns",
                "not",
                "issue",
                "problem",
                "manual",
                "manually",
                "currently",
                "bulk",
                "best practice",
                "error details",
                "show",
                "wrong totals",
                "not matching",
                "mismatch",
                "does not match",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "dashboard script",
                "widget script",
                "background dashboard filter",
                "single filter",
                "left-click",
                "resolution deadline",
                "invoice data",
                "column width",
                "list selection",
                "dashboard api",
                "python dataframe",
                "schedule builds",
                "schedule all",
                "build at",
                "cube names",
                "exports like xlsx",
                "filters of the dashboard",
                "replacing ssrs",
                "live detail reporting",
                "widget value does not match table",
                "source data and sisense report don't match",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "how to",
                "trying to",
                "is it possible",
                "can i",
                "i wonder if",
                "better way",
                "need to",
                "want to",
                "currently",
                "best practice",
                "provide a list",
                "is there a way",
            ]
        )
        discussion_style = any(
            term in lowered
            for term in [
                "hey everyone",
                "hello experts",
                "hi!",
                "i wonder if",
                "can someone help",
            ]
        )
        support_boilerplate = any(
            term in lowered
            for term in [
                "accepted solution",
                "mark as accepted solution",
                "community manager",
            ]
        ) and not discrepancy_presence
        infra_noise = any(
            term in lowered
            for term in [
                "kubernetes",
                "helm",
                "upgrade",
                "installation",
                "install",
                "auth0",
                "jwt",
                "sso",
                "support ticket",
                "eks",
                "linux self-hosted",
            ]
        ) and not any(
            term in lowered
            for term in [
                "dashboard",
                "widget",
                "pivot table",
                "wrong totals",
                "export",
                "reporting",
                "replacing ssrs",
                "live detail reporting",
            ]
        )
        low_signal = not any([metric_presence, discrepancy_presence, analysis_context, explanation_burden])

        score = 0.0
        score += 1.2 if metric_presence else 0.0
        score += 1.0 if discrepancy_presence or has_required_problem else 0.0
        score += 0.9 if analysis_context else 0.0
        score += 0.8 if explanation_burden else 0.0
        score += 0.7 if has_workflow_pain else 0.0
        score += 0.5 if discussion_style and (metric_presence or discrepancy_presence or analysis_context) else 0.0
        score -= 1.2 if support_boilerplate else 0.0
        score -= 1.4 if infra_noise else 0.0
        score -= 0.8 if usage_only else 0.0

        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if support_boilerplate:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="support_reply_without_analysis_context",
                rescue_reason="",
                passes=False,
            )
        if infra_noise:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="sisense_infra_install_noise",
                rescue_reason="",
                passes=False,
            )
        if low_signal:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="sisense_low_signal",
                rescue_reason="",
                passes=False,
            )
        if signal_count >= 3 and (discrepancy_presence or has_required_problem or explanation_burden or analysis_context):
            return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
        if signal_count >= 2:
            rescue_reason = "sisense_dashboard_filter_rescue"
            fail_reason = "weak_problem_phrasing"
            if metric_presence and not (discrepancy_presence or has_required_problem):
                fail_reason = "metric_present_but_no_explicit_blocker"
                rescue_reason = "sisense_metric_context_rescue"
            elif not has_workflow_pain and (metric_presence or analysis_context):
                fail_reason = "weak_workflow_signal"
                rescue_reason = "sisense_workflow_weak_rescue"
            elif discussion_style and (metric_presence or discrepancy_presence or analysis_context):
                fail_reason = "discussion_style_but_relevant"
                rescue_reason = "sisense_discussion_style_rescue"
            return QualityAssessment(score=round(score, 3), bucket="borderline", fail_reason=fail_reason, rescue_reason=rescue_reason, passes=True)
        return QualityAssessment(
            score=round(score, 3),
            bucket="fail",
            fail_reason="sisense_problem_without_reporting_context",
            rescue_reason="",
            passes=False,
        )

    if source == "mixpanel_community":
        metric_presence = any(
            term in lowered
            for term in [
                "mixpanel",
                "event",
                "events",
                "funnel",
                "funnels",
                "retention",
                "insights",
                "breakdown",
                "distinct id",
                "identify",
                "mirror",
                "country",
                "city",
                "session duration",
                "conversion time",
                "report",
                "dashboard",
                "export",
                "csv",
                "source of truth",
                "trend",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "wrong",
                "incorrect",
                "missing",
                "not linking",
                "not showing",
                "discrepancy",
                "limitation",
                "hurdle",
                "issue",
                "problem",
                "mismatch",
                "not matching",
                "source of truth",
                "what changed",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "new project",
                "mirror sync",
                "segment",
                "customer web app",
                "marketing website",
                "tracking",
                "query",
                "ui",
                "screen",
                "which report should i use",
                "figure out",
                "explain",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "trying to",
                "facing",
                "why does",
                "why is",
                "can someone help",
                "how can i",
                "what changed",
                "which report should i use",
            ]
        )
        setup_noise = any(
            term in lowered
            for term in [
                "api",
                "sdk",
                "instrumentation",
                "webhook",
                "mobile sdk",
                "send events",
                "implementation",
            ]
        ) and not any(
            term in lowered
            for term in [
                "report",
                "dashboard",
                "export",
                "csv",
                "funnel",
                "insights",
                "retention",
                "breakdown",
                "source of truth",
                "mismatch",
            ]
        )
        low_signal = not any([metric_presence, discrepancy_presence, analysis_context, explanation_burden])

        score = 0.0
        score += 1.2 if metric_presence else 0.0
        score += 1.2 if discrepancy_presence or has_required_problem else 0.0
        score += 0.9 if analysis_context else 0.0
        score += 0.8 if explanation_burden else 0.0
        score += 0.7 if has_workflow_pain else 0.0
        score -= 1.4 if setup_noise else 0.0
        score -= 0.8 if usage_only else 0.0

        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if low_signal:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="mixpanel_low_signal",
                rescue_reason="",
                passes=False,
            )
        if setup_noise:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="mixpanel_api_setup_noise",
                rescue_reason="",
                passes=False,
            )
        if signal_count >= 3 and (discrepancy_presence or has_required_problem or explanation_burden):
            return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
        if signal_count >= 2:
            rescue_reason = "mixpanel_event_reporting_rescue"
            fail_reason = "weak_problem_phrasing"
            if metric_presence and not (discrepancy_presence or has_required_problem):
                fail_reason = "metric_present_but_no_explicit_blocker"
                rescue_reason = "mixpanel_metric_context_rescue"
            elif not has_workflow_pain and (metric_presence or analysis_context):
                fail_reason = "weak_workflow_signal"
                rescue_reason = "mixpanel_workflow_weak_rescue"
            return QualityAssessment(score=round(score, 3), bucket="borderline", fail_reason=fail_reason, rescue_reason=rescue_reason, passes=True)
        return QualityAssessment(
            score=round(score, 3),
            bucket="fail",
            fail_reason="mixpanel_problem_without_reporting_context",
            rescue_reason="",
            passes=False,
        )

    if source == "stackoverflow":
        metric_presence = any(
            term in lowered
            for term in [
                "dashboard",
                "report",
                "reporting",
                "matrix",
                "pivot",
                "tableau",
                "power bi",
                "powerbi",
                "dax",
                "power query",
                "powerquery",
                "sql server",
                "postgresql",
                "mysql",
                "reporting services",
                "ssrs",
                "excel",
                "csv",
            ]
        )
        discrepancy_presence = any(
            term in lowered
            for term in [
                "wrong total",
                "wrong totals",
                "not matching",
                "mismatch",
                "count distinct",
                "group by",
                "left join",
                "duplicate rows",
                "date filter",
                "created_at",
                "event_date",
                "source of truth",
                "rows do not add up",
            ]
        )
        analysis_context = any(
            term in lowered
            for term in [
                "reconcile",
                "reconciliation",
                "summary detail",
                "business definition",
                "calculated field",
                "export csv",
                "rows total",
                "query result",
                "dashboard total",
            ]
        )
        explanation_burden = any(
            term in lowered
            for term in [
                "trying to",
                "need to",
                "how can i",
                "why does",
                "why is",
                "figure out",
                "manual",
                "stakeholder",
                "before sending",
                "sign off",
                "validate",
            ]
        )
        setup_noise = any(
            term in lowered
            for term in [
                "react",
                "javascript",
                "css",
                "html",
                "docker",
                "oauth",
                "selenium",
                "web scraping",
                "strapi",
                "grafana",
                "prometheus",
            ]
        ) and not any(
            term in lowered
            for term in [
                "dashboard",
                "report",
                "reporting",
                "pivot",
                "power bi",
                "powerbi",
                "tableau",
                "sql server",
                "postgresql",
                "mysql",
                "wrong total",
                "mismatch",
            ]
        )
        low_signal = not any([metric_presence, discrepancy_presence, analysis_context, explanation_burden])

        score = 0.0
        score += 1.0 if metric_presence else 0.0
        score += 1.25 if discrepancy_presence or has_required_problem else 0.0
        score += 0.9 if analysis_context else 0.0
        score += 0.7 if explanation_burden else 0.0
        score += 0.65 if has_workflow_pain else 0.0
        score -= 1.3 if setup_noise else 0.0
        score -= 0.8 if usage_only else 0.0

        signal_count = sum(
            int(flag)
            for flag in [
                metric_presence,
                discrepancy_presence or has_required_problem,
                analysis_context,
                explanation_burden,
                has_workflow_pain,
            ]
        )
        if low_signal:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="stackoverflow_low_signal",
                rescue_reason="",
                passes=False,
            )
        if setup_noise:
            return QualityAssessment(
                score=round(score, 3),
                bucket="fail",
                fail_reason="stackoverflow_generic_dev_noise",
                rescue_reason="",
                passes=False,
            )
        if signal_count >= 3 and (discrepancy_presence or has_required_problem or analysis_context):
            return QualityAssessment(score=round(score, 3), bucket="hard_pass", fail_reason="", rescue_reason="", passes=True)
        if signal_count >= 2:
            rescue_reason = "stackoverflow_bi_reconciliation_rescue"
            fail_reason = "weak_problem_phrasing"
            if metric_presence and not (discrepancy_presence or has_required_problem):
                fail_reason = "metric_present_but_no_explicit_blocker"
                rescue_reason = "stackoverflow_metric_context_rescue"
            elif not has_workflow_pain and (metric_presence or analysis_context):
                fail_reason = "weak_workflow_signal"
                rescue_reason = "stackoverflow_workflow_weak_rescue"
            return QualityAssessment(score=round(score, 3), bucket="borderline", fail_reason=fail_reason, rescue_reason=rescue_reason, passes=True)
        return QualityAssessment(
            score=round(score, 3),
            bucket="fail",
            fail_reason="stackoverflow_problem_without_reporting_context",
            rescue_reason="",
            passes=False,
        )

    metric_presence = any(term in lowered for term in ["metric", "metrics", "report", "reporting", "analytics", "dashboard", "export", "csv"])
    discrepancy_presence = any(term in lowered for term in ["discrepancy", "mismatch", "not matching", "wrong", "confusion", "off by", "source of truth", "reconcile", "reconciliation"])
    business_metric_presence = any(term in lowered for term in ["sales", "revenue", "orders", "sessions", "conversion", "aov", "roas", "checkout", "shipping", "payment", "payout", "fees", "refund", "inventory", "tax"])
    analysis_context = any(term in lowered for term in ["compare", "comparison", "trend", "weekly", "monthly", "performance", "ga4", "google analytics", "google ads", "meta ads", "facebook ads", "bank deposit", "settlement", "finance", "accounting"])
    explanation_burden = any(term in lowered for term in ["figure out", "cannot explain", "explain", "why", "interpret", "understand", "what changed", "before sending", "sign off", "validate", "double check"])
    discussion_style = any(term in lowered for term in ["feedback", "curious", "anyone else", "what do you check first", "how do you handle", "looking for advice"])
    operational_context = any(term in lowered for term in ["store", "campaign", "product", "inventory", "checkout", "report", "dashboard", "shipping", "payment", "orders", "refund", "payout", "settlement", "bank"])
    pure_feature_request = any(term in lowered for term in ["feature request", "would be nice", "idea:"]) and not metric_presence and not business_metric_presence
    generic_tips = any(term in lowered for term in ["tips", "best apps", "inspiration", "advice"]) and not metric_presence and not discrepancy_presence
    reconciliation_presence = any(term in lowered for term in ["reconcile", "reconciliation", "source of truth", "bank deposit", "payout", "settlement", "finance", "accounting", "ga4", "google ads", "meta ads", "facebook ads"])
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
    score += 0.9 if reconciliation_presence else 0.0
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
            reconciliation_presence,
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
        elif metric_presence and reconciliation_presence:
            fail_reason = "metric_present_but_no_explicit_blocker"
            rescue_reason = "shopify_reconciliation_context_rescue"
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


def _is_non_boundary_segment(text: str, rules: dict[str, Any], source: str = "") -> bool:
    """Return True when a unit is mostly boilerplate, promo, or repetitive filler."""
    lowered = clean_text(text).lower()
    if not lowered:
        return True
    min_unit_len = int(rules.get("min_unit_len", 80))
    if source in {"adobe_analytics_community", "domo_community_forum"}:
        min_unit_len = max(90, min_unit_len - 30)
    for pattern in rules.get("non_boundary_patterns", []):
        if str(pattern).lower() in lowered:
            return True
    if len(lowered) < min_unit_len:
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


def _has_strong_boundary_pair(left: SegmentState, right: SegmentState, source: str = "") -> bool:
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
    if different_families >= 2:
        return True
    if source == "adobe_analytics_community" and _adobe_domain_shift(left.text, right.text):
        return True
    if source == "domo_community_forum" and _domo_domain_shift(left.text, right.text):
        return True
    return False


def _split_source_shift_units(text: str, source: str, rules: dict[str, Any]) -> list[str]:
    """Split long source-specific bodies on real shift markers when workflow domains diverge."""
    normalized = clean_text(text)
    min_unit_len = int(rules.get("min_unit_len", 80))
    source_min_unit_len = max(90, min_unit_len - 30)
    if source not in {"adobe_analytics_community", "domo_community_forum"} or len(normalized) < max(220, source_min_unit_len * 2):
        return []

    raw_parts = [
        clean_text(part)
        for part in re.split(
            r"(?i)(?=\b(?:separately|however|on the other hand|meanwhile|in addition|another issue)\b)",
            normalized,
        )
        if clean_text(part)
    ]
    if len(raw_parts) < 2:
        return []

    merged_parts: list[str] = [raw_parts[0]]
    for part in raw_parts[1:]:
        left = merged_parts[-1]
        if source == "adobe_analytics_community":
            domain_shift = _adobe_domain_shift(left, part)
        else:
            domain_shift = _domo_domain_shift(left, part)
        if domain_shift and len(left) >= source_min_unit_len and len(part) >= source_min_unit_len:
            merged_parts.append(part)
        else:
            merged_parts[-1] = combine_text(left, part)

    return merged_parts if len(merged_parts) >= 2 else []


def _adobe_domain_shift(left_text: str, right_text: str) -> bool:
    """Allow Adobe threads to split when adjacent units move across real workflow domains."""
    left_domain = _adobe_problem_domain(left_text)
    right_domain = _adobe_problem_domain(right_text)
    if not left_domain or not right_domain or left_domain == right_domain:
        return False
    meaningful_pairs = {
        ("workspace", "debugger"),
        ("workspace", "cja"),
        ("workspace", "report_builder"),
        ("workspace", "segment_definition"),
        ("workspace", "classification"),
        ("workspace", "data_feed"),
        ("debugger", "workspace"),
        ("cja", "workspace"),
        ("report_builder", "workspace"),
        ("segment_definition", "workspace"),
        ("classification", "workspace"),
        ("data_feed", "workspace"),
        ("metric_definition", "workspace"),
        ("attribution", "workspace"),
        ("workspace", "metric_definition"),
        ("workspace", "attribution"),
    }
    return (left_domain, right_domain) in meaningful_pairs


def _adobe_problem_domain(text: str) -> str:
    """Map Adobe text into a coarse workflow/problem domain for split decisions."""
    lowered = clean_text(text).lower()
    domain_map = {
        "debugger": ["debugger", "assurance"],
        "workspace": ["workspace", "analysis workspace", "freeform table"],
        "cja": ["cja", "customer journey analytics", "data view"],
        "report_builder": ["report builder", "excel add-in"],
        "classification": ["classification", "classification set", "classification rule"],
        "segment_definition": ["segment", "segment builder", "segment container"],
        "data_feed": ["data feed", "feed export", "warehouse", "sftp"],
        "metric_definition": ["calculated metric", "metric", "evar", "prop"],
        "attribution": ["attribution", "marketing channel", "channel"],
    }
    for domain, terms in domain_map.items():
        if any(term in lowered for term in terms):
            return domain
    return ""


def _domo_domain_shift(left_text: str, right_text: str) -> bool:
    """Allow Domo threads to split when adjacent units move across real workflow domains."""
    left_domain = _domo_problem_domain(left_text)
    right_domain = _domo_problem_domain(right_text)
    return bool(left_domain and right_domain and left_domain != right_domain)


def _domo_problem_domain(text: str) -> str:
    """Map Domo text into a coarse workflow/problem domain for split decisions."""
    lowered = clean_text(text).lower()
    domain_map = {
        "filtering": ["filter card", "filter view", "drop down filter", "global filter", "selected"],
        "calculation": ["beast mode", "calculated field", "formula", "lag", "period over period", "summary number"],
        "charting": ["chart", "graph", "line chart", "bar chart", "table card", "heatmap", "gantt"],
        "dataset_ops": ["dataset", "workbench", "magic etl", "recursive magic etl", "appdb", "pro-code editor", "connector", "update"],
    }
    for domain, terms in domain_map.items():
        if any(term in lowered for term in terms):
            return domain
    return ""


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
