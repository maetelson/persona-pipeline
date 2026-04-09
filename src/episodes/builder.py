"""Episode building logic from valid normalized posts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.episodes.schema import EPISODE_COLUMNS, EpisodeRecord
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


def build_episode_table(valid_df: pd.DataFrame, rules: dict[str, Any]) -> pd.DataFrame:
    """Convert valid candidates into episode-level rows."""
    if valid_df.empty:
        return pd.DataFrame(columns=EPISODE_COLUMNS)

    rows: list[dict[str, str]] = []
    for _, row in valid_df.iterrows():
        episodes = build_post_episodes(row, rules)
        rows.extend(episode.to_dict() for episode in episodes)
    return pd.DataFrame(rows, columns=EPISODE_COLUMNS)


def build_post_episodes(row: pd.Series, rules: dict[str, Any]) -> list[EpisodeRecord]:
    """Build one or more conservative episodes from a single valid post."""
    candidate_units = _build_units_from_row(row, rules)
    min_episode_len = int(rules.get("min_episode_len", 120))
    if not candidate_units:
        candidate_units = [combine_text(row.get("title", ""), row.get("body", ""), row.get("comments_text", ""))]

    segments = [_derive_segment_state(unit) for unit in candidate_units if len(clean_text(unit)) >= min_episode_len // 2]
    segments = [segment for segment in segments if not _is_non_boundary_segment(segment.text, rules)]
    if not segments:
        fallback = _derive_segment_state(combine_text(row.get("title", ""), row.get("body", ""), row.get("comments_text", "")))
        segments = [fallback] if len(fallback.text) >= min_episode_len else []
    if not segments:
        return []

    grouped = _group_segments(segments, rules)
    if not grouped:
        grouped = [_derive_segment_state(combine_text(row.get("title", ""), row.get("body", ""), row.get("comments_text", "")))]

    episodes: list[EpisodeRecord] = []
    for index, segment in enumerate(grouped, start=1):
        normalized_episode = clean_text(segment.text)
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
                segmentation_note=_segmentation_note(segment, rules),
            )
        )
    return episodes


def _build_units_from_row(row: pd.Series, rules: dict[str, Any]) -> list[str]:
    """Build conservative segmentation units from title, body, and comment blocks."""
    units: list[str] = []
    title = clean_text(str(row.get("title", "") or ""))
    body = _normalize_bullets(str(row.get("body", "") or ""))
    comments_text = str(row.get("comments_text", "") or "")

    if title:
        units.append(title)
    units.extend(_split_body_into_units(body, rules))
    units.extend(_comment_blocks(comments_text, rules))

    cleaned_units: list[str] = []
    for unit in units:
        unit = clean_text(unit)
        if not unit:
            continue
        if cleaned_units and _text_similarity(cleaned_units[-1], unit) >= float(rules.get("similarity_merge_threshold", 0.62)):
            cleaned_units[-1] = combine_text(cleaned_units[-1], unit)
        else:
            cleaned_units.append(unit)
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
        f"q={segment.question_type};"
        f"b={segment.bottleneck_text};"
        f"tool={segment.tool_env};"
        f"collab={segment.collaborator};"
        f"out={segment.desired_output}"
    )


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
