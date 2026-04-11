"""Episode table schemas."""

from __future__ import annotations

from dataclasses import asdict, dataclass


EPISODE_COLUMNS = [
    "episode_id",
    "source",
    "raw_id",
    "url",
    "normalized_episode",
    "evidence_snippet",
    "role_clue",
    "work_moment",
    "business_question",
    "tool_env",
    "bottleneck_text",
    "workaround_text",
    "desired_output",
    "product_fit",
    "quality_score",
    "quality_bucket",
    "quality_fail_reason",
    "rescue_reason",
    "segmentation_note",
]

LABELED_EPISODE_COLUMNS = [
    "episode_id",
    "role_codes",
    "moment_codes",
    "question_codes",
    "pain_codes",
    "env_codes",
    "workaround_codes",
    "output_codes",
    "fit_code",
    "label_confidence",
    "label_reason",
]


@dataclass(slots=True)
class EpisodeRecord:
    """Episode-level representation derived from valid candidates."""

    episode_id: str
    source: str
    raw_id: str
    url: str
    normalized_episode: str
    evidence_snippet: str
    role_clue: str
    work_moment: str
    business_question: str
    tool_env: str
    bottleneck_text: str
    workaround_text: str
    desired_output: str
    product_fit: str
    quality_score: float
    quality_bucket: str
    quality_fail_reason: str
    rescue_reason: str
    segmentation_note: str

    def to_dict(self) -> dict[str, str]:
        """Serialize the episode record."""
        return asdict(self)


@dataclass(slots=True)
class LabeledEpisodeRecord:
    """Labeled episode schema used after rule and optional LLM labeling."""

    episode_id: str
    role_codes: str
    moment_codes: str
    question_codes: str
    pain_codes: str
    env_codes: str
    workaround_codes: str
    output_codes: str
    fit_code: str
    label_confidence: float
    label_reason: str

    def to_dict(self) -> dict[str, str | float]:
        """Serialize the labeled episode record."""
        return asdict(self)
