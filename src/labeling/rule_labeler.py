"""Rule-based episode prelabeling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.episodes.schema import LABELED_EPISODE_COLUMNS, LabeledEpisodeRecord


@dataclass(slots=True)
class RuleMatch:
    """Rule match result for one label family."""

    codes: str
    hits: int
    reason: str


def _match_codes(texts: list[str], keyword_map: dict[str, list[str]], unknown_code: str) -> RuleMatch:
    """Match keyword codes against one or more text fields."""
    haystack = " ".join(text.lower() for text in texts if text).strip()
    if not haystack:
        return RuleMatch(codes=unknown_code, hits=0, reason=f"{unknown_code}:no_text")

    matched: list[str] = []
    evidence: list[str] = []
    for code, keywords in keyword_map.items():
        hits = [keyword for keyword in keywords if keyword.lower() in haystack]
        if hits:
            matched.append(code)
            evidence.append(f"{code}<{','.join(hits[:2])}>")

    if not matched:
        return RuleMatch(codes=unknown_code, hits=0, reason=f"{unknown_code}:no_match")
    return RuleMatch(codes="|".join(matched), hits=len(matched), reason=";".join(evidence))


def _resolve_fit_code(row: pd.Series, fit_map: dict[str, list[str]]) -> RuleMatch:
    """Map product fit and explicit text cues to a fit label."""
    texts = [
        str(row.get("normalized_episode", "") or ""),
        str(row.get("product_fit", "") or ""),
        str(row.get("segmentation_note", "") or ""),
    ]
    matched = _match_codes(texts, fit_map, "unknown")
    if matched.codes != "unknown":
        return matched
    product_fit = str(row.get("product_fit", "") or "")
    if product_fit == "strong_fit":
        return RuleMatch(codes="F_STRONG", hits=1, reason="product_fit<strong_fit>")
    if product_fit == "review":
        return RuleMatch(codes="F_REVIEW", hits=1, reason="product_fit<review>")
    return RuleMatch(codes="unknown", hits=0, reason="unknown:no_fit")


def prelabel_episodes(episodes_df: pd.DataFrame, codebook: dict[str, Any]) -> pd.DataFrame:
    """Apply conservative rule labels and intentionally leave ambiguity as `unknown`.

    The LLM hook is expected to review only the rows that still have unknown or
    low-confidence families after this conservative pass.
    """
    if episodes_df.empty:
        return pd.DataFrame(columns=LABELED_EPISODE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for _, row in episodes_df.iterrows():
        episode_text = str(row.get("normalized_episode", "") or "")
        business_question = str(row.get("business_question", "") or "")
        bottleneck_text = str(row.get("bottleneck_text", "") or "")
        tool_env = str(row.get("tool_env", "") or "")
        workaround_text = str(row.get("workaround_text", "") or "")
        desired_output = str(row.get("desired_output", "") or "")
        role_clue = str(row.get("role_clue", "") or "")
        work_moment = str(row.get("work_moment", "") or "")

        role_match = _match_codes([role_clue, episode_text], codebook.get("role_keywords", {}), "unknown")
        moment_match = _match_codes([work_moment, episode_text], codebook.get("moment_keywords", {}), "unknown")
        question_match = _match_codes(
            [business_question, bottleneck_text, episode_text],
            codebook.get("question_codes", {}),
            "unknown",
        )
        pain_match = _match_codes(
            [bottleneck_text, workaround_text, episode_text],
            codebook.get("pain_codes", {}),
            "unknown",
        )
        env_match = _match_codes(
            [tool_env, desired_output, episode_text],
            codebook.get("env_codes", {}),
            "unknown",
        )
        workaround_match = _match_codes(
            [workaround_text, episode_text],
            codebook.get("workaround_codes", {}),
            "unknown",
        )
        output_match = _match_codes(
            [desired_output, business_question, episode_text],
            codebook.get("output_codes", {}),
            "unknown",
        )
        fit_match = _resolve_fit_code(row, codebook.get("fit_keywords", {}))

        hit_total = sum(
            match.hits
            for match in [
                role_match,
                moment_match,
                question_match,
                pain_match,
                env_match,
                workaround_match,
                output_match,
                fit_match,
            ]
        )
        confidence = 0.45 + min(hit_total * 0.05, 0.4)
        core_codes = {
            "role_codes": role_match.codes,
            "question_codes": question_match.codes,
            "pain_codes": pain_match.codes,
            "output_codes": output_match.codes,
        }
        core_known_count = sum(1 for value in core_codes.values() if value != "unknown")
        unknown_family_count = sum(
            1
            for value in [
                role_match.codes,
                moment_match.codes,
                question_match.codes,
                pain_match.codes,
                env_match.codes,
                workaround_match.codes,
                output_match.codes,
                fit_match.codes,
            ]
            if value == "unknown"
        )
        coarse_rule_match = int(
            core_known_count <= 2
            or any(value != "unknown" and "|" not in value for value in core_codes.values())
        )
        reason = " | ".join(
            [
                f"role={role_match.reason}",
                f"moment={moment_match.reason}",
                f"question={question_match.reason}",
                f"pain={pain_match.reason}",
                f"env={env_match.reason}",
                f"workaround={workaround_match.reason}",
                f"output={output_match.reason}",
                f"fit={fit_match.reason}",
            ]
        )

        labeled = LabeledEpisodeRecord(
            episode_id=str(row["episode_id"]),
            role_codes=role_match.codes,
            moment_codes=moment_match.codes,
            question_codes=question_match.codes,
            pain_codes=pain_match.codes,
            env_codes=env_match.codes,
            workaround_codes=workaround_match.codes,
            output_codes=output_match.codes,
            fit_code=fit_match.codes,
            label_confidence=round(confidence, 2),
            label_reason=reason,
        )
        labeled_row = labeled.to_dict()
        labeled_row["rule_hit_count"] = hit_total
        labeled_row["rule_core_known_count"] = core_known_count
        labeled_row["rule_unknown_family_count"] = unknown_family_count
        labeled_row["rule_coarse_match"] = bool(coarse_rule_match)
        rows.append(labeled_row)

    extra_columns = [
        "rule_hit_count",
        "rule_core_known_count",
        "rule_unknown_family_count",
        "rule_coarse_match",
    ]
    return pd.DataFrame(rows, columns=LABELED_EPISODE_COLUMNS + extra_columns)
