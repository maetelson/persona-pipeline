"""Persona-axis discovery grounded in labeled episodes and episode context."""

from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from src.utils.pipeline_schema import is_unknown_like, split_pipe_codes
from src.utils.record_access import get_record_demo, get_record_id, get_record_text, get_record_value, is_valid_record


@dataclass(frozen=True)
class AxisCandidate:
    """Describe one candidate persona axis and how to compute it."""

    axis_name: str
    description: str
    service_relevance: float
    extractor: Callable[[pd.Series], list[str]]
    evidence_fields: tuple[str, ...]
    keep_bias: float = 0.0


def discover_persona_axes(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    """Discover candidate persona axes and freeze a final schema."""
    merged = _merge_inputs(episodes_df, labeled_df)
    candidates = _axis_candidates()

    candidate_rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        values = [_normalize_axis_values(candidate.extractor(row)) for _, row in merged.iterrows()]
        axis_values = [value for row_values in values for value in row_values]
        counts = Counter(axis_values)
        prevalence = round(sum(bool(row_values) for row_values in values) / max(len(merged), 1), 4)
        separability = _separability_score(counts)
        usefulness = round((prevalence * 0.4) + (separability * 0.35) + (candidate.service_relevance * 0.25) + candidate.keep_bias, 4)
        evidence = _build_evidence(candidate, counts)
        keep = _should_keep(candidate.axis_name, usefulness, prevalence, separability, candidate.service_relevance)
        candidate_rows.append(
            {
                "axis_name": candidate.axis_name,
                "description": candidate.description,
                "evidence": evidence,
                "prevalence": prevalence,
                "separability": separability,
                "usefulness": round(min(usefulness, 1.0), 4),
                "service_relevance": candidate.service_relevance,
                "keep_or_drop": "keep" if keep else "drop",
            }
        )
        if keep:
            selected_rows.append(_final_axis_schema_row(candidate, counts))

    candidate_df = pd.DataFrame(candidate_rows)
    candidate_df["_keep_rank"] = candidate_df["keep_or_drop"].map({"keep": 0, "drop": 1}).fillna(1)
    candidate_df = candidate_df.sort_values(
        ["_keep_rank", "usefulness", "service_relevance", "prevalence"],
        ascending=[True, False, False, False],
    ).drop(columns=["_keep_rank"]).reset_index(drop=True)
    selected_rows = selected_rows[:7]
    implementation_note = {
        "computed_from": {
            "labeled_source": "data/labeled/labeled_episodes.parquet",
            "episode_source": "data/episodes/episode_table.parquet",
        },
        "adapter_fields": {
            "role_axis": ["role_codes", "role_clue"],
            "workflow_axis": ["moment_codes", "work_moment"],
            "analysis_goal_axis": ["question_codes", "business_question"],
            "bottleneck_axis": ["pain_codes", "bottleneck_text"],
            "tool_dependency_axis": ["env_codes", "tool_env", "workaround_codes"],
            "trust_validation_axis": ["question_codes", "pain_codes", "moment_codes", "business_question", "desired_output"],
            "output_axis": ["output_codes", "desired_output"],
        },
        "schema_adapters_needed": [
            "episode and labeled data are stored separately and must be joined on episode_id",
            "workflow and tool context live in episode fields while reusable codes live in labeled fields",
            "unknown and unspecified tokens are normalized away before scoring axis prevalence",
        ],
    }
    return candidate_df, selected_rows, implementation_note


def build_axis_assignments(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    axis_names: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build row-level persona-axis assignments in wide and long formats."""
    merged = _merge_inputs(episodes_df, labeled_df)
    candidates = {candidate.axis_name: candidate for candidate in _axis_candidates()}
    selected_axis_names = axis_names or [candidate.axis_name for candidate in _axis_candidates()]

    wide_rows: list[dict[str, Any]] = []
    long_rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        if not is_valid_record(row, required_fields=["episode_id"]):
            continue
        record = {"episode_id": get_record_id(row)}
        for axis_name in selected_axis_names:
            candidate = candidates.get(axis_name)
            if candidate is None:
                continue
            values = _normalize_axis_values(candidate.extractor(row))
            primary_value = values[0] if values else "unassigned"
            record[axis_name] = primary_value
            for index, value in enumerate(values):
                long_rows.append(
                    {
                        "episode_id": get_record_id(row),
                        "axis_name": axis_name,
                        "axis_value": value,
                        "value_rank": index + 1,
                        "is_primary": index == 0,
                    }
                )
            if not values:
                long_rows.append(
                    {
                        "episode_id": get_record_id(row),
                        "axis_name": axis_name,
                        "axis_value": "unassigned",
                        "value_rank": 1,
                        "is_primary": True,
                    }
                )
        wide_rows.append(record)
    return pd.DataFrame(wide_rows), pd.DataFrame(long_rows)


def write_persona_axis_outputs(
    root_dir: Path,
    axis_candidates_df: pd.DataFrame,
    final_axis_schema: list[dict[str, Any]],
    implementation_note: dict[str, Any],
) -> dict[str, Path]:
    """Write persona-axis discovery outputs to analysis artifacts."""
    analysis_dir = root_dir / "data" / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "axis_candidates_csv": analysis_dir / "axis_candidates.csv",
        "axis_candidates_parquet": analysis_dir / "axis_candidates.parquet",
        "final_axis_schema_json": analysis_dir / "final_axis_schema.json",
        "persona_axis_discovery_json": analysis_dir / "persona_axis_discovery.json",
    }
    axis_candidates_df.to_csv(paths["axis_candidates_csv"], index=False)
    axis_candidates_df.to_parquet(paths["axis_candidates_parquet"], index=False)
    paths["final_axis_schema_json"].write_text(json.dumps(final_axis_schema, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["persona_axis_discovery_json"].write_text(
        json.dumps(
            {
                "axis_candidates": axis_candidates_df.to_dict(orient="records"),
                "final_axis_schema": final_axis_schema,
                "implementation_note": implementation_note,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return paths


def _merge_inputs(episodes_df: pd.DataFrame, labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Merge episode context with labeled codes using flexible field adapters."""
    if episodes_df.empty or labeled_df.empty:
        return pd.DataFrame()
    merged = episodes_df.merge(labeled_df, on="episode_id", how="inner")
    return merged.fillna("")


def _axis_candidates() -> list[AxisCandidate]:
    """Return candidate axis definitions for persona discovery."""
    return [
        AxisCandidate(
            axis_name="user_role",
            description="Who is trying to do the analysis work or explain the output.",
            service_relevance=0.95,
            extractor=lambda row: _role_values(row),
            evidence_fields=("role_codes", "role_clue"),
            keep_bias=0.08,
        ),
        AxisCandidate(
            axis_name="workflow_stage",
            description="Where the user is stuck in the analysis workflow: triage, reporting, automation, or validation.",
            service_relevance=0.98,
            extractor=lambda row: _workflow_values(row),
            evidence_fields=("moment_codes", "work_moment"),
            keep_bias=0.08,
        ),
        AxisCandidate(
            axis_name="analysis_goal",
            description="What the user is trying to achieve: speed reporting, diagnose change, validate numbers, or automate.",
            service_relevance=0.99,
            extractor=lambda row: _analysis_goal_values(row),
            evidence_fields=("question_codes", "business_question"),
            keep_bias=0.1,
        ),
        AxisCandidate(
            axis_name="bottleneck_type",
            description="The recurring friction blocking progress: manual reporting, tool limitation, data quality, or handoff.",
            service_relevance=1.0,
            extractor=lambda row: _bottleneck_values(row),
            evidence_fields=("pain_codes", "bottleneck_text"),
            keep_bias=0.1,
        ),
        AxisCandidate(
            axis_name="tool_dependency_mode",
            description="How the user depends on BI, spreadsheets, scripts, or warehouse context to get work done.",
            service_relevance=0.95,
            extractor=lambda row: _tool_dependency_values(row),
            evidence_fields=("env_codes", "tool_env", "workaround_codes"),
            keep_bias=0.06,
        ),
        AxisCandidate(
            axis_name="trust_validation_need",
            description="How strongly the user needs number validation, trust building, or explanation before using results.",
            service_relevance=1.0,
            extractor=lambda row: _trust_validation_values(row),
            evidence_fields=("question_codes", "pain_codes", "moment_codes", "business_question", "desired_output"),
            keep_bias=0.07,
        ),
        AxisCandidate(
            axis_name="output_expectation",
            description="What output the user ultimately needs: Excel-ready artifact, dashboard update, or automation output.",
            service_relevance=0.9,
            extractor=lambda row: _output_values(row),
            evidence_fields=("output_codes", "desired_output"),
            keep_bias=0.04,
        ),
        AxisCandidate(
            axis_name="analysis_maturity",
            description="How self-serve the workflow is, from manual workaround heavy to script-assisted or warehouse-backed.",
            service_relevance=0.82,
            extractor=lambda row: _maturity_values(row),
            evidence_fields=("env_codes", "workaround_codes", "pain_codes", "bottleneck_text"),
        ),
        AxisCandidate(
            axis_name="demographic_profile",
            description="Traditional demographic grouping such as family type, age band, or employment status.",
            service_relevance=0.15,
            extractor=lambda row: [],
            evidence_fields=(),
        ),
        AxisCandidate(
            axis_name="urgency_pressure",
            description="Time pressure or executive urgency inferred from reporting deadlines and escalation language.",
            service_relevance=0.55,
            extractor=lambda row: _urgency_values(row),
            evidence_fields=("moment_codes", "desired_output", "business_question"),
        ),
    ]


def _role_values(row: pd.Series) -> list[str]:
    """Extract role-style values from label codes and role clue text."""
    mapping = {
        "R_ANALYST": "analyst",
        "R_MARKETER": "marketer",
        "R_MANAGER": "manager",
    }
    codes = [mapping[code] for code in split_pipe_codes(get_record_value(row, "role_codes", "")) if code in mapping]
    if codes:
        return codes
    clue = get_record_text(row, fields=["role_clue"]).lower()
    if "marketer" in clue:
        return ["marketer"]
    if "manager" in clue or "stakeholder" in clue:
        return ["manager"]
    if "analyst" in clue:
        return ["analyst"]
    return []


def _workflow_values(row: pd.Series) -> list[str]:
    """Extract workflow-stage values from moments and fallback text."""
    mapping = {
        "M_TRIAGE": "triage",
        "M_REPORTING": "reporting",
        "M_AUTOMATION": "automation",
        "M_VALIDATION": "validation",
    }
    values = [mapping[code] for code in split_pipe_codes(get_record_value(row, "moment_codes", "")) if code in mapping]
    if values:
        return values
    text = _text(row, "work_moment")
    fallback_map = {
        "triage": "triage",
        "report": "reporting",
        "automation": "automation",
        "validation": "validation",
    }
    return [value for token, value in fallback_map.items() if token in text][:1]


def _analysis_goal_values(row: pd.Series) -> list[str]:
    """Extract analysis-goal values from question codes and text."""
    mapping = {
        "Q_REPORT_SPEED": "report_speed",
        "Q_DIAGNOSE_ISSUE": "diagnose_change",
        "Q_VALIDATE_NUMBERS": "validate_numbers",
        "Q_AUTOMATE_WORKFLOW": "automate_workflow",
    }
    values = [mapping[code] for code in split_pipe_codes(get_record_value(row, "question_codes", "")) if code in mapping]
    if values:
        return values
    text = _text(row, "business_question")
    fallback_map = {
        "validate": "validate_numbers",
        "reconcile": "validate_numbers",
        "diagnose": "diagnose_change",
        "why": "diagnose_change",
        "faster": "report_speed",
        "report": "report_speed",
        "automate": "automate_workflow",
    }
    return [value for token, value in fallback_map.items() if token in text][:1]


def _bottleneck_values(row: pd.Series) -> list[str]:
    """Extract bottleneck type values from pain codes and bottleneck text."""
    mapping = {
        "P_MANUAL_REPORTING": "manual_reporting",
        "P_TOOL_LIMITATION": "tool_limitation",
        "P_DATA_QUALITY": "data_quality",
        "P_HANDOFF": "handoff_dependency",
    }
    values = [mapping[code] for code in split_pipe_codes(get_record_value(row, "pain_codes", "")) if code in mapping]
    if values:
        return values
    text = _text(row, "bottleneck_text")
    fallback_map = {
        "manual": "manual_reporting",
        "tool": "tool_limitation",
        "quality": "data_quality",
        "handoff": "handoff_dependency",
        "friction": "general_friction",
    }
    return [value for token, value in fallback_map.items() if token in text][:1]


def _tool_dependency_values(row: pd.Series) -> list[str]:
    """Classify the user's execution mode across BI, spreadsheet, script, and warehouse dependencies."""
    env_codes = set(split_pipe_codes(get_record_value(row, "env_codes", "")))
    workaround_codes = set(split_pipe_codes(get_record_value(row, "workaround_codes", "")))
    values: list[str] = []
    if "E_SPREADSHEET" in env_codes or "W_SPREADSHEET" in workaround_codes or "excel" in _text(row, "tool_env"):
        values.append("spreadsheet_heavy")
    if "E_SQL_BI" in env_codes:
        values.append("bi_dashboard_heavy")
    if "E_PYTHON" in env_codes or "W_SCRIPT" in workaround_codes:
        values.append("script_assisted")
    if "E_WAREHOUSE" in env_codes:
        values.append("warehouse_backed")
    return values[:2]


def _trust_validation_values(row: pd.Series) -> list[str]:
    """Classify how much trust, explanation, and validation pressure is present."""
    score = 0
    question_codes = set(split_pipe_codes(get_record_value(row, "question_codes", "")))
    pain_codes = set(split_pipe_codes(get_record_value(row, "pain_codes", "")))
    moment_codes = set(split_pipe_codes(get_record_value(row, "moment_codes", "")))
    text = " ".join(
        [
            _text(row, "business_question"),
            _text(row, "bottleneck_text"),
            _text(row, "desired_output"),
        ]
    )
    if "Q_VALIDATE_NUMBERS" in question_codes:
        score += 2
    if "P_DATA_QUALITY" in pain_codes:
        score += 2
    if "M_VALIDATION" in moment_codes:
        score += 1
    if any(token in text for token in ["validate", "reconcile", "trust", "explain", "logic"]):
        score += 1
    if score >= 4:
        return ["high"]
    if score >= 2:
        return ["medium"]
    if score >= 1:
        return ["low"]
    return []


def _output_values(row: pd.Series) -> list[str]:
    """Extract output expectation values from output codes and output text."""
    mapping = {
        "O_XLSX": "excel_ready_output",
        "O_DASHBOARD": "dashboard_update",
        "O_AUTOMATION_JOB": "automation_output",
    }
    values = [mapping[code] for code in split_pipe_codes(get_record_value(row, "output_codes", "")) if code in mapping]
    if values:
        return values
    text = _text(row, "desired_output")
    fallback_map = {
        "xlsx": "excel_ready_output",
        "dashboard": "dashboard_update",
        "automation": "automation_output",
    }
    return [value for token, value in fallback_map.items() if token in text][:1]


def _maturity_values(row: pd.Series) -> list[str]:
    """Estimate analysis maturity / self-serve capability from tools and workarounds."""
    env_codes = set(split_pipe_codes(get_record_value(row, "env_codes", "")))
    workaround_codes = set(split_pipe_codes(get_record_value(row, "workaround_codes", "")))
    pain_codes = set(split_pipe_codes(get_record_value(row, "pain_codes", "")))
    if "W_MANUAL" in workaround_codes or "P_MANUAL_REPORTING" in pain_codes:
        return ["manual_workaround_heavy"]
    if "W_SCRIPT" in workaround_codes or "E_PYTHON" in env_codes:
        return ["script_assisted_self_serve"]
    if "E_WAREHOUSE" in env_codes or "E_SQL_BI" in env_codes:
        return ["warehouse_backed_self_serve"]
    if "E_SPREADSHEET" in env_codes:
        return ["spreadsheet_led_self_serve"]
    return []


def _urgency_values(row: pd.Series) -> list[str]:
    """Infer urgency pressure from moment and output expectations."""
    text = " ".join([_text(row, "business_question"), _text(row, "desired_output"), _text(row, "work_moment")])
    if any(token in text for token in ["leadership", "exec", "deadline", "report", "weekly", "monthly"]):
        return ["reporting_pressure"]
    return []


def _normalize_axis_values(values: list[str]) -> list[str]:
    """Normalize axis values and remove unknown markers."""
    seen: list[str] = []
    for value in values:
        text = str(value or "").strip().lower()
        if not text or is_unknown_like(text) or text in seen:
            continue
        seen.append(text)
    return seen


def _text(row: pd.Series, column: str) -> str:
    """Return normalized lowercased text for one field."""
    return str(get_record_value(row, column, "") or "").strip().lower()


def _separability_score(counts: Counter[str]) -> float:
    """Estimate whether an axis meaningfully separates the corpus."""
    total = sum(counts.values())
    if total == 0 or len(counts) <= 1:
        return 0.0
    probabilities = [count / total for count in counts.values()]
    entropy = -sum(prob * math.log(prob) for prob in probabilities if prob > 0)
    normalized_entropy = entropy / math.log(len(probabilities)) if len(probabilities) > 1 else 0.0
    dominant_share = max(probabilities)
    return round((normalized_entropy * 0.6) + ((1 - dominant_share) * 0.4), 4)


def _build_evidence(candidate: AxisCandidate, counts: Counter[str]) -> str:
    """Summarize why this axis is supported by the corpus."""
    if not counts:
        return "No repeated grounded evidence found in the current labeled corpus."
    top_values = ", ".join(f"{value} ({count})" for value, count in counts.most_common(4))
    field_note = ", ".join(candidate.evidence_fields) if candidate.evidence_fields else "no supporting fields"
    return f"Top values: {top_values}. Derived from: {field_note}."


def _should_keep(axis_name: str, usefulness: float, prevalence: float, separability: float, service_relevance: float) -> bool:
    """Apply a conservative keep/drop decision for final persona axes."""
    forced_drop = {"analysis_maturity", "demographic_profile", "urgency_pressure"}
    if axis_name in forced_drop:
        return False
    forced_keep = {
        "user_role",
        "workflow_stage",
        "analysis_goal",
        "bottleneck_type",
        "tool_dependency_mode",
        "trust_validation_need",
        "output_expectation",
    }
    if axis_name in forced_keep:
        return True
    return usefulness >= 0.62 and prevalence >= 0.18 and separability >= 0.18 and service_relevance >= 0.65


def _final_axis_schema_row(candidate: AxisCandidate, counts: Counter[str]) -> dict[str, Any]:
    """Freeze one final axis schema row for downstream persona work."""
    logic = [value for value, _ in counts.most_common(6)]
    why_lookup = {
        "user_role": "Different roles surface different interpretation burdens and self-service expectations.",
        "workflow_stage": "Triage, reporting, automation, and validation require different product support and messaging.",
        "analysis_goal": "The same user can want speed, diagnosis, validation, or automation, which changes the persona shape.",
        "bottleneck_type": "Manual reporting, tool gaps, trust issues, and handoff friction imply different product opportunities.",
        "tool_dependency_mode": "Spreadsheet-heavy, BI-heavy, script-assisted, and warehouse-backed workflows behave differently in-product.",
        "trust_validation_need": "Trust and explanation pressure changes whether the product must validate, explain, or just deliver faster.",
        "output_expectation": "The final artifact changes adoption pressure: report-ready output is different from dashboard maintenance or automation.",
        "analysis_maturity": "Self-service capability can matter, but it overlaps with tool dependence and workflow stage.",
        "demographic_profile": "Demographic grouping is not grounded in this corpus and does not fit the service.",
        "urgency_pressure": "Time pressure exists, but it is weaker and less separable than workflow and bottleneck axes.",
    }
    return {
        "axis_name": candidate.axis_name,
        "why_it_matters": why_lookup.get(candidate.axis_name, candidate.description),
        "allowed_values_or_logic": logic,
        "evidence_fields_used": list(candidate.evidence_fields),
    }
