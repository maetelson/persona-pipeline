"""Bounded Reddit RevOps / analytics pilot utilities."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import time
from typing import Any
from urllib.parse import urlencode

import pandas as pd

from src.collectors.reddit_public_parser import parse_reddit_comment_payload, parse_reddit_listing_payload, reddit_timestamp_to_iso
from src.utils.http_fetch import fetch_text
from src.utils.io import ensure_dir, load_yaml, write_jsonl

SOURCE_NAME = "reddit_revops_analytics_bundle"
USER_AGENT = "persona-research-bot/0.1 (+bounded reddit pilot; public json only; local audit)"
DEFAULT_SEED_PATH = Path("config") / "seeds" / "reddit" / "reddit_revops_analytics_pilot.yaml"
DEFAULT_SPEC_PATH = Path("artifacts") / "readiness" / "reddit_revops_analytics_pilot_spec.json"

POSITIVE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "reporting_pain_signal": (
        "report",
        "reporting",
        "spreadsheet",
        "excel",
        "manual",
        "recurring",
        "weekly report",
        "monthly report",
    ),
    "dashboard_or_metric_signal": (
        "dashboard",
        "metric",
        "metrics",
        "kpi",
        "scorecard",
        "numbers",
    ),
    "stakeholder_reporting_signal": (
        "stakeholder",
        "leadership",
        "exec",
        "client",
        "board",
        "sales team",
        "manager asked",
        "boss asked",
    ),
    "attribution_or_funnel_signal": (
        "attribution",
        "funnel",
        "conversion",
        "pipeline",
        "lead source",
        "touchpoint",
        "utm",
    ),
    "CRM_or_salesops_reporting_signal": (
        "crm",
        "salesforce",
        "hubspot",
        "revops",
        "salesops",
        "forecast",
        "quota",
        "opportunity",
    ),
    "manual_spreadsheet_work_signal": (
        "spreadsheet",
        "excel",
        "google sheets",
        "csv",
        "manual export",
        "copy paste",
        "copy-paste",
    ),
    "validation_or_reconciliation_signal": (
        "mismatch",
        "reconcile",
        "reconciliation",
        "which number",
        "don't match",
        "doesnt match",
        "not matching",
        "source of truth",
        "trust the dashboard",
        "wrong number",
    ),
    "recurring_report_delivery_signal": (
        "weekly report",
        "monthly report",
        "qbr",
        "business review",
        "recurring report",
        "report delivery",
        "deadline",
        "deck",
    ),
}

NOISE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "career_training_noise": (
        "career",
        "certification",
        "certificate",
        "course",
        "bootcamp",
        "learning path",
    ),
    "job_salary_resume_noise": (
        "salary",
        "resume",
        "interview",
        "job",
        "jobs",
        "hiring",
        "internship",
    ),
    "generic_chatter_noise": (
        "anyone else",
        "what do you all think",
        "meme",
        "rant",
        "vent",
        "just curious",
    ),
    "pure_coding_debug_noise": (
        "python script",
        "sql syntax",
        "stack trace",
        "debug",
        "exception",
        "api client",
        "javascript",
        "node",
    ),
    "self_promotion_noise": (
        "i built",
        "my startup",
        "my product",
        "newsletter",
        "dm me",
        "book a demo",
    ),
    "vendor_marketing_noise": (
        "webinar",
        "announcement",
        "launch",
        "new feature",
        "product update",
    ),
    "homework_noise": (
        "homework",
        "assignment",
        "class project",
        "student project",
    ),
    "tool_recommendation_noise": (
        "best tool",
        "which tool",
        "tool recommendation",
        "what tool should",
        "best software",
    ),
}


@dataclass(slots=True)
class RedditPilotRow:
    """One scored Reddit pilot row."""

    source: str
    subreddit: str
    raw_id: str
    url: str
    title: str
    body_or_excerpt: str
    comment_excerpt: str
    created_at: str
    score: int | None
    num_comments: int | None
    permalink: str
    fetch_method: str
    fetched_at: str
    reporting_pain_signal: int
    dashboard_or_metric_signal: int
    stakeholder_reporting_signal: int
    attribution_or_funnel_signal: int
    CRM_or_salesops_reporting_signal: int
    manual_spreadsheet_work_signal: int
    validation_or_reconciliation_signal: int
    recurring_report_delivery_signal: int
    career_training_noise: int
    job_salary_resume_noise: int
    generic_chatter_noise: int
    pure_coding_debug_noise: int
    self_promotion_noise: int
    vendor_marketing_noise: int
    homework_noise: int
    tool_recommendation_noise: int
    persona_01_fit: str
    persona_02_fit: str
    persona_03_fit: str
    persona_04_fit: str
    persona_05_fit: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize the pilot row."""
        return asdict(self)


def load_pilot_seed_config(root_dir: Path) -> dict[str, Any]:
    """Load the pilot seed configuration."""
    return load_yaml(root_dir / DEFAULT_SEED_PATH)


def load_pilot_spec(root_dir: Path) -> dict[str, Any]:
    """Load the pilot specification JSON when present."""
    path = root_dir / DEFAULT_SPEC_PATH
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def is_pilot_output_path(path: Path, root_dir: Path) -> bool:
    """Return whether a path stays inside the pilot artifact directory."""
    try:
        path.resolve().relative_to((root_dir / "artifacts" / "source_pilots").resolve())
        return True
    except ValueError:
        return False


def keyword_signal(text: str, keywords: tuple[str, ...]) -> int:
    """Return 1 when any keyword appears."""
    lowered = text.lower()
    return int(any(keyword in lowered for keyword in keywords))


def score_signals(title: str, body: str, comment_excerpt: str, subreddit: str) -> dict[str, int]:
    """Score positive and noise signals for one pilot row."""
    text = " ".join([title, body, comment_excerpt, subreddit]).lower()
    scores: dict[str, int] = {}
    for name, keywords in POSITIVE_KEYWORDS.items():
        scores[name] = keyword_signal(text, keywords)
    for name, keywords in NOISE_KEYWORDS.items():
        scores[name] = keyword_signal(text, keywords)
    return scores


def _fit_label(score: int) -> str:
    """Map an integer fit score to a label."""
    if score >= 2:
        return "strong"
    if score == 1:
        return "medium"
    return "weak"


def score_persona_fit(signals: dict[str, int], title: str, body: str, comment_excerpt: str) -> dict[str, str]:
    """Estimate coarse persona fit from pilot-only heuristics."""
    text = " ".join([title, body, comment_excerpt]).lower()
    persona_01_score = min(
        2,
        signals["reporting_pain_signal"]
        + signals["manual_spreadsheet_work_signal"]
        + signals["recurring_report_delivery_signal"],
    )
    persona_02_score = min(
        2,
        signals["stakeholder_reporting_signal"]
        + signals["attribution_or_funnel_signal"]
        + int("why" in text or "explain" in text or "root cause" in text or "what changed" in text),
    )
    persona_03_score = min(
        2,
        signals["dashboard_or_metric_signal"]
        + int("workaround" in text or "limitation" in text or "can't" in text or "cannot" in text or "broken" in text),
    )
    persona_04_score = min(
        2,
        signals["validation_or_reconciliation_signal"]
        + signals["CRM_or_salesops_reporting_signal"]
        + int("trust" in text or "reconcile" in text or "mismatch" in text),
    )
    persona_05_score = min(
        2,
        signals["stakeholder_reporting_signal"]
        + signals["dashboard_or_metric_signal"]
        + int("layout" in text or "filter" in text or "deck" in text or "presentation" in text),
    )
    return {
        "persona_01_fit": _fit_label(persona_01_score),
        "persona_02_fit": _fit_label(persona_02_score),
        "persona_03_fit": _fit_label(persona_03_score),
        "persona_04_fit": _fit_label(persona_04_score),
        "persona_05_fit": _fit_label(persona_05_score),
    }


def fit_is_meaningful(value: str) -> bool:
    """Return whether a fit label counts as meaningful."""
    return value in {"medium", "strong"}


def estimate_valid_candidate(row: RedditPilotRow) -> bool:
    """Estimate whether a row looks relevant for later stages."""
    positive = (
        row.reporting_pain_signal
        + row.dashboard_or_metric_signal
        + row.stakeholder_reporting_signal
        + row.validation_or_reconciliation_signal
        + row.CRM_or_salesops_reporting_signal
    )
    noise = (
        row.career_training_noise
        + row.job_salary_resume_noise
        + row.generic_chatter_noise
        + row.pure_coding_debug_noise
        + row.self_promotion_noise
        + row.vendor_marketing_noise
        + row.homework_noise
    )
    return positive >= 2 and noise <= 1


def estimate_persona_core_candidate(row: RedditPilotRow) -> bool:
    """Estimate whether a row looks persona-core rather than merely relevant."""
    if not estimate_valid_candidate(row):
        return False
    meaningful_fits = sum(
        int(fit_is_meaningful(value))
        for value in [
            row.persona_01_fit,
            row.persona_02_fit,
            row.persona_03_fit,
            row.persona_04_fit,
        ]
    )
    return meaningful_fits >= 1 and (
        row.reporting_pain_signal
        + row.validation_or_reconciliation_signal
        + row.recurring_report_delivery_signal
        + row.CRM_or_salesops_reporting_signal
    ) >= 2


def estimate_labelable(row: RedditPilotRow) -> bool:
    """Estimate whether a row is likely labelable."""
    if not estimate_valid_candidate(row):
        return False
    return bool(row.title.strip() and (row.body_or_excerpt.strip() or row.comment_excerpt.strip()))


def _normalize_text(value: str, limit: int = 1800) -> str:
    """Collapse and truncate text for pilot artifacts."""
    collapsed = " ".join(str(value or "").split()).strip()
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 1].rstrip() + "…"


def _listing_url(subreddit: str, limit: int, after: str | None = None) -> str:
    """Build a public Reddit listing URL."""
    params: dict[str, Any] = {"limit": limit, "raw_json": 1}
    if after:
        params["after"] = after
    return f"https://www.reddit.com/r/{subreddit}/new.json?{urlencode(params)}"


def _comments_url(raw_id: str, limit: int) -> str:
    """Build a public Reddit comments URL."""
    params = {"limit": limit, "depth": 1, "raw_json": 1}
    return f"https://www.reddit.com/comments/{raw_id}.json?{urlencode(params)}"


def _created_after_cutoff(post: dict[str, Any], cutoff: datetime) -> bool:
    """Return whether a post falls inside the pilot window."""
    created = post.get("created_utc")
    if created in (None, ""):
        return False
    created_dt = datetime.fromtimestamp(float(created), tz=UTC)
    return created_dt >= cutoff


def _body_excerpt(post: dict[str, Any]) -> str:
    """Return the best available post body excerpt."""
    return _normalize_text(str(post.get("selftext", "") or ""))


def _build_comment_excerpt(
    comments: list[dict[str, Any]],
    subreddit: str,
    limit: int,
) -> tuple[str, int]:
    """Return a bounded comment excerpt and the number of fetched comments examined."""
    chosen: list[str] = []
    examined = 0
    for comment in comments:
        body = _normalize_text(str(comment.get("body", "") or ""), limit=600)
        if not body:
            continue
        examined += 1
        signals = score_signals("", body, "", subreddit)
        positive = (
            signals["reporting_pain_signal"]
            + signals["dashboard_or_metric_signal"]
            + signals["stakeholder_reporting_signal"]
            + signals["validation_or_reconciliation_signal"]
            + signals["CRM_or_salesops_reporting_signal"]
        )
        noise = (
            signals["career_training_noise"]
            + signals["job_salary_resume_noise"]
            + signals["generic_chatter_noise"]
            + signals["pure_coding_debug_noise"]
            + signals["self_promotion_noise"]
            + signals["vendor_marketing_noise"]
            + signals["homework_noise"]
        )
        if positive >= 2 and noise == 0:
            chosen.append(body)
        if len(chosen) >= limit:
            break
    return " || ".join(chosen), examined


def build_pilot_row(post: dict[str, Any], subreddit: str, comment_excerpt: str, fetched_at: str, fetch_method: str) -> RedditPilotRow:
    """Build a scored pilot row from one Reddit post."""
    permalink = str(post.get("permalink", "") or "")
    canonical_url = f"https://www.reddit.com{permalink}" if permalink else str(post.get("url", "") or "")
    title = _normalize_text(str(post.get("title", "") or ""), limit=400)
    body = _body_excerpt(post)
    signals = score_signals(title, body, comment_excerpt, subreddit)
    fits = score_persona_fit(signals, title, body, comment_excerpt)
    return RedditPilotRow(
        source=SOURCE_NAME,
        subreddit=subreddit,
        raw_id=str(post.get("id", "") or ""),
        url=canonical_url,
        title=title,
        body_or_excerpt=body,
        comment_excerpt=_normalize_text(comment_excerpt, limit=1200),
        created_at=reddit_timestamp_to_iso(post.get("created_utc")),
        score=int(post.get("score", 0) or 0),
        num_comments=int(post.get("num_comments", 0) or 0),
        permalink=permalink,
        fetch_method=fetch_method,
        fetched_at=fetched_at,
        **signals,
        **fits,
    )


def build_summary(
    rows: list[RedditPilotRow],
    total_fetched_posts: int,
    total_fetched_comments: int,
    subreddit_post_counts: dict[str, int],
    exhausted_subreddits: list[str],
    request_error_count: int,
    fetched_posts_target: int,
) -> dict[str, Any]:
    """Build pilot summary metrics and onboarding recommendation."""
    usable_rows = sum(1 for row in rows if bool(row.title.strip() or row.body_or_excerpt.strip()))
    valid_candidate_count = sum(1 for row in rows if estimate_valid_candidate(row))
    persona_core_candidate_count = sum(1 for row in rows if estimate_persona_core_candidate(row))
    labelable_count = sum(1 for row in rows if estimate_labelable(row))
    safe_usable = max(usable_rows, 1)
    valid_ratio = valid_candidate_count / safe_usable
    labelable_ratio = labelable_count / safe_usable
    career_training_noise_count = sum(1 for row in rows if row.career_training_noise or row.job_salary_resume_noise)
    generic_chatter_count = sum(1 for row in rows if row.generic_chatter_noise)
    tool_recommendation_noise_count = sum(1 for row in rows if row.tool_recommendation_noise)
    persona_fit_counts = {
        "persona_01_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_01_fit)),
        "persona_02_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_02_fit)),
        "persona_03_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_03_fit)),
        "persona_04_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_04_fit)),
        "persona_05_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_05_fit)),
    }
    noise_dominant = (career_training_noise_count + generic_chatter_count + tool_recommendation_noise_count) > max(usable_rows * 0.5, 1)

    if valid_ratio >= 0.55 and labelable_ratio >= 0.60 and not noise_dominant:
        expected_source_tier = "core_representative_source_candidate"
    elif valid_ratio >= 0.35:
        expected_source_tier = "strong_supporting_validation_source_candidate"
    else:
        expected_source_tier = "low_value_supporting_source_candidate"

    if noise_dominant or valid_ratio < 0.25:
        weak_source_risk = "high"
    elif valid_ratio >= 0.45 and labelable_ratio >= 0.55:
        weak_source_risk = "low"
    else:
        weak_source_risk = "medium"

    smaller_count_justified = total_fetched_posts < fetched_posts_target and len(exhausted_subreddits) >= 2 and valid_ratio >= 0.45
    gate_checks = {
        "enough_raw_posts_or_justified_smaller_count": total_fetched_posts >= fetched_posts_target or smaller_count_justified,
        "valid_candidate_count_threshold": valid_candidate_count >= 300 or (usable_rows >= 200 and valid_ratio >= 0.45),
        "persona_core_candidate_threshold": persona_core_candidate_count >= 150 or (usable_rows >= 200 and persona_core_candidate_count >= 100),
        "estimated_labelable_ratio_ge_60": labelable_ratio >= 0.60,
        "two_personas_show_meaningful_signal": sum(
            int(persona_fit_counts[name] >= 25)
            for name in [
                "persona_01_fit_count",
                "persona_02_fit_count",
                "persona_03_fit_count",
                "persona_04_fit_count",
            ]
        ) >= 2,
        "career_training_noise_not_dominant": career_training_noise_count <= max(usable_rows * 0.35, 1),
        "generic_chatter_not_dominant": generic_chatter_count <= max(usable_rows * 0.35, 1),
        "bundle_not_weak_source_cost_center_like": weak_source_risk != "high",
    }
    gate_pass = all(gate_checks.values())

    if gate_pass and total_fetched_posts >= fetched_posts_target and expected_source_tier == "core_representative_source_candidate":
        decision = "promote_to_active_source_bundle"
    elif gate_pass:
        decision = "expand_pilot_sample"
    elif weak_source_risk == "high" and persona_fit_counts["persona_01_fit_count"] < 20 and persona_fit_counts["persona_04_fit_count"] < 20:
        decision = "fallback_to_github_discussions_expansion"
    elif career_training_noise_count > max(usable_rows * 0.25, 1) or generic_chatter_count > max(usable_rows * 0.25, 1):
        decision = "narrow_subreddit_scope"
    else:
        decision = "reject_reddit_bundle"

    summary: dict[str, Any] = {
        "source": SOURCE_NAME,
        "total_fetched_posts": total_fetched_posts,
        "total_fetched_comments": total_fetched_comments,
        "usable_rows": usable_rows,
        "valid_candidate_count": valid_candidate_count,
        "valid_candidate_ratio": round(valid_ratio, 4),
        "estimated_persona_core_candidate_count": persona_core_candidate_count,
        "estimated_labelable_ratio": round(labelable_ratio, 4),
        "career_training_noise_count": career_training_noise_count,
        "generic_chatter_count": generic_chatter_count,
        "tool_recommendation_noise_count": tool_recommendation_noise_count,
        **persona_fit_counts,
        "expected_source_tier": expected_source_tier,
        "estimated_weak_source_risk": weak_source_risk,
        "request_error_count": request_error_count,
        "subreddit_post_counts": subreddit_post_counts,
        "exhausted_subreddits": exhausted_subreddits,
        "onboarding_gate_checks": gate_checks,
        "onboarding_gate_result": "pass" if gate_pass else "fail",
        "decision": decision,
    }
    if decision == "promote_to_active_source_bundle":
        summary["activation_plan"] = {
            "source_config_changes_needed": [
                "add pilot-proven subreddit seeds to the existing reddit collector seed bank",
                "keep production source id as reddit rather than adding a new collector family",
            ],
            "seed_changes_needed": [
                "promote surviving subreddits from the pilot bundle into an approved production seed set",
                "retain explicit career and training negative terms",
            ],
            "expected_row_counts": "target at least 1000 raw posts per bounded collection run across the approved subreddit subset",
            "production_acceptance_criteria": [
                "follow-up pilot still clears labelable and persona-core thresholds",
                "bundle remains outside weak-source cost-center behavior",
                "persona_01 and persona_04 contribution remains meaningful",
            ],
            "rollback_criteria": [
                "career or generic chatter rises above pilot thresholds",
                "subreddit access stability regresses",
                "follow-up run fails relevance quality checks",
            ],
            "tests_needed": [
                "seed load regression",
                "noise scoring regression",
                "persona fit scoring regression",
                "production config immutability regression",
            ],
        }
    return summary


def _fetch_listing_page(subreddit: str, limit: int, after: str | None, timeout_seconds: int) -> tuple[dict[str, Any] | None, str]:
    """Fetch one listing page and return the parsed payload with crawl status."""
    url = _listing_url(subreddit, limit=limit, after=after)
    response = fetch_text(url, user_agent=USER_AGENT, timeout_seconds=timeout_seconds)
    if not response.ok:
        return None, f"{response.crawl_status}:{response.status_code or response.error_message}"
    try:
        return json.loads(response.body_text), "ok"
    except json.JSONDecodeError:
        return None, "json_decode_error"


def _fetch_comments(raw_id: str, limit: int, timeout_seconds: int) -> tuple[list[dict[str, Any]], str]:
    """Fetch top-level comments for one post."""
    url = _comments_url(raw_id, limit=limit)
    response = fetch_text(url, user_agent=USER_AGENT, timeout_seconds=timeout_seconds)
    if not response.ok:
        return [], f"{response.crawl_status}:{response.status_code or response.error_message}"
    try:
        payload = json.loads(response.body_text)
    except json.JSONDecodeError:
        return [], "json_decode_error"
    return parse_reddit_comment_payload(payload), "ok"


def run_reddit_revops_analytics_pilot(root_dir: Path) -> dict[str, Path]:
    """Run the bounded Reddit RevOps / analytics pilot."""
    config = load_pilot_seed_config(root_dir)
    _ = load_pilot_spec(root_dir)
    artifact_dir = ensure_dir(root_dir / str(config["pilot_output_dir"]))
    raw_path = artifact_dir / str(config["pilot_output_files"]["raw_jsonl"])
    sample_path = artifact_dir / str(config["pilot_output_files"]["sample_csv"])
    summary_path = artifact_dir / str(config["pilot_output_files"]["summary_json"])
    for path in [raw_path, sample_path, summary_path]:
        if not is_pilot_output_path(path, root_dir):
            raise ValueError(f"Pilot output path must stay under artifacts/source_pilots: {path}")

    now = datetime.now(UTC)
    preferred_window = str(config.get("preferred_window", "trailing_12_months"))
    window_days = int(config.get("window_days", {}).get(preferred_window, 365))
    cutoff = now - timedelta(days=window_days)
    fetched_at = now.isoformat()

    total_fetched_posts = 0
    total_fetched_comments = 0
    comment_fetch_count = 0
    request_error_count = 0
    seen_ids: set[str] = set()
    rows: list[RedditPilotRow] = []
    subreddit_post_counts: dict[str, int] = {}
    exhausted_subreddits: list[str] = []

    for subreddit in config.get("target_subreddits", []):
        subreddit_name = str(subreddit).strip()
        if not subreddit_name:
            continue
        collected_for_subreddit = 0
        after: str | None = None
        older_posts_seen = False
        pages_fetched = 0
        while pages_fetched < int(config.get("max_pages_per_subreddit", 12)):
            payload, status = _fetch_listing_page(
                subreddit=subreddit_name,
                limit=int(config.get("listing_limit_per_page", 100)),
                after=after,
                timeout_seconds=int(config.get("timeout_seconds", 20)),
            )
            pages_fetched += 1
            if status != "ok" or payload is None:
                request_error_count += 1
                break
            posts = parse_reddit_listing_payload(payload)
            if not posts:
                break
            for post in posts:
                raw_id = str(post.get("id", "") or "")
                if not raw_id or raw_id in seen_ids:
                    continue
                if not _created_after_cutoff(post, cutoff):
                    older_posts_seen = True
                    continue
                seen_ids.add(raw_id)
                total_fetched_posts += 1
                collected_for_subreddit += 1
                base_title = _normalize_text(str(post.get("title", "") or ""), limit=400)
                base_body = _body_excerpt(post)
                pre_signals = score_signals(base_title, base_body, "", subreddit_name)
                comment_excerpt = ""
                if (
                    int(post.get("num_comments", 0) or 0) > 0
                    and comment_fetch_count < int(config.get("max_comment_post_fetches", 200))
                    and (
                        pre_signals["reporting_pain_signal"]
                        + pre_signals["dashboard_or_metric_signal"]
                        + pre_signals["stakeholder_reporting_signal"]
                        + pre_signals["validation_or_reconciliation_signal"]
                        + pre_signals["CRM_or_salesops_reporting_signal"]
                    ) >= 2
                ):
                    comments, comment_status = _fetch_comments(
                        raw_id=raw_id,
                        limit=int(config.get("top_level_comment_cap", 5)),
                        timeout_seconds=int(config.get("timeout_seconds", 20)),
                    )
                    comment_fetch_count += 1
                    if comment_status == "ok":
                        comment_excerpt, examined = _build_comment_excerpt(
                            comments,
                            subreddit=subreddit_name,
                            limit=int(config.get("top_level_comment_cap", 5)),
                        )
                        total_fetched_comments += examined
                    else:
                        request_error_count += 1
                rows.append(
                    build_pilot_row(
                        post=post,
                        subreddit=subreddit_name,
                        comment_excerpt=comment_excerpt,
                        fetched_at=fetched_at,
                        fetch_method=str(config.get("fetch_method", "reddit_public_json")),
                    )
                )
                if total_fetched_posts >= int(config.get("target_raw_post_count", 1000)):
                    break
                if collected_for_subreddit >= int(config.get("max_posts_per_subreddit", 300)):
                    break
            if total_fetched_posts >= int(config.get("target_raw_post_count", 1000)):
                break
            if collected_for_subreddit >= int(config.get("max_posts_per_subreddit", 300)):
                break
            after = payload.get("data", {}).get("after")
            if not after:
                break
            if older_posts_seen:
                exhausted_subreddits.append(subreddit_name)
                break
            time.sleep(float(config.get("request_delay_seconds", 0.5)))
        subreddit_post_counts[subreddit_name] = collected_for_subreddit
        if collected_for_subreddit < int(config.get("max_posts_per_subreddit", 300)) and subreddit_name not in exhausted_subreddits:
            exhausted_subreddits.append(subreddit_name)
        if total_fetched_posts >= int(config.get("target_raw_post_count", 1000)):
            break

    write_jsonl(raw_path, [row.to_dict() for row in rows])
    pd.DataFrame([row.to_dict() for row in rows]).to_csv(sample_path, index=False)
    summary = build_summary(
        rows=rows,
        total_fetched_posts=total_fetched_posts,
        total_fetched_comments=total_fetched_comments,
        subreddit_post_counts=subreddit_post_counts,
        exhausted_subreddits=sorted(dict.fromkeys(exhausted_subreddits)),
        request_error_count=request_error_count,
        fetched_posts_target=int(config.get("target_raw_post_count", 1000)),
    )
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "seed_config": root_dir / DEFAULT_SEED_PATH,
        "raw_jsonl": raw_path,
        "sample_csv": sample_path,
        "summary_json": summary_path,
    }
