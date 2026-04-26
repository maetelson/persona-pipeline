"""Bounded public-HTML pilot utilities for Supermetrics Community."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html import unescape
from html.parser import HTMLParser
import json
import math
from pathlib import Path
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import pandas as pd

from src.utils.http_fetch import FetchResponse, fetch_text
from src.utils.io import ensure_dir, write_jsonl

SOURCE_NAME = "supermetrics_community"
BASE_URL = "https://community.supermetrics.com"
USER_AGENT = "persona-research-bot/0.1 (+public html pilot; no login; local audit)"
ARTIFACT_DIR = Path("artifacts") / "source_pilots"
RAW_JSONL_NAME = "supermetrics_raw_pilot.jsonl"
SAMPLE_CSV_NAME = "supermetrics_pilot_sample.csv"
SUMMARY_JSON_NAME = "supermetrics_pilot_summary.json"

LISTING_URLS = [
    f"{BASE_URL}/community",
    f"{BASE_URL}/ask-the-community-43",
    f"{BASE_URL}/ask-and-discuss-31",
    f"{BASE_URL}/product-updates",
]
MAX_LISTING_ATTEMPTS = 8
TARGET_DISCOVERED_THREADS = 120
MAX_DETAIL_ATTEMPTS = 200
REQUEST_DELAY_SECONDS = 0.75
TIMEOUT_SECONDS = 20

THREAD_PATH_RE = re.compile(r"^/[a-z0-9-]+-\d+/[a-z0-9-]+-\d+/?$", re.IGNORECASE)
LISTING_PATH_RE = re.compile(r"^/[a-z0-9-]+-\d+/?(?:\?page=\d+)?$", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
JSON_LD_RE = re.compile(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', re.IGNORECASE | re.DOTALL)

POSITIVE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "reporting_pain_signal": (
        "report",
        "reporting",
        "dashboard",
        "kpi",
        "metric",
        "scorecard",
        "client report",
    ),
    "dashboard_or_metric_signal": (
        "dashboard",
        "metric",
        "scorecard",
        "visualization",
        "looker studio",
        "report",
    ),
    "attribution_or_blended_data_signal": (
        "attribution",
        "blend",
        "blending",
        "cross-channel",
        "join",
        "merge",
        "cohort",
        "mismatch",
    ),
    "export_or_spreadsheet_signal": (
        "spreadsheet",
        "excel",
        "google sheets",
        "sheet",
        "csv",
        "export",
    ),
    "validation_or_reconciliation_signal": (
        "validate",
        "validation",
        "reconcile",
        "reconciliation",
        "mismatch",
        "discrep",
        "wrong results",
        "trust",
    ),
    "stakeholder_or_delivery_context": (
        "client",
        "stakeholder",
        "exec",
        "deliverable",
        "delivery",
        "operator",
        "report for",
        "dashboard for",
    ),
}

NOISE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "setup_support_noise": (
        "login",
        "reauth",
        "permission",
        "account",
        "trial",
        "billing",
        "error message",
        "support",
        "connect",
    ),
    "connector_setup_noise": (
        "connector",
        "oauth",
        "api key",
        "token",
        "credentials",
        "authentication",
        "connection",
        "postman",
    ),
    "api_developer_noise": (
        "api",
        "endpoint",
        "script",
        "json",
        "developer",
        "postman",
        "oauth",
    ),
    "training_certification_noise": (
        "training",
        "course",
        "certification",
        "webinar",
        "bootcamp",
    ),
    "hiring_career_noise": (
        "consultant",
        "developer",
        "hiring",
        "job",
        "career",
        "looking for",
    ),
    "vendor_announcement_noise": (
        "what's new",
        "product update",
        "announcement",
        "release",
        "new fields",
        "new home",
    ),
}


@dataclass(slots=True)
class PilotRow:
    """One bounded Supermetrics pilot row."""

    source: str
    raw_id: str
    url: str
    title: str
    body_or_excerpt: str
    category: str
    tags: str
    author: str
    created_at: str
    reply_count: int | None
    accepted_solution: bool | None
    fetch_status: str
    fetch_method: str
    fetched_at: str
    reporting_pain_signal: int
    dashboard_or_metric_signal: int
    attribution_or_blended_data_signal: int
    export_or_spreadsheet_signal: int
    validation_or_reconciliation_signal: int
    stakeholder_or_delivery_context: int
    setup_support_noise: int
    connector_setup_noise: int
    api_developer_noise: int
    training_certification_noise: int
    hiring_career_noise: int
    vendor_announcement_noise: int
    persona_01_fit: int
    persona_02_fit: int
    persona_03_fit: int
    persona_04_fit: int
    persona_05_fit: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize the pilot row."""
        return asdict(self)


@dataclass(slots=True)
class DetailParseResult:
    """Parsed detail-thread fields from public HTML."""

    raw_id: str
    title: str
    body_or_excerpt: str
    category: str
    tags: list[str]
    author: str
    created_at: str
    reply_count: int | None
    accepted_solution: bool | None


class _RobotsCache:
    """Cache robots.txt decisions by hostname."""

    def __init__(self, user_agent: str) -> None:
        self.user_agent = user_agent
        self._parsers: dict[str, RobotFileParser | None] = {}

    def allowed(self, url: str) -> tuple[bool, str]:
        """Return whether the URL is allowed by cached robots rules."""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        parser = self._parsers.get(base)
        if parser is None and base not in self._parsers:
            robots_url = urljoin(base, "/robots.txt")
            response = fetch_text(robots_url, self.user_agent, timeout_seconds=TIMEOUT_SECONDS)
            if not response.ok:
                self._parsers[base] = None
                return True, ""
            parser = RobotFileParser()
            parser.parse(response.body_text.splitlines())
            self._parsers[base] = parser
        parser = self._parsers.get(base)
        if parser is None:
            return True, ""
        allowed = parser.can_fetch(self.user_agent, url)
        return allowed, "" if allowed else "robots_disallow"


class _TextExtractor(HTMLParser):
    """Extract visible text from HTML."""

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        """Capture text nodes."""
        value = data.strip()
        if value:
            self.parts.append(value)


class _AnchorExtractor(HTMLParser):
    """Extract links from HTML pages."""

    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Capture anchor href values."""
        if tag.lower() != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href")
        if href:
            self.hrefs.append(href)


def collapse_text(value: str) -> str:
    """Collapse whitespace and strip HTML artifacts."""
    no_tags = TAG_RE.sub(" ", unescape(value))
    return WHITESPACE_RE.sub(" ", no_tags).strip()


def extract_thread_urls(html_text: str, base_url: str = BASE_URL) -> list[str]:
    """Extract thread detail URLs from a listing page."""
    parser = _AnchorExtractor()
    parser.feed(html_text)
    discovered: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        joined = urljoin(base_url, href)
        path = urlparse(joined).path
        if not THREAD_PATH_RE.match(path):
            continue
        normalized = normalize_url(joined)
        if normalized in seen:
            continue
        seen.add(normalized)
        discovered.append(normalized)
    return discovered


def extract_listing_urls(html_text: str, base_url: str = BASE_URL) -> list[str]:
    """Extract additional listing pages from a listing HTML response."""
    parser = _AnchorExtractor()
    parser.feed(html_text)
    discovered: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        joined = urljoin(base_url, href)
        path = urlparse(joined).path
        query = urlparse(joined).query
        candidate = path if not query else f"{path}?{query}"
        if not LISTING_PATH_RE.match(candidate):
            continue
        normalized = normalize_url(joined)
        if normalized in seen:
            continue
        seen.add(normalized)
        discovered.append(normalized)
    return discovered


def parse_thread_detail_html(url: str, html_text: str) -> DetailParseResult:
    """Parse a Supermetrics thread page from public HTML."""
    title = ""
    title_match = re.search(r"<title>(.*?)</title>", html_text, re.IGNORECASE | re.DOTALL)
    if title_match:
        title = collapse_text(title_match.group(1))
        title = title.replace("| Supermetrics Community", "").strip()

    heading_match = re.search(r"<h1[^>]*>(.*?)</h1>", html_text, re.IGNORECASE | re.DOTALL)
    if heading_match:
        title = collapse_text(heading_match.group(1)) or title

    text = collapse_text(html_text)
    category = ""
    category_match = re.search(r"Ask the Community|Data visualization|Spreadsheets|Storage solutions|Data transformations|Trends and hot topics|Tips and tricks|Connector Builder|Product updates", text, re.IGNORECASE)
    if category_match:
        category = category_match.group(0)

    author = ""
    author_match = re.search(r"\b([A-Z][A-Za-z0-9_.-]{1,40})\b\s+Newbie", text)
    if author_match:
        author = author_match.group(1)

    created_at = ""
    date_match = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}", text)
    if date_match:
        created_at = date_match.group(0)

    reply_count = None
    reply_match = re.search(r"(\d+)\s+repl(?:y|ies)\b", text, re.IGNORECASE)
    if reply_match:
        reply_count = int(reply_match.group(1))

    accepted_solution = bool(re.search(r"\bSolved\b|\bBest answer\b", text, re.IGNORECASE))
    tags = extract_tags_from_json_ld(html_text)
    raw_id = extract_raw_id_from_url(url)

    body_excerpt = text[:2500]
    return DetailParseResult(
        raw_id=raw_id,
        title=title,
        body_or_excerpt=body_excerpt,
        category=category,
        tags=tags,
        author=author,
        created_at=created_at,
        reply_count=reply_count,
        accepted_solution=accepted_solution,
    )


def extract_tags_from_json_ld(html_text: str) -> list[str]:
    """Extract tags from JSON-LD blobs when present."""
    tags: list[str] = []
    for match in JSON_LD_RE.findall(html_text):
        try:
            payload = json.loads(match)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            keywords = payload.get("keywords")
            if isinstance(keywords, str):
                tags.extend(part.strip() for part in keywords.split(",") if part.strip())
    return dedupe_preserve_order(tags)


def extract_raw_id_from_url(url: str) -> str:
    """Build a stable raw id from the thread URL."""
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1] if path else normalize_url(url)


def normalize_url(url: str) -> str:
    """Normalize a public community URL."""
    parsed = urlparse(urljoin(BASE_URL, url))
    normalized_path = parsed.path.rstrip("/")
    normalized = f"{parsed.scheme}://{parsed.netloc}{normalized_path}"
    if parsed.query:
        normalized = f"{normalized}?{parsed.query}"
    return normalized


def dedupe_preserve_order(values: list[str]) -> list[str]:
    """Deduplicate strings while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def keyword_signal(text: str, keywords: tuple[str, ...]) -> int:
    """Return 1 if any keyword appears, otherwise 0."""
    lowered = text.lower()
    return int(any(keyword in lowered for keyword in keywords))


def score_signals(title: str, body: str, category: str, tags: list[str]) -> dict[str, int]:
    """Score pilot-quality positive and noise signals."""
    text = " ".join([title, body, category, " ".join(tags)]).lower()
    scores: dict[str, int] = {}
    for name, keywords in POSITIVE_KEYWORDS.items():
        scores[name] = keyword_signal(text, keywords)
    for name, keywords in NOISE_KEYWORDS.items():
        scores[name] = keyword_signal(text, keywords)
    return scores


def score_persona_fit(signal_scores: dict[str, int], title: str, body: str) -> dict[str, int]:
    """Estimate coarse persona fit using pilot-only heuristics."""
    text = f"{title} {body}".lower()
    persona_01 = min(
        2,
        signal_scores["reporting_pain_signal"]
        + signal_scores["export_or_spreadsheet_signal"]
        + int("manual" in text or "spreadsheet" in text or "report" in text),
    )
    persona_02 = min(
        2,
        signal_scores["attribution_or_blended_data_signal"]
        + int("why" in text or "diagnos" in text or "root cause" in text or "explain" in text),
    )
    persona_03 = min(
        2,
        signal_scores["dashboard_or_metric_signal"]
        + int("not support" in text or "fails" in text or "missing" in text or "workaround" in text),
    )
    persona_04 = min(
        2,
        signal_scores["validation_or_reconciliation_signal"]
        + signal_scores["dashboard_or_metric_signal"]
        + int("mismatch" in text or "reconcile" in text or "trust" in text),
    )
    persona_05 = min(
        2,
        signal_scores["stakeholder_or_delivery_context"]
        + signal_scores["dashboard_or_metric_signal"]
        + int("layout" in text or "filter" in text or "visualization" in text or "shareable" in text),
    )
    return {
        "persona_01_fit": persona_01,
        "persona_02_fit": persona_02,
        "persona_03_fit": persona_03,
        "persona_04_fit": persona_04,
        "persona_05_fit": persona_05,
    }


def build_pilot_row(parsed: DetailParseResult, url: str, fetch_status: str, fetched_at: str) -> PilotRow:
    """Build one scored pilot row from parsed detail HTML."""
    signals = score_signals(parsed.title, parsed.body_or_excerpt, parsed.category, parsed.tags)
    persona_scores = score_persona_fit(signals, parsed.title, parsed.body_or_excerpt)
    return PilotRow(
        source=SOURCE_NAME,
        raw_id=parsed.raw_id,
        url=url,
        title=parsed.title,
        body_or_excerpt=parsed.body_or_excerpt,
        category=parsed.category,
        tags=" | ".join(parsed.tags),
        author=parsed.author,
        created_at=parsed.created_at,
        reply_count=parsed.reply_count,
        accepted_solution=parsed.accepted_solution,
        fetch_status=fetch_status,
        fetch_method="public_html",
        fetched_at=fetched_at,
        **signals,
        **persona_scores,
    )


def estimate_valid_candidate(row: PilotRow) -> bool:
    """Estimate whether a row looks like a viable candidate for later stages."""
    positive = (
        row.reporting_pain_signal
        + row.dashboard_or_metric_signal
        + row.validation_or_reconciliation_signal
        + row.stakeholder_or_delivery_context
        + row.attribution_or_blended_data_signal
    )
    noise = (
        row.setup_support_noise
        + row.connector_setup_noise
        + row.api_developer_noise
        + row.training_certification_noise
        + row.hiring_career_noise
        + row.vendor_announcement_noise
    )
    return positive >= 2 and noise <= 1


def estimate_persona_core_candidate(row: PilotRow) -> bool:
    """Estimate whether a row resembles persona-core evidence."""
    return estimate_valid_candidate(row) and max(
        row.persona_01_fit,
        row.persona_02_fit,
        row.persona_03_fit,
        row.persona_04_fit,
        row.persona_05_fit,
    ) >= 1


def estimate_labelable(row: PilotRow) -> bool:
    """Estimate whether a row is likely labelable."""
    return estimate_valid_candidate(row) and (
        row.reporting_pain_signal
        or row.dashboard_or_metric_signal
        or row.validation_or_reconciliation_signal
        or row.attribution_or_blended_data_signal
    )


def build_summary(
    rows: list[PilotRow],
    listing_pages_attempted: int,
    listing_pages_succeeded: int,
    listing_forbidden_403_count: int,
    listing_not_found_404_count: int,
    listing_timeout_error_count: int,
    thread_urls_discovered: int,
    detail_attempts: int,
    forbidden_403_count: int,
    not_found_404_count: int,
    timeout_error_count: int,
    duplicate_url_count: int,
    stable_pagination_seen: bool,
) -> dict[str, Any]:
    """Build summary metrics and onboarding recommendation."""
    successful_detail_count = sum(1 for row in rows if row.fetch_status == "ok")
    usable_content_count = sum(1 for row in rows if bool(row.body_or_excerpt.strip()))
    valid_candidate_count = sum(1 for row in rows if estimate_valid_candidate(row))
    persona_core_candidate_count = sum(1 for row in rows if estimate_persona_core_candidate(row))
    estimated_labelable_count = sum(1 for row in rows if estimate_labelable(row))

    detail_attempts_safe = max(detail_attempts, 1)
    usable_safe = max(usable_content_count, 1)
    success_rate = successful_detail_count / detail_attempts_safe
    forbidden_rate = forbidden_403_count / detail_attempts_safe
    valid_ratio = valid_candidate_count / usable_safe
    estimated_labelable_ratio = estimated_labelable_count / usable_safe

    persona_fit_counts = {
        "persona_01_fit_count": sum(1 for row in rows if row.persona_01_fit >= 1),
        "persona_02_fit_count": sum(1 for row in rows if row.persona_02_fit >= 1),
        "persona_03_fit_count": sum(1 for row in rows if row.persona_03_fit >= 1),
        "persona_04_fit_count": sum(1 for row in rows if row.persona_04_fit >= 1),
        "persona_05_fit_count": sum(1 for row in rows if row.persona_05_fit >= 1),
    }
    setup_support_noise_count = sum(1 for row in rows if row.setup_support_noise)
    api_developer_noise_count = sum(1 for row in rows if row.api_developer_noise)
    vendor_announcement_noise_count = sum(1 for row in rows if row.vendor_announcement_noise)

    if listing_pages_succeeded == 0:
        stability = "blocked"
    elif success_rate < 0.5 or forbidden_rate > 0.25:
        stability = "unstable"
    elif stable_pagination_seen and success_rate >= 0.75:
        stability = "stable"
    else:
        stability = "limited"

    expected_source_tier = "supporting_validation_source"
    if valid_ratio >= 0.55 and estimated_labelable_ratio >= 0.60 and forbidden_rate <= 0.10:
        expected_source_tier = "core_representative_source_candidate"

    weak_source_risk = "high"
    if valid_ratio >= 0.45 and setup_support_noise_count <= max(valid_candidate_count, 1):
        weak_source_risk = "medium"
    if valid_ratio >= 0.60 and api_developer_noise_count < max(valid_candidate_count // 3, 1):
        weak_source_risk = "low"

    gate_checks = {
        "enough_discovered_threads_or_pagination": thread_urls_discovered >= 100 or stable_pagination_seen,
        "acceptable_successful_detail_rate": success_rate >= 0.60,
        "acceptable_403_rate": forbidden_rate <= 0.25,
        "strong_valid_candidate_ratio": valid_ratio >= 0.35,
        "estimated_labelable_ratio_ge_60": estimated_labelable_ratio >= 0.60,
        "support_or_api_noise_not_dominant": (setup_support_noise_count + api_developer_noise_count) <= max(usable_content_count * 0.5, 1),
        "persona_01_or_persona_04_fit_meaningful": persona_fit_counts["persona_01_fit_count"] >= 10 or persona_fit_counts["persona_04_fit_count"] >= 10,
        "public_html_extraction_stable_enough": stability in {"limited", "stable"},
    }
    gate_pass = all(gate_checks.values())

    if gate_pass:
        recommendation = "build_supermetrics_collector"
    elif listing_pages_succeeded == 0 or stability == "blocked":
        recommendation = "reject_supermetrics_and_try_next_candidate"
    elif thread_urls_discovered < 40:
        recommendation = "expand_pilot_with_different_category"
    elif weak_source_risk == "high":
        recommendation = "keep_supermetrics_as_low_priority_supporting_source"
    else:
        recommendation = "pause_source_expansion"

    summary = {
        "source": SOURCE_NAME,
        "total_discovered_threads": thread_urls_discovered,
        "total_detail_attempts": detail_attempts,
        "successful_detail_count": successful_detail_count,
        "forbidden_403_count": forbidden_403_count,
        "forbidden_403_rate": round(forbidden_rate, 4),
        "not_found_404_count": not_found_404_count,
        "timeout_error_count": timeout_error_count,
        "duplicate_url_count": duplicate_url_count,
        "usable_content_count": usable_content_count,
        "valid_candidate_count": valid_candidate_count,
        "valid_candidate_ratio": round(valid_ratio, 4),
        "estimated_persona_core_candidate_count": persona_core_candidate_count,
        "estimated_labelable_ratio": round(estimated_labelable_ratio, 4),
        **persona_fit_counts,
        "setup_support_noise_count": setup_support_noise_count,
        "api_developer_noise_count": api_developer_noise_count,
        "vendor_announcement_noise_count": vendor_announcement_noise_count,
        "expected_source_tier": expected_source_tier,
        "estimated_weak_source_risk": weak_source_risk,
        "listing_pages_attempted": listing_pages_attempted,
        "listing_pages_succeeded": listing_pages_succeeded,
        "listing_forbidden_403_count": listing_forbidden_403_count,
        "listing_not_found_404_count": listing_not_found_404_count,
        "listing_timeout_error_count": listing_timeout_error_count,
        "public_access_stability_assessment": stability,
        "successful_detail_rate": round(success_rate, 4),
        "stable_pagination_seen": stable_pagination_seen,
        "onboarding_gate_checks": gate_checks,
        "onboarding_gate_result": "pass" if gate_pass else "fail",
        "recommendation": recommendation,
    }
    if gate_pass:
        summary["collector_implementation_plan"] = {
            "source_config": "create config/sources/supermetrics_community.yaml only after pilot approval",
            "seed_category_urls": [
                f"{BASE_URL}/community",
                f"{BASE_URL}/ask-the-community-43",
                f"{BASE_URL}/ask-and-discuss-31",
            ],
            "parser_assumptions": [
                "thread urls follow /category-id/thread-slug-id pattern",
                "title and body remain visible in public HTML",
                "reply counts and solved badges are text-parseable",
            ],
            "normalizer_fields": [
                "raw_id",
                "title",
                "body",
                "category",
                "tags",
                "author",
                "created_at",
                "reply_count",
                "accepted_solution",
            ],
            "dedupe_strategy": "canonicalize thread URLs and use thread slug-id tail as raw_id",
            "rate_limit": f"{REQUEST_DELAY_SECONDS} seconds between requests, max {MAX_DETAIL_ATTEMPTS} detail attempts",
            "test_fixtures": "store one listing fixture, one detail fixture, one noisy support fixture, and one blocked response fixture",
            "production_activation_criteria": "must pass pilot gate and must not land as weak_source_cost_center in extended trial",
            "rollback_criteria": "sustained 403 or 404 rates, unstable HTML structure, or weak-source behavior in downstream diagnostics",
        }
    return summary


def run_supermetrics_pilot(root_dir: Path) -> dict[str, Path]:
    """Run the bounded Supermetrics Community HTML-only pilot."""
    artifact_dir = ensure_dir(root_dir / ARTIFACT_DIR)
    robots = _RobotsCache(USER_AGENT)
    fetched_at = datetime.now(UTC).isoformat()

    listing_queue = list(LISTING_URLS)
    seen_listing_urls: set[str] = set()
    thread_urls: list[str] = []
    thread_seen: set[str] = set()
    duplicate_url_count = 0
    listing_pages_attempted = 0
    listing_pages_succeeded = 0
    stable_pagination_seen = False
    listing_forbidden_403_count = 0
    listing_not_found_404_count = 0
    listing_timeout_error_count = 0

    while listing_queue and listing_pages_attempted < MAX_LISTING_ATTEMPTS and len(thread_urls) < TARGET_DISCOVERED_THREADS:
        url = normalize_url(listing_queue.pop(0))
        if url in seen_listing_urls:
            continue
        seen_listing_urls.add(url)
        listing_pages_attempted += 1
        allowed, reason = robots.allowed(url)
        if not allowed:
            time.sleep(REQUEST_DELAY_SECONDS)
            continue
        response = fetch_text(url, USER_AGENT, timeout_seconds=TIMEOUT_SECONDS)
        if response.ok and "html" in response.content_type.lower():
            listing_pages_succeeded += 1
            discovered_threads = extract_thread_urls(response.body_text)
            for thread_url in discovered_threads:
                if thread_url in thread_seen:
                    duplicate_url_count += 1
                    continue
                thread_seen.add(thread_url)
                thread_urls.append(thread_url)
                if len(thread_urls) >= TARGET_DISCOVERED_THREADS:
                    break
            extra_listing_urls = extract_listing_urls(response.body_text)
            if extra_listing_urls:
                stable_pagination_seen = True
            for listing_url in extra_listing_urls:
                normalized = normalize_url(listing_url)
                if normalized not in seen_listing_urls and normalized not in listing_queue:
                    listing_queue.append(normalized)
        elif response.status_code == 403:
            listing_forbidden_403_count += 1
        elif response.status_code == 404:
            listing_not_found_404_count += 1
        else:
            listing_timeout_error_count += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    rows: list[PilotRow] = []
    forbidden_403_count = 0
    not_found_404_count = 0
    timeout_error_count = 0

    detail_urls = thread_urls[:MAX_DETAIL_ATTEMPTS]
    for thread_url in detail_urls:
        allowed, reason = robots.allowed(thread_url)
        if not allowed:
            forbidden_403_count += 1
            rows.append(
                PilotRow(
                    source=SOURCE_NAME,
                    raw_id=extract_raw_id_from_url(thread_url),
                    url=thread_url,
                    title="",
                    body_or_excerpt="",
                    category="",
                    tags="",
                    author="",
                    created_at="",
                    reply_count=None,
                    accepted_solution=None,
                    fetch_status=reason,
                    fetch_method="public_html",
                    fetched_at=fetched_at,
                    reporting_pain_signal=0,
                    dashboard_or_metric_signal=0,
                    attribution_or_blended_data_signal=0,
                    export_or_spreadsheet_signal=0,
                    validation_or_reconciliation_signal=0,
                    stakeholder_or_delivery_context=0,
                    setup_support_noise=0,
                    connector_setup_noise=0,
                    api_developer_noise=0,
                    training_certification_noise=0,
                    hiring_career_noise=0,
                    vendor_announcement_noise=0,
                    persona_01_fit=0,
                    persona_02_fit=0,
                    persona_03_fit=0,
                    persona_04_fit=0,
                    persona_05_fit=0,
                )
            )
            time.sleep(REQUEST_DELAY_SECONDS)
            continue
        response = fetch_text(thread_url, USER_AGENT, timeout_seconds=TIMEOUT_SECONDS)
        if not response.ok:
            if response.status_code == 403:
                forbidden_403_count += 1
            elif response.status_code == 404:
                not_found_404_count += 1
            else:
                timeout_error_count += 1
            rows.append(
                PilotRow(
                    source=SOURCE_NAME,
                    raw_id=extract_raw_id_from_url(thread_url),
                    url=thread_url,
                    title="",
                    body_or_excerpt="",
                    category="",
                    tags="",
                    author="",
                    created_at="",
                    reply_count=None,
                    accepted_solution=None,
                    fetch_status=f"{response.crawl_status}:{response.status_code or response.error_message}",
                    fetch_method="public_html",
                    fetched_at=fetched_at,
                    reporting_pain_signal=0,
                    dashboard_or_metric_signal=0,
                    attribution_or_blended_data_signal=0,
                    export_or_spreadsheet_signal=0,
                    validation_or_reconciliation_signal=0,
                    stakeholder_or_delivery_context=0,
                    setup_support_noise=0,
                    connector_setup_noise=0,
                    api_developer_noise=0,
                    training_certification_noise=0,
                    hiring_career_noise=0,
                    vendor_announcement_noise=0,
                    persona_01_fit=0,
                    persona_02_fit=0,
                    persona_03_fit=0,
                    persona_04_fit=0,
                    persona_05_fit=0,
                )
            )
            time.sleep(REQUEST_DELAY_SECONDS)
            continue
        parsed = parse_thread_detail_html(thread_url, response.body_text)
        rows.append(build_pilot_row(parsed, thread_url, "ok", fetched_at))
        time.sleep(REQUEST_DELAY_SECONDS)

    summary = build_summary(
        rows=rows,
        listing_pages_attempted=listing_pages_attempted,
        listing_pages_succeeded=listing_pages_succeeded,
        listing_forbidden_403_count=listing_forbidden_403_count,
        listing_not_found_404_count=listing_not_found_404_count,
        listing_timeout_error_count=listing_timeout_error_count,
        thread_urls_discovered=len(thread_urls),
        detail_attempts=len(detail_urls),
        forbidden_403_count=forbidden_403_count,
        not_found_404_count=not_found_404_count,
        timeout_error_count=timeout_error_count,
        duplicate_url_count=duplicate_url_count,
        stable_pagination_seen=stable_pagination_seen,
    )

    raw_path = artifact_dir / RAW_JSONL_NAME
    csv_path = artifact_dir / SAMPLE_CSV_NAME
    summary_path = artifact_dir / SUMMARY_JSON_NAME
    write_jsonl(raw_path, [row.to_dict() for row in rows])
    dataframe = pd.DataFrame([row.to_dict() for row in rows])
    if dataframe.empty:
        dataframe = pd.DataFrame(columns=list(PilotRow.__annotations__.keys()))
    dataframe.to_csv(csv_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "raw_jsonl": raw_path,
        "sample_csv": csv_path,
        "summary_json": summary_path,
    }
