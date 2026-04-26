"""Bounded GitHub Discussions expansion pilot utilities."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html import unescape
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd

from src.utils.http_fetch import check_robots_allowed, fetch_text
from src.utils.io import ensure_dir, load_yaml, write_jsonl

SOURCE_NAME = "github_discussions_expansion_bi_tools"
BASE_URL = "https://github.com"
USER_AGENT = "persona-research-bot/0.1 (+bounded github discussions pilot; public html only; local audit)"
DEFAULT_SEED_PATH = Path("config") / "seeds" / "github_discussions" / "github_discussions_expansion_pilot.yaml"
DEFAULT_SPEC_PATH = Path("artifacts") / "readiness" / "github_discussions_expansion_pilot_spec.json"
DISCUSSION_PATH_RE = re.compile(r"^/[^/]+/[^/]+/discussions/\d+/?$", re.IGNORECASE)
PAGINATION_PATH_RE = re.compile(r"^/[^/]+/[^/]+/discussions(?:\?page=\d+)?$", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
COMMENT_BLOCK_RE = re.compile(
    r'<td[^>]+class="[^"]*comment-body[^"]*markdown-body[^"]*"[^>]*>(.*?)</td>',
    re.IGNORECASE | re.DOTALL,
)
TITLE_BDI_RE = re.compile(r'<bdi[^>]+class="[^"]*js-issue-title[^"]*"[^>]*>(.*?)</bdi>', re.IGNORECASE | re.DOTALL)
HTML_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)
CATEGORY_RE = re.compile(
    r'(?:started this conversation|asked)[^<]{0,120}in\s*<a[^>]*href="/[^"]*/discussions/categories/[^"]+"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
RELATIVE_TIME_RE = re.compile(r'<relative-time[^>]+datetime="([^"]+)"', re.IGNORECASE)
AUTHOR_RE = re.compile(
    r'<a[^>]+data-hovercard-type="user"[^>]*>\s*<span[^>]*>\s*([^<\s][^<]*)\s*</span>\s*</a>',
    re.IGNORECASE | re.DOTALL,
)
AUTHOR_FALLBACK_RE = re.compile(
    r'<span[^>]*>\s*([^<\s][^<]*)\s*</span>\s*</a>\s*(?:started this conversation|asked)',
    re.IGNORECASE | re.DOTALL,
)
AUTHOR_SIMPLE_RE = re.compile(
    r'<span[^>]*>\s*([^<\s][^<]*)\s*</span>\s*(?:started this conversation|asked)',
    re.IGNORECASE | re.DOTALL,
)
LABEL_RE = re.compile(
    r'<a[^>]+href="/[^"]*/labels/[^"]+"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

POSITIVE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "dashboard_reporting_pain": (
        "dashboard",
        "reporting",
        "report",
        "chart",
        "visualization",
        "totals",
        "wrong totals",
    ),
    "metric_definition_confusion": (
        "metric definition",
        "semantic metric",
        "which metric",
        "definition",
        "wrong metric",
        "source of truth",
    ),
    "semantic_layer_or_model_confusion_tied_to_reporting": (
        "semantic layer",
        "model confusion",
        "model freshness",
        "drill down by segment",
        "metricflow",
        "dimensions",
    ),
    "stakeholder_reporting_need": (
        "stakeholder",
        "leadership",
        "executive",
        "business review",
        "explain numbers",
        "share with",
    ),
    "export_or_spreadsheet_workaround": (
        "export to excel",
        "spreadsheet",
        "csv export",
        "manual report",
        "copy paste",
    ),
    "data_trust_or_reconciliation_issue": (
        "reconcile",
        "reconciliation",
        "numbers don't match",
        "numbers do not match",
        "trust",
        "source of truth",
        "mismatch",
    ),
    "workflow_limitation_in_bi_reporting_tool": (
        "not enough",
        "workaround",
        "limitation",
        "missing drill down",
        "dashboard not enough",
        "cannot",
        "can't",
    ),
    "root_cause_or_explanation_handoff": (
        "why did this change",
        "root cause",
        "explain numbers",
        "what changed",
        "handoff",
        "segment",
    ),
}

NEGATIVE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "install_deploy_debug_noise": (
        "install",
        "deployment",
        "docker",
        "helm",
        "kubernetes",
        "stack trace",
        "traceback",
    ),
    "auth_permission_setup_noise": (
        "permission",
        "permissions",
        "authentication",
        "oauth",
        "login",
        "setup",
        "configure",
    ),
    "generic_feature_request_without_user_pain": (
        "feature request",
        "would be nice",
        "please add",
        "enhancement",
    ),
    "api_library_coding_issue": (
        "api",
        "sdk",
        "library",
        "python model",
        "javascript",
        "typescript",
        "sql syntax",
    ),
    "ci_cd_infrastructure_issue": (
        "ci/cd",
        "github actions",
        "build pipeline",
        "infra",
        "infrastructure",
        "terraform",
    ),
    "beginner_tutorial_help": (
        "tutorial",
        "beginner",
        "how do i start",
        "new to",
        "learning",
    ),
    "release_announcement": (
        "announcement",
        "release",
        "what's new",
        "now available",
    ),
    "maintainer_internal_discussion_without_user_pain": (
        "maintainer",
        "internal",
        "roadmap only",
        "community guidelines",
    ),
}


@dataclass(slots=True)
class GithubDiscussionPilotRow:
    """One scored GitHub Discussions pilot row."""

    source: str
    repo: str
    raw_id: str
    url: str
    title: str
    body_or_excerpt: str
    category: str
    labels: str
    author: str
    created_at: str
    updated_at: str
    comment_count: int
    comment_excerpt: str
    fetch_status: str
    fetch_method: str
    fetched_at: str
    dashboard_reporting_pain: int
    metric_definition_confusion: int
    semantic_layer_or_model_confusion_tied_to_reporting: int
    stakeholder_reporting_need: int
    export_or_spreadsheet_workaround: int
    data_trust_or_reconciliation_issue: int
    workflow_limitation_in_bi_reporting_tool: int
    root_cause_or_explanation_handoff: int
    install_deploy_debug_noise: int
    auth_permission_setup_noise: int
    generic_feature_request_without_user_pain: int
    api_library_coding_issue: int
    ci_cd_infrastructure_issue: int
    beginner_tutorial_help: int
    release_announcement: int
    maintainer_internal_discussion_without_user_pain: int
    persona_01_fit: str
    persona_02_fit: str
    persona_03_fit: str
    persona_04_fit: str
    persona_05_fit: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize the pilot row."""
        return asdict(self)


@dataclass(slots=True)
class DiscussionParseResult:
    """Parsed detail fields from a public GitHub discussion page."""

    raw_id: str
    title: str
    body_or_excerpt: str
    category: str
    labels: list[str]
    author: str
    created_at: str
    updated_at: str
    comment_count: int
    comment_excerpt: str


class _AnchorExtractor(HTMLParser):
    """Extract href values from HTML."""

    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Capture anchor hrefs."""
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.hrefs.append(href)


def load_pilot_seed_config(root_dir: Path, seed_path: Path | None = None) -> dict[str, Any]:
    """Load the pilot repo bundle config."""
    path = seed_path or DEFAULT_SEED_PATH
    return load_yaml(root_dir / path)


def load_pilot_spec(root_dir: Path, spec_path: Path | None = None) -> dict[str, Any]:
    """Load the pilot spec JSON if present."""
    path = root_dir / (spec_path or DEFAULT_SPEC_PATH)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def is_pilot_output_path(path: Path, root_dir: Path) -> bool:
    """Guard pilot outputs so they stay outside production directories."""
    try:
        path.resolve().relative_to((root_dir / "artifacts" / "source_pilots").resolve())
        return True
    except ValueError:
        return False


def collapse_text(value: str) -> str:
    """Collapse HTML to compact readable text."""
    no_tags = TAG_RE.sub(" ", unescape(value))
    return WHITESPACE_RE.sub(" ", no_tags).strip()


def normalize_url(url: str) -> str:
    """Normalize GitHub discussion or listing URLs."""
    parsed = urlparse(urljoin(BASE_URL, url))
    normalized_path = parsed.path.rstrip("/")
    normalized = f"{parsed.scheme}://{parsed.netloc}{normalized_path}"
    if parsed.query:
        normalized = f"{normalized}?{parsed.query}"
    return normalized


def extract_discussion_urls(html_text: str, repo_name: str) -> list[str]:
    """Extract discussion detail URLs for one repo listing page."""
    parser = _AnchorExtractor()
    parser.feed(html_text)
    prefix = f"/{repo_name}/discussions/"
    discovered: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        joined = normalize_url(href)
        parsed = urlparse(joined)
        if not parsed.path.startswith(prefix):
            continue
        if not DISCUSSION_PATH_RE.match(parsed.path):
            continue
        if joined in seen:
            continue
        seen.add(joined)
        discovered.append(joined)
    return discovered


def extract_listing_urls(html_text: str, repo_name: str) -> list[str]:
    """Extract pagination URLs for one repo listing page."""
    parser = _AnchorExtractor()
    parser.feed(html_text)
    prefix = f"/{repo_name}/discussions"
    discovered: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        joined = normalize_url(href)
        parsed = urlparse(joined)
        candidate = parsed.path if not parsed.query else f"{parsed.path}?{parsed.query}"
        if not parsed.path.startswith(prefix):
            continue
        if not PAGINATION_PATH_RE.match(candidate):
            continue
        if joined in seen:
            continue
        seen.add(joined)
        discovered.append(joined)
    return discovered


def parse_discussion_detail_html(url: str, repo_name: str, html_text: str, comment_cap: int) -> DiscussionParseResult:
    """Parse a public GitHub discussion detail page."""
    raw_id = urlparse(url).path.rstrip("/").split("/")[-1]
    title = ""
    match = TITLE_BDI_RE.search(html_text)
    if match:
        title = collapse_text(match.group(1))
    if not title:
        title_match = HTML_TITLE_RE.search(html_text)
        if title_match:
            title = collapse_text(title_match.group(1)).replace(" · Discussions · GitHub", "").strip()

    category = ""
    category_match = CATEGORY_RE.search(html_text)
    if category_match:
        category = collapse_text(category_match.group(1))

    labels = [collapse_text(item) for item in LABEL_RE.findall(html_text)]

    author = ""
    author_match = AUTHOR_RE.search(html_text)
    if author_match:
        author = collapse_text(author_match.group(1))
    if not author:
        author_fallback_match = AUTHOR_FALLBACK_RE.search(html_text)
        if author_fallback_match:
            author = collapse_text(author_fallback_match.group(1))
    if not author:
        author_simple_match = AUTHOR_SIMPLE_RE.search(html_text)
        if author_simple_match:
            author = collapse_text(author_simple_match.group(1))

    timestamps = RELATIVE_TIME_RE.findall(html_text)
    created_at = timestamps[0] if timestamps else ""
    updated_at = timestamps[-1] if timestamps else created_at

    comment_blocks = [collapse_text(block) for block in COMMENT_BLOCK_RE.findall(html_text)]
    body_or_excerpt = comment_blocks[0][:3000] if comment_blocks else ""
    reply_blocks = [block[:800] for block in comment_blocks[1 : 1 + max(comment_cap, 0)] if block]
    comment_excerpt = " || ".join(reply_blocks)
    comment_count = len(comment_blocks) - 1 if comment_blocks else 0

    return DiscussionParseResult(
        raw_id=raw_id,
        title=title,
        body_or_excerpt=body_or_excerpt,
        category=category,
        labels=labels,
        author=author,
        created_at=created_at,
        updated_at=updated_at,
        comment_count=max(comment_count, 0),
        comment_excerpt=comment_excerpt,
    )


def keyword_signal(text: str, keywords: tuple[str, ...]) -> int:
    """Return 1 when any keyword appears in text."""
    lowered = text.lower()
    return int(any(keyword in lowered for keyword in keywords))


def score_signals(title: str, body: str, category: str, labels: list[str], comment_excerpt: str) -> dict[str, int]:
    """Score positive and negative pilot signals."""
    text = " ".join([title, body, category, " ".join(labels), comment_excerpt]).lower()
    scores: dict[str, int] = {}
    for name, keywords in POSITIVE_KEYWORDS.items():
        scores[name] = keyword_signal(text, keywords)
    for name, keywords in NEGATIVE_KEYWORDS.items():
        scores[name] = keyword_signal(text, keywords)
    return scores


def _fit_label(score: int) -> str:
    """Map a pilot fit score to weak/medium/strong."""
    if score >= 2:
        return "strong"
    if score == 1:
        return "medium"
    return "weak"


def score_persona_fit(signals: dict[str, int], title: str, body: str, comment_excerpt: str) -> dict[str, str]:
    """Estimate persona fit from discussion content."""
    text = " ".join([title, body, comment_excerpt]).lower()
    persona_01 = min(
        2,
        signals["dashboard_reporting_pain"]
        + signals["export_or_spreadsheet_workaround"]
        + int("weekly report" in text or "monthly report" in text or "manual report" in text),
    )
    persona_02 = min(
        2,
        signals["root_cause_or_explanation_handoff"]
        + signals["stakeholder_reporting_need"]
        + int("why did this change" in text or "explain" in text),
    )
    persona_03 = min(
        2,
        signals["workflow_limitation_in_bi_reporting_tool"]
        + signals["semantic_layer_or_model_confusion_tied_to_reporting"]
        + int("limitation" in text or "workaround" in text or "drill down" in text),
    )
    persona_04 = min(
        2,
        signals["data_trust_or_reconciliation_issue"]
        + signals["metric_definition_confusion"]
        + int("source of truth" in text or "numbers don't match" in text or "numbers do not match" in text),
    )
    persona_05 = min(
        2,
        signals["stakeholder_reporting_need"]
        + signals["workflow_limitation_in_bi_reporting_tool"]
        + int("presentation" in text or "layout" in text or "dashboard not enough" in text),
    )
    return {
        "persona_01_fit": _fit_label(persona_01),
        "persona_02_fit": _fit_label(persona_02),
        "persona_03_fit": _fit_label(persona_03),
        "persona_04_fit": _fit_label(persona_04),
        "persona_05_fit": _fit_label(persona_05),
    }


def fit_is_meaningful(value: str) -> bool:
    """Return whether a persona fit counts as meaningful signal."""
    return value in {"medium", "strong"}


def build_pilot_row(
    parsed: DiscussionParseResult,
    repo_name: str,
    url: str,
    fetch_method: str,
    fetched_at: str,
    *,
    source_name: str = SOURCE_NAME,
    fetch_status: str = "ok",
) -> GithubDiscussionPilotRow:
    """Build a scored pilot row from parsed detail HTML."""
    signals = score_signals(parsed.title, parsed.body_or_excerpt, parsed.category, parsed.labels, parsed.comment_excerpt)
    fits = score_persona_fit(signals, parsed.title, parsed.body_or_excerpt, parsed.comment_excerpt)
    return GithubDiscussionPilotRow(
        source=source_name,
        repo=repo_name,
        raw_id=parsed.raw_id,
        url=url,
        title=parsed.title,
        body_or_excerpt=parsed.body_or_excerpt,
        category=parsed.category,
        labels=" | ".join(parsed.labels),
        author=parsed.author,
        created_at=parsed.created_at,
        updated_at=parsed.updated_at,
        comment_count=parsed.comment_count,
        comment_excerpt=parsed.comment_excerpt,
        fetch_status=fetch_status,
        fetch_method=fetch_method,
        fetched_at=fetched_at,
        **signals,
        **fits,
    )


def estimate_valid_candidate(row: GithubDiscussionPilotRow) -> bool:
    """Estimate whether a row is relevant enough for downstream persona work."""
    positive = (
        row.dashboard_reporting_pain
        + row.metric_definition_confusion
        + row.semantic_layer_or_model_confusion_tied_to_reporting
        + row.stakeholder_reporting_need
        + row.data_trust_or_reconciliation_issue
        + row.workflow_limitation_in_bi_reporting_tool
        + row.root_cause_or_explanation_handoff
    )
    noise = (
        row.install_deploy_debug_noise
        + row.auth_permission_setup_noise
        + row.api_library_coding_issue
        + row.ci_cd_infrastructure_issue
        + row.beginner_tutorial_help
        + row.release_announcement
        + row.maintainer_internal_discussion_without_user_pain
    )
    return positive >= 2 and noise <= 1


def estimate_persona_core_candidate(row: GithubDiscussionPilotRow) -> bool:
    """Estimate whether a row looks persona-core rather than merely relevant."""
    if not estimate_valid_candidate(row):
        return False
    meaningful = sum(
        int(fit_is_meaningful(value))
        for value in [
            row.persona_01_fit,
            row.persona_02_fit,
            row.persona_03_fit,
            row.persona_04_fit,
        ]
    )
    return meaningful >= 1 and (
        row.dashboard_reporting_pain
        + row.metric_definition_confusion
        + row.data_trust_or_reconciliation_issue
        + row.workflow_limitation_in_bi_reporting_tool
    ) >= 2


def estimate_labelable(row: GithubDiscussionPilotRow) -> bool:
    """Estimate whether a row is likely labelable."""
    if not estimate_valid_candidate(row):
        return False
    return bool(row.title.strip() and row.body_or_excerpt.strip())


def build_summary(
    rows: list[GithubDiscussionPilotRow],
    total_discovered_discussions: int,
    total_fetched_discussions: int,
    total_fetched_comments: int,
    unavailable_discussion_count: int,
    request_error_count: int,
    *,
    source_name: str = SOURCE_NAME,
) -> dict[str, Any]:
    """Build pilot summary and recommendation."""
    usable_rows = sum(1 for row in rows if bool(row.title.strip() or row.body_or_excerpt.strip()))
    valid_candidate_count = sum(1 for row in rows if estimate_valid_candidate(row))
    persona_core_candidate_count = sum(1 for row in rows if estimate_persona_core_candidate(row))
    labelable_count = sum(1 for row in rows if estimate_labelable(row))
    safe_usable = max(usable_rows, 1)
    valid_ratio = valid_candidate_count / safe_usable
    labelable_ratio = labelable_count / safe_usable
    persona_fit_counts = {
        "persona_01_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_01_fit)),
        "persona_02_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_02_fit)),
        "persona_03_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_03_fit)),
        "persona_04_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_04_fit)),
        "persona_05_fit_count": sum(1 for row in rows if fit_is_meaningful(row.persona_05_fit)),
    }
    install_deploy_debug_noise_count = sum(1 for row in rows if row.install_deploy_debug_noise)
    auth_permission_setup_noise_count = sum(1 for row in rows if row.auth_permission_setup_noise)
    api_library_coding_noise_count = sum(1 for row in rows if row.api_library_coding_issue)
    generic_feature_request_noise_count = sum(1 for row in rows if row.generic_feature_request_without_user_pain)

    per_repo_summary: dict[str, dict[str, Any]] = {}
    for row in rows:
        entry = per_repo_summary.setdefault(
            row.repo,
            {
                "fetched_discussions": 0,
                "fetched_comments": 0,
                "valid_candidate_count": 0,
                "persona_core_candidate_count": 0,
                "developer_support_noise_count": 0,
                "persona_01_fit_count": 0,
                "persona_02_fit_count": 0,
                "persona_03_fit_count": 0,
                "persona_04_fit_count": 0,
                "persona_05_fit_count": 0,
            },
        )
        entry["fetched_discussions"] += 1
        entry["fetched_comments"] += int(row.comment_count)
        entry["valid_candidate_count"] += int(estimate_valid_candidate(row))
        entry["persona_core_candidate_count"] += int(estimate_persona_core_candidate(row))
        entry["developer_support_noise_count"] += int(
            row.install_deploy_debug_noise
            or row.auth_permission_setup_noise
            or row.api_library_coding_issue
            or row.ci_cd_infrastructure_issue
        )
        entry["persona_01_fit_count"] += int(fit_is_meaningful(row.persona_01_fit))
        entry["persona_02_fit_count"] += int(fit_is_meaningful(row.persona_02_fit))
        entry["persona_03_fit_count"] += int(fit_is_meaningful(row.persona_03_fit))
        entry["persona_04_fit_count"] += int(fit_is_meaningful(row.persona_04_fit))
        entry["persona_05_fit_count"] += int(fit_is_meaningful(row.persona_05_fit))
    for repo_name, entry in per_repo_summary.items():
        fetched = max(int(entry["fetched_discussions"]), 1)
        entry["valid_candidate_ratio"] = round(entry["valid_candidate_count"] / fetched, 4)
        entry["developer_support_noise_ratio"] = round(entry["developer_support_noise_count"] / fetched, 4)
        if entry["valid_candidate_ratio"] >= 0.35 and entry["developer_support_noise_ratio"] <= 0.35:
            entry["expected_source_tier"] = "supporting_validation_source_candidate"
            entry["estimated_weak_source_risk"] = "medium"
        elif entry["valid_candidate_ratio"] >= 0.2 and entry["developer_support_noise_ratio"] <= 0.4:
            entry["expected_source_tier"] = "low_value_supporting_source_candidate"
            entry["estimated_weak_source_risk"] = "medium"
        else:
            entry["expected_source_tier"] = "low_value_supporting_source_candidate"
            entry["estimated_weak_source_risk"] = "high"

    developer_noise_total = (
        install_deploy_debug_noise_count
        + auth_permission_setup_noise_count
        + api_library_coding_noise_count
    )
    if valid_ratio >= 0.45 and labelable_ratio >= 0.60 and developer_noise_total <= max(usable_rows * 0.35, 1):
        expected_source_tier = "strong_supporting_validation_source_candidate"
    elif valid_ratio >= 0.35:
        expected_source_tier = "supporting_validation_source_candidate"
    else:
        expected_source_tier = "low_value_supporting_source_candidate"

    if developer_noise_total > max(usable_rows * 0.5, 1) or valid_ratio < 0.25:
        weak_source_risk = "high"
    elif valid_ratio >= 0.35 and labelable_ratio >= 0.50:
        weak_source_risk = "medium"
    else:
        weak_source_risk = "low"

    gate_checks = {
        "enough_public_discussions_are_accessible": total_fetched_discussions >= 300,
        "valid_candidate_ratio_ge_0_35": valid_ratio >= 0.35,
        "estimated_labelable_ratio_ge_0_60": labelable_ratio >= 0.60,
        "estimated_persona_core_candidate_count_ge_120": persona_core_candidate_count >= 120,
        "two_personas_have_40_meaningful_rows": sum(
            int(persona_fit_counts[name] >= 40)
            for name in [
                "persona_01_fit_count",
                "persona_02_fit_count",
                "persona_03_fit_count",
                "persona_04_fit_count",
            ]
        ) >= 2,
        "developer_support_noise_not_dominant": developer_noise_total <= max(usable_rows * 0.45, 1),
        "estimated_weak_source_risk_not_high": weak_source_risk != "high",
    }
    gate_pass = all(gate_checks.values())

    one_bad_repo_dragging = any(
        entry["fetched_discussions"] >= 40 and entry["valid_candidate_ratio"] < 0.15 and entry["developer_support_noise_ratio"] > 0.4
        for entry in per_repo_summary.values()
    )

    single_repo_mode = len(per_repo_summary) == 1
    if single_repo_mode:
        meaningful_persona_count = sum(
            int(persona_fit_counts[name] >= 20)
            for name in [
                "persona_01_fit_count",
                "persona_02_fit_count",
                "persona_03_fit_count",
                "persona_04_fit_count",
            ]
        )
        materially_improved_quality = valid_ratio >= 0.20 and labelable_ratio >= 0.20
        if gate_pass and expected_source_tier == "strong_supporting_validation_source_candidate":
            decision = "promote_lightdash_to_active_source_candidate"
        elif total_fetched_discussions < 120 and materially_improved_quality:
            decision = "expand_lightdash_sample_if_more_discussions_available"
        elif materially_improved_quality and developer_noise_total <= max(usable_rows * 0.5, 1) and meaningful_persona_count >= 2:
            decision = "keep_lightdash_as_low_priority_supporting_source"
        else:
            decision = "fallback_to_stackoverflow_tag_expansion"
    else:
        if gate_pass and expected_source_tier == "strong_supporting_validation_source_candidate":
            decision = "promote_to_active_github_discussions_bundle"
        elif gate_pass:
            decision = "expand_repo_sample"
        elif one_bad_repo_dragging:
            decision = "narrow_repo_scope"
        elif total_fetched_discussions < 200:
            decision = "fallback_to_stackoverflow_tag_expansion"
        elif developer_noise_total > max(usable_rows * 0.45, 1):
            decision = "narrow_repo_scope"
        else:
            decision = "reject_github_discussions_expansion"

    failure_diagnosis: list[str] = []
    if not gate_checks["enough_public_discussions_are_accessible"]:
        failure_diagnosis.append("low_access_volume")
    if not gate_checks["developer_support_noise_not_dominant"]:
        failure_diagnosis.append("developer_support_noise")
    if one_bad_repo_dragging:
        failure_diagnosis.append("one_bad_repo_dragging_down_bundle")
    if not gate_checks["estimated_persona_core_candidate_count_ge_120"]:
        failure_diagnosis.append("weak_persona_core_fit")

    summary: dict[str, Any] = {
        "source": source_name,
        "total_discovered_discussions": total_discovered_discussions,
        "total_fetched_discussions": total_fetched_discussions,
        "total_fetched_comments": total_fetched_comments,
        "usable_rows": usable_rows,
        "valid_candidate_count": valid_candidate_count,
        "valid_candidate_ratio": round(valid_ratio, 4),
        "estimated_persona_core_candidate_count": persona_core_candidate_count,
        "estimated_labelable_ratio": round(labelable_ratio, 4),
        **persona_fit_counts,
        "install_deploy_debug_noise_count": install_deploy_debug_noise_count,
        "auth_permission_setup_noise_count": auth_permission_setup_noise_count,
        "api_library_coding_noise_count": api_library_coding_noise_count,
        "generic_feature_request_noise_count": generic_feature_request_noise_count,
        "expected_source_tier": expected_source_tier,
        "estimated_weak_source_risk": weak_source_risk,
        "unavailable_discussion_count": unavailable_discussion_count,
        "request_error_count": request_error_count,
        "per_repo_summary": per_repo_summary,
        "onboarding_gate_checks": gate_checks,
        "onboarding_gate_result": "pass" if gate_pass else "fail",
        "decision": decision,
    }
    if failure_diagnosis:
        summary["failure_diagnosis"] = failure_diagnosis
    if decision in {"promote_to_active_github_discussions_bundle", "promote_lightdash_to_active_source_candidate"}:
        summary["activation_plan"] = {
            "source_config_changes_needed": [
                "extend the existing github_discussions repository allowlist with the approved pilot repos",
                "keep the collector family unchanged",
            ],
            "seed_changes_needed": [
                "preserve the existing github discussions seed bank",
                "optionally tune query caps for the approved repo subset",
            ],
            "expected_row_counts": "target 500 to 1000 public discussions per bounded run across the approved bundle",
            "production_acceptance_criteria": [
                "follow-up run still clears valid candidate and labelable thresholds",
                "developer-support noise remains below dominance thresholds",
                "bundle remains outside weak-source cost-center behavior",
            ],
            "rollback_criteria": [
                "developer-support noise spikes in production-like runs",
                "public access structure changes materially",
                "follow-up run fails relevance or persona-core thresholds",
            ],
            "tests_needed": [
                "bundle config load regression",
                "listing/detail parser regression",
                "noise scoring regression",
                "production config immutability regression",
            ],
        }
    return summary


def _fetch_allowed(url: str, timeout_seconds: int) -> tuple[bool, str]:
    """Check robots allowance for one public GitHub page."""
    allowed, reason = check_robots_allowed(url, user_agent=USER_AGENT)
    return allowed, reason


def run_github_discussions_expansion_pilot(
    root_dir: Path,
    *,
    seed_path: Path | None = None,
    spec_path: Path | None = None,
) -> dict[str, Path]:
    """Run the bounded GitHub Discussions expansion pilot."""
    resolved_seed_path = seed_path or DEFAULT_SEED_PATH
    config = load_pilot_seed_config(root_dir, resolved_seed_path)
    _ = load_pilot_spec(root_dir, spec_path)
    source_name = str(config.get("bundle_name", SOURCE_NAME) or SOURCE_NAME)
    artifact_dir = ensure_dir(root_dir / str(config["pilot_output_dir"]))
    raw_path = artifact_dir / str(config["pilot_output_files"]["raw_jsonl"])
    sample_path = artifact_dir / str(config["pilot_output_files"]["sample_csv"])
    summary_path = artifact_dir / str(config["pilot_output_files"]["summary_json"])
    for path in [raw_path, sample_path, summary_path]:
        if not is_pilot_output_path(path, root_dir):
            raise ValueError(f"Pilot output path must stay under artifacts/source_pilots: {path}")

    fetched_at = datetime.now(UTC).isoformat()
    rows: list[GithubDiscussionPilotRow] = []
    seen_urls: set[str] = set()
    total_discovered_discussions = 0
    total_fetched_discussions = 0
    total_fetched_comments = 0
    unavailable_discussion_count = 0
    request_error_count = 0

    for repo_name in config.get("target_repos", []):
        listing_queue = [f"{BASE_URL}/{repo_name}/discussions"]
        seen_listing_urls: set[str] = set()
        repo_discussion_count = 0
        while listing_queue and len(seen_listing_urls) < int(config.get("max_listing_pages_per_repo", 10)):
            listing_url = normalize_url(listing_queue.pop(0))
            if listing_url in seen_listing_urls:
                continue
            seen_listing_urls.add(listing_url)
            allowed, reason = _fetch_allowed(listing_url, int(config.get("timeout_seconds", 20)))
            if not allowed:
                request_error_count += 1
                break
            response = fetch_text(listing_url, user_agent=USER_AGENT, timeout_seconds=int(config.get("timeout_seconds", 20)))
            if not response.ok:
                request_error_count += 1
                break
            discussion_urls = extract_discussion_urls(response.body_text, repo_name)
            total_discovered_discussions += len(discussion_urls)
            for discussion_url in discussion_urls:
                if discussion_url in seen_urls:
                    continue
                seen_urls.add(discussion_url)
                allowed_detail, _ = _fetch_allowed(discussion_url, int(config.get("timeout_seconds", 20)))
                if not allowed_detail:
                    unavailable_discussion_count += 1
                    continue
                detail_response = fetch_text(
                    discussion_url,
                    user_agent=USER_AGENT,
                    timeout_seconds=int(config.get("timeout_seconds", 20)),
                )
                if not detail_response.ok:
                    unavailable_discussion_count += 1
                    continue
                parsed = parse_discussion_detail_html(
                    discussion_url,
                    repo_name=repo_name,
                    html_text=detail_response.body_text,
                    comment_cap=int(config.get("comment_cap", 10)),
                )
                row = build_pilot_row(
                    parsed=parsed,
                    repo_name=repo_name,
                    url=discussion_url,
                    fetch_method=str(config.get("fetch_method", "public_html")),
                    fetched_at=fetched_at,
                    source_name=source_name,
                    fetch_status="ok",
                )
                rows.append(row)
                total_fetched_discussions += 1
                total_fetched_comments += row.comment_count
                repo_discussion_count += 1
                if repo_discussion_count >= int(config.get("thread_limit_per_repo", 150)):
                    break
                time.sleep(float(config.get("request_delay_seconds", 0.75)))
            if repo_discussion_count >= int(config.get("thread_limit_per_repo", 150)):
                break
            for next_url in extract_listing_urls(response.body_text, repo_name):
                normalized = normalize_url(next_url)
                if normalized not in seen_listing_urls and normalized not in listing_queue:
                    listing_queue.append(normalized)
            time.sleep(float(config.get("request_delay_seconds", 0.75)))

    write_jsonl(raw_path, [row.to_dict() for row in rows])
    pd.DataFrame([row.to_dict() for row in rows]).to_csv(sample_path, index=False)
    summary = build_summary(
        rows=rows,
        total_discovered_discussions=total_discovered_discussions,
        total_fetched_discussions=total_fetched_discussions,
        total_fetched_comments=total_fetched_comments,
        unavailable_discussion_count=unavailable_discussion_count,
        request_error_count=request_error_count,
        source_name=source_name,
    )
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "pilot_config": root_dir / resolved_seed_path,
        "raw_jsonl": raw_path,
        "sample_csv": sample_path,
        "summary_json": summary_path,
    }
