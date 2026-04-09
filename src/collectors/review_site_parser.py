"""Parsers for review-site HTML, CSV, and JSON snapshots."""

from __future__ import annotations

import csv
import json
from pathlib import Path
import re
from typing import Any

try:
    from bs4 import BeautifulSoup, Tag
except ImportError:  # pragma: no cover - exercised in environments without bs4
    BeautifulSoup = None
    Tag = Any

from src.utils.text import clean_text

REVIEW_PARSE_VERSION = "review_v1"
REVIEW_SECTION_LABELS = {
    "role_hint": ["role", "job title", "title"],
    "company_size_hint": ["company size", "employee range", "employees"],
    "industry_hint": ["industry"],
    "workflow_hint": ["use case", "primary use case", "workflow", "used for"],
    "pain_point_hint": ["problems solved", "cons", "pain points", "challenges"],
    "output_need_hint": ["outcome", "results", "benefits", "reporting need"],
}


def parse_review_snapshot(
    file_path: Path,
    source_name: str,
    product_or_tool: str = "",
) -> list[dict[str, Any]]:
    """Parse one review snapshot file into structured review rows."""
    suffix = file_path.suffix.lower()
    if suffix in {".html", ".htm"}:
        return parse_review_html(
            file_path.read_text(encoding="utf-8"),
            source_name=source_name,
            canonical_url="",
            raw_file_path=str(file_path),
            product_or_tool=product_or_tool,
        )
    if suffix == ".json":
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [normalize_review_row(item, raw_file_path=str(file_path), source_name=source_name) for item in payload]
        if isinstance(payload, dict):
            if isinstance(payload.get("reviews"), list):
                return [
                    normalize_review_row(item, raw_file_path=str(file_path), source_name=source_name)
                    for item in payload.get("reviews", [])
                ]
            return [normalize_review_row(payload, raw_file_path=str(file_path), source_name=source_name)]
    if suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        for line in file_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                rows.append(normalize_review_row(json.loads(line), raw_file_path=str(file_path), source_name=source_name))
        return rows
    if suffix == ".csv":
        with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            return [normalize_review_row(dict(row), raw_file_path=str(file_path), source_name=source_name) for row in reader]
    return []


def parse_review_html(
    html_text: str,
    source_name: str,
    canonical_url: str,
    raw_file_path: str,
    product_or_tool: str = "",
) -> list[dict[str, Any]]:
    """Extract review rows from public or manually saved HTML."""
    if BeautifulSoup is None:
        return _parse_review_html_without_bs4(
            html_text=html_text,
            source_name=source_name,
            canonical_url=canonical_url,
            raw_file_path=raw_file_path,
            product_or_tool=product_or_tool,
        )

    soup = BeautifulSoup(html_text, "html.parser")
    for selector in ["script", "style", "nav", "header", "footer", "form", "button", "svg"]:
        for node in soup.select(selector):
            node.decompose()

    containers = _review_containers(soup)
    if not containers:
        containers = [soup]

    rows: list[dict[str, Any]] = []
    for index, container in enumerate(containers, start=1):
        title = _extract_first_text(container, ["h1", "h2", "h3", ".review-title", "[data-review-title]"])
        body = _extract_body_text(container)
        if len(body) < 40:
            continue
        rows.append(
            {
                "raw_source_id": f"{source_name}-review-{index}",
                "canonical_url": canonical_url,
                "title": title or _extract_first_text(soup, ["title"]) or f"{source_name} review",
                "body_text": body,
                "author_name": _extract_first_text(container, [".author", ".reviewer", "[data-reviewer-name]"]),
                "product_or_tool": product_or_tool or _extract_labeled_value(container, ["product", "product name"]),
                "role_hint": _extract_labeled_value(container, REVIEW_SECTION_LABELS["role_hint"]),
                "company_size_hint": _extract_labeled_value(container, REVIEW_SECTION_LABELS["company_size_hint"]),
                "industry_hint": _extract_labeled_value(container, REVIEW_SECTION_LABELS["industry_hint"]),
                "workflow_hint": _extract_labeled_value(container, REVIEW_SECTION_LABELS["workflow_hint"]),
                "pain_point_hint": _extract_labeled_value(container, REVIEW_SECTION_LABELS["pain_point_hint"]),
                "output_need_hint": _extract_labeled_value(container, REVIEW_SECTION_LABELS["output_need_hint"]),
                "parent_context": _extract_labeled_value(container, ["pros", "cons", "use case", "problems solved"]),
                "source_meta": {
                    "rating": _extract_rating(container),
                    "verified_marker": _extract_verified_flag(container),
                },
                "parse_version": REVIEW_PARSE_VERSION,
                "raw_file_path": raw_file_path,
                "source_name": source_name,
            }
        )
    return rows


def normalize_review_row(row: dict[str, Any], raw_file_path: str, source_name: str) -> dict[str, Any]:
    """Normalize review rows imported from CSV or JSON snapshots."""
    return {
        "raw_source_id": str(row.get("raw_source_id", row.get("id", "")) or ""),
        "canonical_url": str(row.get("canonical_url", row.get("url", "")) or ""),
        "title": clean_text(row.get("title") or row.get("thread_title") or ""),
        "body_text": clean_text(row.get("body_text") or row.get("body") or row.get("review_body") or ""),
        "author_name": clean_text(row.get("author_name") or row.get("reviewer_name") or row.get("author") or ""),
        "product_or_tool": clean_text(row.get("product_or_tool") or row.get("product") or ""),
        "role_hint": clean_text(row.get("role_hint") or row.get("reviewer_role") or row.get("role") or ""),
        "company_size_hint": clean_text(row.get("company_size_hint") or row.get("company_size") or ""),
        "industry_hint": clean_text(row.get("industry_hint") or row.get("industry") or ""),
        "workflow_hint": clean_text(row.get("workflow_hint") or row.get("use_case") or ""),
        "pain_point_hint": clean_text(row.get("pain_point_hint") or row.get("problems_solved") or row.get("cons") or ""),
        "output_need_hint": clean_text(row.get("output_need_hint") or row.get("outcome") or row.get("benefits") or ""),
        "parent_context": clean_text(row.get("parent_context") or row.get("pros") or ""),
        "source_meta": {
            "rating": row.get("rating", ""),
            "verified_marker": row.get("verified_marker", ""),
        },
        "parse_version": REVIEW_PARSE_VERSION,
        "raw_file_path": raw_file_path,
        "source_name": source_name,
    }


def _review_containers(soup: BeautifulSoup) -> list[Tag]:
    """Return likely review content blocks."""
    selectors = [
        "[data-review-id]",
        ".review-card",
        ".review-item",
        ".review",
        "article",
    ]
    containers: list[Tag] = []
    for selector in selectors:
        containers.extend([node for node in soup.select(selector) if isinstance(node, Tag)])
        if containers:
            return containers
    return containers


def _extract_first_text(node: Tag | BeautifulSoup, selectors: list[str]) -> str:
    """Return the first non-empty selector text."""
    for selector in selectors:
        match = node.select_one(selector)
        if match:
            text = clean_text(match.get_text(" ", strip=True))
            if text:
                return text
    return ""


def _extract_body_text(container: Tag | BeautifulSoup) -> str:
    """Extract dense body text from a review block."""
    body_selectors = [".review-body", ".content", ".review-content", "[data-review-body]", "p", "li"]
    parts: list[str] = []
    for selector in body_selectors:
        for node in container.select(selector):
            text = clean_text(node.get_text(" ", strip=True))
            if text and text not in parts:
                parts.append(text)
    if not parts:
        return clean_text(container.get_text(" ", strip=True))
    return clean_text(" ".join(parts))


def _extract_labeled_value(container: Tag | BeautifulSoup, labels: list[str]) -> str:
    """Extract a value from label:value text patterns."""
    lines = [clean_text(text) for text in container.stripped_strings]
    lowered_labels = [label.lower() for label in labels]
    for line in lines:
        lowered = line.lower()
        for label in lowered_labels:
            prefix = f"{label}:"
            if lowered.startswith(prefix):
                return clean_text(line[len(prefix):])
    return ""


def _extract_rating(container: Tag | BeautifulSoup) -> str:
    """Extract a simple rating marker when present."""
    text = container.get_text(" ", strip=True)
    for token in ["5/5", "4/5", "4.5/5", "3/5"]:
        if token in text:
            return token
    return ""


def _extract_verified_flag(container: Tag | BeautifulSoup) -> str:
    """Extract a verified marker when present."""
    text = container.get_text(" ", strip=True).lower()
    return "verified" if "verified" in text else ""


def _parse_review_html_without_bs4(
    html_text: str,
    source_name: str,
    canonical_url: str,
    raw_file_path: str,
    product_or_tool: str,
) -> list[dict[str, Any]]:
    """Fallback parser for environments without BeautifulSoup."""
    text = re.sub(r"(?is)<(script|style|nav|header|footer|form|button|svg).*?>.*?</\1>", " ", html_text)
    text = re.sub(r"(?i)</(p|div|h1|h2|h3|li|article)>", "\n", text)
    plain = clean_text(re.sub(r"(?is)<[^>]+>", " ", text))
    lines = [clean_text(line) for line in re.sub(r"(?is)<[^>]+>", "\n", text).splitlines() if clean_text(line)]
    if len(plain) < 40:
        return []
    title_match = re.search(r"(?is)<h[1-3][^>]*>(.*?)</h[1-3]>", html_text)
    title = clean_text(title_match.group(1) if title_match else "")
    return [
        {
            "raw_source_id": f"{source_name}-review-1",
            "canonical_url": canonical_url,
            "title": title or f"{source_name} review",
            "body_text": plain,
            "author_name": _regex_after_label(lines, "reviewer") or _regex_after_label(lines, "author"),
            "product_or_tool": product_or_tool,
            "role_hint": _regex_after_label(lines, "role"),
            "company_size_hint": _regex_after_label(lines, "company size"),
            "industry_hint": _regex_after_label(lines, "industry"),
            "workflow_hint": _regex_after_label(lines, "use case"),
            "pain_point_hint": _regex_after_label(lines, "problems solved"),
            "output_need_hint": _regex_after_label(lines, "benefits"),
            "parent_context": _regex_after_label(lines, "pros"),
            "source_meta": {
                "rating": "4/5" if "4/5" in plain else "",
                "verified_marker": "verified" if "verified" in plain.lower() else "",
            },
            "parse_version": REVIEW_PARSE_VERSION,
            "raw_file_path": raw_file_path,
            "source_name": source_name,
        }
    ]


def _regex_after_label(lines: list[str], label: str) -> str:
    """Return a short value after a label in line-oriented plain text."""
    prefix = f"{label.lower()}:"
    for line in lines:
        lowered = line.lower()
        if lowered.startswith(prefix):
            return clean_text(line[len(prefix):])
    return ""
