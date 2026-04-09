"""Parsers for official BI community feeds and HTML thread snapshots."""

from __future__ import annotations

from datetime import UTC, datetime
import re
from typing import Any
import xml.etree.ElementTree as ET

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - exercised in environments without bs4
    BeautifulSoup = None

from src.utils.text import clean_text

OFFICIAL_COMMUNITY_PARSE_VERSION = "official_community_v1"


def parse_feed_entries(xml_text: str) -> list[dict[str, Any]]:
    """Parse RSS or Atom feed items into shallow thread rows."""
    root = ET.fromstring(xml_text)
    rows: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        rows.append(
            {
                "title": clean_text(item.findtext("title", default="")),
                "canonical_url": clean_text(item.findtext("link", default="")),
                "body_text": clean_text(item.findtext("description", default="")),
                "created_at": _normalize_feed_date(item.findtext("pubDate", default="")),
                "thread_title": clean_text(item.findtext("title", default="")),
                "subreddit_or_forum": "",
                "parent_context": "",
                "parse_version": OFFICIAL_COMMUNITY_PARSE_VERSION,
            }
        )
    for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = clean_text(entry.findtext("{http://www.w3.org/2005/Atom}title", default=""))
        body = clean_text(entry.findtext("{http://www.w3.org/2005/Atom}summary", default=""))
        link_node = entry.find("{http://www.w3.org/2005/Atom}link")
        rows.append(
            {
                "title": title,
                "canonical_url": str(link_node.attrib.get("href", "") if link_node is not None else ""),
                "body_text": body,
                "created_at": _normalize_feed_date(entry.findtext("{http://www.w3.org/2005/Atom}updated", default="")),
                "thread_title": title,
                "subreddit_or_forum": "",
                "parent_context": "",
                "parse_version": OFFICIAL_COMMUNITY_PARSE_VERSION,
            }
        )
    return rows


def parse_official_community_html(
    html_text: str,
    forum_name: str,
    product_or_tool: str,
    canonical_url: str,
    raw_file_path: str = "",
) -> list[dict[str, Any]]:
    """Parse official forum thread HTML into one thread row plus reply rows."""
    if BeautifulSoup is None:
        return _parse_official_community_html_without_bs4(
            html_text=html_text,
            forum_name=forum_name,
            product_or_tool=product_or_tool,
            canonical_url=canonical_url,
            raw_file_path=raw_file_path,
        )

    soup = BeautifulSoup(html_text, "html.parser")
    for selector in ["script", "style", "nav", "header", "footer", "form", "button", "svg"]:
        for node in soup.select(selector):
            node.decompose()

    title = clean_text(_first_text(soup, ["h1", ".lia-message-subject", ".topic-title", "title"]))
    body = clean_text(_first_text(soup, [".lia-message-body-content", ".message-body", ".topic-body", "article", "main"]))
    category = clean_text(_first_text(soup, [".lia-link-navigation", ".board-name", ".topic-category", ".breadcrumbs"]))
    accepted_solution = "accepted_solution" if "accepted solution" in soup.get_text(" ", strip=True).lower() else ""
    tags = [clean_text(tag.get_text(" ", strip=True)) for tag in soup.select(".lia-tag, .tag, .topic-tag") if clean_text(tag.get_text(" ", strip=True))]

    rows = [
        {
            "raw_source_id": f"{forum_name}-thread",
            "title": title,
            "body_text": body,
            "canonical_url": canonical_url,
            "thread_title": title,
            "subreddit_or_forum": category or forum_name,
            "parent_context": accepted_solution,
            "product_or_tool": product_or_tool,
            "source_meta": {"tags": tags, "accepted_solution": accepted_solution},
            "raw_file_path": raw_file_path,
            "parse_version": OFFICIAL_COMMUNITY_PARSE_VERSION,
        }
    ]

    for index, node in enumerate(soup.select(".lia-message-body-content, .reply-content, .message-body"), start=1):
        reply_text = clean_text(node.get_text(" ", strip=True))
        if not reply_text or reply_text == body:
            continue
        rows.append(
            {
                "raw_source_id": f"{forum_name}-reply-{index}",
                "title": f"Reply {index} on {title}",
                "body_text": reply_text,
                "canonical_url": canonical_url,
                "thread_title": title,
                "subreddit_or_forum": category or forum_name,
                "parent_context": title,
                "product_or_tool": product_or_tool,
                "source_type": "reply",
                "source_meta": {"tags": tags, "accepted_solution": accepted_solution},
                "raw_file_path": raw_file_path,
                "parse_version": OFFICIAL_COMMUNITY_PARSE_VERSION,
            }
        )
    return rows


def _first_text(soup: BeautifulSoup, selectors: list[str]) -> str:
    """Return the first selector text from the soup."""
    for selector in selectors:
        match = soup.select_one(selector)
        if match:
            text = clean_text(match.get_text(" ", strip=True))
            if text:
                return text
    return ""


def _normalize_feed_date(raw_value: str) -> str:
    """Normalize feed dates conservatively when possible."""
    value = clean_text(raw_value)
    if not value:
        return ""
    if "T" in value:
        return value
    for pattern in ["%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%d %H:%M:%S"]:
        try:
            parsed = datetime.strptime(value, pattern)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC).replace(microsecond=0).isoformat()
        except ValueError:
            continue
    return value


def _parse_official_community_html_without_bs4(
    html_text: str,
    forum_name: str,
    product_or_tool: str,
    canonical_url: str,
    raw_file_path: str,
) -> list[dict[str, Any]]:
    """Fallback parser for environments without BeautifulSoup."""
    plain = clean_text(re.sub(r"(?is)<[^>]+>", " ", html_text))
    title_match = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", html_text)
    title = clean_text(title_match.group(1) if title_match else forum_name)
    accepted_solution = "accepted_solution" if "accepted solution" in plain.lower() else ""
    rows = [
        {
            "raw_source_id": f"{forum_name}-thread",
            "title": title,
            "body_text": plain,
            "canonical_url": canonical_url,
            "thread_title": title,
            "subreddit_or_forum": forum_name,
            "parent_context": accepted_solution,
            "product_or_tool": product_or_tool,
            "source_meta": {"tags": [], "accepted_solution": accepted_solution},
            "raw_file_path": raw_file_path,
            "parse_version": OFFICIAL_COMMUNITY_PARSE_VERSION,
        }
    ]
    return rows
