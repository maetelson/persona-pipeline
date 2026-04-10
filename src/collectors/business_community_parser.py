"""HTML parsing helpers for public business community pages."""

from __future__ import annotations

from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
import json
import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
import xml.etree.ElementTree as ET

from src.utils.text import clean_text, make_hash_id


@dataclass(slots=True)
class ThreadLink:
    """One discovered public thread link."""

    url: str
    title: str
    board: str = ""
    snippet: str = ""
    reply_count: int | None = None
    activity_date: str = ""


@dataclass(slots=True)
class ParsedThread:
    """Parsed public thread data."""

    raw_id: str
    canonical_url: str
    title: str
    body_text: str
    board: str
    author_name: str
    published_at: str
    reply_count: int | None
    parse_status: str
    source_meta: dict[str, Any]


class _AnchorParser(HTMLParser):
    """Collect anchor tags with their visible text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        values = {key.lower(): value or "" for key, value in attrs}
        self._href = values.get("href", "")
        self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._href is None:
            return
        text = clean_text(" ".join(self._text))
        if self._href and text:
            self.links.append((self._href, text))
        self._href = None
        self._text = []


class _TextParser(HTMLParser):
    """Extract rough visible text and selected metadata from HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.visible_parts: list[str] = []
        self.meta: dict[str, str] = {}
        self.json_ld: list[dict[str, Any]] = []
        self._capture_title = False
        self._capture_script = False
        self._script_type = ""
        self._script_parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        values = {key.lower(): value or "" for key, value in attrs}
        if tag in {"script", "style", "noscript", "svg"}:
            self._skip_depth += 1
            if tag == "script" and "ld+json" in values.get("type", ""):
                self._capture_script = True
                self._script_type = values.get("type", "")
                self._script_parts = []
            return
        if tag == "title":
            self._capture_title = True
        if tag == "meta":
            key = values.get("property") or values.get("name")
            content = values.get("content", "")
            if key and content:
                self.meta[key.lower()] = clean_text(unescape(content))

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "title":
            self._capture_title = False
        if tag in {"script", "style", "noscript", "svg"} and self._skip_depth:
            if self._capture_script and tag == "script":
                self._store_json_ld()
            self._capture_script = False
            self._script_type = ""
            self._script_parts = []
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._capture_script:
            self._script_parts.append(data)
            return
        if self._skip_depth:
            return
        text = clean_text(data)
        if not text:
            return
        if self._capture_title:
            self.title_parts.append(text)
        elif len(text) > 2:
            self.visible_parts.append(text)

    def _store_json_ld(self) -> None:
        if "ld+json" not in self._script_type:
            return
        text = "\n".join(self._script_parts).strip()
        if not text:
            return
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return
        if isinstance(payload, list):
            self.json_ld.extend(item for item in payload if isinstance(item, dict))
        elif isinstance(payload, dict):
            self.json_ld.append(payload)


def canonicalize_business_url(url: str, base_url: str, platform: str) -> str:
    """Normalize public community thread URLs for dedupe-safe ingestion."""
    absolute = urljoin(base_url, unescape(url or ""))
    parsed = urlparse(absolute)
    scheme = "https"
    netloc = parsed.netloc.lower()
    path = re.sub(r"/+", "/", parsed.path).rstrip("/")
    if platform == "hubspot" and path.endswith("/page/1"):
        path = path.removesuffix("/page/1")
    if platform == "shopify":
        match = re.search(r"(/t/[^/?#]+/\d+)", path)
        if match:
            path = match.group(1)
    allowed_query_keys: set[str] = set()
    query = urlencode(
        [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=False) if key in allowed_query_keys]
    )
    return urlunparse((scheme, netloc, path or "/", "", query, ""))


def discover_thread_links(html: str, base_url: str, platform: str, board: str = "") -> list[ThreadLink]:
    """Extract likely public thread URLs from a listing/category page."""
    parser = _AnchorParser()
    parser.feed(html)
    discovered: dict[str, ThreadLink] = {}
    for href, text in parser.links:
        canonical = canonicalize_business_url(href, base_url, platform)
        if not _looks_like_thread_url(canonical, platform):
            continue
        title = _clean_title(text, platform)
        if len(title.split()) < 2:
            continue
        if canonical not in discovered:
            discovered[canonical] = ThreadLink(url=canonical, title=title, board=board)
    return list(discovered.values())


def discover_rss_thread_links(xml_text: str, base_url: str, platform: str, board: str = "") -> list[ThreadLink]:
    """Extract thread links from RSS/Atom feeds for incremental discovery."""
    discovered: dict[str, ThreadLink] = {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    for item in root.findall(".//item"):
        title = clean_text(item.findtext("title") or "")
        href = clean_text(item.findtext("link") or "")
        snippet = clean_text(item.findtext("description") or "")
        activity_date = clean_text(item.findtext("pubDate") or "")
        if not href or not title:
            continue
        canonical = canonicalize_business_url(href, base_url, platform)
        if not _looks_like_thread_url(canonical, platform):
            continue
        discovered[canonical] = ThreadLink(
            url=canonical,
            title=_clean_title(title, platform),
            board=board,
            snippet=snippet,
            activity_date=activity_date,
        )
    for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = clean_text(entry.findtext("{http://www.w3.org/2005/Atom}title") or "")
        link = entry.find("{http://www.w3.org/2005/Atom}link")
        href = clean_text(link.attrib.get("href", "") if link is not None else "")
        snippet = clean_text(entry.findtext("{http://www.w3.org/2005/Atom}summary") or "")
        activity_date = clean_text(entry.findtext("{http://www.w3.org/2005/Atom}updated") or "")
        if not href or not title:
            continue
        canonical = canonicalize_business_url(href, base_url, platform)
        if not _looks_like_thread_url(canonical, platform):
            continue
        discovered[canonical] = ThreadLink(
            url=canonical,
            title=_clean_title(title, platform),
            board=board,
            snippet=snippet,
            activity_date=activity_date,
        )
    return list(discovered.values())


def parse_thread_page(
    html: str,
    url: str,
    platform: str,
    fallback: ThreadLink | None = None,
    product_or_tool: str = "",
) -> ParsedThread:
    """Parse a public community thread page into source-neutral fields."""
    parser = _TextParser()
    parser.feed(html)
    json_data = _flatten_json_ld(parser.json_ld)
    fallback_title = fallback.title if fallback else ""
    canonical = canonicalize_business_url(
        parser.meta.get("og:url") or url,
        base_url=url,
        platform=platform,
    )
    title = _clean_title(
        json_data.get("headline")
        or json_data.get("name")
        or parser.meta.get("og:title")
        or parser.meta.get("twitter:title")
        or " ".join(parser.title_parts)
        or fallback_title,
        platform,
    )
    description = clean_text(
        json_data.get("text")
        or json_data.get("articlebody")
        or parser.meta.get("og:description")
        or parser.meta.get("description")
        or (fallback.snippet if fallback else "")
    )
    visible_text = _candidate_visible_text(parser.visible_parts, title)
    google_support_text = _extract_google_support_thread_text(html, title) if platform == "google_support" else ""
    body = google_support_text or description or visible_text
    board = clean_text(
        json_data.get("articleSection")
        or json_data.get("discussionCategory")
        or (fallback.board if fallback else "")
    )
    author = _extract_author(json_data)
    published = clean_text(
        json_data.get("datePublished")
        or json_data.get("dateCreated")
        or json_data.get("dateModified")
        or parser.meta.get("article:published_time")
        or parser.meta.get("article:modified_time")
        or _extract_date(parser.visible_parts)
        or (fallback.activity_date if fallback else "")
    )
    if not author:
        author = clean_text(
            parser.meta.get("author")
            or parser.meta.get("twitter:data1")
            or ""
        )
    if not board:
        board = clean_text(parser.meta.get("article:section") or "")
    reply_count = _first_int(
        json_data.get("commentCount"),
        json_data.get("answerCount"),
        fallback.reply_count if fallback else None,
        _extract_reply_count(parser.visible_parts),
    )
    parse_status = "ok" if title and body else "ok_listing_only" if title and fallback else "parse_empty"
    if parse_status == "ok_listing_only":
        body = fallback.snippet or fallback.title
    return ParsedThread(
        raw_id=_raw_id_from_url(canonical),
        canonical_url=canonical,
        title=title,
        body_text=body,
        board=board,
        author_name=author,
        published_at=published,
        reply_count=reply_count,
        parse_status=parse_status,
        source_meta={
            "platform": platform,
            "product_or_tool": product_or_tool,
            "meta": parser.meta,
            "json_ld_keys": sorted(json_data.keys()),
            "fallback_title": fallback_title,
        },
    )


def _looks_like_thread_url(url: str, platform: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path
    if platform == "shopify":
        return parsed.netloc.endswith("community.shopify.com") and re.search(r"/t/[^/]+/\d+$", path) is not None
    if platform == "hubspot":
        return parsed.netloc.endswith("community.hubspot.com") and "/td-p/" in path
    if platform == "klaviyo":
        return (
            parsed.netloc.endswith("community.klaviyo.com")
            and path.count("/") >= 2
            and re.search(r"/[^/]+-\d+$", path) is not None
        )
    if platform == "google_support":
        return (
            parsed.netloc.endswith("support.google.com")
            and re.search(r"/(?:google-ads|merchants)/thread/\d+/[^/]+$", path) is not None
        )
    return False


def _clean_title(value: str, platform: str) -> str:
    title = clean_text(unescape(str(value or "")))
    replacements = [
        " | Shopify Community",
        " - Shopify Community",
        " - HubSpot Community",
        " | Klaviyo Community",
        " - Klaviyo Community",
        " - Google Ads Community",
        " - Merchant Center Community",
    ]
    for replacement in replacements:
        title = title.replace(replacement, "")
    if platform == "hubspot":
        title = re.sub(r"\s+-\s+HubSpot Community\s*$", "", title)
    if platform == "google_support":
        title = re.split(r"\s+\d+\s+Recommended Answers?\b", title, maxsplit=1)[0]
        title = re.split(r"\s+\d+\s+Replies?\b", title, maxsplit=1)[0]
        title = re.split(r"\s+\d+\s+Upvotes?\b", title, maxsplit=1)[0]
    return clean_text(title)


def _candidate_visible_text(parts: list[str], title: str) -> str:
    seen: list[str] = []
    for part in parts:
        text = clean_text(part)
        if not text or text == title or text in seen:
            continue
        if any(skip in text.lower() for skip in ["skip to main content", "sign in", "turn on suggestions"]):
            continue
        if len(text) >= 40:
            seen.append(text)
        if len(" ".join(seen)) >= 1200:
            break
    return clean_text(" ".join(seen[:8]))


def _extract_google_support_thread_text(html: str, title: str) -> str:
    """Extract thread body text from Google Support's escaped page state."""
    if not title:
        return ""
    title_index = html.find(_google_support_escaped_string(title))
    if title_index < 0:
        title_index = html.find(title)
    if title_index < 0:
        return ""
    segment = html[title_index : title_index + 12000]
    strings = re.findall(r"\\x22((?:[^\\]|\\(?!x22)|\\x(?!22))*?)\\x22", segment)
    candidates: list[str] = []
    for raw_value in strings:
        value = _decode_google_support_string(raw_value)
        if not value or value == title:
            continue
        if len(value) < 80:
            continue
        if _looks_like_google_support_noise(value):
            continue
        candidates.append(value)
    return clean_text(" ".join(candidates[:3]))


def _google_support_escaped_string(value: str) -> str:
    """Return the escaped string wrapper used in Google Support page state."""
    return "\\x22" + value.replace("\\", "\\\\").replace('"', '\\"') + "\\x22"


def _decode_google_support_string(value: str) -> str:
    """Decode one Google Support escaped string and strip embedded HTML."""
    try:
        decoded = bytes(value, "utf-8").decode("unicode_escape", errors="ignore")
    except UnicodeDecodeError:
        decoded = value
    for escaped, replacement in {
        "\\u003c": "<",
        "\\u003e": ">",
        "\\u0026": "&",
        "\\u003d": "=",
        "\\u0027": "'",
        "\\n": " ",
    }.items():
        decoded = decoded.replace(escaped, replacement)
    decoded = decoded.replace("\\/", "/")
    decoded = re.sub(r"<br\\s*/?>", " ", decoded, flags=re.IGNORECASE)
    decoded = re.sub(r"</(?:div|p|li|ul|ol)>", " ", decoded, flags=re.IGNORECASE)
    decoded = re.sub(r"<[^>]+>", " ", decoded)
    return clean_text(unescape(decoded))


def _looks_like_google_support_noise(value: str) -> bool:
    """Return whether an escaped string is UI/chrome rather than thread evidence."""
    lowered = value.lower()
    noisy_phrases = [
        "recommended answer",
        "google product expert",
        "notification preferences",
        "community content may not be verified",
        "please follow our community policy",
        "productexperts.withgoogle.com",
        "youtube.com",
    ]
    return any(phrase in lowered for phrase in noisy_phrases)


def _flatten_json_ld(items: list[dict[str, Any]]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    stack = list(items)
    while stack:
        item = stack.pop(0)
        graph = item.get("@graph")
        if isinstance(graph, list):
            stack.extend(node for node in graph if isinstance(node, dict))
        item_type = str(item.get("@type", "")).lower()
        if any(token in item_type for token in ["discussion", "question", "article", "posting"]):
            for key, value in item.items():
                if key not in flattened and isinstance(value, (str, int, float, dict, list)):
                    flattened[key] = value
    if not flattened and items:
        for key, value in items[0].items():
            if isinstance(value, (str, int, float, dict, list)):
                flattened[key] = value
    return flattened


def _extract_author(payload: dict[str, Any]) -> str:
    author = payload.get("author")
    if isinstance(author, dict):
        return clean_text(str(author.get("name") or author.get("url") or ""))
    if isinstance(author, list) and author and isinstance(author[0], dict):
        return clean_text(str(author[0].get("name") or ""))
    return clean_text(str(author or ""))


def _extract_date(parts: list[str]) -> str:
    text = " ".join(parts)
    match = re.search(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}\b", text)
    return match.group(0) if match else ""


def _extract_reply_count(parts: list[str]) -> int | None:
    text = " ".join(parts).lower()
    match = re.search(r"(\d+)\s+(?:replies|reply|comments|comment|answers|answer)\b", text)
    return int(match.group(1)) if match else None


def _first_int(*values: object) -> int | None:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _raw_id_from_url(url: str) -> str:
    support_match = re.search(r"/thread/(\d+)(?:/|$)", urlparse(url).path)
    if support_match:
        return support_match.group(1)
    match = re.search(r"/(\d+)$", urlparse(url).path)
    if match:
        return match.group(1)
    return make_hash_id(url)
