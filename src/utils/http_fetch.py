"""Robots-aware fetch helpers for safe public web ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser


@dataclass(slots=True)
class FetchResponse:
    """Result of one attempted public fetch."""

    url: str
    ok: bool
    status_code: int
    body_text: str
    content_type: str
    crawl_status: str
    error_message: str = ""


def check_robots_allowed(url: str, user_agent: str) -> tuple[bool, str]:
    """Return whether robots.txt allows fetching the URL."""
    parsed = urlparse(url)
    robots_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", "/robots.txt")
    try:
        request = Request(robots_url, headers={"User-Agent": user_agent or "*", "Accept": "text/plain,*/*;q=0.1"})
        with urlopen(request, timeout=20) as response:
            payload = response.read()
            encoding = response.headers.get_content_charset() or "utf-8"
            body_text = payload.decode(encoding, errors="replace")
    except HTTPError as exc:
        if int(exc.code) in {401, 403}:
            return False, f"robots_http_error:{exc.code}"
        return True, ""
    except (URLError, TimeoutError):
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"robots_check_failed:{type(exc).__name__}"
    parser = RobotFileParser()
    parser.parse(body_text.splitlines())
    allowed = parser.can_fetch(user_agent or "*", url)
    return allowed, "" if allowed else "robots_disallow"


def fetch_text(url: str, user_agent: str, timeout_seconds: int = 20) -> FetchResponse:
    """Fetch text content from a public URL using a declared user agent."""
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/json,application/xml,text/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read()
            encoding = response.headers.get_content_charset() or "utf-8"
            return FetchResponse(
                url=url,
                ok=True,
                status_code=int(getattr(response, "status", 200) or 200),
                body_text=payload.decode(encoding, errors="replace"),
                content_type=response.headers.get("Content-Type", ""),
                crawl_status="ok",
            )
    except HTTPError as exc:
        return FetchResponse(
            url=url,
            ok=False,
            status_code=int(exc.code),
            body_text="",
            content_type="",
            crawl_status="http_error",
            error_message=f"HTTP {exc.code}",
        )
    except URLError as exc:
        return FetchResponse(
            url=url,
            ok=False,
            status_code=0,
            body_text="",
            content_type="",
            crawl_status="network_error",
            error_message=str(exc.reason),
        )
    except TimeoutError as exc:
        return FetchResponse(
            url=url,
            ok=False,
            status_code=0,
            body_text="",
            content_type="",
            crawl_status="network_timeout",
            error_message=str(exc),
        )
