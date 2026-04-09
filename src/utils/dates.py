"""Date helpers used across the pipeline."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO 8601 datetime string into a timezone-aware datetime."""
    if not value:
        return None

    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def build_relative_time_window(window_config: dict[str, Any], now: datetime | None = None) -> tuple[datetime, datetime]:
    """Build an inclusive UTC time window from relative config values."""
    explicit_start = parse_datetime(str(window_config.get("window_start", "") or ""))
    explicit_end = parse_datetime(str(window_config.get("window_end", "") or ""))
    if explicit_start is not None and explicit_end is not None:
        if explicit_start > explicit_end:
            raise ValueError("time_window config is invalid: window_start must be <= window_end")
        return explicit_start, explicit_end

    now_utc = (now or datetime.now(UTC)).astimezone(UTC)
    earliest_years_ago = int(window_config.get("earliest_years_ago", 5))
    latest_years_ago = int(window_config.get("latest_years_ago", 0))

    start = _shift_year(now_utc, -earliest_years_ago)
    end = _shift_year(now_utc, -latest_years_ago)
    if start > end:
        raise ValueError("time_window config is invalid: earliest_years_ago must be >= latest_years_ago")
    return start, end


def shift_year(value: datetime, offset_years: int) -> datetime:
    """Shift a datetime by whole years while handling leap-day edge cases."""
    target_year = value.year + offset_years
    day = value.day
    while day > 28:
        try:
            return value.replace(year=target_year, day=day)
        except ValueError:
            day -= 1
    return value.replace(year=target_year, day=day)


def split_time_window(start: datetime, end: datetime, years_per_slice: int = 1) -> list[tuple[datetime, datetime]]:
    """Split a time window into deterministic UTC slices.

    The collection layer uses slices to broaden recall across the configured
    history range. This is intentionally simple and exploratory.
    """
    if years_per_slice < 1:
        raise ValueError("years_per_slice must be >= 1")
    if start > end:
        return []

    slices: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < end:
        next_cursor = shift_year(cursor, years_per_slice)
        slice_end = min(next_cursor, end)
        slices.append((cursor, slice_end))
        if slice_end == end:
            break
        cursor = slice_end + timedelta(seconds=1)
    return slices


def build_time_slices(
    window_config: dict[str, Any],
    source_name: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, str]]:
    """Build collector time slices from explicit config or source-level granularity."""
    start_at, end_at = build_relative_time_window(window_config, now=now)
    override = dict((window_config.get("source_overrides", {}) or {}).get(source_name or "", {}) or {})
    strategy = str(override.get("strategy", window_config.get("default_strategy", "explicit")) or "explicit")

    if strategy == "explicit":
        configured_slices = override.get("slices") or window_config.get("default_slices") or []
        slices = [_normalize_slice_row(row) for row in configured_slices]
        if slices:
            return slices
        return _build_period_slices(start_at, end_at, frequency="yearly", prefix=f"{source_name or 'window'}")

    if strategy == "monthly":
        return _build_period_slices(start_at, end_at, frequency="monthly", prefix=f"{source_name or 'window'}")
    if strategy == "quarterly":
        return _build_period_slices(start_at, end_at, frequency="quarterly", prefix=f"{source_name or 'window'}")
    if strategy == "yearly":
        return _build_period_slices(start_at, end_at, frequency="yearly", prefix=f"{source_name or 'window'}")
    raise ValueError(f"Unsupported time slice strategy: {strategy}")


def _shift_year(value: datetime, offset_years: int) -> datetime:
    """Shift a datetime by whole years while handling leap-day edge cases."""
    return shift_year(value, offset_years)


def _normalize_slice_row(row: dict[str, Any]) -> dict[str, str]:
    """Normalize one configured time slice row."""
    start_at = parse_datetime(str(row.get("window_start", "") or ""))
    end_at = parse_datetime(str(row.get("window_end", "") or ""))
    if start_at is None or end_at is None:
        raise ValueError("Each configured time slice requires window_start and window_end")
    return {
        "window_id": str(row.get("window_id", "")) or f"window_{start_at.date().isoformat()}",
        "window_start": start_at.isoformat(),
        "window_end": end_at.isoformat(),
    }


def _build_period_slices(start_at: datetime, end_at: datetime, frequency: str, prefix: str) -> list[dict[str, str]]:
    """Build monthly, quarterly, or yearly calendar slices."""
    slices: list[dict[str, str]] = []
    cursor = _start_of_period(start_at, frequency)
    index = 1
    while cursor <= end_at:
        period_end = _end_of_period(cursor, frequency)
        slice_start = max(cursor, start_at)
        slice_end = min(period_end, end_at)
        slices.append(
            {
                "window_id": f"{prefix}_{frequency}_{index:03d}",
                "window_start": slice_start.isoformat(),
                "window_end": slice_end.isoformat(),
            }
        )
        cursor = slice_end + timedelta(seconds=1)
        cursor = _start_of_period(cursor, frequency)
        index += 1
    return slices


def _start_of_period(value: datetime, frequency: str) -> datetime:
    """Return the UTC period start for monthly, quarterly, or yearly slicing."""
    if frequency == "monthly":
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if frequency == "quarterly":
        month = ((value.month - 1) // 3) * 3 + 1
        return value.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
    if frequency == "yearly":
        return value.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unsupported frequency: {frequency}")


def _end_of_period(value: datetime, frequency: str) -> datetime:
    """Return the UTC period end for monthly, quarterly, or yearly slicing."""
    if frequency == "monthly":
        next_start = _add_months(_start_of_period(value, frequency), 1)
        return next_start - timedelta(seconds=1)
    if frequency == "quarterly":
        next_start = _add_months(_start_of_period(value, frequency), 3)
        return next_start - timedelta(seconds=1)
    if frequency == "yearly":
        next_start = value.replace(year=value.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return next_start - timedelta(seconds=1)
    raise ValueError(f"Unsupported frequency: {frequency}")


def _add_months(value: datetime, months: int) -> datetime:
    """Add whole months to a datetime at UTC period boundaries."""
    month_index = value.month - 1 + months
    target_year = value.year + month_index // 12
    target_month = month_index % 12 + 1
    return value.replace(year=target_year, month=target_month, day=1)
