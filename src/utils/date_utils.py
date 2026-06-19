"""date_utils.py
Phase 2 — Date and Trading Calendar Utilities
"""
from __future__ import annotations
from datetime import date, datetime, timedelta
import pytz

# NYSE trading hours (Eastern Time)
_ET = pytz.timezone("America/New_York")
_MARKET_OPEN_HOUR  = 9
_MARKET_OPEN_MIN   = 30
_MARKET_CLOSE_HOUR = 16
_OPTIONS_FETCH_HOUR = 15   # 3:00 PM ET — 30 min before close


def _is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # Saturday=5, Sunday=6


def is_trading_day(d: date) -> bool:
    """Return True if d is a weekday (simplified — does not account for market holidays)."""
    return not _is_weekend(d)


def get_trading_days(start: date, end: date) -> list[date]:
    """Return all weekday dates between start and end inclusive."""
    days = []
    current = start
    while current <= end:
        if is_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


def last_n_trading_days(n: int, as_of: date | None = None) -> list[date]:
    """Return the last N trading days up to and including as_of (default: today)."""
    as_of = as_of or date.today()
    days: list[date] = []
    current = as_of
    while len(days) < n:
        if is_trading_day(current):
            days.append(current)
        current -= timedelta(days=1)
    return list(reversed(days))


def trading_days_ago(d: date, n: int) -> date:
    """Return the date that is N trading days before d."""
    count = 0
    current = d - timedelta(days=1)
    while count < n:
        if is_trading_day(current):
            count += 1
        if count < n:
            current -= timedelta(days=1)
    return current


def five_years_ago(as_of: date | None = None) -> date:
    """Return the date exactly 5 years before as_of (default: today)."""
    as_of = as_of or date.today()
    return as_of.replace(year=as_of.year - 5)


def market_close_et(d: date | None = None) -> datetime:
    """Return today's 4:00 PM ET market close as a timezone-aware datetime."""
    d = d or date.today()
    return _ET.localize(datetime(d.year, d.month, d.day, _MARKET_CLOSE_HOUR, 0, 0))


def options_fetch_time_et(d: date | None = None) -> datetime:
    """Return today's 3:00 PM ET options fetch time as a timezone-aware datetime."""
    d = d or date.today()
    return _ET.localize(datetime(d.year, d.month, d.day, _OPTIONS_FETCH_HOUR, 0, 0))


def today_et() -> date:
    """Return today's date in US/Eastern timezone."""
    return datetime.now(_ET).date()


def format_date(d: date) -> str:
    """Format a date as YYYY-MM-DD string."""
    return d.strftime("%Y-%m-%d")
