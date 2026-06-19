"""tastytrade_option_loader.py
Phase 2B — Tastytrade Options Chain Ingestion

Strategy:
- Authenticate via Tastytrade session token (username + password from Databricks Secrets)
- Fetch full options chain per ticker daily at 3:00 PM ET (30 min before close)
- Captures: all expirations, all strikes, bid/ask/mid, Greeks, IV, volume, open interest
- MERGE upsert into fazdane_finance.bronze.options_chain_raw
  on (ticker, trade_date, expiration_date, option_type, strike)

SECURITY: Credentials read exclusively from Databricks Secrets scope 'fazdane'.
          Tokens are never logged, printed, or stored to disk.

Tastytrade API docs: https://developer.tastytrade.com
"""
from __future__ import annotations

import time
from datetime import date, datetime
from typing import Optional

import requests
import pandas as pd
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

from src.utils.logging_utils import get_logger
from src.utils.secrets import get_secret
from src.utils.spark_utils import get_spark, delta_merge

logger = get_logger(__name__)

BRONZE_TABLE = "fazdane_finance.bronze.options_chain_raw"
SOURCE_NAME  = "tastytrade"

# Tastytrade base URLs
_BASE_URL      = "https://api.tastytrade.com"
_SANDBOX_URL   = "https://api.cert.tastytrade.com"   # for testing

# Fetch config
MAX_DTE       = 90    # only fetch expirations within 90 days
MIN_DTE       = 5     # skip very near-term (< 5 DTE)
RETRY_LIMIT   = 3
RETRY_DELAY   = 2     # seconds between retries

# Spark schema matching bronze.options_chain_raw
_SCHEMA = StructType([
    StructField("ticker",             StringType(),    nullable=False),
    StructField("trade_date",         DateType(),      nullable=False),
    StructField("expiration_date",    DateType(),      nullable=False),
    StructField("dte",                IntegerType(),   nullable=True),
    StructField("option_type",        StringType(),    nullable=False),
    StructField("strike",             DoubleType(),    nullable=False),
    StructField("bid",                DoubleType(),    nullable=True),
    StructField("ask",                DoubleType(),    nullable=True),
    StructField("mid",                DoubleType(),    nullable=True),
    StructField("last",               DoubleType(),    nullable=True),
    StructField("volume",             LongType(),      nullable=True),
    StructField("open_interest",      LongType(),      nullable=True),
    StructField("implied_volatility", DoubleType(),    nullable=True),
    StructField("delta",              DoubleType(),    nullable=True),
    StructField("gamma",              DoubleType(),    nullable=True),
    StructField("theta",              DoubleType(),    nullable=True),
    StructField("vega",               DoubleType(),    nullable=True),
    StructField("source",             StringType(),    nullable=True),
    StructField("ingestion_ts",       TimestampType(), nullable=True),
])


# ── authentication ─────────────────────────────────────────────────────────────

class TastytradeSession:
    """Manages a Tastytrade API session token.

    Authenticates once and reuses the token for all requests.
    Credentials are pulled from Databricks Secrets — never hardcoded.
    """

    def __init__(self, use_sandbox: bool = False) -> None:
        self._base = _SANDBOX_URL if use_sandbox else _BASE_URL
        self._token: Optional[str] = None
        self._headers: dict[str, str] = {}

    def login(self) -> None:
        """Authenticate and store the session token.

        Credentials sourced from Databricks Secrets scope 'fazdane':
            - tastytrade_username
            - tastytrade_password
        """
        username = get_secret("tastytrade_username")
        password = get_secret("tastytrade_password")

        resp = requests.post(
            f"{self._base}/sessions",
            json={"login": username, "password": password},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        self._token = data["data"]["session-token"]
        self._headers = {
            "Authorization": self._token,
            "Content-Type":  "application/json",
        }
        logger.info("Tastytrade session authenticated successfully.")

    def logout(self) -> None:
        """Invalidate the session token."""
        if self._token:
            try:
                requests.delete(f"{self._base}/sessions", headers=self._headers, timeout=10)
            except Exception:
                pass
            self._token = None
            self._headers = {}

    def get(self, path: str, params: dict | None = None) -> dict:
        """Make an authenticated GET request.

        Args:
            path:   API path, e.g. '/option-chains/AAPL/nested'
            params: Optional query parameters dict.

        Returns:
            Parsed JSON response dict.
        """
        if not self._token:
            raise RuntimeError("Not authenticated. Call login() first.")

        for attempt in range(1, RETRY_LIMIT + 1):
            try:
                resp = requests.get(
                    f"{self._base}{path}",
                    headers=self._headers,
                    params=params or {},
                    timeout=30,
                )
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as e:
                if resp.status_code == 429:
                    logger.warning(f"Rate limited — waiting {RETRY_DELAY * attempt}s")
                    time.sleep(RETRY_DELAY * attempt)
                elif attempt == RETRY_LIMIT:
                    raise
                else:
                    logger.warning(f"HTTP {resp.status_code} on attempt {attempt}: {e}")
                    time.sleep(RETRY_DELAY)
            except Exception as e:
                if attempt == RETRY_LIMIT:
                    raise
                logger.warning(f"Request attempt {attempt} failed: {e}")
                time.sleep(RETRY_DELAY)
        return {}


# ── chain parsing ──────────────────────────────────────────────────────────────

def _parse_chain(ticker: str, trade_date: date, chain_data: dict) -> pd.DataFrame:
    """Parse the nested Tastytrade options chain response into a flat DataFrame.

    Args:
        ticker:     Ticker symbol.
        trade_date: The snapshot date (today at 3:00 PM ET).
        chain_data: Raw JSON from /option-chains/{ticker}/nested

    Returns:
        Flat Pandas DataFrame with one row per (expiration, option_type, strike).
    """
    rows: list[dict] = []
    expirations = chain_data.get("data", {}).get("items", [])

    for exp in expirations:
        exp_date_str = exp.get("expiration-date", "")
        try:
            exp_date = date.fromisoformat(exp_date_str)
        except ValueError:
            continue

        dte = (exp_date - trade_date).days
        if dte < MIN_DTE or dte > MAX_DTE:
            continue

        for option_type_key, otype_label in [("calls", "C"), ("puts", "P")]:
            for contract in exp.get(option_type_key, []):
                strike = float(contract.get("strike-price", 0) or 0)
                if strike <= 0:
                    continue

                bid  = _safe_float(contract.get("bid"))
                ask  = _safe_float(contract.get("ask"))
                mid  = round((bid + ask) / 2, 4) if bid is not None and ask is not None else None

                rows.append({
                    "ticker":             ticker,
                    "trade_date":         trade_date,
                    "expiration_date":    exp_date,
                    "dte":                dte,
                    "option_type":        otype_label,
                    "strike":             strike,
                    "bid":                bid,
                    "ask":                ask,
                    "mid":                mid,
                    "last":               _safe_float(contract.get("last")),
                    "volume":             _safe_int(contract.get("volume")),
                    "open_interest":      _safe_int(contract.get("open-interest")),
                    "implied_volatility": _safe_float(contract.get("implied-volatility")),
                    "delta":              _safe_float(contract.get("delta")),
                    "gamma":              _safe_float(contract.get("gamma")),
                    "theta":              _safe_float(contract.get("theta")),
                    "vega":               _safe_float(contract.get("vega")),
                    "source":             SOURCE_NAME,
                    "ingestion_ts":       datetime.utcnow(),
                })

    return pd.DataFrame(rows)


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None and val != "" else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        return int(val) if val is not None and val != "" else None
    except (ValueError, TypeError):
        return None


# ── public API ─────────────────────────────────────────────────────────────────

def run_options_load(
    tickers: list[str],
    trade_date: Optional[date] = None,
    use_sandbox: bool = False,
) -> None:
    """Fetch full options chain for each ticker and MERGE into bronze.

    Called daily at 3:00 PM ET (30 min before market close) to capture
    peak intraday volume and open interest.

    Args:
        tickers:     List of ticker symbols to fetch.
        trade_date:  Snapshot date. Defaults to today.
        use_sandbox: Use Tastytrade sandbox environment for testing.
    """
    if not tickers:
        logger.info("run_options_load: no tickers to process.")
        return

    trade_date = trade_date or date.today()
    spark = get_spark()
    session = TastytradeSession(use_sandbox=use_sandbox)

    try:
        session.login()

        for ticker in tickers:
            logger.info(f"Fetching options chain: {ticker} (trade_date={trade_date})")

            try:
                data = session.get(f"/option-chains/{ticker}/nested")
                pdf = _parse_chain(ticker, trade_date, data)

                if pdf.empty:
                    logger.warning(f"{ticker}: empty options chain — skipping.")
                    continue

                sdf = spark.createDataFrame(pdf, schema=_SCHEMA)
                delta_merge(
                    sdf,
                    BRONZE_TABLE,
                    merge_keys=["ticker", "trade_date", "expiration_date", "option_type", "strike"],
                )
                logger.info(
                    f"{ticker}: {len(pdf):,} contracts merged "
                    f"({pdf['expiration_date'].nunique()} expirations, "
                    f"DTE {pdf['dte'].min()}–{pdf['dte'].max()})"
                )

            except Exception as e:
                logger.error(f"{ticker}: options chain fetch failed — {e}")
                continue

    finally:
        session.logout()
        logger.info("Tastytrade session closed.")
