"""yahoo_price_loader.py
Phase 2A — Yahoo Finance Historical Price Ingestion

Strategy:
- Full 5-year OHLCV load for new tickers (no prior data in bronze)
- Incremental daily load for existing tickers (fetch only days after MAX(trade_date))
- MERGE upsert into fazdane_finance.bronze.stock_price_raw on (ticker, trade_date)
- Batch API calls to avoid rate limiting (batch_size tickers at a time)
"""
from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType
)

from src.utils.date_utils import five_years_ago, today_et, format_date
from src.utils.logging_utils import get_logger
from src.utils.spark_utils import get_spark, delta_merge

logger = get_logger(__name__)

BRONZE_TABLE  = "fazdane_finance.bronze.stock_price_raw"
SOURCE_NAME   = "yahoo_finance"
BATCH_SIZE    = 10   # tickers per yfinance batch call
RETRY_LIMIT   = 3

# Spark schema matching bronze.stock_price_raw
_SCHEMA = StructType([
    StructField("ticker",       StringType(),    nullable=False),
    StructField("trade_date",   DateType(),      nullable=False),
    StructField("open",         DoubleType(),    nullable=True),
    StructField("high",         DoubleType(),    nullable=True),
    StructField("low",          DoubleType(),    nullable=True),
    StructField("close",        DoubleType(),    nullable=True),
    StructField("adj_close",    DoubleType(),    nullable=True),
    StructField("volume",       LongType(),      nullable=True),
    StructField("source",       StringType(),    nullable=True),
    StructField("ingestion_ts", TimestampType(), nullable=True),
])


# ── helpers ────────────────────────────────────────────────────────────────────

def _get_max_dates() -> dict[str, date]:
    """Return dict of {ticker: max(trade_date)} for all tickers in bronze."""
    spark = get_spark()
    try:
        rows = spark.sql(
            f"SELECT ticker, MAX(trade_date) AS max_date FROM {BRONZE_TABLE} GROUP BY ticker"
        ).collect()
        return {row["ticker"]: row["max_date"] for row in rows}
    except Exception:
        return {}


def _fetch_yahoo(ticker: str, start: date, end: date) -> pd.DataFrame:
    """Fetch daily OHLCV from Yahoo Finance for one ticker.

    Uses Ticker.history() which is reliable across yfinance v0.2.x and v1.x.

    Args:
        ticker: Ticker symbol.
        start:  Start date (inclusive).
        end:    End date (inclusive).

    Returns:
        Pandas DataFrame with columns: trade_date, open, high, low, close, adj_close, volume.
        Empty DataFrame if no data returned.
    """
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            t = yf.Ticker(ticker)
            raw = t.history(
                start=format_date(start),
                end=format_date(end + timedelta(days=1)),  # history end is exclusive
                interval="1d",
                auto_adjust=False,
                actions=False,
            )

            if raw.empty:
                logger.warning(f"{ticker}: no data returned from Yahoo Finance ({start} → {end})")
                return pd.DataFrame()

            # Ticker.history() returns a DatetimeIndex — reset to get a date column
            df = raw.reset_index()

            # Normalize the date column name (may be "Date" or "Datetime")
            date_col = next((c for c in df.columns if str(c).lower() in ("date", "datetime")), None)
            if date_col is None:
                logger.error(f"{ticker}: no date column found. Columns: {list(df.columns)}")
                return pd.DataFrame()

            df = df.rename(columns={
                date_col:    "trade_date",
                "Open":      "open",
                "High":      "high",
                "Low":       "low",
                "Close":     "close",
                "Adj Close": "adj_close",
                "Volume":    "volume",
            })

            # Strip timezone info if present
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None).dt.date

            df["ticker"]       = ticker
            df["source"]       = SOURCE_NAME
            df["ingestion_ts"] = datetime.utcnow()

            # Ensure required columns exist
            required = ["ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "source", "ingestion_ts"]
            missing = [c for c in required if c not in df.columns]
            if missing:
                logger.error(f"{ticker}: missing columns after rename: {missing}. Available: {list(df.columns)}")
                return pd.DataFrame()

            return df[required]

        except Exception as e:
            logger.warning(f"{ticker}: Yahoo Finance attempt {attempt}/{RETRY_LIMIT} failed — {e}")

    logger.error(f"{ticker}: all {RETRY_LIMIT} Yahoo Finance attempts failed.")
    return pd.DataFrame()


def _pandas_to_spark(pdf: pd.DataFrame) -> DataFrame:
    """Convert a Pandas DataFrame to a Spark DataFrame using the bronze schema."""
    spark = get_spark()
    return spark.createDataFrame(pdf, schema=_SCHEMA)


# ── public API ─────────────────────────────────────────────────────────────────

def run_full_load(tickers: list[str]) -> None:
    """Fetch 5 years of daily OHLCV for each ticker and MERGE into bronze.

    Called for new tickers that have no history in the bronze table.

    Args:
        tickers: List of ticker symbols to backfill.
    """
    if not tickers:
        logger.info("run_full_load: no tickers to process.")
        return

    start = five_years_ago()
    end   = today_et()
    logger.info(f"Full 5-year load: {len(tickers)} tickers from {start} to {end}")

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i: i + BATCH_SIZE]
        for ticker in batch:
            logger.info(f"  Full load: {ticker}")
            pdf = _fetch_yahoo(ticker, start, end)
            if pdf.empty:
                continue
            sdf = _pandas_to_spark(pdf)
            delta_merge(sdf, BRONZE_TABLE, merge_keys=["ticker", "trade_date"])
            logger.info(f"  {ticker}: {len(pdf):,} rows merged into {BRONZE_TABLE}")


def run_incremental_load(tickers: list[str]) -> None:
    """Fetch only new trading days (after MAX trade_date) for each ticker.

    Called for existing tickers that already have bronze history.

    Args:
        tickers: List of ticker symbols to update incrementally.
    """
    if not tickers:
        logger.info("run_incremental_load: no tickers to process.")
        return

    max_dates = _get_max_dates()
    end       = today_et()
    logger.info(f"Incremental load: {len(tickers)} tickers up to {end}")

    for ticker in tickers:
        last_date = max_dates.get(ticker)
        if last_date is None:
            logger.warning(f"{ticker}: no max_date found — falling back to full load")
            run_full_load([ticker])
            continue

        start = last_date + timedelta(days=1)
        if start > end:
            logger.info(f"  {ticker}: already up to date (max date = {last_date})")
            continue

        logger.info(f"  Incremental: {ticker} from {start} to {end}")
        pdf = _fetch_yahoo(ticker, start, end)
        if pdf.empty:
            continue
        sdf = _pandas_to_spark(pdf)
        delta_merge(sdf, BRONZE_TABLE, merge_keys=["ticker", "trade_date"])
        logger.info(f"  {ticker}: {len(pdf):,} new rows merged")
