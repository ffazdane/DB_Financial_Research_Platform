# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Prices (Yahoo Finance)
# MAGIC **Phase 2A** — Full 5-year load for new tickers; incremental daily update for existing tickers.
# MAGIC
# MAGIC **Schedule:** Daily after market close (~4:30 PM ET) via `FazDane Daily Finance Pipeline` Workflow
# MAGIC
# MAGIC **Writes to:** `fazdane_finance.bronze.stock_price_raw`
# MAGIC
# MAGIC **Logic:**
# MAGIC - New ticker (no bronze history) → fetch full 5-year OHLCV
# MAGIC - Existing ticker → fetch only days after `MAX(trade_date)`
# MAGIC - MERGE upsert on `(ticker, trade_date)` — no duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Install dependencies

# COMMAND ----------

%pip install yfinance pyyaml pytz --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Classify tickers: new vs existing

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/ffazdane@gmail.com/DB_Financial_Research_Platform")

from src.ingestion.universe_loader import classify_tickers

new_tickers, existing_tickers = classify_tickers()

print(f"New tickers (full 5yr load):   {new_tickers}")
print(f"Existing tickers (incremental): {existing_tickers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1b — Diagnostic: verify yfinance download structure

# COMMAND ----------

import yfinance as yf, pandas as pd
test = yf.download("SPY", start="2025-01-01", end="2025-01-10", interval="1d", auto_adjust=False, progress=False)
print(f"yfinance version: {yf.__version__}")
print(f"Shape: {test.shape}")
print(f"Empty: {test.empty}")
print(f"Columns type: {type(test.columns)}")
print(f"Columns: {list(test.columns)}")
if not test.empty:
    df2 = test.reset_index()
    print(f"After reset_index columns: {list(df2.columns)}")
    print(df2.head(2))
dbutils.notebook.exit(f"yfinance={yf.__version__}, shape={test.shape}, cols={list(test.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Full 5-year backfill for new tickers

# COMMAND ----------

from src.ingestion.yahoo_price_loader import run_full_load

run_full_load(new_tickers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Incremental update for existing tickers

# COMMAND ----------

from src.ingestion.yahoo_price_loader import run_incremental_load

run_incremental_load(existing_tickers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify: row counts and date range per ticker

# COMMAND ----------

spark.sql("""
    SELECT
        ticker,
        COUNT(*)        AS total_rows,
        MIN(trade_date) AS earliest_date,
        MAX(trade_date) AS latest_date,
        COUNT(DISTINCT trade_date) AS trading_days
    FROM fazdane_finance.bronze.stock_price_raw
    GROUP BY ticker
    ORDER BY ticker
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Quality check: flag any nulls in key columns

# COMMAND ----------

spark.sql("""
    SELECT
        ticker,
        SUM(CASE WHEN close    IS NULL THEN 1 ELSE 0 END) AS null_close,
        SUM(CASE WHEN volume   IS NULL THEN 1 ELSE 0 END) AS null_volume,
        SUM(CASE WHEN adj_close IS NULL THEN 1 ELSE 0 END) AS null_adj_close
    FROM fazdane_finance.bronze.stock_price_raw
    GROUP BY ticker
    HAVING null_close > 0 OR null_volume > 0 OR null_adj_close > 0
    ORDER BY ticker
""").display()
