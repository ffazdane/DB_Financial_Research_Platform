# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Prices (Yahoo Finance)
# MAGIC **Phase 2A** — Full 5-year load for new tickers; incremental daily update for existing tickers.
# MAGIC
# MAGIC **Schedule:** Daily after market close (~4:30 PM ET)
# MAGIC
# MAGIC **Writes to:** `fazdane_finance.bronze.stock_price_raw`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load ticker universe and classify new vs existing tickers

# COMMAND ----------
# TODO: implement in Phase 2
# from src.ingestion.universe_loader import load_universe
# from src.ingestion.yahoo_price_loader import run_full_load, run_incremental_load

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Full 5-year backfill for new tickers

# COMMAND ----------
# TODO: implement in Phase 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Incremental update for existing tickers

# COMMAND ----------
# TODO: implement in Phase 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify row counts

# COMMAND ----------
# TODO: implement in Phase 2
# spark.sql("SELECT ticker, COUNT(*) as rows, MIN(trade_date), MAX(trade_date) FROM fazdane_finance.bronze.stock_price_raw GROUP BY ticker ORDER BY ticker").display()
