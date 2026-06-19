# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingest Options Chain (Tastytrade)
# MAGIC **Phase 2B** — Daily options chain snapshot at 3:00 PM ET.
# MAGIC
# MAGIC **Schedule:** Daily at 3:00 PM ET (30 min before close) — Mon–Fri
# MAGIC
# MAGIC **Writes to:** `fazdane_finance.bronze.options_chain_raw`
# MAGIC
# MAGIC **Security:** Credentials read from Databricks Secrets scope `fazdane`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Authenticate to Tastytrade

# COMMAND ----------
# TODO: implement in Phase 2
# from src.utils.secrets import get_secret
# from src.ingestion.tastytrade_option_loader import TastytradeClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Fetch options chain for all liquid tickers

# COMMAND ----------
# TODO: implement in Phase 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — MERGE into bronze.options_chain_raw

# COMMAND ----------
# TODO: implement in Phase 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify row counts

# COMMAND ----------
# TODO: implement in Phase 2
# spark.sql("SELECT ticker, trade_date, COUNT(*) as contracts FROM fazdane_finance.bronze.options_chain_raw WHERE trade_date = current_date() GROUP BY ticker, trade_date ORDER BY ticker").display()
