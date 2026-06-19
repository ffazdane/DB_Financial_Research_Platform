# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingest Options Chain (Tastytrade)
# MAGIC **Phase 2B** — Daily options chain snapshot at **3:00 PM ET** (30 min before market close).
# MAGIC
# MAGIC **Schedule:** `FazDane Daily Finance Pipeline` Workflow — triggered at 3:00 PM ET, Mon–Fri
# MAGIC
# MAGIC **Writes to:** `fazdane_finance.bronze.options_chain_raw`
# MAGIC
# MAGIC **Primary key:** `(ticker, trade_date, expiration_date, option_type, strike)`
# MAGIC
# MAGIC **Security:** Tastytrade credentials read from Databricks Secrets scope `fazdane`.
# MAGIC Run once to store credentials (OAuth 2.0 — client secret + refresh token):
# MAGIC ```
# MAGIC databricks secrets put-secret --scope fazdane --key tastytrade_client_secret
# MAGIC databricks secrets put-secret --scope fazdane --key tastytrade_refresh_token
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Install dependencies

# COMMAND ----------

%pip install requests pyyaml pytz --quiet

# COMMAND ----------

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load liquid ticker universe

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/ffazdane/DB_Financial_Research_Platform")

from src.ingestion.universe_loader import load_tickers

tickers = load_tickers()
print(f"Tickers to fetch options for: {len(tickers)}")
print(tickers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Fetch options chain at 3:00 PM ET

# COMMAND ----------

from datetime import date
from src.ingestion.tastytrade_option_loader import run_options_load

# trade_date defaults to today — pass explicitly if running manually for a past date
run_options_load(
    tickers=tickers,
    trade_date=date.today(),
    use_sandbox=False,   # set True for testing with Tastytrade sandbox
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Verify: contract counts per ticker for today

# COMMAND ----------

from datetime import date
today_str = str(date.today())

spark.sql(f"""
    SELECT
        ticker,
        COUNT(*)                              AS total_contracts,
        COUNT(DISTINCT expiration_date)       AS expirations,
        MIN(dte)                              AS min_dte,
        MAX(dte)                              AS max_dte,
        SUM(CASE WHEN option_type='C' THEN 1 ELSE 0 END) AS calls,
        SUM(CASE WHEN option_type='P' THEN 1 ELSE 0 END) AS puts
    FROM fazdane_finance.bronze.options_chain_raw
    WHERE trade_date = '{today_str}'
    GROUP BY ticker
    ORDER BY ticker
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Quality check: flag missing Greeks or IV

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        COUNT(*)                                                          AS total_rows,
        SUM(CASE WHEN implied_volatility IS NULL THEN 1 ELSE 0 END)      AS null_iv,
        SUM(CASE WHEN delta             IS NULL THEN 1 ELSE 0 END)       AS null_delta,
        SUM(CASE WHEN bid               IS NULL THEN 1 ELSE 0 END)       AS null_bid,
        SUM(CASE WHEN ask               IS NULL THEN 1 ELSE 0 END)       AS null_ask,
        SUM(CASE WHEN bid >= ask        AND bid IS NOT NULL THEN 1 ELSE 0 END) AS bid_gte_ask_violations
    FROM fazdane_finance.bronze.options_chain_raw
    WHERE trade_date = '{today_str}'
    GROUP BY ticker
    HAVING null_iv > 0 OR null_delta > 0 OR bid_gte_ask_violations > 0
    ORDER BY ticker
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Sample: ATM options for AAPL (sanity check)

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker, trade_date, expiration_date, dte, option_type, strike,
        bid, ask, mid, volume, open_interest, implied_volatility,
        delta, gamma, theta, vega
    FROM fazdane_finance.bronze.options_chain_raw
    WHERE trade_date = '{today_str}'
      AND ticker     = 'AAPL'
      AND dte BETWEEN 20 AND 45
      AND ABS(delta) BETWEEN 0.4 AND 0.6
    ORDER BY expiration_date, option_type, strike
    LIMIT 20
""").display()
