"""tastytrade_option_loader.py
Phase 2B — Tastytrade Options Chain Ingestion

Responsibilities:
- Authenticate to Tastytrade API using credentials from Databricks Secrets
- Fetch full options chain per ticker: all expirations, all strikes
- Capture: bid/ask/mid, Greeks (delta/gamma/theta/vega), IV, volume, OI
- Run daily at 3:00 PM ET (30 min before close) for maximum volume capture
- MERGE upsert into fazdane_finance.bronze.options_chain_raw
  on (ticker, trade_date, expiration_date, option_type, strike)

SECURITY: Credentials read exclusively from Databricks Secrets scope 'fazdane'.
          Never hardcode or log credentials.
"""
# TODO: implement in Phase 2
