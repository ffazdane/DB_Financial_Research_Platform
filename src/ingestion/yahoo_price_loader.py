"""yahoo_price_loader.py
Phase 2A — Yahoo Finance Historical Price Ingestion

Responsibilities:
- Full 5-year OHLCV load for new tickers (no prior history in bronze)
- Daily incremental load — fetch only days after MAX(trade_date) per ticker
- Auto-detect new tickers added to tickers.yaml and trigger backfill
- MERGE upsert into fazdane_finance.bronze.stock_price_raw on (ticker, trade_date)

SECURITY: No API keys required for Yahoo Finance.
"""
# TODO: implement in Phase 2
