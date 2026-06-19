"""universe_loader.py
Phase 2A — Ticker Universe Loader

Responsibilities:
- Load ticker list from config/tickers.yaml
- Compare against tickers already present in bronze.stock_price_raw
- Identify new tickers with no history (trigger full 5-year backfill)
- Identify existing tickers needing incremental update
- Return two lists: new_tickers, existing_tickers
"""
# TODO: implement in Phase 2
