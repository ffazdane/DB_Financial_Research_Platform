-- =============================================================================
-- FazDane Finance Platform
-- Script 02: Bronze Tables — Raw Ingested Data
-- Run after: 01_create_catalog_schema.sql
-- Primary keys are enforced via MERGE upsert in the ingestion code.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- bronze.stock_price_raw
-- Source: Yahoo Finance (yfinance)
-- Ingestion: Full 5-year load on first run; incremental daily thereafter.
-- New ticker: auto-triggers full 5-year backfill.
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.bronze.stock_price_raw (
    ticker        STRING    NOT NULL COMMENT 'Ticker symbol, e.g. AAPL',
    trade_date    DATE      NOT NULL COMMENT 'Trading date',
    open          DOUBLE    COMMENT 'Open price',
    high          DOUBLE    COMMENT 'High price',
    low           DOUBLE    COMMENT 'Low price',
    close         DOUBLE    COMMENT 'Close price',
    adj_close     DOUBLE    COMMENT 'Adjusted close (splits + dividends)',
    volume        BIGINT    COMMENT 'Daily volume',
    source        STRING    COMMENT 'Data source identifier, e.g. yahoo_finance',
    ingestion_ts  TIMESTAMP COMMENT 'UTC timestamp when row was written'
)
USING DELTA
COMMENT 'Raw daily OHLCV stock prices ingested from Yahoo Finance'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'            = 'true',
    'delta.autoOptimize.optimizeWrite'      = 'true',
    'delta.autoOptimize.autoCompact'        = 'true'
);

-- ---------------------------------------------------------------------------
-- bronze.options_chain_raw
-- Source: Tastytrade API
-- Ingestion: Daily snapshot at 3:00 PM ET (30 min before market close).
-- Primary key: (ticker, trade_date, expiration_date, option_type, strike)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.bronze.options_chain_raw (
    ticker             STRING    NOT NULL COMMENT 'Underlying ticker symbol',
    trade_date         DATE      NOT NULL COMMENT 'Date of the options snapshot (3:00 PM ET)',
    expiration_date    DATE      NOT NULL COMMENT 'Option expiration date',
    dte                INT       COMMENT 'Days to expiration at time of snapshot',
    option_type        STRING    NOT NULL COMMENT 'C = call, P = put',
    strike             DOUBLE    NOT NULL COMMENT 'Strike price',
    bid                DOUBLE    COMMENT 'Bid price',
    ask                DOUBLE    COMMENT 'Ask price',
    mid                DOUBLE    COMMENT 'Mid-point of bid/ask',
    last               DOUBLE    COMMENT 'Last traded price',
    volume             BIGINT    COMMENT 'Intraday option volume at snapshot time',
    open_interest      BIGINT    COMMENT 'Open interest at snapshot time',
    implied_volatility DOUBLE    COMMENT 'Implied volatility (decimal, e.g. 0.25 = 25%)',
    delta              DOUBLE    COMMENT 'Delta greek',
    gamma              DOUBLE    COMMENT 'Gamma greek',
    theta              DOUBLE    COMMENT 'Theta greek (daily)',
    vega               DOUBLE    COMMENT 'Vega greek',
    source             STRING    COMMENT 'Data source identifier, e.g. tastytrade',
    ingestion_ts       TIMESTAMP COMMENT 'UTC timestamp when row was written'
)
USING DELTA
COMMENT 'Raw daily options chain snapshots from Tastytrade — captured at 3:00 PM ET'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'            = 'true',
    'delta.autoOptimize.optimizeWrite'      = 'true',
    'delta.autoOptimize.autoCompact'        = 'true'
);

-- Verify
SHOW TABLES IN fazdane_finance.bronze;
