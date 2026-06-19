-- =============================================================================
-- FazDane Finance Platform
-- Script 03: Silver Tables — Cleaned and Validated Data
-- Run after: 02_create_bronze_tables.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- silver.stock_price_clean
-- Cleaned, deduplicated daily prices with return calculations.
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.silver.stock_price_clean (
    ticker          STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date      DATE      NOT NULL COMMENT 'Trading date',
    open            DOUBLE    COMMENT 'Open price',
    high            DOUBLE    COMMENT 'High price',
    low             DOUBLE    COMMENT 'Low price',
    close           DOUBLE    COMMENT 'Close price',
    adj_close       DOUBLE    COMMENT 'Adjusted close',
    volume          BIGINT    COMMENT 'Daily volume',
    dollar_volume   DOUBLE    COMMENT 'close × volume',
    return_1d       DOUBLE    COMMENT '1-day return',
    return_5d       DOUBLE    COMMENT '5-day return',
    return_20d      DOUBLE    COMMENT '20-day return',
    return_60d      DOUBLE    COMMENT '60-day return',
    is_liquid       BOOLEAN   COMMENT 'Passes liquidity filter for this date',
    updated_ts      TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Cleaned and validated daily stock prices with return calculations'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true'
);

-- ---------------------------------------------------------------------------
-- silver.options_chain_clean
-- Validated options data with spread quality flags.
-- Primary key: (ticker, trade_date, expiration_date, option_type, strike)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.silver.options_chain_clean (
    ticker             STRING    NOT NULL COMMENT 'Underlying ticker symbol',
    trade_date         DATE      NOT NULL COMMENT 'Snapshot date',
    expiration_date    DATE      NOT NULL COMMENT 'Option expiration date',
    dte                INT       COMMENT 'Days to expiration',
    option_type        STRING    NOT NULL COMMENT 'C = call, P = put',
    strike             DOUBLE    NOT NULL COMMENT 'Strike price',
    bid                DOUBLE    COMMENT 'Bid price',
    ask                DOUBLE    COMMENT 'Ask price',
    mid                DOUBLE    COMMENT 'Mid-point of bid/ask',
    spread_pct         DOUBLE    COMMENT '(ask - bid) / mid — bid/ask spread as a percentage',
    volume             BIGINT    COMMENT 'Option volume at snapshot',
    open_interest      BIGINT    COMMENT 'Open interest at snapshot',
    implied_volatility DOUBLE    COMMENT 'Implied volatility (decimal)',
    delta              DOUBLE    COMMENT 'Delta',
    gamma              DOUBLE    COMMENT 'Gamma',
    theta              DOUBLE    COMMENT 'Theta (daily)',
    vega               DOUBLE    COMMENT 'Vega',
    is_liquid_option   BOOLEAN   COMMENT 'Passes option liquidity filter',
    updated_ts         TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Cleaned options chain data with spread quality flags'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true'
);

-- ---------------------------------------------------------------------------
-- silver.liquid_universe
-- Daily liquid ticker universe — tickers that pass all liquidity filters.
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.silver.liquid_universe (
    ticker                  STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date              DATE      NOT NULL COMMENT 'Date of liquidity assessment',
    close                   DOUBLE    COMMENT 'Closing price',
    avg_volume_20d          DOUBLE    COMMENT '20-day average share volume',
    avg_dollar_volume_20d   DOUBLE    COMMENT '20-day average dollar volume',
    option_volume           DOUBLE    COMMENT 'Total option volume (all strikes/expirations)',
    option_open_interest    DOUBLE    COMMENT 'Total option open interest',
    option_spread_pct       DOUBLE    COMMENT 'Average option bid/ask spread %',
    is_liquid               BOOLEAN   COMMENT 'Passes all liquidity filters',
    liquidity_score         DOUBLE    COMMENT 'Composite liquidity score 0–100',
    updated_ts              TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Daily liquid ticker universe — 30% avg dollar vol, 25% OI, 20% option vol, 15% spread, 10% price stability'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true'
);

-- Verify
SHOW TABLES IN fazdane_finance.silver;
