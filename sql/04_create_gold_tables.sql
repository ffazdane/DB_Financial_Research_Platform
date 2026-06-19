-- =============================================================================
-- FazDane Finance Platform
-- Script 04: Gold Tables — Feature Outputs and Strategy Results
-- Run after: 03_create_silver_tables.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- gold.technical_indicators
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.technical_indicators (
    ticker              STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date          DATE      NOT NULL COMMENT 'Trading date',
    close               DOUBLE    COMMENT 'Close price',
    sma_10              DOUBLE    COMMENT '10-day simple moving average',
    sma_20              DOUBLE    COMMENT '20-day simple moving average',
    sma_50              DOUBLE    COMMENT '50-day simple moving average',
    sma_100             DOUBLE    COMMENT '100-day simple moving average',
    sma_200             DOUBLE    COMMENT '200-day simple moving average',
    ema_8               DOUBLE    COMMENT '8-day exponential moving average',
    ema_21              DOUBLE    COMMENT '21-day exponential moving average',
    ema_34              DOUBLE    COMMENT '34-day exponential moving average',
    ema_55              DOUBLE    COMMENT '55-day exponential moving average',
    rsi_14              DOUBLE    COMMENT 'RSI 14-period',
    macd                DOUBLE    COMMENT 'MACD line (12/26)',
    macd_signal         DOUBLE    COMMENT 'MACD signal line (9)',
    atr_14              DOUBLE    COMMENT 'ATR 14-period',
    bollinger_upper     DOUBLE    COMMENT 'Bollinger Band upper (20, 2 std dev)',
    bollinger_lower     DOUBLE    COMMENT 'Bollinger Band lower (20, 2 std dev)',
    vwap                DOUBLE    COMMENT 'Volume-weighted average price',
    ichimoku_tenkan     DOUBLE    COMMENT 'Ichimoku Tenkan-sen (9)',
    ichimoku_kijun      DOUBLE    COMMENT 'Ichimoku Kijun-sen (26)',
    trend_score         DOUBLE    COMMENT 'Composite trend score 0–100',
    momentum_score      DOUBLE    COMMENT 'Composite momentum score 0–100',
    volume_score        DOUBLE    COMMENT 'Volume trend score 0–100',
    updated_ts          TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Technical indicators: SMA/EMA/RSI/MACD/ATR/Bollinger/VWAP/Ichimoku'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- gold.fdts_indicator
-- FDTS: FazDane Timing System — proprietary timing engine
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.fdts_indicator (
    ticker              STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date          DATE      NOT NULL COMMENT 'Trading date',
    fdts_fast           DOUBLE    COMMENT 'Fast FDTS line',
    fdts_slow           DOUBLE    COMMENT 'Slow FDTS line',
    fdts_signal         STRING    COMMENT 'buy | sell | neutral',
    fdts_trend_state    STRING    COMMENT 'uptrend | downtrend | consolidating',
    fdts_score          DOUBLE    COMMENT 'FDTS score 0–100',
    fdts_buy_signal     BOOLEAN   COMMENT 'True when fast crosses above slow with positive slope',
    fdts_sell_signal    BOOLEAN   COMMENT 'True when fast crosses below slow with negative slope',
    fdts_confidence     DOUBLE    COMMENT 'Signal confidence 0–100',
    updated_ts          TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'FDTS proprietary timing indicator — buy/sell/neutral signals with score and confidence'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- gold.ticker_strength
-- Ticker Strength: stock selection engine
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.ticker_strength (
    ticker                  STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date              DATE      NOT NULL COMMENT 'Trading date',
    price_strength_score    DOUBLE    COMMENT 'Price-based strength sub-score',
    volume_strength_score   DOUBLE    COMMENT 'Volume-based strength sub-score',
    relative_strength_spy   DOUBLE    COMMENT 'Return relative to SPY',
    relative_strength_qqq   DOUBLE    COMMENT 'Return relative to QQQ',
    relative_strength_iwm   DOUBLE    COMMENT 'Return relative to IWM',
    momentum_5d             DOUBLE    COMMENT '5-day momentum',
    momentum_20d            DOUBLE    COMMENT '20-day momentum',
    momentum_60d            DOUBLE    COMMENT '60-day momentum',
    trend_alignment_score   DOUBLE    COMMENT 'Alignment across EMA stack',
    sector_relative_strength DOUBLE   COMMENT 'Strength vs sector peers (if available)',
    ticker_strength_score   DOUBLE    COMMENT 'Composite score 0–100',
    ticker_strength_label   STRING    COMMENT 'Strong Leader | Positive | Neutral | Weak | Laggard',
    updated_ts              TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Ticker Strength stock selection engine — composite score 0-100 with label'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- gold.option_features
-- Options-derived features per ticker per day
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.option_features (
    ticker                  STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date              DATE      NOT NULL COMMENT 'Trading date',
    iv_rank                 DOUBLE    COMMENT 'IV Rank (0–100)',
    iv_percentile           DOUBLE    COMMENT 'IV Percentile (0–100)',
    expected_move           DOUBLE    COMMENT 'Expected 1-SD move (price units)',
    avg_bid_ask_spread_pct  DOUBLE    COMMENT 'Average bid/ask spread % across liquid strikes',
    option_volume_score     DOUBLE    COMMENT 'Option volume score 0–100',
    open_interest_score     DOUBLE    COMMENT 'Open interest score 0–100',
    liquidity_score         DOUBLE    COMMENT 'Options liquidity composite score 0–100',
    gamma_risk_score        DOUBLE    COMMENT 'Gamma risk score 0–100',
    theta_score             DOUBLE    COMMENT 'Theta score 0–100',
    vega_score              DOUBLE    COMMENT 'Vega score 0–100',
    earnings_risk_score     DOUBLE    COMMENT 'Earnings proximity risk score 0–100',
    updated_ts              TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Options feature engine outputs — IV Rank, expected move, Greeks scores, earnings risk'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- gold.market_regime
-- Daily market regime classification (index level, not ticker level)
-- Primary key: (trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.market_regime (
    trade_date          DATE      NOT NULL COMMENT 'Trading date',
    spy_regime          STRING    COMMENT 'SPY regime classification',
    qqq_regime          STRING    COMMENT 'QQQ regime classification',
    iwm_regime          STRING    COMMENT 'IWM regime classification',
    vix_state           STRING    COMMENT 'VIX state: low | normal | elevated | extreme',
    dominant_regime     STRING    COMMENT 'bullish | bearish | sideways | high_volatility | low_volatility | risk_off',
    regime_score        DOUBLE    COMMENT 'Regime confidence score 0–100',
    risk_level          STRING    COMMENT 'low | medium | high',
    updated_ts          TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Daily market regime classification across SPY/QQQ/IWM and VIX'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- gold.strategy_selection
-- Strategy selected per ticker per day
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.strategy_selection (
    ticker                  STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date              DATE      NOT NULL COMMENT 'Trading date',
    market_regime           STRING    COMMENT 'Dominant market regime on this date',
    fdts_signal             STRING    COMMENT 'buy | sell | neutral',
    fdts_score              DOUBLE    COMMENT 'FDTS score 0–100',
    ticker_strength_score   DOUBLE    COMMENT 'Ticker Strength score 0–100',
    ticker_strength_label   STRING    COMMENT 'Ticker Strength label',
    iv_rank                 DOUBLE    COMMENT 'IV Rank 0–100',
    iv_percentile           DOUBLE    COMMENT 'IV Percentile 0–100',
    expected_move           DOUBLE    COMMENT 'Expected 1-SD move',
    option_liquidity_score  DOUBLE    COMMENT 'Option liquidity score 0–100',
    earnings_risk_score     DOUBLE    COMMENT 'Earnings risk score 0–100',
    trend_score             DOUBLE    COMMENT 'Trend score 0–100',
    volatility_score        DOUBLE    COMMENT 'Volatility score 0–100',
    strategy_selected       STRING    COMMENT 'calendar | diagonal | iron_condor | no_trade',
    strategy_direction      STRING    COMMENT 'bullish | bearish | neutral',
    strategy_reason         STRING    COMMENT 'Human-readable explanation of selection',
    confidence_score        DOUBLE    COMMENT 'Strategy confidence 0–100',
    risk_level              STRING    COMMENT 'low | medium | high',
    updated_ts              TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Strategy selection engine output — one row per ticker per day'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- gold.trade_candidates
-- Fully constructed trade candidates with Greeks and risk metrics
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.trade_candidates (
    ticker                  STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date              DATE      NOT NULL COMMENT 'Trading date',
    strategy_selected       STRING    COMMENT 'calendar | diagonal | iron_condor',
    strategy_direction      STRING    COMMENT 'bullish | bearish | neutral',
    short_expiration        DATE      COMMENT 'Short leg expiration date',
    long_expiration         DATE      COMMENT 'Long leg expiration date',
    short_strike            DOUBLE    COMMENT 'Short leg strike price',
    long_strike             DOUBLE    COMMENT 'Long leg strike price',
    net_debit_credit        DOUBLE    COMMENT 'Net debit (positive) or credit (negative)',
    max_risk                DOUBLE    COMMENT 'Maximum risk in dollars',
    max_profit_estimate     DOUBLE    COMMENT 'Maximum profit estimate in dollars',
    breakeven_low           DOUBLE    COMMENT 'Lower breakeven price',
    breakeven_high          DOUBLE    COMMENT 'Upper breakeven price',
    delta                   DOUBLE    COMMENT 'Net position delta',
    gamma                   DOUBLE    COMMENT 'Net position gamma',
    theta                   DOUBLE    COMMENT 'Net position theta (daily)',
    vega                    DOUBLE    COMMENT 'Net position vega',
    probability_score       DOUBLE    COMMENT 'ML probability score for success',
    final_rank_score        DOUBLE    COMMENT 'XGBoost final ranking score',
    trade_action            STRING    COMMENT 'enter | watch | skip',
    notes                   STRING    COMMENT 'Additional notes or flags',
    updated_ts              TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Fully constructed trade candidates with strikes, expirations, Greeks, and ML rank score'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- Verify
SHOW TABLES IN fazdane_finance.gold;
