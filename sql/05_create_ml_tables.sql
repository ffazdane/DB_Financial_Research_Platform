-- =============================================================================
-- FazDane Finance Platform
-- Script 05: ML Tables — Feature Store and Prediction Outputs
-- Run after: 04_create_gold_tables.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- ml.ml_feature_table
-- Combined feature set used for XGBoost training and inference.
-- Forward return columns populated by a lagged calculation job.
-- Primary key: (ticker, trade_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.ml.ml_feature_table (
    ticker                  STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date              DATE      NOT NULL COMMENT 'Feature date',
    fdts_score              DOUBLE    COMMENT 'FDTS score 0–100',
    ticker_strength_score   DOUBLE    COMMENT 'Ticker Strength score 0–100',
    trend_score             DOUBLE    COMMENT 'Trend score 0–100',
    momentum_score          DOUBLE    COMMENT 'Momentum score 0–100',
    volume_score            DOUBLE    COMMENT 'Volume score 0–100',
    iv_rank                 DOUBLE    COMMENT 'IV Rank 0–100',
    iv_percentile           DOUBLE    COMMENT 'IV Percentile 0–100',
    option_liquidity_score  DOUBLE    COMMENT 'Option liquidity score 0–100',
    market_regime_score     DOUBLE    COMMENT 'Market regime encoded as numeric score',
    -- Forward return labels (populated by lagged job, NULL until date + N passes)
    return_forward_5d       DOUBLE    COMMENT 'Actual 5-day forward return',
    return_forward_10d      DOUBLE    COMMENT 'Actual 10-day forward return',
    return_forward_20d      DOUBLE    COMMENT 'Actual 20-day forward return',
    -- Strategy outcome labels
    profitable_calendar     BOOLEAN   COMMENT 'True if calendar trade would have been profitable',
    profitable_diagonal     BOOLEAN   COMMENT 'True if diagonal trade would have been profitable',
    profitable_iron_condor  BOOLEAN   COMMENT 'True if iron condor would have been profitable',
    updated_ts              TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'ML feature table — inputs for XGBoost ranker, with forward return labels for training'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ---------------------------------------------------------------------------
-- ml.model_predictions
-- Daily batch inference output from the registered MLflow model.
-- Primary key: (ticker, trade_date, model_version)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fazdane_finance.ml.model_predictions (
    ticker                  STRING    NOT NULL COMMENT 'Ticker symbol',
    trade_date              DATE      NOT NULL COMMENT 'Prediction date',
    model_version           STRING    NOT NULL COMMENT 'MLflow model version used',
    prob_positive_5d        DOUBLE    COMMENT 'Probability of positive return in 5 days',
    prob_positive_10d       DOUBLE    COMMENT 'Probability of positive return in 10 days',
    prob_positive_20d       DOUBLE    COMMENT 'Probability of positive return in 20 days',
    prob_in_range           DOUBLE    COMMENT 'Probability price stays inside expected move',
    xgb_rank_score          DOUBLE    COMMENT 'XGBoost ranking score (higher = better candidate)',
    hmm_regime              STRING    COMMENT 'HMM-predicted regime for this date',
    survival_days_estimate  DOUBLE    COMMENT 'Estimated remaining trend duration (days)',
    updated_ts              TIMESTAMP COMMENT 'UTC timestamp of last update'
)
USING DELTA
COMMENT 'Daily batch ML prediction scores — probability of success per ticker per strategy'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- Verify
SHOW TABLES IN fazdane_finance.ml;
