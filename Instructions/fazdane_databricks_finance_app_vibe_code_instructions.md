# FazDane Databricks Finance Research Platform

## Goal
Build a Databricks-based financial research and options strategy application.

The application must ingest 5 years of historical stock data, ingest daily options chain data, calculate technical indicators, calculate proprietary indicators, run ML models, and recommend options strategies.

Core strategies:
1. Calendar
2. Diagonal
3. Iron Condor

Core proprietary indicators:
1. FDTS
2. Ticker Strength

Frontend:
Use Databricks Apps with Streamlit. Build mobile-first screens so the application is usable from iPhone.

---

## Platform Components

Use these Databricks components:

1. Unity Catalog
2. Delta Lake tables
3. Databricks Workflows
4. Databricks SQL
5. MLflow
6. Feature Engineering / Feature Store in Unity Catalog
7. Databricks Apps
8. Streamlit frontend
9. Databricks Secrets
10. GitHub / Databricks Repos

---

## Data Sources

Use Yahoo Finance for:

- Historical stock OHLCV
- 5-year daily price history
- Index data: SPY, QQQ, IWM
- VIX if available

Use Tastytrade API for:

- Options chain
- Greeks
- IV
- Expiration dates
- Strikes
- Bid / ask
- Open interest
- Option volume
- Account positions if available

Start with 50 to 100 highly liquid tickers first.

---

## Suggested Repo Structure

```text
/fazdane_databricks_finance_app
    /config
        tickers.yaml
        strategy_rules.yaml
        model_config.yaml
        app_config.yaml

    /sql
        01_create_catalog_schema.sql
        02_create_bronze_tables.sql
        03_create_silver_tables.sql
        04_create_gold_tables.sql

    /src
        /ingestion
            yahoo_price_loader.py
            tastytrade_option_loader.py
            universe_loader.py

        /quality
            data_validation.py
            dedupe.py

        /features
            technical_indicators.py
            fdts_engine.py
            ticker_strength_engine.py
            option_features.py
            market_regime.py
            volatility_engine.py
            correlation_engine.py

        /strategies
            strategy_selector.py
            calendar_engine.py
            diagonal_engine.py
            iron_condor_engine.py
            trade_constructor.py
            risk_engine.py

        /ml
            feature_builder.py
            xgboost_ranker.py
            hmm_regime_model.py
            survival_model.py
            batch_predict.py

        /utils
            spark_utils.py
            date_utils.py
            logging_utils.py
            secrets.py

    /notebooks
        01_ingest_prices.py
        02_ingest_options.py
        03_calculate_indicators.py
        04_calculate_fdts_strength.py
        05_strategy_selection.py
        06_train_models.py
        07_batch_prediction.py

    /app
        app.py
        pages/
            1_Today_Candidates.py
            2_FDTS_Signals.py
            3_Ticker_Strength.py
            4_Options_Strategies.py
            5_Portfolio_Risk.py
            6_Backtest.py
        components/
            charts.py
            filters.py
            tables.py
        services/
            databricks_sql.py
            queries.py

    requirements.txt
    README.md
```

---

## Database Design

Create catalog and schemas:

```sql
CREATE CATALOG IF NOT EXISTS fazdane_finance;
CREATE SCHEMA IF NOT EXISTS fazdane_finance.bronze;
CREATE SCHEMA IF NOT EXISTS fazdane_finance.silver;
CREATE SCHEMA IF NOT EXISTS fazdane_finance.gold;
CREATE SCHEMA IF NOT EXISTS fazdane_finance.ml;
```

---

## Bronze Tables

Raw stock price table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.bronze.stock_price_raw (
    ticker STRING,
    trade_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT,
    source STRING,
    ingestion_ts TIMESTAMP
)
USING DELTA;
```

Raw options chain table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.bronze.options_chain_raw (
    ticker STRING,
    trade_date DATE,
    expiration_date DATE,
    dte INT,
    option_type STRING,
    strike DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    last DOUBLE,
    volume BIGINT,
    open_interest BIGINT,
    implied_volatility DOUBLE,
    delta DOUBLE,
    gamma DOUBLE,
    theta DOUBLE,
    vega DOUBLE,
    source STRING,
    ingestion_ts TIMESTAMP
)
USING DELTA;
```

---

## Silver Tables

Clean stock price table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.silver.stock_price_clean (
    ticker STRING,
    trade_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT,
    dollar_volume DOUBLE,
    return_1d DOUBLE,
    return_5d DOUBLE,
    return_20d DOUBLE,
    return_60d DOUBLE,
    is_liquid BOOLEAN,
    updated_ts TIMESTAMP
)
USING DELTA;
```

Clean options table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.silver.options_chain_clean (
    ticker STRING,
    trade_date DATE,
    expiration_date DATE,
    dte INT,
    option_type STRING,
    strike DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    spread_pct DOUBLE,
    volume BIGINT,
    open_interest BIGINT,
    implied_volatility DOUBLE,
    delta DOUBLE,
    gamma DOUBLE,
    theta DOUBLE,
    vega DOUBLE,
    is_liquid_option BOOLEAN,
    updated_ts TIMESTAMP
)
USING DELTA;
```

Liquid universe table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.silver.liquid_universe (
    ticker STRING,
    trade_date DATE,
    close DOUBLE,
    avg_volume_20d DOUBLE,
    avg_dollar_volume_20d DOUBLE,
    option_volume DOUBLE,
    option_open_interest DOUBLE,
    option_spread_pct DOUBLE,
    is_liquid BOOLEAN,
    liquidity_score DOUBLE,
    updated_ts TIMESTAMP
)
USING DELTA;
```

---

## Gold Tables

Technical indicators table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.technical_indicators (
    ticker STRING,
    trade_date DATE,
    close DOUBLE,
    sma_10 DOUBLE,
    sma_20 DOUBLE,
    sma_50 DOUBLE,
    sma_100 DOUBLE,
    sma_200 DOUBLE,
    ema_8 DOUBLE,
    ema_21 DOUBLE,
    ema_34 DOUBLE,
    ema_55 DOUBLE,
    rsi_14 DOUBLE,
    macd DOUBLE,
    macd_signal DOUBLE,
    atr_14 DOUBLE,
    bollinger_upper DOUBLE,
    bollinger_lower DOUBLE,
    vwap DOUBLE,
    ichimoku_tenkan DOUBLE,
    ichimoku_kijun DOUBLE,
    trend_score DOUBLE,
    momentum_score DOUBLE,
    volume_score DOUBLE,
    updated_ts TIMESTAMP
)
USING DELTA;
```

FDTS table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.fdts_indicator (
    ticker STRING,
    trade_date DATE,
    fdts_fast DOUBLE,
    fdts_slow DOUBLE,
    fdts_signal STRING,
    fdts_trend_state STRING,
    fdts_score DOUBLE,
    fdts_buy_signal BOOLEAN,
    fdts_sell_signal BOOLEAN,
    fdts_confidence DOUBLE,
    updated_ts TIMESTAMP
)
USING DELTA;
```

Ticker Strength table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.ticker_strength (
    ticker STRING,
    trade_date DATE,
    price_strength_score DOUBLE,
    volume_strength_score DOUBLE,
    relative_strength_spy DOUBLE,
    relative_strength_qqq DOUBLE,
    relative_strength_iwm DOUBLE,
    momentum_5d DOUBLE,
    momentum_20d DOUBLE,
    momentum_60d DOUBLE,
    trend_alignment_score DOUBLE,
    sector_relative_strength DOUBLE,
    ticker_strength_score DOUBLE,
    ticker_strength_label STRING,
    updated_ts TIMESTAMP
)
USING DELTA;
```

Option features table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.option_features (
    ticker STRING,
    trade_date DATE,
    iv_rank DOUBLE,
    iv_percentile DOUBLE,
    expected_move DOUBLE,
    avg_bid_ask_spread_pct DOUBLE,
    option_volume_score DOUBLE,
    open_interest_score DOUBLE,
    liquidity_score DOUBLE,
    gamma_risk_score DOUBLE,
    theta_score DOUBLE,
    vega_score DOUBLE,
    earnings_risk_score DOUBLE,
    updated_ts TIMESTAMP
)
USING DELTA;
```

Market regime table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.market_regime (
    trade_date DATE,
    spy_regime STRING,
    qqq_regime STRING,
    iwm_regime STRING,
    vix_state STRING,
    dominant_regime STRING,
    regime_score DOUBLE,
    risk_level STRING,
    updated_ts TIMESTAMP
)
USING DELTA;
```

Strategy selection table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.strategy_selection (
    ticker STRING,
    trade_date DATE,
    market_regime STRING,
    fdts_signal STRING,
    fdts_score DOUBLE,
    ticker_strength_score DOUBLE,
    ticker_strength_label STRING,
    iv_rank DOUBLE,
    iv_percentile DOUBLE,
    expected_move DOUBLE,
    option_liquidity_score DOUBLE,
    earnings_risk_score DOUBLE,
    trend_score DOUBLE,
    volatility_score DOUBLE,
    strategy_selected STRING,
    strategy_direction STRING,
    strategy_reason STRING,
    confidence_score DOUBLE,
    risk_level STRING,
    updated_ts TIMESTAMP
)
USING DELTA;
```

Trade candidates table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.gold.trade_candidates (
    ticker STRING,
    trade_date DATE,
    strategy_selected STRING,
    strategy_direction STRING,
    short_expiration DATE,
    long_expiration DATE,
    short_strike DOUBLE,
    long_strike DOUBLE,
    net_debit_credit DOUBLE,
    max_risk DOUBLE,
    max_profit_estimate DOUBLE,
    breakeven_low DOUBLE,
    breakeven_high DOUBLE,
    delta DOUBLE,
    gamma DOUBLE,
    theta DOUBLE,
    vega DOUBLE,
    probability_score DOUBLE,
    final_rank_score DOUBLE,
    trade_action STRING,
    notes STRING,
    updated_ts TIMESTAMP
)
USING DELTA;
```

---

## Liquid Stock Universe Rules

Create a daily liquid universe.

Rules:

- Price greater than 20
- Average 20-day volume greater than 1,000,000
- 20-day dollar volume greater than 50,000,000
- Options open interest available
- Options bid/ask spread less than 10%
- Exclude penny stocks
- Exclude illiquid options

Liquidity scoring:

- 30% average dollar volume
- 25% options open interest
- 20% options volume
- 15% bid/ask spread quality
- 10% price stability

---

## Technical Indicator Requirements

Implement these in PySpark or Pandas UDF:

- SMA 10, 20, 50, 100, 200
- EMA 8, 21, 34, 55
- RSI 14
- MACD 12/26/9
- ATR 14
- Bollinger Bands 20, 2 standard deviations
- VWAP
- Ichimoku Tenkan 9
- Ichimoku Kijun 26
- 52-week high distance
- 20-day high breakout
- 20-day low breakdown
- Trend slope
- Price deviation from SMA 20, 50, 200
- Volume trend
- Relative strength versus SPY, QQQ, IWM

---

## FDTS Engine Instruction

Build FDTS as proprietary timing indicator.

Inputs:

- Close price
- EMA values
- TEMA / zero-lag TEMA logic if available
- MACD confirmation
- Trend slope
- Price deviation from cloud / moving averages
- Volume confirmation

Outputs:

- FDTS Buy
- FDTS Sell
- FDTS Neutral
- FDTS Score 0 to 100
- FDTS Confidence 0 to 100

Rules:

- Buy when fast FDTS crosses above slow FDTS and slope is positive
- Sell when fast FDTS crosses below slow FDTS and slope is negative
- Neutral when no clean signal
- Score should reward trend alignment, positive slope, volume confirmation, and relative strength
- Penalize extended price, weak volume, and conflicting market regime

Example scoring:

- 30% crossover signal
- 25% trend slope
- 20% price above key moving averages
- 15% MACD confirmation
- 10% volume confirmation

---

## Ticker Strength Engine Instruction

Ticker Strength identifies which tickers deserve priority.

Inputs:

- Price momentum
- Volume momentum
- Relative strength versus SPY
- Relative strength versus QQQ
- Relative strength versus IWM
- Sector strength if available
- Trend alignment
- Liquidity

Score:

- 0 to 100

Example formula:

- 25% relative strength versus SPY/QQQ/IWM
- 20% 20-day momentum
- 15% 60-day momentum
- 15% trend alignment
- 10% volume confirmation
- 10% liquidity
- 5% sector leadership

Labels:

- 80 to 100 = Strong Leader
- 60 to 79 = Positive
- 40 to 59 = Neutral
- 20 to 39 = Weak
- 0 to 19 = Laggard

---

## Options Feature Engine

For each ticker and date, calculate:

- IV Rank
- IV Percentile
- Expected move
- Option liquidity score
- Average bid/ask spread
- Open interest score
- Volume score
- Delta availability
- Calendar spread cost
- Diagonal spread cost
- Iron condor credit
- Theta score
- Vega score
- Gamma risk
- Earnings risk

---

## Market Regime Engine

Classify each day:

- Bullish
- Bearish
- Sideways
- High volatility
- Low volatility
- Buy the dip
- Sell the rip
- Risk off

Inputs:

- SPY trend
- QQQ trend
- IWM trend
- VIX trend
- Market breadth if available
- Index returns 5D, 20D, 60D
- FDTS index signal
- Correlation concentration

---

## Strategy Selection Engine

Select one of:

- Calendar
- Diagonal
- Iron Condor
- No Trade

Calendar rules:

Use when:

- Directional bias exists
- FDTS Buy or Sell exists
- Ticker strength is positive
- IV is low to medium
- Expected move is controlled
- Trend is steady, not explosive

Diagonal rules:

Use when:

- Stronger directional move expected
- FDTS signal is strong
- Ticker strength above 75
- Trend score above 70
- User wants more directional exposure
- Price may move beyond short strike

Iron Condor rules:

Use when:

- Sideways regime
- High IV Rank
- Low directional strength
- Price is range bound
- FDTS neutral
- Ticker strength neutral
- Expected move fits within range

No Trade rules:

Use when:

- Liquidity is weak
- Earnings risk is high
- Spread is too wide
- FDTS and Ticker Strength conflict
- Market regime is unstable
- IV too low for premium selling
- Gamma risk too high

---

## Trade Construction Rules

Calendar:

- Same strike
- Short leg 20 to 30 DTE
- Long leg 40 to 60 DTE
- Use 25 to 35 delta strike
- Call calendar for bullish
- Put calendar for bearish

Diagonal:

- Short leg 20 to 30 DTE
- Long leg 40 to 60 DTE
- Long strike deeper ITM or closer to current price
- Short strike further OTM
- Use when directional conviction is stronger

Iron Condor:

- Short put delta 15 to 20
- Short call delta 15 to 20
- Wings 5 to 10 points wide depending on ticker
- 30 to 45 DTE preferred
- Target credit at least 25% to 35% of width
- Avoid earnings unless specifically enabled

---

## ML Layer

Use MLflow for experiment tracking and model registry.

Models:

1. XGBoost ranking model
2. HMM market regime model
3. Survival model for trend duration
4. Probability model for next 20 trading days
5. Strategy success classifier

ML feature table:

```sql
CREATE TABLE IF NOT EXISTS fazdane_finance.ml.ml_feature_table (
    ticker STRING,
    trade_date DATE,
    fdts_score DOUBLE,
    ticker_strength_score DOUBLE,
    trend_score DOUBLE,
    momentum_score DOUBLE,
    volume_score DOUBLE,
    iv_rank DOUBLE,
    iv_percentile DOUBLE,
    option_liquidity_score DOUBLE,
    market_regime_score DOUBLE,
    return_forward_5d DOUBLE,
    return_forward_10d DOUBLE,
    return_forward_20d DOUBLE,
    profitable_calendar BOOLEAN,
    profitable_diagonal BOOLEAN,
    profitable_iron_condor BOOLEAN,
    updated_ts TIMESTAMP
)
USING DELTA;
```

Target variables:

- Next 5-day return
- Next 10-day return
- Next 20-day return
- Probability of positive move
- Probability of staying inside expected range
- Strategy expected value
- Strategy win probability

---

## Daily Workflow

Create Databricks Workflow:

Job name:

```text
FazDane Daily Finance Pipeline
```

Tasks:

1. ingest_price_history
2. ingest_options_chain
3. clean_stock_data
4. clean_options_data
5. update_liquid_universe
6. calculate_technical_indicators
7. calculate_fdts
8. calculate_ticker_strength
9. calculate_option_features
10. calculate_market_regime
11. run_strategy_selection
12. construct_trade_candidates
13. run_ml_predictions
14. refresh_dashboard_tables

Schedule:
Run daily after market close.

For Databricks Free Edition, keep the first version small because Free Edition has serverless and job concurrency limits.

---

## Streamlit App Requirements

Build mobile-first Streamlit pages.

Page 1: Today’s Trade Candidates

Filters:

- Strategy
- Ticker
- Sector
- FDTS signal
- Ticker strength
- Confidence
- Risk level

Show:

- Ticker
- Strategy
- Score
- Reason
- Debit/credit
- Greeks
- Action

Page 2: FDTS Signals

Show:

- Buy signals
- Sell signals
- Neutral signals
- Score
- Confidence
- Chart

Page 3: Ticker Strength

Show:

- Strong leaders
- Positive tickers
- Neutral tickers
- Weak tickers
- Laggards

Page 4: Options Strategy Scanner

Tabs:

- Calendar
- Diagonal
- Iron Condor

Page 5: Portfolio Risk

Show:

- Total delta
- Gamma
- Theta
- Vega
- Position risk
- Ticker concentration
- Strategy concentration

Page 6: Backtest

Show:

- Past signals
- Strategy result
- Win rate
- Average return
- Max drawdown

---

## Streamlit UI Design

Use compact mobile layout.

Use:

- st.metric
- st.dataframe
- st.tabs
- st.sidebar filters
- Plotly candlestick chart
- Line chart for FDTS
- Bar chart for Ticker Strength
- Table for trade candidates

---

## Quality Checks

Add validation:

- No duplicate ticker/date rows
- Option expiration date must be greater than trade date
- Bid must be less than ask
- Volume cannot be negative
- Close price cannot be null
- Liquidity score must be 0 to 100
- FDTS score must be 0 to 100
- Ticker Strength score must be 0 to 100

---

## Security

Do not hardcode API keys.

Use Databricks Secrets for:

- Tastytrade username
- Tastytrade password
- Market data API keys
- Email alert credentials

Do not store:

- API keys in Git
- Raw credentials in notebooks
- Account numbers in open tables

---

## Git Rules

Use GitHub repo.

Branch structure:

- feature/price-ingestion
- feature/options-ingestion
- feature/fdts-engine
- feature/ticker-strength
- feature/strategy-engine
- feature/streamlit-app
- feature/ml-ranking

Do not commit:

- Data files
- Model artifacts
- Secrets
- .env files

---

## First MVP Scope

Build only this first:

Tickers:

- SPY
- QQQ
- IWM
- NVDA
- AAPL
- MSFT
- AMZN
- META
- GOOGL
- TSLA
- AVGO
- AMD
- LLY
- JPM
- XOM

Features:

- Yahoo Finance price ingestion
- Delta tables
- SMA/EMA/RSI/MACD/ATR
- FDTS score
- Ticker Strength score
- Strategy selection
- Streamlit dashboard

Do not build full options chain for 1,000 tickers in MVP.

---

## Phase Plan

Phase 1:
Databricks workspace, Git, tables, Yahoo ingestion.

Phase 2:
Technical indicators, FDTS, Ticker Strength.

Phase 3:
Tastytrade options ingestion and liquidity filter.

Phase 4:
Calendar, Diagonal, Iron Condor strategy selection.

Phase 5:
MLflow, XGBoost ranking, regime model.

Phase 6:
Streamlit Databricks App.

Phase 7:
Portfolio upload, Greek command center, alerts.

---

## Final Output Expected

The application should produce:

1. Daily liquid ticker universe
2. Daily FDTS signals
3. Daily Ticker Strength ranking
4. Daily option strategy recommendation
5. Daily ML probability score
6. Daily top trade candidates
7. Portfolio risk dashboard
8. Streamlit mobile-friendly frontend

---

## Important Design Principle

FDTS is the timing engine.

Ticker Strength is the stock selection engine.

Market Regime is the environment filter.

Options Features are the structure filter.

ML is the ranking engine.

Strategy Engine is the final decision layer.
