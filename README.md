# DB Financial Research Platform

Databricks-based financial research and options strategy platform.

## Overview

The FazDane platform ingests market data, calculates proprietary indicators, selects options strategies, and serves results through a mobile-first Streamlit dashboard — all running on Databricks.

## Architecture

```
Yahoo Finance ──▶ Bronze (raw prices)  ──▶ Silver (clean) ──▶ Gold (indicators)
Tastytrade API ──▶ Bronze (raw options) ──▶ Silver (clean) ──▶ Gold (features)
                                                                      │
                                                              Strategy Engine
                                                                      │
                                                              ML Ranking (XGBoost)
                                                                      │
                                                           Streamlit App (Databricks Apps)
```

## Databricks Components

| Component | Purpose |
|---|---|
| Unity Catalog | `fazdane_finance` — bronze / silver / gold / ml schemas |
| Delta Lake | All tables stored as Delta |
| Databricks Workflows | `FazDane Daily Finance Pipeline` — 14 tasks |
| MLflow | Experiment tracking + model registry |
| Feature Store | ML feature table in Unity Catalog |
| Databricks Apps | Streamlit frontend hosting |
| Databricks Secrets | Tastytrade credentials + API tokens |
| Databricks Repos | GitHub sync |

## Repository Structure

```
/config              — YAML config files (tickers, strategy rules, app settings, ML config)
/sql                 — SQL scripts to create Unity Catalog, schemas, and all tables
/src
  /ingestion         — Yahoo Finance + Tastytrade data loaders
  /quality           — Validation and deduplication
  /features          — Technical indicators, FDTS, Ticker Strength, options features
  /strategies        — Calendar, Diagonal, Iron Condor selection + trade construction
  /ml                — XGBoost ranker, HMM regime, survival model, batch predict
  /utils             — Spark utilities, secrets, logging, date helpers
/notebooks           — Databricks orchestration notebooks (run in Workflows)
/app                 — Streamlit application (Databricks Apps)
  /pages             — 6 pages: Candidates, FDTS, Strength, Scanner, Risk, Backtest
  /components        — Reusable charts, filters, tables
  /services          — SQL Warehouse connector and queries
/Implementation      — Build log and implementation plan notebook
```

## Phase Plan

| Phase | Focus |
|---|---|
| 1 | Foundation — workspace, catalog, Git, config, SQL scaffold |
| 2 | Bronze ingestion — Yahoo Finance prices + Tastytrade options |
| 3 | Silver layer — cleaning, deduplication, liquid universe |
| 4 | Gold indicators — FDTS, Ticker Strength, technical indicators |
| 5 | Options features + market regime |
| 6 | Strategy engine — Calendar / Diagonal / Iron Condor |
| 7 | ML layer — XGBoost, HMM, survival model |
| 8 | Streamlit App — 6-page mobile-first UI |
| 9 | Production — 100 tickers, full workflow schedule, alerts |

## MVP Tickers (Phase 1)

SPY, QQQ, IWM, NVDA, AAPL, MSFT, AMZN, META, GOOGL, TSLA, AVGO, AMD, LLY, JPM, XOM

## Security

- No credentials committed to Git
- All secrets via Databricks Secrets scope `fazdane`
- No data files committed — all data in Delta Lake
- No model artifacts in Git — all models in MLflow Registry

## Daily Pipeline Schedule

| Task | Time (ET) |
|---|---|
| Options chain ingestion (Tastytrade) | 3:00 PM |
| Price ingestion (Yahoo Finance) | After close (~4:30 PM) |
| Silver + Gold pipeline | After price ingestion |
| ML batch prediction | After gold pipeline |
| Dashboard refresh | End of pipeline |

## Getting Started

1. Clone this repo into Databricks Repos
2. Run `/sql/01_create_catalog_schema.sql` through `/sql/05_create_ml_tables.sql` in order
3. Store Tastytrade credentials in Databricks Secrets: `databricks secrets put-secret --scope fazdane --key tastytrade_username`
4. Configure `config/app_config.yaml` with your SQL Warehouse HTTP path and workspace URL
5. Run notebook `notebooks/01_ingest_prices.py` to load 5-year history
6. Follow the implementation plan in `Implementation/fazdane_implementation_plan.ipynb`
