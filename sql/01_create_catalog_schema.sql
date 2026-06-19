-- =============================================================================
-- FazDane Finance Platform
-- Script 01: Create Unity Catalog and Schemas
-- Run this FIRST in your Databricks workspace before any other SQL script.
-- Requires: Unity Catalog enabled on the workspace.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS fazdane_finance
  COMMENT 'FazDane Financial Research Platform — Unity Catalog root';

-- Bronze: raw ingested data — never modified after write
CREATE SCHEMA IF NOT EXISTS fazdane_finance.bronze
  COMMENT 'Raw ingested data — Yahoo Finance daily prices and Tastytrade options chain';

-- Silver: cleaned, validated, deduplicated
CREATE SCHEMA IF NOT EXISTS fazdane_finance.silver
  COMMENT 'Cleaned and validated data — quality-checked, liquid universe filter applied';

-- Gold: feature tables, indicators, strategy outputs
CREATE SCHEMA IF NOT EXISTS fazdane_finance.gold
  COMMENT 'Feature tables — technical indicators, FDTS, Ticker Strength, strategy selection, trade candidates';

-- ML: model features, forward returns, prediction scores
CREATE SCHEMA IF NOT EXISTS fazdane_finance.ml
  COMMENT 'ML feature tables, model outputs, and batch prediction scores';

-- Verify
SHOW SCHEMAS IN fazdane_finance;
