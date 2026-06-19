"""universe_loader.py
Phase 2A — Ticker Universe Loader

Loads tickers from config/tickers.yaml, compares against bronze table,
and classifies each ticker as:
  - new_tickers:      no history in bronze → trigger full 5-year backfill
  - existing_tickers: already have rows → trigger incremental update only
"""
from __future__ import annotations
from pathlib import Path
from typing import Any

import yaml

from src.utils.logging_utils import get_logger
from src.utils.spark_utils import get_spark

logger = get_logger(__name__)

# Default config path relative to repo root
_DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "tickers.yaml"

BRONZE_PRICE_TABLE = "fazdane_finance.bronze.stock_price_raw"


def load_tickers(config_path: Path | str | None = None) -> list[str]:
    """Load the flat ticker list from tickers.yaml.

    Args:
        config_path: Path to tickers.yaml. Defaults to config/tickers.yaml.

    Returns:
        List of ticker symbols as uppercase strings.
    """
    path = Path(config_path) if config_path else _DEFAULT_CONFIG
    with open(path, "r") as f:
        cfg: dict[str, Any] = yaml.safe_load(f)
    tickers = [t.upper() for t in cfg.get("all_tickers", [])]
    logger.info(f"Loaded {len(tickers)} tickers from {path.name}")
    return tickers


def get_bronze_tickers() -> set[str]:
    """Return the set of tickers that already have at least one row in the bronze price table."""
    spark = get_spark()
    try:
        rows = spark.sql(
            f"SELECT DISTINCT ticker FROM {BRONZE_PRICE_TABLE}"
        ).collect()
        return {row["ticker"] for row in rows}
    except Exception as e:
        logger.warning(f"Could not read {BRONZE_PRICE_TABLE}: {e}. Assuming empty table.")
        return set()


def classify_tickers(
    config_path: Path | str | None = None,
) -> tuple[list[str], list[str]]:
    """Classify configured tickers into new (need full 5yr load) and existing (incremental).

    Args:
        config_path: Optional path to tickers.yaml.

    Returns:
        Tuple of (new_tickers, existing_tickers).
        new_tickers:      no data in bronze → full 5-year backfill required.
        existing_tickers: already have history → incremental update only.
    """
    all_tickers   = load_tickers(config_path)
    bronze_set    = get_bronze_tickers()

    new_tickers      = [t for t in all_tickers if t not in bronze_set]
    existing_tickers = [t for t in all_tickers if t in bronze_set]

    logger.info(
        f"Ticker classification: {len(new_tickers)} new (full 5yr load), "
        f"{len(existing_tickers)} existing (incremental)"
    )
    if new_tickers:
        logger.info(f"New tickers → full backfill: {new_tickers}")

    return new_tickers, existing_tickers
