"""logging_utils.py
Phase 2 — Structured Logging Utilities
"""
from __future__ import annotations
import logging
import sys
from datetime import datetime


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger that writes to stdout (captured by Databricks cluster logs).

    Args:
        name: Logger name — use __name__ in each module.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_phase(logger: logging.Logger, phase: int, task: str, status: str = "started") -> None:
    """Log a pipeline phase/task transition."""
    logger.info(f"PHASE {phase} | {task} | {status.upper()}")
