"""secrets.py
Phase 2 — Databricks Secrets Wrapper

SECURITY: All credentials are stored in Databricks Secrets scope 'fazdane'.
          This module never logs, prints, or exposes secret values.

Usage in Databricks notebooks:
    from src.utils.secrets import get_secret
    username = get_secret("tastytrade_username")
    password = get_secret("tastytrade_password")

Local dev fallback — set environment variables:
    FAZDANE_TASTYTRADE_USERNAME, FAZDANE_TASTYTRADE_PASSWORD, etc.

To store a secret in Databricks:
    databricks secrets put-secret --scope fazdane --key tastytrade_username
"""
from __future__ import annotations
import os

_SCOPE = "fazdane"

_ENV_FALLBACKS: dict[str, str] = {
    "tastytrade_client_secret":  "FAZDANE_TT_CLIENT_SECRET",
    "tastytrade_refresh_token":  "FAZDANE_TT_REFRESH_TOKEN",
    "databricks_token":          "FAZDANE_DATABRICKS_TOKEN",
    "sql_warehouse_http_path":   "FAZDANE_SQL_WAREHOUSE_HTTP_PATH",
}


def get_secret(key: str) -> str:
    """Retrieve a secret from Databricks Secrets (or env var fallback for local dev).

    Args:
        key: Secret key name, e.g. 'tastytrade_username'

    Returns:
        Secret value as a string.

    Raises:
        RuntimeError: If the secret is not found in either source.
    """
    # Databricks runtime — use dbutils.secrets
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        return dbutils.secrets.get(scope=_SCOPE, key=key)
    except Exception:
        pass

    # Local dev — environment variable fallback
    env_var = _ENV_FALLBACKS.get(key, f"FAZDANE_{key.upper()}")
    value = os.environ.get(env_var)
    if value:
        return value

    raise RuntimeError(
        f"Secret '{key}' not found in Databricks Secrets scope '{_SCOPE}' "
        f"or environment variable '{env_var}'.\n"
        f"Run: databricks secrets put-secret --scope {_SCOPE} --key {key}"
    )
