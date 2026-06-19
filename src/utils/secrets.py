"""secrets.py
Phase 2 — Databricks Secrets Wrapper

SECURITY: This module is the ONLY place credentials are accessed.
          All credentials are read from Databricks Secrets at runtime.
          Nothing sensitive is stored in this file, hardcoded, or logged.

Usage:
    from src.utils.secrets import get_secret
    username = get_secret('fazdane', 'tastytrade_username')
    password = get_secret('fazdane', 'tastytrade_password')
"""
# TODO: implement in Phase 2
# Implementation will use:
#   dbutils.secrets.get(scope=scope, key=key)  — within Databricks
#   databricks-sdk SecretClient              — outside Databricks
