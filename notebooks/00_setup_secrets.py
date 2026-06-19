# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup: Store Secrets in Databricks
# MAGIC
# MAGIC **Run this notebook ONCE** to store Tastytrade credentials into Databricks Secrets scope `fazdane`.
# MAGIC
# MAGIC After running:
# MAGIC 1. **Clear all output** from this notebook (Cell → Clear All Outputs)
# MAGIC 2. The secrets are now stored securely — all other notebooks read them via `get_secret()`
# MAGIC
# MAGIC > Widget values are NOT stored in notebook revision history or output.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Enter your credentials using the widgets above
# MAGIC
# MAGIC Fill in each widget field at the top of the notebook, then run Steps 2–4.

# COMMAND ----------

dbutils.widgets.text("tt_client_secret",  "", "Tastytrade Client Secret (TT_SECRET)")
dbutils.widgets.text("tt_refresh_token",  "", "Tastytrade Refresh Token (TT_REFRESH)")
dbutils.widgets.text("databricks_token",  "", "Databricks Personal Access Token (optional)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create the secrets scope (run once, safe to re-run)

# COMMAND ----------

import requests, json

# Use the notebook's built-in context for the workspace URL and token
ctx         = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace   = "https://" + ctx.tags().get("browserHostName").get()
token       = ctx.apiToken().get()

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Create scope if it doesn't exist
resp = requests.post(
    f"{workspace}/api/2.0/secrets/scopes/create",
    headers=headers,
    json={"scope": "fazdane", "initial_manage_principal": "users"},
)
if resp.status_code == 200:
    print("✓ Secrets scope 'fazdane' created.")
elif "RESOURCE_ALREADY_EXISTS" in resp.text:
    print("✓ Secrets scope 'fazdane' already exists — OK.")
else:
    print(f"✗ Unexpected response: {resp.status_code} — {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Store Tastytrade credentials

# COMMAND ----------

def put_secret(scope: str, key: str, value: str) -> None:
    """Store a secret via Databricks REST API."""
    if not value or not value.strip():
        print(f"  SKIPPED: {key} — widget is empty.")
        return
    resp = requests.post(
        f"{workspace}/api/2.0/secrets/put",
        headers=headers,
        json={"scope": scope, "key": key, "string_value": value.strip()},
    )
    if resp.status_code == 200:
        print(f"  ✓ Stored: {key}")
    else:
        print(f"  ✗ Failed: {key} — {resp.status_code}: {resp.text}")

print("Storing Tastytrade credentials in scope 'fazdane'...")
put_secret("fazdane", "tastytrade_client_secret",  dbutils.widgets.get("tt_client_secret"))
put_secret("fazdane", "tastytrade_refresh_token",  dbutils.widgets.get("tt_refresh_token"))

# Store optional Databricks token for SQL Warehouse connectivity
db_token = dbutils.widgets.get("databricks_token")
if db_token.strip():
    put_secret("fazdane", "databricks_token", db_token)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify secrets are stored (lists key names only — values never shown)

# COMMAND ----------

resp = requests.get(
    f"{workspace}/api/2.0/secrets/list",
    headers=headers,
    params={"scope": "fazdane"},
)
secrets_list = resp.json().get("secrets", [])
print(f"Secrets stored in scope 'fazdane' ({len(secrets_list)} keys):")
for s in secrets_list:
    print(f"  - {s['key']}  (last updated: {s.get('last_updated_timestamp', 'unknown')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Quick auth test (verifies credentials work against Tastytrade API)

# COMMAND ----------

from src.utils.secrets import get_secret

try:
    client_secret  = get_secret("tastytrade_client_secret")
    refresh_token  = get_secret("tastytrade_refresh_token")

    resp = requests.post(
        "https://api.tastytrade.com/oauth/token",
        json={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_secret": client_secret,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    access_token = data.get("access_token") or data.get("data", {}).get("access-token")
    if access_token:
        print("✓ Tastytrade OAuth: authentication successful — access token obtained.")
    else:
        print(f"✗ Token response missing access_token. Full response: {data}")

except Exception as e:
    print(f"✗ Authentication failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Clear all output after confirming secrets are stored
# MAGIC
# MAGIC **Click: Edit → Clear All Outputs** before closing this notebook.
# MAGIC
# MAGIC The widget values are cleared automatically when you detach from the cluster.
