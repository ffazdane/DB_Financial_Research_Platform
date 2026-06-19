"""
local_price_loader.py
Downloads 5-year daily OHLCV for all tickers from Yahoo Finance locally
(using the v8 chart API directly — no yfinance crumb/auth needed),
then upserts into Databricks fazdane_finance.bronze.stock_price_raw
via the SQL Statements REST API.

Run: python scripts/local_price_loader.py
"""
import sys
import time
import warnings
import datetime
import os

import requests
import pandas as pd

warnings.filterwarnings("ignore")
try:
    import urllib3
    urllib3.disable_warnings()
except Exception:
    pass

# ── Config — set DATABRICKS_TOKEN env var before running ──────────────────────
DB_HOST      = "https://dbc-225b7a82-e569.cloud.databricks.com"
DB_TOKEN     = os.environ.get("DATABRICKS_TOKEN", "")  # export DATABRICKS_TOKEN=dapi...
WAREHOUSE_ID = "76e3d9414486df1d"
TARGET_TABLE = "fazdane_finance.bronze.stock_price_raw"
INSERT_BATCH = 50   # rows per MERGE VALUES statement (keep SQL under size limit)

TICKERS = [
    "SPY", "QQQ", "IWM",
    "NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL",
    "TSLA", "AVGO", "AMD", "LLY", "JPM", "XOM",
]

# Single session with SSL verification disabled (corporate proxy)
SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

DB_HEADERS = {
    "Authorization": f"Bearer {DB_TOKEN}",
    "Content-Type":  "application/json",
}


# ── Yahoo Finance v8 chart API ─────────────────────────────────────────────────
def fetch_ticker(ticker: str) -> pd.DataFrame:
    """Fetch 5yr daily OHLCV via Yahoo Finance v8 chart API (no auth needed)."""
    print(f"  {ticker}...", end=" ", flush=True)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1d", "range": "5y", "events": "history"}
    try:
        r = SESSION.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        results = data.get("chart", {}).get("result", [])
        if not results:
            err = data.get("chart", {}).get("error")
            print(f"NO DATA — {err}")
            return pd.DataFrame()

        result     = results[0]
        timestamps = result.get("timestamp", [])
        if not timestamps:
            print("NO TIMESTAMPS")
            return pd.DataFrame()

        quote          = result.get("indicators", {}).get("quote", [{}])[0]
        adjclose_data  = result.get("indicators", {}).get("adjclose", [{}])
        adj_close_list = adjclose_data[0].get("adjclose", []) if adjclose_data else []

        rows = []
        for i, ts in enumerate(timestamps):
            trade_date = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).date()
            o = quote.get("open",   [None])[i]
            h = quote.get("high",   [None])[i]
            l = quote.get("low",    [None])[i]
            c = quote.get("close",  [None])[i]
            v = quote.get("volume", [None])[i]
            a = adj_close_list[i] if i < len(adj_close_list) else c
            if c is None:
                continue
            rows.append({
                "ticker":       ticker,
                "trade_date":   trade_date,
                "open":         o,
                "high":         h,
                "low":          l,
                "close":        c,
                "adj_close":    a,
                "volume":       int(v) if v is not None else None,
                "source":       "yahoo_finance",
                "ingestion_ts": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            })

        df = pd.DataFrame(rows)
        print(f"{len(df):,} rows  ({df['trade_date'].min()} → {df['trade_date'].max()})")
        return df

    except Exception as e:
        print(f"ERROR — {e}")
        return pd.DataFrame()


# ── Databricks SQL helper ──────────────────────────────────────────────────────
def run_sql(statement: str, timeout_s: int = 120) -> dict:
    """Execute a SQL statement via Databricks SQL Statements REST API."""
    payload = {
        "statement":    statement,
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "0s",   # async — poll for result
    }
    resp = SESSION.post(
        f"{DB_HOST}/api/2.0/sql/statements",
        headers=DB_HEADERS,
        json=payload,
        timeout=timeout_s + 10,
    )
    if not resp.ok:
        print(f"\nSQL API error {resp.status_code}: {resp.text[:500]}")
    resp.raise_for_status()
    result  = resp.json()
    state   = result.get("status", {}).get("state", "UNKNOWN")
    stmt_id = result.get("statement_id", "")

    for _ in range(timeout_s):
        if state == "SUCCEEDED":
            return result
        if state in ("FAILED", "CANCELED", "CLOSED"):
            raise RuntimeError(f"SQL failed [{state}]: {result}")
        if state in ("RUNNING", "PENDING"):
            time.sleep(2)
            poll = SESSION.get(
                f"{DB_HOST}/api/2.0/sql/statements/{stmt_id}",
                headers=DB_HEADERS,
                timeout=30,
            )
            poll.raise_for_status()
            result = poll.json()
            state  = result.get("status", {}).get("state", "UNKNOWN")
            continue
        break

    raise TimeoutError(f"SQL statement {stmt_id} timed out after {timeout_s}s")


# ── INSERT via MERGE ───────────────────────────────────────────────────────────
def _fmt_val(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"
    if isinstance(v, datetime.date):
        return f"DATE '{v.strftime('%Y-%m-%d')}'"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def insert_dataframe(df: pd.DataFrame) -> int:
    """Upsert DataFrame into target Delta table in batches. Returns rows processed."""
    if df.empty:
        return 0

    cols     = list(df.columns)
    col_str  = ", ".join(cols)
    set_cols = [c for c in cols if c not in ("ticker", "trade_date")]
    set_cls  = ", ".join(f"target.{c} = source.{c}" for c in set_cols)
    ins_vals = ", ".join(f"source.{c}" for c in cols)
    total    = 0

    for start_idx in range(0, len(df), INSERT_BATCH):
        chunk = df.iloc[start_idx:start_idx + INSERT_BATCH]
        vals_list = [
            "(" + ", ".join(_fmt_val(v) for v in row) + ")"
            for row in chunk.itertuples(index=False)
        ]
        values_str = ", ".join(vals_list)

        merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING (SELECT * FROM (VALUES {values_str}) AS t({col_str})) AS source
ON target.ticker = source.ticker AND target.trade_date = source.trade_date
WHEN MATCHED THEN UPDATE SET {set_cls}
WHEN NOT MATCHED THEN INSERT ({col_str}) VALUES ({ins_vals})
"""
        run_sql(merge_sql, timeout_s=120)
        total += len(chunk)
        end_idx = start_idx + len(chunk)
        if end_idx < len(df):
            print(f"      {end_idx:,}/{len(df):,}...", end="\r", flush=True)

    return total


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("FazDane — Local Price Loader")
    print(f"Target : {TARGET_TABLE}")
    print(f"Tickers: {TICKERS}")
    print("=" * 60)

    # Step 1: Download
    print("\n[1] Downloading from Yahoo Finance v8 API...")
    all_dfs = []
    for ticker in TICKERS:
        df = fetch_ticker(ticker)
        if not df.empty:
            all_dfs.append(df)
        time.sleep(0.3)

    if not all_dfs:
        print("No data downloaded. Exiting.")
        sys.exit(1)

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal: {len(combined):,} rows across {combined['ticker'].nunique()} tickers")

    # Step 2: Upload
    print("\n[2] Merging into Databricks...")
    total = 0
    for ticker, group in combined.groupby("ticker"):
        print(f"  [{ticker}] {len(group):,} rows → ", end="", flush=True)
        n = insert_dataframe(group.reset_index(drop=True))
        total += n
        print("done")

    print(f"\nTotal rows merged: {total:,}")

    # Step 3: Verify
    print("\n[3] Verifying row counts in Databricks...")
    result = run_sql(
        f"SELECT ticker, COUNT(*) AS rows, MIN(trade_date) AS from_date, "
        f"MAX(trade_date) AS to_date "
        f"FROM {TARGET_TABLE} GROUP BY ticker ORDER BY ticker",
        timeout_s=60,
    )
    rows = result.get("result", {}).get("data_array", [])
    if rows:
        print(f"\n{'ticker':<8} {'rows':>6}  {'from':>12}  to_date")
        print("-" * 44)
        for row in rows:
            print(f"{row[0]:<8} {row[1]:>6}  {row[2]:>12}  {row[3]}")
    else:
        print("WARNING: No rows found in table after merge!")


if __name__ == "__main__":
    main()
