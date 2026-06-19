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
INSERT_BATCH = 200   # rows per MERGE VALUES statement

TICKERS = [
    "A",
    "AAL",
    "AAOI",
    "AAP",
    "AAPL",
    "ABBV",
    "ABT",
    "ACN",
    "ADBE",
    "ADI",
    "ADM",
    "ADP",
    "ADSK",
    "AEE",
    "AEIS",
    "AEM",
    "AEP",
    "AES",
    "AFL",
    "AGCO",
    "AIG",
    "AIT",
    "AJG",
    "AKAM",
    "ALAB",
    "ALB",
    "ALGN",
    "ALL",
    "ALLE",
    "ALRM",
    "AMAT",
    "AMD",
    "AME",
    "AMGN",
    "AMT",
    "AMZN",
    "ANET",
    "ANF",
    "AON",
    "APA",
    "APD",
    "APH",
    "APP",
    "APTV",
    "ARE",
    "ARGT",
    "ARKK",
    "ARM",
    "ARQQ",
    "ASML",
    "ASTS",
    "ATO",
    "AVB",
    "AVGO",
    "AVY",
    "AWK",
    "AXP",
    "AZO",
    "BA",
    "BABA",
    "BAC",
    "BALL",
    "BAX",
    "BBIO",
    "BBWI",
    "BBY",
    "BDC",
    "BDX",
    "BEN",
    "BF-B",
    "BIDU",
    "BITI",
    "BKNG",
    "BKR",
    "BLDR",
    "BLK",
    "BPOP",
    "BRK-B",
    "BRKR",
    "BRO",
    "BSX",
    "BUD",
    "BX",
    "BXP",
    "BYND",
    "C",
    "CAG",
    "CAH",
    "CARR",
    "CASY",
    "CAT",
    "CAVA",
    "CB",
    "CBRE",
    "CC",
    "CCI",
    "CDE",
    "CDNS",
    "CE",
    "CEG",
    "CF",
    "CFG",
    "CHD",
    "CHRW",
    "CHTR",
    "CL",
    "CLS",
    "CLX",
    "CMA",
    "CMCSA",
    "CME",
    "CMG",
    "CMS",
    "CNC",
    "CNP",
    "COF",
    "COHR",
    "COIN",
    "COO",
    "COP",
    "COR",
    "COST",
    "CPB",
    "CPRT",
    "CPT",
    "CRDO",
    "CRL",
    "CRM",
    "CRS",
    "CRWD",
    "CSCO",
    "CSX",
    "CTAS",
    "CTRA",
    "CTSH",
    "CTVA",
    "CVNA",
    "CVS",
    "CVX",
    "CYD",
    "D",
    "DAL",
    "DASH",
    "DDOG",
    "DE",
    "DECK",
    "DELL",
    "DG",
    "DGX",
    "DHI",
    "DHR",
    "DHX",
    "DIA",
    "DIS",
    "DK",
    "DKNG",
    "DKS",
    "DLR",
    "DLTR",
    "DOC",
    "DOV",
    "DOW",
    "DPST",
    "DPZ",
    "DRI",
    "DTE",
    "DUK",
    "DVA",
    "DVN",
    "DWSN",
    "DXC",
    "DXCM",
    "DY",
    "EA",
    "EBAY",
    "ECH",
    "ECL",
    "ED",
    "EDEN",
    "EEM",
    "EFNL",
    "EFX",
    "EIDO",
    "EIRL",
    "EIX",
    "EL",
    "ELF",
    "ELV",
    "EMN",
    "EMR",
    "ENPH",
    "ENSG",
    "EOG",
    "EPAM",
    "EPHE",
    "EPI",
    "EPOL",
    "EQIX",
    "EQR",
    "EQT",
    "ES",
    "ESS",
    "ETN",
    "ETR",
    "ETSY",
    "EVRG",
    "EW",
    "EWA",
    "EWC",
    "EWD",
    "EWG",
    "EWH",
    "EWI",
    "EWJ",
    "EWK",
    "EWL",
    "EWM",
    "EWN",
    "EWO",
    "EWP",
    "EWQ",
    "EWS",
    "EWT",
    "EWU",
    "EWW",
    "EWY",
    "EWZ",
    "EWZS",
    "EXC",
    "EXE",
    "EXPE",
    "EXR",
    "EZA",
    "F",
    "FANG",
    "FAST",
    "FCX",
    "FDX",
    "FE",
    "FICO",
    "FITB",
    "FIX",
    "FLUT",
    "FMC",
    "FN",
    "FOX",
    "FOXA",
    "FSLR",
    "FSLY",
    "FTAI",
    "FTNT",
    "FUTU",
    "FXI",
    "GD",
    "GDDY",
    "GDX",
    "GDXJ",
    "GE",
    "GEHC",
    "GEV",
    "GH",
    "GILD",
    "GIS",
    "GL",
    "GLD",
    "GLPI",
    "GLW",
    "GM",
    "GNRC",
    "GOOG",
    "GOOGL",
    "GPC",
    "GRAL",
    "GREK",
    "GRMN",
    "GS",
    "GTLS",
    "GVA",
    "GWW",
    "GXG",
    "H",
    "HAL",
    "HAS",
    "HBAN",
    "HCA",
    "HD",
    "HIMS",
    "HKD",
    "HL",
    "HLT",
    "HOLX",
    "HON",
    "HOOD",
    "HPE",
    "HPQ",
    "HSBC",
    "HSIC",
    "HST",
    "HSY",
    "HTH",
    "HUBB",
    "HUM",
    "HWM",
    "HYG",
    "IBIT",
    "IBKR",
    "IBM",
    "ICE",
    "IDCC",
    "IDXX",
    "IEX",
    "IFF",
    "INDA",
    "INSM",
    "INTC",
    "INTU",
    "IONQ",
    "IP",
    "IQV",
    "IREN",
    "IRM",
    "ISRG",
    "ITW",
    "IVZ",
    "IWM",
    "JBHT",
    "JBL",
    "JD",
    "JKHY",
    "JNJ",
    "JOYY",
    "JPM",
    "KB",
    "KEY",
    "KEYS",
    "KGS",
    "KHC",
    "KIM",
    "KKR",
    "KLAC",
    "KMB",
    "KMI",
    "KMX",
    "KO",
    "KOS",
    "KRE",
    "KSA",
    "KTOS",
    "L",
    "LDOS",
    "LECO",
    "LEN",
    "LEU",
    "LFVN",
    "LI",
    "LIN",
    "LITE",
    "LKQ",
    "LLY",
    "LMND",
    "LMT",
    "LNT",
    "LNW",
    "LOW",
    "LRCX",
    "LULU",
    "LUV",
    "LVS",
    "LW",
    "LYB",
    "LYV",
    "MA",
    "MAA",
    "MAR",
    "MARA",
    "MARB",
    "MCD",
    "MCHI",
    "MCK",
    "MCO",
    "MDB",
    "MDLZ",
    "MDT",
    "MELI",
    "MET",
    "META",
    "MGA",
    "MHK",
    "MKC",
    "MLM",
    "MMC",
    "MMM",
    "MNST",
    "MO",
    "MOD",
    "MOH",
    "MOS",
    "MPC",
    "MRK",
    "MRVL",
    "MS",
    "MSCI",
    "MSFT",
    "MSGS",
    "MSI",
    "MSTR",
    "MTB",
    "MTCH",
    "MTD",
    "MTDR",
    "MTH",
    "MTN",
    "MU",
    "NDSN",
    "NEE",
    "NEM",
    "NET",
    "NFLX",
    "NI",
    "NIO",
    "NKE",
    "NNE",
    "NNN",
    "NOC",
    "NORW",
    "NOW",
    "NPO",
    "NRG",
    "NSC",
    "NTAP",
    "NTR",
    "NTRA",
    "NTRS",
    "NUE",
    "NVDA",
    "NVR",
    "NWL",
    "NWS",
    "NWSA",
    "NXP",
    "NXPI",
    "NXT",
    "O",
    "ODFL",
    "OKE",
    "OLLI",
    "OMC",
    "ON",
    "ONTO",
    "ORCL",
    "ORLA",
    "ORLY",
    "OXY",
    "PANW",
    "PCAR",
    "PCG",
    "PDD",
    "PEG",
    "PENN",
    "PEP",
    "PFE",
    "PG",
    "PGR",
    "PH",
    "PHM",
    "PINS",
    "PKG",
    "PLD",
    "PLNT",
    "PLTR",
    "PM",
    "PNC",
    "PNW",
    "POOL",
    "PPG",
    "PPL",
    "PRCT",
    "PRU",
    "PSA",
    "PSX",
    "PYPL",
    "QAT",
    "QCOM",
    "QQQ",
    "QRVO",
    "QXO",
    "RBLX",
    "RCL",
    "RDDT",
    "REG",
    "REGN",
    "RF",
    "RIO",
    "RIOT",
    "RIVN",
    "RL",
    "RMBS",
    "ROKU",
    "ROP",
    "RRBI",
    "RRX",
    "RSG",
    "RSP",
    "RTX",
    "RVTY",
    "RYAN",
    "SAIA",
    "SATS",
    "SBAC",
    "SBNY",
    "SBUX",
    "SCHW",
    "SE",
    "SEE",
    "SEZL",
    "SF",
    "SHAK",
    "SHEL",
    "SHG",
    "SHOP",
    "SHW",
    "SIG",
    "SITM",
    "SJM",
    "SLB",
    "SLV",
    "SMCI",
    "SMH",
    "SN",
    "SNA",
    "SNAP",
    "SNDK",
    "SNOW",
    "SNPS",
    "SO",
    "SOFI",
    "SOXL",
    "SPG",
    "SPGI",
    "SPHR",
    "SPMO",
    "SPOT",
    "SPXC",
    "SPXW",
    "SPY",
    "SQ",
    "SRE",
    "SRPT",
    "STE",
    "STNE",
    "STRL",
    "STT",
    "STX",
    "STZ",
    "SYK",
    "SYY",
    "T",
    "TAP",
    "TDG",
    "TDY",
    "TEAM",
    "TEL",
    "TER",
    "TFX",
    "TGT",
    "THD",
    "TJX",
    "TKO",
    "TKR",
    "TLN",
    "TLT",
    "TM",
    "TMO",
    "TMUS",
    "TOL",
    "TPH",
    "TPL",
    "TPR",
    "TRGP",
    "TRMB",
    "TROW",
    "TROX",
    "TRV",
    "TSCO",
    "TSLA",
    "TSM",
    "TSN",
    "TT",
    "TTMI",
    "TTWO",
    "TUR",
    "TXN",
    "TXT",
    "TYL",
    "UAE",
    "UAL",
    "UBER",
    "UDR",
    "UI",
    "ULTA",
    "UMBF",
    "UNG",
    "UNH",
    "UNP",
    "UPS",
    "UPST",
    "URI",
    "USB",
    "USO",
    "V",
    "VAL",
    "VEEV",
    "VFC",
    "VICI",
    "VLO",
    "VMC",
    "VOO",
    "VRE",
    "VRNS",
    "VRSK",
    "VRSN",
    "VRT",
    "VRTX",
    "VST",
    "VTR",
    "VTRS",
    "VZ",
    "WAL",
    "WAT",
    "WBD",
    "WCC",
    "WDAY",
    "WDC",
    "WEC",
    "WELL",
    "WFC",
    "WHR",
    "WM",
    "WMB",
    "WMT",
    "WPM",
    "WST",
    "WTS",
    "WTTR",
    "WY",
    "WYNN",
    "XLB",
    "XLC",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLRE",
    "XLU",
    "XLV",
    "XLY",
    "XOM",
    "XOP",
    "XPEV",
    "XPO",
    "XRAY",
    "XYL",
    "YUM",
    "ZBH",
    "ZION",
    "ZS",
    "ZTS",
    # Yahoo-compatible indices
    "^DJI",
    "^GSPC",
    "^IXIC",
    "^NDX",
    "^NYA",
    "^RUT",
    "^VIX",
    # Continuous futures
    "BTC=F",
    "CL=F",
    "ES=F",
    "GC=F",
    "HG=F",
    "NQ=F",
    "RTY=F",
    "YM=F",
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
