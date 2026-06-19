"""
daily_price_refresh.py
Incremental price update — fetches only the last 5 trading days from Yahoo Finance
and upserts into fazdane_finance.bronze.stock_price_raw.
Much faster than the full 5-year load (~5 minutes vs ~90 minutes).

Skips weekends and US market holidays automatically.

Run: python scripts/daily_price_refresh.py
"""
import os
import sys
import time
import warnings
import datetime

import requests
import pandas as pd

warnings.filterwarnings("ignore")
try:
    import urllib3
    urllib3.disable_warnings()
except Exception:
    pass

# ── Re-use config from local_price_loader ────────────────────────────────────
DB_HOST      = "https://dbc-225b7a82-e569.cloud.databricks.com"
DB_TOKEN     = os.environ.get("DATABRICKS_TOKEN", "")
WAREHOUSE_ID = "76e3d9414486df1d"
TARGET_TABLE = "fazdane_finance.bronze.stock_price_raw"
INSERT_BATCH = 200

YAHOO_RANGE    = "5d"   # only last 5 days — fast incremental
YAHOO_INTERVAL = "1d"

MARKET_HOLIDAYS_2026 = {
    datetime.date(2026, 1, 1), datetime.date(2026, 1, 19), datetime.date(2026, 2, 16),
    datetime.date(2026, 4, 3), datetime.date(2026, 5, 25), datetime.date(2026, 7, 3),
    datetime.date(2026, 9, 7), datetime.date(2026, 11, 26), datetime.date(2026, 11, 27),
    datetime.date(2026, 12, 25),
}

# Full 683-ticker universe
TICKERS = [
    "A", "AAL", "AAOI", "AAP", "AAPL", "ABBV", "ABT", "ACN", "ADBE", "ADI",
    "ADM", "ADP", "ADSK", "AEE", "AEIS", "AEM", "AEP", "AES", "AFL", "AGCO",
    "AIG", "AIT", "AJG", "AKAM", "ALAB", "ALB", "ALGN", "ALL", "ALLE", "ALRM",
    "AMAT", "AMD", "AME", "AMGN", "AMT", "AMZN", "ANET", "ANF", "AON", "APA",
    "APD", "APH", "APP", "APTV", "ARE", "ARGT", "ARKK", "ARM", "ARQQ", "ASML",
    "ASTS", "ATO", "AVB", "AVGO", "AVY", "AWK", "AXP", "AZO", "BA", "BABA",
    "BAC", "BALL", "BAX", "BBIO", "BBWI", "BBY", "BDC", "BDX", "BEN", "BF-B",
    "BIDU", "BITI", "BKNG", "BKR", "BLDR", "BLK", "BPOP", "BRK-B", "BRKR",
    "BRO", "BSX", "BUD", "BX", "BXP", "BYND", "C", "CAG", "CAH", "CARR",
    "CASY", "CAT", "CAVA", "CB", "CBRE", "CC", "CCI", "CDE", "CDNS", "CE",
    "CEG", "CF", "CFG", "CHD", "CHRW", "CHTR", "CL", "CLS", "CLX", "CMA",
    "CMCSA", "CME", "CMG", "CMS", "CNC", "CNP", "COF", "COHR", "COIN", "COO",
    "COP", "COR", "COST", "CPB", "CPRT", "CPT", "CRDO", "CRL", "CRM", "CRS",
    "CRWD", "CSCO", "CSX", "CTAS", "CTRA", "CTSH", "CTVA", "CVNA", "CVS",
    "CVX", "CYD", "D", "DAL", "DASH", "DDOG", "DE", "DECK", "DELL", "DG",
    "DGX", "DHI", "DHR", "DHX", "DIA", "DIS", "DK", "DKNG", "DKS", "DLR",
    "DLTR", "DOC", "DOV", "DOW", "DPST", "DPZ", "DRI", "DTE", "DUK", "DVA",
    "DVN", "DWSN", "DXC", "DXCM", "DY", "EA", "EBAY", "ECH", "ECL", "ED",
    "EDEN", "EEM", "EFNL", "EFX", "EIDO", "EIRL", "EIX", "EL", "ELF", "ELV",
    "EMN", "EMR", "ENPH", "ENSG", "EOG", "EPAM", "EPHE", "EPI", "EPOL", "EQIX",
    "EQR", "EQT", "ES", "ESS", "ETN", "ETR", "ETSY", "EVRG", "EW", "EWA",
    "EWC", "EWD", "EWG", "EWH", "EWI", "EWJ", "EWK", "EWL", "EWM", "EWN",
    "EWO", "EWP", "EWQ", "EWS", "EWT", "EWU", "EWW", "EWY", "EWZ", "EWZS",
    "EXC", "EXE", "EXPE", "EXR", "EZA", "F", "FANG", "FAST", "FCX", "FDX",
    "FE", "FICO", "FITB", "FIX", "FLUT", "FMC", "FN", "FOX", "FOXA", "FSLR",
    "FSLY", "FTAI", "FTNT", "FUTU", "FXI", "GD", "GDDY", "GDX", "GDXJ", "GE",
    "GEHC", "GEV", "GH", "GILD", "GIS", "GL", "GLD", "GLPI", "GLW", "GM",
    "GNRC", "GOOG", "GOOGL", "GPC", "GRAL", "GREK", "GRMN", "GS", "GTLS",
    "GVA", "GWW", "GXG", "H", "HAL", "HAS", "HBAN", "HCA", "HD", "HIMS",
    "HKD", "HL", "HLT", "HOLX", "HON", "HOOD", "HPE", "HPQ", "HSBC", "HSIC",
    "HST", "HSY", "HTH", "HUBB", "HUM", "HWM", "HYG", "IBIT", "IBKR", "IBM",
    "ICE", "IDCC", "IDXX", "IEX", "IFF", "INDA", "INSM", "INTC", "INTU",
    "IONQ", "IP", "IQV", "IREN", "IRM", "ISRG", "ITW", "IVZ", "IWM", "JBHT",
    "JBL", "JD", "JKHY", "JNJ", "JOYY", "JPM", "KB", "KEY", "KEYS", "KGS",
    "KHC", "KIM", "KKR", "KLAC", "KMB", "KMI", "KMX", "KO", "KOS", "KRE",
    "KSA", "KTOS", "L", "LDOS", "LECO", "LEN", "LEU", "LFVN", "LI", "LIN",
    "LITE", "LKQ", "LLY", "LMND", "LMT", "LNT", "LNW", "LOW", "LRCX", "LULU",
    "LUV", "LVS", "LW", "LYB", "LYV", "MA", "MAA", "MAR", "MARA", "MARB",
    "MCD", "MCHI", "MCK", "MCO", "MDB", "MDLZ", "MDT", "MELI", "MET", "META",
    "MGA", "MHK", "MKC", "MLM", "MMC", "MMM", "MNST", "MO", "MOD", "MOH",
    "MOS", "MPC", "MRK", "MRVL", "MS", "MSCI", "MSFT", "MSGS", "MSI", "MSTR",
    "MTB", "MTCH", "MTD", "MTDR", "MTH", "MTN", "MU", "NDSN", "NEE", "NEM",
    "NET", "NFLX", "NI", "NIO", "NKE", "NNE", "NNN", "NOC", "NORW", "NOW",
    "NPO", "NRG", "NSC", "NTAP", "NTR", "NTRA", "NTRS", "NUE", "NVDA", "NVR",
    "NWL", "NWS", "NWSA", "NXP", "NXPI", "NXT", "O", "ODFL", "OKE", "OLLI",
    "OMC", "ON", "ONTO", "ORCL", "ORLA", "ORLY", "OXY", "PANW", "PCAR", "PCG",
    "PDD", "PEG", "PENN", "PEP", "PFE", "PG", "PGR", "PH", "PHM", "PINS",
    "PKG", "PLD", "PLNT", "PLTR", "PM", "PNC", "PNW", "POOL", "PPG", "PPL",
    "PRCT", "PRU", "PSA", "PSX", "PYPL", "QAT", "QCOM", "QQQ", "QRVO", "QXO",
    "RBLX", "RCL", "RDDT", "REG", "REGN", "RF", "RIO", "RIOT", "RIVN", "RL",
    "RMBS", "ROKU", "ROP", "RRBI", "RRX", "RSG", "RSP", "RTX", "RVTY", "RYAN",
    "SAIA", "SATS", "SBAC", "SBNY", "SBUX", "SCHW", "SE", "SEZL", "SF",
    "SHAK", "SHEL", "SHG", "SHOP", "SHW", "SIG", "SITM", "SJM", "SLB", "SLV",
    "SMCI", "SMH", "SN", "SNA", "SNAP", "SNDK", "SNOW", "SNPS", "SO", "SOFI",
    "SOXL", "SPG", "SPGI", "SPHR", "SPMO", "SPOT", "SPXC", "SPY", "SRE",
    "SRPT", "STE", "STNE", "STRL", "STT", "STX", "STZ", "SYK", "SYY", "T",
    "TAP", "TDG", "TDY", "TEAM", "TEL", "TER", "TFX", "TGT", "THD", "TJX",
    "TKO", "TKR", "TLN", "TLT", "TM", "TMO", "TMUS", "TOL", "TPH", "TPL",
    "TPR", "TRGP", "TRMB", "TROW", "TROX", "TRV", "TSCO", "TSLA", "TSM",
    "TSN", "TT", "TTMI", "TTWO", "TUR", "TXN", "TXT", "TYL", "UAE", "UAL",
    "UBER", "UDR", "UI", "ULTA", "UMBF", "UNG", "UNH", "UNP", "UPS", "UPST",
    "URI", "USB", "USO", "V", "VAL", "VEEV", "VFC", "VICI", "VLO", "VMC",
    "VOO", "VRE", "VRNS", "VRSK", "VRSN", "VRT", "VRTX", "VST", "VTR", "VTRS",
    "VZ", "WAL", "WAT", "WBD", "WCC", "WDAY", "WDC", "WEC", "WELL", "WFC",
    "WHR", "WM", "WMB", "WMT", "WPM", "WST", "WTS", "WTTR", "WY", "WYNN",
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV",
    "XLY", "XOM", "XOP", "XPEV", "XPO", "XRAY", "XYL", "YUM", "ZBH", "ZION",
    "ZS", "ZTS",
    # Yahoo-compatible indices
    "^DJI", "^GSPC", "^IXIC", "^NDX", "^NYA", "^RUT", "^VIX",
    # Continuous futures
    "BTC=F", "CL=F", "ES=F", "GC=F", "HG=F", "NQ=F", "RTY=F", "YM=F",
]

SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

DB_HEADERS = {
    "Authorization": f"Bearer {DB_TOKEN}",
    "Content-Type": "application/json",
}


def is_trading_day(dt: datetime.date = None) -> bool:
    if dt is None:
        dt = datetime.date.today()
    if dt.weekday() >= 5:
        return False
    if dt in MARKET_HOLIDAYS_2026:
        return False
    return True


def fetch_ticker(ticker: str) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": YAHOO_INTERVAL, "range": YAHOO_RANGE, "events": "history"}
    try:
        r = SESSION.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        result = data["chart"]["result"][0]
        ts     = result["timestamp"]
        ohlcv  = result["indicators"]["quote"][0]
        adj    = result.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [None] * len(ts))
        rows = []
        for i, t in enumerate(ts):
            dt = datetime.date.fromtimestamp(t)
            if not is_trading_day(dt):
                continue
            rows.append({
                "ticker":       ticker,
                "trade_date":   dt,
                "open":         ohlcv["open"][i],
                "high":         ohlcv["high"][i],
                "low":          ohlcv["low"][i],
                "close":        ohlcv["close"][i],
                "adj_close":    adj[i] if adj else None,
                "volume":       ohlcv["volume"][i],
                "source":       "yahoo_v8",
                "ingestion_ts": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"  {ticker}... ERROR — {e}")
        return pd.DataFrame()


def run_sql(statement: str, timeout_s: int = 120) -> dict:
    payload = {"statement": statement, "warehouse_id": WAREHOUSE_ID, "wait_timeout": "0s"}
    resp = SESSION.post(f"{DB_HOST}/api/2.0/sql/statements", headers=DB_HEADERS, json=payload, timeout=timeout_s + 10)
    resp.raise_for_status()
    result  = resp.json()
    state   = result.get("status", {}).get("state", "UNKNOWN")
    stmt_id = result.get("statement_id", "")
    for _ in range(timeout_s):
        if state == "SUCCEEDED":
            return result
        if state in ("FAILED", "CANCELED", "CLOSED"):
            raise RuntimeError(f"SQL failed [{state}]: {result}")
        time.sleep(2)
        poll = SESSION.get(f"{DB_HOST}/api/2.0/sql/statements/{stmt_id}", headers=DB_HEADERS, timeout=30)
        poll.raise_for_status()
        result = poll.json()
        state  = result.get("status", {}).get("state", "UNKNOWN")
    raise TimeoutError(f"SQL {stmt_id} timed out")


def _fmt(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"
    if isinstance(v, datetime.date):
        return f"DATE '{v.strftime('%Y-%m-%d')}'"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def upsert_df(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    cols     = list(df.columns)
    col_str  = ", ".join(cols)
    set_cols = [c for c in cols if c not in ("ticker", "trade_date")]
    set_cls  = ", ".join(f"target.{c} = source.{c}" for c in set_cols)
    ins_vals = ", ".join(f"source.{c}" for c in cols)
    total    = 0
    for start in range(0, len(df), INSERT_BATCH):
        chunk = df.iloc[start:start + INSERT_BATCH]
        vals  = ["(" + ", ".join(_fmt(v) for v in row) + ")" for row in chunk.itertuples(index=False)]
        merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING (SELECT * FROM (VALUES {", ".join(vals)}) AS t({col_str})) AS source
ON target.ticker = source.ticker AND target.trade_date = source.trade_date
WHEN MATCHED THEN UPDATE SET {set_cls}
WHEN NOT MATCHED THEN INSERT ({col_str}) VALUES ({ins_vals})
"""
        run_sql(merge_sql, timeout_s=120)
        total += len(chunk)
    return total


def main():
    today = datetime.date.today()
    print("=" * 60)
    print(f"FazDane — Daily Price Refresh ({today})")
    print(f"Range: last {YAHOO_RANGE} | Tickers: {len(TICKERS)}")
    print("=" * 60)

    if not is_trading_day(today):
        print(f"\n{today} is not a trading day. Exiting.")
        sys.exit(0)

    if not DB_TOKEN:
        print("ERROR: DATABRICKS_TOKEN not set.")
        sys.exit(1)

    print(f"\nDownloading last {YAHOO_RANGE} for {len(TICKERS)} tickers...")
    all_dfs = []
    for ticker in TICKERS:
        df = fetch_ticker(ticker)
        if not df.empty:
            print(f"  {ticker}... {len(df)} rows")
            all_dfs.append(df)
        time.sleep(0.2)

    if not all_dfs:
        print("No data. Exiting.")
        sys.exit(1)

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal: {len(combined):,} rows across {combined['ticker'].nunique()} tickers")

    print("\nUploading to Databricks...")
    total = 0
    for ticker, group in combined.groupby("ticker"):
        n = upsert_df(group.reset_index(drop=True))
        total += n
    print(f"Done. {total:,} rows merged.")


if __name__ == "__main__":
    main()
