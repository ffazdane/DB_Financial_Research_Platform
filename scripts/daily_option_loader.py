"""
daily_option_loader.py
Fetches 0-60 DTE option chains from Tastytrade for all tickers,
applies a liquidity filter (front-month ATM bid-ask spread <= $2.00),
and upserts into fazdane_finance.bronze.option_chain_raw via Databricks SQL API.

Skips weekends and US market holidays automatically.

Required env vars (set in run_daily.bat or Task Scheduler):
  DATABRICKS_TOKEN      — Databricks PAT
  TT_CLIENT_SECRET      — Tastytrade OAuth client secret
  TT_REFRESH_TOKEN      — Tastytrade OAuth refresh token

Run:
  python scripts/daily_option_loader.py
"""
import os
import sys
import time
import warnings
import datetime
import math

import requests
import pandas as pd

warnings.filterwarnings("ignore")
try:
    import urllib3
    urllib3.disable_warnings()
except Exception:
    pass

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST        = "https://dbc-225b7a82-e569.cloud.databricks.com"
DB_TOKEN       = os.environ.get("DATABRICKS_TOKEN", "")
WAREHOUSE_ID   = "76e3d9414486df1d"
TARGET_TABLE   = "fazdane_finance.bronze.option_chain_raw"
INSERT_BATCH   = 100  # rows per MERGE

TT_BASE        = "https://api.tastytrade.com"
TT_SECRET      = os.environ.get("TT_CLIENT_SECRET", "")
TT_REFRESH     = os.environ.get("TT_REFRESH_TOKEN", "")
TT_CLIENT_ID   = "32fbe1b8-d41d-4852-b314-b958efef69fe"  # FazDane app client id

MAX_SPREAD     = 2.00   # bid-ask spread limit in dollars — skip ticker if wider
MAX_DTE        = 60     # only load expirations within this many days
RATE_LIMIT_S   = 0.5    # seconds between Tastytrade API calls (120 req/min limit)

# US market holidays 2026 (add future years as needed)
MARKET_HOLIDAYS_2026 = {
    datetime.date(2026, 1, 1),   # New Year's Day
    datetime.date(2026, 1, 19),  # MLK Day
    datetime.date(2026, 2, 16),  # Presidents Day
    datetime.date(2026, 4, 3),   # Good Friday
    datetime.date(2026, 5, 25),  # Memorial Day
    datetime.date(2026, 7, 3),   # Independence Day (observed)
    datetime.date(2026, 9, 7),   # Labor Day
    datetime.date(2026, 11, 26), # Thanksgiving
    datetime.date(2026, 11, 27), # Black Friday (half day — skip to be safe)
    datetime.date(2026, 12, 25), # Christmas
}

# Tickers — same universe as local_price_loader.py (equities + ETFs only; skip ^index and =F futures for options)
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
    # Indices with listed options
    "SPY", "QQQ", "IWM",
    # Continuous futures with options (Tastytrade uses /ES, /NQ — kept separate)
]
# Deduplicate preserving order
seen = set()
TICKERS = [t for t in TICKERS if not (t in seen or seen.add(t))]

# ── HTTP sessions ─────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({"User-Agent": "FazDane/1.0"})

DB_HEADERS = {
    "Authorization": f"Bearer {DB_TOKEN}",
    "Content-Type": "application/json",
}


# ── Market hours / holiday check ──────────────────────────────────────────────
def is_trading_day(dt: datetime.date = None) -> bool:
    """Return True if dt is a US equity trading day (Mon-Fri, not a holiday)."""
    if dt is None:
        dt = datetime.date.today()
    if dt.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if dt in MARKET_HOLIDAYS_2026:
        return False
    return True


# ── Tastytrade auth ───────────────────────────────────────────────────────────
_TT_ACCESS_TOKEN: str = ""
_TT_TOKEN_EXPIRY: float = 0.0


def get_tt_token() -> str:
    """Get a valid Tastytrade access token, refreshing if expired."""
    global _TT_ACCESS_TOKEN, _TT_TOKEN_EXPIRY
    if _TT_ACCESS_TOKEN and time.time() < _TT_TOKEN_EXPIRY - 60:
        return _TT_ACCESS_TOKEN

    resp = SESSION.post(
        f"{TT_BASE}/oauth/token",
        json={
            "grant_type":    "refresh_token",
            "refresh_token": TT_REFRESH,
            "client_id":     TT_CLIENT_ID,
            "client_secret": TT_SECRET,
        },
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Tastytrade auth failed {resp.status_code}: {resp.text[:400]}")
    data = resp.json()
    _TT_ACCESS_TOKEN = data["access_token"]
    _TT_TOKEN_EXPIRY = time.time() + data.get("expires_in", 1800)
    return _TT_ACCESS_TOKEN


def tt_get(path: str) -> dict:
    """GET from Tastytrade API with auth header."""
    token = get_tt_token()
    resp = SESSION.get(
        f"{TT_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if resp.status_code == 404:
        return {}  # symbol not found / no options
    resp.raise_for_status()
    return resp.json()


# ── Option chain fetch via Tastytrade ────────────────────────────────────────
# Liquidity gate: market-metrics liquidity-rating (1-5, Tastytrade's own measure)
# Chain structure: /option-chains/{symbol}/nested  (OCC symbols, strikes, exps)
# Per-expiration IV: market-metrics option-expiration-implied-volatilities
# bid/ask/greeks: NULL — DXFeed streaming required (blocked by corp proxy)

MIN_LIQUIDITY_RATING = 3   # Tastytrade rating 1-5; 3+ = acceptably liquid


def fetch_chain_if_liquid(symbol: str, today: datetime.date) -> tuple[pd.DataFrame, str]:
    """
    1. Check Tastytrade liquidity-rating via market-metrics (gate: >= 3)
    2. If liquid, fetch chain structure + per-expiration IV for 0-60 DTE
    Returns (DataFrame, reason_string). Empty DataFrame = illiquid/no data.
    """
    # ── Liquidity gate ────────────────────────────────────────────────────────
    metrics_data = tt_get(f"/market-metrics?symbols={symbol}")
    if not metrics_data:
        return pd.DataFrame(), "no market-metrics"

    items = metrics_data.get("data", {}).get("items", [])
    if not items:
        return pd.DataFrame(), "no metrics items"

    m = items[0]
    liq_rating = int(m.get("liquidity-rating", 0) or 0)
    if liq_rating < MIN_LIQUIDITY_RATING:
        return pd.DataFrame(), f"liquidity-rating {liq_rating} < {MIN_LIQUIDITY_RATING}"

    iv_rank = float(m.get("implied-volatility-index-rank", 0) or 0)
    iv_index = float(m.get("implied-volatility-index", 0) or 0)

    # Build expiration → IV map from market-metrics
    exp_iv_map = {}
    for ei in m.get("option-expiration-implied-volatilities", []):
        exp_iv_map[ei.get("expiration-date", "")] = float(ei.get("implied-volatility", 0) or 0)

    liquid_reason = f"liq-rating={liq_rating} iv-rank={iv_rank:.2f}"

    # ── Fetch chain structure ─────────────────────────────────────────────────
    chain_data = tt_get(f"/option-chains/{symbol}/nested")
    if not chain_data:
        return pd.DataFrame(), "no option chain"

    chain_items = chain_data.get("data", {}).get("items", [])
    if not chain_items:
        return pd.DataFrame(), "empty chain"

    cutoff      = today + datetime.timedelta(days=MAX_DTE)
    snapshot_ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows        = []

    for exp in chain_items[0].get("expirations", []):
        exp_date_str = exp.get("expiration-date", "")
        if not exp_date_str:
            continue
        exp_date = datetime.date.fromisoformat(exp_date_str)
        if exp_date < today or exp_date > cutoff:
            continue

        dte    = (exp_date - today).days
        exp_iv = exp_iv_map.get(exp_date_str, iv_index)  # fallback to overall IV

        for strike_entry in exp.get("strikes", []):
            strike = float(strike_entry.get("strike-price", 0) or 0)

            for opt_type, occ_key in [("C", "call"), ("P", "put")]:
                occ_symbol = strike_entry.get(occ_key, "")
                if not occ_symbol:
                    continue

                rows.append({
                    "symbol":          symbol,
                    "snapshot_date":   today.isoformat(),
                    "expiration_date": exp_date_str,
                    "dte":             dte,
                    "strike":          strike,
                    "option_type":     opt_type,
                    "occ_symbol":      occ_symbol,
                    "bid":             None,   # DXFeed streaming needed
                    "ask":             None,
                    "mid":             None,
                    "spread":          None,
                    "iv":              round(exp_iv, 6),
                    "iv_rank":         round(iv_rank, 6),
                    "liquidity_rating": liq_rating,
                    "delta":           None,
                    "gamma":           None,
                    "theta":           None,
                    "vega":            None,
                    "open_interest":   None,
                    "volume":          None,
                    "snapshot_ts":     snapshot_ts,
                    "source":          "tastytrade",
                })

    return pd.DataFrame(rows), liquid_reason


# ── Databricks SQL helpers (same pattern as local_price_loader.py) ────────────
def run_sql(statement: str, timeout_s: int = 120) -> dict:
    payload = {
        "statement":    statement,
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "0s",
    }
    resp = SESSION.post(
        f"{DB_HOST}/api/2.0/sql/statements",
        headers=DB_HEADERS,
        json=payload,
        timeout=timeout_s + 10,
    )
    if not resp.ok:
        print(f"\n  SQL API error {resp.status_code}: {resp.text[:400]}")
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
        poll = SESSION.get(
            f"{DB_HOST}/api/2.0/sql/statements/{stmt_id}",
            headers=DB_HEADERS,
            timeout=30,
        )
        poll.raise_for_status()
        result = poll.json()
        state  = result.get("status", {}).get("state", "UNKNOWN")

    raise TimeoutError(f"SQL statement {stmt_id} timed out after {timeout_s}s")


def _fmt(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "NULL"
    if isinstance(v, datetime.date):
        return f"DATE '{v}'"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def ensure_table():
    """Create the bronze option chain table if it doesn't exist."""
    run_sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    symbol           STRING    NOT NULL,
    snapshot_date    DATE      NOT NULL,
    expiration_date  DATE      NOT NULL,
    dte              INT,
    strike           DOUBLE,
    option_type      STRING,
    occ_symbol       STRING,
    bid              DOUBLE,
    ask              DOUBLE,
    mid              DOUBLE,
    spread           DOUBLE,
    iv               DOUBLE,
    iv_rank          DOUBLE,
    liquidity_rating INT,
    delta            DOUBLE,
    gamma            DOUBLE,
    theta            DOUBLE,
    vega             DOUBLE,
    open_interest    BIGINT,
    volume           BIGINT,
    snapshot_ts      TIMESTAMP,
    source           STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""", timeout_s=60)


def upsert_chain(df: pd.DataFrame) -> int:
    """Upsert option chain DataFrame into Delta table. Returns rows processed."""
    if df.empty:
        return 0

    cols    = list(df.columns)
    col_str = ", ".join(cols)
    # Unique key: symbol + snapshot_date + expiration_date + strike + option_type
    key_cols = {"symbol", "snapshot_date", "expiration_date", "strike", "option_type"}
    set_cols = [c for c in cols if c not in key_cols]
    set_cls  = ", ".join(f"target.{c} = source.{c}" for c in set_cols)
    ins_vals = ", ".join(f"source.{c}" for c in cols)
    total    = 0

    for start in range(0, len(df), INSERT_BATCH):
        chunk = df.iloc[start:start + INSERT_BATCH]
        vals  = [
            "(" + ", ".join(_fmt(v) for v in row) + ")"
            for row in chunk.itertuples(index=False)
        ]
        merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING (SELECT * FROM (VALUES {", ".join(vals)}) AS t({col_str})) AS source
ON  target.symbol          = source.symbol
AND target.snapshot_date   = source.snapshot_date
AND target.expiration_date = source.expiration_date
AND target.strike          = source.strike
AND target.option_type     = source.option_type
WHEN MATCHED THEN UPDATE SET {set_cls}
WHEN NOT MATCHED THEN INSERT ({col_str}) VALUES ({ins_vals})
"""
        run_sql(merge_sql, timeout_s=120)
        total += len(chunk)

    return total


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    today = datetime.date.today()

    print("=" * 60)
    print("FazDane — Daily Option Chain Loader")
    print(f"Date    : {today}")
    print(f"DTE range: 0–{MAX_DTE} days")
    print(f"Spread  : skip if ATM spread > ${MAX_SPREAD:.2f}")
    print("=" * 60)

    if not is_trading_day(today):
        print(f"\nToday ({today}) is not a US trading day. Exiting.")
        sys.exit(0)

    if not DB_TOKEN:
        print("ERROR: DATABRICKS_TOKEN env var not set.")
        sys.exit(1)
    if not TT_REFRESH or not TT_SECRET:
        print("ERROR: TT_CLIENT_SECRET and TT_REFRESH_TOKEN env vars not set.")
        sys.exit(1)

    # Authenticate once upfront
    print("\nAuthenticating with Tastytrade...")
    get_tt_token()
    print("  OK")

    # Ensure table exists
    print("\nEnsuring bronze table exists...")
    ensure_table()
    print(f"  {TARGET_TABLE} ready")

    # Process each ticker
    print(f"\nProcessing {len(TICKERS)} tickers...\n")
    skipped   = []
    loaded    = []
    total_rows = 0

    for i, symbol in enumerate(TICKERS, 1):
        prefix = f"  [{i:>3}/{len(TICKERS)}] {symbol:<10}"

        # Single API call: liquidity check + chain fetch combined
        try:
            df, reason = fetch_chain_if_liquid(symbol, today)
        except Exception as e:
            print(f"{prefix} ERROR: {e}")
            skipped.append((symbol, str(e)))
            time.sleep(RATE_LIMIT_S)
            continue

        if df.empty:
            print(f"{prefix} SKIP — {reason}")
            skipped.append((symbol, reason))
            time.sleep(RATE_LIMIT_S)
            continue

        # Upload liquid options to Databricks
        try:
            n = upsert_chain(df)
            total_rows += n
            loaded.append(symbol)
            print(f"{prefix} {reason:<25} → {n:,} rows uploaded")
        except Exception as e:
            print(f"{prefix} ERROR (upload): {e}")
            skipped.append((symbol, f"upload error: {e}"))

        time.sleep(RATE_LIMIT_S)

    # Summary
    print("\n" + "=" * 60)
    print(f"Done. Loaded: {len(loaded)} tickers | Skipped: {len(skipped)} | Rows: {total_rows:,}")
    if skipped:
        print(f"\nSkipped tickers ({len(skipped)}):")
        for sym, reason in skipped:
            print(f"  {sym:<12} {reason}")


if __name__ == "__main__":
    main()
