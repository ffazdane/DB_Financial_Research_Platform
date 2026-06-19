"""
update_universe.py
Reads Implementation/consolidated_tickers.md, extracts the full ticker list,
filters to Yahoo Finance-compatible tickers (ETFs + equities, no futures/indices),
updates config/tickers.yaml and scripts/local_price_loader.py,
then runs the full price download to Databricks.

Run: python scripts/update_universe.py
"""
import re
import sys
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# ── Tickers to explicitly exclude ─────────────────────────────────────────────
# Indices (not tradeable stocks), DXY, NDXP option notation
EXCLUDE = {
    "NDX", "SPX", "RUT", "XSP", "NDXP", "RTY", "YM",  # index roots
    "^DJI", "^GSPC", "^IXIC", "^NDX", "^NYA", "^RUT", "^VIX",  # ^ indices
    "DX-Y",   # DXY dollar index (not on Yahoo as simple ticker)
    "BTC=F",  # covered by =F filter but explicit
}


def parse_tickers(md_path: Path) -> list[str]:
    """Extract and filter tickers from the consolidated_tickers.md code block."""
    text = md_path.read_text(encoding="utf-8")

    # Find the code block with the full alphabetical list
    match = re.search(r"```text\s*([\s\S]+?)```", text)
    if not match:
        raise ValueError("Could not find ```text block in markdown file")

    raw = match.group(1).strip()
    # All tickers are comma-separated on one or more lines
    raw_tickers = [t.strip() for t in raw.replace("\n", ",").split(",") if t.strip()]

    filtered = []
    for t in raw_tickers:
        # Exclude futures: starts with / or ends with =F
        if t.startswith("/") or t.endswith("=F"):
            continue
        # Exclude ^ indices
        if t.startswith("^"):
            continue
        # Explicit exclusions
        if t in EXCLUDE:
            continue
        # Exclude anything with obviously invalid Yahoo chars (but keep BF-B, BRK-B)
        if " " in t:
            continue
        filtered.append(t)

    # Deduplicate preserving order, then sort
    seen = set()
    unique = []
    for t in filtered:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    unique.sort()
    return unique


def update_tickers_yaml(tickers: list[str]) -> None:
    """Overwrite config/tickers.yaml with the new ticker universe."""
    yaml_path = REPO_ROOT / "config" / "tickers.yaml"

    # Keep the header comments and liquidity_filters section
    existing = yaml_path.read_text(encoding="utf-8")
    # Extract liquidity_filters block to preserve it
    lf_match = re.search(r"(# Liquidity filter.*)", existing, re.DOTALL)
    lf_block = lf_match.group(1) if lf_match else ""

    lines = [
        "# FazDane Finance Platform — Full Ticker Universe",
        f"# Total tickers: {len(tickers)}",
        "# Updated from Implementation/consolidated_tickers.md",
        "# Excludes: futures (/ES, =F), ^ indices, DX-Y",
        "",
        "# Flat list used by universe_loader.py",
        "all_tickers:",
    ]
    for t in tickers:
        lines.append(f"  - {t}")

    if lf_block:
        lines.append("")
        lines.append(lf_block.rstrip())

    yaml_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Updated {yaml_path.relative_to(REPO_ROOT)} — {len(tickers)} tickers")


def update_local_loader(tickers: list[str]) -> None:
    """Update the TICKERS list in scripts/local_price_loader.py."""
    loader_path = REPO_ROOT / "scripts" / "local_price_loader.py"
    text = loader_path.read_text(encoding="utf-8")

    # Build new TICKERS block
    ticker_lines = ["TICKERS = ["]
    for i, t in enumerate(tickers):
        comma = "," if i < len(tickers) - 1 else ""
        ticker_lines.append(f'    "{t}"{comma}')
    ticker_lines.append("]")
    new_block = "\n".join(ticker_lines)

    # Replace existing TICKERS = [...] block
    new_text = re.sub(
        r"TICKERS\s*=\s*\[[\s\S]*?\]",
        new_block,
        text,
        count=1,
    )
    if new_text == text:
        print("WARNING: Could not find TICKERS = [...] block to replace in local_price_loader.py")
    else:
        loader_path.write_text(new_text, encoding="utf-8")
        print(f"Updated {loader_path.relative_to(REPO_ROOT)} — {len(tickers)} tickers")


def main():
    md_path = REPO_ROOT / "Implementation" / "consolidated_tickers.md"
    if not md_path.exists():
        print(f"ERROR: {md_path} not found")
        sys.exit(1)

    print("=" * 60)
    print("FazDane — Update Ticker Universe")
    print(f"Source: {md_path.relative_to(REPO_ROOT)}")
    print("=" * 60)

    tickers = parse_tickers(md_path)
    print(f"\nExtracted {len(tickers)} Yahoo-compatible tickers")
    print(f"Sample (first 20): {tickers[:20]}")
    print(f"Sample (last 10):  {tickers[-10:]}")

    print("\nUpdating config files...")
    update_tickers_yaml(tickers)
    update_local_loader(tickers)

    print("\nDone. Now running local_price_loader.py to download all data...")
    print("(This will take ~20-40 minutes for the full universe)\n")

    # Run the loader
    loader_script = REPO_ROOT / "scripts" / "local_price_loader.py"
    os.execv(sys.executable, [sys.executable, str(loader_script)])


if __name__ == "__main__":
    main()
