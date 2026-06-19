"""trade_constructor.py
Phase 6 — Trade Object Assembly

Assembles the full trade candidate object from strategy engine outputs:
- Strike selection from options chain
- Expiration date selection by DTE target
- Net debit/credit calculation
- Max risk / max profit estimate
- Breakeven prices (low and high)
- Net position Greeks (delta, gamma, theta, vega)
- Trade action: enter | watch | skip

Writes to gold.trade_candidates via MERGE on (ticker, trade_date).
"""
# TODO: implement in Phase 6
