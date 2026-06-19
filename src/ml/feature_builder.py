"""feature_builder.py
Phase 7 — ML Feature Table Construction

Joins gold layer outputs into ml.ml_feature_table:
- FDTS score, Ticker Strength score, trend/momentum/volume scores
- IV Rank, IV Percentile, option liquidity score
- Market regime score (encoded)

Also appends forward return labels (populated by lagged job after N trading days).
Writes to ml.ml_feature_table via MERGE on (ticker, trade_date).
"""
# TODO: implement in Phase 7
