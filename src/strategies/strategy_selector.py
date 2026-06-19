"""strategy_selector.py
Phase 6 — Rules-Based Strategy Selection Engine

Selects one of: calendar | diagonal | iron_condor | no_trade

Decision logic (in order):
1. Apply no_trade filters first (earnings, liquidity, spread, regime)
2. Check iron_condor conditions (sideways regime + high IV)
3. Check diagonal conditions (strong directional FDTS + high strength)
4. Default to calendar when directional bias is present
5. Fall back to no_trade if no conditions are met

Reads rules from config/strategy_rules.yaml.
"""
# TODO: implement in Phase 6
