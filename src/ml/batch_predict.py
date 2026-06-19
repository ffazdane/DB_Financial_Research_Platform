"""batch_predict.py
Phase 7 — Daily Batch Inference Pipeline

Loads the registered MLflow model and runs daily inference on all
tickers in the liquid universe. Writes scores to ml.model_predictions.

Outputs per ticker:
- prob_positive_5d / 10d / 20d
- prob_in_range (stays within expected move)
- xgb_rank_score
- hmm_regime
- survival_days_estimate
"""
# TODO: implement in Phase 7
