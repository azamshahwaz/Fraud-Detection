select event_id, fraud_probability, anomaly_score, risk_score, risk_level, is_fraud_pred, model_version, scored_at
from {{ source('raw', 'predictions') }}
