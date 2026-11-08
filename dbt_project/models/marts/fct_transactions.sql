select t.*, p.fraud_probability, p.anomaly_score, p.risk_score, p.risk_level, p.is_fraud_pred, p.model_version,
       a.status as alert_status
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_predictions') }} p using (event_id)
left join {{ source('raw', 'fraud_alerts') }} a using (event_id)
