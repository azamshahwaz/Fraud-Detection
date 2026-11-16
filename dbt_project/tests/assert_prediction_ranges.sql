select event_id
from {{ ref('stg_predictions') }}
where fraud_probability < 0 or fraud_probability > 1
   or risk_score < 0 or risk_score > 100
