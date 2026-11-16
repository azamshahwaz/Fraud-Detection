select
    event_date,
    coalesce(risk_level, 'UNSCORED') as risk_level,
    count(*) as txn_count,
    sum(amount) as txn_volume,
    count(*) filter (where is_fraud = 1) as fraud_count,
    count(*) filter (where is_fraud_pred) as predicted_fraud_count
from {{ ref('fct_transactions') }}
group by event_date, coalesce(risk_level, 'UNSCORED')
