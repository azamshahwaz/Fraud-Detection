select
    name_orig as customer_id,
    count(*) as txn_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    max(amount) as max_amount,
    count(*) filter (where is_fraud = 1) as confirmed_fraud_count,
    count(*) filter (where is_fraud_pred) as flagged_count,
    max(risk_score) as max_risk_score,
    min(event_time) as first_seen,
    max(event_time) as last_seen,
    case when count(*) >= 5 then 'frequent' when count(*) >= 2 then 'repeat' else 'one_time' end as segment
from {{ ref('fct_transactions') }}
group by name_orig
