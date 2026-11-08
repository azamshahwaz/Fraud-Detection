select
    event_date,
    count(*) as txn_count,
    sum(amount) as txn_volume,
    count(*) filter (where is_fraud = 1) as fraud_count,
    coalesce(sum(amount) filter (where is_fraud = 1), 0) as fraud_loss,
    count(*) filter (where is_fraud_pred) as flagged_count,
    count(*) filter (where is_fraud_pred and is_fraud = 1) as true_positives,
    round(100.0 * count(*) filter (where is_fraud = 1) / nullif(count(*), 0), 4) as fraud_rate_pct
from {{ ref('fct_transactions') }}
group by event_date
