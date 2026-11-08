select
    name_dest as merchant_id,
    count(*) as txn_count,
    sum(amount) as total_received,
    avg(amount) as avg_ticket,
    count(distinct name_orig) as unique_customers,
    max(event_time) as last_seen
from {{ ref('fct_transactions') }}
where dest_is_merchant
group by name_dest
