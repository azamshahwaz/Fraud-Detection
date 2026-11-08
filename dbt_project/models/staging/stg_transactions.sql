select
    event_id, step, event_time, type, amount::numeric(18,2) as amount,
    name_orig, name_dest, old_balance_org, new_balance_org, old_balance_dest, new_balance_dest,
    is_fraud, is_flagged_fraud,
    (name_dest like 'M%')                         as dest_is_merchant,
    (new_balance_org = 0 and old_balance_org > 0) as org_emptied,
    event_time::date                              as event_date,
    extract(hour from event_time)::int            as event_hour
from {{ source('raw', 'transactions') }}
