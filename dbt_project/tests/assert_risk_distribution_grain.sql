select event_date, risk_level
from {{ ref('fct_risk_distribution') }}
group by event_date, risk_level
having count(*) > 1
