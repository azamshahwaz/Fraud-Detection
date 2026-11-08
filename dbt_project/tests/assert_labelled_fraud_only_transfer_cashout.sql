-- PaySim property: ground-truth fraud only occurs in TRANSFER / CASH_OUT. Returns violating rows.
select event_id from {{ ref('stg_transactions') }}
where is_fraud = 1 and type not in ('TRANSFER', 'CASH_OUT')
