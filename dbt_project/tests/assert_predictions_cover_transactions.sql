-- Every transaction older than the current scoring window should have a prediction. Fails if any are unscored.
select t.event_id from {{ ref('stg_transactions') }} t
left join {{ ref('stg_predictions') }} p using (event_id)
where p.event_id is null
