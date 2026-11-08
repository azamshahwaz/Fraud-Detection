-- Analytics views consumed by API, Streamlit and Power BI.
CREATE OR REPLACE VIEW v_transactions_scored AS
SELECT t.event_id, t.step, t.event_time, t.type, t.amount, t.name_orig, t.name_dest,
       t.old_balance_org, t.new_balance_org, t.old_balance_dest, t.new_balance_dest,
       t.is_fraud, t.is_flagged_fraud,
       (t.name_dest LIKE 'M%') AS dest_is_merchant,
       p.fraud_probability, p.anomaly_score, p.risk_score, p.risk_level, p.is_fraud_pred, p.model_version, p.scored_at
FROM transactions t LEFT JOIN predictions p USING (event_id);

CREATE OR REPLACE VIEW v_daily_kpis AS
SELECT date_trunc('day', event_time)::date AS day,
       count(*)                                        AS txn_count,
       sum(amount)                                     AS txn_volume,
       count(*) FILTER (WHERE is_fraud = 1)            AS fraud_count,
       coalesce(sum(amount) FILTER (WHERE is_fraud = 1), 0) AS fraud_loss,
       round(100.0 * count(*) FILTER (WHERE is_fraud = 1) / nullif(count(*),0), 4) AS fraud_rate_pct
FROM transactions GROUP BY 1;

CREATE OR REPLACE VIEW v_hourly_kpis AS
SELECT date_trunc('hour', event_time) AS hour,
       count(*) AS txn_count, sum(amount) AS txn_volume,
       count(*) FILTER (WHERE is_fraud = 1) AS fraud_count
FROM transactions GROUP BY 1;

CREATE OR REPLACE VIEW v_type_kpis AS
SELECT type, count(*) AS txn_count, sum(amount) AS txn_volume,
       count(*) FILTER (WHERE is_fraud = 1) AS fraud_count,
       coalesce(sum(amount) FILTER (WHERE is_fraud = 1),0) AS fraud_loss,
       round(100.0 * count(*) FILTER (WHERE is_fraud = 1) / nullif(count(*),0), 4) AS fraud_rate_pct
FROM transactions GROUP BY type;

CREATE OR REPLACE VIEW v_customer_patterns AS
SELECT name_orig AS customer, count(*) AS txn_count, sum(amount) AS total_amount,
       avg(amount) AS avg_amount, max(amount) AS max_amount,
       count(*) FILTER (WHERE is_fraud = 1) AS fraud_count,
       min(event_time) AS first_seen, max(event_time) AS last_seen
FROM transactions GROUP BY name_orig;

CREATE OR REPLACE VIEW v_merchant_analysis AS
SELECT name_dest AS merchant, count(*) AS txn_count, sum(amount) AS total_received,
       avg(amount) AS avg_ticket, count(DISTINCT name_orig) AS unique_customers
FROM transactions WHERE name_dest LIKE 'M%' GROUP BY name_dest;
