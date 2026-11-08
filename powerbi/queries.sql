-- One query per Power BI table. Each is verified by scripts/check_powerbi_queries.py.
-- name: FactTransactions
SELECT event_id, event_time, step, type, amount, name_orig, name_dest, is_fraud, dest_is_merchant,
       fraud_probability, risk_score, risk_level, is_fraud_pred
FROM v_transactions_scored;
-- name: DailyKPIs
SELECT day, txn_count, txn_volume, fraud_count, fraud_loss, fraud_rate_pct FROM v_daily_kpis;
-- name: HourlyKPIs
SELECT hour, txn_count, txn_volume, fraud_count FROM v_hourly_kpis;
-- name: TypeKPIs
SELECT type, txn_count, txn_volume, fraud_count, fraud_loss, fraud_rate_pct FROM v_type_kpis;
-- name: Customers
SELECT customer, txn_count, total_amount, avg_amount, max_amount, fraud_count, first_seen, last_seen FROM v_customer_patterns;
-- name: Merchants
SELECT merchant, txn_count, total_received, avg_ticket, unique_customers FROM v_merchant_analysis;
