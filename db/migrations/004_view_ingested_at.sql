-- Expose ingestion time on the scored view (used by Streamlit / monitoring). Column appended at the end so
-- CREATE OR REPLACE VIEW stays compatible with existing consumers.
CREATE OR REPLACE VIEW v_transactions_scored AS
SELECT t.event_id, t.step, t.event_time, t.type, t.amount, t.name_orig, t.name_dest,
       t.old_balance_org, t.new_balance_org, t.old_balance_dest, t.new_balance_dest,
       t.is_fraud, t.is_flagged_fraud,
       (t.name_dest LIKE 'M%') AS dest_is_merchant,
       p.fraud_probability, p.anomaly_score, p.risk_score, p.risk_level, p.is_fraud_pred, p.model_version, p.scored_at,
       t.ingested_at
FROM transactions t LEFT JOIN predictions p USING (event_id);
