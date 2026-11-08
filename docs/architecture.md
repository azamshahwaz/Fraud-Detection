# Architecture

```mermaid
flowchart LR
  subgraph Sources
    G[PaySim CSV / generator] --> P[Kafka producer]
  end
  P -->|JSON events, key=event_id| K[(Kafka topic: transactions)]
  K --> C[Kafka consumer<br/>micro-batch, manual commit]
  K --> S[Spark Structured Streaming<br/>checkpointed]
  K -. bad messages .-> D[(DLQ topic)]
  C --> PR
  S --> PR
  subgraph PR[Shared processor]
    V[validate] --> DD[de-duplicate] --> SC[ML scoring<br/>XGBoost + IsolationForest] --> RS[risk score 0-100]
  end
  PR --> PG[(PostgreSQL<br/>transactions / predictions / fraud_alerts)]
  G --> E[PySpark batch ETL] --> PG
  AF[Airflow DAG] --> E
  AF --> BS[batch scoring] --> PG
  AF --> DBT[dbt run + test] --> PG
  AF --> DR[drift check PSI] --> PG
  T[Training] --> ML[(MLflow tracking + registry)]
  T --> B[model bundle .joblib] --> SC
  PG --> API[FastAPI]
  API --> UI[React console]
  PG --> ST[Streamlit monitor]
  PG --> PBI[Power BI]
```

## Data flow (real-time path)
1. `producer` replays transactions into Kafka (key = deterministic `event_id`).
2. `consumer` **or** `spark_stream` reads micro-batches. Both call `src/streaming/processor.py`.
3. Records are validated (bad ones → DLQ), de-duplicated inside the batch, inserted with `ON CONFLICT DO NOTHING`.
4. The scorer computes fraud probability (XGBoost), anomaly score (IsolationForest) and
   `risk_score = 100 × (0.85·p + 0.15·anomaly)`; LOW <30, MEDIUM <60, HIGH <80, CRITICAL ≥80.
5. Predictions are upserted; an alert is opened when `p ≥ threshold` or `risk_score ≥ ALERT_RISK_THRESHOLD`.
6. API / React / Streamlit / Power BI read PostgreSQL. Nothing is hard-coded.

## Reliability design
| Concern | Mechanism |
|---|---|
| Duplicate events | deterministic `event_id` (sha256) + `ON CONFLICT DO NOTHING`; same id logic in Python and Spark |
| Retries | DB calls retried with exponential backoff; consumer retries a batch 5× and only then fails *without committing offsets* |
| Crash recovery | Kafka: offsets committed after DB write (at-least-once + idempotent sink). Spark: checkpoint directory |
| Bad data | validation in ETL (reject reasons) and streaming (DLQ) |
| Pipeline failure visibility | `pipeline_runs` table (status, rows in/out, error), shown in the React "Pipelines" page |
| Train/serve skew | one `build_features()` used by training, API, batch and streaming |
| Leakage | time-based train/val/test split on `step`; threshold chosen on validation only |
