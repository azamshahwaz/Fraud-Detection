# Real-Time Fraud Detection & Intelligent Data Engineering Platform

End-to-end fraud detection platform on PaySim-style mobile-money transactions: streaming ingestion, distributed
processing, ML scoring, warehouse transformations, REST API, React console, Streamlit monitor and Power BI analytics.

> **Read this first — what was and was not verified.** Everything below marked ✅ was executed in the build
> environment (Linux, Python 3.12, Java 21, PostgreSQL 16, Node 22) and the evidence is in
> [`TEST_REPORT.md`](TEST_REPORT.md) and `docs/evidence/`. Items marked ⚠️ could **not** be executed there
> (no Docker daemon, no Kafka broker, no Power BI Desktop, no Kaggle access) and are documented honestly.
> Model metrics in this README come from the **synthetic PaySim-schema dataset** produced by
> `src/data/generate_paysim.py`, not from the real Kaggle file — see [Dataset](#dataset).

## Contents
1. [Objectives](#objectives) · 2. [Architecture](#architecture) · 3. [Tech stack](#tech-stack) · 4. [Folder structure](#folder-structure)
5. [Prerequisites](#prerequisites) · 6. [Quick start (Linux)](#quick-start-linux) · 7. [Quick start (Windows PowerShell)](#quick-start-windows-powershell)
8. [Dataset](#dataset) · 9. [Configuration](#configuration) · 10. [Database](#database-setup) · 11. [Docker](#docker) · 12. [Run each component](#run-each-component)
13. [API reference](#api-reference) · 14. [Power BI](#power-bi) · 15. [Testing](#testing) · 16. [Troubleshooting](#troubleshooting) · 17. [Limitations & roadmap](#known-limitations)

## Objectives
* Ingest transactions in batch (PySpark ETL) and in real time (Kafka → consumer or Spark Structured Streaming).
* Score every transaction with a fraud probability, an anomaly score and a 0–100 risk score; raise alerts.
* Store everything in PostgreSQL, transform with dbt (with data-quality tests), orchestrate with Airflow.
* Track experiments and register models in MLflow; monitor data drift (PSI) and pipeline health.
* Expose a FastAPI backend and three front ends (React console, Streamlit monitor, Power BI) that all show **database data, never hard-coded values**.

## Architecture
Full diagram and reliability design: [`docs/architecture.md`](docs/architecture.md).

```
PaySim CSV ──► PySpark batch ETL ───────────────────────────────┐
     │                                                          ▼
     └─► Kafka producer ─► Kafka ─► consumer ─┐         ┌──────────────┐    ┌─► FastAPI ─► React console
                                  └► Spark SS ┴► validate ► dedupe ► score ─► │  PostgreSQL  │ ───┼─► Streamlit monitor
                                         └─► DLQ topic                       └──────────────┘    └─► Power BI
Airflow DAG: check input → Spark ETL → batch score → dbt run → dbt test → drift check
Training → MLflow (tracking + registry) → model bundle used by API / batch / streaming
```

## Tech stack
| Area | Tool | Pinned version |
|---|---|---|
| Language | Python | 3.12 |
| Processing | PySpark | 3.5.3 (needs Java 17 or 21) |
| Streaming | Apache Kafka (KRaft) / kafka-python | image `apache/kafka:3.8.0` / 2.3.3 |
| Orchestration | Apache Airflow | 2.10.3 |
| Database | PostgreSQL | 16 |
| Transformation | dbt-core / dbt-postgres | 1.8.8 / 1.8.2 |
| ML | scikit-learn, XGBoost, MLflow | 1.5.2, 2.1.3, 2.17.2 |
| API | FastAPI + Uvicorn + SQLAlchemy | 0.115.5 / 0.32.1 / 2.0.36 |
| Frontend | React + Vite + Recharts | 18.3.1 / 5.4.11 / 2.13.3 |
| Monitoring UI | Streamlit | 1.40.1 |
| BI | Power BI Desktop | any recent |
| Deploy | Docker + Compose v2 | Docker 24+ |

## Folder structure
```
backend/api/            FastAPI app (main.py, routers/, schemas.py, deps.py)
backend/src/common/     config (env vars), db helpers (retry, idempotent inserts, pipeline_run), schema/event ids
backend/src/data/       PaySim-schema generator
backend/src/etl/        PySpark batch ETL + input check
backend/src/ml/         features (shared), training (XGBoost + LogReg baseline + IsolationForest, MLflow)
backend/src/scoring/    Scorer (single scoring implementation) and batch scoring job
backend/src/streaming/  producer, consumer, shared processor, Spark Structured Streaming
backend/src/monitoring/ PSI drift
db/migrations/          001..004 SQL migrations;  scripts/migrate.py applies them
dbt_project/            staging + marts models, generic + singular tests, profiles via env vars
orchestration/dags/     Airflow DAGs (batch pipeline, weekly retrain)
streamlit_app/          real-time monitor
frontend/               React console (+ vitest integration tests)
powerbi/                .pbids, import queries, DAX measures, theme
docker/ + docker-compose.yml
backend/tests/          pytest: unit, integration, API, Streamlit, Spark, e2e
docs/                   architecture + test evidence logs
```

## Prerequisites
| Tool | Version | Notes |
|---|---|---|
| Python | 3.12 | 3.11 should work; 3.12 is what was tested |
| Java (JDK) | 17 or 21 | required by PySpark; set `JAVA_HOME` |
| PostgreSQL | 16 (14+ ok) | or use Docker |
| Node.js | 20+ (22 tested) | frontend |
| Docker + Compose v2 | 24+ | optional, ⚠️ not executed in the build environment |
| Power BI Desktop | current (Windows) | optional |

Hardware: 4 GB RAM minimum for Spark local mode with 100k rows; 8 GB recommended for the full Docker stack.

## Quick start (Linux)
```bash
git clone <your-repo-url> fraud-platform && cd fraud-platform
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env            # then edit POSTGRES_PASSWORD (and API_KEY if you want auth)
# create role + database (needs a local PostgreSQL superuser), then apply migrations:
bash scripts/init_db.sh         # or follow "Database setup" below

python -m backend.src.data.generate_paysim --rows 100000      # or place the real PaySim CSV at data/raw/paysim.csv
python -m backend.src.etl.batch_etl                           # PySpark ETL  -> PostgreSQL
python -m backend.src.ml.train                                # train + MLflow + model bundle
python -m backend.src.scoring.batch_score                     # predictions + alerts

python -m uvicorn backend.api.main:app --port 8000 &  # API  http://localhost:8000/docs
(cd frontend && npm install && npm run dev)           # React http://localhost:5173
streamlit run streamlit_app/app.py                    # Streamlit http://localhost:8501
```

## Quick start (Windows PowerShell)
> ⚠️ Not executed on Windows in the build environment. PySpark on native Windows can additionally require
> `winutils.exe`/`HADOOP_HOME`; the most reliable options are **WSL2 (use the Linux steps)** or **Docker**.
```powershell
git clone <your-repo-url> fraud-platform; cd fraud-platform
py -3.12 -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env          # edit POSTGRES_PASSWORD
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17"   # adjust to your JDK

# Database (psql must be on PATH; replace values with those from .env)
psql -U postgres -c "CREATE ROLE fraud_user LOGIN PASSWORD 'your-password'"
psql -U postgres -c "CREATE DATABASE fraud_db OWNER fraud_user"
python scripts\migrate.py

python -m backend.src.data.generate_paysim --rows 100000
python -m backend.src.etl.batch_etl
python -m backend.src.ml.train
python -m backend.src.scoring.batch_score

Start-Process python -ArgumentList "-m","uvicorn","backend.api.main:app","--port","8000"
cd frontend; npm install; npm run dev
# new terminal:  streamlit run streamlit_app\app.py
```

## Dataset
The project uses the **PaySim** mobile-money simulation (Lopez-Rojas et al.), columns:
`step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud`.

**Download the real data (recommended for real results):**
```bash
pip install kaggle                      # needs a Kaggle API token in ~/.kaggle/kaggle.json (PowerShell: %USERPROFILE%\.kaggle)
kaggle datasets download -d ealaxi/paysim1 -p data/raw --unzip
mv data/raw/PS_20174392719_1491204439457_log.csv data/raw/paysim.csv     # PowerShell: Move-Item
```
The full file has ~6.36 M rows (~470 MB). On a laptop start with a slice:
`python -m backend.src.ml.train --max-rows 1000000` and `head -n 1000001 data/raw/paysim.csv > data/raw/paysim_1m.csv`.

**Built-in generator (used for all verification here):** `python -m backend.src.data.generate_paysim --rows 100000 --fraud-rate 0.01`
produces the same schema and the same qualitative fraud pattern (fraud only in `TRANSFER`/`CASH_OUT`, large amounts,
often draining the account) with deliberately overlapping hard negatives. **It is not real data**: reported metrics describe
this synthetic set only. Real PaySim has ≈0.13 % fraud, so expect a lower PR-AUC and a different threshold.
`step` = one simulated hour; `event_time` is derived as `2026-01-01 + step hours`.

## Configuration
All settings are environment variables (see `.env.example`); nothing secret is hard-coded and `.env` is git-ignored.

| Variable | Purpose | Default |
|---|---|---|
| `POSTGRES_HOST/PORT/DB/USER/PASSWORD` | database (password is **required**, no default) | localhost/5432/fraud_db/fraud_user/– |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC`, `KAFKA_GROUP_ID` | streaming | localhost:9092 / transactions / transactions.dlq / fraud-scorer |
| `SPARK_CHECKPOINT_DIR`, `SPARK_SHUFFLE_PARTITIONS` | Spark | ./data/checkpoints, 4 |
| `MLFLOW_TRACKING_URI`, `MLFLOW_EXPERIMENT`, `MODEL_DIR`, `MODEL_NAME` | ML | sqlite:///./mlflow.db, fraud-detection, ./models, fraud-xgb |
| `PAYSIM_CSV`, `DATA_DIR` | data | ./data/raw/paysim.csv, ./data |
| `API_KEY` | if set, every endpoint except `/health` requires header `X-API-Key` | empty (open) |
| `CORS_ORIGINS` | allowed browser origins | http://localhost:5173,http://localhost:3000 |
| `ALERT_RISK_THRESHOLD` | risk score that opens an alert (also opens on model-predicted fraud) | 80 |

## Database setup
Schema is managed by numbered SQL migrations in `db/migrations/` (tracked in `schema_migrations`); `scripts/migrate.py` is idempotent.
```bash
# Option A - local PostgreSQL
bash scripts/init_db.sh                  # creates role + database from .env, then migrates
# Option B - already have role/db
python scripts/migrate.py
# Option C - Docker
docker compose up -d postgres && docker compose run --rm migrate
```
Tables: `transactions`, `predictions`, `fraud_alerts`, `pipeline_runs`, `model_metrics`, `drift_reports`.
Views for BI: `v_transactions_scored`, `v_daily_kpis`, `v_hourly_kpis`, `v_type_kpis`, `v_customer_patterns`, `v_merchant_analysis`.

## Docker
> ⚠️ `docker-compose.yml` is syntax- and reference-validated but **was not run** (no Docker daemon in the build environment).
```bash
cp .env.example .env                                   # set POSTGRES_PASSWORD
docker compose up -d --build postgres kafka kafka-init migrate
docker compose --profile bootstrap run --rm bootstrap   # generate data (if none), ETL, train, score
docker compose up -d api frontend streamlit mlflow      # React :3000, API :8000/docs, Streamlit :8501, MLflow :5000
docker compose --profile streaming up -d stream-consumer producer    # real-time path via Kafka consumer
#   or: docker compose --profile spark up -d spark-stream producer    # real-time path via Spark Structured Streaming
docker compose --profile airflow up -d airflow          # Airflow UI :8080 (see logs for the generated admin password)
docker compose down            # add -v to also delete volumes
```
To use the real PaySim file in Docker: `docker compose run --rm -v "$PWD/PS_...csv:/app/data/raw/paysim.csv" bootstrap`.

## Run each component

### Kafka (real-time path) — ⚠️ broker not available in the build environment
```bash
docker compose up -d kafka kafka-init                       # single-node KRaft broker + topics
python -m backend.src.streaming.producer --rate 50 --limit 5000 --dup-ratio 0.02    # replay events (2% duplicates on purpose)
python -m backend.src.streaming.consumer                            # validate -> dedupe -> score -> PostgreSQL -> alerts; Ctrl-C to stop
```
Bad JSON / invalid records go to `KAFKA_DLQ_TOPIC`. Offsets are committed only after the batch is stored.

### Spark
```bash
python -m backend.src.etl.batch_etl --input data/raw/paysim.csv     # batch ETL; rejects written to data/processed/rejects
python -m backend.src.streaming.spark_stream --source kafka         # ⚠️ Kafka source (downloads spark-sql-kafka package from Maven on first run)
python -m backend.src.streaming.spark_stream --source files --input-dir data/stream_in          # ✅ file source, same logic, no broker
python -m backend.src.streaming.spark_stream --source files --once  # process what is there, then exit (checkpoint recovery demo)
```
Files for the file source are JSON-lines with the producer's fields (`event_id, step, type, amount, name_orig, …`).

### Airflow (local, standalone)
```bash
python -m venv .venv-af && .venv-af/bin/pip install "apache-airflow==2.10.3" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt
python -m venv .venv-dbt && .venv-dbt/bin/pip install -r requirements-dbt.txt
export AIRFLOW_HOME=$PWD/.airflow AIRFLOW__CORE__LOAD_EXAMPLES=False \
       AIRFLOW__CORE__DAGS_FOLDER=$PWD/orchestration/dags PROJECT_DIR=$PWD \
       PYTHON_BIN=$PWD/.venv/bin/python DBT_BIN=$PWD/.venv-dbt/bin/dbt
set -a; source .env; set +a
.venv-af/bin/airflow db migrate
.venv-af/bin/airflow dags test fraud_batch_pipeline 2026-01-01      # run the whole DAG once, in-process
.venv-af/bin/airflow standalone                                      # UI on :8080
```
DAGs: `fraud_batch_pipeline` (daily; 2 retries with exponential back-off per task) and `fraud_model_retrain` (weekly).
Tasks run the project's own CLI modules in the project venv, so Airflow's dependency pins never clash with Spark/ML packages.

### dbt
```bash
python -m venv .venv-dbt && .venv-dbt/bin/pip install -r requirements-dbt.txt
cd dbt_project && set -a && source ../.env && set +a
../.venv-dbt/bin/dbt run  --profiles-dir .      # 2 staging views + 4 marts in schemas staging / analytics
../.venv-dbt/bin/dbt test --profiles-dir .      # 19 tests: unique, not_null, accepted_values, relationships, custom + singular
```
> Install dbt in its own venv. Plain `pip install dbt-postgres` can pull a pre-release `dbt-core` 2.x that does not support Postgres; `requirements-dbt.txt` pins `dbt-core==1.8.8`.

### ML training and inference
```bash
python -m backend.src.ml.train                         # --max-rows N to subsample, --no-db to skip writing model_metrics
mlflow ui --backend-store-uri sqlite:///./mlflow.db        # needs full `pip install mlflow==2.17.2`; compare runs & registry
python -m backend.src.scoring.batch_score              # score all unscored rows
python - <<'PY'                                # single inference from Python
from backend.src.scoring.scorer import get_scorer
import pandas as pd
row = dict(step=300, type="TRANSFER", amount=250000, name_orig="C1", old_balance_org=250000, new_balance_org=0,
           name_dest="C2", old_balance_dest=0, new_balance_dest=0)
print(get_scorer().score(pd.DataFrame([row])).T)
PY
```
Training is reproducible (fixed seeds, deterministic time-based split, `n_jobs=1`). It logs params, validation and test metrics,
confusion matrix and feature importance to MLflow, registers the XGBoost model (`fraud-xgb`), and writes `models/fraud_bundle.joblib`
(model + isolation forest + threshold + drift reference) which every scorer loads.

Metrics on the 100k-row **synthetic** set (time-based test split, 15 000 rows, 145 fraud): see `models/model_meta.json` after training.
Typical result: XGBoost precision ≈ 0.74, recall ≈ 0.57, F1 ≈ 0.64, PR-AUC ≈ 0.75 vs logistic-regression baseline PR-AUC ≈ 0.65.

### Backend and frontend
```bash
python -m uvicorn backend.api.main:app --reload --port 8000  # Swagger UI at /docs, ReDoc at /redoc
cd frontend && npm install && npm run dev        # dev server proxies /api/* to :8000 (set VITE_PROXY_TARGET to change)
cd frontend && npm run build                     # static bundle in frontend/dist (served by nginx in Docker)
```
Pages: Overview (KPIs, daily trend, risk mix, by type) · Transactions (search, filters, sorting, pagination, detail drawer) ·
Alerts (triage: investigate / confirm / dismiss) · Analytics (fraud over time, rate by type, top-risk accounts) ·
Model (precision/recall/F1/PR-AUC, confusion matrix, history, drift check) · Pipelines (runs, failures).
If `API_KEY` is set, put the API behind the same origin / a gateway that injects the header; the browser app does not embed secrets.

### Streamlit
```bash
streamlit run streamlit_app/app.py        # sidebar: window size, minimum risk, auto-refresh (default 10 s)
```

## API reference
Base URL `http://localhost:8000`. Interactive docs: `/docs`. Errors are JSON `{"detail": ...}` (422 validation, 404, 401, 503).

| Method & path | Purpose |
|---|---|
| `GET /health` | database + model status |
| `POST /predict` | score one transaction, nothing stored |
| `POST /predict/batch` | score 1–1000 transactions |
| `POST /transactions` | ingest + score + store; opens an alert when risky; idempotent on `event_id` |
| `GET /transactions` | filters `q,type,risk_level,is_fraud,min_amount,max_amount`, `sort`, `order`, `limit`, `offset` |
| `GET /transactions/{event_id}` | one transaction with prediction |
| `GET /alerts`, `PATCH /alerts/{id}` | list (by `status`) / change status |
| `GET /analytics/overview`, `/timeseries?grain=day\|hour`, `/by-type`, `/top-risk-accounts` | analytics |
| `GET /model/metrics` | active model, test metrics, confusion matrix, history |
| `GET /pipeline/runs` | pipeline summary + recent runs |
| `POST /monitoring/drift/run?window=5000`, `GET /monitoring/drift` | PSI drift |

**Sample request**
```bash
curl -s -X POST http://localhost:8000/transactions -H 'Content-Type: application/json' -d '{
  "step": 300, "type": "TRANSFER", "amount": 250000,
  "name_orig": "C111", "old_balance_org": 250000, "new_balance_org": 0,
  "name_dest": "C222", "old_balance_dest": 0, "new_balance_dest": 0 }'
```
**Sample response** (`201`, real output from the build run)
```json
{"fraud_probability":0.9970383644104004,"anomaly_score":1.0,"risk_score":99.75,"risk_level":"CRITICAL",
 "is_fraud_pred":true,"model_version":"fraud-xgb-20261107042755",
 "event_id":"b8238216aea29067f1f4e4083dbf8c27","duplicate":false,"alert_created":true}
```
Sending the same body again returns `"duplicate": true, "alert_created": false`. Validation failure (`amount: -5`):
```json
{"detail":[{"loc":["body","amount"],"msg":"Input should be greater than or equal to 0"}]}
```
With `API_KEY` set: `curl -H "X-API-Key: $API_KEY" http://localhost:8000/alerts`.

## Power BI
> ⚠️ **Power BI Desktop is Windows-only and was not available here, so no `.pbix` file is included.** What *is* provided and verified:
> all six import queries run successfully against the database (`python scripts/check_powerbi_queries.py` → 6 PASS), the data-source file, DAX measures and theme.

1. Open `powerbi/fraud_dashboard.pbids` in Power BI Desktop (edit server/database if needed), sign in with the PostgreSQL user from `.env`.
   (Npgsql/PostgreSQL connector prompts for a one-time driver install on some machines.)
2. *Get data → PostgreSQL → Advanced options → SQL statement*: paste each query from `powerbi/queries.sql` as its own table
   (`FactTransactions`, `DailyKPIs`, `HourlyKPIs`, `TypeKPIs`, `Customers`, `Merchants`). Use *Import* mode; schedule refresh through a gateway for shared use.
3. Relationships: none required (each table is pre-aggregated); optionally relate `Customers[customer]` ↔ `FactTransactions[name_orig]`.
4. *View → Themes → Browse for themes* → `powerbi/theme.json`. Paste measures from `powerbi/measures.dax` into `FactTransactions`.
5. Suggested pages: **Executive** (cards: Total Transactions, Total Volume, Fraud Loss, Fraud Rate %; line: DailyKPIs fraud_loss by day) ·
   **Fraud by type & time** (bar: TypeKPIs fraud_rate_pct; line: HourlyKPIs fraud_count) · **Customers** (table: Customers sorted by fraud_count; scatter total_amount vs txn_count) ·
   **Merchants** (bar: Merchants total_received; table top merchants by unique_customers) · **Model** (Model Precision, Model Recall, risk_level distribution).

## Testing
```bash
pytest -q                                   # Python: unit, integration, API, Streamlit AppTest, Spark, e2e (uses database fraud_test)
pytest -q --cov=src --cov=api               # with coverage
cd frontend && npm test                     # React integration tests against the running API (TEST_API_URL, default :8000)
cd dbt_project && dbt test --profiles-dir . # data-quality tests
airflow dags test fraud_batch_pipeline 2026-01-01      # (airflow venv) run the DAG
python scripts/check_powerbi_queries.py     # Power BI import queries
```
Python tests need a second database: `createdb -O fraud_user fraud_test` (or set `TEST_POSTGRES_DB`); they train a small model in a temp dir,
run migrations and truncate tables between tests — they never touch `fraud_db`. Frontend tests expect the API on `:8000` backed by data (run Quick start first).
Results and the list of unverified components: [`TEST_REPORT.md`](TEST_REPORT.md).

## Troubleshooting
| Symptom | Fix |
|---|---|
| `Missing required environment variable: POSTGRES_PASSWORD` | create `.env` from `.env.example` and set it |
| `Model bundle not found` / API `/health` says `degraded`, `/predict` returns 503 | run `python -m backend.src.ml.train`; the API loads the model lazily, no restart needed |
| `JAVA_HOME is not set` / Spark won't start | install JDK 17 or 21 and export `JAVA_HOME` |
| `ModuleNotFoundError: kafka.vendor.six.moves` | old kafka-python on Python 3.12; use the pinned `kafka-python==2.3.3` |
| `No module named 'distutils'` in Spark foreachBatch | already avoided (`collect()` instead of `toPandas()`); don't reintroduce `toPandas` on Python 3.12 without `setuptools` |
| `ModuleNotFoundError: alembic` from MLflow | `pip install alembic` (in `requirements.txt`) |
| dbt: "'postgres' adapter is not yet supported" | you got dbt-core 2.x; use a fresh venv with `requirements-dbt.txt` |
| Airflow task `No module named dotenv` | tasks must run via `PYTHON_BIN` (project venv); set `PYTHON_BIN`/`DBT_BIN` |
| `connection refused` to PostgreSQL | start the server; the API returns 503 (never a stack trace) while the DB is down |
| Frontend shows "API unreachable" | start the API; in dev the Vite proxy forwards `/api` to `VITE_PROXY_TARGET` |
| Spark `OutOfMemory` on full PaySim | raise `spark.driver.memory` in `src/etl/batch_etl.py:get_spark`, or use a slice of the file |

## Known limitations
* Verified on synthetic PaySim-schema data; real-PaySim metrics will differ (far lower fraud rate). Retrain and re-pick the threshold on real data.
* Kafka broker, Docker Compose stack, the Spark **Kafka source**, Windows installation and Power BI Desktop were not executed here (see TEST_REPORT).
  Their logic is tested through shared code paths (the processor, the Spark file source, query checks) but the transport itself is unverified.
* The React UI was verified with jsdom integration tests against the live API and a production build, not in a real browser; no visual/screenshot review was possible.
* Alerts are not pushed (email/Slack/webhook); they are stored and shown. No authentication beyond an optional shared API key; no per-user roles.
* PaySim labels are simulated; `isFlaggedFraud` is stored but not used as a feature.
* Spark runs in `local[*]`; production would target a cluster and the JDBC sink instead of parquet → COPY-style load.

## Future improvements
Feature store with rolling per-account aggregates (velocity, new-beneficiary), SHAP explanations per alert, champion/challenger
deployment through the MLflow registry, Kafka schema registry (Avro/Protobuf), alert notifications, role-based access (OIDC),
Prometheus/Grafana metrics, CI pipeline (GitHub Actions) running pytest + dbt + frontend tests, cluster-mode Spark.
