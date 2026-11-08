# TEST_REPORT

Generated from commands actually executed in the build environment (Linux x86-64, Python 3.12.3, OpenJDK 21,
PostgreSQL 16.x, Node 22, 1 vCPU / 3 GB RAM). Raw logs are in `docs/evidence/`. **No result below is assumed;
anything not executed is listed under "Not verified".**

Dataset used for every run: 100 000-row **synthetic PaySim-schema** set (`python -m src.data.generate_paysim`, seed 42, 1 % fraud).
Real PaySim (Kaggle) was unreachable from the sandbox, so reported model quality does **not** describe real PaySim.

## 1. Summary
| Area | Command | Result |
|---|---|---|
| Python unit / integration / API / Streamlit / Spark / e2e | `pytest -v --cov=src --cov=api` | **56 passed, 1 skipped, 2 warnings in 120.23s (0:02:00)** · coverage 84 % |
| Airflow DAG integrity (Airflow 2.10.3 venv) | `pytest tests/test_dags.py` | 1 passed (skipped in the main venv by design) |
| Airflow DAG execution | `airflow dags test fraud_batch_pipeline <date>` | **PASS** — all 6 tasks SUCCESS (`docs/evidence/airflow_dag_run.txt`) |
| dbt models | `dbt run` | **PASS** — PASS=6 WARN=0 ERROR=0 SKIP=0 TOTAL=6 |
| dbt data-quality tests | `dbt test` | **PASS** — PASS=19 WARN=0 ERROR=0 SKIP=0 TOTAL=19 |
| Frontend integration (vitest + jsdom, live API) | `npm test` | **PASS** — 8/8 |
| Frontend production build | `npm run build` | **PASS** |
| Power BI import queries | `python scripts/check_powerbi_queries.py` | **PASS** — 6/6 |
| Spark file-source streaming + checkpoint recovery | e2e test + manual run | **PASS** |
| compose file | YAML parse + `depends_on` reference check | syntax valid only (**not run**) |

### Python test files
| File | Passed | Failed | Skipped |
|---|---|---|---|
| `tests/test_api.py` | 13 | 0 | 0 |
| `tests/test_core_units.py` | 11 | 0 | 0 |
| `tests/test_e2e_stream.py` | 1 | 0 | 0 |
| `tests/test_pipeline.py` | 12 | 0 | 0 |
| `tests/test_streamlit.py` | 2 | 0 | 0 |

### Coverage (`pytest --cov`)
```
api/__init__.py                     0      0   100%
api/deps.py                        13      0   100%
api/main.py                        52      8    85%
api/routers/__init__.py             0      0   100%
api/routers/analytics.py           55      0   100%
api/routers/core.py                83      2    98%
api/schemas.py                     38      1    97%
src/__init__.py                     0      0   100%
src/common/__init__.py              0      0   100%
src/common/config.py               49      1    98%
src/common/db.py                   84      5    94%
src/common/logging_setup.py        12      0   100%
src/common/schema.py               30      2    93%
src/data/__init__.py                0      0   100%
src/data/generate_paysim.py        54     11    80%
src/etl/__init__.py                 0      0   100%
src/etl/batch_etl.py               79     14    82%
src/etl/check_input.py             17     17     0%
src/ml/__init__.py                  0      0   100%
src/ml/features.py                 27      0   100%
src/ml/train.py                   116     17    85%
src/monitoring/__init__.py          0      0   100%
src/monitoring/drift.py            30      1    97%
src/monitoring/run_drift.py        22     22     0%
src/scoring/__init__.py             0      0   100%
src/scoring/batch_score.py         34      3    91%
src/scoring/scorer.py              52      1    98%
src/streaming/__init__.py           0      0   100%
src/streaming/consumer.py          59     36    39%
src/streaming/processor.py         54      0   100%
src/streaming/producer.py          40     22    45%
src/streaming/spark_stream.py      55      6    89%
TOTAL                            1055    169    84%
```
Lines not covered are mostly the `main()` / CLI wrappers and the Kafka-specific loops (`consumer.main`, `producer.main`,
`spark_stream.main`), which need a broker or are exercised only manually / via the e2e subprocess (subprocess runs are not counted by coverage).

## 2. What each verified item actually proves
* **Dependency installation** — `pip install -r requirements.txt` into a fresh venv succeeded (after the kafka-python pin fix, bug #1); dbt and Airflow installed in separate venvs; `npm install` + `npm run build` succeeded.
* **PostgreSQL** — role/db creation, 4 migrations applied twice (second run = 0 new), constraints (`CHECK`, FKs), idempotent inserts, retry helper (mocked transient `OperationalError`: retried then succeeded; gave up after N), `pipeline_runs` success/failure recording.
* **ETL / PySpark** — 100 000 rows → 100 000 loaded; second run inserts 0; bad rows (bad type, negative amount, null party, null numeric) rejected with reasons; exact duplicates collapsed; **Spark `event_id` equals the Python `event_id` for 300 rows** (needed for cross-path de-duplication).
* **Spark streaming + checkpoint recovery** — file source standing in for Kafka: wave 1 (422 rows: 400 new + 20 duplicates + 2 invalid) → 400 inserted, 20 duplicates ignored, 2 rejected; restart with no new files → **0 batches reprocessed**; add wave 2 → only the 200 new rows processed. Re-verified inside `tests/test_e2e_stream.py` (500 rows total).
* **ML** — `python -m src.ml.train` ran end-to-end; MLflow experiment with 2 runs (baseline + XGBoost), model registered as `fraud-xgb` in the sqlite registry; metrics persisted to `model_metrics`; test asserts model beats trivial baselines and scores stay within ranges.
* **FastAPI** — all endpoints exercised including 422 validation, 404, 401 (API key), 503 (model missing), idempotent ingest, filters/pagination/sort, a SQL-injection-style search string (harmless; table intact).
* **Frontend** — 8 tests render the real app against the live API: health badge, Overview KPIs equal DB numbers, transaction list/filter/detail drawer, empty-state, alert status update through the API, model metrics + confusion matrix + drift run, analytics and pipelines pages, error banner on a 503.
* **End-to-end** — `tests/test_e2e_stream.py`: generation → JSON events → Spark Structured Streaming → validation/dedup → ML scoring → PostgreSQL → alerts → API → analytics, plus restart/recovery.

## 3. Model results (synthetic data; time-based test split of 15 000 rows, 145 fraud)
| Model | Precision | Recall | F1 | PR-AUC | ROC-AUC |
|---|---|---|---|---|---|
| XGBoost (threshold 0.863, chosen on validation) | 0.738 | 0.568 | 0.642 | 0.752 | 0.996 |
| Logistic regression baseline | 0.544 | 0.626 | 0.582 | 0.647 | 0.995 |
Confusion matrix (XGBoost, test): `{'tn': 14833, 'fp': 28, 'fn': 60, 'tp': 79}`. IsolationForest anomaly ROC-AUC: 0.932.

Note: an earlier version of the generator made fraud trivially separable and gave F1 = 1.0 on the test split; that was judged
not meaningful and the generator was changed (hard negatives, partial-drain fraud) before any reported number.

## 4. Bugs found by testing and fixed
| # | Found by | Bug | Fix |
|---|---|---|---|
| 1 | first producer import | `kafka-python 2.0.2` fails to import on Python 3.12 (`kafka.vendor.six.moves`) | pinned `kafka-python==2.3.3` |
| 2 | migration run | `exec_driver_sql` choked on `%` in `LIKE 'M%'` | execute scripts through a raw DBAPI cursor |
| 3 | training | MLflow sqlite backend needs `alembic` (missing with `mlflow-skinny`) | added `alembic` to requirements |
| 4 | Spark streaming | PySpark 3.5 `toPandas()` needs `distutils` (removed in 3.12) inside `foreachBatch` | use `collect()` for micro-batches |
| 5 | Streamlit AppTest | `v_transactions_scored` lacked `ingested_at` → dashboard error | migration `004_view_ingested_at.sql` |
| 6 | Airflow DAG run | Python tasks ran in Airflow's env without project deps (`No module named dotenv`) | tasks now call project CLI modules via `PYTHON_BIN` / `DBT_BIN`; added `check_input.py`, `run_drift.py` |
| 7 | pytest in main venv | project folder `airflow/` shadowed the `airflow` package | renamed to `orchestration/dags/` |
| 8 | dbt install | `pip install dbt-postgres` resolved `dbt-core 2.0.0rc` (no Postgres support) | `requirements-dbt.txt` pins `dbt-core==1.8.8` |
| 9 | model sanity check | synthetic data too easy (F1 = 1.0) | harder generator, see §3 |
| 10 | frontend test | my test used `findByText` on text present twice | `findAllByText` (test bug, not product bug) |
| 11 | housekeeping | my own cleanup command briefly deleted `api/routers/`; both router files were re-created from source and the full suite re-run | — |
Observed retry behaviour: during bug #6 the Airflow task `check_input` was marked `UP_FOR_RETRY` and re-attempted automatically (retry config is real, not just declared).

## 5. Not verified (could not be executed in this environment)
| Component | Why | Risk / what was done instead |
|---|---|---|
| **Kafka broker, producer ↔ consumer communication, DLQ topic** | no Kafka distribution reachable (only PyPI/npm/GitHub/Ubuntu mirrors allowed; Docker absent) | consumer/producer code imports and its pure logic is unit-tested (`decode`, event building, shared `process_records`); real broker round-trip, rebalancing and offset-commit behaviour are **untested** |
| **Spark Kafka source** (`--source kafka`) | needs `spark-sql-kafka` jar from Maven Central (blocked) + broker | identical sink logic verified through the file source; Kafka-specific read path untested |
| **Docker Compose stack / Dockerfiles** | no Docker daemon | YAML parsed, `depends_on` targets checked; image builds, healthchecks, Kafka listener config, nginx proxy and the Airflow image are untested |
| **Power BI Desktop connectivity and .pbix** | Windows desktop app | the 6 import queries run against PostgreSQL; `.pbids`, DAX and theme are provided but not opened in Power BI; **no .pbix included** |
| **Windows PowerShell install** | Linux-only sandbox | commands written to mirror the Linux flow; PySpark on native Windows may need winutils (use WSL2/Docker) |
| **Real PaySim dataset** | Kaggle not reachable | all runs use synthetic data; metrics are not comparable to published PaySim results |
| **Real-browser rendering of the React UI** | no browser available | jsdom tests + production build only; layout/visual quality not inspected |
| **Airflow scheduler/UI, multi-worker** | only `airflow dags test` (in-process) was run | DAG logic and retries proven; scheduler behaviour untested |
| **Performance at scale** (6.3 M rows) | 1 vCPU / 3 GB sandbox | only 100 k rows tested |
| **Clean-machine install of documented commands** | see §6 | partially exercised in a fresh venv |

## 6. Clean-room check of the shipped ZIP (executed)
The ZIP was extracted into an empty directory; a **new virtualenv** was created and `pip install -r requirements.txt` run; `.env` was created
from `.env.example` (only the password and a separate database name changed); then the README commands were executed from that copy
(full log: `docs/evidence/cleanroom.txt`):

| Step | Result |
|---|---|
| `unzip`, `python -m venv`, `pip install -r requirements.txt` | exit 0 |
| `python scripts/migrate.py` (empty database) | 4 migrations applied |
| `python -m src.data.generate_paysim --rows 30000` | 30 000 rows, 300 fraud |
| `python -m src.etl.batch_etl` | 30 000 loaded |
| `python -m src.ml.train` | XGBoost test precision 0.875, recall 0.467, F1 0.609, PR-AUC 0.809 (30k-row run; different data size than §3) |
| `python -m src.scoring.batch_score` | 30 000 scored, 241 alerts |
| `uvicorn api.main:app` + `curl /health`, `/analytics/overview` | status ok, 30 000 transactions |
| `python scripts/check_powerbi_queries.py` | 6/6 PASS |
| `pytest -q` | 56 passed, 1 skipped |

Not part of the clean-room run: dbt and Airflow (separate venvs, verified earlier in the build tree with the same files), the React frontend tests, and anything in §5.
