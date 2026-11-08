#!/usr/bin/env bash
# Build a clean distributable ZIP (no secrets, venvs, caches, generated data).
set -euo pipefail
cd "$(dirname "$0")/.."
OUT="${1:-../fraud-detection-platform.zip}"; rm -f "$OUT"
zip -qr "$OUT" . -x ".env" "*/__pycache__/*" "__pycache__/*" "*.pyc" ".pytest_cache/*" ".coverage" "htmlcov/*" \
  "frontend/node_modules/*" "frontend/dist/*" "mlruns/*" "mlflow.db" "models/*.joblib" "models/*.json" \
  "data/raw/*.csv" "data/processed/*" "data/checkpoints/*" "data/stream_in/*" "dbt_project/target/*" "dbt_project/logs/*" \
  "dbt_project/dbt_packages/*" "*.log" ".venv/*" "venv/*" "spark-warehouse/*" "*/.DS_Store"
echo "created $OUT"
