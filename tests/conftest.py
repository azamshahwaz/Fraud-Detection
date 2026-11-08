import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
_TMP = Path(tempfile.mkdtemp(prefix="fraud_tests_"))
# Isolated test environment: separate DB, model dir and MLflow store. Must be set before src imports.
os.environ["POSTGRES_DB"] = os.environ.get("TEST_POSTGRES_DB", "fraud_test")
os.environ["MODEL_DIR"] = str(_TMP / "models")
os.environ["MLFLOW_TRACKING_URI"] = f"sqlite:///{_TMP}/mlflow.db"
os.environ["API_KEY"] = ""
os.environ["ALERT_RISK_THRESHOLD"] = "80"

import pytest  # noqa: E402
from sqlalchemy import text  # noqa: E402

from src.common.config import reset_settings  # noqa: E402

reset_settings()


@pytest.fixture(scope="session", autouse=True)
def _env_setup():
    import subprocess
    from src.common.db import get_engine
    from src.data.generate_paysim import generate
    from src.ml.train import train
    from src.scoring.scorer import get_scorer
    subprocess.run([sys.executable, str(ROOT / "scripts" / "migrate.py")], check=True, env=os.environ.copy(),
                   capture_output=True)
    csv = _TMP / "small.csv"
    generate(30000, 0.02, seed=7).to_csv(csv, index=False)
    train(csv, save_db=False)
    get_scorer(reload=True)
    yield
    get_engine().dispose()


@pytest.fixture()
def clean_db():
    from src.common.db import get_engine
    with get_engine().begin() as c:
        c.execute(text("TRUNCATE transactions, pipeline_runs, model_metrics, drift_reports RESTART IDENTITY CASCADE"))
    yield


@pytest.fixture(scope="session")
def scorer():
    from src.scoring.scorer import get_scorer
    return get_scorer()


@pytest.fixture(scope="session")
def spark():
    from src.etl.batch_etl import get_spark
    return get_spark("tests")
