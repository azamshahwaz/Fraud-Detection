"""Central configuration. All values come from environment variables (.env supported)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")


def _env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val  # type: ignore[return-value]


def _path(name: str, default: str) -> Path:
    p = Path(_env(name, default))
    return p if p.is_absolute() else (ROOT / p).resolve()


@dataclass(frozen=True)
class Settings:
    pg_host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: int(_env("POSTGRES_PORT", "5432")))
    pg_db: str = field(default_factory=lambda: _env("POSTGRES_DB", "fraud_db"))
    pg_user: str = field(default_factory=lambda: _env("POSTGRES_USER", "fraud_user"))
    pg_password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", required=True))
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    kafka_topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "transactions"))
    kafka_dlq_topic: str = field(default_factory=lambda: _env("KAFKA_DLQ_TOPIC", "transactions.dlq"))
    kafka_group: str = field(default_factory=lambda: _env("KAFKA_GROUP_ID", "fraud-scorer"))
    checkpoint_dir: Path = field(default_factory=lambda: _path("SPARK_CHECKPOINT_DIR", "./data/checkpoints"))
    shuffle_partitions: int = field(default_factory=lambda: int(_env("SPARK_SHUFFLE_PARTITIONS", "4")))
    mlflow_uri: str = field(default_factory=lambda: _env("MLFLOW_TRACKING_URI", "sqlite:///./mlflow.db"))
    mlflow_experiment: str = field(default_factory=lambda: _env("MLFLOW_EXPERIMENT", "fraud-detection"))
    model_dir: Path = field(default_factory=lambda: _path("MODEL_DIR", "./models"))
    model_name: str = field(default_factory=lambda: _env("MODEL_NAME", "fraud-xgb"))
    data_dir: Path = field(default_factory=lambda: _path("DATA_DIR", "./data"))
    paysim_csv: Path = field(default_factory=lambda: _path("PAYSIM_CSV", "./data/raw/paysim.csv"))
    api_key: str = field(default_factory=lambda: _env("API_KEY", ""))
    cors_origins: list[str] = field(
        default_factory=lambda: [o.strip() for o in _env("CORS_ORIGINS", "http://localhost:5173").split(",") if o.strip()]
    )
    alert_threshold: float = field(default_factory=lambda: float(_env("ALERT_RISK_THRESHOLD", "80")))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))

    @property
    def pg_dsn(self) -> str:
        from urllib.parse import quote_plus
        return (f"postgresql+psycopg2://{quote_plus(self.pg_user)}:{quote_plus(self.pg_password)}"
                f"@{self.pg_host}:{self.pg_port}/{self.pg_db}")


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Used by tests to re-read the environment."""
    global _settings
    _settings = None
