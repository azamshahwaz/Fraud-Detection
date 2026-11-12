
"""Central configuration for the Fraud Detection Platform."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


# =========================================================
# PROJECT DIRECTORIES
# =========================================================

# File location:
# backend/src/common/config.py

# Project root:
# fraud-detection-platform/

ROOT = Path(__file__).resolve().parents[3]

# Backend directory:
# fraud-detection-platform/backend/

BACKEND_DIR = ROOT / "backend"

# Prefer backend/.env (used by the current backend setup), with root .env as
# a fallback for the repository's documented copy-.env-example workflow.
load_dotenv(BACKEND_DIR / ".env", override=False)
load_dotenv(ROOT / ".env", override=False)


# =========================================================
# ENVIRONMENT HELPERS
# =========================================================

def _env(
    name: str,
    default: str | None = None,
    required: bool = False,
) -> str:
    """Read an environment variable."""

    value = os.getenv(name, default)

    if required and (value is None or value == ""):
        raise RuntimeError(
            f"Missing required environment variable: {name}"
        )

    return value  # type: ignore[return-value]


def _path(name: str, default: str) -> Path:
    """Resolve relative paths from the project root."""

    path = Path(_env(name, default))

    if path.is_absolute():
        return path

    return (ROOT / path).resolve()


# =========================================================
# APPLICATION SETTINGS
# =========================================================

@dataclass(frozen=True)
class Settings:

    # -----------------------------------------------------
    # API SERVER
    # -----------------------------------------------------

    api_host: str = field(default_factory=lambda: _env("API_HOST", "0.0.0.0"))
    api_port: int = field(default_factory=lambda: int(_env("API_PORT", "8000")))

    # -----------------------------------------------------
    # POSTGRESQL
    # -----------------------------------------------------

    pg_host: str = field(
        default_factory=lambda: _env("POSTGRES_HOST", "localhost")
    )

    pg_port: int = field(
        default_factory=lambda: int(_env("POSTGRES_PORT", "5432"))
    )

    pg_db: str = field(
        default_factory=lambda: _env("POSTGRES_DB", "fraud_db")
    )

    pg_user: str = field(
        default_factory=lambda: _env("POSTGRES_USER", "fraud_user")
    )

    pg_password: str = field(
        default_factory=lambda: _env(
            "POSTGRES_PASSWORD",
            required=True,
        )
    )

    # -----------------------------------------------------
    # KAFKA
    # -----------------------------------------------------

    kafka_bootstrap: str = field(
        default_factory=lambda: _env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )
    )

    kafka_topic: str = field(
        default_factory=lambda: _env(
            "KAFKA_TOPIC",
            "transactions",
        )
    )

    kafka_dlq_topic: str = field(
        default_factory=lambda: _env(
            "KAFKA_DLQ_TOPIC",
            "transactions.dlq",
        )
    )

    kafka_group: str = field(
        default_factory=lambda: _env(
            "KAFKA_GROUP_ID",
            "fraud-scorer",
        )
    )

    # -----------------------------------------------------
    # SPARK
    # -----------------------------------------------------

    checkpoint_dir: Path = field(
        default_factory=lambda: _path(
            "SPARK_CHECKPOINT_DIR",
            "./data/checkpoints",
        )
    )

    shuffle_partitions: int = field(
        default_factory=lambda: int(
            _env("SPARK_SHUFFLE_PARTITIONS", "4")
        )
    )

    # -----------------------------------------------------
    # MLFLOW
    # -----------------------------------------------------

    mlflow_uri: str = field(
        default_factory=lambda: _env(
            "MLFLOW_TRACKING_URI",
            "sqlite:///./mlflow.db",
        )
    )

    mlflow_experiment: str = field(
        default_factory=lambda: _env(
            "MLFLOW_EXPERIMENT",
            "fraud-detection",
        )
    )

    # -----------------------------------------------------
    # MACHINE LEARNING MODEL
    # -----------------------------------------------------

    model_dir: Path = field(
        default_factory=lambda: _path(
            "MODEL_DIR",
            "./models",
        )
    )

    model_name: str = field(
        default_factory=lambda: _env(
            "MODEL_NAME",
            "fraud-xgb",
        )
    )

    # -----------------------------------------------------
    # DATASET
    # -----------------------------------------------------

    data_dir: Path = field(
        default_factory=lambda: _path(
            "DATA_DIR",
            "./data",
        )
    )

    paysim_csv: Path = field(
        default_factory=lambda: _path(
            "PAYSIM_CSV",
            "./data/raw/paysim.csv",
        )
    )

    # -----------------------------------------------------
    # API SECURITY
    # -----------------------------------------------------

    api_key: str = field(
        default_factory=lambda: _env("API_KEY", "")
    )

    # -----------------------------------------------------
    # CORS
    # -----------------------------------------------------

    cors_origins: list[str] = field(
        default_factory=lambda: [
            origin.strip()
            for origin in _env(
                "CORS_ORIGINS",
                "http://localhost:5173,http://127.0.0.1:5173",
            ).split(",")
            if origin.strip()
        ]
    )

    # -----------------------------------------------------
    # FRAUD ALERTS
    # -----------------------------------------------------

    alert_threshold: float = field(
        default_factory=lambda: float(
            _env("ALERT_RISK_THRESHOLD", "80")
        )
    )

    # -----------------------------------------------------
    # LOGGING
    # -----------------------------------------------------

    log_level: str = field(
        default_factory=lambda: _env("LOG_LEVEL", "INFO")
    )

    # -----------------------------------------------------
    # DATABASE CONNECTION STRING
    # -----------------------------------------------------

    @property
    def pg_dsn(self) -> str:
        """Build PostgreSQL SQLAlchemy connection URL."""

        from urllib.parse import quote_plus

        username = quote_plus(self.pg_user)
        password = quote_plus(self.pg_password)

        return (
            f"postgresql+psycopg2://"
            f"{username}:{password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


# =========================================================
# SETTINGS SINGLETON
# =========================================================

_settings: Settings | None = None

def get_settings() -> Settings:
    """Return cached application settings."""

    global _settings

    if _settings is None:
        _settings = Settings()

    return _settings


def reset_settings() -> None:
    """Reset settings cache, primarily for testing."""

    global _settings

    _settings = None
