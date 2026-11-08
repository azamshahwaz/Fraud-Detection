from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

TxnType = Literal["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]


class TransactionIn(BaseModel):
    step: int = Field(..., ge=0, description="Simulated hour (PaySim step)")
    type: TxnType
    amount: float = Field(..., ge=0)
    name_orig: str = Field(..., min_length=1, max_length=40)
    old_balance_org: float = Field(..., ge=0)
    new_balance_org: float = Field(..., ge=0)
    name_dest: str = Field(..., min_length=1, max_length=40)
    old_balance_dest: float = Field(..., ge=0)
    new_balance_dest: float = Field(..., ge=0)
    event_id: Optional[str] = Field(None, max_length=64, description="Optional idempotency key")

    @field_validator("amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("must be finite")
        return v


class PredictionOut(BaseModel):
    model_config = {"protected_namespaces": ()}
    fraud_probability: float
    anomaly_score: float
    risk_score: float
    risk_level: str
    is_fraud_pred: bool
    model_version: str


class IngestOut(PredictionOut):
    model_config = {"protected_namespaces": ()}
    event_id: str
    duplicate: bool
    alert_created: bool


class BatchIn(BaseModel):
    transactions: list[TransactionIn] = Field(..., min_length=1, max_length=1000)


class AlertUpdate(BaseModel):
    status: Literal["OPEN", "INVESTIGATING", "CONFIRMED", "DISMISSED"]
