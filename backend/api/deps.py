import hmac

from fastapi import Header, HTTPException

from backend.src.common.config import get_settings
from backend.src.scoring.scorer import ModelNotAvailable, get_scorer


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """If API_KEY is set in the environment, every request must send a matching X-API-Key header."""
    expected = get_settings().api_key
    if expected and not (x_api_key and hmac.compare_digest(x_api_key, expected)):
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


def scorer_dep():
    try:
        return get_scorer()
    except ModelNotAvailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
