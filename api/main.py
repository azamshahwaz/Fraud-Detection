"""FastAPI application. Run: uvicorn api.main:app --reload"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api.routers import analytics, core
from src.common.config import get_settings
from src.common.db import get_engine
from src.common.logging_setup import get_logger
from src.scoring.scorer import ModelNotAvailable, get_scorer

log = get_logger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        get_scorer()
    except ModelNotAvailable as exc:
        log.warning("starting without model: %s", exc)
    yield


app = FastAPI(title="Fraud Detection Platform API", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=get_settings().cors_origins, allow_methods=["*"], allow_headers=["*"])


@app.middleware("http")
async def access_log(request: Request, call_next):
    t0 = time.perf_counter()
    resp = await call_next(request)
    log.info("%s %s -> %s (%.1f ms)", request.method, request.url.path, resp.status_code, (time.perf_counter() - t0) * 1000)
    return resp


@app.exception_handler(SQLAlchemyError)
async def db_error(_: Request, exc: SQLAlchemyError):
    log.error("database error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "Database unavailable or query failed"})


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={"detail": [{"loc": e["loc"], "msg": e["msg"]} for e in exc.errors()]})


@app.get("/health", tags=["system"])
def health():
    db_ok = True
    try:
        with get_engine().connect() as c:
            c.execute(text("SELECT 1"))
    except Exception:  # noqa: BLE001
        db_ok = False
    try:
        model = get_scorer().version
    except ModelNotAvailable:
        model = None
    return {"status": "ok" if db_ok and model else "degraded", "database": db_ok, "model": model}


app.include_router(core.router)
app.include_router(analytics.router)
