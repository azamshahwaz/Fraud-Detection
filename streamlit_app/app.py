"""Real-time fraud monitoring (Streamlit). Reads live data from PostgreSQL; auto-refreshes.

  streamlit run streamlit_app/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.src.common.db import get_engine  # noqa: E402

st.set_page_config(page_title="Fraud Monitor", layout="wide")
st.title("Real-time fraud monitor")

with st.sidebar:
    window = st.slider("Window (latest N transactions)", 100, 20000, 2000, step=100)
    min_risk = st.slider("Minimum risk score", 0, 100, 60)
    refresh = st.selectbox("Auto-refresh", ["Off", "5s", "10s", "30s"], index=2)


def q(sql: str, **params) -> pd.DataFrame:
    with get_engine().connect() as c:
        return pd.read_sql(text(sql), c, params=params)


@st.fragment(run_every=None if refresh == "Off" else refresh)
def dashboard() -> None:
    try:
        kpi = q("SELECT count(*) n, coalesce(sum(amount),0) vol, "
                "count(*) FILTER (WHERE is_fraud_pred) flagged, "
                "(SELECT count(*) FROM fraud_alerts WHERE status='OPEN') open_alerts, "
                "max(ingested_at) last_ingest FROM v_transactions_scored")
        recent = q("SELECT event_id, event_time, type, amount, name_orig, name_dest, risk_score, risk_level, "
                   "is_fraud_pred FROM v_transactions_scored WHERE risk_score IS NOT NULL "
                   "ORDER BY ingested_at DESC, event_time DESC LIMIT :w", w=window)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Cannot read from PostgreSQL: {exc}")
        return
    k = kpi.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Transactions", f"{int(k.n):,}")
    c2.metric("Volume", f"{float(k.vol):,.0f}")
    c3.metric("Flagged by model", f"{int(k.flagged):,}")
    c4.metric("Open alerts", f"{int(k.open_alerts):,}")
    if recent.empty:
        st.info("No scored transactions yet. Run the ETL and scoring pipeline, then refresh.")
        return
    left, right = st.columns(2)
    with left:
        st.subheader("Risk level mix (window)")
        st.bar_chart(recent["risk_level"].value_counts().reindex(["LOW", "MEDIUM", "HIGH", "CRITICAL"]).fillna(0))
    with right:
        st.subheader("Average risk by transaction type")
        st.bar_chart(recent.groupby("type")["risk_score"].mean())
    st.subheader(f"Latest transactions with risk ≥ {min_risk}")
    hot = recent[recent["risk_score"] >= min_risk].head(200)
    st.caption(f"{len(hot)} shown")
    st.dataframe(hot, use_container_width=True, hide_index=True)


dashboard()
