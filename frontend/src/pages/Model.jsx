import { useState } from "react";
import { api } from "../api";
import { fmtPct, fmtTime, useApi } from "../hooks";
import { Kpi, Panel, State } from "../components/ui";

export default function Model() {
  const m = useApi("/model/metrics");
  const drift = useApi("/monitoring/drift");
  const [running, setRunning] = useState(false);
  const [err, setErr] = useState(null);
  const run = async () => {
    setRunning(true); setErr(null);
    try { await api("/monitoring/drift/run?window=5000", { method: "POST" }); drift.reload(); }
    catch (e) { setErr(e); } finally { setRunning(false); }
  };
  const c = m.data?.confusion;
  return (
    <>
      <h1>Model performance</h1>
      <State loading={m.loading} error={m.error}>
        {m.data && (
          <>
            <p className="muted">Active model <span className="mono">{m.data.active_model}</span> · decision threshold {m.data.threshold.toFixed(3)} · held-out test split (time-based)</p>
            <div className="kpis">
              <Kpi label="Precision" value={fmtPct(m.data.test.precision)} />
              <Kpi label="Recall" value={fmtPct(m.data.test.recall)} />
              <Kpi label="F1" value={fmtPct(m.data.test.f1)} />
              <Kpi label="PR-AUC" value={m.data.test.pr_auc.toFixed(3)} hint={`baseline logistic regression: ${m.data.baseline_logreg.pr_auc.toFixed(3)}`} />
            </div>
            <div className="grid2">
              <Panel title="Confusion matrix (test)">
                <div className="cm" role="table" aria-label="Confusion matrix">
                  <div className="cm-h" /><div className="cm-h">Predicted legit</div><div className="cm-h">Predicted fraud</div>
                  <div className="cm-h">Actually legit</div><div className="cm-c ok">{c.tn.toLocaleString("en-US")}<small>true negatives</small></div><div className="cm-c bad">{c.fp.toLocaleString("en-US")}<small>false positives</small></div>
                  <div className="cm-h">Actually fraud</div><div className="cm-c bad">{c.fn.toLocaleString("en-US")}<small>missed fraud</small></div><div className="cm-c ok">{c.tp.toLocaleString("en-US")}<small>caught fraud</small></div>
                </div>
              </Panel>
              <Panel title="Training history">
                <table>
                  <thead><tr><th>Trained</th><th>Model</th><th className="num">F1</th><th className="num">PR-AUC</th></tr></thead>
                  <tbody>{m.data.history.map((h) => (
                    <tr key={h.model_version + h.created_at}><td>{fmtTime(h.created_at)}</td><td className="mono">{h.model_version}</td><td className="num">{fmtPct(h.metrics.f1)}</td><td className="num">{h.metrics.pr_auc.toFixed(3)}</td></tr>))}</tbody>
                </table>
              </Panel>
            </div>
          </>
        )}
      </State>
      <Panel title="Data drift (PSI vs training data)" actions={<button className="primary" onClick={run} disabled={running}>{running ? "Running…" : "Run drift check"}</button>}>
        {err && <div className="notice error" role="alert">Drift check failed. {err.message}</div>}
        <State loading={drift.loading} error={drift.error}>
          {drift.data && drift.data.features.length === 0 && <div className="notice">No drift report yet. Run a drift check to compare recent transactions with the training data.</div>}
          {drift.data && drift.data.features.length > 0 && (
            <table>
              <thead><tr><th>Feature</th><th className="num">PSI</th><th>Status</th><th className="num">Rows compared</th><th>Checked</th></tr></thead>
              <tbody>{drift.data.features.map((d) => (
                <tr key={d.feature}><td>{d.feature}</td><td className="num">{d.psi.toFixed(4)}</td><td><span className={`pill ${d.status === "OK" ? "LOW" : d.status === "WARN" ? "MEDIUM" : "CRITICAL"}`}>{d.status}</span></td><td className="num">{d.n_current.toLocaleString("en-US")}</td><td>{fmtTime(d.created_at)}</td></tr>))}</tbody>
            </table>
          )}
        </State>
      </Panel>
    </>
  );
}
