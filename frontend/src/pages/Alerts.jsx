import { useState } from "react";
import { api } from "../api";
import { fmtMoney, fmtTime, useApi } from "../hooks";
import { Panel, RiskMeter, State } from "../components/ui";

const STATUSES = ["OPEN", "INVESTIGATING", "CONFIRMED", "DISMISSED"];

export default function Alerts() {
  const [status, setStatus] = useState("OPEN");
  const [err, setErr] = useState(null);
  const { data, error, loading, reload } = useApi(`/alerts?status=${status}&limit=50`, { refreshMs: 15000 });
  const act = async (id, next) => {
    try { setErr(null); await api(`/alerts/${id}`, { method: "PATCH", body: { status: next } }); reload(); }
    catch (e) { setErr(e); }
  };
  return (
    <>
      <h1>Fraud alerts</h1>
      <div className="tabs" role="tablist">
        {STATUSES.map((s) => <button key={s} role="tab" aria-selected={s === status} className={s === status ? "on" : ""} onClick={() => setStatus(s)}>{s.toLowerCase()}</button>)}
      </div>
      {err && <div className="notice error" role="alert">Could not update alert. {err.message}</div>}
      <Panel title={`${status.toLowerCase()} alerts, highest risk first`}>
        <State loading={loading && !data} error={error}>
          {data && data.items.length === 0 && <div className="notice">No {status.toLowerCase()} alerts.</div>}
          {data && data.items.length > 0 && (
            <div className="scroll">
              <p className="muted">Showing {data.items.length} of {data.total.toLocaleString("en-US")}</p>
              <table>
                <thead><tr><th>Raised</th><th>Type</th><th className="num">Amount</th><th>From</th><th>To</th><th>Risk</th><th>Action</th></tr></thead>
                <tbody>
                  {data.items.map((a) => (
                    <tr key={a.alert_id}>
                      <td>{fmtTime(a.created_at)}</td><td>{a.type}</td><td className="num">{fmtMoney(a.amount)}</td>
                      <td className="mono">{a.name_orig}</td><td className="mono">{a.name_dest}</td>
                      <td><RiskMeter score={a.risk_score} level={a.risk_level} /></td>
                      <td className="actions">
                        {a.status !== "INVESTIGATING" && <button onClick={() => act(a.alert_id, "INVESTIGATING")}>Investigate</button>}
                        {a.status !== "CONFIRMED" && <button className="danger" onClick={() => act(a.alert_id, "CONFIRMED")}>Confirm fraud</button>}
                        {a.status !== "DISMISSED" && <button onClick={() => act(a.alert_id, "DISMISSED")}>Dismiss</button>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </State>
      </Panel>
    </>
  );
}
