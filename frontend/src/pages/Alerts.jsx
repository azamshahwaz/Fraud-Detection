import { useState } from "react";
import { api } from "../api";
import { fmtMoney, fmtTime, useApi } from "../hooks";
import { Pager, Panel, RiskMeter, State } from "../components/ui";

const STATUSES = ["OPEN", "INVESTIGATING", "CONFIRMED", "DISMISSED"];
const LIMIT = 50;

export default function Alerts() {
  const [status, setStatus] = useState("OPEN");
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState(null);
  const [err, setErr] = useState(null);
  const [failedAction, setFailedAction] = useState(null);
  const { data, error, loading, reload } = useApi(`/alerts?status=${status}&limit=${LIMIT}&offset=${offset}`, { refreshMs: 15000 });
  const detail = useApi(selected ? `/transactions/${selected}` : null);
  const act = async (id, next) => {
    try { setErr(null); setFailedAction(null); await api(`/alerts/${id}`, { method: "PATCH", body: { status: next } }); reload(); }
    catch (e) { setErr(e); setFailedAction({ id, next }); }
  };
  return (
    <>
      <h1>Fraud alerts</h1>
      <div className="tabs" role="tablist">
        {STATUSES.map((s) => <button key={s} role="tab" aria-selected={s === status} className={s === status ? "on" : ""} onClick={() => { setStatus(s); setOffset(0); }}>{s.toLowerCase()}</button>)}
      </div>
      {err && <div className="notice error" role="alert">Could not update alert. {err.message}{failedAction && <> <button onClick={() => act(failedAction.id, failedAction.next)}>Retry update</button></>}</div>}
      <Panel title={`${status.toLowerCase()} alerts, highest risk first`}>
        <State loading={loading && !data} error={error} retry={reload}>
          {data && data.items.length === 0 && <div className="notice">No {status.toLowerCase()} alerts.</div>}
          {data && data.items.length > 0 && (
            <div className="scroll">
              <p className="muted">Showing {data.items.length} of {data.total.toLocaleString("en-US")}</p>
              <table>
                <thead><tr><th>Raised</th><th>Type</th><th className="num">Amount</th><th>From</th><th>To</th><th>Risk</th><th>Action</th></tr></thead>
                <tbody>
                  {data.items.map((a) => (
                    <tr key={a.alert_id}>
                      <td><button className="link-button" onClick={() => setSelected(a.event_id)}>{fmtTime(a.created_at)}</button></td><td>{a.type}</td><td className="num">{fmtMoney(a.amount)}</td>
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
              <Pager total={data.total} limit={LIMIT} offset={offset} onChange={setOffset} />
            </div>
          )}
        </State>
      </Panel>
      {selected && <aside className="drawer" aria-label="Alert detail">
        <button className="close" onClick={() => setSelected(null)}>Close</button>
        <h2>Alert transaction detail</h2>
        <State loading={detail.loading} error={detail.error} retry={detail.reload}>
          {detail.data && <dl>{["event_id", "event_time", "type", "amount", "name_orig", "name_dest", "fraud_probability", "anomaly_score", "risk_score", "risk_level", "model_version"].map((k) => <div key={k}><dt>{k.replaceAll("_", " ")}</dt><dd className="mono">{String(detail.data[k] ?? "–")}</dd></div>)}</dl>}
        </State>
      </aside>}
    </>
  );
}
