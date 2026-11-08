import { useMemo, useState } from "react";
import { qs } from "../api";
import { fmtMoney, fmtTime, useApi } from "../hooks";
import { Pager, Panel, RiskMeter, State } from "../components/ui";

const LIMIT = 25;
const TYPES = ["", "PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"];
const LEVELS = ["", "LOW", "MEDIUM", "HIGH", "CRITICAL"];

export default function Transactions() {
  const [f, setF] = useState({ q: "", type: "", risk_level: "", min_amount: "", sort: "event_time" });
  const [applied, setApplied] = useState(f);
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState(null);
  const path = useMemo(() => `/transactions${qs({ ...applied, limit: LIMIT, offset })}`, [applied, offset]);
  const { data, error, loading } = useApi(path);
  const detail = useApi(selected ? `/transactions/${selected}` : null);
  const set = (k) => (e) => setF({ ...f, [k]: e.target.value });
  const submit = (e) => { e.preventDefault(); setOffset(0); setApplied(f); };

  return (
    <>
      <h1>Transactions</h1>
      <form className="filters" onSubmit={submit} aria-label="Transaction filters">
        <label>Search<input value={f.q} onChange={set("q")} placeholder="Account or event id" /></label>
        <label>Type<select value={f.type} onChange={set("type")}>{TYPES.map((t) => <option key={t} value={t}>{t || "All types"}</option>)}</select></label>
        <label>Risk level<select value={f.risk_level} onChange={set("risk_level")}>{LEVELS.map((t) => <option key={t} value={t}>{t || "All levels"}</option>)}</select></label>
        <label>Minimum amount<input type="number" min="0" value={f.min_amount} onChange={set("min_amount")} /></label>
        <label>Sort by<select value={f.sort} onChange={set("sort")}><option value="event_time">Newest</option><option value="amount">Amount</option><option value="risk_score">Risk score</option></select></label>
        <button type="submit" className="primary">Apply filters</button>
      </form>
      <Panel title="Results">
        <State loading={loading && !data} error={error}>
          {data && data.items.length === 0 && <div className="notice">No transactions match these filters. Clear a filter to widen the search.</div>}
          {data && data.items.length > 0 && (
            <>
              <div className="scroll">
                <table>
                  <thead><tr><th>Time</th><th>Type</th><th className="num">Amount</th><th>From</th><th>To</th><th>Risk</th><th>Label</th></tr></thead>
                  <tbody>
                    {data.items.map((r) => (
                      <tr key={r.event_id} className="click" onClick={() => setSelected(r.event_id)}>
                        <td>{fmtTime(r.event_time)}</td><td>{r.type}</td><td className="num">{fmtMoney(r.amount)}</td>
                        <td className="mono">{r.name_orig}</td><td className="mono">{r.name_dest}</td>
                        <td><RiskMeter score={r.risk_score} level={r.risk_level} /></td>
                        <td>{r.is_fraud == null ? <span className="muted">unlabelled</span> : r.is_fraud ? <span className="pill CRITICAL">fraud</span> : "legit"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <Pager total={data.total} limit={LIMIT} offset={offset} onChange={setOffset} />
            </>
          )}
        </State>
      </Panel>
      {selected && (
        <aside className="drawer" aria-label="Transaction detail">
          <button className="close" onClick={() => setSelected(null)}>Close</button>
          <h2>Transaction detail</h2>
          <State loading={detail.loading} error={detail.error}>
            {detail.data && (
              <dl>
                {["event_id", "event_time", "type", "amount", "name_orig", "name_dest", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest", "fraud_probability", "anomaly_score", "risk_score", "risk_level", "model_version"].map((k) => (
                  <div key={k}><dt>{k.replaceAll("_", " ")}</dt><dd className="mono">{String(detail.data[k] ?? "–")}</dd></div>
                ))}
              </dl>
            )}
          </State>
        </aside>
      )}
    </>
  );
}
