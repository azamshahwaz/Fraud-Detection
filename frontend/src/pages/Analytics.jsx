import { useState } from "react";
import { Area, AreaChart, Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmtMoney, fmtNum, useApi } from "../hooks";
import { Panel, State } from "../components/ui";

export default function Analytics() {
  const [grain, setGrain] = useState("day");
  const ts = useApi(`/analytics/timeseries?grain=${grain}&limit=${grain === "day" ? 120 : 168}`);
  const ty = useApi("/analytics/by-type");
  const top = useApi("/analytics/top-risk-accounts?limit=10");
  const xKey = grain === "day" ? "day" : "hour";
  return (
    <>
      <h1>Historical fraud analytics</h1>
      <Panel title="Fraud loss over time" actions={
        <div className="seg">{["day", "hour"].map((g) => <button key={g} className={g === grain ? "on" : ""} onClick={() => setGrain(g)}>{g}</button>)}</div>}>
        <State loading={ts.loading} error={ts.error}>
          <div className="chart">
            <ResponsiveContainer width="100%" height={280}>
              <AreaChart data={ts.data?.points || []}>
                <CartesianGrid stroke="#e3e7ec" vertical={false} />
                <XAxis dataKey={xKey} tick={{ fontSize: 11 }} tickFormatter={(v) => String(v).slice(5, 16).replace("T", " ")} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip />
                <Area dataKey="fraud_count" name="Fraud transactions" stroke="#c0392b" fill="#f1c9c4" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </State>
      </Panel>
      <div className="grid2">
        <Panel title="Fraud rate by transaction type">
          <State loading={ty.loading} error={ty.error}>
            <div className="chart">
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={ty.data?.items || []}>
                  <CartesianGrid stroke="#e3e7ec" vertical={false} />
                  <XAxis dataKey="type" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} unit="%" />
                  <Tooltip />
                  <Bar dataKey="fraud_rate_pct" name="Fraud rate %" fill="#b7791f" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </State>
        </Panel>
        <Panel title="Accounts with the most confirmed fraud">
          <State loading={top.loading} error={top.error}>
            <table>
              <thead><tr><th>Account</th><th className="num">Transactions</th><th className="num">Fraud</th><th className="num">Total amount</th></tr></thead>
              <tbody>{(top.data?.items || []).map((r) => (
                <tr key={r.customer}><td className="mono">{r.customer}</td><td className="num">{fmtNum(r.txn_count)}</td><td className="num">{fmtNum(r.fraud_count)}</td><td className="num">{fmtMoney(r.total_amount)}</td></tr>))}</tbody>
            </table>
          </State>
        </Panel>
      </div>
    </>
  );
}
