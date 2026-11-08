import { Bar, BarChart, CartesianGrid, ComposedChart, Legend, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useApi, fmtMoney, fmtNum } from "../hooks";
import { Kpi, Panel, State } from "../components/ui";

const LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];

export default function Overview() {
  const ov = useApi("/analytics/overview", { refreshMs: 15000 });
  const ts = useApi("/analytics/timeseries?grain=day");
  const ty = useApi("/analytics/by-type");
  return (
    <>
      <h1>Overview</h1>
      <State loading={ov.loading} error={ov.error}>
        {ov.data && (
          <div className="kpis">
            <Kpi label="Transactions" value={fmtNum(ov.data.total_transactions)} />
            <Kpi label="Volume" value={fmtMoney(ov.data.total_volume)} />
            <Kpi label="Fraud loss" value={fmtMoney(ov.data.fraud_loss)} tone="danger" hint={`${ov.data.fraud_rate_pct}% of transactions`} />
            <Kpi label="Flagged by model" value={fmtNum(ov.data.flagged_by_model)} />
            <Kpi label="Open alerts" value={fmtNum(ov.data.open_alerts)} tone={ov.data.open_alerts ? "warn" : ""} />
          </div>
        )}
      </State>
      <div className="grid2">
        <Panel title="Daily transactions and confirmed fraud">
          <State loading={ts.loading} error={ts.error}>
            <div className="chart">
              <ResponsiveContainer width="100%" height={260}>
                <ComposedChart data={ts.data?.points || []}>
                  <CartesianGrid stroke="#e3e7ec" vertical={false} />
                  <XAxis dataKey="day" tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="l" tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="r" orientation="right" tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Legend />
                  <Bar yAxisId="l" dataKey="txn_count" name="Transactions" fill="#5b6b82" />
                  <Line yAxisId="r" dataKey="fraud_count" name="Fraud" stroke="#c0392b" strokeWidth={2} dot={false} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </State>
        </Panel>
        <Panel title="Risk level distribution">
          <State loading={ov.loading} error={ov.error}>
            <div className="chart">
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={LEVELS.map((l) => ({ level: l, n: ov.data?.risk_distribution?.find((r) => r.risk_level === l)?.n || 0 }))}>
                  <CartesianGrid stroke="#e3e7ec" vertical={false} />
                  <XAxis dataKey="level" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Bar dataKey="n" name="Transactions" fill="#0e7c7b" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </State>
        </Panel>
      </div>
      <Panel title="By transaction type">
        <State loading={ty.loading} error={ty.error}>
          <table>
            <thead><tr><th>Type</th><th className="num">Transactions</th><th className="num">Volume</th><th className="num">Fraud</th><th className="num">Fraud loss</th><th className="num">Fraud rate</th></tr></thead>
            <tbody>
              {(ty.data?.items || []).map((r) => (
                <tr key={r.type}>
                  <td>{r.type}</td><td className="num">{fmtNum(r.txn_count)}</td><td className="num">{fmtMoney(r.txn_volume)}</td>
                  <td className="num">{fmtNum(r.fraud_count)}</td><td className="num">{fmtMoney(r.fraud_loss)}</td><td className="num">{r.fraud_rate_pct}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </State>
      </Panel>
    </>
  );
}
