import { fmtNum, fmtTime, useApi } from "../hooks";
import { Panel, State } from "../components/ui";

export default function Pipelines() {
  const { data, error, loading } = useApi("/pipeline/runs?limit=30", { refreshMs: 10000 });
  return (
    <>
      <h1>Pipeline monitoring</h1>
      <State loading={loading && !data} error={error}>
        {data && (
          <>
            <Panel title="Pipelines">
              <table>
                <thead><tr><th>Pipeline</th><th className="num">Runs</th><th className="num">Failures</th><th>Last run</th></tr></thead>
                <tbody>{data.summary.map((s) => (
                  <tr key={s.pipeline}><td>{s.pipeline}</td><td className="num">{s.runs}</td><td className="num">{s.failures > 0 ? <span className="pill CRITICAL">{s.failures}</span> : 0}</td><td>{fmtTime(s.last_run)}</td></tr>))}</tbody>
              </table>
            </Panel>
            <Panel title="Recent runs">
              {data.runs.length === 0 && <div className="notice">No pipeline runs recorded yet.</div>}
              <div className="scroll">
                <table>
                  <thead><tr><th>Started</th><th>Pipeline</th><th>Status</th><th className="num">Rows in</th><th className="num">Rows out</th><th>Error</th></tr></thead>
                  <tbody>{data.runs.map((r) => (
                    <tr key={r.run_id}><td>{fmtTime(r.started_at)}</td><td>{r.pipeline}</td>
                      <td><span className={`pill ${r.status === "SUCCESS" ? "LOW" : r.status === "RUNNING" ? "MEDIUM" : "CRITICAL"}`}>{r.status}</span></td>
                      <td className="num">{fmtNum(r.rows_in)}</td><td className="num">{fmtNum(r.rows_out)}</td><td className="muted">{r.error || ""}</td></tr>))}</tbody>
                </table>
              </div>
            </Panel>
          </>
        )}
      </State>
    </>
  );
}
