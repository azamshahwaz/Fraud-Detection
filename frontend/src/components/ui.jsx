export function State({ loading, error, children }) {
  if (error) return <div className="notice error" role="alert">Could not load data. {error.message}</div>;
  if (loading) return <div className="notice">Loading…</div>;
  return children;
}

export function Kpi({ label, value, hint, tone }) {
  return (
    <div className={`kpi ${tone || ""}`}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value}</div>
      {hint && <div className="kpi-hint">{hint}</div>}
    </div>
  );
}

export function Panel({ title, actions, children }) {
  return (
    <section className="panel">
      <header className="panel-head"><h2>{title}</h2>{actions}</header>
      {children}
    </section>
  );
}

/** The signature element: a score position on a four-zone strip (LOW / MEDIUM / HIGH / CRITICAL). */
export function RiskMeter({ score, level }) {
  if (score == null) return <span className="muted">not scored</span>;
  return (
    <div className="meter" title={`${level} · ${Number(score).toFixed(1)}`}>
      <div className="meter-strip"><i /><i /><i /><i /><b className="meter-pin" style={{ left: `${Math.min(100, Math.max(0, score))}%` }} /></div>
      <span className={`pill ${level}`}>{level}</span>
      <span className="meter-num">{Number(score).toFixed(1)}</span>
    </div>
  );
}

export function Pager({ total, limit, offset, onChange }) {
  const page = Math.floor(offset / limit) + 1;
  const pages = Math.max(1, Math.ceil(total / limit));
  return (
    <div className="pager">
      <button disabled={offset === 0} onClick={() => onChange(Math.max(0, offset - limit))}>Previous</button>
      <span>Page {page} of {pages} · {total.toLocaleString("en-US")} rows</span>
      <button disabled={offset + limit >= total} onClick={() => onChange(offset + limit)}>Next</button>
    </div>
  );
}
