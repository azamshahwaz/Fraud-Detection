import { NavLink, Route, Routes } from "react-router-dom";
import { useApi } from "./hooks";
import Alerts from "./pages/Alerts";
import Analytics from "./pages/Analytics";
import Model from "./pages/Model";
import Overview from "./pages/Overview";
import Pipelines from "./pages/Pipelines";
import Transactions from "./pages/Transactions";

const NAV = [["/", "Overview"], ["/transactions", "Transactions"], ["/alerts", "Alerts"], ["/analytics", "Analytics"], ["/model", "Model"], ["/pipelines", "Pipelines"]];

export default function App() {
  const health = useApi("/health", { refreshMs: 30000 });
  const ok = health.data?.status === "ok";
  return (
    <div className="shell">
      <nav className="side" aria-label="Main">
        <div className="brand">Fraud Console</div>
        {NAV.map(([to, label]) => <NavLink key={to} to={to} end={to === "/"}>{label}</NavLink>)}
        <div className={`health ${ok ? "ok" : "bad"}`} role="status">
          {health.error ? "API unreachable" : health.data ? (ok ? "API and model healthy" : "API degraded") : "Checking API…"}
        </div>
      </nav>
      <main>
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/transactions" element={<Transactions />} />
          <Route path="/alerts" element={<Alerts />} />
          <Route path="/analytics" element={<Analytics />} />
          <Route path="/model" element={<Model />} />
          <Route path="/pipelines" element={<Pipelines />} />
          <Route path="*" element={<div className="notice">Page not found.</div>} />
        </Routes>
      </main>
    </div>
  );
}
