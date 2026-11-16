import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import App from "../App";

const overview = {
  total_transactions: 17, total_volume: 1200, fraud_transactions: 2, fraud_loss: 300,
  flagged_by_model: 3, fraud_rate_pct: 11.7647, open_alerts: 1,
  risk_distribution: [{ risk_level: "HIGH", n: 4 }],
};
const transaction = (event_id, type = "CASH_OUT") => ({
  event_id, event_time: "2026-01-01T10:00:00", type, amount: 125.5,
  name_orig: "C123", name_dest: "C456", is_fraud: null, fraud_probability: 0.8,
  anomaly_score: 0.6, risk_score: 87, risk_level: "HIGH", model_version: "model-v1",
});
const transactions = { total: 17, limit: 25, offset: 0, items: Array.from({ length: 17 }, (_, i) => transaction(`evt-${i}`)) };
const alert = { alert_id: 1, event_id: "evt-0", status: "OPEN", risk_score: 87, risk_level: "HIGH", created_at: "2026-01-01T10:00:00", ...transaction("evt-0") };
const metrics = {
  active_model: "model-v1", threshold: 0.5, test: { precision: 0.8, recall: 0.7, f1: 0.75, pr_auc: 0.82 },
  confusion: { tn: 90, fp: 10, fn: 3, tp: 7 },
  baseline_logreg: { pr_auc: 0.61 }, history: [],
};

function response(data, status = 200) {
  return { ok: status >= 200 && status < 300, status, statusText: "Request failed", json: async () => data };
}

function makeFetch() {
  return vi.fn(async (input, options = {}) => {
    const url = new URL(typeof input === "string" ? input : input.url, "http://localhost");
    const path = url.pathname.replace(/^\/api/, "");
    if (path === "/health") return response({ status: "ok", database: true, model: "model-v1" });
    if (path === "/analytics/overview") return response(overview);
    if (path === "/analytics/timeseries") return response({ points: [{ day: "2026-01-01", txn_count: 17, fraud_count: 2 }] });
    if (path === "/analytics/by-type") return response({ items: [{ type: "CASH_OUT", txn_count: 17, txn_volume: 1200, fraud_count: 2, fraud_loss: 300, fraud_rate_pct: 11.8 }] });
    if (path === "/analytics/top-risk-accounts") return response({ items: [{ customer: "C123", txn_count: 4, fraud_count: 2, total_amount: 300 }] });
    if (path === "/transactions" && options.method === "GET") {
      const type = url.searchParams.get("type");
      const q = url.searchParams.get("q");
      const items = q === "ZZZ_NO_SUCH_ACCOUNT" ? [] : transactions.items.filter((t) => !type || t.type === type);
      return response({ ...transactions, total: items.length, items });
    }
    if (path.startsWith("/transactions/")) return response(transaction(path.split("/").at(-1)));
    if (path === "/alerts") return response({ total: 1, items: [alert] });
    if (path === "/alerts/1" && options.method === "PATCH") return response({ alert_id: 1, event_id: "evt-0", status: JSON.parse(options.body).status });
    if (path === "/model/metrics") return response(metrics);
    if (path === "/monitoring/drift" && options.method === "GET") return response({ features: [{ feature: "amount_to_balance", psi: 0.01, status: "OK", n_current: 17, created_at: "2026-01-01T10:00:00" }] });
    if (path === "/monitoring/drift/run") return response({ window: 5000, features: [] });
    if (path === "/pipeline/runs") return response({ summary: [{ pipeline: "batch_etl", runs: 2, failures: 0, last_run: "2026-01-01T10:00:00" }], runs: [{ run_id: 1, pipeline: "batch_etl", status: "SUCCESS", rows_in: 17, rows_out: 17, started_at: "2026-01-01T10:00:00" }] });
    return response({ detail: "Not found" }, 404);
  });
}

const open = (path) => render(<MemoryRouter initialEntries={[path]}><App /></MemoryRouter>);

beforeEach(() => { globalThis.fetch = makeFetch(); });

describe("Fraud console API contract integration", () => {
  it("shows API health and live-shape overview metrics", async () => {
    open("/");
    expect(await screen.findByText("API and model healthy")).toBeInTheDocument();
    const label = await screen.findByText("Transactions", { selector: ".kpi-label" });
    expect(within(label.parentElement).getByText("17")).toBeInTheDocument();
    expect(await screen.findByText("By transaction type")).toBeInTheDocument();
  });

  it("filters, paginates and opens transaction details", async () => {
    open("/transactions");
    expect((await screen.findAllByRole("row")).length).toBe(18);
    fireEvent.change(screen.getByLabelText("Type"), { target: { value: "TRANSFER" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply filters" }));
    expect(await screen.findByText(/No transactions match these filters/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Type"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply filters" }));
    fireEvent.click((await screen.findAllByRole("row"))[1]);
    const drawer = await screen.findByLabelText("Transaction detail");
    expect(await within(drawer).findByText("fraud probability")).toBeInTheDocument();
  });

  it("lists alerts, changes status and opens alert detail", async () => {
    open("/alerts");
    fireEvent.click(await screen.findByRole("button", { name: "Investigate" }));
    await waitFor(() => expect(globalThis.fetch).toHaveBeenCalledWith(expect.stringContaining("/alerts/1"), expect.objectContaining({ method: "PATCH" })));
    fireEvent.click(screen.getByRole("button", { name: /2026-01-01/ }));
    expect(await screen.findByLabelText("Alert detail")).toBeInTheDocument();
  });

  it("renders evaluation metrics and drift data", async () => {
    open("/model");
    expect(await screen.findByText("Precision", { selector: ".kpi-label" })).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "Confusion matrix" })).toHaveTextContent("caught fraud");
    expect(await screen.findByText("amount_to_balance", { selector: "td" })).toBeInTheDocument();
  });

  it("renders analytics and recorded pipeline runs with honest integration status", async () => {
    const analyticsPage = open("/analytics");
    expect(await screen.findByText("Accounts with the most confirmed fraud")).toBeInTheDocument();
    expect(await screen.findByText("C123", { selector: "td" })).toBeInTheDocument();
    analyticsPage.unmount();
    open("/pipelines");
    expect(await screen.findByText(/does not expose Airflow scheduler health or a live Kafka broker status/)).toBeInTheDocument();
    expect((await screen.findAllByText("batch_etl", { selector: "td" })).length).toBeGreaterThanOrEqual(2);
  });

  it("shows an error and retries a failed API request", async () => {
    const normalFetch = makeFetch();
    const failed = vi.fn(async (input, options = {}) => {
      const path = new URL(typeof input === "string" ? input : input.url, "http://localhost").pathname;
      if (path === "/api/analytics/overview" && !failed.retryDone) {
        failed.retryDone = true;
        return response({ detail: "Database unavailable" }, 503);
      }
      return normalFetch(input, options);
    });
    globalThis.fetch = failed;
    open("/");
    expect((await screen.findAllByText(/503: Database unavailable/)).length).toBeGreaterThan(0);
    fireEvent.click(screen.getAllByRole("button", { name: "Retry" })[0]);
    expect(await screen.findByText("API and model healthy")).toBeInTheDocument();
  });
});
