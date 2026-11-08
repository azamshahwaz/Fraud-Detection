import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import App from "../App";

const API = process.env.TEST_API_URL || "http://localhost:8000";
const get = (p) => fetch(`${API}${p}`).then((r) => r.json());
const open = (path) => render(<MemoryRouter initialEntries={[path]}><App /></MemoryRouter>);

describe("Fraud console against the live API", () => {
  it("shows API health in the sidebar", async () => {
    open("/");
    expect(await screen.findByText("API and model healthy", {}, { timeout: 15000 })).toBeInTheDocument();
  });

  it("Overview KPIs equal the database numbers", async () => {
    const ov = await get("/analytics/overview");
    open("/");
    const label = await screen.findByText("Transactions", { selector: ".kpi-label" }, { timeout: 15000 });
    expect(within(label.parentElement).getByText(ov.total_transactions.toLocaleString("en-US"))).toBeInTheDocument();
    expect(screen.getByText("Open alerts").parentElement).toHaveTextContent(ov.open_alerts.toLocaleString("en-US"));
    expect(await screen.findByText("By transaction type")).toBeInTheDocument();
    expect(await screen.findByText("CASH_OUT", { selector: "td" })).toBeInTheDocument();
  });

  it("Transactions: lists rows, filters by type, opens detail", async () => {
    open("/transactions");
    const rows = await screen.findAllByRole("row", {}, { timeout: 15000 });
    expect(rows.length).toBeGreaterThan(10);
    fireEvent.change(screen.getByLabelText("Type"), { target: { value: "TRANSFER" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply filters" }));
    await waitFor(() => {
      const body = screen.getAllByRole("row").slice(1);
      expect(body.length).toBeGreaterThan(0);
      body.forEach((r) => expect(r).toHaveTextContent("TRANSFER"));
    }, { timeout: 15000 });
    fireEvent.click(screen.getAllByRole("row")[1]);
    const drawer = await screen.findByLabelText("Transaction detail", {}, { timeout: 15000 });
    expect(await within(drawer).findByText("fraud probability")).toBeInTheDocument();
  });

  it("Transactions: empty result shows guidance", async () => {
    open("/transactions");
    await screen.findAllByRole("row", {}, { timeout: 15000 });
    fireEvent.change(screen.getByLabelText("Search"), { target: { value: "ZZZ_NO_SUCH_ACCOUNT" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply filters" }));
    expect(await screen.findByText(/No transactions match these filters/, {}, { timeout: 15000 })).toBeInTheDocument();
  });

  it("Alerts: lists open alerts and updates status through the API", async () => {
    const before = await get("/alerts?status=INVESTIGATING");
    open("/alerts");
    const btns = await screen.findAllByRole("button", { name: "Investigate" }, { timeout: 15000 });
    fireEvent.click(btns[0]);
    await waitFor(async () => {
      const after = await get("/alerts?status=INVESTIGATING");
      expect(after.total).toBe(before.total + 1);
    }, { timeout: 15000 });
  });

  it("Model: metrics, confusion matrix and drift run", async () => {
    open("/model");
    expect(await screen.findByText("Precision", { selector: ".kpi-label" }, { timeout: 15000 })).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "Confusion matrix" })).toHaveTextContent("caught fraud");
    fireEvent.click(screen.getByRole("button", { name: "Run drift check" }));
    expect(await screen.findByText("amount_to_balance", { selector: "td" }, { timeout: 30000 })).toBeInTheDocument();
  });

  it("Analytics and Pipelines pages render live data", async () => {
    const a = open("/analytics");
    expect(await screen.findByText("Accounts with the most confirmed fraud", {}, { timeout: 15000 })).toBeInTheDocument();
    expect(await screen.findAllByText(/^C\d+/, { selector: "td" }, { timeout: 15000 })).not.toHaveLength(0);
    a.unmount();
    open("/pipelines");
    expect((await screen.findAllByText("batch_etl", { selector: "td" }, { timeout: 15000 })).length).toBeGreaterThanOrEqual(2); // summary + recent runs
  });

  it("shows an error notice when the API fails", async () => {
    const orig = globalThis.fetch;
    globalThis.fetch = vi.fn(() => Promise.resolve({ ok: false, status: 503, statusText: "x", json: () => Promise.resolve({ detail: "Database unavailable" }) }));
    open("/");
    expect((await screen.findAllByRole("alert"))[0]).toHaveTextContent("503: Database unavailable");
    globalThis.fetch = orig;
  });
});
