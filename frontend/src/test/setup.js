import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

afterEach(() => cleanup());
globalThis.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
// Integration tests hit the REAL FastAPI backend (TEST_API_URL, default http://localhost:8000); strip the /api prefix like the dev proxy.
const API = process.env.TEST_API_URL || "http://localhost:8000";
const realFetch = globalThis.fetch;
globalThis.fetch = (url, opts) => realFetch(typeof url === "string" && url.startsWith("/api") ? API + url.slice(4) : url, opts);
