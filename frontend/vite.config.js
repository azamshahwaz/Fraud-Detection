import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server proxies /api/* to the FastAPI backend (strip the /api prefix).
const target = process.env.VITE_PROXY_TARGET || "http://localhost:8000";
export default defineConfig({
  plugins: [react()],
  server: { port: 5176, proxy: { "/api": { target, changeOrigin: true, rewrite: (p) => p.replace(/^\/api/, "") } } },
  test: { environment: "jsdom", globals: true, setupFiles: ["./src/test/setup.js"], testTimeout: 20000 },
});
