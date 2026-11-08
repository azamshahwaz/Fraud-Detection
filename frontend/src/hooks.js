import { useCallback, useEffect, useState } from "react";
import { api } from "./api";

/** Fetch `path` (string or null to skip). Returns {data, error, loading, reload}. */
export function useApi(path, { refreshMs } = {}) {
  const [state, setState] = useState({ data: null, error: null, loading: !!path });
  const load = useCallback(() => {
    if (!path) return undefined;
    const ctl = new AbortController();
    setState((s) => ({ ...s, loading: true }));
    api(path, { signal: ctl.signal })
      .then((data) => setState({ data, error: null, loading: false }))
      .catch((error) => error.name !== "AbortError" && setState({ data: null, error, loading: false }));
    return () => ctl.abort();
  }, [path]);
  useEffect(() => {
    const cancel = load();
    const t = refreshMs ? setInterval(load, refreshMs) : null;
    return () => { cancel && cancel(); t && clearInterval(t); };
  }, [load, refreshMs]);
  return { ...state, reload: load };
}

export const fmtNum = (n) => (n == null ? "–" : Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 }));
export const fmtMoney = (n) =>
  n == null ? "–" : Number(n) >= 1e6 ? `${(Number(n) / 1e6).toFixed(2)}M` : Number(n).toLocaleString("en-US", { maximumFractionDigits: 2 });
export const fmtPct = (n, d = 1) => (n == null ? "–" : `${(Number(n) * 100).toFixed(d)}%`);
export const fmtTime = (s) => (s ? String(s).replace("T", " ").slice(0, 16) : "–");
