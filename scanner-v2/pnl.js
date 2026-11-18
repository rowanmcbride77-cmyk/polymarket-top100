import { requestJson } from "./data-api.js";

const numberOrNull = value => {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

/**
 * Computes selected-window realized P&L from Polymarket's cumulative PnL
 * series. Never mixes category-specific leaderboard P&L or position P&L.
 */
export async function getSelectedPeriodPnl(wallet, period, options = {}) {
  const interval = String(period).toUpperCase() === "MONTH" ? "1m" : "1w";
  const request = options.request ?? requestJson;
  const payload = await request("/v2/user-pnl", {
    user: wallet,
    interval,
    fidelity: "1d"
  }, options);
  const series = payload.data;
  const points = Array.isArray(series?.points) ? series.points : [];
  if (points.length < 2) return { value: null, complete: false, reason: "insufficient-pnl-points" };

  const first = numberOrNull(points[0].realized_pnl);
  const last = numberOrNull(points[points.length - 1].realized_pnl);
  if (first === null || last === null) {
    return { value: null, complete: false, reason: "missing-realized-pnl" };
  }
  const firstTimestamp = Number(points[0].timestamp);
  const lastTimestamp = Number(points[points.length - 1].timestamp);
  if (!Number.isFinite(firstTimestamp) || !Number.isFinite(lastTimestamp) || lastTimestamp <= firstTimestamp) {
    return { value: null, complete: false, reason: "invalid-pnl-timestamps" };
  }
  return {
    value: last - first,
    complete: true,
    interval,
    fidelity: series.fidelity ?? "unknown",
    firstTimestamp,
    lastTimestamp,
    points: points.length,
    basis: "Difference between first and latest cumulative realized_pnl points in the requested Data API v2 interval."
  };
}
