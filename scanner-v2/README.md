# Clean scanner engine (v2)

This folder is the replacement engine being built alongside the existing dashboard.
The legacy `update.mjs` is not the source of truth for settlement accuracy.

## Non-negotiable rules

- Rank candidates by the selected Polymarket leaderboard period P&L.
- Discover broadly; do not cap qualification to the canonical Top 100.
- A settlement result must come from explicit token-resolution metadata and an exact binary final payout (0 or 1), joined by both token ID and condition ID.
- A `CLOSED` position is an exit and is never counted as a settlement result, regardless of realized P&L.
- A position without matching token + condition resolution metadata is unverified, not a loss or win.
- Count one unambiguous result per market condition.
- Qualification: at least 90% verified settlement accuracy and at least 20 resolved markets in the rolling 30-day window.
- No partial/incomplete history may qualify.
- Consensus is computed only from currently open positions held on the exact same condition and outcome by at least two qualified wallets.

## Verified API building blocks

- Data API v2 uses the `{ data, pagination }` envelope and opaque `next_cursor` pagination. No offset pagination is used.
- `/v2/tokens` provides each outcome token's `resolved` flag and `finalPrice`.
- `/v2/resolutions` provides condition-level finality and `resolvedAt`.
- `/v2/positions` distinguishes live holdings, redeemable settled positions, losing redeemable positions, and `CLOSED` exits.
- `/v2/activity` includes explicit `REDEEM` activity. Historical results must be built from verified settlement/redemption evidence, not from closed-position realized P&L.

## Build order

1. API client with cursor integrity and retry handling (implemented).
2. Settlement primitives and qualification tests (implemented; API history adapter still to build).
3. Verify real activity/token/resolution payload joins with fixtures.
4. Implement wallet discovery and cursor-safe pagination.
5. Implement complete per-wallet settlement history with bounded concurrency and caching.
6. Implement ranking, open-position overlap, and consensus.
7. Add output schema validation and only then switch the dashboard data feed.

The existing dashboard remains in place until the replacement engine produces validated output.
