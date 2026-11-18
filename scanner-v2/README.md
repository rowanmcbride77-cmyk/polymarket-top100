# Clean scanner engine (v2)

This folder is the replacement engine being built alongside the existing dashboard.
The legacy `update.mjs` is not the source of truth for settlement accuracy.

## Non-negotiable rules

- Rank candidates by the selected Polymarket leaderboard period P&L.
- Discover broadly; do not cap qualification to the canonical Top 100.
- A settlement win/loss must come from explicit resolved-token metadata and a final payout of exactly 1 or 0.
- A `CLOSED` position is an exit and is never counted as a settlement result, regardless of realized P&L.
- A position without matching token + condition resolution metadata is unverified, not a loss or win.
- Count one unambiguous result per market condition.
- Qualification: at least 90% verified settlement accuracy and at least 20 resolved markets in the rolling 30-day window.
- No partial/incomplete record may qualify.
- Consensus must be computed only from currently open positions held on the exact same condition and outcome by at least two qualified wallets.

## Build order

1. Settlement primitives and tests (this commit).
2. Verify the live Data API resolution and token endpoints against real responses.
3. Implement wallet discovery and cursor-safe pagination.
4. Implement per-wallet settlement history with bounded concurrency, caching, and completeness checks.
5. Implement ranking, open-position overlap, and consensus.
6. Add output schema validation and only then switch the dashboard data feed.

The existing dashboard remains in place until the replacement engine produces validated output.
