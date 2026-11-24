# Polymarket scanner v3

This scanner builds the weekly and monthly Top 100 leaderboards from verified settlement history and produces same-side consensus signals.

## Qualification rules

- Discover candidate wallets from sports, esports, crypto, and overall leaderboard streams.
- Rank WEEK and MONTH independently by their corresponding Polymarket leaderboard P&L.
- Require at least **90% verified settlement accuracy** and **10 resolved markets** in the rolling 60-day window.
- Score the trader's selected side against the verified final market outcome, including positions sold before resolution when that market later resolves inside the 60-day window. This is directional market accuracy, not realized close P&L.
- Incomplete or unverified settlement histories cannot qualify.
- Consensus requires at least two qualified traders on the exact same outcome token, verified current holdings, recent same-token BUY activity, and at least $10 verified entry cost per trader.
- Consensus markets must have a verified end time within 24 hours. Politics and government markets are excluded.

## Controlled production rollout

The production workflow currently runs a **1,000-wallet proof of concept**. Discovery is category-balanced (50% sports, 15% esports, 20% crypto, 15% overall), and the audit is shared between WEEK and MONTH so the same wallet's settlement history is not fetched twice.

The workflow runs JavaScript syntax validation and unit tests before the scan, allows up to six hours for execution, and does not cancel an active scan when a newer run is queued. It validates the four data files and status timestamps before publishing. A zero-signal consensus array is valid only when its diagnostics report the same zero count; a missing or inconsistent consensus payload is rejected.

Increase `DISCOVERY_LIMIT` from 1,000 toward 20,000 only after the controlled scan has completed and the resulting leaderboard and consensus output have been reviewed.

## Output files

The scanner writes `week.json`, `month.json`, `consensus-week.json`, `consensus-month.json`, and `status.json` to the repository root. The live dashboard is updated only by a successful, validated publication.
