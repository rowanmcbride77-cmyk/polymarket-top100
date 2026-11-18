# Clean scanner engine (v2)

This is the replacement Polymarket scanner, developed alongside the existing dashboard. The legacy dashboard and scheduled scanner remain untouched until this engine produces validated output.

## Rules

- Discover wallets from sports, esports, crypto, and overall leaderboard streams; politics/government markets are excluded from consensus.
- Rank separately by selected WEEK or MONTH leaderboard P&L.
- Qualify only with at least 90% verified settlement accuracy and at least 20 resolved markets in the rolling 30-day window.
- Early exits are excluded from settlement wins, losses, denominator, and accuracy.
- Incomplete or unverified settlement history cannot qualify.
- Consensus requires verified current holdings and recent same-token BUY activity on the exact same condition/outcome.
- Consensus markets must have a verified end time within 24 hours. Groups with fewer than eight qualified holders are watchlist candidates, not strong signals.
- Replacement output is generated in isolation. It is not automatically published to the live dashboard.

## Running locally

Node.js 22 or newer:

```sh
npm test
DISCOVERY_LIMIT=20000 QUALIFICATION_AUDIT_LIMIT=1000 npm run scan
```

The scanner writes `week.json`, `month.json`, `consensus-week.json`, `consensus-month.json`, and `status.json` into the current working directory.

## Controlled GitHub run

Use **Actions → Scanner v2 controlled run → Run workflow**. The workflow runs unit tests first, executes the replacement scanner, and uploads its output as an artifact. It deliberately does not commit or publish generated data.

## Verification status

The unit-test workflow and isolated execution workflow are in place. The live Polymarket API adapters, settlement-history completeness, and output quality still require a successful controlled run and review before the dashboard can be switched over.
