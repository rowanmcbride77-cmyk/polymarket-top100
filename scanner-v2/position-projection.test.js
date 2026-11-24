import test from "node:test";
import assert from "node:assert/strict";
import { projectSettlementPosition } from "./position-projection.js";

test("projects only the fields required for verified settlement classification", () => {
  const row = {
    conditionId: "0xcondition",
    tokenId: "123",
    outcomeIndex: 1,
    outcome: "No",
    lastEventAt: 1_800_000_000,
    title: "large display field",
    icon: "large metadata field",
    nested: { unused: true }
  };
  assert.deepEqual(projectSettlementPosition(row, "CLOSED"), {
    condition_id: "0xcondition",
    token_id: "123",
    outcome_index: 1,
    outcome: "No",
    last_event_at: 1_800_000_000,
    _cohort: "CLOSED"
  });
});

test("preserves snake-case settlement fields and timestamp fallbacks", () => {
  const row = {
    condition_id: "0xabc",
    token_id: "456",
    outcome_index: 0,
    outcome: "Yes",
    timestamp: "2026-11-24T12:00:00Z"
  };
  assert.deepEqual(projectSettlementPosition(row, "REDEEMABLE"), {
    condition_id: "0xabc",
    token_id: "456",
    outcome_index: 0,
    outcome: "Yes",
    last_event_at: "2026-11-24T12:00:00Z",
    _cohort: "REDEEMABLE"
  });
});
