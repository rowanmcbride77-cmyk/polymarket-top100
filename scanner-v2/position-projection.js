/**
 * Keep only fields consumed by settlement classification.
 * The Data API includes many large display/metadata fields that are not needed
 * for W/L calculation. Projecting each row before retaining it keeps the
 * multi-wallet audit bounded without changing the settlement inputs.
 */
export function projectSettlementPosition(row, cohort) {
  return {
    condition_id: row.condition_id ?? row.conditionId ?? "",
    token_id: row.token_id ?? row.tokenId ?? "",
    outcome_index: row.outcome_index ?? row.outcomeIndex ?? null,
    outcome: row.outcome ?? "",
    last_event_at: row.last_event_at ?? row.lastEventAt ??
      row.timestamp ?? row.updated_at ?? row.updatedAt ?? null,
    _cohort: String(cohort ?? "")
  };
}
