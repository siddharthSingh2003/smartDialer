from ..domain.enums import CallState
from ..domain.transitions import RANK

CREATE = """
INSERT INTO calls (campaign_id, agent_id, borrower_id, provider, state, state_rank,
                    attempt_no, idempotency_key, lease_owner, lease_expires_at)
VALUES ($1,$2,$3,$4,'RESERVED',1,$5,$6,$7,to_timestamp($8))
RETURNING id
"""

# THE ORDERING GUARD lives in `state_rank < $3`, enforced by the database, not
# by application code. Two workers advancing the same call concurrently cannot
# interleave into an inconsistent state: the row lock serialises them and the
# loser's predicate simply fails (0 rows returned).
ADVANCE = """
UPDATE calls
   SET state = $2::call_state, state_rank = $3,
       provider_call_id = COALESCE(provider_call_id, $4),
       initiated_at = COALESCE(initiated_at, CASE WHEN $2::text='INITIATED' THEN now() END),
       ringing_at   = COALESCE(ringing_at,   CASE WHEN $2::text='RINGING'   THEN now() END),
       answered_at  = COALESCE(answered_at,  CASE WHEN $2::text='ANSWERED'  THEN now() END),
       connected_at = COALESCE(connected_at, CASE WHEN $2::text='CONNECTED' THEN now() END),
       terminal_at  = CASE WHEN $3 >= 6 THEN now() ELSE terminal_at END,
       lease_expires_at = CASE WHEN $3 >= 6 THEN NULL ELSE to_timestamp($5) END,
       failure_reason = COALESCE($6, failure_reason)
 WHERE id = $1
   AND state_rank < $3
RETURNING id, agent_id, borrower_id, state, state_rank
"""

# Used by the safety pullback sweep (allocator/allocator.py::cancel_excess_ringing).
# It selects "currently ringing" candidates in one transaction and cancels
# them in later ones; a candidate can legitimately be answered in between.
# The generic rank guard alone (state_rank < CANCELLED's rank) would still
# let that through — CONNECTED's rank is lower than CANCELLED's — so this
# adds an explicit state check: only cancel while it is still true that
# nobody has picked up.
CANCEL_IF_NOT_ANSWERED = """
UPDATE calls
   SET state = 'CANCELLED'::call_state, state_rank = 6, terminal_at = now(),
       lease_expires_at = NULL, failure_reason = $2
 WHERE id = $1
   AND state IN ('INITIATED','RINGING')
RETURNING id, agent_id, borrower_id, state, state_rank
"""

GET = "SELECT * FROM calls WHERE id = $1"
GET_BY_PROVIDER_REF = "SELECT * FROM calls WHERE provider = $1 AND provider_call_id = $2"

LIST_INFLIGHT = """
SELECT id, agent_id, borrower_id, provider, provider_call_id, state, initiated_at
  FROM calls
 WHERE campaign_id = $1
   AND state IN ('RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED')
 ORDER BY initiated_at ASC NULLS LAST
"""


class CallRepo:
    async def create(self, con, campaign_id: int, agent_id: int, borrower_id: int,
                      provider: str, attempt_no: int, idem: str,
                      worker: str, lease_until: float) -> int:
        return await con.fetchval(CREATE, campaign_id, agent_id, borrower_id, provider,
                                   attempt_no, idem, worker, lease_until)

    async def advance(self, con, call_id: int, state: CallState,
                       provider_call_id: str | None = None,
                       lease_until: float | None = None,
                       failure_reason: str | None = None):
        return await con.fetchrow(ADVANCE, call_id, str(state), RANK[state],
                                   provider_call_id, lease_until, failure_reason)

    async def fail(self, con, call_id: int, reason: str):
        return await self.advance(con, call_id, CallState.FAILED, failure_reason=reason)

    async def cancel(self, con, call_id: int, reason: str = "SAFETY_PULLBACK"):
        return await self.advance(con, call_id, CallState.CANCELLED, failure_reason=reason)

    async def cancel_if_not_answered(self, con, call_id: int, reason: str = "SAFETY_PULLBACK"):
        """Like `cancel`, but only takes effect if the call is still
        INITIATED/RINGING at the moment of the update — see
        CANCEL_IF_NOT_ANSWERED. Returns None (a no-op, same as any other lost
        race) if the borrower answered in the meantime."""
        return await con.fetchrow(CANCEL_IF_NOT_ANSWERED, call_id, reason)

    async def get(self, con, call_id: int):
        return await con.fetchrow(GET, call_id)

    async def get_by_provider_ref(self, con, provider: str, provider_call_id: str):
        return await con.fetchrow(GET_BY_PROVIDER_REF, provider, provider_call_id)

    async def list_inflight(self, con, campaign_id: int):
        return await con.fetch(LIST_INFLIGHT, campaign_id)

    async def list_ringing_oldest(self, con, campaign_id: int, limit: int):
        return await con.fetch(
            "SELECT id FROM calls WHERE campaign_id=$1 AND state IN ('INITIATED','RINGING') "
            "ORDER BY initiated_at ASC LIMIT $2", campaign_id, limit)
