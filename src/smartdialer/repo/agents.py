from ..domain.enums import AgentState

RESERVE_CAS = """
UPDATE agents
   SET state = 'RESERVED', version = version + 1,
       lease_owner = $2, lease_expires_at = to_timestamp($3), updated_at = now()
 WHERE id = $1 AND state = 'AVAILABLE' AND version = $4
RETURNING id, version
"""

PICK_CANDIDATES = """
SELECT id, version FROM agents
 WHERE campaign_id = $1 AND state = 'AVAILABLE'
 ORDER BY updated_at ASC
 LIMIT $2
 FOR UPDATE SKIP LOCKED
"""

# Unconditional-by-id update. Safe because every call site already holds
# exclusivity through some other guard: either it just won the RESERVE_CAS in
# the same transaction, or it is the event applier acting on a row it already
# advanced under the rank guard (domain/transitions.py::can_apply). Nothing
# outside those two paths is allowed to move an agent.
FORCE_STATE = """
UPDATE agents
   SET state = $2::agent_state, version = version + 1, updated_at = now(),
       state_changed_at = now(),
       lease_owner       = CASE WHEN $2::text IN ('RESERVED','DIALING') THEN $3 ELSE NULL END,
       lease_expires_at   = CASE WHEN $2::text IN ('RESERVED','DIALING','WRAP_UP')
                                 THEN to_timestamp($4) ELSE NULL END,
       current_call_id = $5
 WHERE id = $1
RETURNING id, version
"""

GET = "SELECT * FROM agents WHERE id = $1"

COUNT_AVAILABLE_BY_CAMPAIGN = """
SELECT count(*) FROM agents WHERE campaign_id = $1 AND state = 'AVAILABLE'
"""


class AgentRepo:
    async def pick_candidates(self, con, campaign_id: int, n: int):
        return await con.fetch(PICK_CANDIDATES, campaign_id, n)

    async def reserve(self, con, agent_id: int, version: int,
                       worker: str, lease_until: float) -> bool:
        row = await con.fetchrow(RESERVE_CAS, agent_id, worker, lease_until, version)
        return row is not None          # None == lost the race, caller moves on

    async def force_state(self, con, agent_id: int, state: AgentState,
                           worker: str | None = None, lease_until: float | None = None,
                           call_id: int | None = None) -> bool:
        row = await con.fetchrow(FORCE_STATE, agent_id, str(state), worker,
                                  lease_until, call_id)
        return row is not None

    async def release(self, con, agent_id: int) -> bool:
        return await self.force_state(con, agent_id, AgentState.AVAILABLE)

    async def login(self, con, agent_id: int) -> bool:
        return await self.force_state(con, agent_id, AgentState.AVAILABLE)

    async def logout(self, con, agent_id: int) -> bool:
        return await self.force_state(con, agent_id, AgentState.OFFLINE)

    async def pause(self, con, agent_id: int) -> bool:
        return await self.force_state(con, agent_id, AgentState.PAUSED)

    async def get(self, con, agent_id: int):
        return await con.fetchrow(GET, agent_id)

    async def create(self, con, campaign_id: int, ext_ref: str):
        return await con.fetchval(
            "INSERT INTO agents (campaign_id, ext_ref) VALUES ($1,$2) RETURNING id",
            campaign_id, ext_ref)
