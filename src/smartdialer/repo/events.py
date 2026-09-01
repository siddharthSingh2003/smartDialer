import json

INGEST = """
INSERT INTO provider_events
   (provider, provider_event_id, provider_call_id, event_type, provider_ts, payload)
VALUES ($1,$2,$3,$4,to_timestamp($5),$6)
ON CONFLICT (provider, provider_event_id) DO NOTHING
RETURNING id
"""

FETCH_UNAPPLIED = """
SELECT * FROM provider_events
 WHERE applied = false
 ORDER BY received_at
 LIMIT $1 FOR UPDATE SKIP LOCKED
"""

MARK = """
UPDATE provider_events SET applied = $2, anomaly = $3, call_id = COALESCE(call_id, $4)
 WHERE id = $1
"""

# A referenced call may never materialise if the worker crashed before the
# calls row was created (the reaper's orphan sweep covers that case from the
# call side). Give up on UNKNOWN_CALL rows after the same order of magnitude
# as a lease timeout so the unapplied backlog cannot grow unbounded.
GIVE_UP_UNKNOWN = """
UPDATE provider_events
   SET applied = true
 WHERE applied = false AND anomaly = 'UNKNOWN_CALL'
   AND received_at < now() - interval '60 seconds'
RETURNING id
"""


class EventRepo:
    async def ingest(self, con, provider: str, event_id: str, call_ref: str | None,
                      etype: str, ts: float, payload: dict) -> int | None:
        """Returns row id, or None if this exact event was already recorded."""
        return await con.fetchval(INGEST, provider, event_id, call_ref, etype, ts,
                                   json.dumps(payload, default=str))

    async def fetch_unapplied(self, con, limit: int = 200):
        return await con.fetch(FETCH_UNAPPLIED, limit)

    async def mark(self, con, event_id: int, applied: bool,
                    anomaly: str | None = None, call_id: int | None = None) -> None:
        await con.execute(MARK, event_id, applied, anomaly, call_id)

    async def give_up_unknown(self, con):
        return await con.fetch(GIVE_UP_UNKNOWN)
