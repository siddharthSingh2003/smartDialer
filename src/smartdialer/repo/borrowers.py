CLAIM_BATCH = """
WITH cand AS (
    SELECT id FROM borrowers
     WHERE campaign_id = $1 AND state = 'PENDING' AND next_eligible_at <= now()
     ORDER BY priority ASC, next_eligible_at ASC
     LIMIT $2
     FOR UPDATE SKIP LOCKED
)
UPDATE borrowers b
   SET state = 'LOCKED', locked_by = $3, locked_until = to_timestamp($4)
  FROM cand
 WHERE b.id = cand.id
RETURNING b.id, b.phone, b.attempt_count, b.priority, b.max_attempts
"""

RELEASE = """
UPDATE borrowers SET state = 'PENDING', locked_by = NULL, locked_until = NULL
 WHERE id = $1
"""

RESCHEDULE = """
UPDATE borrowers
   SET attempt_count = attempt_count + 1,
       state = (CASE WHEN attempt_count + 1 >= max_attempts THEN 'EXHAUSTED' ELSE 'PENDING' END)::borrower_state,
       next_eligible_at = now() + make_interval(secs => $2),
       locked_by = NULL, locked_until = NULL
 WHERE id = $1
RETURNING id, state, attempt_count
"""

MARK_DONE = """
UPDATE borrowers
   SET attempt_count = attempt_count + 1, state = 'DONE',
       locked_by = NULL, locked_until = NULL
 WHERE id = $1
RETURNING id, state, attempt_count
"""

RECLAIM_EXPIRED_LOCKS = """
UPDATE borrowers
   SET state = 'PENDING', locked_by = NULL, locked_until = NULL
 WHERE state = 'LOCKED' AND locked_until < now()
RETURNING id
"""


class BorrowerRepo:
    async def claim_batch(self, con, campaign_id: int, n: int, worker: str, lease_until: float):
        return await con.fetch(CLAIM_BATCH, campaign_id, n, worker, lease_until)

    async def claim_one(self, con, campaign_id: int, worker: str, lease_until: float):
        rows = await self.claim_batch(con, campaign_id, 1, worker, lease_until)
        return rows[0] if rows else None

    async def release(self, con, borrower_id: int) -> None:
        await con.execute(RELEASE, borrower_id)

    async def reschedule(self, con, borrower_id: int, backoff_s: float):
        return await con.fetchrow(RESCHEDULE, borrower_id, backoff_s)

    async def mark_done(self, con, borrower_id: int):
        return await con.fetchrow(MARK_DONE, borrower_id)

    async def on_call_terminal(self, con, borrower_id: int, call_state: str, backoff_s: float = 60):
        if call_state == "COMPLETED":
            return await self.mark_done(con, borrower_id)
        return await self.reschedule(con, borrower_id, backoff_s)

    async def reclaim_expired_locks(self, con):
        return await con.fetch(RECLAIM_EXPIRED_LOCKS)

    async def seed_many(self, con, campaign_id: int, phones: list[str]):
        await con.executemany(
            "INSERT INTO borrowers (campaign_id, phone) VALUES ($1,$2) "
            "ON CONFLICT (campaign_id, phone) DO NOTHING",
            [(campaign_id, p) for p in phones])
