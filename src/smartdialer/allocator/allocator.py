import asyncio
import logging

import asyncpg

from ..domain.enums import AgentState, CallState
from ..providers.base import CallRequest, ProviderError
from .leases import dial_until, reserve_until

logger = logging.getLogger(__name__)

MAX_DEADLOCK_RETRIES = 3


class CallAllocator:
    """The ONLY module that can place a call. Constructed once per worker and
    handed to exactly one object: the SafetyController — see safety/controller.py
    and ARCHITECTURE.md §8 for why that wiring is the non-bypassability proof."""

    def __init__(self, db, repos, registry, clock, cfg, worker_id: str):
        self.db = db
        self.repos = repos
        self.registry = registry
        self.clock = clock
        self.cfg = cfg
        self.worker_id = worker_id

    async def allocate_batch(self, campaign_id: int, n: int) -> int:
        """Places up to `n` calls concurrently, not sequentially: each
        `_allocate_one` makes a real provider round trip, and serialising N
        of those behind each other would make one tick's allocation work take
        up to N x provider-setup-latency instead of roughly one round trip.
        Concurrency is safe here for the same reason it's safe across
        workers — every step is CAS/SKIP LOCKED-guarded, so two allocations
        racing each other just means one of them legitimately loses and moves
        on (see repo/agents.py, repo/borrowers.py)."""
        results = await asyncio.gather(
            *[self._allocate_one(campaign_id) for _ in range(n)])
        return sum(1 for ok in results if ok)

    async def _reserve_and_create(self, campaign_id: int, lease: float):
        """Step 1: reserve agent (CAS) + claim borrower (SKIP LOCKED) + create
        the call row. Returns None if this attempt legitimately found nothing
        to do (no candidates, lost the CAS race, no borrower, no healthy
        provider) — that is a normal outcome, not an error, and the caller
        just moves on. Concurrent reservations across many rows can trigger a
        genuine Postgres deadlock under FK-check contention even though each
        transaction only ever touches its own agent/borrower rows; that is
        retried a bounded number of times here rather than surfaced, exactly
        like any other lost race."""
        for attempt in range(MAX_DEADLOCK_RETRIES):
            try:
                async with self.db.tx() as con:
                    cands = await self.repos.agents.pick_candidates(con, campaign_id, 1)
                    if not cands:
                        return None
                    a = cands[0]
                    if not await self.repos.agents.reserve(con, a["id"], a["version"],
                                                             self.worker_id, lease):
                        return None                             # lost the race
                    b = await self.repos.borrowers.claim_one(con, campaign_id, self.worker_id, lease)
                    if b is None:
                        await self.repos.agents.release(con, a["id"])
                        return None
                    provider = self.registry.pick()
                    if provider is None:
                        await self.repos.agents.release(con, a["id"])
                        await self.repos.borrowers.release(con, b["id"])
                        return None
                    attempt_no = b["attempt_count"] + 1
                    idem = f"{campaign_id}:{b['id']}:{attempt_no}"
                    call_id = await self.repos.calls.create(
                        con, campaign_id, a["id"], b["id"], provider.name, attempt_no, idem,
                        self.worker_id, lease)
                return a, b, provider, call_id, idem
            except asyncpg.exceptions.DeadlockDetectedError:
                if attempt == MAX_DEADLOCK_RETRIES - 1:
                    logger.warning("campaign %s: gave up after %d deadlock retries",
                                    campaign_id, MAX_DEADLOCK_RETRIES)
                    return None
        return None

    async def _allocate_one(self, campaign_id: int) -> bool:
        lease = reserve_until(self.clock, self.cfg)

        reserved = await self._reserve_and_create(campaign_id, lease)
        if reserved is None:
            return False
        a, b, provider, call_id, idem = reserved

        # 2. provider call OUTSIDE the transaction — never hold a row lock across IO.
        # The row above is created BEFORE this call, not after: a crash here
        # leaves a RESERVED call with an expiring lease that the reaper cleans
        # up, instead of a live provider call with no database record.
        try:
            pcid = await provider.place_call(
                CallRequest(call_id, b["phone"], self.cfg.caller_id, idem))
            self.registry.record(provider.name, ok=True)
        except ProviderError:
            self.registry.record(provider.name, ok=False)
            async with self.db.tx() as con:
                await self.repos.calls.fail(con, call_id, reason="PROVIDER_REJECTED")
                await self.repos.agents.release(con, a["id"])
                await self.repos.borrowers.reschedule(con, b["id"], backoff_s=60)
            return False

        # 3. record INITIATED + agent DIALING. Retried on deadlock for the
        # same reason step 1 is: concurrent allocations across many
        # unrelated rows can still deadlock under Postgres' FK-check
        # locking. This one matters more to get right than step 1's,
        # though — the provider call has ALREADY succeeded by this point, so
        # simply giving up and returning False would leave a live call the
        # database doesn't know about. If every retry still fails, the
        # call's own RESERVED-with-a-lease row is still there, so the
        # reaper's orphan sweep bounds the damage to one lease timeout
        # instead of leaving it stuck forever.
        dial_lease = dial_until(self.clock, self.cfg)
        for attempt in range(MAX_DEADLOCK_RETRIES):
            try:
                async with self.db.tx() as con:
                    await self.repos.calls.advance(con, call_id, CallState.INITIATED,
                                                    provider_call_id=pcid, lease_until=dial_lease)
                    await self.repos.agents.force_state(con, a["id"], AgentState.DIALING,
                                                         worker=self.worker_id,
                                                         lease_until=dial_lease, call_id=call_id)
                return True
            except asyncpg.exceptions.DeadlockDetectedError:
                if attempt == MAX_DEADLOCK_RETRIES - 1:
                    logger.warning(
                        "campaign %s: gave up recording INITIATED for call %s after %d "
                        "deadlock retries; the reaper's lease sweep will reconcile it",
                        campaign_id, call_id, MAX_DEADLOCK_RETRIES)
                    return False
        return False

    async def cancel_excess_ringing(self, campaign_id: int, allowed_ringing: int) -> int:
        """Cancel the oldest not-yet-answered calls beyond `allowed_ringing`.
        Cancelling before answer has zero compliance cost (ARCHITECTURE.md
        §12.3) — this is the free option that lets predictive pacing pull back
        the instant capacity drops instead of waiting for calls to fail.

        The candidate list is selected here but cancelled in later,
        per-candidate transactions — a candidate can legitimately be answered
        in the gap between the two. `cancel_if_not_answered` re-checks the
        state at cancel time so that race can only ever result in a no-op,
        never in hanging up on someone who just picked up."""
        async with self.db.tx() as con:
            current = await con.fetchval(
                "SELECT count(*) FROM calls WHERE campaign_id=$1 AND state IN "
                "('INITIATED','RINGING')", campaign_id)
            excess = max(0, current - allowed_ringing)
            if excess == 0:
                return 0
            rows = await self.repos.calls.list_ringing_oldest(con, campaign_id, excess)

        cancelled = 0
        for r in rows:
            for attempt in range(MAX_DEADLOCK_RETRIES):
                try:
                    async with self.db.tx() as con:
                        row = await self.repos.calls.cancel_if_not_answered(
                            con, r["id"], reason="SAFETY_PULLBACK")
                        if row is None:
                            break                     # answered (or already terminal) in the meantime
                        if row["agent_id"]:
                            await self.repos.agents.release(con, row["agent_id"])
                        await self.repos.borrowers.on_call_terminal(
                            con, row["borrower_id"], "CANCELLED", backoff_s=30)
                        cancelled += 1
                    break
                except asyncpg.exceptions.DeadlockDetectedError:
                    # a concurrent background provider task can still be
                    # writing against an unrelated call at the same moment;
                    # skip this candidate and let the next pullback cycle
                    # retry it rather than block the whole sweep on it
                    if attempt == MAX_DEADLOCK_RETRIES - 1:
                        logger.warning(
                            "campaign %s: gave up cancelling call %s after %d deadlock retries",
                            campaign_id, r["id"], MAX_DEADLOCK_RETRIES)
        return cancelled
