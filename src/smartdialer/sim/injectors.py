import asyncio
from dataclasses import dataclass


@dataclass
class ProviderOutage:
    """All calls through `provider` start failing/timing out for `dur`
    seconds, starting at virtual time `at`. See ARCHITECTURE.md §12.2."""
    at: float
    dur: float
    provider: str = "mock_b"

    def schedule(self, clock, db, repos, registry, campaign_id: int) -> None:
        async def _run():
            await clock.sleep(self.at)
            registry.get(self.provider).trigger_outage(self.dur)
        asyncio.ensure_future(_run())


@dataclass
class AgentMassLogout:
    """`count` currently-available agents log out at once, at virtual time
    `at`. See ARCHITECTURE.md §12.3. Deliberately scoped to AVAILABLE agents
    only — logging out an agent mid-call is a different failure mode
    (a dropped agent, §12 table) that this injector does not model."""
    at: float
    count: int

    def schedule(self, clock, db, repos, registry, campaign_id: int) -> None:
        async def _run():
            await clock.sleep(self.at)
            async with db.tx() as con:
                rows = await con.fetch(
                    "SELECT id FROM agents WHERE campaign_id=$1 AND state='AVAILABLE' "
                    "LIMIT $2", campaign_id, self.count)
                for r in rows:
                    await repos.agents.logout(con, r["id"])
        asyncio.ensure_future(_run())
