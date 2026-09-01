import math
import random

INSERT_CAMPAIGN = """
INSERT INTO campaigns (name, mode, max_overdial_ratio, max_abandon_rate)
VALUES ($1, $2, $3, $4) RETURNING id
"""


async def setup_campaign(db, repos, scenario, seed: int) -> int:
    """Seeds one campaign with `scenario.agents` logged-in agents and a
    borrower pool sized so the campaign never runs out mid-run."""
    async with db.tx() as con:
        campaign_id = await con.fetchval(
            INSERT_CAMPAIGN, f"sim-{scenario.name}-{seed}", scenario.mode, 1.5, 0.03)
        for i in range(scenario.agents):
            agent_id = await repos.agents.create(con, campaign_id, f"agent-{i}")
            await repos.agents.login(con, agent_id)

        rng = random.Random(seed)
        n_borrowers = max(scenario.agents * 80, 2000)
        phones = [f"+1555{rng.randint(1000000, 9999999)}-{i}" for i in range(n_borrowers)]
        await repos.borrowers.seed_many(con, campaign_id, phones)
    return campaign_id


def sample_talk_time(mean_s: float, rng: random.Random, sigma: float = 0.4) -> float:
    """Lognormal centred on the scenario's mean talk time, per §13.1."""
    mu = math.log(max(mean_s, 1.0)) - 0.5 * (sigma ** 2)
    return max(5.0, rng.lognormvariate(mu, sigma))
