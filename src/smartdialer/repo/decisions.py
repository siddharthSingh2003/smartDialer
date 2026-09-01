import json

INSERT = """
INSERT INTO pacing_decisions (campaign_id, tick_at, sim_tick, mode, requested, approved,
                               reason_code, inputs)
VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8::jsonb)
"""


class DecisionRepo:
    """Owns its own transaction: the audit write is deliberately decoupled from
    the allocation transactions that follow it in the same tick."""

    def __init__(self, db):
        self.db = db

    async def record(self, campaign_id: int, ts: float, requested: int, approved: int,
                      reason: str, inputs: dict, mode: str = "PREDICTIVE",
                      sim_tick: int | None = None) -> None:
        async with self.db.tx() as con:
            await con.execute(INSERT, campaign_id, ts, sim_tick, mode, requested,
                               approved, str(reason), json.dumps(inputs, default=str))

    async def latest(self, con, campaign_id: int, limit: int = 1):
        return await con.fetch(
            "SELECT * FROM pacing_decisions WHERE campaign_id=$1 ORDER BY id DESC LIMIT $2",
            campaign_id, limit)
