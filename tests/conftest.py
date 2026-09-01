import os
import time

import pytest
import pytest_asyncio

from smartdialer.clock import VirtualClock
from smartdialer.db import Db
from smartdialer.repo import Repos

DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", os.environ.get("DATABASE_URL",
                                         "postgresql://dialer:dialer@localhost:5432/dialer"))

TABLES = ["provider_events", "pacing_decisions", "calls", "borrowers", "agents",
          "campaign_counters", "campaigns"]


@pytest_asyncio.fixture
async def db():
    d = await Db.connect(DATABASE_URL, min_size=2, max_size=25)
    async with d.tx() as con:
        await con.execute("TRUNCATE " + ", ".join(TABLES) + " RESTART IDENTITY CASCADE")
    yield d
    await d.close()


@pytest.fixture
def repos(db):
    return Repos(db)


@pytest_asyncio.fixture
async def campaign_id(db):
    async with db.tx() as con:
        return await con.fetchval(
            "INSERT INTO campaigns (name) VALUES ('test') RETURNING id")


@pytest.fixture
def clock():
    # Seeded at the real wall-clock time: lease_expires_at is written to
    # Postgres via to_timestamp(clock.now() + lease_s), and the reaper compares
    # it against Postgres' own now(). A VirtualClock starting at 0.0 would make
    # every lease look already-expired against real wall-clock time.
    return VirtualClock(start=time.time())
