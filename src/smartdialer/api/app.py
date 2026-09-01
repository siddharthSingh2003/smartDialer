from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..config import Settings
from ..db import Db
from ..repo.events import EventRepo
from .webhooks import router


def create_app(cfg: Settings | None = None) -> FastAPI:
    cfg = cfg or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.db = await Db.connect(cfg.database_url)
        app.state.event_repo = EventRepo()
        yield
        await app.state.db.close()

    app = FastAPI(title="SmartDialer webhook ingest", lifespan=lifespan)
    app.include_router(router)

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app


app = create_app()
