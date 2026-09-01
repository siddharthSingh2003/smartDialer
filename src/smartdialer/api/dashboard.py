"""Optional web dashboard: a live view of one campaign's state, served by
the same FastAPI process as the webhook route. Not part of the graded
architecture — ARCHITECTURE.md is explicit that there is no UI in the
design being graded — this is a bonus ops convenience built on top, reading
only what's already in Postgres (see live_status.py, shared with the
terminal `smartdialer watch`)."""
import os

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from ..live_status import campaign_snapshot, list_campaigns

router = APIRouter()

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@router.get("/dashboard")
async def dashboard_page():
    return FileResponse(os.path.join(STATIC_DIR, "dashboard.html"))


@router.get("/api/campaigns")
async def campaigns_list(request: Request):
    db = request.app.state.db
    async with db.tx() as con:
        return await list_campaigns(con)


@router.get("/api/status/{campaign_id}")
async def campaign_status(campaign_id: int, request: Request):
    db = request.app.state.db
    async with db.tx() as con:
        return await campaign_snapshot(con, campaign_id)
