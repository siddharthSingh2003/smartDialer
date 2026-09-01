import logging
import time

from fastapi import APIRouter, Request

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/webhooks/{provider}")
async def receive_webhook(provider: str, request: Request):
    """Ingest a provider webhook. Always returns 200: providers retry on
    non-2xx, and a duplicate delivery is a normal, expected event here — it is
    absorbed by `ON CONFLICT (provider, provider_event_id) DO NOTHING`
    (repo/events.py), not by anything in this handler. See ARCHITECTURE.md
    §5.3 Guard 1."""
    try:
        payload = await request.json()
    except Exception:
        logger.warning("webhook from %s: unparsable body", provider)
        return {"ok": True, "note": "unparsable body, dropped"}

    event_id = str(payload.get("event_id") or payload.get("id") or "")
    if not event_id:
        return {"ok": True, "note": "missing event_id, dropped"}

    provider_call_id = (payload.get("provider_call_id") or payload.get("call_id")
                         or payload.get("CallUUID") or payload.get("RequestUUID"))
    event_type = payload.get("event_type") or payload.get("status") or payload.get("Status") or ""
    ts = payload.get("ts") or time.time()

    db = request.app.state.db
    events = request.app.state.event_repo
    async with db.tx() as con:
        await events.ingest(con, provider, event_id, provider_call_id, str(event_type), ts, payload)
    return {"ok": True}
