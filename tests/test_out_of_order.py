import pytest

pytestmark = pytest.mark.asyncio


async def test_completed_then_stale_answered_and_ringing(db, repos, campaign_id):
    """The assignment's evil sequence: COMPLETED, ANSWERED, RINGING. Final
    state is COMPLETED; the two late events are recorded with anomaly
    OUT_OF_ORDER, never silently dropped, and zero illegal transitions occur."""
    from test_event_dedup import _ingest_and_apply, _make_initiated_call

    call_id, _agent_id, _borrower_id = await _make_initiated_call(db, repos, campaign_id)

    await _ingest_and_apply(db, repos, "ev-1", "PCID-1", "COMPLETED")
    await _ingest_and_apply(db, repos, "ev-2", "PCID-1", "ANSWERED")
    await _ingest_and_apply(db, repos, "ev-3", "PCID-1", "RINGING")

    async with db.tx() as con:
        call = await con.fetchrow("SELECT * FROM calls WHERE id=$1", call_id)
        anomalies = await con.fetch(
            "SELECT event_type, anomaly FROM provider_events WHERE call_id=$1 ORDER BY id",
            call_id)

    assert call["state"] == "COMPLETED"
    assert call["state_rank"] == 6
    by_type = {r["event_type"]: r["anomaly"] for r in anomalies}
    assert by_type["COMPLETED"] is None
    assert by_type["ANSWERED"] == "OUT_OF_ORDER"
    assert by_type["RINGING"] == "OUT_OF_ORDER"
