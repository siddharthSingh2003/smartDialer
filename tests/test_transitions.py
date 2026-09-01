from smartdialer.domain.enums import CallState as C
from smartdialer.domain.transitions import can_apply


def test_rejects_stale_after_terminal():
    # the assignment's evil sequence: COMPLETED, ANSWERED, RINGING
    ok, anomaly = can_apply(C.COMPLETED, C.ANSWERED)
    assert ok is False
    assert anomaly == "OUT_OF_ORDER"

    ok, anomaly = can_apply(C.COMPLETED, C.RINGING)
    assert ok is False
    assert anomaly == "OUT_OF_ORDER"


def test_accepts_forward_legal_transition():
    ok, anomaly = can_apply(C.RINGING, C.ANSWERED)
    assert ok is True
    assert anomaly is None


def test_rejects_illegal_but_forward_transition():
    ok, anomaly = can_apply(C.QUEUED, C.CONNECTED)
    assert ok is False
    assert anomaly == "ILLEGAL_TRANSITION"


def test_terminal_absorbs_everything():
    for terminal in (C.COMPLETED, C.FAILED, C.CANCELLED, C.ABANDONED):
        ok, anomaly = can_apply(terminal, C.RINGING)
        assert ok is False
        assert anomaly == "OUT_OF_ORDER"


def test_stale_duplicate_same_rank_rejected():
    ok, anomaly = can_apply(C.ANSWERED, C.ANSWERED)
    assert ok is False
    assert anomaly == "OUT_OF_ORDER"
