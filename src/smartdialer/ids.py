import os
import socket
import uuid


def worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


def idempotency_key(campaign_id: int, borrower_id: int, attempt_no: int) -> str:
    return f"{campaign_id}:{borrower_id}:{attempt_no}"
