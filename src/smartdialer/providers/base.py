from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class CallRequest:
    call_id: int
    to_number: str
    from_number: str
    idempotency_key: str


@dataclass(frozen=True)
class HealthSnapshot:
    name: str
    healthy: bool
    error_rate: float
    p95_setup_s: float
    open_circuit: bool

    @property
    def score(self) -> float:
        return 0.0 if self.open_circuit else max(0.0, 1.0 - self.error_rate)


class ProviderError(Exception):
    ...


class ProviderTimeout(ProviderError):
    ...


class TelecomProvider(Protocol):
    name: str

    async def place_call(self, req: CallRequest) -> str: ...   # -> provider_call_id
    async def cancel(self, provider_call_id: str) -> None: ...
    def health(self) -> HealthSnapshot: ...


# Provider-specific event names, normalised at the webhook boundary so nothing
# downstream knows which provider a call came from.
EVENT_MAP = {
    "ringing": "RINGING",
    "answered": "ANSWERED",
    "completed": "COMPLETED",
    "failed": "FAILED",
    "busy": "FAILED",
    "no-answer": "FAILED",
}


def map_provider_event(raw: str) -> str:
    return EVENT_MAP.get(raw, raw.upper())


class WebhookSink:
    """Delivers a provider event straight into the ledger, through the exact
    same `EventRepo.ingest` used by the real HTTP webhook route
    (api/webhooks.py). The in-process mock providers use this so a live demo
    does not require running the API process to receive callbacks; real
    providers (Plivo) go through the HTTP route instead."""

    def __init__(self, db, event_repo):
        self.db = db
        self.events = event_repo

    async def deliver(self, provider: str, provider_event_id: str, provider_call_id: str,
                       event_type: str, ts: float, payload: dict) -> None:
        async with self.db.tx() as con:
            await self.events.ingest(con, provider, provider_event_id, provider_call_id,
                                      event_type, ts, payload)
