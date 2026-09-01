"""Optional bonus integration — a real telecom provider, not exercised by the
test suite or the simulator. Requires PLIVO_AUTH_ID / PLIVO_AUTH_TOKEN /
PLIVO_ANSWER_URL in the environment. Structurally it is just another
TelecomProvider: the dialer, safety controller, and event applier do not know
or care that this one makes a real HTTP call instead of a virtual one.
"""
import os

import httpx

from .base import CallRequest, HealthSnapshot, ProviderError, ProviderTimeout
from .breaker import CircuitBreaker

PLIVO_API = "https://api.plivo.com/v1/Account/{auth_id}/Call/"


class PlivoProvider:
    name = "plivo"

    def __init__(self, clock, auth_id: str | None = None, auth_token: str | None = None,
                 answer_url: str | None = None, timeout_s: float = 10.0):
        self.clock = clock
        self.auth_id = auth_id or os.environ.get("PLIVO_AUTH_ID")
        self.auth_token = auth_token or os.environ.get("PLIVO_AUTH_TOKEN")
        self.answer_url = answer_url or os.environ.get("PLIVO_ANSWER_URL")
        self.timeout_s = timeout_s
        self.breaker = CircuitBreaker(clock)

    def health(self) -> HealthSnapshot:
        return HealthSnapshot(name=self.name, healthy=self.breaker.allow(),
                               error_rate=self.breaker.error_rate, p95_setup_s=1.5,
                               open_circuit=self.breaker.state == "open")

    async def place_call(self, req: CallRequest) -> str:
        if not (self.auth_id and self.auth_token and self.answer_url):
            raise ProviderError("plivo not configured")
        url = PLIVO_API.format(auth_id=self.auth_id)
        payload = {
            "from": req.from_number,
            "to": req.to_number,
            "answer_url": self.answer_url,
            "answer_method": "POST",
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_s) as client:
                resp = await client.post(url, json=payload, auth=(self.auth_id, self.auth_token))
        except httpx.TimeoutException as e:
            raise ProviderTimeout(self.name) from e
        except httpx.HTTPError as e:
            raise ProviderError(str(e)) from e
        if resp.status_code >= 400:
            raise ProviderError(f"plivo {resp.status_code}: {resp.text}")
        data = resp.json()
        return data.get("request_uuid") or data.get("call_uuid", "")

    async def cancel(self, provider_call_id: str) -> None:
        if not (self.auth_id and self.auth_token):
            return
        url = f"https://api.plivo.com/v1/Account/{self.auth_id}/Call/{provider_call_id}/"
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            await client.delete(url, auth=(self.auth_id, self.auth_token))
