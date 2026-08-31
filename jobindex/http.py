"""Async HTTP with per-host concurrency caps, pacing, and retry/backoff.

Every outbound request in the system goes through this. Rate limits are keyed
by hostname, so a slow Workday tenant can never starve the Greenhouse crawler.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Optional
from urllib.parse import urlsplit

import httpx

log = logging.getLogger(__name__)

DEFAULT_RPS = 2.0
DEFAULT_CONCURRENCY = 3

# host -> (requests per second, max in-flight). The public board APIs are
# CDN-fronted read endpoints and tolerate well above the original 4-5 rps;
# the retry path honours Retry-After, so an over-aggressive value degrades
# to graceful backoff rather than failure. Tune against `client.retry_counts`.
HOST_LIMITS: dict[str, tuple[float, int]] = {
    "boards-api.greenhouse.io": (15.0, 12),
    "api.lever.co": (8.0, 6),
    "api.ashbyhq.com": (8.0, 6),
    "api.smartrecruiters.com": (8.0, 6),
    "index.commoncrawl.org": (1.0, 1),
    "api.search.brave.com": (1.0, 1),
}

RETRY_STATUS = {429, 500, 502, 503, 504, 408}
MAX_ATTEMPTS = 4
USER_AGENT = (
    "jobindex/0.1 (+https://example.com/bot; job board indexer) "
    "python-httpx"
)


class FetchError(Exception):
    def __init__(self, url: str, status: Optional[int], detail: str = ""):
        super().__init__(f"{url} -> {status} {detail}".strip())
        self.url = url
        self.status = status


class _HostGate:
    """Token-bucket-ish pacing plus a concurrency semaphore, per host."""

    def __init__(self, rps: float, concurrency: int):
        self.min_interval = 1.0 / rps if rps > 0 else 0.0
        self.sem = asyncio.Semaphore(concurrency)
        self.lock = asyncio.Lock()
        self.next_slot = 0.0

    async def __aenter__(self):
        await self.sem.acquire()
        async with self.lock:
            now = time.monotonic()
            wait = self.next_slot - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self.next_slot = now + self.min_interval
        return self

    async def __aexit__(self, *exc):
        self.sem.release()


class Client:
    """Thin wrapper over httpx.AsyncClient. Use as an async context manager."""

    def __init__(self, timeout: float = 20.0, user_agent: str = USER_AGENT):
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": user_agent, "Accept": "application/json, */*"},
            limits=httpx.Limits(max_connections=60, max_keepalive_connections=20),
        )
        self._gates: dict[str, _HostGate] = {}
        self._gates_lock = asyncio.Lock()
        #: host -> count of retryable responses (429/5xx/408). Watch this when
        #: raising HOST_LIMITS; a climbing number means you've gone too far.
        self.retry_counts: dict[str, int] = {}

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc):
        await self._client.aclose()

    async def _gate(self, host: str) -> _HostGate:
        async with self._gates_lock:
            gate = self._gates.get(host)
            if gate is None:
                rps, conc = HOST_LIMITS.get(host, (DEFAULT_RPS, DEFAULT_CONCURRENCY))
                gate = _HostGate(rps, conc)
                self._gates[host] = gate
            return gate

    async def request(self, method: str, url: str, **kw) -> httpx.Response:
        host = urlsplit(url).netloc.lower()
        gate = await self._gate(host)
        last: Optional[Exception] = None

        for attempt in range(MAX_ATTEMPTS):
            if attempt:
                delay = min(30.0, 1.5 ** attempt) * (0.5 + random.random())
                await asyncio.sleep(delay)
            try:
                async with gate:
                    resp = await self._client.request(method, url, **kw)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last = exc
                log.debug("transport error %s (attempt %d): %s", url, attempt, exc)
                continue

            if resp.status_code in RETRY_STATUS:
                self.retry_counts[host] = self.retry_counts.get(host, 0) + 1
                retry_after = resp.headers.get("retry-after")
                if retry_after:
                    try:
                        await asyncio.sleep(min(60.0, float(retry_after)))
                    except ValueError:
                        pass
                last = FetchError(url, resp.status_code)
                log.debug("retryable %s on %s", resp.status_code, url)
                continue

            return resp

        raise FetchError(url, getattr(last, "status", None), str(last))

    async def get_json(self, url: str, **kw) -> Any:
        resp = await self.request("GET", url, **kw)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def post_json(self, url: str, json: dict, **kw) -> Any:
        resp = await self.request("POST", url, json=json, **kw)
        if resp.status_code in (404, 400):
            return None
        resp.raise_for_status()
        return resp.json()
