"""Adaptive best-first board discovery.

The frontier holds query *intentions*, not URLs. After each query we measure

    yield = new_boards / results_returned

and use it to reprice neighbouring queries. Branches that keep returning
companies we already have quietly starve.
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from .adapters import adapter_for
from .detect import detect_many
from .http import Client
from .models import BoardRef, QueryResult
from .sources import ATS_HOSTS, brave_queries, brave_search
from .store import Store

log = logging.getLogger(__name__)

LOCATIONS = [
    "India", "Bengaluru", "Hyderabad", "Pune", "Mumbai", "Delhi", "Gurugram",
    "Noida", "Chennai", "Remote India", "Singapore", "London", "Berlin",
]
ROLES = [
    "software engineer", "backend engineer", "frontend engineer", "data engineer",
    "machine learning", "platform engineer", "devops", "security engineer",
    "product manager", "mobile engineer",
]

_counter = itertools.count()


@dataclass(order=True)
class FrontierItem:
    sort_key: float
    seq: int = field(compare=True)
    payload: dict[str, Any] = field(compare=False, default_factory=dict)


class Frontier:
    def __init__(self):
        self._heap: list[FrontierItem] = []
        self._seen: set[str] = set()

    def push(self, priority: float, **payload):
        sig = repr(sorted(payload.items()))
        if sig in self._seen:
            return
        self._seen.add(sig)
        heapq.heappush(self._heap, FrontierItem(-priority, next(_counter), payload))

    def pop(self) -> Optional[dict]:
        if not self._heap:
            return None
        return heapq.heappop(self._heap).payload

    def __len__(self) -> int:
        return len(self._heap)


def seed_frontier(frontier: Frontier, ats_list: list[str]) -> None:
    """Broad country-level queries first; role/city splits are earned, not assumed."""
    for ats in ats_list:
        for loc in ("India", "Bengaluru", "Hyderabad", "Remote India"):
            base = 1.0 if loc == "India" else 0.7
            frontier.push(base, source="brave", ats=ats, location=loc, role=None)


def expand(frontier: Frontier, payload: dict, yield_score: float) -> None:
    """Good branches beget siblings; bad ones get nothing."""
    if yield_score < 0.05:
        log.info("branch exhausted: %s", payload)
        return
    ats, loc, role = payload["ats"], payload.get("location"), payload.get("role")
    prio = yield_score

    if role is None:
        # Location paid off — split it by role.
        for r in ROLES[:5]:
            frontier.push(prio * 0.8, source="brave", ats=ats, location=loc, role=r)
    # Try neighbouring locations at a discount.
    for other in LOCATIONS:
        if other != loc:
            frontier.push(prio * 0.5, source="brave", ats=ats, location=other, role=role)


def build_query(payload: dict) -> list[str]:
    parts = []
    if payload.get("location"):
        parts.append(f'"{payload["location"]}"')
    if payload.get("role"):
        parts.append(f'"{payload["role"]}"')
    tail = " ".join(parts)
    return [f"{q} {tail}".strip() for q in brave_queries(payload["ats"], "")
            ] if not tail else [
        f"site:{host} {tail}" for host in ATS_HOSTS[payload["ats"]]]


async def validate_and_store(client: Client, store: Store, refs: list[BoardRef],
                             *, source: str, concurrency: int = 8,
                             on_active=None) -> int:
    """Probe each candidate against its real ATS before committing it.

    `on_active`, if given, is awaited with the new board_id the moment a board
    validates — the pipeline uses this to fetch its jobs immediately instead of
    waiting for the next due-boards poll.
    """
    sem = asyncio.Semaphore(concurrency)
    stored = 0

    async def one(ref: BoardRef):
        nonlocal stored
        adapter = adapter_for(ref.ats)
        async with sem:
            ok = await adapter.validate(client, ref)
        bid = store.upsert_board(
            ref, api_url=adapter.api_url(ref), career_url=adapter.career_url(ref),
            source=source, status="candidate")
        store.set_board_status(bid, "active" if ok else "dead")
        if ok:
            stored += 1
            if on_active is not None:
                await on_active(bid)

    await asyncio.gather(*(one(r) for r in refs))
    return stored


async def run_discovery(store: Store, client: Client, brave_key: str, *,
                        ats_list: list[str] = None, budget: int = 25,
                        on_active=None) -> dict:
    """Spend `budget` search queries, best-first."""
    ats_list = ats_list or list(ATS_HOSTS)
    frontier = Frontier()
    seed_frontier(frontier, ats_list)
    known = store.known_board_keys()
    totals = {"queries": 0, "urls": 0, "new_boards": 0, "validated": 0}

    while frontier and totals["queries"] < budget:
        payload = frontier.pop()
        for query in build_query(payload):
            if totals["queries"] >= budget or store.query_seen("brave", query):
                continue
            urls = await brave_search(client, query, brave_key)
            totals["queries"] += 1
            totals["urls"] += len(urls)

            found = detect_many(urls)
            fresh = [ref for key, ref in found.items() if key not in known]
            known.update(r.key for r in fresh)

            res = QueryResult("brave", query, payload, urls,
                              list(found.values()), len(fresh))
            store.record_query(res)
            totals["new_boards"] += len(fresh)
            log.info("[%s] %d urls -> %d boards (%d new, yield %.2f)",
                     query, len(urls), len(found), len(fresh), res.yield_score)

            if fresh:
                totals["validated"] += await validate_and_store(
                    client, store, fresh, source=f"brave:{query}", on_active=on_active)
            expand(frontier, payload, res.yield_score)

    return totals
