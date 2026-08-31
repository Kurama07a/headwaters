"""Ingestion worker. Never touches a search engine.

Pulls due boards off the registry, fetches every posting from the ATS,
normalizes, diffs against the last crawl, and reschedules based on churn.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from .adapters import adapter_for
from .http import Client, FetchError
from .models import BoardRef
from .store import Store

log = logging.getLogger(__name__)

DEAD_AFTER_FAILURES = 4


def row_to_ref(row) -> BoardRef:
    return BoardRef(row["ats_type"], row["board_identifier"],
                    row["career_url"], row["country_hint"])


async def ingest_board(client: Client, store: Store, row) -> dict:
    ref = row_to_ref(row)
    adapter = adapter_for(ref.ats)
    try:
        jobs = await adapter.fetch_jobs(client, ref)
    except (FetchError, Exception) as exc:  # noqa: BLE001
        # Unstable adapters (Workday) are expected to fail sometimes; never let
        # one tenant's breakage kill the run.
        level = logging.INFO if not adapter.stable else logging.WARNING
        log.log(level, "fetch failed %s: %s", ref, exc)
        store.set_board_status(row["id"], "error")
        if row["fail_count"] + 1 >= DEAD_AFTER_FAILURES:
            store.set_board_status(row["id"], "dead")
        store.reschedule(row["id"], changed=0, interval_s=row["refresh_interval_s"])
        return {"board": str(ref), "error": str(exc)}

    if jobs is None:
        store.set_board_status(row["id"], "dead")
        return {"board": str(ref), "error": "no valid response"}

    for job in jobs:
        job.company = job.company or row["board_identifier"]

    counts = store.sync_jobs(row["id"], jobs, close_missing=adapter.closes_missing)
    store.set_board_status(row["id"], "active", job_count=len(jobs))
    store.reschedule(row["id"], changed=counts["new"] + counts["changed"],
                     interval_s=row["refresh_interval_s"])
    log.info("%s: %d jobs (+%d new, ~%d changed, -%d closed)", ref,
             counts["total"], counts["new"], counts["changed"], counts["closed"])
    return {"board": str(ref), **counts}


async def run_ingest(store: Store, client: Client, *, limit: int = 100,
                     concurrency: int = 6, ats: str = None) -> list[dict]:
    rows = store.due_boards(limit=limit, ats=ats)
    if not rows:
        log.info("nothing due")
        return []
    sem = asyncio.Semaphore(concurrency)

    async def guarded(row):
        async with sem:
            return await ingest_board(client, store, row)

    return await asyncio.gather(*(guarded(r) for r in rows))


async def run_pipeline(store: Store, client: Client, *, workers: int = 32,
                       poll_interval: int = 300, ats: str = None,
                       discover_budget: int = 0, brave_key: str = None,
                       max_idle_polls: int = None) -> dict:
    """Long-lived ingest pipeline: one process, one Client, one Store.

    A producer polls `due_boards()` and feeds an asyncio.Queue; a fixed pool of
    workers drains it. Per-host gates in http.py do the rate shaping, so workers
    can be heavily over-provisioned — Greenhouse work self-throttles while other
    ATSs run in parallel. If discovery runs, freshly-validated boards are pushed
    straight onto the queue (no wait for the next poll).

    Runs until Ctrl-C, or until `max_idle_polls` consecutive empty polls.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=workers * 4)
    in_flight: set[int] = set()
    stop = asyncio.Event()
    totals = {"boards": 0, "new": 0, "changed": 0, "closed": 0, "errors": 0}
    t0 = time.monotonic()

    async def enqueue(row):
        if row is not None and row["id"] not in in_flight:
            in_flight.add(row["id"])
            await queue.put(row)

    async def worker():
        while True:
            row = await queue.get()
            try:
                res = await ingest_board(client, store, row)
                totals["boards"] += 1
                if "error" in res:
                    totals["errors"] += 1
                else:
                    for k in ("new", "changed", "closed"):
                        totals[k] += res[k]
            except Exception as exc:  # noqa: BLE001 - one board must not kill the pool
                totals["errors"] += 1
                log.warning("worker crashed on board %s: %s", row["id"], exc)
            finally:
                in_flight.discard(row["id"])
                queue.task_done()

    async def producer():
        idle = 0
        while not stop.is_set():
            rows = store.due_boards(limit=max(500, workers * 10), ats=ats)
            fresh = [r for r in rows if r["id"] not in in_flight]
            for r in fresh:
                await enqueue(r)
            if fresh or in_flight or not queue.empty():
                idle = 0
                await asyncio.sleep(1)   # spin fast while a backlog is draining
            else:
                idle += 1
                if max_idle_polls and idle >= max_idle_polls:
                    return
                log.info("idle — nothing due; next poll in %ds", poll_interval)
                await asyncio.sleep(poll_interval)

    workers_t = [asyncio.create_task(worker()) for _ in range(workers)]
    producer_t = asyncio.create_task(producer())

    if discover_budget and brave_key:
        from .discovery import run_discovery
        log.info("discovery pass (budget=%d) …", discover_budget)
        await run_discovery(store, client, brave_key,
                            ats_list=[ats] if ats else None,
                            budget=discover_budget, on_active=enqueue_by_id(store, enqueue))

    try:
        await producer_t
        await queue.join()
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("interrupted — draining in-flight work")
    finally:
        stop.set()
        for t in workers_t:
            t.cancel()
        await asyncio.gather(*workers_t, return_exceptions=True)
        if not producer_t.done():
            producer_t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await producer_t

    dt = time.monotonic() - t0
    rate = totals["boards"] / dt if dt else 0
    log.info("pipeline done: %s in %.0fs (%.1f boards/s) retries=%s",
             totals, dt, rate, client.retry_counts)
    return totals


def enqueue_by_id(store: Store, enqueue):
    """Adapt on_active(board_id) -> enqueue(board_row)."""
    async def _cb(board_id: int):
        await enqueue(store.get_board(board_id))
    return _cb


async def validate_candidates(store: Store, client: Client, limit: int = 500) -> dict:
    """Promote candidate boards to active (or bury them) before ingest."""
    rows = store.candidates(limit)
    sem = asyncio.Semaphore(8)
    stats = {"active": 0, "dead": 0}

    async def one(row):
        ref = row_to_ref(row)
        async with sem:
            ok = await adapter_for(ref.ats).validate(client, ref)
        store.set_board_status(row["id"], "active" if ok else "dead")
        stats["active" if ok else "dead"] += 1

    await asyncio.gather(*(one(r) for r in rows))
    return stats
