"""CLI: python -m jobindex.cli <command>

    seed        import URLs (Common Crawl dump, a seed list, anything) -> boards
    seed-feeds  register the aggregator feed boards (RemoteOK, Remotive, ...)
    probe-slugs company-name list -> candidate ATS slugs -> validate_and_store
    import-yc   pull YC directory (optionally --location India --hiring) -> probe slugs
    discover-subdomains  CT logs (certspotter) -> boards for a given ATS domain
    crawl-cc    pull ATS URLs straight out of a Common Crawl index
    discover    spend Brave queries, best-first
    validate    probe candidate boards against their ATS
    ingest      one bounded pass over due boards
    run         long-lived pipeline: producer + worker pool, runs forever
    dedupe      merge boards that differ only by identifier case
    reclassify  recompute experience_level / seniority_rank / location over stored jobs
    search      query the local index
    report      jobs discovered since a watermark (JSON for n8n/cron alerts)
    stats       registry counts
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone

from .adapters import FEED_ATS, adapter_for
from .detect import detect_many
from .discovery import run_discovery, validate_and_store
from .http import Client
from .ingest import run_ingest, run_pipeline, validate_candidates
from .models import BoardRef
from .normalize import SENIORITY_RANK, classify_seniority, parse_location
from .sources import (ATS_HOSTS, cc_columnar_sql, cc_indexes, cc_urls,
                      certspotter_hostnames, company_slug_candidates, yc_companies)
from .store import Store

# ATS types slug-probing can cheaply validate (one GET per candidate)
PROBEABLE_ATS = ["greenhouse", "lever", "ashby", "smartrecruiters",
                 "workable", "recruitee", "breezy", "keka"]

# how to turn a bare CT-log hostname into a URL detect() can parse
_HOST_TO_URL = {
    "keka": lambda h: f"https://{h}/careers/",
    "recruitee": lambda h: f"https://{h}/",
    "breezy": lambda h: f"https://{h}/",
    "workday": lambda h: f"https://{h}/External",   # site guess; validate filters
}


def _log(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname).1s %(name)s: %(message)s",
        datefmt="%H:%M:%S")


async def cmd_seed(args):
    store = Store(args.db)
    urls = [l.strip() for l in open(args.file) if l.strip()]
    refs = list(detect_many(urls).values())
    print(f"{len(urls)} urls -> {len(refs)} unique boards")
    known = store.known_board_keys()
    fresh = [r for r in refs if r.key not in known]
    print(f"{len(fresh)} not already in registry")
    async with Client() as client:
        n = await validate_and_store(client, store, fresh, source=f"seed:{args.file}")
    print(f"{n} validated active")
    store.close()


async def cmd_seed_feeds(args):
    store = Store(args.db)
    for ats in FEED_ATS:
        a = adapter_for(ats)
        ref = BoardRef(ats, ats)
        bid = store.upsert_board(ref, api_url=a.api_url(ref), career_url=a.career_url(ref),
                                 source="feed", status="candidate")
        store.set_board_status(bid, "active")
        print(f"  {ats:12} -> board {bid} active  ({a.api_url(ref)})")
    print(f"\n{len(FEED_ATS)} feed board(s) registered — `ingest`/`run` will pull them")
    store.close()


async def _probe_names(store, names, ats_list, *, source, concurrency=8):
    """company names -> slug candidates -> validate_and_store, deduped vs registry."""
    ats_list = ats_list or PROBEABLE_ATS
    known = store.known_board_keys()
    refs, seen = [], set()
    for name in names:
        for slug in company_slug_candidates(name):
            for ats in ats_list:
                if (ats, slug) in known or (ats, slug) in seen:
                    continue
                seen.add((ats, slug))
                refs.append(BoardRef(ats, slug))
    print(f"{len(names)} names -> {len(refs)} candidate (ats, slug) probes across {ats_list}")
    async with Client() as client:
        n = await validate_and_store(client, store, refs, source=source,
                                     concurrency=concurrency)
    print(f"{n} validated active")


async def cmd_probe_slugs(args):
    store = Store(args.db)
    names = [l.strip() for l in open(args.file, encoding="utf-8") if l.strip()]
    await _probe_names(store, names, args.ats, source=f"probe:{args.file}",
                       concurrency=args.concurrency)
    store.close()


async def cmd_import_yc(args):
    store = Store(args.db)
    async with Client() as client:
        cos = await yc_companies(client, hiring_only=args.hiring, location=args.location)
    print(f"YC directory: {len(cos)} companies"
          f"{' hiring' if args.hiring else ''}"
          f"{' in ' + args.location if args.location else ''}")
    await _probe_names(store, [c["name"] for c in cos[:args.limit]], args.ats,
                       source="yc-oss", concurrency=args.concurrency)
    store.close()


async def cmd_discover_subdomains(args):
    store = Store(args.db)
    async with Client() as client:
        hosts = await certspotter_hostnames(client, args.domain)
        print(f"{len(hosts)} hostnames from CT logs for {args.domain}")
        mk = _HOST_TO_URL.get(args.ats, lambda h: f"https://{h}/")
        refs = list(detect_many(mk(h) for h in hosts).values())
        known = store.known_board_keys()
        fresh = [r for r in refs if r.key not in known]
        print(f"{len(refs)} parsed to boards, {len(fresh)} new")
        n = await validate_and_store(client, store, fresh,
                                     source=f"certspotter:{args.domain}")
    print(f"{n} validated active")
    store.close()


async def cmd_crawl_cc(args):
    store = Store(args.db)
    async with Client() as client:
        indexes = await cc_indexes(client, latest=args.indexes)
        print("indexes:", indexes)
        collected: list[str] = []
        for index in indexes:
            for ats in (args.ats or list(ATS_HOSTS)):
                for host in ATS_HOSTS[ats]:
                    async for url in cc_urls(client, index, host,
                                             page_limit=args.pages):
                        collected.append(url)
        refs = list(detect_many(collected).values())
        print(f"{len(collected)} urls -> {len(refs)} boards")
        known = store.known_board_keys()
        fresh = [r for r in refs if r.key not in known]
        n = await validate_and_store(client, store, fresh, source="commoncrawl")
    print(f"{n} validated active")
    store.close()


async def cmd_discover(args):
    key = args.brave_key or os.environ.get("BRAVE_API_KEY")
    if not key:
        sys.exit("set BRAVE_API_KEY or pass --brave-key")
    store = Store(args.db)
    async with Client() as client:
        totals = await run_discovery(store, client, key,
                                     ats_list=args.ats, budget=args.budget)
    print(json.dumps(totals, indent=2))
    store.close()


async def cmd_validate(args):
    store = Store(args.db)
    async with Client() as client:
        print(await validate_candidates(store, client, limit=args.limit))
    store.close()


async def cmd_ingest(args):
    store = Store(args.db)
    async with Client() as client:
        results = await run_ingest(store, client, limit=args.limit,
                                   concurrency=args.concurrency, ats=args.ats)
    ok = [r for r in results if "error" not in r]
    print(f"{len(ok)}/{len(results)} boards ok; "
          f"+{sum(r['new'] for r in ok)} new, -{sum(r['closed'] for r in ok)} closed")
    store.close()


async def cmd_run(args):
    store = Store(args.db)
    async with Client() as client:
        await run_pipeline(
            store, client, workers=args.workers, poll_interval=args.poll_interval,
            ats=args.ats, discover_budget=args.discover_budget,
            brave_key=args.brave_key or os.environ.get("BRAVE_API_KEY"),
            max_idle_polls=args.max_idle_polls)
    store.close()


async def cmd_dedupe(args):
    store = Store(args.db)
    groups = store.duplicate_board_groups()
    merged = 0
    for keep, losers in groups:
        for lo in losers:
            moved = store.merge_board(keep["id"], lo["id"])
            merged += 1
            print(f"{keep['ats_type']}:{keep['board_identifier']}  <- "
                  f"{lo['board_identifier']} ({moved} jobs moved)")
    print(f"\nmerged {merged} duplicate board(s) across {len(groups)} identit(ies)")
    store.close()


async def cmd_reclassify(args):
    """Recompute seniority + location over stored jobs, from title / description /
    location_raw. Idempotent — re-run after a normalize.py change instead of a
    full re-ingest."""
    store = Store(args.db)
    rows = list(store.db.execute(
        "SELECT id, title, description, location_raw FROM jobs"))
    updates = []
    for r in rows:
        label, rank = classify_seniority(r["title"], r["description"])
        loc = parse_location(r["location_raw"])
        updates.append((label, rank, loc["city"], loc["region"], loc["country"],
                        loc["remote_type"], r["id"]))
    store.db.executemany(
        "UPDATE jobs SET experience_level=?, seniority_rank=?, city=?, region=?, "
        "country=?, remote_type=? WHERE id=?", updates)
    store.db.commit()
    ranked = sum(1 for u in updates if u[1] is not None)
    print(f"renormalized {len(updates)} jobs; {ranked} have a seniority rank")
    store.close()


async def cmd_search(args):
    store = Store(args.db)
    max_level = SENIORITY_RANK[args.max_level] if args.max_level else None
    rows = store.search(text=args.text, country=args.country, city=args.city,
                        remote=args.remote, level=args.level, max_level=max_level,
                        days=args.days, limit=args.limit)
    for r in rows:
        loc = " / ".join(x for x in (r["city"], r["country"], r["remote_type"]) if x)
        lvl = f"[{r['experience_level']}] " if r["experience_level"] else ""
        print(f"{r['title'][:60]:60}  {r['board_identifier'][:20]:20}  {lvl}{loc}")
        if args.urls and r["apply_url"]:
            print(f"    {r['apply_url']}")
    print(f"\n{len(rows)} result(s)")
    store.close()


async def cmd_stats(args):
    store = Store(args.db)
    print(json.dumps(store.stats(), indent=2))
    store.close()


def _parse_since(s: str | None) -> str | None:
    """'4h' / '90m' / '2d' -> ISO cutoff; an ISO string passes through."""
    if not s:
        return None
    m = re.fullmatch(r"(\d+)\s*([mhd])", s.strip())
    if m:
        secs = int(m.group(1)) * {"m": 60, "h": 3600, "d": 86400}[m.group(2)]
        return (datetime.now(timezone.utc) - timedelta(seconds=secs)).isoformat()
    return s


async def cmd_report(args):
    """Jobs newly discovered since a watermark — the alerting feed for n8n/cron.

    With --advance-watermark it reads/writes meta.last_report_at, so back-to-back
    runs never re-report the same posting regardless of the schedule interval.
    """
    store = Store(args.db)
    since = (_parse_since(args.since)
             or store.get_meta("last_report_at")
             or (datetime.now(timezone.utc) - timedelta(days=1)).isoformat())
    run_at = datetime.now(timezone.utc).isoformat()
    max_level = SENIORITY_RANK[args.max_level] if args.max_level else None
    rows = store.search(text=args.text, country=args.country, city=args.city,
                        remote=args.remote, max_level=max_level, since=since,
                        limit=args.limit)
    if args.json:
        print(json.dumps([{
            "title": r["title"], "company": r["company"] or r["board_identifier"],
            "ats": r["ats_type"], "city": r["city"], "country": r["country"],
            "remote_type": r["remote_type"], "level": r["experience_level"],
            "apply_url": r["apply_url"], "first_seen_at": r["first_seen_at"],
        } for r in rows], ensure_ascii=False))
    else:
        for r in rows:
            loc = " / ".join(x for x in (r["city"], r["country"], r["remote_type"]) if x)
            lvl = f"[{r['experience_level']}] " if r["experience_level"] else ""
            print(f"{r['title'][:58]:58}  {(r['company'] or r['board_identifier'])[:22]:22}  {lvl}{loc}")
            if r["apply_url"]:
                print(f"    {r['apply_url']}")
        print(f"\n{len(rows)} new posting(s) since {since}")
    if args.advance_watermark:
        store.set_meta("last_report_at", run_at)
    store.close()


def main(argv=None):
    p = argparse.ArgumentParser(prog="jobindex")
    p.add_argument("--db", default="jobs.db")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed"); s.add_argument("file"); s.set_defaults(fn=cmd_seed)

    sub.add_parser("seed-feeds").set_defaults(fn=cmd_seed_feeds)

    s = sub.add_parser("probe-slugs")
    s.add_argument("file", help="text file, one company name per line")
    s.add_argument("--ats", nargs="*", choices=PROBEABLE_ATS)
    s.add_argument("--concurrency", type=int, default=8)
    s.set_defaults(fn=cmd_probe_slugs)

    s = sub.add_parser("discover-subdomains")
    s.add_argument("domain", help="e.g. keka.com, recruitee.com, myworkdayjobs.com")
    s.add_argument("--ats", choices=list(_HOST_TO_URL), default="keka")
    s.set_defaults(fn=cmd_discover_subdomains)

    s = sub.add_parser("import-yc")
    s.add_argument("--location", help="filter by location substring, e.g. India")
    s.add_argument("--hiring", action="store_true", help="only companies marked hiring")
    s.add_argument("--limit", type=int, default=500)
    s.add_argument("--ats", nargs="*", choices=PROBEABLE_ATS)
    s.add_argument("--concurrency", type=int, default=8)
    s.set_defaults(fn=cmd_import_yc)

    s = sub.add_parser("crawl-cc")
    s.add_argument("--ats", nargs="*", choices=list(ATS_HOSTS))
    s.add_argument("--indexes", type=int, default=1)
    s.add_argument("--pages", type=int, default=3)
    s.set_defaults(fn=cmd_crawl_cc)

    s = sub.add_parser("discover")
    s.add_argument("--brave-key")
    s.add_argument("--ats", nargs="*", choices=list(ATS_HOSTS))
    s.add_argument("--budget", type=int, default=25)
    s.set_defaults(fn=cmd_discover)

    s = sub.add_parser("validate")
    s.add_argument("--limit", type=int, default=500)
    s.set_defaults(fn=cmd_validate)

    s = sub.add_parser("ingest")
    s.add_argument("--limit", type=int, default=100)
    s.add_argument("--concurrency", type=int, default=6)
    s.add_argument("--ats", choices=list(ATS_HOSTS) + FEED_ATS)
    s.set_defaults(fn=cmd_ingest)

    s = sub.add_parser("run")
    s.add_argument("--workers", type=int, default=32)
    s.add_argument("--poll-interval", type=int, default=300, help="seconds between due-board polls")
    s.add_argument("--ats", choices=list(ATS_HOSTS) + FEED_ATS)
    s.add_argument("--discover-budget", type=int, default=0, help="Brave queries to spend on startup")
    s.add_argument("--brave-key")
    s.add_argument("--max-idle-polls", type=int, default=None,
                   help="exit after N empty polls (default: run forever)")
    s.set_defaults(fn=cmd_run)

    sub.add_parser("dedupe").set_defaults(fn=cmd_dedupe)

    sub.add_parser("reclassify").set_defaults(fn=cmd_reclassify)

    s = sub.add_parser("search")
    s.add_argument("text", nargs="?")
    s.add_argument("--country"); s.add_argument("--city")
    s.add_argument("--remote", choices=["remote", "hybrid", "onsite"])
    s.add_argument("--level")
    s.add_argument("--max-level", choices=list(SENIORITY_RANK),
                   help="cap seniority: 'entry' shows intern+entry only")
    s.add_argument("--days", type=int)
    s.add_argument("--limit", type=int, default=50)
    s.add_argument("--urls", action="store_true")
    s.set_defaults(fn=cmd_search)

    s = sub.add_parser("report")
    s.add_argument("--since", help="'4h' / '90m' / '2d' / ISO ts; default: last watermark or 24h")
    s.add_argument("--advance-watermark", action="store_true",
                   help="persist 'now' as meta.last_report_at after reporting")
    s.add_argument("--country"); s.add_argument("--city")
    s.add_argument("--remote", choices=["remote", "hybrid", "onsite"])
    s.add_argument("--max-level", choices=list(SENIORITY_RANK))
    s.add_argument("--text")
    s.add_argument("--limit", type=int, default=200)
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_report)

    sub.add_parser("stats").set_defaults(fn=cmd_stats)
    sub.add_parser("cc-sql").set_defaults(
        fn=lambda a: print(cc_columnar_sql()) or asyncio.sleep(0))

    args = p.parse_args(argv)
    _log(args.verbose)
    result = args.fn(args)
    if asyncio.iscoroutine(result):
        asyncio.run(result)


if __name__ == "__main__":
    main()
