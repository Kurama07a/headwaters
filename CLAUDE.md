# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`jobindex` — an ATS job-board discovery and indexing engine. The central idea:
**the board is the unit, not the URL.** One search result reveals one board;
one board yields thousands of jobs over its lifetime. The system is two loops
with opposite cost profiles:

```
discovery (search engines)  ──►  board registry (SQLite)  ──►  ingestion (ATS APIs)
      rare, expensive                                            constant, cheap
```

Status per README: a working skeleton. **ATS field mappings in `adapters.py`
are unverified against live traffic** — fetch one real response per ATS and
diff before trusting job counts.

## Commands

```bash
pip install httpx                 # the only runtime dependency

python test_offline.py            # full test suite, no network needed; exits nonzero on failure

# CLI — all commands take global --db (default jobs.db) and -v/--verbose
python -m jobindex.cli crawl-cc --ats lever ashby --pages 2   # bootstrap boards from Common Crawl
python -m jobindex.cli seed urls.txt                          # import a URL list -> boards
python -m jobindex.cli seed-feeds                             # register RemoteOK/Remotive/Arbeitnow/HN/Adzuna boards
python -m jobindex.cli probe-slugs companies.txt --ats greenhouse lever ashby workable keka  # name list -> slug guesses -> validate
python -m jobindex.cli import-yc --location India --hiring                    # YC directory -> probe slugs
python -m jobindex.cli discover-subdomains myworkdayjobs.com --ats workday   # CT logs (certspotter) -> boards
python -m jobindex.cli discover --budget 25                   # spend Brave queries (needs BRAVE_API_KEY)
python -m jobindex.cli validate --limit 500                   # promote/bury candidate boards
python -m jobindex.cli ingest --limit 200                     # one bounded pass over due boards
python -m jobindex.cli run --workers 32 --poll-interval 300   # long-lived pipeline; runs forever
python -m jobindex.cli run --max-idle-polls 1                 # ...or one full drain then exit
python -m jobindex.cli dedupe                                 # merge boards differing only by identifier case
python -m jobindex.cli reclassify                             # recompute seniority + location over stored jobs
python -m jobindex.cli search "backend" --country IN --max-level entry --days 7 --urls
python -m jobindex.cli report --country IN --max-level mid --json --advance-watermark  # new-since-last-run feed for cron/n8n
python -m jobindex.cli stats
python -m jobindex.cli cc-sql                                 # prints the DuckDB columnar-index query
```

Global flags (`--db`, `-v`) go **before** the subcommand: `python -m jobindex.cli -v run ...`.

Environment: Python 3.14, httpx 0.28. No `requirements.txt`, `pyproject.toml`,
lint config, or CI — none exist yet. `jobs.db` / `jobs.db-shm` / `jobs.db-wal`
are the live local registry (SQLite in WAL mode).

### Tests

`test_offline.py` is a single flat script, not pytest — a sequence of `check(label, got, want)`
calls covering detection, normalization, adapter parsing against fixture payloads,
and store change-detection. There is no per-test selector; to run a subset,
edit the script. It writes a throwaway DB to `/tmp/t.db`. Adapters are tested
with a `Fake` client that returns a canned payload, so tests never hit the network.

## Architecture

### Layering — everything above the adapter is ATS-agnostic

All modules speak in the three types from `models.py`:

- **`BoardRef`** (frozen) — the deduplication unit. `(ats, identifier)` is the key.
  `identifier` is the stable tenant slug (`greenhouse` → `"openai"`). Workday packs
  three fields into it: `"tenant|dc|site"`, unpacked via `.workday_parts`.
- **`Job`** — normalized posting, one per `(board, external_job_id)`. `.as_row()` drops `raw`.
- **`QueryResult`** — outcome of one discovery query; `.yield_score = new_boards / urls`
  drives the frontier.

### Data flow

1. **URLs / slugs in** — from `sources.py` (Common Crawl CDX / columnar, Brave API,
   or `company_slug_candidates(name)` for slug-probing a company-name list via
   `cli probe-slugs` — highest yield per rupee for known companies).
2. **`detect.py`** — `detect(url) -> BoardRef | None`, deliberately strict (a false
   positive pollutes the registry with a garbage slug). `detect_many()` collapses a
   URL stream to unique boards.
3. **`discovery.validate_and_store()`** — probes each candidate against its real ATS
   (`adapter.validate`, a `limit=1` fetch), then `store.upsert_board(...)` and sets
   status `active` / `dead`.
4. **`ingest.py`** — `run_ingest` (bounded) or `run_pipeline` (long-lived: one
   `Client`, one `Store`, a producer polling `due_boards()` into an `asyncio.Queue`
   drained by a fixed worker pool). Both call `adapter.fetch_jobs` → `store.sync_jobs()`
   (diff vs. last crawl) → `store.reschedule()` (cadence by churn). Rate shaping is
   entirely in `http.py`'s per-host gates, so workers can be over-provisioned freely.
   Discovery passes an `on_active` callback so a freshly validated board is fetched
   immediately instead of waiting for the next poll. Never touches a search engine.

### `adapters.py` — one class per ATS

Contract: `api_url(ref)`, `career_url(ref)`, `validate(client, ref) -> bool`,
`fetch_jobs(client, ref, limit=None) -> list[Job] | None`. Registered in the
`ADAPTERS` dict by `.name`; look up with `adapter_for(ats)`.

Per-tenant ATSs, all verified against one live response: **Greenhouse, Lever, Ashby,
SmartRecruiters, Workday, Workable** (`apply.workable.com` widget endpoint),
**Recruitee** (`{slug}.recruitee.com/api/offers/`), **Breezy** (`{slug}.breezy.hr/json`
— summary feed, no per-job description), **Keka** (`{slug}.keka.com/careers/api/jobs/default/active`
— Indian HRMS), **Oracle** Recruiting Cloud (`stable = False`; `identifier` packs
`"{host}|{siteNumber}"` like Workday packs three; list response has no description).
`detect.py` recognises all of them.

**Aggregator feeds** (`FeedAdapter` subclasses, listed in `FEED_ATS`): **RemoteOK,
Remotive, Arbeitnow, HNWhoIsHiring** (heuristic parse of the monthly thread via
Algolia), **AdzunaIN** (dormant unless `ADZUNA_APP_ID` / `ADZUNA_APP_KEY` set). Not
per-tenant — one synthetic board each, registered by `cli seed-feeds`, never
discovered from a URL. They set `stable = False` and `closes_missing = False`
(they return only a recent slice, so `sync_jobs(close_missing=False)` ages jobs out by
`first_seen_at` instead of force-closing everything that fell off the feed).

**Workday is quarantined**: `stable = False`. It's a scrape of the CXS frontend, not
a supported API. `ingest_board` logs unstable-adapter failures at INFO instead of
WARNING and never lets one tenant's breakage abort the run. In production, give it a
separate worker.

**Not built:**
- **Eightfold** — the old public `{co}.eightfold.ai/api/apply/v2/jobs` now returns
  `403 {"message":"Not authorized for PCSX"}` on *every* tenant tried; Eightfold
  disabled the unauthenticated jobs API. Effectively closed — not worth pursuing.
- **iCIMS** (`careers-{co}.icims.com`) — the portal serves server-rendered **HTML
  fragments**, not JSON (`format=json`/`rss` are ignored), and `/sitemap.xml` is
  IP-reputation-gated (`403 "not on a trusted network"` from datacenter egress). A
  real adapter here is an HTML scraper (`stable=False`), best run from a residential
  IP or with a browser-captured request. Medium value, deferred.
- **SuccessFactors** — OData API needs auth; career-site scrape is bespoke per tenant.

**Company discovery** (`sources.py`): `company_slug_candidates(name)` +
`yc_companies(client, hiring_only=, location=)` (yc-oss daily JSON — a free stand-in
for Tracxn/Crunchbase). `cli import-yc --location India` pulls the YC India list
(~223 cos) straight into slug-probing. `certspotter_hostnames()` for CT-log tenant
enumeration (only productive for Workday — wildcard-cert ATSs like Keka don't expose
per-tenant hostnames).

### `http.py` — every outbound request goes through `Client`

Per-**host** rate gates (`_HostGate`: token-bucket pacing + concurrency semaphore),
so a slow Workday tenant can't starve the Greenhouse crawler. Per-host limits in
`HOST_LIMITS`; retry with backoff+jitter on `RETRY_STATUS` (429/5xx/408), honoring
`Retry-After`. `get_json` returns `None` on 404; `post_json` returns `None` on 404/400.
Use as an async context manager.

### `store.py` — SQLite registry + change detection

`SCHEMA` (a module string) is executed on every `Store()` construction — idempotent
`CREATE TABLE IF NOT EXISTS`. Tables: `companies`, `boards`, `jobs`, `queries`.
Board status lifecycle: `candidate → active → dead | error`; `fail_count` increments
on non-active transitions, `DEAD_AFTER_FAILURES = 4` in `ingest.py` buries a board.

`sync_jobs()` is the change-detection core: builds a temp `seen` table of this crawl's
`external_job_id`s, upserts each job (comparing `content_hash` from `normalize.content_hash`
to flag `changed`), then marks any active job **not** in `seen` as `is_active=0` with a
`closed_at`. A job that reappears is reopened. Works even when the ATS gives no reliable
`posted_at`.

`reschedule()`: churn → interval halves (floor 4h); no churn → interval doubles (ceiling 7d).

`meta` table is key/value (`get_meta`/`set_meta`). `cli report` uses `meta.last_report_at`
as a watermark: `search(since=…)` filters on `first_seen_at >= watermark` and
`--advance-watermark` writes `now` back, so a cron/n8n job on any interval never
re-alerts a posting. `--json` emits an array for a downstream notifier node.

`_migrate()` runs after `SCHEMA` on every construction — `CREATE TABLE IF NOT EXISTS`
never alters an existing table, so additive columns (e.g. `jobs.seniority_rank`) land
there via `ALTER TABLE`, with dependent index creation after. WAL is set to
`synchronous=NORMAL` (durable across app crash, no per-commit fsync); `sync_jobs`
upserts via one `executemany`. `dedupe` → `duplicate_board_groups()` + `merge_board()`
collapse boards that collide on `lower(identifier)` (Ashby historically double-registered
`Vultr`/`vultr`).

### `discovery.py` — adaptive best-first frontier

The `Frontier` heap holds query **intentions** (`{ats, location, role}`), not URLs.
`seed_frontier` starts with broad country-level queries. After each query, `expand()`
uses `yield_score` to reprice neighbours: a productive location gets split by role and
its neighbouring cities get queued at a discount; anything below `0.05` yield is dropped.
`build_query` turns an intention into Brave `site:` query strings. Tune this **last** —
its scoring only means something once the `queries` table has yield history.

### `normalize.py` — runs at ingest time, never at search time

`parse_location()` (alias tables `CITY_ALIASES` / `CITY_COUNTRY` / `CITY_REGION` /
`COUNTRY_ALIASES`, India-heavy; bare `"in"` is deliberately absent — it collided with
Indiana, USA). `classify_seniority(title, description, explicit) -> (label, rank)` scans
all `SENIORITY` buckets + a numbered-ladder regex + years-of-experience parsed from the
body, takes the **highest** rank (so "Senior Associate" → senior), and lets a strong
title veto a low YoE figure; `SENIORITY_RANK` maps the six labels to 0..5, which
`search(max_level=…)` / `--max-level` filter on. `guess_level()` is a label-only shim.
`strip_html()`, `to_iso()` (epoch-ms / epoch-s / ISO string → ISO-8601 UTC),
`company_key()` (loose cross-ATS identity — naive, see caveats).

## Caveats to keep in mind

- **`first_seen_at` (from change detection), not `posted_at`, is the reliable recency
  field.** `posted_at` is missing or relative text ("Posted 3 Days Ago") across ATSs;
  `search --days` already falls back to `COALESCE(posted_at, first_seen_at)`.
- **Company identity is unsolved.** `company_key()` is a slug-normalizer; one company
  legitimately has several boards and two companies share names. Plan to attach a domain.
- **CDX vs columnar for Common Crawl.** Paging `index.commoncrawl.org` for a whole
  domain is thousands of requests. `cc-sql` prints a one-shot DuckDB query against the
  Parquet columnar index — prefer it for full sweeps.
- **Search is SQLite `LIKE`.** Moving to Postgres + `tsvector` or FTS5 is a planned
  step; the SQL in `store.py` is kept plain to ease the `asyncpg` port (README has the
  migration notes).
