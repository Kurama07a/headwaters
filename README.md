# jobindex — ATS board discovery & job indexing

A working skeleton of the spec. The load-bearing idea: **the board is the unit,
not the URL.** One search result reveals one board; one board yields thousands
of jobs over its life.

```
discovery (search engines)  ──►  board registry  ──►  ingestion (ATS APIs)
        rare, expensive                                 constant, cheap
```

## Layout

| file | role |
|---|---|
| `models.py` | `BoardRef`, `Job`, `QueryResult` |
| `http.py` | per-host rate limiting, 429 handling, backoff + jitter |
| `detect.py` | URL → `BoardRef`, strictly |
| `adapters.py` | Greenhouse, Lever, Ashby, SmartRecruiters, Workday |
| `normalize.py` | location aliases, remote type, seniority, hashing |
| `store.py` | SQLite registry + change detection |
| `sources.py` | Brave API, Common Crawl CDX, columnar-index SQL |
| `discovery.py` | best-first frontier with yield-based expansion |
| `ingest.py` | fetch due boards, diff, reschedule |
| `cli.py` | commands |

## Quickstart

```bash
pip install httpx
python test_offline.py            # no network needed

# bootstrap from Common Crawl (free, bulk, a few weeks stale)
python -m jobindex.cli crawl-cc --ats lever ashby --pages 2

# or import any URL list you already have
python -m jobindex.cli seed urls.txt

# long tail / freshness (needs BRAVE_API_KEY)
python -m jobindex.cli discover --budget 25

# then this is the loop you actually run forever
python -m jobindex.cli ingest --limit 200

python -m jobindex.cli search "backend" --country IN --days 7 --urls
python -m jobindex.cli stats
```

## Build order

1. **Adapters + store + ingest.** Hand-seed 20 boards you know exist. Get to
   "I have 5,000 real, normalized, deduped jobs in SQLite" before writing a
   single line of discovery code.
2. **Common Crawl bootstrap.** One pass gets you thousands of boards.
3. **Validation loop.** Promote/bury candidates. Now you have a registry.
4. **Brave discovery.** Only for what CC missed and for new companies.
5. **Scheduling + change detection.** Already wired; tune the intervals.
6. **Search layer.** Move off SQLite `LIKE` to Postgres + `tsvector`, or FTS5.

The priority frontier in `discovery.py` is the last thing to tune, not the
first. Its scoring only means something once you have query-yield history in
the `queries` table.

## Things worth knowing before you run this

- **Field mappings are unverified against live traffic.** I couldn't reach the
  ATS hosts from this sandbox. Fetch one real response per ATS, diff it against
  `adapters.py`, and fix before trusting counts. Greenhouse `content=true`,
  Lever `descriptionPlain`, and the SmartRecruiters posting-URL shape are the
  three most likely to need a tweak.
- **CDX vs columnar.** Paging `index.commoncrawl.org` for
  `matchType=domain&url=myworkdayjobs.com` is thousands of requests. For a full
  sweep use the Parquet columnar index instead — `python -m jobindex.cli cc-sql`
  prints a DuckDB query that pulls every ATS URL in a crawl in one shot.
- **Slug probing beats searching.** For Greenhouse/Lever/Ashby the slug is
  usually the company name, and validation is a single cheap GET. Running a
  company-name list (Crunchbase, YC, a funding dataset) through
  `validate_and_store()` will out-discover Brave per rupee spent, by a lot.
  This is the highest-leverage source the spec doesn't mention.
- **Workday is quarantined.** `stable = False`, its own failure path, no
  officially supported contract. Give it a separate worker in production so a
  tenant that changes its CXS shape can't stall Greenhouse ingest.
- **`posted_at` is unreliable** across ATSs (Workday gives relative text like
  "Posted 3 Days Ago"). `first_seen_at` from change detection is the field you
  should actually rank and filter on.
- **Company identity is harder than board identity.** `company_key()` is a
  naive slug-normalizer. One company legitimately has several boards
  (acquisitions, regions, contractor pipelines), and two companies share a name.
  Plan to attach a domain and dedup on that.
- Check each platform's terms and `robots.txt` for your jurisdiction and use
  case. The endpoints are public, but "public" and "licensed for redistribution"
  aren't the same thing, and that distinction matters more the moment this
  becomes a product.

## Postgres migration

The SQL is deliberately plain. Swap `sqlite3` for `asyncpg`, change
`INSERT OR IGNORE` → `ON CONFLICT DO NOTHING`, move the temp table in
`sync_jobs` to an `UNNEST($1::text[])`, and add
`ALTER TABLE jobs ADD COLUMN tsv tsvector GENERATED ALWAYS AS
(to_tsvector('english', title || ' ' || coalesce(description,''))) STORED;`
