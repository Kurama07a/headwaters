"""Discovery sources. These produce URLs; `detect.py` turns them into boards.

Two very different cost profiles:

* Common Crawl is bulk and free but stale (weeks to months old). Use it once,
  hard, to bootstrap thousands of boards.
* Brave is fresh, paid, and rate-limited. Use it for the long tail and for
  companies that appeared after the last crawl.

For a full sweep of a domain across a whole crawl, prefer the columnar index
over CDX — see README, `cc_columnar_sql()` prints the query.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import AsyncIterator, Optional
from urllib.parse import quote

from .http import Client, FetchError

log = logging.getLogger(__name__)

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
CC_INDEX = "https://index.commoncrawl.org"

ATS_HOSTS = {
    "greenhouse": ["job-boards.greenhouse.io", "boards.greenhouse.io"],
    "lever": ["jobs.lever.co"],
    "ashby": ["jobs.ashbyhq.com"],
    "smartrecruiters": ["jobs.smartrecruiters.com"],
    "workday": ["myworkdayjobs.com"],
    "workable": ["apply.workable.com"],
    "recruitee": ["recruitee.com"],
    "breezy": ["breezy.hr"],
    "keka": ["keka.com"],
    "oracle": ["oraclecloud.com"],
}


# --------------------------------------------------------------------------
async def brave_search(client: Client, query: str, api_key: str,
                       count: int = 20, pages: int = 1,
                       country: Optional[str] = None) -> list[str]:
    """Run one Brave query and return result URLs."""
    headers = {"X-Subscription-Token": api_key, "Accept": "application/json"}
    urls: list[str] = []
    for page in range(pages):
        params = {"q": query, "count": count, "offset": page}
        if country:
            params["country"] = country
        try:
            data = await client.get_json(BRAVE_ENDPOINT, params=params, headers=headers)
        except FetchError as exc:
            log.warning("brave query failed %r: %s", query, exc)
            break
        results = ((data or {}).get("web") or {}).get("results") or []
        urls += [r["url"] for r in results if r.get("url")]
        if len(results) < count:
            break
    return urls


def brave_queries(ats: str, term: str) -> list[str]:
    return [f'site:{host} "{term}"' for host in ATS_HOSTS[ats]]


# --------------------------------------------------------------------------
_SLUG_STRIP = re.compile(
    r"\b(inc|llc|ltd|limited|corp|corporation|gmbh|pvt|private|technologies|"
    r"technology|labs|software|solutions|systems|the|group|holdings|co)\b", re.I)


def company_slug_candidates(name: str) -> list[str]:
    """Company name -> a few plausible ATS tenant slugs, most-likely first.
    The highest-leverage discovery source: run a funded-company list through
    these against `validate_and_store` — validation is one cheap GET."""
    n = _SLUG_STRIP.sub(" ", name.strip().lower())
    words = [w for w in re.split(r"[^a-z0-9]+", n) if w]
    if not words:
        return []
    joined = "".join(words)
    out = [joined, "-".join(words)]
    if len(words) > 1:
        out.append(words[0])                       # first word alone
        out.append("".join(words[:2]))             # first two, squashed
    seen, uniq = set(), []
    for s in out:
        if 2 <= len(s) <= 63 and s not in seen:
            seen.add(s); uniq.append(s)
    return uniq


YC_OSS = "https://yc-oss.github.io/api/companies"  # daily-refreshed YC directory


async def yc_companies(client: Client, *, hiring_only: bool = False,
                       location: Optional[str] = None) -> list[dict]:
    """YC company directory (name/slug/website/locations), optionally filtered to
    those currently hiring and/or a location substring ('India', 'Bengaluru').
    A free, maintained stand-in for a Tracxn/Crunchbase feed — pipe the names
    through `company_slug_candidates` + `validate_and_store`."""
    data = await client.get_json(f"{YC_OSS}/{'hiring' if hiring_only else 'all'}.json")
    rows = data if isinstance(data, list) else []
    if location:
        loc = location.lower()
        rows = [r for r in rows if loc in (r.get("all_locations") or "").lower()]
    return [{"name": r["name"], "slug": r.get("slug"),
             "website": r.get("website"), "locations": r.get("all_locations")}
            for r in rows if r.get("name")]


CERTSPOTTER = "https://api.certspotter.com/v1/issuances"


async def certspotter_hostnames(client: Client, domain: str) -> list[str]:
    """Subdomains of `domain` seen in Certificate Transparency logs. The scalable
    way to enumerate Workday / Oracle / enterprise-ATS tenants (crt.sh is a 502
    machine). Free tier: ~100 newest issuances per call, unauthenticated, rate
    limited — paginate with `after=<id>` if you need the full history."""
    try:
        data = await client.get_json(
            f"{CERTSPOTTER}?domain={quote(domain)}&include_subdomains=true"
            f"&expand=dns_names&match_wildcards=false")
    except FetchError as exc:
        log.warning("certspotter %s failed: %s", domain, exc)
        return []
    hosts: set[str] = set()
    for issuance in (data or []):
        for name in issuance.get("dns_names", []):
            if name.startswith("*.") or not name.endswith(domain):
                continue
            hosts.add(name.lower())
    return sorted(hosts)


# --------------------------------------------------------------------------
async def cc_indexes(client: Client, latest: int = 3) -> list[str]:
    """Names of the most recent Common Crawl indexes, newest first."""
    data = await client.get_json(f"{CC_INDEX}/collinfo.json")
    return [c["id"] for c in (data or [])][:latest]


async def cc_urls(client: Client, index: str, domain: str, *,
                  match_type: str = "domain", page_limit: int = 20,
                  page_size: int = 5) -> AsyncIterator[str]:
    """Stream URLs for a domain out of one Common Crawl CDX index.

    `match_type='domain'` includes subdomains, which is what Workday needs
    (`*.myworkdayjobs.com`). Volumes are large — cap `page_limit` while testing.
    """
    base = f"{CC_INDEX}/{index}-index"
    q = f"url={quote(domain)}&output=json&filter=~status:200&pageSize={page_size}"
    meta = await client.get_json(f"{base}?{q}&matchType={match_type}&showNumPages=true")
    pages = min((meta or {}).get("pages", 1), page_limit)
    log.info("common crawl %s %s: %d page(s)", index, domain, pages)

    for page in range(pages):
        try:
            resp = await client.request(
                "GET", f"{base}?{q}&matchType={match_type}&page={page}")
        except FetchError as exc:
            log.warning("cc page %d failed: %s", page, exc)
            continue
        if resp.status_code != 200:
            continue
        for line in resp.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                import json
                yield json.loads(line)["url"]
            except (ValueError, KeyError):
                continue
        await asyncio.sleep(0)


def cc_columnar_sql(crawl: str = "CC-MAIN-2025-05") -> str:
    """The scalable path: query the Parquet columnar index with DuckDB.

    One query gets every ATS host in a crawl without paging CDX thousands of
    times. Run locally with duckdb + httpfs; costs bandwidth, not API quota.
    """
    return f"""
INSTALL httpfs; LOAD httpfs;
SET s3_region='us-east-1'; SET s3_access_key_id=''; SET s3_secret_access_key='';

COPY (
  SELECT DISTINCT url
  FROM read_parquet(
    's3://commoncrawl/cc-index/table/cc-main/warc/crawl={crawl}/subset=warc/*.parquet'
  )
  WHERE url_host_registered_domain IN
        ('lever.co','greenhouse.io','ashbyhq.com','smartrecruiters.com','myworkdayjobs.com')
    AND fetch_status = 200
) TO 'ats_urls.parquet' (FORMAT PARQUET);
""".strip()
