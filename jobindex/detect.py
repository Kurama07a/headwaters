"""Turn an arbitrary URL into a BoardRef, or None.

This is deliberately strict: a false positive costs a wasted validation probe
and, worse, pollutes the registry with garbage slugs. Reject anything that
doesn't look like a tenant identifier.
"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlsplit, parse_qs

from .models import BoardRef

# Slugs that are actually application routes, not tenants.
RESERVED = {
    "embed", "jobs", "job", "api", "v1", "static", "assets", "search",
    "applications", "apply", "boards", "board", "en-us", "en", "login",
    "sitemap.xml", "robots.txt", "favicon.ico", "signup", "privacy", "terms",
}

SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{1,63}$")
LOCALE_RE = re.compile(r"^[a-z]{2}(-[A-Za-z]{2})?$")

GREENHOUSE_HOSTS = {
    "boards.greenhouse.io", "job-boards.greenhouse.io",
    "boards.eu.greenhouse.io", "job-boards.eu.greenhouse.io",
}
LEVER_HOSTS = {"jobs.lever.co", "jobs.eu.lever.co"}
ASHBY_HOSTS = {"jobs.ashbyhq.com"}
SMARTRECRUITERS_HOSTS = {"jobs.smartrecruiters.com", "careers.smartrecruiters.com"}
WORKABLE_HOSTS = {"apply.workable.com"}
WORKDAY_HOST_RE = re.compile(
    r"^(?P<tenant>[a-z0-9][a-z0-9-]*)\.(?P<dc>wd\d+)\.myworkdayjobs\.com$", re.I
)
# {slug}.recruitee.com , {slug}.breezy.hr , {slug}.keka.com — subdomain is the tenant
RECRUITEE_HOST_RE = re.compile(r"^(?P<slug>[a-z0-9][a-z0-9-]*)\.recruitee\.com$", re.I)
BREEZY_HOST_RE = re.compile(r"^(?P<slug>[a-z0-9][a-z0-9-]*)\.breezy\.hr$", re.I)
KEKA_HOST_RE = re.compile(r"^(?P<slug>[a-z0-9][a-z0-9-]*)\.keka\.com$", re.I)
# Oracle Recruiting Cloud: {pod}.fa.{dc}.oraclecloud.com/.../sites/{siteNumber}/...
ORACLE_HOST_RE = re.compile(r"\.oraclecloud\.com$", re.I)


def _segments(path: str) -> list[str]:
    return [s for s in path.split("/") if s]


def _valid_slug(s: str) -> bool:
    return bool(s) and s.lower() not in RESERVED and bool(SLUG_RE.match(s))


def detect(url: str) -> Optional[BoardRef]:
    """Best-effort ATS detection. Returns None when the URL isn't a known board."""
    try:
        parts = urlsplit(url if "//" in url else "https://" + url)
    except ValueError:
        return None

    host = parts.netloc.lower().split(":")[0]
    host = host[4:] if host.startswith("www.") else host
    segs = _segments(parts.path)

    # --- Greenhouse -------------------------------------------------------
    if host in GREENHOUSE_HOSTS:
        # https://boards.greenhouse.io/embed/job_board?for=slug
        if segs and segs[0] == "embed":
            for_slug = parse_qs(parts.query).get("for", [None])[0]
            if for_slug and _valid_slug(for_slug):
                return BoardRef("greenhouse", for_slug.lower(), url)
            return None
        if segs and _valid_slug(segs[0]):
            return BoardRef("greenhouse", segs[0].lower(), url)
        return None

    # --- Lever ------------------------------------------------------------
    if host in LEVER_HOSTS:
        if segs and _valid_slug(segs[0]):
            return BoardRef("lever", segs[0].lower(), url)
        return None

    # --- Ashby ------------------------------------------------------------
    if host in ASHBY_HOSTS:
        if segs and _valid_slug(segs[0]):
            # The Ashby posting API is case-insensitive on the slug and returns
            # identical data for jobs.ashbyhq.com/Vultr and /vultr. Lowercase so
            # the two don't become two boards with every posting duplicated.
            return BoardRef("ashby", segs[0].lower(), url)
        return None

    # --- SmartRecruiters --------------------------------------------------
    if host in SMARTRECRUITERS_HOSTS:
        if segs and _valid_slug(segs[0]):
            return BoardRef("smartrecruiters", segs[0], url)
        return None

    # --- Workable -------------------------------------------------------
    # https://apply.workable.com/{slug}/  (or /j/{shortcode} for one job)
    if host in WORKABLE_HOSTS:
        if segs and segs[0] not in ("j", "job") and _valid_slug(segs[0]):
            return BoardRef("workable", segs[0].lower(), url)
        return None

    # --- Recruitee / Breezy (tenant is the subdomain) -------------------
    m = RECRUITEE_HOST_RE.match(host)
    if m and _valid_slug(m.group("slug")):
        return BoardRef("recruitee", m.group("slug").lower(), url)
    m = BREEZY_HOST_RE.match(host)
    if m and _valid_slug(m.group("slug")):
        return BoardRef("breezy", m.group("slug").lower(), url)

    # --- Keka (only the /careers portal, not the HR app login) ---------
    m = KEKA_HOST_RE.match(host)
    if m and segs and segs[0] == "careers" and _valid_slug(m.group("slug")):
        return BoardRef("keka", m.group("slug").lower(), url)

    # --- Oracle Recruiting Cloud -------------------------------------
    # .../CandidateExperience/<lang>/sites/<siteNumber>/[requisitions|job/<id>]
    if ORACLE_HOST_RE.search(host) and "sites" in segs:
        i = segs.index("sites")
        site = segs[i + 1] if i + 1 < len(segs) else None
        if site and _valid_slug(site):
            return BoardRef("oracle", f"{host}|{site}", url)
        return None

    # --- Workday ----------------------------------------------------------
    m = WORKDAY_HOST_RE.match(host)
    if m:
        tenant = m.group("tenant").lower()
        dc = m.group("dc").lower()
        # Path shapes: /Site/..., /en-US/Site/..., /wday/cxs/tenant/Site/jobs
        rest = segs
        if len(rest) >= 3 and rest[0] == "wday" and rest[1] == "cxs":
            site = rest[3] if len(rest) > 3 else None
        else:
            if rest and LOCALE_RE.match(rest[0]):
                rest = rest[1:]
            site = rest[0] if rest else None
        if site and _valid_slug(site):
            return BoardRef("workday", f"{tenant}|{dc}|{site}", url)
        return None

    return None


def detect_many(urls) -> dict[tuple[str, str], BoardRef]:
    """Collapse a stream of URLs down to unique boards. This is the whole point:
    500 lever.co job URLs from one company become one BoardRef."""
    out: dict[tuple[str, str], BoardRef] = {}
    for u in urls:
        ref = detect(u)
        if ref and ref.key not in out:
            out[ref.key] = ref
    return out
