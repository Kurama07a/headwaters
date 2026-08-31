"""Location and field normalization.

Run this at ingest time, never at search time. The whole reason we pull from
ATS APIs is that we control normalization instead of inheriting a search
engine's idea of what "Bangalore" means.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from html import unescape as _unescape
from typing import Any, Optional

CITY_ALIASES = {
    "bangalore": "bengaluru", "bangaluru": "bengaluru", "blr": "bengaluru",
    "bengaluru urban": "bengaluru",
    "gurgaon": "gurugram", "gurgaon haryana": "gurugram",
    "bombay": "mumbai", "navi mumbai": "mumbai", "thane": "mumbai",
    "calcutta": "kolkata", "madras": "chennai",
    "new delhi": "delhi", "delhi ncr": "delhi", "ncr": "delhi",
    "noida": "noida", "gaziabad": "ghaziabad",
    "hyderabad telangana": "hyderabad", "secunderabad": "hyderabad",
    "trivandrum": "thiruvananthapuram",
    "pune maharashtra": "pune",
    "sf": "san francisco", "sfo": "san francisco",
    "nyc": "new york", "new york city": "new york",
    "bay area": "san francisco", "blore": "bengaluru",
}

CITY_COUNTRY = {
    c: "IN" for c in [
        "bengaluru", "hyderabad", "pune", "mumbai", "delhi", "gurugram",
        "noida", "chennai", "kolkata", "ahmedabad", "jaipur", "indore",
        "thiruvananthapuram", "kochi", "coimbatore", "chandigarh", "lucknow",
    ]
}
CITY_REGION = {
    "bengaluru": "Karnataka", "hyderabad": "Telangana", "pune": "Maharashtra",
    "mumbai": "Maharashtra", "delhi": "Delhi", "gurugram": "Haryana",
    "noida": "Uttar Pradesh", "chennai": "Tamil Nadu",
    "kolkata": "West Bengal", "lucknow": "Uttar Pradesh",
}

COUNTRY_ALIASES = {
    "india": "IN", "bharat": "IN",
    # NB: bare "in" is deliberately absent — it collides with Indiana, USA
    # ("Indianapolis, IN"). India still resolves via "india"/"bharat" and via
    # CITY_COUNTRY for the metros.
    "united states": "US", "united states of america": "US", "usa": "US",
    "u.s.": "US", "us": "US", "u.s.a.": "US",
    "united kingdom": "GB", "uk": "GB", "england": "GB",
    "singapore": "SG", "germany": "DE", "canada": "CA",
    "netherlands": "NL", "ireland": "IE", "australia": "AU",
}

REMOTE_RE = re.compile(r"\b(remote|work from home|wfh|anywhere|distributed)\b", re.I)
HYBRID_RE = re.compile(r"\b(hybrid|flex(ible)? (work|location))\b", re.I)
ONSITE_RE = re.compile(r"\b(on[- ]?site|in[- ]?office|in[- ]?person)\b", re.I)

SPLIT_RE = re.compile(r"\s*[,/|·•>–—-]\s*|\s+\bor\b\s+|\s*;\s*")
NOISE = {
    "", "n/a", "na", "multiple locations", "various", "various locations",
    "global", "worldwide", "any", "anywhere", "flexible", "hq", "office",
    "headquarters", "remote", "onsite", "on-site", "on site", "hybrid",
    "unspecified", "tbd", "location",
}

# Ordered low -> high. classify_seniority() scans ALL buckets and takes the
# highest rank that matches, so "Senior Associate" resolves to senior, not entry.
SENIORITY_RANK = {"intern": 0, "entry": 1, "mid": 2, "senior": 3,
                  "staff": 4, "manager": 5}
_RANK_LABEL = {v: k for k, v in SENIORITY_RANK.items()}

SENIORITY = [
    ("intern", [" intern", "internship", " co-op", " co op", "apprentice",
                "trainee", "working student", "industrial placement", "praktikum"]),
    ("entry", ["entry level", "entry-level", "new grad", "new-grad", "newgrad",
               "recent grad", "graduate ", " grad ", "campus", "university grad",
               "early career", "early-career", "fresher", "0-1 year", "0-2 year",
               "1-2 year", "associate engineer", "associate software",
               "junior", " jr ", " jr.", "level 1", " l1 "]),
    ("mid", ["mid level", "mid-level", "intermediate", " l2 ", "level 2"]),
    ("senior", ["senior", " sr ", " sr.", "snr ", " lead ", "lead ", "team lead",
                "tech lead", "staff-ish", " l3 ", "level 3"]),
    ("staff", ["staff ", " staff", "principal", "architect", "distinguished",
               "fellow", "member of technical staff"]),
    ("manager", ["manager", " mgr", "director", "head of", " vp ", "vp,",
                 "vice president", "chief "]),
]

# "Engineer II", "SDE 3", "Analyst, IV" -> a numbered-ladder rank.
LEVEL_RE = re.compile(
    r"\b(?:swe|sde|engineer|developer|analyst|designer|scientist|specialist"
    r"|consultant|architect)\s*[-,]?\s*(i{1,3}|iv|[1-4])\b", re.I)
_ROMAN = {"i": 1, "ii": 2, "iii": 3, "iv": 4}

# Years-of-experience in a job description ("3+ years", "minimum of 5 years").
_YOE_PATTERNS = [
    re.compile(r"(\d{1,2})\s*\+?\s*(?:to|[-–])?\s*\d{0,2}\s*(?:years?|yrs?)"
               r"[^.]{0,40}?experien", re.I),
    re.compile(r"experien[^.]{0,40}?(\d{1,2})\s*\+?\s*(?:years?|yrs?)", re.I),
    re.compile(r"minimum\s+(?:of\s+)?(\d{1,2})\s*\+?\s*(?:years?|yrs?)", re.I),
]

# Free-text seniority labels some ATSs / feeds hand us verbatim.
_EXPLICIT_MAP = {
    "internship": 0, "intern": 0, "entry level": 1, "entry-level": 1,
    "associate": 1, "junior": 1, "mid": 2, "mid-level": 2, "mid level": 2,
    "mid-senior level": 3, "mid senior level": 3, "senior": 3, "lead": 3,
    "staff": 4, "principal": 4, "manager": 5, "director": 5, "executive": 5,
}


def canon_city(raw: str) -> str:
    key = re.sub(r"[^a-z ]", "", raw.strip().lower())
    key = re.sub(r"\s+", " ", key).strip()
    key = re.sub(r"^(remote|hybrid|onsite)\s+", "", key)
    key = re.sub(r"\s+(hq|headquarters|head office|office|area|region|metro)$", "", key)
    return CITY_ALIASES.get(key, key)


def parse_location(raw: Optional[str], fallback_remote: Optional[bool] = None) -> dict:
    """'Hybrid - Bangalore, Karnataka' -> city/region/country/remote_type."""
    out = {"location_raw": raw, "city": None, "region": None,
           "country": None, "remote_type": None}
    if not raw:
        if fallback_remote:
            out["remote_type"] = "remote"
        return out

    text = raw.strip()
    if HYBRID_RE.search(text):
        out["remote_type"] = "hybrid"
    elif REMOTE_RE.search(text):
        out["remote_type"] = "remote"
    elif ONSITE_RE.search(text):
        out["remote_type"] = "onsite"
    elif fallback_remote is True:
        out["remote_type"] = "remote"
    elif fallback_remote is False:
        out["remote_type"] = "onsite"

    tokens = [t.strip() for t in SPLIT_RE.split(text) if t.strip()]
    tokens = [t for t in tokens
              if t.lower() not in NOISE
              and not REMOTE_RE.fullmatch(t)
              and not HYBRID_RE.fullmatch(t)
              and not ONSITE_RE.fullmatch(t)]
    if not tokens:
        return out

    # Country is usually last.
    tail = tokens[-1].lower().strip(".")
    if tail in COUNTRY_ALIASES:
        out["country"] = COUNTRY_ALIASES[tail]
        tokens = tokens[:-1]

    if tokens:
        city = canon_city(tokens[0])
        if city and city not in NOISE:
            out["city"] = city.title()
            out["region"] = CITY_REGION.get(city)
            if not out["country"]:
                out["country"] = CITY_COUNTRY.get(city)
        if len(tokens) > 1 and not out["region"]:
            out["region"] = tokens[1]

    if not out["country"] and REMOTE_RE.search(text) and "india" in text.lower():
        out["country"] = "IN"
    return out


def min_years_required(text: Optional[str]) -> Optional[int]:
    """Smallest 'N years experience' figure stated in a job description, or None."""
    if not text:
        return None
    hay = text[:4000]
    found = [int(m.group(1)) for pat in _YOE_PATTERNS for m in pat.finditer(hay)]
    found = [n for n in found if 0 <= n <= 15]
    return min(found) if found else None


def _years_to_rank(years: Optional[int]) -> Optional[int]:
    if years is None:
        return None
    if years <= 2:
        return SENIORITY_RANK["entry"]
    if years <= 5:
        return SENIORITY_RANK["mid"]
    if years <= 8:
        return SENIORITY_RANK["senior"]
    return SENIORITY_RANK["staff"]


def _title_rank(title: str, explicit: Optional[str] = None) -> Optional[int]:
    ranks: list[int] = []
    if explicit:
        e = re.sub(r"\s+", " ", explicit.strip().lower())
        if e in SENIORITY_RANK:
            ranks.append(SENIORITY_RANK[e])
        elif e in _EXPLICIT_MAP:
            ranks.append(_EXPLICIT_MAP[e])
    t = f" {(title or '').lower()} "
    for level, needles in SENIORITY:
        if any(n in t for n in needles):
            ranks.append(SENIORITY_RANK[level])
    m = LEVEL_RE.search(title or "")
    if m:
        tok = m.group(1).lower()
        n = _ROMAN.get(tok) or (int(tok) if tok.isdigit() else None)
        if n:
            ranks.append(min(n, 4))  # I->entry, II->mid, III->senior, IV->staff
    return max(ranks) if ranks else None


def classify_seniority(title: str, description: Optional[str] = None,
                       explicit: Optional[str] = None) -> tuple[Optional[str], Optional[int]]:
    """(label, rank) from title + description. Title wins when it carries a
    strong signal; a bare 'Engineer II' defers to the years stated in the body."""
    title_rank = _title_rank(title, explicit)
    years_rank = _years_to_rank(min_years_required(description))
    if title_rank is not None and title_rank != SENIORITY_RANK["mid"]:
        rank = title_rank
    elif years_rank is not None:
        rank = years_rank
    else:
        rank = title_rank
    if rank is None:
        return None, None
    return _RANK_LABEL[rank], rank


def guess_level(title: str, explicit: Optional[str] = None) -> Optional[str]:
    """Back-compat shim — label only. Prefer classify_seniority()."""
    return classify_seniority(title, None, explicit)[0]


def strip_html(html: Optional[str], limit: int = 20000) -> Optional[str]:
    if not html:
        return None
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<br\s*/?>|</p>|</li>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = _unescape(text)  # &nbsp; &amp; &#x2F; &#39; ... (feeds like HN use hex entities)
    text = text.replace("\xa0", " ").replace("​", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:limit] or None


def to_iso(value: Any) -> Optional[str]:
    """Accept epoch ms, epoch s, or an ISO-ish string; emit ISO-8601 UTC."""
    if value in (None, "", 0):
        return None
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1e11:  # milliseconds
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        s = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def company_key(name: str) -> str:
    """Loose company identity key for cross-ATS dedup."""
    n = name.lower()
    n = re.sub(r"\b(inc|llc|ltd|limited|corp|corporation|gmbh|pvt|private|"
               r"technologies|technology|labs|software|solutions|systems)\b", " ", n)
    n = re.sub(r"[^a-z0-9]+", "", n)
    return n or name.lower()


def content_hash(job) -> str:
    parts = [job.title or "", job.location_raw or "", job.department or "",
             job.employment_type or "", (job.description or "")[:4000]]
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:32]


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()
