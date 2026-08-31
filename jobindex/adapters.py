"""One adapter per ATS. Everything above this layer is ATS-agnostic.

Contract:
    api_url(ref)      -> canonical endpoint, stored on the board row
    validate(client, ref) -> bool, cheap probe before we commit a board
    fetch_jobs(client, ref) -> list[Job], already normalized

Field mappings below follow each platform's documented/observed response shape.
Verify them against one live response per ATS before trusting counts — these
payloads drift, and Workday in particular is a frontend contract, not an API.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from .http import Client, FetchError
from .models import BoardRef, Job
from .normalize import classify_seniority, parse_location, strip_html, to_iso

log = logging.getLogger(__name__)


class ATSAdapter:
    name: str = ""
    #: Workday is a best-effort frontend integration; keep it quarantined so
    #: its failure modes never take down the rest of the ingest loop.
    stable: bool = True
    #: Per-tenant ATS APIs return the tenant's *entire* active set, so a job
    #: that vanished is genuinely closed. Aggregator feeds only return the most
    #: recent slice — for those, set False so sync_jobs doesn't close everything
    #: that fell off the recent list.
    closes_missing: bool = True

    def api_url(self, ref: BoardRef) -> str:
        raise NotImplementedError

    def career_url(self, ref: BoardRef) -> str:
        raise NotImplementedError

    async def validate(self, client: Client, ref: BoardRef) -> bool:
        try:
            jobs = await self.fetch_jobs(client, ref, limit=1)
        except (FetchError, Exception) as exc:  # noqa: BLE001 - probe must not raise
            log.debug("validate failed %s: %s", ref, exc)
            return False
        return jobs is not None

    async def fetch_jobs(self, client: Client, ref: BoardRef,
                         limit: Optional[int] = None) -> Optional[list[Job]]:
        raise NotImplementedError


# --------------------------------------------------------------------------
class Greenhouse(ATSAdapter):
    name = "greenhouse"

    def api_url(self, ref: BoardRef) -> str:
        return f"https://boards-api.greenhouse.io/v1/boards/{ref.identifier}/jobs"

    def career_url(self, ref: BoardRef) -> str:
        return f"https://job-boards.greenhouse.io/{ref.identifier}"

    async def fetch_jobs(self, client, ref, limit=None):
        url = self.api_url(ref) + ("" if limit else "?content=true")
        data = await client.get_json(url)
        if not isinstance(data, dict) or "jobs" not in data:
            return None
        out = []
        for j in data["jobs"][:limit] if limit else data["jobs"]:
            offices = [o.get("name") for o in (j.get("offices") or []) if o.get("name")]
            loc = (j.get("location") or {}).get("name") or (offices[0] if offices else None)
            depts = [d.get("name") for d in (j.get("departments") or []) if d.get("name")]
            job = Job(
                external_job_id=str(j.get("id")),
                title=j.get("title") or "",
                department=depts[0] if depts else None,
                team=depts[1] if len(depts) > 1 else None,
                description=strip_html(j.get("content")),
                apply_url=j.get("absolute_url"),
                posted_at=to_iso(j.get("first_published") or j.get("updated_at")),
                raw=j,
            )
            job.__dict__.update(parse_location(loc))
            job.experience_level, job.seniority_rank = classify_seniority(
                job.title, job.description)
            out.append(job)
        return out


# --------------------------------------------------------------------------
class Lever(ATSAdapter):
    name = "lever"

    def api_url(self, ref: BoardRef) -> str:
        return f"https://api.lever.co/v0/postings/{ref.identifier}?mode=json"

    def career_url(self, ref: BoardRef) -> str:
        return f"https://jobs.lever.co/{ref.identifier}"

    async def fetch_jobs(self, client, ref, limit=None):
        data = await client.get_json(self.api_url(ref))
        if not isinstance(data, list):
            return None
        out = []
        for p in data[:limit] if limit else data:
            cats = p.get("categories") or {}
            workplace = (p.get("workplaceType") or "").lower() or None
            job = Job(
                external_job_id=str(p.get("id")),
                title=p.get("text") or "",
                department=cats.get("department") or cats.get("team"),
                team=cats.get("team"),
                employment_type=cats.get("commitment"),
                description=p.get("descriptionPlain") or strip_html(p.get("description")),
                apply_url=p.get("hostedUrl") or p.get("applyUrl"),
                posted_at=to_iso(p.get("createdAt")),
                raw=p,
            )
            job.__dict__.update(parse_location(
                cats.get("location") or cats.get("allLocations", [None])[0]))
            if workplace in ("remote", "hybrid", "onsite", "on-site"):
                job.remote_type = "onsite" if workplace == "on-site" else workplace
            job.experience_level, job.seniority_rank = classify_seniority(
                job.title, job.description)
            out.append(job)
        return out


# --------------------------------------------------------------------------
class Ashby(ATSAdapter):
    name = "ashby"

    def api_url(self, ref: BoardRef) -> str:
        return (f"https://api.ashbyhq.com/posting-api/job-board/"
                f"{ref.identifier}?includeCompensation=true")

    def career_url(self, ref: BoardRef) -> str:
        return f"https://jobs.ashbyhq.com/{ref.identifier}"

    async def fetch_jobs(self, client, ref, limit=None):
        data = await client.get_json(self.api_url(ref))
        if not isinstance(data, dict) or "jobs" not in data:
            return None
        out = []
        for j in data["jobs"][:limit] if limit else data["jobs"]:
            job = Job(
                external_job_id=str(j.get("id")),
                title=j.get("title") or "",
                department=j.get("department"),
                team=j.get("team"),
                employment_type=j.get("employmentType"),
                description=j.get("descriptionPlain") or strip_html(j.get("descriptionHtml")),
                apply_url=j.get("applyUrl") or j.get("jobUrl"),
                posted_at=to_iso(j.get("publishedAt")),
                raw=j,
            )
            job.__dict__.update(parse_location(j.get("location"),
                                               fallback_remote=j.get("isRemote")))
            comp = (j.get("compensation") or {}).get("summaryComponents") or []
            for c in comp:
                if c.get("compensationType") == "Salary":
                    job.salary_min = c.get("minValue")
                    job.salary_max = c.get("maxValue")
                    job.salary_currency = c.get("currencyCode")
                    break
            job.experience_level, job.seniority_rank = classify_seniority(
                job.title, job.description)
            out.append(job)
        return out


# --------------------------------------------------------------------------
class SmartRecruiters(ATSAdapter):
    name = "smartrecruiters"
    PAGE = 100

    def api_url(self, ref: BoardRef) -> str:
        return f"https://api.smartrecruiters.com/v1/companies/{ref.identifier}/postings"

    def career_url(self, ref: BoardRef) -> str:
        return f"https://jobs.smartrecruiters.com/{ref.identifier}"

    async def fetch_jobs(self, client, ref, limit=None):
        base = self.api_url(ref)
        out: list[Job] = []
        offset, total = 0, None
        page = min(self.PAGE, limit) if limit else self.PAGE
        while True:
            data = await client.get_json(f"{base}?limit={page}&offset={offset}")
            if not isinstance(data, dict) or "content" not in data:
                return out or None
            total = data.get("totalFound", 0)
            for p in data["content"]:
                loc = p.get("location") or {}
                raw_loc = ", ".join(
                    x for x in (loc.get("city"), loc.get("region"), loc.get("country")) if x
                )
                job = Job(
                    external_job_id=str(p.get("id") or p.get("uuid")),
                    title=p.get("name") or "",
                    department=(p.get("department") or {}).get("label"),
                    employment_type=(p.get("typeOfEmployment") or {}).get("label"),
                    experience_level=(p.get("experienceLevel") or {}).get("label"),
                    apply_url=f"{self.career_url(ref)}/{p.get('id')}",
                    posted_at=to_iso(p.get("releasedDate") or p.get("createdOn")),
                    raw=p,
                )
                job.__dict__.update(parse_location(raw_loc or None,
                                                   fallback_remote=loc.get("remote")))
                if (loc.get("country") or "").lower() in ("india", "in"):
                    job.country = "IN"
                job.experience_level, job.seniority_rank = classify_seniority(
                    job.title, job.description, explicit=job.experience_level)
                out.append(job)
            offset += page
            if limit and len(out) >= limit:
                return out[:limit]
            if total is None or offset >= total or not data["content"]:
                return out


# --------------------------------------------------------------------------
class Workday(ATSAdapter):
    """Best-effort integration with the Workday careers frontend (CXS).

    Not an officially supported public developer API. Expect breakage; the
    ingest loop treats `stable = False` adapters as allowed to fail.
    """
    name = "workday"
    stable = False
    PAGE = 20  # CXS rejects larger limits on many tenants

    def _base(self, ref: BoardRef) -> str:
        tenant, dc, site = ref.workday_parts
        return f"https://{tenant}.{dc}.myworkdayjobs.com"

    def api_url(self, ref: BoardRef) -> str:
        tenant, _, site = ref.workday_parts
        return f"{self._base(ref)}/wday/cxs/{tenant}/{site}/jobs"

    def career_url(self, ref: BoardRef) -> str:
        _, _, site = ref.workday_parts
        return f"{self._base(ref)}/{site}"

    async def fetch_jobs(self, client, ref, limit=None):
        url = self.api_url(ref)
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        out: list[Job] = []
        offset = 0
        while True:
            payload = {"appliedFacets": {}, "limit": self.PAGE,
                       "offset": offset, "searchText": ""}
            data = await client.post_json(url, json=payload, headers=headers)
            if not isinstance(data, dict) or "jobPostings" not in data:
                return out or None
            postings = data["jobPostings"]
            total = data.get("total", 0)
            for p in postings:
                ext = p.get("externalPath") or ""
                job = Job(
                    external_job_id=ext.rsplit("/", 1)[-1] or ext,
                    title=p.get("title") or "",
                    apply_url=self._base(ref) + ext if ext else None,
                    posted_at=None,  # `postedOn` is relative text like "Posted 3 Days Ago"
                    raw=p,
                )
                job.__dict__.update(parse_location(p.get("locationsText")))
                job.experience_level, job.seniority_rank = classify_seniority(
                    job.title, job.description)
                out.append(job)
            offset += self.PAGE
            if limit and len(out) >= limit:
                return out[:limit]
            if not postings or offset >= total:
                return out


# --------------------------------------------------------------------------
class Workable(ATSAdapter):
    """Public job-board widget endpoint (no SPI token needed). Returns the
    tenant's whole published set in one response."""
    name = "workable"

    def api_url(self, ref: BoardRef) -> str:
        return (f"https://apply.workable.com/api/v1/widget/accounts/"
                f"{ref.identifier}?details=true")

    def career_url(self, ref: BoardRef) -> str:
        return f"https://apply.workable.com/{ref.identifier}/"

    async def fetch_jobs(self, client, ref, limit=None):
        data = await client.get_json(self.api_url(ref))
        if not isinstance(data, dict) or "jobs" not in data:
            return None
        account = data.get("name")
        out = []
        for j in data["jobs"][:limit] if limit else data["jobs"]:
            loc = ", ".join(x for x in (j.get("city"), j.get("state"),
                                        j.get("country")) if x)
            job = Job(
                external_job_id=str(j.get("shortcode") or j.get("id")),
                title=j.get("title") or "",
                company=account,
                department=j.get("department"),
                employment_type=j.get("employment_type"),
                description=strip_html(j.get("description")),
                apply_url=j.get("application_url") or j.get("url"),
                posted_at=to_iso(j.get("published_on") or j.get("created_at")),
                raw=j,
            )
            job.__dict__.update(parse_location(loc or None,
                                               fallback_remote=j.get("telecommuting")))
            job.experience_level, job.seniority_rank = classify_seniority(
                job.title, job.description, explicit=j.get("experience"))
            out.append(job)
        return out


# --------------------------------------------------------------------------
class Recruitee(ATSAdapter):
    name = "recruitee"

    def api_url(self, ref: BoardRef) -> str:
        return f"https://{ref.identifier}.recruitee.com/api/offers/"

    def career_url(self, ref: BoardRef) -> str:
        return f"https://{ref.identifier}.recruitee.com/"

    async def fetch_jobs(self, client, ref, limit=None):
        data = await client.get_json(self.api_url(ref))
        if not isinstance(data, dict) or "offers" not in data:
            return None
        offers = data["offers"]
        out = []
        for o in offers[:limit] if limit else offers:
            desc = strip_html(o.get("description"))
            reqs = strip_html(o.get("requirements"))
            if desc and reqs:
                desc = f"{desc}\n\n{reqs}"
            job = Job(
                external_job_id=str(o.get("id")),
                title=o.get("title") or o.get("sharing_title") or "",
                company=o.get("company_name"),
                department=o.get("department"),
                employment_type=o.get("employment_type_code") or o.get("category_code"),
                description=desc,
                apply_url=o.get("careers_apply_url") or o.get("careers_url"),
                posted_at=to_iso((o.get("published_at") or o.get("created_at") or "")
                                 .replace(" UTC", "").replace(" ", "T") or None),
                raw=o,
            )
            job.__dict__.update(parse_location(
                o.get("location") or None, fallback_remote=o.get("remote")))
            if o.get("hybrid"):
                job.remote_type = "hybrid"
            if not job.country and o.get("country_code"):
                job.country = o["country_code"].upper()
            job.experience_level, job.seniority_rank = classify_seniority(
                job.title, job.description, explicit=(o.get("experience_code") or "")
                .replace("_level", "").replace("student_internship", "intern"))
            out.append(job)
        return out


# --------------------------------------------------------------------------
class Breezy(ATSAdapter):
    """`/json` feed — a summary list; individual descriptions aren't included."""
    name = "breezy"

    def api_url(self, ref: BoardRef) -> str:
        return f"https://{ref.identifier}.breezy.hr/json"

    def career_url(self, ref: BoardRef) -> str:
        return f"https://{ref.identifier}.breezy.hr/"

    async def fetch_jobs(self, client, ref, limit=None):
        data = await client.get_json(self.api_url(ref))
        if not isinstance(data, list):
            return None
        out = []
        for j in data[:limit] if limit else data:
            if not isinstance(j, dict) or not j.get("id"):
                continue
            loc = j.get("location") or {}
            loc_name = loc.get("name") or ", ".join(x for x in (
                loc.get("city"), (loc.get("state") or {}).get("name"),
                (loc.get("country") or {}).get("name")) if x)
            job = Job(
                external_job_id=str(j.get("id")),
                title=j.get("name") or "",
                company=(j.get("company") or {}).get("name"),
                department=j.get("department"),
                employment_type=(j.get("type") or {}).get("name"),
                apply_url=(j.get("url") or "").rstrip("/") + "/apply" if j.get("url") else None,
                posted_at=to_iso(j.get("published_date")),
                raw=j,
            )
            job.__dict__.update(parse_location(
                loc_name or None, fallback_remote=loc.get("is_remote")))
            job.experience_level, job.seniority_rank = classify_seniority(job.title)
            out.append(job)
        return out


# --------------------------------------------------------------------------
class Keka(ATSAdapter):
    """Indian HRMS. Careers portal is a jQuery SPA; the list endpoint is
    `/careers/api/jobs/default/active` ('default' is the portal slug)."""
    name = "keka"
    _JOB_TYPE = {1: "full-time", 2: "part-time", 3: "contract",
                 4: "internship", 5: "temporary", 6: "freelance"}

    def api_url(self, ref: BoardRef) -> str:
        return f"https://{ref.identifier}.keka.com/careers/api/jobs/default/active"

    def career_url(self, ref: BoardRef) -> str:
        return f"https://{ref.identifier}.keka.com/careers/"

    async def fetch_jobs(self, client, ref, limit=None):
        data = await client.get_json(self.api_url(ref))
        if not isinstance(data, list):
            return None
        base = self.career_url(ref)
        out = []
        for j in data[:limit] if limit else data:
            if not isinstance(j, dict) or not j.get("id"):
                continue
            locs = j.get("jobLocations") or []
            loc = locs[0] if locs else {}
            loc_str = ", ".join(x for x in (loc.get("city"),
                                            loc.get("countryName")) if x)
            job = Job(
                external_job_id=str(j.get("id")),
                title=j.get("title") or "",
                department=j.get("departmentName"),
                employment_type=self._JOB_TYPE.get(j.get("jobType")),
                description=strip_html(j.get("description")),
                apply_url=f"{base}jobdetails/{j.get('id')}",
                posted_at=to_iso(j.get("publishedOn")),
                salary_currency=(j.get("salaryRange") or {}).get("currency"),
                raw=j,
            )
            job.__dict__.update(parse_location(loc_str or None))
            if not job.country and loc.get("countryCode"):
                job.country = loc["countryCode"].upper()
            job.experience_level, job.seniority_rank = classify_seniority(
                job.title, job.description)
            out.append(job)
        return out


# --------------------------------------------------------------------------
class OracleORC(ATSAdapter):
    """Oracle Recruiting Cloud (Fusion) public career-site REST endpoint.

    `identifier` packs two fields: "{host}|{siteNumber}", e.g.
    "eeho.fa.us2.oraclecloud.com|CX_1". Like Workday it's a frontend contract,
    not a supported API — quarantined. The list response has no description;
    that needs a per-job details call we skip.
    """
    name = "oracle"
    stable = False
    PAGE = 50

    @staticmethod
    def _parts(ref: BoardRef) -> tuple[str, str]:
        host, _, site = ref.identifier.partition("|")
        return host, (site or "CX_1")

    def _rest(self, ref: BoardRef) -> str:
        host, _ = self._parts(ref)
        return f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

    def api_url(self, ref: BoardRef) -> str:
        return self._rest(ref)

    def career_url(self, ref: BoardRef) -> str:
        host, site = self._parts(ref)
        return (f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/requisitions")

    async def fetch_jobs(self, client, ref, limit=None):
        host, site = self._parts(ref)
        out: list[Job] = []
        offset = 0
        while True:
            q = (f"?onlyData=true&expand=requisitionList.secondaryLocations"
                 f"&finder=findReqs;siteNumber={site},sortBy=POSTING_DATES_DESC"
                 f",limit={self.PAGE},offset={offset}")
            data = await client.get_json(self._rest(ref) + q)
            items = (data or {}).get("items") if isinstance(data, dict) else None
            if not items:
                return out or None
            reqs = items[0].get("requisitionList") or []
            total = items[0].get("TotalJobsCount") or 0
            for r in reqs:
                job = Job(
                    external_job_id=str(r.get("Id")),
                    title=r.get("Title") or "",
                    department=r.get("Department") or r.get("JobFamily"),
                    posted_at=to_iso(r.get("PostedDate")),
                    apply_url=f"https://{host}/hcmUI/CandidateExperience/en/sites/"
                              f"{site}/job/{r.get('Id')}",
                    raw=r,
                )
                job.__dict__.update(parse_location(r.get("PrimaryLocation") or None))
                if not job.country and r.get("PrimaryLocationCountry"):
                    job.country = r["PrimaryLocationCountry"].upper()
                wt = (r.get("WorkplaceType") or "").lower()
                if "remote" in wt:
                    job.remote_type = "remote"
                elif "hybrid" in wt:
                    job.remote_type = "hybrid"
                job.experience_level, job.seniority_rank = classify_seniority(job.title)
                out.append(job)
            offset += self.PAGE
            if limit and len(out) >= limit:
                return out[:limit]
            if offset >= total or not reqs:
                return out


# ==========================================================================
# Aggregator feeds. Not per-tenant: one synthetic board each, seeded via
# `cli seed-feeds`. They return only the most recent slice, so closes_missing
# is False (jobs age out by first_seen_at instead of being force-closed).
# ==========================================================================
class FeedAdapter(ATSAdapter):
    stable = False
    closes_missing = False

    def career_url(self, ref: BoardRef) -> str:
        return self.api_url(ref)


class RemoteOK(FeedAdapter):
    name = "remoteok"

    def api_url(self, ref: BoardRef) -> str:
        return "https://remoteok.com/api"

    async def fetch_jobs(self, client, ref, limit=None):
        data = await client.get_json(self.api_url(ref))
        if not isinstance(data, list):
            return None
        out = []
        for j in data:
            if not isinstance(j, dict) or "legal" in j or not j.get("id"):
                continue
            job = Job(
                external_job_id=str(j.get("id")),
                title=j.get("position") or "",
                company=j.get("company"),
                department=(j.get("tags") or [None])[0],
                description=strip_html(j.get("description")),
                apply_url=j.get("apply_url") or j.get("url"),
                posted_at=to_iso(j.get("epoch") or j.get("date")),
                salary_min=j.get("salary_min") or None,
                salary_max=j.get("salary_max") or None,
                raw=j,
            )
            job.__dict__.update(parse_location(j.get("location") or None,
                                               fallback_remote=True))
            job.remote_type = job.remote_type or "remote"
            job.experience_level, job.seniority_rank = classify_seniority(
                job.title, job.description)
            out.append(job)
            if limit and len(out) >= limit:
                break
        return out


class Remotive(FeedAdapter):
    name = "remotive"

    def api_url(self, ref: BoardRef) -> str:
        return "https://remotive.com/api/remote-jobs"

    async def fetch_jobs(self, client, ref, limit=None):
        data = await client.get_json(self.api_url(ref))
        if not isinstance(data, dict) or "jobs" not in data:
            return None
        out = []
        for j in data["jobs"][:limit] if limit else data["jobs"]:
            job = Job(
                external_job_id=str(j.get("id")),
                title=j.get("title") or "",
                company=j.get("company_name"),
                department=j.get("category"),
                employment_type=j.get("job_type"),
                description=strip_html(j.get("description")),
                apply_url=j.get("url"),
                posted_at=to_iso(j.get("publication_date")),
                raw=j,
            )
            job.__dict__.update(parse_location(
                j.get("candidate_required_location") or None, fallback_remote=True))
            job.remote_type = job.remote_type or "remote"
            job.experience_level, job.seniority_rank = classify_seniority(
                job.title, job.description)
            out.append(job)
        return out


class Arbeitnow(FeedAdapter):
    name = "arbeitnow"
    MAX_PAGES = 5

    def api_url(self, ref: BoardRef) -> str:
        return "https://www.arbeitnow.com/api/job-board-api"

    async def fetch_jobs(self, client, ref, limit=None):
        out, url, pages = [], self.api_url(ref), 0
        while url and pages < self.MAX_PAGES:
            data = await client.get_json(url)
            if not isinstance(data, dict) or "data" not in data:
                return out or None
            for j in data["data"]:
                job = Job(
                    external_job_id=str(j.get("slug")),
                    title=j.get("title") or "",
                    company=j.get("company_name"),
                    employment_type=", ".join(j.get("job_types") or []) or None,
                    description=strip_html(j.get("description")),
                    apply_url=j.get("url"),
                    posted_at=to_iso(j.get("created_at")),
                    raw=j,
                )
                job.__dict__.update(parse_location(
                    j.get("location") or None, fallback_remote=j.get("remote")))
                job.experience_level, job.seniority_rank = classify_seniority(
                    job.title, job.description)
                out.append(job)
                if limit and len(out) >= limit:
                    return out[:limit]
            url = (data.get("links") or {}).get("next")
            pages += 1
        return out


class HNWhoIsHiring(FeedAdapter):
    """Monthly 'Ask HN: Who is hiring?' thread. Heuristic parse — one job per
    top-level comment; title/company sniffed from the first line."""
    name = "hn-hiring"
    ALGOLIA = "https://hn.algolia.com/api/v1"

    def api_url(self, ref: BoardRef) -> str:
        return f"{self.ALGOLIA}/search_by_date?tags=story,author_whoishiring&query=hiring"

    async def _latest_story(self, client) -> Optional[str]:
        data = await client.get_json(self.api_url(None))
        for h in (data or {}).get("hits", []):
            if "who is hiring?" in (h.get("title") or "").lower():
                return h["objectID"]
        return None

    async def fetch_jobs(self, client, ref, limit=None):
        story = await self._latest_story(client)
        if not story:
            return None
        data = await client.get_json(f"{self.ALGOLIA}/items/{story}")
        kids = (data or {}).get("children") or []
        out = []
        for c in kids[:limit] if limit else kids:
            text = strip_html(c.get("text"))
            if not text or not c.get("id"):
                continue
            head = text.split("\n", 1)[0]
            company = head.split("|")[0].strip()[:80] or "unknown"
            title = (head.split("|")[1].strip()[:120]
                     if "|" in head else head[:120])
            job = Job(
                external_job_id=str(c["id"]),
                title=title,
                company=company,
                description=text,
                apply_url=f"https://news.ycombinator.com/item?id={c['id']}",
                posted_at=to_iso(c.get("created_at")),
                raw=c,
            )
            job.__dict__.update(parse_location(
                None, fallback_remote="remote" in text.lower()))
            job.experience_level, job.seniority_rank = classify_seniority(title, text)
            out.append(job)
        return out


class AdzunaIN(FeedAdapter):
    """India job aggregator. Dormant unless ADZUNA_APP_ID / ADZUNA_APP_KEY are set."""
    name = "adzuna-in"
    MAX_PAGES = 5
    PER_PAGE = 50

    def _creds(self):
        import os
        return os.environ.get("ADZUNA_APP_ID"), os.environ.get("ADZUNA_APP_KEY")

    def api_url(self, ref: BoardRef) -> str:
        return "https://api.adzuna.com/v1/api/jobs/in/search"

    async def fetch_jobs(self, client, ref, limit=None):
        app_id, app_key = self._creds()
        if not app_id or not app_key:
            log.info("adzuna: ADZUNA_APP_ID / ADZUNA_APP_KEY not set — skipping")
            return None
        out = []
        for page in range(1, self.MAX_PAGES + 1):
            url = (f"{self.api_url(ref)}/{page}?app_id={app_id}&app_key={app_key}"
                   f"&results_per_page={self.PER_PAGE}&content-type=application/json")
            data = await client.get_json(url)
            results = (data or {}).get("results") if isinstance(data, dict) else None
            if not results:
                return out or None
            for j in results:
                loc = (j.get("location") or {})
                job = Job(
                    external_job_id=str(j.get("id")),
                    title=j.get("title") or "",
                    company=(j.get("company") or {}).get("display_name"),
                    department=(j.get("category") or {}).get("label"),
                    employment_type=j.get("contract_time"),
                    description=strip_html(j.get("description")),
                    apply_url=j.get("redirect_url"),
                    posted_at=to_iso(j.get("created")),
                    salary_min=j.get("salary_min"),
                    salary_max=j.get("salary_max"),
                    raw=j,
                )
                job.__dict__.update(parse_location(loc.get("display_name") or None))
                job.country = job.country or "IN"
                job.experience_level, job.seniority_rank = classify_seniority(
                    job.title, job.description)
                out.append(job)
                if limit and len(out) >= limit:
                    return out[:limit]
        return out


ADAPTERS: dict[str, ATSAdapter] = {
    a.name: a for a in (
        Greenhouse(), Lever(), Ashby(), SmartRecruiters(), Workday(),
        Workable(), Recruitee(), Breezy(), Keka(), OracleORC(),
        RemoteOK(), Remotive(), Arbeitnow(), HNWhoIsHiring(), AdzunaIN(),
    )
}

#: Aggregator feeds — one synthetic board each, seeded not discovered.
FEED_ATS = ["remoteok", "remotive", "arbeitnow", "hn-hiring", "adzuna-in"]


def adapter_for(ats: str) -> ATSAdapter:
    try:
        return ADAPTERS[ats]
    except KeyError:
        raise ValueError(f"no adapter for ATS {ats!r}") from None
