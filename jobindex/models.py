"""Core value objects. Everything else speaks in these types."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional


@dataclass(frozen=True)
class BoardRef:
    """A discovered ATS board. This is the deduplication unit of the system.

    `identifier` is the stable, reusable tenant key:
        greenhouse       -> "openai"
        lever            -> "stripe"
        ashby            -> "notion"
        smartrecruiters  -> "company"
        workday          -> "tenant|wd5|External"
    """
    ats: str
    identifier: str
    career_url: Optional[str] = None
    country_hint: Optional[str] = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.ats, self.identifier)

    # Workday packs three fields into `identifier`; unpack on demand.
    @property
    def workday_parts(self) -> tuple[str, str, str]:
        tenant, dc, site = self.identifier.split("|")
        return tenant, dc, site

    def __str__(self) -> str:
        return f"{self.ats}:{self.identifier}"


@dataclass
class Job:
    """Normalized job, one row per (board, external_job_id)."""
    external_job_id: str
    title: str
    company: Optional[str] = None

    location_raw: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    remote_type: Optional[str] = None  # onsite | hybrid | remote

    department: Optional[str] = None
    team: Optional[str] = None
    employment_type: Optional[str] = None
    experience_level: Optional[str] = None  # intern | entry | mid | senior | staff | manager
    seniority_rank: Optional[int] = None    # 0..5, ordered; enables "entry or below" filters

    description: Optional[str] = None
    apply_url: Optional[str] = None

    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = None

    posted_at: Optional[str] = None  # ISO-8601 UTC

    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    def as_row(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("raw", None)
        return d


@dataclass
class QueryResult:
    """Outcome of one discovery query — feeds the frontier's yield scoring."""
    source: str
    query: str
    dims: dict[str, Any]
    urls: list[str] = field(default_factory=list)
    boards: list[BoardRef] = field(default_factory=list)
    new_boards: int = 0

    @property
    def yield_score(self) -> float:
        if not self.urls:
            return 0.0
        return self.new_boards / len(self.urls)
