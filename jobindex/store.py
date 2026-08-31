"""Persistence. SQLite for the prototype; the SQL is plain enough to move to
Postgres by swapping the connection and the two `INSERT ... ON CONFLICT` bits.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Iterable, Optional

from .models import BoardRef, Job, QueryResult
from .normalize import company_key, content_hash, utcnow

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;   -- WAL + NORMAL: durable across app crash, ~no per-commit fsync
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS companies (
  id INTEGER PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  name_key TEXT NOT NULL UNIQUE,
  domain TEXT, country TEXT,
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS boards (
  id INTEGER PRIMARY KEY,
  company_id INTEGER REFERENCES companies(id),
  ats_type TEXT NOT NULL,
  board_identifier TEXT NOT NULL,
  career_url TEXT, api_url TEXT, country_hint TEXT,
  status TEXT NOT NULL DEFAULT 'candidate',   -- candidate|active|dead|error
  source TEXT,
  fail_count INTEGER NOT NULL DEFAULT 0,
  job_count INTEGER NOT NULL DEFAULT 0,
  refresh_interval_s INTEGER NOT NULL DEFAULT 86400,
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  last_fetch_at TEXT, next_fetch_at TEXT,
  UNIQUE(ats_type, board_identifier)
);
CREATE INDEX IF NOT EXISTS ix_boards_due ON boards(status, next_fetch_at);

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY,
  board_id INTEGER NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
  external_job_id TEXT NOT NULL,
  content_hash TEXT,
  title TEXT NOT NULL, company TEXT,
  location_raw TEXT, city TEXT, region TEXT, country TEXT, remote_type TEXT,
  department TEXT, team TEXT, employment_type TEXT, experience_level TEXT,
  seniority_rank INTEGER,
  description TEXT, apply_url TEXT,
  salary_min REAL, salary_max REAL, salary_currency TEXT,
  posted_at TEXT,
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, closed_at TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  UNIQUE(board_id, external_job_id)
);
CREATE INDEX IF NOT EXISTS ix_jobs_active ON jobs(is_active, country, city);
CREATE INDEX IF NOT EXISTS ix_jobs_seen ON jobs(first_seen_at);
CREATE INDEX IF NOT EXISTS ix_jobs_title ON jobs(title);

CREATE TABLE IF NOT EXISTS queries (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL, query TEXT NOT NULL, dims TEXT,
  executed_at TEXT NOT NULL,
  results INTEGER DEFAULT 0, new_boards INTEGER DEFAULT 0,
  yield_score REAL DEFAULT 0,
  UNIQUE(source, query)
);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""


class Store:
    def __init__(self, path: str = "jobs.db"):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self._migrate()
        self.db.commit()

    def _migrate(self):
        """Additive column migrations for DBs created before a column existed.
        `CREATE TABLE IF NOT EXISTS` never alters an existing table, so new
        columns land here. Index creation follows the ALTER so it can't run
        against a column that isn't there yet."""
        jobs_cols = {r["name"] for r in self.db.execute("PRAGMA table_info(jobs)")}
        if "seniority_rank" not in jobs_cols:
            self.db.execute("ALTER TABLE jobs ADD COLUMN seniority_rank INTEGER")
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_jobs_seniority "
                        "ON jobs(is_active, seniority_rank)")

    def close(self):
        self.db.close()

    # -- boards ------------------------------------------------------------
    def upsert_board(self, ref: BoardRef, *, api_url: str, career_url: str,
                     source: str = "discovery", status: str = "candidate") -> int:
        now = utcnow()
        cur = self.db.execute(
            """INSERT INTO boards (ats_type, board_identifier, career_url, api_url,
                                   country_hint, status, source,
                                   first_seen_at, last_seen_at, next_fetch_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(ats_type, board_identifier) DO UPDATE SET
                   last_seen_at=excluded.last_seen_at,
                   career_url=COALESCE(boards.career_url, excluded.career_url)
               RETURNING id""",
            (ref.ats, ref.identifier, career_url, api_url, ref.country_hint,
             status, source, now, now, now),
        )
        board_id = cur.fetchone()[0]
        self.db.commit()
        return board_id

    def known_board_keys(self) -> set[tuple[str, str]]:
        return {(r["ats_type"], r["board_identifier"])
                for r in self.db.execute("SELECT ats_type, board_identifier FROM boards")}

    def set_board_status(self, board_id: int, status: str, *, job_count: int = None):
        sql = "UPDATE boards SET status=?, last_seen_at=?"
        args = [status, utcnow()]
        if job_count is not None:
            sql += ", job_count=?"
            args.append(job_count)
        if status == "active":
            sql += ", fail_count=0"
        else:
            sql += ", fail_count=fail_count+1"
        sql += " WHERE id=?"
        args.append(board_id)
        self.db.execute(sql, args)
        self.db.commit()

    def candidates(self, limit: int = 500) -> list[sqlite3.Row]:
        return list(self.db.execute(
            "SELECT * FROM boards WHERE status='candidate' LIMIT ?", (limit,)))

    def due_boards(self, limit: int = 200, ats: Optional[str] = None) -> list[sqlite3.Row]:
        sql = ("SELECT * FROM boards WHERE status='active' "
               "AND (next_fetch_at IS NULL OR next_fetch_at <= ?)")
        args: list = [utcnow()]
        if ats:
            sql += " AND ats_type=?"
            args.append(ats)
        sql += " ORDER BY next_fetch_at ASC LIMIT ?"
        args.append(limit)
        return list(self.db.execute(sql, args))

    def reschedule(self, board_id: int, *, changed: int, interval_s: int):
        """Boards that keep producing changes get crawled more often."""
        from datetime import datetime, timedelta, timezone
        new = max(4 * 3600, interval_s // 2) if changed else min(7 * 86400, interval_s * 2)
        nxt = (datetime.now(timezone.utc) + timedelta(seconds=new)).isoformat()
        self.db.execute(
            "UPDATE boards SET refresh_interval_s=?, next_fetch_at=?, last_fetch_at=? "
            "WHERE id=?", (new, nxt, utcnow(), board_id))
        self.db.commit()

    def get_board(self, board_id: int) -> Optional[sqlite3.Row]:
        return self.db.execute("SELECT * FROM boards WHERE id=?", (board_id,)).fetchone()

    # -- key/value meta (watermarks etc) --------------------------------
    def get_meta(self, key: str, default=None):
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

    def set_meta(self, key: str, value: str):
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.db.commit()

    # -- dedup: collapse boards that differ only by identifier case -------
    def duplicate_board_groups(self) -> list[tuple[sqlite3.Row, list[sqlite3.Row]]]:
        """Groups of boards that collide on (ats_type, lower(identifier)).
        Returns (keeper, [losers]) — keeper is active-first, then most jobs."""
        groups: dict[tuple, list] = {}
        for r in self.db.execute("SELECT * FROM boards"):
            groups.setdefault((r["ats_type"], r["board_identifier"].lower()), []).append(r)
        out = []
        for members in groups.values():
            if len(members) > 1:
                members.sort(key=lambda r: (r["status"] != "active", -r["job_count"], r["id"]))
                out.append((members[0], members[1:]))
        return out

    def merge_board(self, keep_id: int, drop_id: int) -> int:
        """Reassign the loser's jobs to the keeper (keeper wins on any
        external_job_id collision), then delete the loser board. Returns jobs moved."""
        cur = self.db.execute(
            "UPDATE OR IGNORE jobs SET board_id=? WHERE board_id=?", (keep_id, drop_id))
        moved = cur.rowcount
        self.db.execute("DELETE FROM jobs WHERE board_id=?", (drop_id,))  # collided leftovers
        self.db.execute("DELETE FROM boards WHERE id=?", (drop_id,))
        self.db.commit()
        return moved

    # -- companies ---------------------------------------------------------
    def link_company(self, board_id: int, name: str) -> int:
        now = utcnow()
        cur = self.db.execute(
            """INSERT INTO companies (canonical_name, name_key, first_seen_at, last_seen_at)
               VALUES (?,?,?,?)
               ON CONFLICT(name_key) DO UPDATE SET last_seen_at=excluded.last_seen_at
               RETURNING id""",
            (name, company_key(name), now, now))
        cid = cur.fetchone()[0]
        self.db.execute("UPDATE boards SET company_id=? WHERE id=?", (cid, board_id))
        self.db.commit()
        return cid

    # -- jobs --------------------------------------------------------------
    def sync_jobs(self, board_id: int, jobs: Iterable[Job],
                  close_missing: bool = True) -> dict[str, int]:
        """Upsert this crawl's jobs and close anything that vanished.

        Returns counts of new / changed / closed. This is the change-detection
        mechanism the spec calls for — it works even when the ATS gives us no
        reliable posted_at.

        `close_missing=False` for aggregator feeds, which return only a recent
        slice: a job absent from this pull hasn't necessarily closed, it just
        aged off the feed. Those age out by `first_seen_at` instead.
        """
        now = utcnow()
        jobs = list(jobs)
        self.db.execute("CREATE TEMP TABLE IF NOT EXISTS seen(ext TEXT PRIMARY KEY)")
        self.db.execute("DELETE FROM seen")

        existing = {r["external_job_id"]: r["content_hash"] for r in self.db.execute(
            "SELECT external_job_id, content_hash FROM jobs WHERE board_id=?", (board_id,))}

        new = changed = 0
        seen_rows: list[tuple] = []
        insert_rows: list[list] = []
        sql = None
        for job in jobs:
            row = job.as_row()
            row["content_hash"] = content_hash(job)
            ext = row["external_job_id"]
            seen_rows.append((ext,))
            if ext not in existing:
                new += 1
            elif existing[ext] != row["content_hash"]:
                changed += 1
            if sql is None:  # column set is identical for every Job.as_row()
                cols = ["board_id", "first_seen_at", "last_seen_at"] + list(row.keys())
                placeholders = ",".join("?" * len(cols))
                updates = ",".join(f"{c}=excluded.{c}" for c in row
                                   if c != "external_job_id")
                sql = (f"INSERT INTO jobs ({','.join(cols)}) VALUES ({placeholders}) "
                       f"ON CONFLICT(board_id, external_job_id) DO UPDATE SET "
                       f"{updates}, last_seen_at=excluded.last_seen_at, "
                       f"is_active=1, closed_at=NULL")
            insert_rows.append([board_id, now, now] + list(row.values()))

        if seen_rows:
            self.db.executemany("INSERT OR IGNORE INTO seen(ext) VALUES (?)", seen_rows)
        if insert_rows:
            self.db.executemany(sql, insert_rows)

        closed = 0
        if close_missing:
            cur = self.db.execute(
                """UPDATE jobs SET is_active=0, closed_at=?
                   WHERE board_id=? AND is_active=1
                     AND external_job_id NOT IN (SELECT ext FROM seen)""",
                (now, board_id))
            closed = cur.rowcount
        self.db.commit()
        return {"new": new, "changed": changed, "closed": closed, "total": len(jobs)}

    # -- discovery bookkeeping --------------------------------------------
    def record_query(self, res: QueryResult):
        self.db.execute(
            """INSERT INTO queries (source, query, dims, executed_at, results,
                                    new_boards, yield_score)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(source, query) DO UPDATE SET
                   executed_at=excluded.executed_at, results=excluded.results,
                   new_boards=excluded.new_boards, yield_score=excluded.yield_score""",
            (res.source, res.query, json.dumps(res.dims), utcnow(),
             len(res.urls), res.new_boards, res.yield_score))
        self.db.commit()

    def query_seen(self, source: str, query: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM queries WHERE source=? AND query=?", (source, query)
        ).fetchone() is not None

    # -- search ------------------------------------------------------------
    def search(self, *, text: str = None, country: str = None, city: str = None,
               remote: str = None, level: str = None, max_level: int = None,
               days: int = None, since: str = None,
               limit: int = 50) -> list[sqlite3.Row]:
        sql = ["SELECT j.*, b.ats_type, b.board_identifier FROM jobs j "
               "JOIN boards b ON b.id=j.board_id WHERE j.is_active=1"]
        args: list = []
        if text:
            sql.append("AND (j.title LIKE ? OR j.description LIKE ?)")
            args += [f"%{text}%", f"%{text}%"]
        if country:
            sql.append("AND j.country=?"); args.append(country)
        if city:
            sql.append("AND j.city LIKE ?"); args.append(f"%{city}%")
        if remote:
            sql.append("AND j.remote_type=?"); args.append(remote)
        if level:
            sql.append("AND j.experience_level=?"); args.append(level)
        if max_level is not None:
            sql.append("AND j.seniority_rank IS NOT NULL AND j.seniority_rank <= ?")
            args.append(max_level)
        if days:
            from datetime import datetime, timedelta, timezone
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            sql.append("AND COALESCE(j.posted_at, j.first_seen_at) >= ?"); args.append(cutoff)
        if since:  # newly *discovered* since a watermark — the alerting path
            sql.append("AND j.first_seen_at >= ?"); args.append(since)
            order = "j.first_seen_at DESC"
        else:
            order = "COALESCE(j.posted_at, j.first_seen_at) DESC"
        sql.append(f"ORDER BY {order} LIMIT ?")
        args.append(limit)
        return list(self.db.execute(" ".join(sql), args))

    def stats(self) -> dict:
        q = lambda s: self.db.execute(s).fetchone()[0]  # noqa: E731
        return {
            "boards_total": q("SELECT COUNT(*) FROM boards"),
            "boards_active": q("SELECT COUNT(*) FROM boards WHERE status='active'"),
            "boards_candidate": q("SELECT COUNT(*) FROM boards WHERE status='candidate'"),
            "boards_dead": q("SELECT COUNT(*) FROM boards WHERE status='dead'"),
            "jobs_active": q("SELECT COUNT(*) FROM jobs WHERE is_active=1"),
            "jobs_total": q("SELECT COUNT(*) FROM jobs"),
            "jobs_active_in": q("SELECT COUNT(*) FROM jobs WHERE is_active=1 AND country='IN'"),
            "jobs_active_entry": q("SELECT COUNT(*) FROM jobs WHERE is_active=1 "
                                   "AND seniority_rank IS NOT NULL AND seniority_rank<=1"),
            "jobs_active_classified": q("SELECT COUNT(*) FROM jobs WHERE is_active=1 "
                                        "AND seniority_rank IS NOT NULL"),
            "queries_run": q("SELECT COUNT(*) FROM queries"),
        }
