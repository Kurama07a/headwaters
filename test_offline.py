"""Offline smoke test: detection, normalization, adapter parsing, change detection."""
import asyncio, json, sys
from jobindex.detect import detect, detect_many
from jobindex.normalize import (parse_location, guess_level, classify_seniority,
                                min_years_required, to_iso, company_key)
from jobindex.adapters import (Greenhouse, Lever, Ashby, SmartRecruiters, Workday,
                               Workable, Recruitee, Breezy, Keka, OracleORC,
                               RemoteOK, Remotive, Arbeitnow)
from jobindex.models import BoardRef
from jobindex.store import Store

fails = []
def check(label, got, want):
    if got != want:
        fails.append(f"{label}: got {got!r} want {want!r}")

# ---- detection
check("lever job url", detect("https://jobs.lever.co/stripe/abc123").key, ("lever","stripe"))
check("gh new host", detect("https://job-boards.greenhouse.io/openai/jobs/12").key, ("greenhouse","openai"))
check("gh embed", detect("https://boards.greenhouse.io/embed/job_board?for=notion").key, ("greenhouse","notion"))
check("ashby lowercased", detect("https://jobs.ashbyhq.com/Notion/xyz").key, ("ashby","notion"))
check("ashby dedup", detect("https://jobs.ashbyhq.com/Vultr/x").key,
      detect("https://jobs.ashbyhq.com/vultr/y").key)
check("smartrec", detect("https://jobs.smartrecruiters.com/Acme/74400").key, ("smartrecruiters","Acme"))
check("workday locale", detect("https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite/job/x").key,
      ("workday","nvidia|wd5|NVIDIAExternalCareerSite"))
check("workday cxs", detect("https://acme.wd3.myworkdayjobs.com/wday/cxs/acme/External/jobs").key,
      ("workday","acme|wd3|External"))
check("reject bare host", detect("https://jobs.lever.co/"), None)
check("reject unrelated", detect("https://linkedin.com/jobs/view/123"), None)
check("reject reserved", detect("https://job-boards.greenhouse.io/embed/"), None)
check("workable", detect("https://apply.workable.com/blueground/j/0FD01ABC66/").key, ("workable","blueground"))
check("workable reject /j/", detect("https://apply.workable.com/j/0FD01ABC66"), None)
check("recruitee subdomain", detect("https://formo.recruitee.com/o/some-role").key, ("recruitee","formo"))
check("breezy subdomain", detect("https://acme.breezy.hr/p/abc-role").key, ("breezy","acme"))
check("keka careers", detect("https://acme.keka.com/careers/jobdetails/42").key, ("keka","acme"))
check("keka non-careers reject", detect("https://acme.keka.com/login"), None)
check("oracle orc", detect("https://x.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions").key,
      ("oracle","x.fa.us2.oraclecloud.com|CX_1"))

# ---- board is the dedup unit
urls = [f"https://jobs.lever.co/stripe/job{i}" for i in range(50)] + \
       [f"https://jobs.ashbyhq.com/notion/{i}" for i in range(20)]
check("collapse 70 urls", len(detect_many(urls)), 2)

# ---- location normalization
check("bangalore alias", parse_location("Bangalore, Karnataka, India")["city"], "Bengaluru")
check("blr country", parse_location("BLR")["country"], "IN")
check("hybrid", parse_location("Hybrid - Bangalore")["remote_type"], "hybrid")
check("remote india", parse_location("Remote - India")["country"], "IN")
check("gurgaon", parse_location("Gurgaon")["city"], "Gurugram")
check("region", parse_location("Hyderabad, India")["region"], "Telangana")
check("indiana not india", parse_location("Indianapolis, IN")["country"], None)
check("hq suffix stripped", parse_location("Stockholm HQ")["city"], "Stockholm")
check("epoch ms", to_iso(1700000000000)[:4], "2023")
check("company key", company_key("Acme Technologies Pvt Ltd"), "acme")

# ---- seniority classification: highest signal wins, title vetoes years
check("level senior", guess_level("Senior Backend Engineer"), "senior")
check("level entry", guess_level("New Grad Software Engineer"), "entry")
check("senior beats associate", classify_seniority("Senior Associate Engineer")[0], "senior")
check("assoc engineer entry", classify_seniority("Associate Software Engineer")[1], 1)
check("engineer I entry", classify_seniority("Software Engineer I")[1], 1)
check("engineer II mid", classify_seniority("Software Engineer II")[1], 2)
check("plain title unknown", classify_seniority("Software Engineer"), (None, None))
check("years from body", classify_seniority("Software Engineer",
      "You have 5+ years of experience building backends.")[1], 2)
check("min years", min_years_required("3-5 years of experience required"), 3)
check("intern rank", classify_seniority("ML Intern")[1], 0)
check("associate manager is manager", classify_seniority("Associate Manager")[0], "manager")

# ---- adapter parsing against fixture payloads
class Fake:
    def __init__(self, payload): self.payload = payload
    async def get_json(self, url, **kw): return self.payload
    async def post_json(self, url, json=None, **kw): return self.payload

async def run():
    gh = await Greenhouse().fetch_jobs(Fake({"jobs":[{"id":1,"title":"Senior SWE",
        "location":{"name":"Bangalore, India"},"absolute_url":"u",
        "departments":[{"name":"Eng"},{"name":"Core"}],"updated_at":"2025-01-01T00:00:00Z",
        "content":"<p>Hi&nbsp;there</p>"}]}), BoardRef("greenhouse","x"))
    check("gh city", gh[0].city, "Bengaluru")
    check("gh team", gh[0].team, "Core")
    check("gh desc", gh[0].description, "Hi there")
    check("gh level", gh[0].experience_level, "senior")

    lv = await Lever().fetch_jobs(Fake([{"id":"a","text":"Data Engineer",
        "categories":{"location":"Pune","team":"Data","commitment":"Full-time"},
        "workplaceType":"hybrid","createdAt":1700000000000,"hostedUrl":"u"}]),
        BoardRef("lever","x"))
    check("lever remote", lv[0].remote_type, "hybrid")
    check("lever city", lv[0].city, "Pune")

    ab = await Ashby().fetch_jobs(Fake({"jobs":[{"id":"b","title":"ML Intern",
        "location":"Remote","isRemote":True,"publishedAt":"2025-02-01T00:00:00Z",
        "compensation":{"summaryComponents":[{"compensationType":"Salary",
        "minValue":100,"maxValue":200,"currencyCode":"USD"}]}}]}), BoardRef("ashby","x"))
    check("ashby remote", ab[0].remote_type, "remote")
    check("ashby salary", ab[0].salary_max, 200)
    check("ashby level", ab[0].experience_level, "intern")

    sr = await SmartRecruiters().fetch_jobs(Fake({"totalFound":1,"content":[
        {"id":"c","name":"Backend Engineer","location":{"city":"Bangalore",
        "region":"Karnataka","country":"India"},"releasedDate":"2025-03-01T00:00:00Z"}]}),
        BoardRef("smartrecruiters","x"))
    check("sr country", sr[0].country, "IN")
    check("sr city", sr[0].city, "Bengaluru")

    wd = await Workday().fetch_jobs(Fake({"total":1,"jobPostings":[
        {"title":"Staff Engineer","externalPath":"/job/Hyderabad/Staff_R123",
         "locationsText":"Hyderabad, India"}]}), BoardRef("workday","t|wd5|External"))
    check("wd id", wd[0].external_job_id, "Staff_R123")
    check("wd city", wd[0].city, "Hyderabad")
    check("wd level", wd[0].experience_level, "staff")

    wk = await Workable().fetch_jobs(Fake({"name":"Acme","jobs":[{"shortcode":"AB12",
        "title":"Senior Backend Engineer","department":"Eng","employment_type":"Full-time",
        "city":"Bengaluru","country":"India","telecommuting":True,"experience":"Associate",
        "application_url":"u","published_on":"2025-04-01","description":"<p>hi</p>"}]}),
        BoardRef("workable","acme"))
    check("workable id", wk[0].external_job_id, "AB12")
    check("workable city", wk[0].city, "Bengaluru")
    check("workable remote", wk[0].remote_type, "remote")
    check("workable level", wk[0].experience_level, "senior")

    rc = await Recruitee().fetch_jobs(Fake({"offers":[{"id":42,"title":"Data Engineer",
        "location":"Pune, Maharashtra, India","country_code":"in","remote":False,
        "hybrid":True,"experience_code":"entry_level","published_at":"2025-05-01 00:00:00 UTC",
        "careers_apply_url":"u","description":"<p>d</p>","requirements":"<p>r</p>"}]}),
        BoardRef("recruitee","acme"))
    check("recruitee id", rc[0].external_job_id, "42")
    check("recruitee hybrid", rc[0].remote_type, "hybrid")
    check("recruitee city", rc[0].city, "Pune")
    check("recruitee level", rc[0].experience_level, "entry")

    bz = await Breezy().fetch_jobs(Fake([{"id":"zz","name":"QA Intern","department":"Eng",
        "type":{"name":"Full-Time"},"url":"https://acme.breezy.hr/p/zz-qa",
        "published_date":"2025-06-01T00:00:00Z","company":{"name":"Acme"},
        "location":{"name":"Chennai, Tamil Nadu, India","is_remote":False}}]),
        BoardRef("breezy","acme"))
    check("breezy id", bz[0].external_job_id, "zz")
    check("breezy apply", bz[0].apply_url, "https://acme.breezy.hr/p/zz-qa/apply")
    check("breezy city", bz[0].city, "Chennai")
    check("breezy level", bz[0].experience_level, "intern")

    ro = await RemoteOK().fetch_jobs(Fake([{"legal":"tos"},{"id":"7","position":"Backend Engineer",
        "company":"Acme","tags":["backend"],"epoch":1700000000,"location":"Remote",
        "apply_url":"u","description":"<p>x</p>","salary_min":0,"salary_max":0}]),
        BoardRef("remoteok","remoteok"))
    check("remoteok skips legal", len(ro), 1)
    check("remoteok remote", ro[0].remote_type, "remote")
    check("remoteok id", ro[0].external_job_id, "7")

    rm = await Remotive().fetch_jobs(Fake({"jobs":[{"id":9,"title":"SRE","company_name":"Acme",
        "category":"DevOps","job_type":"full_time","url":"u","publication_date":"2025-07-01",
        "candidate_required_location":"India","description":"<p>x</p>"}]}),
        BoardRef("remotive","remotive"))
    check("remotive company", rm[0].company, "Acme")
    check("remotive remote", rm[0].remote_type, "remote")

    an = await Arbeitnow().fetch_jobs(Fake({"data":[{"slug":"abc-123","title":"Dev",
        "company_name":"Acme","description":"<p>x</p>","remote":True,"url":"u",
        "job_types":["Full time"],"location":"Berlin","created_at":1700000000}],
        "links":{"next":None}}), BoardRef("arbeitnow","arbeitnow"))
    check("arbeitnow id", an[0].external_job_id, "abc-123")
    check("arbeitnow remote", an[0].remote_type, "remote")

    kk = await Keka().fetch_jobs(Fake([{"id":900,"title":"Backend Engineer",
        "departmentName":"Engineering","jobType":1,"publishedOn":"2025-08-01T00:00:00Z",
        "description":"<p>Build APIs. 4+ years of experience required.</p>",
        "jobLocations":[{"city":"Hyderabad","countryCode":"IN","countryName":"India"}],
        "salaryRange":{"currency":"INR"}}]), BoardRef("keka","acme"))
    check("keka id", kk[0].external_job_id, "900")
    check("keka city", kk[0].city, "Hyderabad")
    check("keka apply", kk[0].apply_url, "https://acme.keka.com/careers/jobdetails/900")
    check("keka level from body", kk[0].experience_level, "mid")

    orc = await OracleORC().fetch_jobs(Fake({"items":[{"TotalJobsCount":1,"requisitionList":[
        {"Id":"777","Title":"Senior Data Engineer","PostedDate":"2025-09-01",
         "PrimaryLocation":"Bengaluru, KA, India","PrimaryLocationCountry":"IN",
         "WorkplaceType":"Hybrid"}]}]}), BoardRef("oracle","host.oraclecloud.com|CX_1"))
    check("oracle id", orc[0].external_job_id, "777")
    check("oracle city", orc[0].city, "Bengaluru")
    check("oracle hybrid", orc[0].remote_type, "hybrid")
    check("oracle quarantined", OracleORC().stable, False)

    check("feeds don't close-missing", Remotive().closes_missing, False)
    return gh + lv + ab + sr + wd + wk + rc + bz + kk + orc + ro + rm + an

jobs = asyncio.run(run())

# ---- store + change detection
import os
if os.path.exists("/tmp/t.db"): os.remove("/tmp/t.db")
st = Store("/tmp/t.db")
ref = BoardRef("lever","stripe")
bid = st.upsert_board(ref, api_url="a", career_url="c")
check("idempotent upsert", st.upsert_board(ref, api_url="a", career_url="c"), bid)
st.set_board_status(bid, "active")

c1 = st.sync_jobs(bid, jobs)
check("first sync new", c1["new"], len(jobs))
c2 = st.sync_jobs(bid, jobs)
check("resync no new", (c2["new"], c2["closed"]), (0, 0))
jobs[0].title = "Principal SWE"
c3 = st.sync_jobs(bid, jobs)
check("changed detected", c3["changed"], 1)
c4 = st.sync_jobs(bid, jobs[:2])
check("closed detected", c4["closed"], len(jobs)-2)
c5 = st.sync_jobs(bid, jobs)
check("reopened", c5["closed"], 0)
check("active count", len(st.search(limit=100)), len(jobs))
check("filter country IN", len(st.search(country="IN")) > 0, True)
check("filter remote", len(st.search(remote="remote")), 5)  # ashby intern + workable + 3 feeds
check("due boards", len(st.due_boards()) >= 0, True)
st.set_meta("last_report_at", "2020-01-01T00:00:00+00:00")
check("meta roundtrip", st.get_meta("last_report_at"), "2020-01-01T00:00:00+00:00")
check("since=old -> all", len(st.search(since="2000-01-01T00:00:00+00:00", limit=100)), len(jobs))
check("since=future -> none", len(st.search(since="2099-01-01T00:00:00+00:00")), 0)
st.close()

print(f"\n{'FAILED' if fails else 'ALL PASS'} — {len(fails)} failure(s)")
for f in fails: print("  ✗", f)
sys.exit(1 if fails else 0)
