#!/usr/bin/env python3
"""
check_expired_hirebase.py — READ-ONLY dry run: list live job posts whose Hirebase
posting has closed, so they could be expired before the 60-day EXPIRY_DAYS.

No WordPress writes. It reads posts over `ssh tig` (wp db query) and calls Hirebase:
  1. POST /v2/jobs/lookup (100 URLs/call) with each post's _application URL.
     Billed per job found; not_found is free. not_found = closed OR never in Hirebase
     (other feeds such as RevPath/LinkedIn), so it is NOT proof on its own.
  2. GET /v2/jobs/expired-jobs (free) for the last 13 days. Records carry only
     _id, company_slug, job_slug. A record is a candidate when the company and the
     title match a not_found post.
  3. GET /v2/jobs/{_id} for each candidate: confirmed only when its application_link
     equals the post's _application (normalised).

Output: expired_check/<YYYY-MM-DD>/
  would_expire.csv        confirmed: not_found + expired-feed record with same apply URL
  not_found_unconfirmed.csv  not_found, no feed match (other source, closed >13 d ago, URL changed)
  feed_url_mismatch.csv   company+title match in the feed, but a different apply URL
  job_board_counts.csv    job_board of the live (found) posts — input for exclude_job_board
  lookup_matches.json     url -> Hirebase job_id for live posts (for a later daily check)
  summary.txt

Usage:
  PYTHONUNBUFFERED=1 /opt/homebrew/bin/python3.14 -u check_expired_hirebase.py
  ... --limit-posts 500 --feed-pages 3   # smoke test
"""
import argparse, csv, json, re, subprocess, sys, time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import requests
import envfile

API = "https://api.hirebase.org/v2"
EXCLUDE_HOSTS = ("whatjobs", "lensa.com")   # own feeds, never from Hirebase
FEED_DAYS = 13                              # feed allows 14; keep a margin
MIN_INTERVAL = 0.65                         # 100 req/60 s per key

hb = requests.Session()
hb.headers.update({"x-api-key": envfile.env("HIREBASE_API_KEY")})
_last = 0.0


def call(method, path, **kw):
    """Paced request with retries on 429 / 5xx / timeouts."""
    global _last
    for attempt in range(1, 8):
        wait = MIN_INTERVAL - (time.time() - _last)
        if wait > 0:
            time.sleep(wait)
        _last = time.time()
        try:
            r = hb.request(method, API + path, timeout=90, **kw)
        except (requests.Timeout, requests.ConnectionError) as e:
            print(f"  network error ({e}); retry {attempt}")
            time.sleep(10 * attempt)
            continue
        if r.status_code == 429:
            ra = int(r.headers.get("Retry-After", "30") or 30)
            print(f"  429 {r.text[:120]} — sleep {ra}s")
            time.sleep(ra)
            continue
        if r.status_code >= 500:
            print(f"  {r.status_code} — retry {attempt}")
            time.sleep(10 * attempt)
            continue
        return r
    raise RuntimeError(f"{method} {path} failed after retries")


def norm_url(u: str) -> str:
    """Same tolerance as Hirebase lookup: no query/fragment, trailing slash,
    /application or /apply; both Greenhouse hosts equal."""
    u = (u or "").strip()
    if not u:
        return ""
    s = urlsplit(u if "://" in u else "https://" + u)
    host = s.netloc.lower().removeprefix("www.")
    if host == "boards.greenhouse.io":
        host = "job-boards.greenhouse.io"
    path = s.path.rstrip("/")
    path = re.sub(r"/(application|apply)$", "", path)
    return f"{host}{path.rstrip('/')}".lower()


def alnum(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def company_match(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return a == b or (min(len(a), len(b)) >= 4 and (a.startswith(b) or b.startswith(a)))


def load_posts(limit: int | None) -> list[dict]:
    sql = """SELECT p.ID, p.post_date,
  REPLACE(REPLACE(REPLACE(p.post_title, CHAR(9),' '), CHAR(10),' '), CHAR(13),' '),
  REPLACE(REPLACE(IFNULL(a.meta_value,''), CHAR(9),''), CHAR(10),''),
  REPLACE(REPLACE(IFNULL(c.meta_value,''), CHAR(9),' '), CHAR(10),' '),
  IFNULL(e.meta_value,'')
FROM wp_posts p
LEFT JOIN wp_postmeta a ON a.post_id=p.ID AND a.meta_key='_application'
LEFT JOIN wp_postmeta c ON c.post_id=p.ID AND c.meta_key='_company_name'
LEFT JOIN wp_postmeta e ON e.post_id=p.ID AND e.meta_key='_job_expires'
WHERE p.post_type='job_listing' AND p.post_status='publish'
ORDER BY p.post_date DESC;"""
    out = subprocess.run(
        ["ssh", "tig", "cd /home/forge/revopscareers.com/public && wp db query --skip-column-names"],
        input=sql, capture_output=True, text=True, check=True).stdout
    posts, skipped = [], Counter()
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != 6:
            skipped["bad_row"] += 1
            continue
        pid, date, title, app, comp, exp = parts
        if not app.startswith("http"):
            skipped["no_http_url"] += 1
            continue
        host = urlsplit(app).netloc.lower()
        if any(x in host for x in EXCLUDE_HOSTS):
            skipped["own_feed_host"] += 1
            continue
        posts.append(dict(id=int(pid), date=date, title=title, url=app,
                          company=comp, expires=exp))
    print(f"Posts: {len(posts)} candidates; skipped {dict(skipped)}")
    return posts[:limit] if limit else posts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-posts", type=int)
    ap.add_argument("--feed-pages", type=int, help="smoke test: stop the feed walk after N pages")
    args = ap.parse_args()

    outdir = Path(__file__).parent / "expired_check" / datetime.now().strftime("%Y-%m-%d")
    outdir.mkdir(parents=True, exist_ok=True)
    posts = load_posts(args.limit_posts)

    # ---- 1. Lookup by URL ---------------------------------------------------
    by_url: dict[str, list[dict]] = {}
    for p in posts:
        by_url.setdefault(p["url"], []).append(p)
    urls = list(by_url)
    matches, not_found, boards = {}, set(), Counter()
    for i in range(0, len(urls), 100):
        chunk = urls[i:i + 100]
        r = call("POST", "/jobs/lookup", json={"urls": chunk})
        if r.status_code != 200:
            print(f"  lookup {r.status_code}: {r.text[:200]} — chunk skipped")
            continue
        d = r.json()
        jobs = {j["_id"]: j for j in d.get("jobs", [])}
        for m in d.get("matches", []):
            matches[m["url"]] = m["job_id"]
        not_found.update(d.get("not_found", []))
        for j in jobs.values():
            boards[j.get("job_board") or "(none)"] += 1
        if (i // 100) % 25 == 0:
            print(f"  lookup {i + len(chunk)}/{len(urls)}: found {len(matches)}, not_found {len(not_found)}")
    print(f"Lookup done: {len(matches)} live, {len(not_found)} not_found")

    # ---- 2. Expired feed -----------------------------------------------------
    nf_posts = [p for u in not_found for p in by_url.get(u, [])]
    index: dict[str, list[dict]] = {}
    for p in nf_posts:
        index.setdefault(alnum(p["title"]), []).append(p)
    since = (datetime.now(timezone.utc).date() - timedelta(days=FEED_DAYS)).isoformat()
    candidates, page, total_pages, seen = [], 1, None, 0
    while True:
        r = call("GET", "/jobs/expired-jobs", params={"since": since, "page": page, "limit": 1000})
        if r.status_code != 200:
            raise RuntimeError(f"expired feed page {page}: {r.status_code} {r.text[:200]}")
        d = r.json()
        total_pages = d.get("total_pages", 0)
        for rec in d.get("jobs", []):
            seen += 1
            tkey = alnum(re.sub(r"-[0-9a-f]{8}$", "", rec.get("job_slug", "")))
            for p in index.get(tkey, []):
                if company_match(alnum(rec.get("company_slug")), alnum(p["company"])):
                    candidates.append((p, rec))
        if page % 100 == 0:
            print(f"  feed page {page}/{total_pages}: {seen} records, {len(candidates)} candidates")
        if page >= total_pages or not d.get("jobs") or (args.feed_pages and page >= args.feed_pages):
            break
        page += 1
    print(f"Feed done: {seen} records since {since}, {len(candidates)} candidates")

    # ---- 3. Confirm by apply URL --------------------------------------------
    confirmed, mismatch = {}, []
    for p, rec in candidates:
        if p["id"] in confirmed:
            continue
        r = call("GET", f"/jobs/{rec['_id']}")
        link = r.json().get("application_link", "") if r.status_code == 200 else ""
        row = dict(post_id=p["id"], post_date=p["date"], expires=p["expires"],
                   company=p["company"], title=p["title"], url=p["url"],
                   hirebase_id=rec["_id"], date_expired=rec.get("date_expired", ""),
                   hirebase_link=link, get_status=r.status_code)
        if link and norm_url(link) == norm_url(p["url"]):
            confirmed[p["id"]] = row
        else:
            mismatch.append(row)
    print(f"Confirmed: {len(confirmed)}; URL mismatch: {len(mismatch)}")

    # ---- Output ---------------------------------------------------------------
    def write(name, rows, fields):
        with open(outdir / name, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    cf = ["post_id", "post_date", "expires", "company", "title", "url",
          "hirebase_id", "date_expired", "hirebase_link", "get_status"]
    write("would_expire.csv", sorted(confirmed.values(), key=lambda r: r["date_expired"], reverse=True), cf)
    write("feed_url_mismatch.csv", mismatch, cf)
    unconf = [dict(post_id=p["id"], post_date=p["date"], expires=p["expires"], company=p["company"],
                   title=p["title"], url=p["url"]) for p in nf_posts if p["id"] not in confirmed]
    write("not_found_unconfirmed.csv", unconf, ["post_id", "post_date", "expires", "company", "title", "url"])
    write("job_board_counts.csv", [dict(job_board=b, live_posts=n) for b, n in boards.most_common()],
          ["job_board", "live_posts"])
    (outdir / "lookup_matches.json").write_text(json.dumps(matches))

    age = Counter()
    for p in nf_posts:
        if p["id"] in confirmed:
            continue
        days = (datetime.now() - datetime.strptime(p["date"][:10], "%Y-%m-%d")).days
        age["<=13d" if days <= 13 else "14-30d" if days <= 30 else ">30d"] += 1
    nf_hosts = Counter(urlsplit(p["url"]).netloc.lower() for p in unconf).most_common(15)
    summary = [
        f"Run {datetime.now():%Y-%m-%d %H:%M} — DRY RUN, no posts changed",
        f"Candidate posts (excl. WhatJobs/Lensa): {len(posts)}, unique URLs: {len(urls)}",
        f"Lookup: live {len(matches)}, not_found {len(not_found)} URLs ({len(nf_posts)} posts)",
        f"Expired feed since {since}: {seen} records, {len(candidates)} company+title candidates",
        f"WOULD EXPIRE (confirmed by apply URL): {len(confirmed)}",
        f"Feed match but different URL: {len(mismatch)}",
        f"not_found, unconfirmed by post age: {dict(age)}",
        "Top hosts of unconfirmed not_found posts:",
        *[f"  {h}: {n}" for h, n in nf_hosts],
        "Top job_board of live posts:",
        *[f"  {b}: {n}" for b, n in boards.most_common(20)],
    ]
    (outdir / "summary.txt").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))
    print(f"\nFiles in {outdir}")


if __name__ == "__main__":
    main()
