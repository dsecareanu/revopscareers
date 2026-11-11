#!/usr/bin/env python3
"""
check_ats_closed.py — READ-ONLY: find live posts with a Lever or Greenhouse apply link
whose job has closed, for posts the Hirebase expiry check cannot see (empty ID in
hirebase_job_ids.json = Hirebase Lookup never found the URL, so the expired feed can
never match it).

Run after check_expired_hirebase.py --daily, which adds new URLs to the map.

Rules (tested 2026-11-11 on 4,648 posts):
  Greenhouse: board list from boards-api (one call per board). A job not in the list
    is closed only if GET /boards/{board}/jobs/{id} returns 404.
  Lever: board list from api.lever.co. A job not in the list is closed only if its
    job page returns 404 (the API hides unlisted postings that are still live).
  Any network or HTTP error = unknown, never closed.

Output: expired_check/ats/would_expire.csv (overwritten), for expire_closed_jobs.py.

Usage:
  PYTHONUNBUFFERED=1 /opt/homebrew/bin/python3.14 -u check_ats_closed.py [--limit-posts 200]
"""
import argparse, csv, json, subprocess, time
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests

BASE = Path(__file__).resolve().parent
STATE = BASE / "hirebase_job_ids.json"
OUT = BASE / "expired_check" / "ats"
LOCAL_WP = Path.home() / "revopscareers.com" / "public"   # exists on tig only
PAUSE = 0.3                                                # between ATS requests

SQL = """SELECT p.ID,
 REPLACE(REPLACE(IFNULL(a.meta_value,''),CHAR(9),''),CHAR(10),''),
 REPLACE(REPLACE(IFNULL(c.meta_value,''),CHAR(9),' '),CHAR(10),' '),
 REPLACE(REPLACE(REPLACE(p.post_title,CHAR(9),' '),CHAR(10),' '),CHAR(13),' ')
FROM wp_posts p
JOIN wp_postmeta a ON a.post_id=p.ID AND a.meta_key='_application'
LEFT JOIN wp_postmeta c ON c.post_id=p.ID AND c.meta_key='_company_name'
WHERE p.post_type='job_listing' AND p.post_status='publish'
 AND (a.meta_value LIKE '%lever.co%' OR a.meta_value LIKE '%greenhouse.io%');"""

ats = requests.Session()
ats.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"


def get(url: str) -> requests.Response | None:
    """GET with pacing; retries 429/5xx/timeouts. None = unknown."""
    for attempt in range(3):
        time.sleep(PAUSE)
        try:
            r = ats.get(url, timeout=30)
        except requests.RequestException:
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(15 * (attempt + 1))
            continue
        return r
    return None


def parse(url: str):
    """(ats, api_base, board, job_id) or None."""
    s = urlsplit(url)
    host = s.netloc.lower()
    parts = [x for x in s.path.split("/") if x]
    if "lever.co" in host and len(parts) >= 2:
        api = "https://api.eu.lever.co" if ".eu." in host else "https://api.lever.co"
        return "lever", api, parts[0], parts[1]
    if "greenhouse.io" in host:
        if "jobs" in parts:
            i = parts.index("jobs")
            if i >= 1 and i + 1 < len(parts) and parts[i + 1].isdigit():
                return "greenhouse", "", parts[i - 1], parts[i + 1]
        q = parse_qs(s.query)
        if q.get("for") and q.get("token"):
            return "greenhouse", "", q["for"][0], q["token"][0]
    return None


def board_jobs(kind: str, api: str, board: str):
    """Set of open job IDs, "missing" (board 404), or None (unknown)."""
    if kind == "lever":
        r = get(f"{api}/v0/postings/{board}?mode=json")
    else:
        r = get(f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs")
    if r is None:
        return None
    if r.status_code == 404:
        return "missing"
    if not r.ok:
        return None
    try:
        data = r.json()
        jobs = data if kind == "lever" else data.get("jobs", [])
        return {str(j["id"]) for j in jobs}
    except (ValueError, KeyError, TypeError):
        return None


def is_closed(p: dict) -> bool | None:
    """True = closed, False = open, None = unknown. Only for jobs not in the board list."""
    if p["ats"] == "greenhouse":
        r = get(f"https://boards-api.greenhouse.io/v1/boards/{p['board']}/jobs/{p['job']}")
    else:
        r = get(p["url"])   # job page: unlisted postings still answer 200
    if r is None:
        return None
    if r.status_code == 404:
        return True
    return False if r.ok else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-posts", type=int, help="smoke test: first N candidate posts")
    args = ap.parse_args()

    if LOCAL_WP.is_dir():
        cmd = ["wp", f"--path={LOCAL_WP}", "db", "query", "--skip-column-names"]
    else:
        cmd = ["ssh", "tig", "cd /home/forge/revopscareers.com/public && wp db query --skip-column-names"]
    out = subprocess.run(cmd, input=SQL, capture_output=True, text=True, check=True).stdout
    state = json.loads(STATE.read_text()) if STATE.exists() else {}

    posts, skipped = [], Counter()
    for line in out.splitlines():
        f = line.split("\t")
        if len(f) != 4:
            skipped["bad_row"] += 1
            continue
        pid, url, company, title = f
        if state.get(url):          # has a Hirebase ID: check_expired_hirebase.py covers it
            skipped["has_hirebase_id"] += 1
            continue
        r = parse(url)
        if not r:
            skipped["unparsed_url"] += 1
            continue
        posts.append(dict(post_id=int(pid), url=url, company=company, title=title,
                          ats=r[0], api=r[1], board=r[2], job=r[3]))
    posts = posts[: args.limit_posts] if args.limit_posts else posts
    print(f"Candidates (empty/missing Hirebase ID): {len(posts)}; skipped {dict(skipped)}")

    boards = {}
    for key in sorted({(p["ats"], p["api"], p["board"]) for p in posts}):
        boards[key] = board_jobs(*key)
    print(f"Boards: {len(boards)}, unknown: {sum(v is None for v in boards.values())}, "
          f"missing: {sum(v == 'missing' for v in boards.values())}")

    result = Counter()
    closed = []
    for p in posts:
        b = boards[(p["ats"], p["api"], p["board"])]
        if b is None:
            result["unknown_board"] += 1
            continue
        if isinstance(b, set) and p["job"] in b:
            result["open"] += 1
            continue
        c = is_closed(p)
        result[{True: "closed", False: "open_unlisted", None: "unknown_job"}[c]] += 1
        if c:
            closed.append(p)

    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "would_expire.csv", "w", newline="") as f:
        w = csv.DictWriter(f, ["post_id", "company", "title", "url", "ats"], extrasaction="ignore")
        w.writeheader()
        w.writerows(closed)
    summary = f"ATS check: {dict(result)} → WOULD EXPIRE: {len(closed)}"
    (OUT / "summary.txt").write_text(summary + "\n")
    print(summary)


if __name__ == "__main__":
    main()
