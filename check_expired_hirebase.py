#!/usr/bin/env python3
"""
check_expired_hirebase.py — READ-ONLY: list live job posts whose Hirebase posting has
closed, so expire_closed_jobs.py can expire them before the 60-day EXPIRY_DAYS.

No WordPress writes. Posts come from `wp db query` (local on tig, over `ssh tig`
elsewhere). State: hirebase_job_ids.json = {apply URL: Hirebase job_id, or "" when
Lookup did not find it}. Both modes keep it up to date.

Full mode (default, one-off backfill, ~13 days back):
  1. POST /v2/jobs/lookup for every post URL (billed per job found). not_found =
     closed OR never in Hirebase (RevPath/LinkedIn etc.), so not proof on its own.
  2. GET /v2/jobs/expired-jobs (free) since 13 days ago. Candidate = not_found post
     with the same company + title, expired on/after the post date.
  3. GET /v2/jobs/{_id} per candidate: confirmed only if application_link == _application.
  --stop-at HH:MM (UTC) ends step 3 early and writes what it has.

Daily mode (--daily, run by daily_sync.sh):
  1. Lookup only URLs not yet in the state file.
  2. Walk the expired feed since yesterday (UTC); match records by job_id.
  3. Repost guard: Lookup the matched URLs again; a URL that is live again is skipped.
  Output: expired_check/daily/would_expire.csv (overwritten each run).

Output (full: expired_check/<YYYY-MM-DD>/): would_expire.csv, summary.txt, and in full
mode also not_found_unconfirmed.csv, feed_url_mismatch.csv, candidates.csv,
job_board_counts.csv.

Usage:
  PYTHONUNBUFFERED=1 /opt/homebrew/bin/python3.14 -u check_expired_hirebase.py [--stop-at 22:50]
  ... --daily
  ... --limit-posts 500 --feed-pages 3   # smoke test
"""
import argparse, csv, json, os, re, subprocess, time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import requests

BASE = Path(__file__).resolve().parent
STATE = BASE / "hirebase_job_ids.json"
API = "https://api.hirebase.org/v2"
EXCLUDE_HOSTS = ("whatjobs", "lensa.com")   # own feeds, never from Hirebase
FEED_DAYS = 13                              # feed allows 14; keep a margin
MIN_INTERVAL = 0.65                         # 100 req/60 s per key
LOCAL_WP = Path.home() / "revopscareers.com" / "public"   # exists on tig only


def env(name: str) -> str:
    if os.environ.get(name):
        return os.environ[name]
    import envfile   # local Mac helper; tig exports .env instead
    return envfile.env(name)


hb = requests.Session()
hb.headers.update({"x-api-key": env("HIREBASE_API_KEY")})
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
    path = re.sub(r"/(application|apply)$", "", s.path.rstrip("/"))
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
    if LOCAL_WP.is_dir():
        cmd = ["wp", f"--path={LOCAL_WP}", "db", "query", "--skip-column-names"]
    else:
        cmd = ["ssh", "tig", "cd /home/forge/revopscareers.com/public && wp db query --skip-column-names"]
    out = subprocess.run(cmd, input=sql, capture_output=True, text=True, check=True).stdout
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
        if any(x in urlsplit(app).netloc.lower() for x in EXCLUDE_HOSTS):
            skipped["own_feed_host"] += 1
            continue
        posts.append(dict(id=int(pid), date=date, title=title, url=app, company=comp, expires=exp))
    print(f"Posts: {len(posts)} candidates; skipped {dict(skipped)}")
    return posts[:limit] if limit else posts


def lookup(urls: list[str]) -> tuple[dict, set, Counter]:
    matches, not_found, boards = {}, set(), Counter()
    for i in range(0, len(urls), 100):
        chunk = urls[i:i + 100]
        r = call("POST", "/jobs/lookup", json={"urls": chunk})
        if r.status_code != 200:
            print(f"  lookup {r.status_code}: {r.text[:200]} — chunk skipped")
            continue
        d = r.json()
        for m in d.get("matches", []):
            matches[m["url"]] = m["job_id"]
        not_found.update(d.get("not_found", []))
        for j in d.get("jobs", []):
            boards[j.get("job_board") or "(none)"] += 1
        if (i // 100) % 25 == 0:
            print(f"  lookup {i + len(chunk)}/{len(urls)}: found {len(matches)}, not_found {len(not_found)}")
    return matches, not_found, boards


def walk_feed(since: str, max_pages: int | None):
    page = 1
    while True:
        r = call("GET", "/jobs/expired-jobs", params={"since": since, "page": page, "limit": 1000})
        if r.status_code != 200:
            raise RuntimeError(f"expired feed page {page}: {r.status_code} {r.text[:200]}")
        d = r.json()
        yield from d.get("jobs", [])
        if page % 100 == 0:
            print(f"  feed page {page}/{d.get('total_pages')}")
        if page >= d.get("total_pages", 0) or not d.get("jobs") or (max_pages and page >= max_pages):
            return
        page += 1


def load_state() -> dict:
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def save_state(state: dict):
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE)


def write_csv(path: Path, rows: list[dict], fields: list[str]):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


CF = ["post_id", "post_date", "expires", "company", "title", "url",
      "hirebase_id", "date_expired", "hirebase_link", "get_status"]


def row_for(p: dict, rec: dict, link: str = "", status="") -> dict:
    return dict(post_id=p["id"], post_date=p["date"], expires=p["expires"], company=p["company"],
                title=p["title"], url=p["url"], hirebase_id=rec["_id"],
                date_expired=rec.get("date_expired", ""), hirebase_link=link, get_status=status)


def run_daily(args):
    outdir = BASE / "expired_check" / "daily"
    outdir.mkdir(parents=True, exist_ok=True)
    posts = load_posts(args.limit_posts)
    state = load_state()
    live_urls = {p["url"] for p in posts}
    new = [u for u in live_urls if u not in state]
    print(f"State: {len(state)} URLs known, {len(new)} new to look up")
    matches, not_found, _ = lookup(new)
    state.update({u: matches.get(u, "") for u in new})
    if not args.limit_posts:   # a smoke test sees only some posts: keep the rest
        state = {u: v for u, v in state.items() if u in live_urls}   # drop posts no longer live
        save_state(state)

    by_id: dict[str, list[dict]] = {}
    for p in posts:
        if state.get(p["url"]):
            by_id.setdefault(state[p["url"]], []).append(p)
    since = (datetime.now(timezone.utc).date() - timedelta(days=args.since_days)).isoformat()
    hits, seen = [], 0
    for rec in walk_feed(since, args.feed_pages):
        seen += 1
        for p in by_id.get(rec["_id"], []):
            hits.append(row_for(p, rec))
    print(f"Feed since {since}: {seen} records, {len(hits)} posts matched by job_id")

    # Repost guard: a URL that Lookup finds live again is not expired.
    relive, _, _ = lookup(sorted({h["url"] for h in hits}))
    for u, jid in relive.items():
        state[u] = jid
    if not args.limit_posts:
        save_state(state)
    rows = [h for h in hits if h["url"] not in relive]
    write_csv(outdir / "would_expire.csv", rows, CF)
    summary = [f"Daily run {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC — read-only",
               f"Live posts checked: {len(posts)}, new URLs looked up: {len(new)} "
               f"(found {len(matches)}, not_found {len(not_found)})",
               f"Expired feed since {since}: {seen} records, {len(hits)} matched by job_id, "
               f"{len(hits) - len(rows)} live again (repost guard)",
               f"WOULD EXPIRE: {len(rows)}"]
    (outdir / "summary.txt").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))


def run_full(args):
    outdir = BASE / "expired_check" / datetime.now().strftime("%Y-%m-%d")
    outdir.mkdir(parents=True, exist_ok=True)
    posts = load_posts(args.limit_posts)
    by_url: dict[str, list[dict]] = {}
    for p in posts:
        by_url.setdefault(p["url"], []).append(p)
    urls = list(by_url)

    matches, not_found, boards = lookup(urls)
    print(f"Lookup done: {len(matches)} live, {len(not_found)} not_found")
    if not args.limit_posts:   # a smoke test must not overwrite the real state file
        save_state({u: matches.get(u, "") for u in urls})
    write_csv(outdir / "job_board_counts.csv",
              [dict(job_board=b, live_posts=n) for b, n in boards.most_common()], ["job_board", "live_posts"])

    nf_posts = [p for u in not_found for p in by_url.get(u, [])]
    index: dict[str, list[dict]] = {}
    for p in nf_posts:
        index.setdefault(alnum(p["title"]), []).append(p)
    since = (datetime.now(timezone.utc).date() - timedelta(days=FEED_DAYS)).isoformat()
    candidates, pairs, seen = [], set(), 0
    for rec in walk_feed(since, args.feed_pages):
        seen += 1
        tkey = alnum(re.sub(r"-[0-9a-f]{8}$", "", rec.get("job_slug", "")))
        for p in index.get(tkey, []):
            if (rec.get("date_expired", "") >= p["date"][:10]          # closed after we posted it
                    and (p["id"], rec["_id"]) not in pairs
                    and company_match(alnum(rec.get("company_slug")), alnum(p["company"]))):
                pairs.add((p["id"], rec["_id"]))
                candidates.append((p, rec))
    print(f"Feed done: {seen} records since {since}, {len(candidates)} candidates")
    write_csv(outdir / "candidates.csv", [row_for(p, r) for p, r in candidates], CF)

    stop_at = None
    if args.stop_at:
        h, m = map(int, args.stop_at.split(":"))
        stop_at = datetime.now(timezone.utc).replace(hour=h, minute=m, second=0, microsecond=0)
    confirmed, mismatch, checked = {}, [], 0
    for p, rec in candidates:
        if stop_at and datetime.now(timezone.utc) >= stop_at:
            print(f"Stopped at {args.stop_at} UTC (--stop-at)")
            break
        checked += 1
        if p["id"] in confirmed:
            continue
        r = call("GET", f"/jobs/{rec['_id']}")
        link = r.json().get("application_link", "") if r.status_code == 200 else ""
        row = row_for(p, rec, link, r.status_code)
        if link and norm_url(link) == norm_url(p["url"]):
            confirmed[p["id"]] = row
        else:
            mismatch.append(row)
        if checked % 500 == 0:
            print(f"  confirm {checked}/{len(candidates)}: {len(confirmed)} confirmed")
    print(f"Confirmed: {len(confirmed)}; URL mismatch: {len(mismatch)}; "
          f"unchecked: {len(candidates) - checked}")

    write_csv(outdir / "would_expire.csv",
              sorted(confirmed.values(), key=lambda r: r["date_expired"], reverse=True), CF)
    write_csv(outdir / "feed_url_mismatch.csv", mismatch, CF)
    unconf = [p for p in nf_posts if p["id"] not in confirmed]
    write_csv(outdir / "not_found_unconfirmed.csv",
              [dict(post_id=p["id"], post_date=p["date"], expires=p["expires"], company=p["company"],
                    title=p["title"], url=p["url"]) for p in unconf],
              ["post_id", "post_date", "expires", "company", "title", "url"])

    age = Counter()
    for p in unconf:
        days = (datetime.now() - datetime.strptime(p["date"][:10], "%Y-%m-%d")).days
        age["<=13d" if days <= 13 else "14-30d" if days <= 30 else ">30d"] += 1
    nf_hosts = Counter(urlsplit(p["url"]).netloc.lower() for p in unconf).most_common(15)
    summary = [
        f"Run {datetime.now():%Y-%m-%d %H:%M} — read-only, no posts changed",
        f"Candidate posts (excl. WhatJobs/Lensa): {len(posts)}, unique URLs: {len(urls)}",
        f"Lookup: live {len(matches)}, not_found {len(not_found)} URLs ({len(nf_posts)} posts)",
        f"Expired feed since {since}: {seen} records, {len(candidates)} company+title candidates",
        f"Checked {checked}/{len(candidates)} candidates"
        + (f" (stopped at {args.stop_at} UTC)" if checked < len(candidates) else ""),
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", action="store_true", help="incremental: yesterday's feed, match by job_id")
    ap.add_argument("--since-days", type=int, default=1, help="--daily: feed days back (default 1)")
    ap.add_argument("--stop-at", help="full mode: stop confirming at HH:MM UTC")
    ap.add_argument("--limit-posts", type=int)
    ap.add_argument("--feed-pages", type=int, help="smoke test: stop the feed walk after N pages")
    args = ap.parse_args()
    run_daily(args) if args.daily else run_full(args)


if __name__ == "__main__":
    main()
