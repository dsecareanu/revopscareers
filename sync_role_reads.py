#!/usr/bin/env python3
"""
ReLoop.me role reads for featured Revenue Operations jobs
=========================================================
One ReLoop.me Quick Check per job, shown on the job page under the description
(ReLoop's own "Read by ReLoop.me" embed). Replaces our earlier Claude Haiku read
(role_read/METHODOLOGY.md); jobs that still have a Haiku read get a ReLoop read too.

  1. GET  /roc/v1/role-read-queue   published category-21 jobs without a ReLoop read:
                                    featured ones, plus older ones that have a Haiku read
  2. POST https://api.reloop.me/role-intel/v1/quick   (header x-reloop-key, 50 reads a day)
  3. POST /roc/v1/reloop-read/{id}  the full response, stored as post meta _roc_reloop_read

A job is read once. The run stops when ReLoop's daily limit is reached; the rest wait
for the next run.

Usage:
    python sync_role_reads.py              # live run
    python sync_role_reads.py --dry-run    # print reads, no writes (still uses the daily limit)
    python sync_role_reads.py --limit 5    # at most N jobs this run (default 50)
    python sync_role_reads.py --ids 21885  # specific jobs (any category, even if already read)
"""

import argparse
import html
import json
import os
import re
import sys
from pathlib import Path

import requests

# =============================================================================
# CONFIG — loaded from .env
# =============================================================================

def _load_env(path: str = ".env") -> None:
    for base in [Path(__file__).parent, Path.cwd()]:
        env_path = base / path
        if env_path.exists():
            break
    else:
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip()
        if not os.environ.get(key):
            os.environ[key] = val

_load_env()

SITE_URL        = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME     = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD = os.environ.get("WP_APP_PASSWORD", "")
RELOOP_API_KEY  = os.environ.get("RELOOP_API_KEY", "")

API_BASE   = SITE_URL.rstrip("/") + "/wp-json"
RELOOP_URL = "https://api.reloop.me/role-intel/v1/quick"

wp = requests.Session()
wp.auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))
wp.headers.update({"Content-Type": "application/json"})


def plain(text: str) -> str:
    text = re.sub(r"<(br|/p|/li|/h\d)[^>]*>", "\n", text or "", flags=re.I)
    text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    return re.sub(r"[ \t]+", " ", re.sub(r"\n\s*\n+", "\n", text)).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="ReLoop.me role reads for featured RevOps jobs")
    parser.add_argument("--dry-run", action="store_true", help="Print reads, no writes")
    parser.add_argument("--limit", type=int, default=50, help="Max jobs per run (default 50)")
    parser.add_argument("--ids", type=str, default="", help="Comma-separated job IDs to read")
    args = parser.parse_args()

    if not WP_APP_PASSWORD:
        sys.exit("ERROR: WP_APP_PASSWORD not set in .env")
    if not RELOOP_API_KEY:
        sys.exit("ERROR: RELOOP_API_KEY not set in .env")

    if args.ids:
        queue = []
        for job_id in [int(x) for x in args.ids.split(",") if x.strip()]:
            j = wp.get(f"{API_BASE}/wp/v2/job-listings/{job_id}", params={"context": "edit"}, timeout=60).json()
            queue.append({"id": job_id, "title": j["title"]["raw"], "content": j["content"]["raw"]})
    else:
        r = wp.get(f"{API_BASE}/roc/v1/role-read-queue", params={"limit": args.limit}, timeout=60)
        r.raise_for_status()
        queue = r.json()
    print(f"Role reads (ReLoop.me) — {len(queue)} RevOps jobs without a ReLoop read (dry run: {args.dry_run})")

    done = failed = 0
    for job in queue:
        title = html.unescape(job["title"])
        try:
            r = requests.post(RELOOP_URL, headers={"x-reloop-key": RELOOP_API_KEY}, timeout=90,
                              json={"jd_text": (title + "\n" + plain(job["content"]))[:20000]})
        except requests.RequestException as e:
            print(f"  ! {title} (#{job['id']}): network error {e.__class__.__name__}")
            failed += 1
            continue
        if r.status_code == 429:
            print("  ReLoop daily limit reached — the rest wait for the next run")
            break
        if r.status_code != 200:
            print(f"  ! {title} (#{job['id']}): ReLoop {r.status_code}: {r.text[:200]}")
            failed += 1
            continue
        read = r.json()
        done += 1
        print(f"  + {title} (#{job['id']}): {read.get('ad_type')}, "
              f"{(read.get('display') or {}).get('headline', '')}, embed: {bool((read.get('embed') or {}).get('html'))}")
        if args.dry_run:
            print(json.dumps(read.get("embed"), indent=1)[:1500])
            continue
        w = wp.post(f"{API_BASE}/roc/v1/reloop-read/{job['id']}", json={"read": read}, timeout=60)
        if w.status_code != 200:
            print(f"    save failed ({w.status_code}): {w.text[:200]}")
            failed += 1

    print(f"Done. Read: {done}  Failed: {failed}")


if __name__ == "__main__":
    main()
