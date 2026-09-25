#!/usr/bin/env python3
"""
Assign tags (up to 5) to job listings published in the last 5 days that
have no tags set. Uses the same keyword-matching logic as the sync scripts.

Usage:
    python patch_missing_tags.py           # dry run (preview only)
    python patch_missing_tags.py --live    # apply changes
    python patch_missing_tags.py --days 7  # widen the window
"""

import argparse
import html
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from tagging import assign_tags, fetch_tag_ids

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

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
        if not os.environ.get(key.strip()):
            os.environ[key.strip()] = val.strip()

_load_env()

SITE_URL        = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME     = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD = os.environ.get("WP_APP_PASSWORD", "")
WP_API          = f"{SITE_URL}/wp-json/wp/v2"

MAX_TAGS = 5

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", html.unescape(text or ""))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true",
                        help="Apply changes (default is dry-run)")
    parser.add_argument("--days", type=int, default=5,
                        help="How many days back to look (default: 5)")
    parser.add_argument("--all", action="store_true",
                        help="Process all published jobs regardless of date")
    args = parser.parse_args()
    dry_run = not args.live

    session = requests.Session()
    session.auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))

    print("Fetching tag list from WordPress...")
    tag_ids = fetch_tag_ids(session, WP_API)
    print(f"  {len(tag_ids)} tags loaded\n")

    after = None if args.all else (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
    scope = "all time" if args.all else f"after {after[:10]}"
    print(f"Fetching job listings ({scope})...")
    print(f"Dry run: {dry_run}\n")

    jobs = []
    page = 1
    while True:
        params = {
            "per_page": 100,
            "page": page,
            "status": "publish",
            "_fields": "id,title,content,meta,job_listing_tag",
        }
        if after:
            params["after"] = after
        resp = session.get(f"{WP_API}/job-listings", params=params, timeout=30)
        if resp.status_code == 400:
            break
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        jobs.extend(batch)
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        print(f"  Page {page}/{total_pages} — {len(jobs)} fetched so far", end="\r")
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.2)

    print(f"\nTotal recent jobs: {len(jobs)}")

    no_tags = [j for j in jobs if not j.get("job_listing_tag")]
    print(f"Without tags:      {len(no_tags)}\n")

    if not no_tags:
        print("Nothing to do.")
        return

    updated = 0
    skipped = 0
    errors  = 0

    for job in no_tags:
        job_id  = job["id"]
        title   = strip_html(job.get("title", {}).get("rendered", ""))
        content = strip_html(job.get("content", {}).get("rendered", ""))
        company = (job.get("meta") or {}).get("_company_name", "")

        term_ids = assign_tags(title, content, company, tag_ids)[:MAX_TAGS]

        if not term_ids:
            skipped += 1
            continue

        tag_names = [k for k, v in tag_ids.items() if v in term_ids]
        print(f"  [{job_id}] {title} @ {company}")
        print(f"    tags: {', '.join(tag_names)}")

        if dry_run:
            updated += 1
            continue

        resp = session.post(
            f"{WP_API}/job-listings/{job_id}",
            json={"job_listing_tag": term_ids},
            timeout=30,
        )
        if resp.status_code in (200, 201):
            updated += 1
        else:
            print(f"    FAILED ({resp.status_code}): {resp.text[:120]}")
            errors += 1

        if (updated + errors) % 10 == 0:
            time.sleep(1.0)

    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}Done.")
    print(f"  Tagged:        {updated}")
    print(f"  No match:      {skipped}  (no keywords found — left untagged)")
    if not dry_run:
        print(f"  Errors:        {errors}")
    if dry_run and updated:
        print(f"\nRun with --live to apply.")


if __name__ == "__main__":
    main()
