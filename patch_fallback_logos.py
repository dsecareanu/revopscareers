#!/usr/bin/env python3
"""
Set fallback logo (media ID 184) on job listings published in the last 5 days
that have no featured image.

Usage:
    python patch_fallback_logos.py           # dry run (preview only)
    python patch_fallback_logos.py --live    # apply changes
    python patch_fallback_logos.py --days 7  # widen the window
"""

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

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

FALLBACK_LOGO_ID = 184

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true",
                        help="Apply changes (default is dry-run)")
    parser.add_argument("--days", type=int, default=5,
                        help="How many days back to look (default: 5)")
    args = parser.parse_args()
    dry_run = not args.live

    session = requests.Session()
    session.auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))

    after = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()

    print(f"Fetching job listings published after {after[:10]}...")
    print(f"Dry run: {dry_run}\n")

    jobs = []
    page = 1
    while True:
        resp = session.get(
            f"{WP_API}/job-listings",
            params={
                "per_page": 100,
                "page": page,
                "status": "publish",
                "after": after,
                "_fields": "id,title,featured_media,meta",
            },
            timeout=30,
        )
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

    no_logo = [j for j in jobs if not j.get("featured_media")]
    print(f"Missing logo:      {len(no_logo)}")

    if not no_logo:
        print("\nNothing to do.")
        return

    print()
    updated = 0
    errors  = 0
    for job in no_logo:
        job_id  = job["id"]
        title   = job.get("title", {}).get("rendered", "(no title)")
        company = (job.get("meta") or {}).get("_company_name", "")
        label   = f"[{job_id}] {title} @ {company}"

        if dry_run:
            print(f"  [DRY RUN] would patch {label}")
            updated += 1
            continue

        resp = session.post(
            f"{WP_API}/job-listings/{job_id}",
            json={"featured_media": FALLBACK_LOGO_ID},
            timeout=30,
        )
        if resp.status_code in (200, 201):
            print(f"  patched {label}")
            updated += 1
        else:
            print(f"  FAILED  {label} ({resp.status_code}): {resp.text[:120]}")
            errors += 1

        if updated % 10 == 0:
            time.sleep(1.0)

    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}Done.")
    print(f"  Updated: {updated}")
    if not dry_run:
        print(f"  Errors:  {errors}")
    if dry_run:
        print("\nRun with --live to apply.")


if __name__ == "__main__":
    main()
