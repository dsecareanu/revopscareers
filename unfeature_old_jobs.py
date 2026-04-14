#!/usr/bin/env python3
"""
Unfeature Old Job Listings — revopscareers.com
===============================================
Scans all published job listings and removes the "featured" flag from
jobs that are older than FEATURED_MAX_DAYS, but ONLY for jobs posted by
the webadmin account (author ID = WEBADMIN_AUTHOR_ID).

Client-posted jobs (any other author) are left untouched regardless of age,
as they are paid featured listings with their own duration.

Usage:
    python unfeature_old_jobs.py

Configuration:
    Edit the CONFIG section below before running.

Requirements:
    pip install requests
"""

import os
import requests
import sys
from datetime import datetime, timedelta, timezone

# =============================================================================
# CONFIG — loaded from environment variables (or .env file)
# =============================================================================
SITE_URL          = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME       = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD   = os.environ.get("WP_APP_PASSWORD", "")
POST_TYPE         = "job-listings"
WEBADMIN_AUTHOR_ID = 1            # Author ID for the webadmin/RevOps account
FEATURED_MAX_DAYS = 1             # Unfeature webadmin jobs older than this many days
DRY_RUN           = False         # Set to True for a dry-run preview
# =============================================================================

API_BASE = SITE_URL.rstrip("/") + "/wp-json"
WP_API   = API_BASE + "/wp/v2"

auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))
session = requests.Session()
session.auth = auth
session.headers.update({"Content-Type": "application/json"})


def fetch_all_featured_jobs() -> list[dict]:
    """Fetch all published job listings and return only those with _featured=1."""
    posts, page = [], 1
    print(f"Fetching all '{POST_TYPE}' posts to check featured status…")

    while True:
        params = {
            "per_page": 100,
            "page": page,
            "status": "publish",
            "_fields": "id,title,date,author,meta,link",
        }
        resp = session.get(f"{WP_API}/{POST_TYPE}", params=params)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        print(f"  Page {page}/{total_pages}", end="\r")

        for post in batch:
            meta = post.get("meta", {})
            if isinstance(meta, dict) and meta.get("_featured") == 1:
                posts.append(post)

        if page >= total_pages:
            break
        page += 1

    print(f"\n  Done. Found {len(posts)} featured post(s).")
    return posts


def get_title(post: dict) -> str:
    t = post.get("title", {})
    return t.get("rendered", "") if isinstance(t, dict) else str(t)


def unfeature_post(post_id: int) -> bool:
    resp = session.post(
        f"{WP_API}/{POST_TYPE}/{post_id}",
        json={"meta": {"_featured": 0}}
    )
    resp.raise_for_status()
    return True


def main():
    print("=" * 60)
    print("  revopscareers.com — Unfeature Old Job Listings")
    print(f"  Site:      {SITE_URL}")
    print(f"  Mode:      {'DRY RUN (no changes will be made)' if DRY_RUN else '⚠️  LIVE — changes WILL be made'}")
    print(f"  Threshold: {FEATURED_MAX_DAYS} day(s) — unfeature webadmin jobs older than this")
    print(f"  Protected: client-posted jobs (author != {WEBADMIN_AUTHOR_ID}) are never touched")
    print("=" * 60)

    featured = fetch_all_featured_jobs()
    if not featured:
        print("No featured jobs found.")
        sys.exit(0)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=FEATURED_MAX_DAYS)

    to_unfeature = []
    client_protected = []
    still_fresh = []

    for post in featured:
        author = post.get("author")
        post_dt = datetime.fromisoformat(post["date"]).replace(tzinfo=timezone.utc)
        age_days = (now - post_dt).days

        if author != WEBADMIN_AUTHOR_ID:
            client_protected.append((post, age_days))
        elif post_dt < cutoff:
            to_unfeature.append((post, age_days))
        else:
            still_fresh.append((post, age_days))

    print(f"\nFeatured job breakdown:")
    print(f"  Total featured:         {len(featured)}")
    print(f"  Client-posted (keep):   {len(client_protected)}")
    print(f"  Webadmin, fresh (keep): {len(still_fresh)}")
    print(f"  Webadmin, old (remove): {len(to_unfeature)}")

    if client_protected:
        print(f"\n  Protected client jobs ({len(client_protected)}):")
        for post, age in client_protected:
            print(f"    [author:{post['author']}] age:{age}d  ID:{post['id']}  {get_title(post)[:60]}")

    if still_fresh:
        print(f"\n  Fresh webadmin jobs staying featured ({len(still_fresh)}):")
        for post, age in still_fresh[:10]:
            print(f"    age:{age}d  ID:{post['id']}  {get_title(post)[:60]}")
        if len(still_fresh) > 10:
            print(f"    … and {len(still_fresh) - 10} more")

    if not to_unfeature:
        print("\n✅ No old featured jobs to unfeature.")
        sys.exit(0)

    print(f"\n  Jobs to unfeature ({len(to_unfeature)}):")
    for post, age in to_unfeature[:15]:
        print(f"    age:{age}d  ID:{post['id']}  {get_title(post)[:60]}")
    if len(to_unfeature) > 15:
        print(f"    … and {len(to_unfeature) - 15} more")

    print(f"\n{'-'*60}")
    print(f"Summary: {len(to_unfeature)} job(s) to unfeature | {len(client_protected)} client job(s) protected")

    if DRY_RUN:
        print("\n[DRY RUN] No changes made. Set DRY_RUN = False to execute.\n")
        sys.exit(0)

    print("\n⚠️  LIVE — removing featured flag…")
    errors = []
    for post, age in to_unfeature:
        try:
            unfeature_post(post["id"])
            print(f"  [✓] ID:{post['id']} (age:{age}d)  {get_title(post)[:55]}")
        except Exception as e:
            msg = f"  [✗] ID:{post['id']} FAILED: {e}"
            print(msg)
            errors.append(msg)

    print("\n" + "=" * 60)
    if errors:
        print(f"Completed with {len(errors)} error(s).")
    else:
        print(f"✅ Done. {len(to_unfeature)} job(s) unfeatured.")
    print("=" * 60)


if __name__ == "__main__":
    main()
