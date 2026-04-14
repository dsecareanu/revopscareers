#!/usr/bin/env python3
"""
Fix Logo ALT Text — revopscareers.com
======================================
Scans all published job listings with a featured image (company logo) and
ensures each media item has the company name set as its ALT text.

For each unique media ID across all job listings:
  1. Looks up the company name from the posts that use that media item.
  2. Fetches the current alt_text for the media item.
  3. If alt_text is missing or doesn't match the company name, updates it.

Usage:
    python fix_logo_alt_text.py

Configuration:
    Edit the CONFIG section below before running.

Requirements:
    pip install requests
"""

import argparse
import os
import requests
import sys
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# =============================================================================
# CONFIG — loaded from environment variables (or .env file)
# =============================================================================
SITE_URL        = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME     = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD = os.environ.get("WP_APP_PASSWORD", "")
POST_TYPE       = "job-listings"
DRY_RUN         = False  # Live mode — set to True for a dry-run preview
REQUEST_DELAY   = 0.1    # Seconds between media update requests
# =============================================================================

API_BASE = SITE_URL.rstrip("/") + "/wp-json"
WP_API   = API_BASE + "/wp/v2"

auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))
session = requests.Session()
session.auth = auth
session.headers.update({"Content-Type": "application/json"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def get_company_name(post: dict) -> str:
    meta = post.get("meta", {})
    if not isinstance(meta, dict):
        return ""
    for key in ("_company_name", "company_name", "_job_company"):
        val = meta.get(key)
        if val:
            if isinstance(val, list):
                val = val[0] if val else ""
            return str(val).strip()
    return ""


def get_title(post: dict) -> str:
    t = post.get("title", {})
    return t.get("rendered", "") if isinstance(t, dict) else str(t)


# ---------------------------------------------------------------------------
# WordPress API calls
# ---------------------------------------------------------------------------

def fetch_all_jobs(since_days: int | None = None) -> list[dict]:
    posts, page = [], 1
    after = None
    if since_days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        after = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"Fetching '{POST_TYPE}' posts modified in the last {since_days} day(s) (after {after})…")
    else:
        print(f"Fetching all '{POST_TYPE}' posts…")
    while True:
        params = {
            "per_page": 100,
            "page": page,
            "status": "publish",
            "_fields": "id,title,featured_media,meta",
        }
        if after:
            params["modified_after"] = after
        resp = session.get(f"{WP_API}/{POST_TYPE}", params=params)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        posts.extend(batch)
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        print(f"  Page {page}/{total_pages} — {len(posts)} posts", end="\r")
        if page >= total_pages:
            break
        page += 1
    print(f"\n  Done. {len(posts)} total posts.")
    return posts


def fetch_media_alt(media_id: int) -> str | None:
    """Fetch current alt_text for a media item. Returns None on error."""
    try:
        resp = session.get(
            f"{WP_API}/media/{media_id}",
            params={"_fields": "id,alt_text,title"}
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json().get("alt_text", "")
    except Exception as e:
        print(f"    [Fetch error for media {media_id}]: {e}")
        return None


def update_media_alt(media_id: int, alt_text: str) -> bool:
    """Update alt_text on a media item. Returns True on success."""
    try:
        resp = session.post(
            f"{WP_API}/media/{media_id}",
            json={"alt_text": alt_text}
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"    [Update error for media {media_id}]: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fix logo ALT text on revopscareers.com")
    parser.add_argument(
        "--since", type=int, default=None, metavar="DAYS",
        help="Only check logos on jobs modified in the last N days (default: all jobs)"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  revopscareers.com — Fix Logo ALT Text")
    print(f"  Site:  {SITE_URL}")
    print(f"  Mode:  {'DRY RUN (no changes will be made)' if DRY_RUN else '⚠️  LIVE — changes WILL be made'}")
    if args.since:
        print(f"  Scope: jobs modified in the last {args.since} day(s)")
    else:
        print(f"  Scope: all jobs")
    print("=" * 60)

    all_posts = fetch_all_jobs(since_days=args.since)
    if not all_posts:
        print("No posts found.")
        sys.exit(0)

    # Build: media_id → best company name (from first post that has one)
    media_company: dict[int, str] = {}
    media_posts: dict[int, list[int]] = defaultdict(list)
    no_logo = 0

    for post in all_posts:
        mid = post.get("featured_media", 0)
        if not mid:
            no_logo += 1
            continue
        media_posts[mid].append(post["id"])
        if mid not in media_company:
            name = get_company_name(post)
            if name:
                media_company[mid] = name

    unique_media = sorted(media_posts.keys())
    with_name = sum(1 for mid in unique_media if mid in media_company)
    without_name = len(unique_media) - with_name

    print(f"\nStats:")
    print(f"  Posts with a logo:    {len(all_posts) - no_logo}")
    print(f"  Posts without a logo: {no_logo}")
    print(f"  Unique media items:   {len(unique_media)}")
    print(f"  With company name:    {with_name}")
    print(f"  Without company name: {without_name} (will be skipped)")

    if not unique_media:
        print("\nNo logos to process.")
        sys.exit(0)

    # Check current ALT text and decide what needs updating
    print(f"\nChecking ALT text for {len(unique_media)} media items…")
    to_update: list[tuple[int, str, str]] = []  # (media_id, current_alt, desired_alt)
    already_correct = 0
    skipped_no_name = 0
    fetch_errors = 0

    for i, mid in enumerate(unique_media, 1):
        print(f"  [{i}/{len(unique_media)}] checking media {mid}   ", end="\r")

        desired = media_company.get(mid, "")
        if not desired:
            skipped_no_name += 1
            continue

        current = fetch_media_alt(mid)
        if current is None:
            fetch_errors += 1
            continue

        # Compare case-insensitively; update if different
        if normalize(current) == normalize(desired):
            already_correct += 1
        else:
            to_update.append((mid, current, desired))

    print(f"\n  Already correct: {already_correct}")
    print(f"  Needs update:    {len(to_update)}")
    print(f"  Skipped (no company name): {skipped_no_name}")
    print(f"  Fetch errors:    {fetch_errors}")

    if not to_update:
        print("\n✅ All logo ALT texts are already correct.")
        sys.exit(0)

    print(f"\nItems to update ({len(to_update)}):")
    print("-" * 60)
    for mid, current, desired in to_update[:30]:
        pids = media_posts[mid]
        current_display = f'"{current}"' if current else "(empty)"
        print(f"  media:{mid}  {current_display} → \"{desired}\"  posts:{pids[:3]}{'…' if len(pids)>3 else ''}")
    if len(to_update) > 30:
        print(f"  … and {len(to_update) - 30} more")

    if DRY_RUN:
        print(f"\n[DRY RUN] Would update {len(to_update)} media item(s).")
        print("Set DRY_RUN = False to execute.\n")
        sys.exit(0)

    # Live: apply updates
    print(f"\n⚠️  LIVE — updating {len(to_update)} media item(s)…")
    updated = 0
    errors = []

    for mid, current, desired in to_update:
        time.sleep(REQUEST_DELAY)
        ok = update_media_alt(mid, desired)
        if ok:
            print(f"  ✓ media:{mid}  → \"{desired}\"")
            updated += 1
        else:
            msg = f"  ✗ media:{mid} FAILED"
            print(msg)
            errors.append(msg)

    print("\n" + "=" * 60)
    print(f"  Updated:  {updated}/{len(to_update)}")
    print(f"  Errors:   {len(errors)}")
    if errors:
        print("\n  Errors:")
        for e in errors:
            print(f"    {e}")
    elif updated > 0:
        print(f"\n✅ Done.")
    print("=" * 60)


if __name__ == "__main__":
    main()
