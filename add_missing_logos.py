#!/usr/bin/env python3
"""
Add Missing Company Logos — revopscareers.com
==============================================
Finds all published job listings with no featured image (featured_media = 0),
groups them by company name, then for each unique company:
  1. Searches Brandfetch for the company domain.
  2. Downloads the square icon from Brandfetch CDN (/icon endpoint).
  3. Uploads it once to the WordPress media library.
  4. Assigns the new media ID to all posts for that company.

Posts without a company name in meta are reported and skipped.

Usage:
    python add_missing_logos.py

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
SITE_URL             = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME          = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD      = os.environ.get("WP_APP_PASSWORD", "")
POST_TYPE            = "job-listings"
BRANDFETCH_CLIENT_ID = "1idzcKyyhEOReH2YGpD"
DRY_RUN              = False  # Set to True for a dry-run preview
SEARCH_DELAY_SECS    = 0.4   # Polite delay between Brandfetch search requests
CDN_USER_AGENT       = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
# =============================================================================

API_BASE = SITE_URL.rstrip("/") + "/wp-json"
WP_API   = API_BASE + "/wp/v2"

auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))
session = requests.Session()
session.auth = auth
session.headers.update({"Content-Type": "application/json"})

cdn_session = requests.Session()
cdn_session.headers.update({"User-Agent": CDN_USER_AGENT})

search_session = requests.Session()
search_session.headers.update({"User-Agent": CDN_USER_AGENT})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    text = re.sub(r"^-+|-+$", "", text)
    return text or "logo"


def normalize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def is_webp(data: bytes) -> bool:
    return data[:4] == b"RIFF" and data[8:12] == b"WEBP"


def is_png(data: bytes) -> bool:
    return data[:8] == b"\x89PNG\r\n\x1a\n"


def is_jpeg(data: bytes) -> bool:
    return data[:3] == b"\xff\xd8\xff"


def guess_content_type(data: bytes) -> tuple[str, str]:
    if is_webp(data):
        return "image/webp", "webp"
    if is_png(data):
        return "image/png", "png"
    if is_jpeg(data):
        return "image/jpeg", "jpg"
    return "image/png", "png"


def _get_with_retry(s: requests.Session, url: str, **kwargs) -> requests.Response:
    for attempt in range(3):
        try:
            resp = s.get(url, **kwargs)
            if resp.status_code in (502, 503, 504) and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return resp
        except requests.RequestException:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                raise
    return resp


# ---------------------------------------------------------------------------
# WordPress API calls
# ---------------------------------------------------------------------------

def fetch_all_jobs(since_days: int | None = None) -> list[dict]:
    posts, page = [], 1
    after = None
    if since_days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        after = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"Fetching '{POST_TYPE}' posts from the last {since_days} day(s) (after {after})…")
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
            params["after"] = after
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


def get_title(post: dict) -> str:
    t = post.get("title", {})
    return t.get("rendered", "") if isinstance(t, dict) else str(t)


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


def upload_media(image_bytes: bytes, filename: str, content_type: str) -> int | None:
    up = requests.Session()
    up.auth = auth
    resp = up.post(
        f"{WP_API}/media",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": content_type,
        },
        data=image_bytes,
    )
    if not resp.ok:
        print(f"      [Upload failed {resp.status_code}]: {resp.text[:200]}")
        return None
    return resp.json().get("id")


def update_post_featured_media(post_id: int, media_id: int) -> None:
    resp = session.post(f"{WP_API}/{POST_TYPE}/{post_id}", json={"featured_media": media_id})
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Brandfetch
# ---------------------------------------------------------------------------

def brandfetch_search(company_name: str) -> str | None:
    url = f"https://api.brandfetch.io/v2/search/{requests.utils.quote(company_name)}"
    try:
        resp = _get_with_retry(
            search_session,
            url,
            params={"c": BRANDFETCH_CLIENT_ID},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        results = resp.json()
        if results and isinstance(results, list):
            return results[0].get("domain")
    except Exception as e:
        print(f"      [Search error]: {e}")
    return None


def brandfetch_icon(domain: str) -> bytes | None:
    url = f"https://cdn.brandfetch.io/domain/{domain}/icon"
    try:
        resp = cdn_session.get(
            url,
            params={"c": BRANDFETCH_CLIENT_ID},
            timeout=15,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        ct = resp.headers.get("Content-Type", "")
        if not ct.startswith("image/"):
            return None
        data = resp.content
        if not (is_webp(data) or is_png(data) or is_jpeg(data)):
            return None
        return data
    except Exception as e:
        print(f"      [CDN error for {domain}]: {e}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Add missing company logos on revopscareers.com")
    parser.add_argument(
        "--since", type=int, default=None, metavar="DAYS",
        help="Only check jobs posted in the last N days (default: all jobs)"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  revopscareers.com — Add Missing Company Logos")
    print(f"  Site:  {SITE_URL}")
    print(f"  Mode:  {'DRY RUN (no changes will be made)' if DRY_RUN else 'LIVE — changes WILL be made'}")
    if args.since:
        print(f"  Scope: jobs posted in the last {args.since} day(s)")
    print("=" * 60)

    all_posts = fetch_all_jobs(since_days=args.since)
    if not all_posts:
        print("No posts found.")
        sys.exit(0)

    # Filter to posts with no featured image
    no_logo = [p for p in all_posts if not p.get("featured_media")]
    with_logo = len(all_posts) - len(no_logo)

    print(f"\nStats:")
    print(f"  Total posts:       {len(all_posts)}")
    print(f"  Posts with logo:   {with_logo}")
    print(f"  Posts without logo:{len(no_logo)}")

    if not no_logo:
        print("\n✅ All posts already have a company logo.")
        sys.exit(0)

    # Group by normalized company name
    company_posts: dict[str, list[dict]] = defaultdict(list)
    company_display: dict[str, str] = {}   # norm → raw name
    no_company_posts: list[dict] = []

    for post in no_logo:
        name = get_company_name(post)
        if not name:
            no_company_posts.append(post)
            continue
        norm = normalize(name)
        company_posts[norm].append(post)
        company_display[norm] = name

    print(f"\n  Unique companies needing a logo: {len(company_posts)}")
    print(f"  Posts with no company name:      {len(no_company_posts)}")

    if no_company_posts:
        print(f"\n  Posts skipped (no company name in meta):")
        for p in no_company_posts[:20]:
            print(f"    ID:{p['id']}  {get_title(p)[:60]}")
        if len(no_company_posts) > 20:
            print(f"    … and {len(no_company_posts) - 20} more")

    print(f"\nCompanies to process ({len(company_posts)}):")
    print("-" * 60)
    for norm, display in sorted(company_display.items()):
        post_ids = [p["id"] for p in company_posts[norm]]
        print(f"  {display[:50]:50s}  {len(post_ids)} post(s)  {post_ids[:4]}{' …' if len(post_ids)>4 else ''}")

    if DRY_RUN:
        print(f"\n[DRY RUN] Would search Brandfetch for {len(company_posts)} company/companies.")
        print("Set DRY_RUN = False to execute.\n")
        sys.exit(0)

    # Live: search + download + upload + assign
    print(f"\n⚠️  LIVE — processing {len(company_posts)} company/companies…")
    errors = []
    fixed_companies = 0
    fixed_posts = 0
    search_misses = []
    icon_misses = []

    for norm in sorted(company_posts.keys()):
        display = company_display[norm]
        posts_for_company = company_posts[norm]
        post_ids = [p["id"] for p in posts_for_company]

        print(f"\n  [{display}]  {len(post_ids)} post(s)")

        # Step 1: Brandfetch search
        time.sleep(SEARCH_DELAY_SECS)
        domain = brandfetch_search(display)
        if not domain:
            print(f"    ⚠️  Brandfetch: no result — skipping")
            search_misses.append((display, post_ids))
            continue
        print(f"    domain: {domain}")

        # Step 2: Download icon
        icon_bytes = brandfetch_icon(domain)
        if not icon_bytes:
            print(f"    ⚠️  Brandfetch icon not available for {domain} — skipping")
            icon_misses.append((display, domain, post_ids))
            continue

        content_type, ext = guess_content_type(icon_bytes)
        filename = f"{slugify(display)}.{ext}"
        print(f"    ✓ Icon: {len(icon_bytes)} bytes ({content_type})  → {filename}")

        # Step 3: Upload once
        new_media_id = upload_media(icon_bytes, filename, content_type)
        if not new_media_id:
            msg = f"    ✗ Upload failed for {display}"
            print(msg)
            errors.append(msg)
            continue
        print(f"    ✓ Uploaded → new media ID: {new_media_id}")

        # Step 4: Assign to all posts for this company
        post_errors = 0
        for post_id in post_ids:
            try:
                update_post_featured_media(post_id, new_media_id)
                print(f"      ✓ Post {post_id}")
                fixed_posts += 1
            except Exception as e:
                msg = f"      ✗ Post {post_id} FAILED: {e}"
                print(msg)
                errors.append(msg)
                post_errors += 1

        if post_errors == 0:
            fixed_companies += 1

    # Summary
    print("\n" + "=" * 60)
    print(f"  Companies fixed:   {fixed_companies}/{len(company_posts)}")
    print(f"  Posts updated:     {fixed_posts}")
    print(f"  Search misses:     {len(search_misses)}")
    print(f"  Icon misses:       {len(icon_misses)}")
    print(f"  Errors:            {len(errors)}")

    if search_misses:
        print(f"\n  No Brandfetch result:")
        for name, pids in search_misses:
            print(f"    {name}  posts:{pids}")

    if icon_misses:
        print(f"\n  Brandfetch had no icon:")
        for name, domain, pids in icon_misses:
            print(f"    {name}  ({domain})  posts:{pids}")

    if errors:
        print(f"\n  Errors:")
        for e in errors:
            print(f"    {e}")
    elif fixed_companies > 0:
        print(f"\n✅ Done.")

    print("=" * 60)


if __name__ == "__main__":
    main()
