#!/usr/bin/env python3
"""
RevPath RSS → RevOpsCareers Job Sync
====================================
Imports RevOps/GTM jobs from the RevPath RSS feed (revpath.dealhub.io) as
job_listing posts in WordPress. Replaces the n8n workflow
"RevOpsCareers - Post Jobs from RevPath RSS Feed - Optimized" (IEvTEiXzT0gL6sgW).

  1. Read the RSS feed (latest 100 jobs: title, company, location, HTML description, logo)
  2. Keep recent jobs whose title matches the RevPath title filter (same list as n8n)
  3. Skip jobs already in WordPress (application URL or company+title identity key)
  4. Per job: read the RevPath job page JSON-LD (clean title, employment type, remote flag,
     structured location) and resolve the LinkedIn application URL from the /apply redirect
  5. Logo: existing square logo in the media library → feed logo → fallback logo
  6. Create job_listing post; persist imported GUIDs to imported_revpath.json

No LLM: everything the n8n workflow asked Claude for is in the feed or the page JSON-LD.

Usage:
    python sync_revpath_jobs.py              # live run (new jobs only)
    python sync_revpath_jobs.py --dry-run    # preview only, no writes
    python sync_revpath_jobs.py --max-age 3  # only jobs published in the last N days (default 2)
"""

import argparse
import io
import json
import os
import re
import struct
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

import requests

from tagging import assign_tags, fetch_tag_ids

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

SITE_URL          = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME       = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD   = os.environ.get("WP_APP_PASSWORD", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

REVPATH_RSS_URL = "https://revpath.dealhub.io/rss/jobs"
REVPATH_UA      = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")

STATE_FILE       = Path(__file__).parent / "imported_revpath.json"
N8N_WEBHOOK_URL  = "https://n8n.tigros.io/webhook/roc-insert-row"
FALLBACK_LOGO_ID = 184  # RevOpsCareers favicon — used when no company logo is found
EXPIRY_DAYS      = 60

WP_TIMEOUT     = 60   # seconds per WP write
WP_FAIL_PAUSE  = 60   # back off after WP times out / refuses the connection
WP_MAX_FAILS   = 3    # consecutive WP failures before stopping the import

# Title must contain one of these (case-sensitive, mirrored from the n8n "Filter GTM Jobs" node)
TITLE_FILTER = [
    "Revenue Operations", "RevOps", "Chief Revenue Officer", "Marketing Operations",
    "Sales Operations", "GTM", "Success Operations", "Revenue System",
    "HubSpot Consultant", "Clay",
]
TITLE_EXCLUDE = ["Intern", "Executive Assistant"]

# Category keywords — subset of the other sync scripts' map covering the TITLE_FILTER roles
CATEGORY_KEYWORDS: dict[int, list[str]] = {
    64:   ["customer success operations", "cs operations", "cs ops", "customer operations",
           "cx operations", "customer experience operations"],
    22:   ["marketing operations", "marketing ops", "marketingops", "mops",
           "marketing automation", "marketing systems", "marketing technology", "martech"],
    23:   ["sales operations", "sales ops", "salesops", "sales enablement", "deal desk",
           "sales analytics", "sales systems", "sales strategy", "sales planning",
           "hubspot consultant", "cpq"],
    21:   ["revenue operations", "revops", "revenue ops", "rev ops", "revenue enablement",
           "revenue strategy", "revenue architect", "revenue intelligence", "revenue system"],
    1369: ["gtm", "go-to-market", "go to market"],
    19:   ["chief revenue officer"],
}

_CAT_NAMES = {64: "CS Ops", 22: "Mktg Ops", 23: "Sales Ops", 21: "Rev Ops", 1369: "GTM", 19: "Sales"}

# WordPress job-types taxonomy IDs (same as Hirebase script)
JOB_TYPE_REMOTE_FULLTIME  = 14
JOB_TYPE_REMOTE_PARTTIME  = 1283
JOB_TYPE_REMOTE_CONTRACT  = 47
JOB_TYPE_HYBRID           = 26
JOB_TYPE_INPERSON         = 43

# =============================================================================
# HTTP SESSION
# =============================================================================

API_BASE = SITE_URL.rstrip("/") + "/wp-json"
WP_API   = API_BASE + "/wp/v2"

wp = requests.Session()
wp.auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))
wp.headers.update({"Content-Type": "application/json"})

rp = requests.Session()
rp.headers.update({"User-Agent": REVPATH_UA})

tag_ids: dict[str, int] = {}  # populated in main() after session is ready

try:
    from PIL import Image as PILImage
    _HAS_PIL = True
except ImportError:
    _HAS_PIL = False

# =============================================================================
# STATE
# =============================================================================

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"imported_ids": [], "last_run": None}

def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

# =============================================================================
# REVPATH FEED + JOB PAGE
# =============================================================================

def fetch_feed() -> list[dict]:
    r = rp.get(REVPATH_RSS_URL, timeout=60)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    jobs = []
    for item in root.findall("./channel/item"):
        jobs.append({
            "guid":        (item.findtext("guid") or "").strip(),
            "rss_title":   (item.findtext("title") or "").strip(),
            "company":     (item.findtext("company") or "").strip(),
            "location":    (item.findtext("location") or "").strip(),
            "description": (item.findtext("description") or "").strip(),
            "pub_date":    (item.findtext("pubDate") or "").strip(),
            "link":        (item.findtext("link") or "").strip(),
            "logo":        (item.findtext("image") or "").strip(),
        })
    return jobs

def fetch_job_posting(link: str) -> dict:
    """JobPosting JSON-LD from the RevPath job page ({} if missing)."""
    try:
        r = rp.get(link, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"    [revpath] page error: {e}")
        return {}
    for m in re.finditer(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', r.text, re.S):
        try:
            data = json.loads(m.group(1))
        except ValueError:
            continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
    return {}

def fetch_application_url(guid: str) -> str:
    """/jobs/{guid}/apply 302-redirects to the original (LinkedIn) posting."""
    try:
        r = rp.get(f"https://revpath.dealhub.io/jobs/{guid}/apply",
                   allow_redirects=False, timeout=30)
        return r.headers.get("Location", "").strip()
    except Exception as e:
        print(f"    [revpath] apply redirect error: {e}")
        return ""

def build_job(item: dict, posting: dict, app_url: str) -> dict:
    company = item["company"]
    title = (posting.get("title") or "").strip()
    if not title:
        # RSS title is "{title} at {company}"
        title = re.sub(rf"\s+at\s+{re.escape(company)}$", "", item["rss_title"]) if company else item["rss_title"]

    addr  = ((posting.get("jobLocation") or {}).get("address") or {})
    parts = [addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")]
    # Some pages give region "United States" + country "US" — normalize and drop repeats
    parts = [{"US": "United States", "USA": "United States"}.get(p, p) for p in parts if p]
    location = ", ".join(dict.fromkeys(parts)) or re.sub(r"^Remote\s*\((.*)\)$", r"\1", item["location"])

    loc_hint  = f"{item['location']} {title}".lower()
    is_remote = posting.get("jobLocationType") == "TELECOMMUTE" or "remote" in loc_hint
    is_hybrid = not is_remote and "hybrid" in loc_hint

    org = posting.get("hiringOrganization") or {}
    same_as = org.get("sameAs") or ""

    return {
        "guid":        item["guid"],
        "title":       title,
        "company":     company,
        "location":    location,
        "is_remote":   is_remote,
        "is_hybrid":   is_hybrid,
        "employment":  (posting.get("employmentType") or "").upper(),
        "description": item["description"],
        "url":         app_url,
        "logo":        item["logo"] or org.get("logo") or "",
        "linkedin":    same_as if "linkedin.com/company" in same_as else "",
    }

# =============================================================================
# CATEGORIES / JOB TYPES
# =============================================================================

def assign_categories(title: str) -> list[int]:
    t = title.lower()
    return [cat_id for cat_id, kws in CATEGORY_KEYWORDS.items() if any(kw in t for kw in kws)]

def job_type_ids(job: dict) -> list[int]:
    if job["is_hybrid"]:
        return [JOB_TYPE_HYBRID]
    if not job["is_remote"]:
        return [JOB_TYPE_INPERSON]
    if job["employment"] == "PART_TIME":
        return [JOB_TYPE_REMOTE_PARTTIME]
    if job["employment"] in ("CONTRACTOR", "TEMPORARY"):
        return [JOB_TYPE_REMOTE_CONTRACT]
    return [JOB_TYPE_REMOTE_FULLTIME]

# =============================================================================
# LOGOS (media library lookup + upload, as in the other sync scripts)
# =============================================================================

PLACEHOLDER_LOGO_IDS: set[int] = set()

def fetch_placeholder_logo_ids() -> set[int]:
    try:
        r = requests.get(f"{API_BASE}/roc/v1/placeholder-logos", timeout=30)
        r.raise_for_status()
        return {int(i) for i in r.json()}
    except Exception as e:
        print(f"  [logo] Could not load placeholder logo IDs: {e}")
        return set()

def _png_dims(data: bytes) -> tuple[int, int]:
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        return 0, 0
    try:
        return struct.unpack(">II", data[16:24])
    except Exception:
        return 0, 0

def image_is_square(data: bytes) -> bool:
    if _HAS_PIL:
        try:
            w, h = PILImage.open(io.BytesIO(data)).size
            return w > 0 and h > 0 and abs(w - h) <= max(w, h) * 0.05
        except Exception:
            return True
    w, h = _png_dims(data)
    if w > 0 and h > 0:
        return abs(w - h) <= max(w, h) * 0.05
    return True

def find_existing_square_logo(company_slug: str) -> int | None:
    try:
        r = wp.get(f"{WP_API}/media", params={
            "search": company_slug, "per_page": 20,
            "_fields": "id,title,source_url,media_details",
        }, timeout=20)
        r.raise_for_status()
        items = r.json()
    except Exception:
        return None
    for item in sorted(items, key=lambda x: x.get("id", 0)):
        if item.get("id") in PLACEHOLDER_LOGO_IDS:
            continue
        d = item.get("media_details") or {}
        w, h = d.get("width", 0), d.get("height", 0)
        if (w > 0 and h > 0 and abs(w - h) <= max(w, h) * 0.05) or (w == 0 and h == 0):
            return item["id"]
    return None

def upload_image_to_wp(img_bytes: bytes, filename: str, mime: str, alt_text: str) -> int | None:
    try:
        resp = requests.post(
            f"{WP_API}/media", auth=wp.auth,
            headers={"Content-Disposition": f'attachment; filename="{filename}"',
                     "Content-Type": mime},
            data=img_bytes, timeout=30,
        )
        if resp.status_code in (200, 201):
            media_id = resp.json()["id"]
            wp.post(f"{WP_API}/media/{media_id}", json={"alt_text": alt_text}, timeout=15)
            return media_id
        print(f"    [logo] Upload failed ({resp.status_code}): {resp.text[:200]}")
    except Exception as e:
        print(f"    [logo] Upload error: {e}")
    return None

def resolve_logo(company: str, logo_url: str, dry_run: bool) -> int | None:
    slug = re.sub(r"[^a-z0-9]+", "-", company.lower()).strip("-")
    existing_id = find_existing_square_logo(slug)
    if existing_id or dry_run or not logo_url:
        return existing_id
    try:
        r = requests.get(logo_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    except Exception:
        return None
    mime = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
    if r.status_code != 200 or mime not in ("image/jpeg", "image/png", "image/webp", "image/gif"):
        return None
    if not image_is_square(r.content):
        print(f"    [logo] feed logo not square for {company}")
        return None
    ext = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif"}[mime]
    media_id = upload_image_to_wp(r.content, f"{slug}-logo.{ext}", mime, company)
    if media_id:
        print(f"    [logo] feed logo → media_id={media_id}")
    return media_id

# =============================================================================
# N8N DATA TABLE SYNC (as in the other sync scripts)
# =============================================================================

def notify_n8n(table: str, payload: dict) -> None:
    try:
        resp = requests.post(N8N_WEBHOOK_URL, json={"table": table, **payload}, timeout=10)
        if resp.status_code != 200:
            print(f"    [n8n] {table} upsert warn ({resp.status_code})")
    except Exception as e:
        print(f"    [n8n] {table} upsert error: {e}")

# =============================================================================
# WP JOB CREATION
# =============================================================================

def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9\-]", "", str(s).lower().replace(" ", "-"))

def build_slug(job: dict) -> str:
    """{company}-{title}-{city}-{region}-{country}, as Hirebase / the old n8n flow."""
    parts = [_slugify(job["company"]), _slugify(job["title"])]
    parts += [_slugify(p.strip()) for p in job["location"].split(",")[:3]]
    slug = "-".join(p for p in parts if p)
    return re.sub(r"-{2,}", "-", slug).strip("-")[:200]

def _norm_key(s: str) -> str:
    """Normalize a string for dedup key comparison (lowercase, strip punctuation)."""
    if not s:
        return ""
    s = re.sub(r"[^\w\s]", "", s.lower().strip())
    return re.sub(r"\s+", " ", s)

def job_key(job: dict) -> tuple[str, str]:
    """(company, title) identity — the same job posted in several cities collapses into one post."""
    return (_norm_key(job["company"]), _norm_key(job["title"]))

_wp_fails = 0  # consecutive WP timeouts / connection errors

def wp_write(url: str, payload: dict) -> requests.Response | None:
    """POST to WP. On timeout/connection error: back off and return None (no retry —
    a timed-out create may still have gone through; the next run's dedup catches it)."""
    global _wp_fails
    try:
        resp = wp.post(url, json=payload, timeout=WP_TIMEOUT)
    except (requests.Timeout, requests.ConnectionError) as e:
        _wp_fails += 1
        print(f"    [wp] {type(e).__name__} ({_wp_fails}/{WP_MAX_FAILS}) — pausing {WP_FAIL_PAUSE}s")
        time.sleep(WP_FAIL_PAUSE)
        return None
    _wp_fails = 0
    return resp

def wp_down() -> bool:
    return _wp_fails >= WP_MAX_FAILS

def create_wp_job(job: dict, category_ids: list[int], media_id: int, dry_run: bool) -> dict | None:
    payload: dict = {
        "title":   job["title"],
        "slug":    build_slug(job),
        "content": job["description"],
        "status":  "publish",
        "job-categories": category_ids,
        "job-types":      job_type_ids(job),
        "featured_media": media_id,
        "meta": {
            "_job_location":     job["location"] or ("Remote" if job["is_remote"] else ""),
            "_application":      job["url"],
            "_company_name":     job["company"],
            "_company_linkedin": job["linkedin"],
            "_remote_position":  1 if job["is_remote"] else 0,
            "_job_expires":      (datetime.now() + timedelta(days=EXPIRY_DAYS)).strftime("%Y-%m-%d"),
            "_featured":         1 if 21 in category_ids else 0,
            "_filled":           0,
        },
    }

    tag_term_ids = assign_tags(job["title"], job["description"], job["company"],
                               tag_ids, ANTHROPIC_API_KEY)
    if tag_term_ids:
        payload["job_listing_tag"] = tag_term_ids

    if dry_run:
        return {"dry_run": True}

    resp = wp_write(f"{WP_API}/job-listings", payload)
    if resp is None:
        return None
    if resp.status_code in (200, 201):
        return resp.json()
    print(f"    [wp] Create failed ({resp.status_code}): {resp.text[:300]}")
    return None

def load_existing_dedup_keys() -> tuple[set[str], set[tuple[str, str]]]:
    print("Loading existing application URLs from WordPress...")
    try:
        r = wp.get(f"{API_BASE}/roc/v1/job-dedup-keys", timeout=120)
        r.raise_for_status()
        rows = r.json()
    except Exception as e:
        print(f"  ERROR: could not load existing jobs: {e}")
        sys.exit(1)
    urls: set[str] = set()
    keys: set[tuple[str, str]] = set()
    for row in rows:
        app = row.get("application") or ""
        if app:
            urls.add(str(app).strip())
        company, title = _norm_key(row.get("company") or ""), _norm_key(row.get("title") or "")
        if company and title:
            keys.add((company, title))
    print(f"  {len(urls)} existing application URLs + {len(keys)} identity keys loaded.\n")
    return urls, keys

# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Sync RevPath RSS jobs → RevOpsCareers WordPress")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only — no writes to WordPress or media library")
    parser.add_argument("--max-age", type=int, default=2, metavar="DAYS",
                        help="Only jobs published in the last N days (default 2)")
    args = parser.parse_args()

    if not WP_APP_PASSWORD:
        sys.exit("ERROR: WP_APP_PASSWORD not set in .env")

    state = load_state()
    imported_ids: set[str] = set(state["imported_ids"])
    cutoff = (datetime.now() - timedelta(days=args.max_age)).strftime("%Y-%m-%d %H:%M:%S")

    print("RevOpsCareers ← RevPath RSS sync")
    print(f"  Site:        {SITE_URL}")
    print(f"  Dry run:     {args.dry_run}")
    print(f"  Max age:     {args.max_age} days")
    print(f"  Local state: {len(imported_ids)} previously imported GUIDs")
    print()

    try:
        items = fetch_feed()
    except Exception as e:
        sys.exit(f"ERROR: could not read RevPath feed: {e}")
    print(f"Feed: {len(items)} items\n")

    existing_urls, existing_keys = load_existing_dedup_keys()

    global tag_ids
    tag_ids = fetch_tag_ids(wp, WP_API)
    PLACEHOLDER_LOGO_IDS.update(fetch_placeholder_logo_ids())

    new_count = skip_count = error_count = 0
    logo_cache: dict[str, int | None] = {}

    for item in items:
        if wp_down():
            break
        title = item["rss_title"]
        if (item["guid"] in imported_ids or item["pub_date"] < cutoff
                or not any(t in title for t in TITLE_FILTER)
                or any(t in title for t in TITLE_EXCLUDE)):
            skip_count += 1
            continue

        job = build_job(item, fetch_job_posting(item["link"]), fetch_application_url(item["guid"]))
        if not job["url"] or not job["company"]:
            print(f"  ! {title} — no application URL or company, skipped")
            error_count += 1
            continue
        if job["url"] in existing_urls or job_key(job) in existing_keys:
            skip_count += 1
            imported_ids.add(job["guid"])
            continue

        category_ids = assign_categories(job["title"])
        if not category_ids:
            print(f"  - {job['title']} @ {job['company']} — no category match, skipped")
            skip_count += 1
            continue

        jt = {JOB_TYPE_HYBRID: "hybrid", JOB_TYPE_INPERSON: "on-site"}.get(job_type_ids(job)[0], "remote")
        cats = ", ".join(_CAT_NAMES[c] for c in category_ids)
        print(f"  + {job['title']} @ {job['company']}  [{cats}]  {job['location']}  ({jt})")

        if job["company"] not in logo_cache:
            logo_cache[job["company"]] = resolve_logo(job["company"], job["logo"], args.dry_run)
            if logo_cache[job["company"]] and not args.dry_run:
                notify_n8n("logos", {"company_name": job["company"],
                                     "logo_id": logo_cache[job["company"]], "logo_url": ""})
        media_id = logo_cache[job["company"]] or FALLBACK_LOGO_ID

        result = create_wp_job(job, category_ids, media_id, args.dry_run)
        if not result:
            error_count += 1
            continue
        new_count += 1
        existing_urls.add(job["url"])
        existing_keys.add(job_key(job))
        if not args.dry_run:
            print(f"    [wp] post ID {result.get('id', '?')}")
            imported_ids.add(job["guid"])
            notify_n8n("jobs", {
                "job_title":        job["title"],
                "company_name":     job["company"],
                "job_url":          result.get("link", ""),
                "application_link": job["url"],
                "posted_at":        datetime.now().strftime("%Y-%m-%d"),
            })
            state["imported_ids"] = sorted(imported_ids)
            save_state(state)
        time.sleep(1)  # polite pacing for RevPath + WP

    if not args.dry_run:
        state["imported_ids"] = sorted(imported_ids)
        state["last_run"] = datetime.now().isoformat()
        save_state(state)

    print()
    print("Done.")
    print(f"  Imported: {new_count}")
    print(f"  Skipped:  {skip_count}  (already imported, too old, off-topic, or duplicate)")
    print(f"  Errors:   {error_count}")

    if wp_down():
        print(f"ERROR: WordPress failed to respond {WP_MAX_FAILS} times in a row — import stopped early.")
        sys.exit(1)


if __name__ == "__main__":
    main()
