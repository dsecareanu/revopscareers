#!/usr/bin/env python3
"""
Lensa Connect → RevOpsCareers Job Sync
=======================================
Fetches US RevOps/GTM job listings from the Lensa Connect API and
imports them as job_listing posts in WordPress. Full workflow:

  1. Loop over job title keywords (two passes each: standard + remote_only)
  2. Paginate via offset (100 jobs/call, hard limit 180/call)
  3. Skip jobs already in WordPress (by incoming_click_url — checked at startup)
  4. Skip jobs already imported in this run (dedup by unique_id)
  5. Apply title/company blocklists and require a matching category
  6. Per job: resolve best logo via Brandfetch/icon.horse
  7. Create job_listing post with full meta + auto-assigned category
  8. Persist imported IDs to imported_lensa.json so re-runs only import new jobs

Key differences vs. WhatJobs:
  - JSON API (not XML)
  - Deduplication by unique_id (not application URL)
  - Two passes per keyword: standard (US-wide) + remote_only=true
  - Remote jobs mapped to remote job type + _remote_position=1
  - No date field in API response — no age filtering possible
  - description_digest (truncated) used as job content

Usage:
    python sync_lensa_jobs.py              # live run (new jobs only)
    python sync_lensa_jobs.py --dry-run    # preview only, no writes
    python sync_lensa_jobs.py --keywords 3 # limit to first N keywords
    python sync_lensa_jobs.py --reset      # clear state, re-import all
    python sync_lensa_jobs.py --skip-logo  # skip logo resolution (faster)
    python sync_lensa_jobs.py --check-logos # resolve logos even during --dry-run

Requirements:
    pip install requests
    pip install Pillow   (optional — enables reliable image squareness checks)
"""

import argparse
import io
import json
import os
import re
import struct
import sys
import time
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

SITE_URL        = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME     = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD   = os.environ.get("WP_APP_PASSWORD", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Lensa Connect config
LENSA_SDK_KEY   = os.environ.get("LENSA_SDK_KEY", "")
LENSA_CAMPAIGN  = os.environ.get("LENSA_CAMPAIGN_ID", "")
LENSA_API_URL   = "https://connect.lensa.com/jobs-api/v1/job-adverts"

# Job titles to query — each runs as a separate API call (standard + remote pass)
LENSA_JOB_TITLES = [
    # Revenue Operations
    "Revenue Operations",
    "RevOps",
    "Chief Revenue Officer",
    "Revenue Enablement",
    "Revenue Strategy",
    # Sales Operations & Enablement
    "Sales Operations",
    "Sales Enablement",
    "Deal Desk",
    "Sales Manager",
    "Sales Director",
    "VP Sales",
    "Inside Sales",
    # SDR / BDR
    "Sales Development Representative",
    "Business Development Representative",
    # AE / AM
    "Account Executive",
    "Account Manager",
    "Sales Engineer",
    "Solutions Engineer",
    # Business Development
    "Business Development",
    "Partnerships Manager",
    # CRM & Systems
    "Salesforce Administrator",
    "CRM Administrator",
    "HubSpot Administrator",
    "Marketing Technology",
    # Marketing Operations
    "Marketing Operations",
    "Marketing Automation",
    "Martech",
    # Demand Generation
    "Demand Generation",
    "Account-Based Marketing",
    # Marketing Leadership & General
    "Marketing Manager",
    "Digital Marketing",
    "Content Marketing",
    "Email Marketing",
    "Lifecycle Marketing",
    "Brand Manager",
    "Field Marketing",
    "Partner Marketing",
    "Channel Marketing",
    # Product Marketing
    "Product Marketing",
    "Competitive Intelligence",
    "Analyst Relations",
    # GTM
    "GTM Operations",
    "Go-to-Market",
    # Growth
    "Growth Marketing",
    "Growth Manager",
    "User Acquisition",
    # Customer Success
    "Customer Success Operations",
    "Customer Success Manager",
    "Customer Experience",
    "Implementation Manager",
    "Technical Account Manager",
    "Customer Onboarding",
    # Data & Analytics
    "Data Analyst",
    "Business Intelligence",
    "Analytics Engineer",
    "Data Engineer",
    "Data Scientist",
    # Finance Operations
    "Financial Operations",
    "FP&A",
    "Pricing Analyst",
    "Revenue Accounting",
    "Quote to Cash",
    # Web Operations
    "Web Operations",
    "SEO Manager",
    "Conversion Optimization",
    # Business Operations
    "Business Operations",
    # People Operations
    "People Operations",
    # Product Operations
    "Product Operations",
    # Ad Operations
    "Ad Operations",
]

STATE_FILE      = Path(__file__).parent / "imported_lensa.json"
N8N_WEBHOOK_URL = "https://n8n.tigros.io/webhook/roc-insert-row"
FALLBACK_LOGO_ID = 184  # RevOpsCareers favicon — used when no company logo is found

PAGE_SIZE      = 100   # per API call (hard max is 180)
EXPIRY_DAYS    = 60    # days until job expires on the site

WP_BATCH_SIZE  = 10
WP_BATCH_PAUSE = 2.0

ICON_HORSE_CDN = "https://icon.horse/icon/{domain}"
BRANDFETCH_CDN = "https://cdn.brandfetch.io/domain/{domain}"

# ---------------------------------------------------------------------------
# Blocklists (mirrored from WhatJobs script)
# ---------------------------------------------------------------------------
TITLE_BLOCKLIST = [
    "cdl", "truck driver", "class-a driver", "class a driver",
    "forklift", "warehouse", "manufacturing", "assembly technician",
    "wire harness", "assembler",
    "electrician", "plumber", "hvac", "carpenter", "welder",
    "nurse", "physician", "doctor", "pharmacist", "dental",
    "teacher", "tutor", "professor", "instructor",
    "real estate agent", "realtor",
    "sailor", "deckhand", "marine crew",
    # Junior / entry-level / intern roles (site targets mid-to-senior professionals)
    "junior", " jr ", " jr.", "intern",
]

COMPANY_BLOCKLIST = [
    "salmonjobs",
    "ismira recruitment",
]

COMPANY_INDUSTRY_BLOCKLIST = [
    "hilton", "marriott", "hyatt", "sheraton", "westin", "waldorf",
    "radisson", "accor", "ihg", "intercontinental", "crowne plaza",
    "holiday inn", "four seasons", "ritz-carlton", "ritz carlton",
    "wyndham", "best western", "novotel", "sofitel", "ibis hotel",
    "kimpton", "loews hotel", "omni hotel", "fairmont",
    " resort", " resorts", " hotel", " hotels", " lodge", " inn ",
    " casino", " spa ", "cruise line",
]

# ---------------------------------------------------------------------------
# WordPress category IDs (identical to WhatJobs script)
# ---------------------------------------------------------------------------
CATEGORY_KEYWORDS: dict[int, list[str]] = {
    64:   ["customer success operations", "cs operations", "cs ops", "customer operations",
           "cx operations", "customer experience operations"],
    22:   ["marketing operations", "marketing ops", "marketingops", "mops",
           "marketing automation", "marketing analytics", "marketing systems",
           "marketing technology", "martech", "marketing enablement",
           "marketing intelligence", "demand generation operations", "crm marketing",
           "marketing data"],
    23:   ["sales operations", "sales ops", "salesops", "sales enablement", "deal desk",
           "sales analytics", "sales systems", "sales technology", "salestech",
           "sales reporting", "sales process", "sales strategy", "sales planning"],
    21:   ["revenue operations", "revops", "revenue ops", "rev ops", "revenue enablement",
           "revenue strategy", "revenue architect", "revenue intelligence",
           "revenue finance", "revenue accounting"],
    284:  ["business operations"],
    25:   ["business development", "business development manager",
           "business development director", "alliances", "channel partner",
           "partner success", "partnerships manager", "strategic partnerships",
           "partnerships", "partner manager", "technology partnerships"],
    1434: ["product marketing", "product marketing manager", "pmm", "product marketer",
           "product evangelist", "solutions marketing", "technical marketing",
           "analyst relations", "market intelligence", "product messaging",
           "competitive intelligence"],
    1436: ["partner marketing", "channel marketing", "alliance marketing",
           "co-marketing", "ecosystem marketing"],
    1435: ["web operations", "webops", "website operations", "web developer",
           "web manager", "digital operations", "conversion optimization",
           "cro specialist", "cro analyst", "seo manager", "sem manager", "seo", "sem",
           "landing page optimization", "website manager", "a/b testing",
           "paid search manager", "paid search"],
    1437: ["finance operations", "financial operations", "finops", "billing operations",
           "finance systems", "fp&a", "financial planning", "financial analyst",
           "billing manager", "pricing operations", "finance transformation",
           "revenue finance"],
    20:   ["customer success manager", "customer success director", "csm",
           "customer success lead", "vp customer success", "head of customer success",
           "chief customer officer", "customer experience", "account management",
           "customer onboarding", "customer retention", "customer lifecycle",
           "cs strategy", "cx ops", "customer ops"],
    500:  ["data analyst", "data scientist", "analytics manager", "business intelligence",
           "bi analyst", "revenue analyst", "sales analyst", "marketing analyst",
           "data engineer", "analytics engineer", "data operations", "data strategy",
           "bi engineer", "insights manager", "data platform", "analytics lead",
           "data product"],
    1369: ["gtm", "go-to-market", "go to market", "gtm strategy", "gtm operations",
           "gtm lead", "market entry", "go to market strategy", "launch strategy"],
    1368: ["growth", "growth manager", "growth lead", "growth hacker",
           "growth operations", "growth strategy", "growth analyst", "growth engineer",
           "user acquisition"],
    13:   ["marketing manager", "marketing director", "growth marketing", "digital marketing",
           "content marketing", "marketing lead", "vp marketing", "head of marketing",
           "cmo", "chief marketing officer", "lifecycle marketing", "retention marketing",
           "brand marketing", "performance marketing", "email marketing", "paid media",
           "field marketing", "communications", "brand manager", "communications manager",
           "events manager", "pr manager", "field marketer"],
    19:   ["sales manager", "sales director", "account executive", "account manager",
           "sales engineer", "solutions engineer", "sales lead", "vp sales",
           "head of sales", "sales development", "sdr manager", "bdr manager",
           "sdr", "bdr", "sales development representative", "business development representative",
           "inside sales", "outbound sales", "revenue leader", "head of revenue",
           "chief revenue officer", "cro", "vp revenue"],
    1449: ["sales enablement", "enablement manager", "revenue enablement manager",
           "field enablement", "sales enablement manager", "sales enablement specialist",
           "sales onboarding", "enablement specialist", "sales readiness",
           "sales training", "sales coaching", "sales productivity"],
    1450: ["demand generation", "demand gen", "demand generation manager",
           "demand generation specialist", "pipeline marketing", "abm",
           "account-based marketing", "account based marketing",
           "pipeline generation", "inbound marketing", "outbound marketing",
           "lead generation", "lead gen", "marketing qualified lead", "mql"],
    1453: ["people operations", "people ops", "people operations manager",
           "people operations specialist", "people operations director",
           "people operations lead"],
    1454: ["product operations", "product ops", "product operations manager",
           "product operations specialist", "product operations analyst",
           "product operations lead", "head of product operations"],
    1455: ["ad operations", "ad ops", "advertising operations",
           "ad operations manager", "ad operations specialist",
           "advertising operations manager", "programmatic operations",
           "campaign operations", "media operations"],
}

# WordPress job type IDs — verify these match your WP Job Manager setup
JOB_TYPE_INPERSON = 43
JOB_TYPE_REMOTE   = 14   # Remote Full-time (confirmed from Hirebase script)

_CAT_NAMES = {
    64:"CS Ops", 22:"Mktg Ops", 23:"Sales Ops", 21:"Rev Ops",
    284:"Biz Ops", 25:"Biz Dev", 1434:"Product Mktg", 1436:"Partner Mktg",
    1435:"Web Ops", 1437:"Finance Ops", 20:"Cust Success", 500:"Data/Analytics",
    1369:"GTM", 1368:"Growth", 13:"Marketing", 19:"Sales",
    1449:"Sales Enablement", 1450:"Demand Gen",
    1453:"People Ops", 1454:"Product Ops", 1455:"Ad Ops",
}

# =============================================================================
# HTTP SESSION
# =============================================================================

API_BASE = SITE_URL.rstrip("/") + "/wp-json"
WP_API   = API_BASE + "/wp/v2"

wp = requests.Session()
wp.auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))
wp.headers.update({"Content-Type": "application/json"})

tag_ids: dict[str, int] = {}  # populated in main() after session is ready

# =============================================================================
# PIL (optional)
# =============================================================================

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
        state = json.loads(STATE_FILE.read_text())
        if "logo_ids" not in state:
            state["logo_ids"] = {}
        if "imported_ids" not in state:
            state["imported_ids"] = []
        return state
    return {"imported_ids": [], "last_run": None, "logo_ids": {}}

def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

# =============================================================================
# IMAGE UTILITIES (identical to WhatJobs script)
# =============================================================================

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
            img = PILImage.open(io.BytesIO(data))
            w, h = img.size
            return w > 0 and h > 0 and abs(w - h) <= max(w, h) * 0.05
        except Exception:
            return True
    w, h = _png_dims(data)
    if w > 0 and h > 0:
        return abs(w - h) <= max(w, h) * 0.05
    return True

# =============================================================================
# CATEGORIZATION
# =============================================================================

def assign_categories(title: str) -> list[int]:
    t = title.lower()
    matched = set()
    for cat_id, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in t for kw in keywords):
            matched.add(cat_id)
    return list(matched)

# =============================================================================
# WP MEDIA LIBRARY (identical to WhatJobs script)
# =============================================================================

def search_wp_media(company_slug: str) -> list[dict]:
    try:
        r = wp.get(f"{WP_API}/media", params={
            "search": company_slug,
            "per_page": 20,
            "_fields": "id,title,source_url,media_details",
        }, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []

def find_existing_square_logo(company_slug: str) -> int | None:
    items = search_wp_media(company_slug)
    for item in sorted(items, key=lambda x: x.get("id", 0)):
        d = item.get("media_details") or {}
        w, h = d.get("width", 0), d.get("height", 0)
        if (w > 0 and h > 0 and abs(w - h) <= max(w, h) * 0.05) or (w == 0 and h == 0):
            return item["id"]
    return None

def upload_image_to_wp(img_bytes: bytes, filename: str, mime: str,
                       alt_text: str = "") -> int | None:
    try:
        r = requests.Session()
        r.auth = wp.auth
        resp = r.post(
            f"{WP_API}/media",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Type": mime,
                "Content-Length": str(len(img_bytes)),
            },
            data=img_bytes,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            media_id = resp.json()["id"]
            if alt_text:
                wp.post(f"{WP_API}/media/{media_id}",
                        json={"alt_text": alt_text}, timeout=15)
            return media_id
        print(f"    [logo] Upload failed ({resp.status_code}): {resp.text[:200]}")
    except Exception as e:
        print(f"    [logo] Upload error: {e}")
    return None

# =============================================================================
# N8N DATA TABLE SYNC (identical to WhatJobs script)
# =============================================================================

def notify_n8n(table: str, payload: dict) -> None:
    try:
        resp = requests.post(
            N8N_WEBHOOK_URL,
            json={"table": table, **payload},
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"    [n8n] {table} upsert warn ({resp.status_code})")
    except Exception as e:
        print(f"    [n8n] {table} upsert error: {e}")

# =============================================================================
# LOGO RESOLUTION PIPELINE (identical to WhatJobs script)
# =============================================================================

def _fetch_url(url: str) -> tuple[bytes | None, str]:
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            if ct.startswith("image/"):
                return r.content, ct
    except Exception:
        pass
    return None, ""

def _mime_to_ext(mime: str) -> str:
    return {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp",
            "image/gif": "gif"}.get(mime, "jpg")

def resolve_logo(company_name: str, company_domain: str, dry_run: bool) -> int | None:
    slug = re.sub(r"[^a-z0-9]+", "-", company_name.lower()).strip("-")

    existing_id = find_existing_square_logo(slug)
    if existing_id:
        return existing_id

    if dry_run:
        return None

    def try_upload(url: str, label: str) -> int | None:
        if not url:
            return None
        data, mime = _fetch_url(url)
        if not data:
            return None
        if mime == "image/svg+xml":
            return None
        if image_is_square(data):
            ext      = _mime_to_ext(mime)
            media_id = upload_image_to_wp(data, f"{slug}-logo.{ext}", mime,
                                          alt_text=company_name)
            if media_id:
                print(f"    [logo] {label} → media_id={media_id} ({len(data)//1024}KB)")
                return media_id
        else:
            print(f"    [logo] {label} not square — trying next source")
        return None

    if company_domain:
        media_id = try_upload(ICON_HORSE_CDN.format(domain=company_domain),
                              f"icon.horse ({company_domain})")
        if media_id:
            return media_id
        media_id = try_upload(BRANDFETCH_CDN.format(domain=company_domain),
                              f"Brandfetch ({company_domain})")
        if media_id:
            return media_id

    print(f"    [logo] No usable logo found for {company_name}")
    return None

# =============================================================================
# DOMAIN EXTRACTION (identical to WhatJobs script)
# =============================================================================

def extract_domain(company_name: str) -> str:
    name = re.sub(r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|group|holdings|technologies?)\b",
                  "", company_name, flags=re.IGNORECASE)
    name = re.sub(r"[^a-z0-9]+", "", name.lower())
    return f"{name}.com" if name else ""

# =============================================================================
# LENSA API FETCH
# =============================================================================

def fetch_lensa_page(job_title: str, remote_only: bool, offset: int) -> dict:
    """Fetch one page from Lensa Connect API. Returns parsed JSON response."""
    params = {
        "sdk_access_key": LENSA_SDK_KEY,
        "campaign_id":    LENSA_CAMPAIGN,
        "job_title":      job_title,
        "limit":          PAGE_SIZE,
        "offset":         offset,
    }
    if remote_only:
        params["remote_only"] = "true"

    r = requests.get(LENSA_API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_all_lensa(job_title: str, remote_only: bool) -> list[dict]:
    """Paginate through all results for a given job_title + remote flag."""
    jobs   = []
    offset = 0
    label  = f"{job_title} [remote={remote_only}]"

    while True:
        try:
            data = fetch_lensa_page(job_title, remote_only, offset)
        except Exception as e:
            print(f"  Lensa error at offset {offset}: {e}")
            break

        batch = data.get("job_adverts", [])

        if not batch:
            break

        jobs.extend(batch)
        print(f"  {label} — offset {offset}, got {len(batch)}")

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.3)  # polite pacing

    return jobs

def parse_lensa_job(raw: dict, remote_only: bool) -> dict:
    """Normalise a raw Lensa job_advert dict into the internal job format."""
    city  = (raw.get("city") or "").strip()
    state = (raw.get("state") or "").strip()

    if remote_only or not city:
        location = "Remote"
    elif state:
        location = f"{city}, {state}"
    else:
        location = city

    return {
        "unique_id":   raw.get("unique_id", ""),
        "title":       normalize_title((raw.get("cleaned_job_title") or "").strip()),
        "company":     (raw.get("company") or "").strip(),
        "location":    location,
        "snippet":     (raw.get("description_digest") or "").strip(),
        "url":         (raw.get("incoming_click_url") or "").strip(),
        "is_remote":   remote_only,
        "revenue_per_click": raw.get("revenue_per_click", 0),
    }

# =============================================================================
# WP JOB CREATION
# =============================================================================

_TITLE_ACRONYMS = {
    "vp", "svp", "evp", "avp",
    "ceo", "cfo", "cto", "coo", "cmo", "cro", "cso", "cpo", "cdo",
    "sdr", "bdr", "ae", "sae", "csm", "am", "se", "ase",
    "crm", "gtm", "saas", "b2b", "smb", "sme",
    "it", "bi", "ai", "ml", "hr", "pr", "erp", "api",
    "us", "uk", "eu", "emea", "apac", "latam",
}

def normalize_title(title: str) -> str:
    """Apply Title Case if the title is predominantly uppercase; preserve common acronyms."""
    if not title:
        return title
    letters = [c for c in title if c.isalpha()]
    if not letters:
        return title
    if sum(1 for c in letters if c.isupper()) / len(letters) > 0.7:
        words = title.title().split()
        return " ".join(w.upper() if w.lower() in _TITLE_ACRONYMS else w for w in words)
    return title


def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9\-]", "", str(s).lower().replace(" ", "-"))

def build_slug(title: str, company: str, location: str) -> str:
    country = "" if "remote" in location.lower() else "united-states"
    parts = [_slugify(company), _slugify(title), _slugify(location), country]
    slug = "-".join(p for p in parts if p)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return ("lensa-" + slug)[:200]

def build_expiry() -> str:
    return (datetime.now() + timedelta(days=EXPIRY_DAYS)).strftime("%Y-%m-%d")

def create_wp_job(job: dict, media_id: int | None, dry_run: bool) -> dict | None:
    title        = job["title"]
    company      = job["company"]
    location     = job["location"]
    is_remote    = job["is_remote"]
    category_ids = assign_categories(title)

    # Location string: remote jobs → "Remote, United States"
    if is_remote:
        loc_str = "Remote, United States"
    else:
        loc_str = f"{location}, United States" if location else "United States"

    payload: dict = {
        "title":   title,
        "slug":    build_slug(title, company, location),
        "content": job.get("snippet", ""),
        "status":  "publish",
        "meta": {
            "_job_location":         loc_str,
            "_application":          job["url"],
            "_company_name":         company,
            "_company_website":      "",
            "_company_linkedin":     "",
            "_company_tagline":      "",
            "_company_twitter":      "",
            "_company_video":        "",
            "_company_facebook":     "",
            "_remote_position":      1 if is_remote else 0,
            "_job_expires":          build_expiry(),
            "_application_deadline": "",
            "_featured":             1 if 21 in category_ids else 0,
            "_filled":               0,
            "_promoted":             "",
            "_job_salary":           "",
            "_job_salary_currency":  "",
            "_job_salary_unit":      "",
        },
    }

    if category_ids:
        payload["job-categories"] = category_ids

    payload["job-types"] = [JOB_TYPE_REMOTE if is_remote else JOB_TYPE_INPERSON]

    if media_id:
        payload["featured_media"] = media_id

    tag_term_ids = assign_tags(
        title, job.get("snippet", ""), company,
        tag_ids, ANTHROPIC_API_KEY,
    )
    if tag_term_ids:
        payload["job_listing_tag"] = tag_term_ids

    if dry_run:
        return {"dry_run": True}

    resp = wp.post(f"{WP_API}/job-listings", json=payload, timeout=30)
    if resp.status_code in (200, 201):
        return resp.json()
    print(f"    [wp] Create failed ({resp.status_code}): {resp.text[:300]}")
    return None

# =============================================================================
# STARTUP — pre-load existing application URLs from WP
# =============================================================================

def load_existing_application_urls() -> set[str]:
    print("Loading existing application URLs from WordPress...")
    urls: set[str] = set()
    page = 1
    while True:
        try:
            r = wp.get(f"{WP_API}/job-listings", params={
                "per_page": 100,
                "page": page,
                "status": "publish,draft",
                "_fields": "id,meta",
            }, timeout=30)
            r.raise_for_status()
            batch = r.json()
        except Exception as e:
            print(f"  Warning: could not load WP jobs page {page}: {e}")
            break

        if not batch:
            break

        for post in batch:
            meta = post.get("meta") or {}
            app  = meta.get("_application", "")
            if isinstance(app, list):
                app = app[0] if app else ""
            if app:
                urls.add(str(app).strip())

        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        print(f"  Page {page}/{total_pages} — {len(urls)} URLs collected", end="\r")

        if page >= total_pages:
            break
        page += 1
        time.sleep(0.1)

    print(f"\n  Done. {len(urls)} existing application URLs loaded.\n")
    return urls

# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Lensa Connect RevOps/GTM jobs → RevOpsCareers WordPress"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only — no writes to WordPress or media library")
    parser.add_argument("--keywords", type=int, default=None, metavar="N",
                        help="Limit to first N job title keywords (default: all)")
    parser.add_argument("--reset", action="store_true",
                        help="Clear local state file and re-import all")
    parser.add_argument("--skip-logo", action="store_true",
                        help="Skip logo resolution (faster, no media uploads)")
    parser.add_argument("--check-logos", action="store_true",
                        help="Also resolve logos during --dry-run (slower but shows logo results)")
    args = parser.parse_args()

    if not WP_APP_PASSWORD:
        sys.exit("ERROR: WP_APP_PASSWORD not set in .env")
    if not LENSA_SDK_KEY:
        sys.exit("ERROR: LENSA_SDK_KEY not set in .env")
    if not LENSA_CAMPAIGN:
        sys.exit("ERROR: LENSA_CAMPAIGN_ID not set in .env")

    state = load_state()
    if args.reset:
        state = {"imported_ids": [], "last_run": None, "logo_ids": {}}
        print("Local state reset.\n")

    imported_ids: set[str] = set(state["imported_ids"])
    keyword_list = LENSA_JOB_TITLES[:args.keywords] if args.keywords else LENSA_JOB_TITLES

    print("RevOpsCareers ← Lensa Connect sync")
    print(f"  Site:        {SITE_URL}")
    print(f"  Dry run:     {args.dry_run}")
    print(f"  Keywords:    {len(keyword_list)}")
    print(f"  Skip logos:  {args.skip_logo}")
    print(f"  Local state: {len(imported_ids)} previously imported IDs")
    print()

    existing_urls = load_existing_application_urls()

    global tag_ids
    tag_ids = fetch_tag_ids(wp, WP_API)

    new_count   = 0
    skip_count  = 0
    error_count = 0

    logo_cache: dict[str, int | None] = {
        name: mid for name, mid in state.get("logo_ids", {}).items()
    }

    for job_title in keyword_list:
        print(f"\n[Keyword] {job_title}")

        # Two passes: standard (US-wide, no location filter) + remote only
        for remote_only in [False, True]:
            raw_jobs = fetch_all_lensa(job_title, remote_only)

            for raw in raw_jobs:
                job = parse_lensa_job(raw, remote_only)

                unique_id = job["unique_id"]
                app_url   = job["url"]
                title     = job["title"]
                company   = job["company"]

                # Skip already imported (by unique_id or by URL in WP)
                if unique_id in imported_ids:
                    skip_count += 1
                    continue
                if app_url in existing_urls:
                    skip_count += 1
                    imported_ids.add(unique_id)  # sync state
                    continue

                # Blocklists
                title_lower   = title.lower()
                company_lower = company.lower()
                if any(t in title_lower for t in TITLE_BLOCKLIST):
                    skip_count += 1
                    continue
                if any(t in company_lower for t in COMPANY_BLOCKLIST):
                    skip_count += 1
                    continue
                if any(t in company_lower for t in COMPANY_INDUSTRY_BLOCKLIST):
                    skip_count += 1
                    continue

                # Require a matching category
                category_ids = assign_categories(title)
                if not category_ids:
                    skip_count += 1
                    continue

                cat_name   = _CAT_NAMES.get(category_ids[0], "?")
                remote_tag = " [REMOTE]" if job["is_remote"] else ""
                print(f"  + {title} @ {company}  [{cat_name}]  {job['location']}{remote_tag}  "
                      f"${job['revenue_per_click']:.3f}/click")

                # Logo
                media_id: int | None = None
                if not args.skip_logo and not (args.dry_run and not args.check_logos):
                    if company in logo_cache:
                        media_id = logo_cache[company]
                    else:
                        domain   = extract_domain(company)
                        media_id = resolve_logo(company, domain, args.dry_run)
                        logo_cache[company] = media_id
                        if media_id:
                            state["logo_ids"][company] = media_id
                            notify_n8n("logos", {
                                "company_name": company,
                                "logo_id":      media_id,
                                "logo_url":     "",
                            })
                if media_id is None:
                    media_id = FALLBACK_LOGO_ID

                # Create WP post
                result = create_wp_job(job, media_id, args.dry_run)
                if result:
                    if not args.dry_run:
                        post_id = result.get("id", "?")
                        print(f"    [wp] post ID {post_id}")
                        imported_ids.add(unique_id)
                        existing_urls.add(app_url)
                        notify_n8n("jobs", {
                            "job_title":        title,
                            "company_name":     company,
                            "job_url":          f"{SITE_URL}/jobs/{_slugify(title)}/",
                            "application_link": app_url,
                            "posted_at":        datetime.now().strftime("%Y-%m-%d"),
                        })
                    new_count += 1
                else:
                    error_count += 1

                # Batch pause every WP_BATCH_SIZE posts
                if new_count > 0 and new_count % WP_BATCH_SIZE == 0 and not args.dry_run:
                    print(f"  [batch] {new_count} posts published — pausing {WP_BATCH_PAUSE}s...")
                    time.sleep(WP_BATCH_PAUSE)

        # Persist state after each keyword pass — resilient to mid-run crashes
        if not args.dry_run:
            state["imported_ids"] = list(imported_ids)
            state["last_run"]     = datetime.now().isoformat()
            save_state(state)

        time.sleep(0.5)  # pause between keywords

    # Final state save (catches any remaining changes from dry-run skips etc.)
    if not args.dry_run:
        state["imported_ids"] = list(imported_ids)
        state["last_run"]     = datetime.now().isoformat()
        save_state(state)

    print()
    print("Done.")
    print(f"  Imported: {new_count}")
    print(f"  Skipped:  {skip_count}  (already imported, off-topic, or blocklisted)")
    print(f"  Errors:   {error_count}")


if __name__ == "__main__":
    main()
