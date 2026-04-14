#!/usr/bin/env python3
"""
Hirebase → RevOpsCareers Job Sync
===================================
Fetches RevOps/GTM job listings from the Hirebase API and imports them as
job_listing posts in WordPress. Full workflow:

  1. Fetch jobs from Hirebase (paginated, 10/page on free plan)
  2. Skip jobs already in WordPress (by application URL — checked at startup)
  3. Per job: resolve best logo
       a. Check WP media library for an existing square logo (by company slug)
       b. If not found/not square: try Hirebase's supplied logo URL
       c. If not square or missing: fetch from Brandfetch CDN (domain fallback)
       d. Upload whichever wins to WP media library (once per company per run)
  4. Create job_listing post with full meta + auto-assigned category
  5. Persist imported IDs to imported_jobs.json so re-runs only import new jobs

Usage:
    python sync_hirebase_jobs.py              # live run
    python sync_hirebase_jobs.py --dry-run    # preview only, no writes
    python sync_hirebase_jobs.py --pages 5    # limit to N Hirebase pages
    python sync_hirebase_jobs.py --reset      # clear state, re-import all

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
    """Minimal .env parser (no python-dotenv dependency)."""
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
        # .env wins over empty shell env vars
        if not os.environ.get(key):
            os.environ[key] = val

_load_env()

SITE_URL         = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME      = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD  = os.environ.get("WP_APP_PASSWORD", "")
HIREBASE_API_KEY = os.environ.get("HIREBASE_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

HIREBASE_URL    = "https://api.hirebase.org/v2/jobs/neural-search"
STATE_FILE      = Path(__file__).parent / "imported_jobs.json"
N8N_WEBHOOK_URL = "https://n8n.tigros.io/webhook/roc-insert-row"

# Per-cluster search configuration for neural-search endpoint.
# Each cluster gets its own paginated request (up to 2,500 results / 25 pages),
# maximising total daily coverage while keeping result sets focused.
SEARCH_CLUSTERS: dict[str, list[str]] = {
    "Revenue Operations": [
        "revenue operations", "revops", "revenue ops", "chief revenue officer", "CRO",
        "VP revenue", "revenue enablement", "revenue strategy", "revenue intelligence",
        "revenue architect",
    ],
    "Sales Operations": [
        "sales operations", "sales ops", "salesops", "sales enablement",
        "enablement manager", "revenue enablement manager", "field enablement",
        "sales enablement manager", "sales enablement specialist",
        "deal desk", "sales analytics", "sales systems", "sales technology manager",
    ],
    "Sales": [
        "sales manager", "sales director", "sales lead", "VP sales", "head of sales",
        "inside sales", "outbound sales",
        "sales development", "sales development representative", "SDR", "SDR manager",
        "business development representative", "BDR", "BDR manager",
        "account executive", "account manager", "sales engineer", "solutions engineer",
    ],
    "Business Development": [
        "business development", "business development manager",
        "business development director", "partnerships", "strategic partnerships",
    ],
    "CRM & GTM Systems": [
        "salesforce administrator", "salesforce developer", "CRM administrator",
        "CRM manager", "CRM analyst", "HubSpot administrator", "GTM systems",
        "sales technology",
    ],
    "Marketing Operations": [
        "marketing operations", "marketing ops", "marketingops", "marketing automation",
        "marketing analytics", "marketing systems", "marketing technology", "martech",
    ],
    "Demand Generation": [
        "demand generation", "demand gen", "demand generation manager",
        "demand generation specialist", "pipeline marketing", "ABM",
        "account-based marketing", "account based marketing",
    ],
    "Marketing": [
        "marketing manager", "marketing director", "marketing lead",
        "VP marketing", "head of marketing", "CMO", "chief marketing officer",
        "growth marketing", "digital marketing", "content marketing",
        "email marketing manager", "lifecycle marketer", "lifecycle marketing",
        "retention marketing", "brand manager", "communications manager",
        "field marketing", "field marketer", "events manager", "PR manager",
    ],
    "Product Marketing": [
        "product marketing", "product marketing manager", "PMM", "product marketer",
        "product evangelist", "solutions marketing", "technical marketing",
        "competitive intelligence", "analyst relations",
    ],
    "Partner Marketing": [
        "partner marketing", "channel marketing", "alliance marketing",
        "partner manager", "co-marketing",
    ],
    "GTM": [
        "GTM", "go-to-market", "go to market", "GTM engineer",
        "GTM strategy", "GTM operations", "GTM lead",
    ],
    "Growth": [
        "growth", "growth manager", "growth lead", "growth hacker",
        "growth operations", "growth strategy", "growth analyst", "growth engineer",
        "user acquisition",
    ],
    "Customer Success": [
        "customer success operations", "CS operations", "CS ops", "customer operations",
        "customer experience operations", "CX operations", "customer success manager",
        "customer success director", "CSM", "customer success lead", "VP customer success",
        "head of customer success", "chief customer officer", "customer experience",
        "account management", "customer onboarding manager", "onboarding specialist",
        "implementation manager", "professional services", "solutions consultant",
        "technical account manager", "TAM", "customer retention", "customer lifecycle",
    ],
    "Data & Analytics": [
        "data analyst", "data scientist", "analytics manager",
        "business intelligence", "BI analyst", "BI developer", "BI engineer",
        "revenue analyst", "sales analyst", "marketing analyst", "data engineer",
        "analytics engineer", "data operations", "data ops", "dataops",
        "data strategy", "forecasting analyst", "data visualization",
        "SQL analyst", "Tableau developer", "Looker developer", "insights manager",
    ],
    "Finance Operations": [
        "finance operations", "financial operations", "finops",
        "revenue accounting", "billing operations", "finance systems", "FP&A",
        "financial planning", "financial analyst", "pricing analyst", "pricing manager",
        "contract manager", "quote to cash", "QTC", "order management",
    ],
    "Web Operations": [
        "web operations", "webops", "website operations", "digital operations",
        "conversion optimization", "SEO manager", "SEM manager", "web manager",
    ],
    "People Operations": [
        "people operations", "people ops", "people operations manager",
        "people operations specialist", "people operations director",
        "people operations lead", "VP people", "head of people",
    ],
    "Product Operations": [
        "product operations", "product ops", "product operations manager",
        "product operations specialist", "product operations analyst",
        "product operations lead", "head of product operations",
    ],
    "Ad Operations": [
        "ad operations", "ad ops", "advertising operations",
        "ad operations manager", "ad operations specialist",
        "advertising operations manager", "programmatic operations",
        "campaign operations", "media operations",
    ],
}

PAGE_SIZE        = 100   # jobs per page (100 = max)
CLUSTER_PAGE_CAP = 25    # neural-search hard cap: 2500 results / 100 per page

# WP posting rate limits — pause after every BATCH_SIZE posts to avoid server overload
WP_BATCH_SIZE  = 10    # posts per batch
WP_BATCH_PAUSE = 2.0   # seconds to pause between batches
EXPIRY_DAYS = 60    # days from publish date until job expires on the site
FALLBACK_LOGO_ID = 184  # RevOpsCareers favicon — used when no company logo is found

# Brandfetch CDN — no auth needed for icon endpoint
# Logo CDN sources (tried in order after Hirebase logo)
ICON_HORSE_CDN  = "https://icon.horse/icon/{domain}"   # primary — square, good coverage
BRANDFETCH_CDN  = "https://cdn.brandfetch.io/domain/{domain}"  # kept as secondary

# Job title blocklist — skip jobs whose title contains any of these terms
# (Hirebase returns all open roles from matching companies, not just matching titles)
TITLE_BLOCKLIST = [
    "cdl", "truck driver", "class-a driver", "class a driver",
    "forklift", "warehouse", "manufacturing", "assembly technician",
    "wire harness", "assembler",
    "electrician", "plumber", "hvac", "carpenter", "welder",
    "nurse", "physician", "doctor", "pharmacist", "dental",
    "teacher", "tutor", "professor", "instructor",
    "real estate agent", "realtor",
    "sailor", "deckhand", "marine crew",
    # Cannabis industry
    "cannabis", "marijuana", "dispensary",
    # Media/publisher ad-selling roles (not RevOps/GTM)
    "advertising sales", "ads sales", "online ads sales",
    # Insurance agent roles (not tech/SaaS)
    "personal lines", "bancassurance",
    # Trade/wholesale roles
    "plumbing wholesale",
    # Junior / entry-level / intern roles (site targets mid-to-senior professionals)
    "junior", " jr ", " jr.", "intern",
]

# Company blocklist — skip all jobs from these companies (exact/substring match, case-insensitive)
COMPANY_BLOCKLIST = [
    "salmonjobs",
    "ismira recruitment",
    # Staffing agencies placing non-tech roles
    "cross border talents",   # 300+ spam placements (Barcelona relocation jobs etc.)
    "peoplelink staffing",
    "employbridge",
    # Retail / fashion
    "white house black market",
    "chico's fas",            # parent company of White House Black Market
    # Media conglomerates posting non-RevOps roles
    "cox enterprises",
    # Medical device & pharma field sales (territory rep roles)
    "smith+nephew",
    "integra lifesciences",
    "brightspring health",
    "option care health",
    "encompass health",
    "viiv healthcare",
    # Food & beverage chains
    "chefs culinar",
    "dutch bros",
    "purple carrot",
    "freytag florist",
    # Fitness / gyms
    "fitness connection",
    # Indian bank / financial conglomerates (non-tech sales roles)
    "tata capital",
    # Insurance sales companies
    "freeway insurance",
]

# Company name terms that indicate non-tech/hospitality industries — skip any company
# whose name contains one of these terms (case-insensitive substring match)
COMPANY_INDUSTRY_BLOCKLIST = [
    # Hotels & hospitality chains
    "hilton", "marriott", "hyatt", "sheraton", "westin", "waldorf",
    "radisson", "accor", "ihg", "intercontinental", "crowne plaza",
    "holiday inn", "four seasons", "ritz-carlton", "ritz carlton",
    "wyndham", "best western", "novotel", "sofitel", "ibis hotel",
    "kimpton", "loews hotel", "omni hotel", "fairmont",
    # General hospitality/travel/leisure indicators in company names
    " resort", " resorts", " hotel", " hotels", " lodge", " inn ",
    " casino", " spa ", "cruise line",
    # Cannabis
    "cannabis", "marijuana", " dispensary",
    # Medical device / life sciences (field territory sales reps)
    "life sciences", "lifesciences",
    # Staffing / recruiting agencies — imperfect but catches most (space-prefixed to avoid mid-word matches)
    " staffing", " recruiting", "recruitment",
    " talent", " talents",
    " search",      # e.g. "Jones Search Group", "Executive Search Partners"
    " partners",    # e.g. "HR Partners", "Sales Partners"
    " associates",  # e.g. "Recruiting Associates"
]

# ---------------------------------------------------------------------------
# WordPress category IDs — matches n8n workflow category mapping
# Multi-category: a job can match more than one category
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
           "data engineer", "analytics engineer", "data operations", "data ops",
           "dataops", "data operations analyst", "data operations manager",
           "data operations engineer", "data strategy", "bi engineer",
           "insights manager", "data platform", "analytics lead", "data product"],
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
}

# ---------------------------------------------------------------------------
# WordPress job-types taxonomy IDs (WP Job Manager)
# ---------------------------------------------------------------------------
JOB_TYPE_REMOTE_FULLTIME  = 14
JOB_TYPE_REMOTE_PARTTIME  = 1283
JOB_TYPE_REMOTE_CONTRACT  = 47
JOB_TYPE_HYBRID           = 26
JOB_TYPE_INPERSON         = 43

# =============================================================================
# HTTP SESSIONS
# =============================================================================

API_BASE = SITE_URL.rstrip("/") + "/wp-json"
WP_API   = API_BASE + "/wp/v2"

wp = requests.Session()
wp.auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))
wp.headers.update({"Content-Type": "application/json"})

tag_ids: dict[str, int] = {}  # populated in main() after session is ready

hb = requests.Session()
hb.headers.update({
    "x-api-key": HIREBASE_API_KEY,
    "Content-Type": "application/json",
})

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
            state["logo_ids"] = {}   # company_slug → media_id (persistent logo cache)
        return state
    return {"imported_ids": [], "last_run": None, "logo_ids": {}}

def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

# =============================================================================
# IMAGE UTILITIES
# =============================================================================

def _png_dims(data: bytes) -> tuple[int, int]:
    """Extract (w, h) from raw PNG bytes. Returns (0, 0) on failure."""
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        return 0, 0
    try:
        return struct.unpack(">II", data[16:24])
    except Exception:
        return 0, 0


def image_is_square(data: bytes) -> bool:
    """
    Return True if the image is square (or close to it, within 5%).
    Uses PIL if available, otherwise falls back to a PNG header parse.
    LinkedIn CDN URLs with '_200_200' in the path are always square.
    """
    if _HAS_PIL:
        try:
            img = PILImage.open(io.BytesIO(data))
            w, h = img.size
            return w > 0 and h > 0 and abs(w - h) <= max(w, h) * 0.05
        except Exception:
            return True  # assume OK on parse failure

    w, h = _png_dims(data)
    if w > 0 and h > 0:
        return abs(w - h) <= max(w, h) * 0.05
    # Can't determine — assume OK
    return True


def url_likely_square(url: str) -> bool:
    """Heuristic: LinkedIn company-logo_NxN URLs are always square."""
    return bool(re.search(r"company.logo_\d+_\d+", url))

# =============================================================================
# CATEGORIZATION
# =============================================================================

def assign_categories(title: str) -> list[int]:
    """Return all matching category IDs for a job title (multi-category, like n8n)."""
    t = title.lower()
    matched = set()
    for cat_id, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in t for kw in keywords):
            matched.add(cat_id)
    return list(matched)


def assign_category(title: str) -> int | None:
    """Single-category fallback (used for display in logs)."""
    ids = assign_categories(title)
    return ids[0] if ids else None


def resolve_job_types(location_type: str, job_type: str) -> list[int]:
    """Map Hirebase location_type/job_type to WordPress job-types taxonomy IDs."""
    loc = (location_type or "").lower()
    jt  = (job_type or "").lower()
    if loc == "hybrid":
        return [JOB_TYPE_HYBRID]
    if loc == "in-person":
        return [JOB_TYPE_INPERSON]
    # Remote (default)
    if "part" in jt:
        return [JOB_TYPE_REMOTE_PARTTIME]
    if "contract" in jt or "freelance" in jt:
        return [JOB_TYPE_REMOTE_CONTRACT]
    return [JOB_TYPE_REMOTE_FULLTIME]

# =============================================================================
# WP MEDIA LIBRARY — lookup & upload
# =============================================================================

def search_wp_media(company_slug: str) -> list[dict]:
    """Search WP media library for logos matching the company slug."""
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
    """
    Return the media ID of an existing logo in WP for this company.
    Accepts items even when media_details dimensions are missing (avoids re-upload).
    """
    items = search_wp_media(company_slug)
    for item in sorted(items, key=lambda x: x.get("id", 0)):
        d = item.get("media_details") or {}
        w, h = d.get("width", 0), d.get("height", 0)
        # Accept if square OR if dimensions unavailable (assume previously validated)
        if (w > 0 and h > 0 and abs(w - h) <= max(w, h) * 0.05) or (w == 0 and h == 0):
            return item["id"]
    return None


def upload_image_to_wp(img_bytes: bytes, filename: str, mime: str,
                       alt_text: str = "") -> int | None:
    """Upload image bytes to WP media library. Returns new media ID or None."""
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
# N8N DATA TABLE SYNC
# =============================================================================

def notify_n8n(table: str, payload: dict) -> None:
    """
    Upsert a row into the n8n data table via the ROC webhook workflow.
      table = "jobs"  → revops_careers_posted_jobs  (LPMDAEcvJWJVCuY1)
      table = "logos" → revops_careers_company_logos (j6HRPVNNn9nwoTdv)
    Fires-and-mostly-forgets; errors are logged but never fatal.
    """
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
# LOGO RESOLUTION PIPELINE
# =============================================================================

def _fetch_url(url: str) -> tuple[bytes | None, str]:
    """Download URL. Returns (bytes, mime_type) or (None, '')."""
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
            "image/gif": "gif"}.get(mime, "jpg")  # SVG excluded — WP rejects it


def resolve_logo(company_name: str, hirebase_logo_url: str,
                 company_domain: str, dry_run: bool) -> int | None:
    """
    Full logo pipeline:
      1. Check WP media for an existing square logo (fast, avoids re-uploads)
      2. Try Hirebase-supplied logo URL (usually LinkedIn CDN 200×200 = square)
      3. Fallback to Brandfetch CDN if no square found
    Returns a WP media ID or None.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", company_name.lower()).strip("-")

    # 1 — existing WP logo
    existing_id = find_existing_square_logo(slug)
    if existing_id:
        return existing_id

    if dry_run:
        return None

    # Helper: try a URL and upload if square (SVG skipped — WP rejects it)
    def try_upload(url: str, label: str) -> int | None:
        if not url:
            return None
        # LinkedIn company-logo_NxN are always square — skip download check
        if url_likely_square(url):
            data, mime = _fetch_url(url)
            if data and mime != "image/svg+xml":
                ext      = _mime_to_ext(mime)
                media_id = upload_image_to_wp(data, f"{slug}-logo.{ext}", mime,
                                              alt_text=company_name)
                if media_id:
                    print(f"    [logo] {label} → media_id={media_id} (square, {len(data)//1024}KB)")
                    return media_id
            return None
        # Download and verify squareness
        data, mime = _fetch_url(url)
        if data and image_is_square(data):
            ext      = _mime_to_ext(mime)
            media_id = upload_image_to_wp(data, f"{slug}-logo.{ext}", mime,
                                          alt_text=company_name)
            if media_id:
                print(f"    [logo] {label} → media_id={media_id} (square, {len(data)//1024}KB)")
                return media_id
        elif data:
            print(f"    [logo] {label} not square — trying next source")
        return None

    # 2 — Hirebase logo
    media_id = try_upload(hirebase_logo_url, "Hirebase logo")
    if media_id:
        return media_id

    # 3 — icon.horse (primary CDN fallback)
    if company_domain:
        ih_url   = ICON_HORSE_CDN.format(domain=company_domain)
        media_id = try_upload(ih_url, f"icon.horse ({company_domain})")
        if media_id:
            return media_id

    # 4 — Brandfetch CDN (secondary fallback)
    if company_domain:
        bf_url   = BRANDFETCH_CDN.format(domain=company_domain)
        media_id = try_upload(bf_url, f"Brandfetch ({company_domain})")
        if media_id:
            return media_id

    print(f"    [logo] No usable logo found for {company_name}")
    return None

# =============================================================================
# WP JOB CREATION
# =============================================================================

def build_location(job: dict) -> str:
    locations = job.get("locations") or []
    if locations:
        loc   = locations[0]
        parts = [p for p in [loc.get("city"), loc.get("region"), loc.get("country")] if p]
        if parts:
            return ", ".join(parts)
    return job.get("location_raw", "")


def build_expiry(date_posted: str = "") -> str:
    # Expire 60 days from today (the publish date on the site), not from Hirebase's date_posted
    return (datetime.now() + timedelta(days=EXPIRY_DAYS)).strftime("%Y-%m-%d")


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


def normalize_url(url: str) -> str:
    if url and not url.startswith("http"):
        return "https://" + url
    return url or ""


def _slugify(s) -> str:
    """Lowercase, spaces→hyphens, strip non-alphanumeric/hyphen. Matches n8n expression."""
    if not s:
        return ""
    return re.sub(r"[^a-z0-9\-]", "", str(s).lower().replace(" ", "-"))


def build_slug(job: dict) -> str:
    """Build WP slug matching the n8n 'post job' expression:
    {company}-{job_title}-{city}-{region}-{country}
    """
    company = _slugify(job.get("company_name", ""))
    title   = _slugify(job.get("job_title", ""))

    locations = job.get("locations") or []
    loc_slug  = ""
    if locations:
        loc = locations[0]
        loc_parts = [
            _slugify(loc.get("city", "")),
            _slugify(loc.get("region", "")),
            _slugify(loc.get("country", "")),
        ]
        loc_parts = [p for p in loc_parts if p]
        if loc_parts:
            loc_slug = "-" + "-".join(loc_parts)

    slug = f"{company}-{title}{loc_slug}"
    slug = re.sub(r"-{2,}", "-", slug).strip("-")  # collapse double hyphens
    return slug[:200]


def create_wp_job(job: dict, media_id: int | None, dry_run: bool) -> dict | None:
    """Create a WP job_listing post. Returns created post dict or None."""
    company_data  = job.get("company_data") or {}
    salary        = job.get("salary_range") or {}
    location_type = job.get("location_type") or ""
    job_type      = job.get("job_type") or ""
    is_remote     = location_type.lower() == "remote"
    category_ids  = assign_categories(job.get("job_title", ""))
    job_type_ids  = resolve_job_types(location_type, job_type)

    payload: dict = {
        "title":   normalize_title(job["job_title"]),
        "slug":    build_slug(job),
        "content": job.get("description", ""),
        "status":  "publish",
        "meta": {
            "_job_location":         build_location(job) or ("Remote" if is_remote else ""),
            "_application":          job.get("application_link", ""),
            "_company_name":         job.get("company_name", ""),
            "_company_website":      normalize_url(job.get("company_link", "")),
            "_company_linkedin":     company_data.get("linkedin_link", ""),
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
            "_job_salary":           str(salary.get("min", "")) if salary.get("min") else "",
            "_job_salary_currency":  salary.get("currency", "") or "",
            "_job_salary_unit":      "year" if salary.get("period") == "year" else "",
        },
    }

    if category_ids:
        payload["job-categories"] = category_ids

    if job_type_ids:
        payload["job-types"] = job_type_ids

    payload["featured_media"] = media_id or FALLBACK_LOGO_ID

    tag_term_ids = assign_tags(
        job.get("job_title", ""),
        (job.get("company_data") or {}).get("description", "") or job.get("job_description", ""),
        job.get("company_name", ""),
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

def _norm_key(s: str) -> str:
    """Normalize a string for dedup key comparison (lowercase, strip punctuation)."""
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def load_existing_application_urls() -> tuple[set[str], set[tuple[str, str, str]]]:
    """
    Fetch all existing job_listing posts and collect:
      - existing_urls: set of _application URL strings
      - existing_keys: set of (company, title, location) tuples for dedup by identity

    Both are used to skip already-imported jobs without relying on the local state
    file alone. existing_keys catches re-posts of the same job with a new URL.
    """
    print("Loading existing jobs from WordPress...")
    urls: set[str] = set()
    keys: set[tuple[str, str, str]] = set()
    page = 1
    while True:
        try:
            r = wp.get(f"{WP_API}/job-listings", params={
                "per_page": 100,
                "page": page,
                "status": "publish,draft",
                "_fields": "id,slug,meta",
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
            company  = _norm_key(meta.get("_company_name", "") or "")
            location = _norm_key(meta.get("_job_location", "") or "")
            # Derive title from slug: strip company prefix and location suffix
            slug = post.get("slug", "")
            if company and slug:
                # title lives between company prefix and location/suffix in slug
                slug_norm = slug.replace("-", " ")
                company_slug = re.sub(r"[^\w\s]", "", company)
                if slug_norm.startswith(company_slug):
                    slug_norm = slug_norm[len(company_slug):].strip()
                if location:
                    loc_slug = re.sub(r"[^\w\s]", "", location)
                    if slug_norm.endswith(loc_slug):
                        slug_norm = slug_norm[: -len(loc_slug)].strip()
                if company and slug_norm:
                    keys.add((company, slug_norm, location))

        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        print(f"  Page {page}/{total_pages} — {len(urls)} URLs / {len(keys)} keys", end="\r")
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.1)

    print(f"\n  Done. {len(urls)} URLs + {len(keys)} identity keys loaded.\n")
    return urls, keys

# =============================================================================
# HIREBASE FETCH
# =============================================================================

def fetch_hirebase_page(cluster_name: str, cluster_titles: list[str],
                        page: int, date_after: str | None = None) -> dict:
    lexical: dict = {
        "job_titles": cluster_titles,
        "page": page,
        "limit": PAGE_SIZE,
        "sort_by": "date_posted",
        "sort_order": "desc",
    }
    if date_after:
        lexical["date_posted_after"] = date_after
    payload = {
        "vector": {"query": cluster_name},
        "lexical": lexical,
    }
    resp = hb.post(HIREBASE_URL, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

# =============================================================================
# MAIN
# =============================================================================

_CAT_NAMES = {
    64:"CS Ops", 22:"Mktg Ops", 23:"Sales Ops", 21:"Rev Ops",
    284:"Biz Ops", 25:"Biz Dev", 1434:"Product Mktg", 1436:"Partner Mktg",
    1435:"Web Ops", 1437:"Finance Ops", 20:"Cust Success", 500:"Data/Analytics",
    1369:"GTM", 1368:"Growth", 13:"Marketing", 19:"Sales",
    1449:"Sales Enablement", 1450:"Demand Gen",
    1453:"People Ops", 1454:"Product Ops", 1455:"Ad Ops",
}

# Consecutive all-skipped pages before stopping.
# Lower when a date filter is active (old pages drop off quickly).
STALE_PAGE_LIMIT      = 5   # no date filter
STALE_PAGE_LIMIT_DATE = 3   # with date filter


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Hirebase RevOps/GTM jobs → RevOpsCareers WordPress"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only — no writes to WordPress or media library")
    parser.add_argument("--pages", type=int, default=None, metavar="N",
                        help=f"Hard cap on pages per cluster (default: {CLUSTER_PAGE_CAP})")
    parser.add_argument("--reset", action="store_true",
                        help="Clear local state file; re-fetch all jobs")
    parser.add_argument("--skip-logo", action="store_true",
                        help="Skip logo resolution (faster, no media uploads)")
    parser.add_argument("--all", action="store_true",
                        help="Ignore date filter — scan all Hirebase pages (slow, use for first run)")
    parser.add_argument("--since", type=int, default=None, metavar="DAYS",
                        help="Fetch jobs posted within the last N days (overrides date filter)")
    parser.add_argument("--check-logos", action="store_true",
                        help="Also resolve logos during --dry-run (slower but shows logo results)")
    args = parser.parse_args()

    if not HIREBASE_API_KEY:
        sys.exit("ERROR: HIREBASE_API_KEY not set in .env")
    if not WP_APP_PASSWORD:
        sys.exit("ERROR: WP_APP_PASSWORD not set in .env")

    state = load_state()
    if args.reset:
        state = {"imported_ids": [], "last_run": None}
        print("Local state reset.\n")
    imported_ids: set[str] = set(state["imported_ids"])

    # Date filter: use last_run date so we only fetch newly posted jobs.
    # --since N overrides with N days ago; --all disables the filter entirely.
    last_run_date: str | None = None
    if args.since:
        last_run_date = (datetime.now() - timedelta(days=args.since)).strftime("%Y-%m-%d")
    elif state.get("last_run") and not args.all and not args.reset:
        last_run_date = state["last_run"][:10]

    # Per-cluster page cap: neural-search max is 2500 results = 25 pages
    cluster_page_cap = args.pages or CLUSTER_PAGE_CAP

    num_clusters = len(SEARCH_CLUSTERS)
    print(f"RevOpsCareers ← Hirebase sync (neural-search, {num_clusters} clusters)")
    print(f"  Site:          {SITE_URL}")
    print(f"  Dry run:       {args.dry_run}")
    print(f"  Date filter:   {last_run_date or '(none — scanning all pages)'}")
    print(f"  Pages/cluster: {cluster_page_cap}  ({PAGE_SIZE} jobs/page, max {cluster_page_cap * PAGE_SIZE}/cluster)")
    print(f"  Skip logos:    {args.skip_logo}")
    print(f"  Local state:   {len(imported_ids)} previously imported IDs")
    print()

    existing_urls, existing_keys = load_existing_application_urls()

    global tag_ids
    tag_ids = fetch_tag_ids(wp, WP_API)

    new_count    = 0
    skip_count   = 0
    error_count  = 0
    # logo_cache: company_name → media_id. Seeded from persisted state so logos
    # are never re-uploaded across runs (prevents hilton-2.jpg, hilton-3.jpg etc.)
    logo_cache: dict[str, int | None] = {
        name: mid for name, mid in state.get("logo_ids", {}).items()
    }

    for cluster_idx, (cluster_name, cluster_titles) in enumerate(SEARCH_CLUSTERS.items(), 1):
        print(f"\n[Cluster {cluster_idx}/{num_clusters}: {cluster_name}]")
        stale_pages = 0  # reset per cluster

        for page in range(1, cluster_page_cap + 1):
            print(f"  Page {page}/{cluster_page_cap}"
                  f"{f' (after {last_run_date})' if last_run_date else ''}...", end=" ")
            try:
                data = fetch_hirebase_page(cluster_name, cluster_titles,
                                           page, date_after=last_run_date)
            except requests.HTTPError as e:
                print(f"\n  Hirebase error: {e.response.status_code} — {e.response.text[:200]}")
                break
            except Exception as e:
                print(f"\n  Unexpected error: {e}")
                break

            jobs        = data.get("jobs") or []
            total_count = data.get("total_count", "?")
            print(f"{len(jobs)} jobs (cluster total: {total_count})")

            if not jobs:
                print("  No jobs returned — moving to next cluster.")
                break

            page_new = 0
            for job in jobs:
                hb_id   = job.get("_id", "")
                title   = job.get("job_title", "(no title)")
                company = job.get("company_name", "")
                app_url = job.get("application_link", "")

                job_key = (
                    _norm_key(company),
                    _norm_key(title),
                    _norm_key(build_location(job)),
                )

                if hb_id in imported_ids or app_url in existing_urls or job_key in existing_keys:
                    skip_count += 1
                    continue

                # Title / company blocklist — skip irrelevant jobs
                title_lower   = (title or "").lower()
                company_lower = (company or "").lower()
                if any(term in title_lower for term in TITLE_BLOCKLIST):
                    skip_count += 1
                    continue
                if any(term in company_lower for term in COMPANY_BLOCKLIST):
                    skip_count += 1
                    continue
                if any(term in company_lower for term in COMPANY_INDUSTRY_BLOCKLIST):
                    skip_count += 1
                    continue

                # Client-side date filter
                if last_run_date:
                    job_date = job.get("date_posted", "")
                    if job_date and job_date < last_run_date:
                        skip_count += 1
                        continue

                category_ids = assign_categories(title)
                if not category_ids:
                    # No matching GTM/RevOps category — skip to keep site on-profile
                    skip_count += 1
                    continue

                cat_id   = category_ids[0]
                cat_name = _CAT_NAMES.get(cat_id, "?")
                is_remote = (job.get("location_type") or "").lower() == "remote"
                location  = build_location(job)
                print(f"  + {title} @ {company}  [{cat_name}]  "
                      f"{'Remote' if is_remote else location}")

                # Logo resolution (skipped in dry-run for speed unless --check-logos)
                media_id: int | None = None
                if not args.skip_logo and not (args.dry_run and not args.check_logos):
                    if company in logo_cache:
                        media_id = logo_cache[company]
                    else:
                        website = normalize_url(job.get("company_link", ""))
                        domain  = re.sub(r"^https?://(www\.)?", "", website).split("/")[0]
                        if not domain:
                            domain = re.sub(r"\s+", "", company.lower()) + ".com"
                        hb_logo_url = job.get("company_logo", "")
                        media_id = resolve_logo(
                            company_name=company,
                            hirebase_logo_url=hb_logo_url,
                            company_domain=domain,
                            dry_run=args.dry_run,
                        )
                        logo_cache[company] = media_id
                        # Persist to state so future runs skip the upload
                        if media_id and not args.dry_run:
                            state.setdefault("logo_ids", {})[company] = media_id
                        # Sync newly uploaded logo to n8n data table
                        if media_id and not args.dry_run:
                            logo_src = hb_logo_url or ICON_HORSE_CDN.format(domain=domain)
                            notify_n8n("logos", {
                                "company_name": company,
                                "logo_id":      media_id,
                                "logo_url":     logo_src,
                            })

                result = create_wp_job(job, media_id, args.dry_run)
                if result is not None:
                    if not args.dry_run:
                        wp_id  = result.get("id")
                        wp_url = result.get("link", f"{SITE_URL}/?p={wp_id}")
                        print(f"    [wp] post ID {wp_id}")
                        notify_n8n("jobs", {
                            "job_title":       job.get("job_title", ""),
                            "company_name":    job.get("company_name", ""),
                            "job_url":         wp_url,
                            "application_link": app_url,
                            "posted_at":       job.get("date_posted", ""),
                        })
                    imported_ids.add(hb_id)
                    existing_urls.add(app_url)
                    existing_keys.add(job_key)
                    new_count += 1
                    page_new  += 1

                    # Batch pause every WP_BATCH_SIZE posts to avoid overloading the server
                    if not args.dry_run and new_count % WP_BATCH_SIZE == 0:
                        print(f"  [batch] {new_count} posts published — pausing {WP_BATCH_PAUSE}s...")
                        time.sleep(WP_BATCH_PAUSE)
                else:
                    error_count += 1

                time.sleep(2.0)

            # Persist state after each page
            if not args.dry_run:
                state["imported_ids"] = list(imported_ids)
                state["last_run"]     = datetime.now().isoformat()
                save_state(state)

            # Early-exit conditions within this cluster
            if len(jobs) < PAGE_SIZE:
                print("  Last page reached.")
                break

            stale_limit = STALE_PAGE_LIMIT_DATE if last_run_date else STALE_PAGE_LIMIT
            if page_new == 0:
                stale_pages += 1
                print(f"  All skipped ({stale_pages}/{stale_limit} stale pages)")
                if stale_pages >= stale_limit:
                    print("  Too many consecutive all-skipped pages — next cluster.")
                    break
            else:
                stale_pages = 0

            time.sleep(0.5)

    print()
    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"{prefix}Done.")
    print(f"  Imported: {new_count}")
    print(f"  Skipped:  {skip_count}  (already in WordPress)")
    print(f"  Errors:   {error_count}")
    if args.dry_run and new_count:
        print(f"\nRun without --dry-run to import these {new_count} jobs.")


if __name__ == "__main__":
    main()
