#!/usr/bin/env python3
"""
WhatJobs → RevOpsCareers Job Sync
===================================
Fetches US RevOps/GTM job listings from the WhatJobs publisher API and
imports them as job_listing posts in WordPress. Full workflow:

  1. Fetch jobs from WhatJobs XML API (paginated, 50/page)
  2. New jobs (age_days <= max_age): import if not already in WordPress
  3. Known jobs (already imported, within refresh_days): compare title/snippet/
     salary/location vs stored values — PATCH the WP post if anything changed
  4. Per new job: resolve best logo via icon.horse/Brandfetch
  5. Persist state to imported_whatjobs.json (url → {wp_post_id, imported_at,
     last_checked, title, snippet, salary, location})

State file format (v2):
  {
    "jobs": {
      "<url>": {
        "wp_post_id":   12345,
        "imported_at":  "2026-03-23",
        "last_checked": "2026-03-23",
        "title":        "...",
        "snippet":      "...",
        "salary":       "...",
        "location":     "..."
      }
    },
    "logo_ids": { "<company>": <media_id> },
    "last_run":  "2026-03-23T09:30:00"
  }

  Legacy v1 format (imported_urls: [...]) is migrated automatically on first run.

Usage:
    python sync_whatjobs_jobs.py                     # live run (US)
    python sync_whatjobs_jobs.py --region sg         # live run (Singapore)
    python sync_whatjobs_jobs.py --dry-run           # preview only, no writes
    python sync_whatjobs_jobs.py --pages 5           # limit to N pages
    python sync_whatjobs_jobs.py --max-age 2         # import jobs posted in last 2 days
    python sync_whatjobs_jobs.py --refresh-days 30   # refresh jobs imported in last 30 days
    python sync_whatjobs_jobs.py --reset             # clear state, re-import all

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
import urllib.parse
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

SITE_URL        = os.environ.get("WP_SITE_URL", "https://revopscareers.com")
WP_USERNAME     = os.environ.get("WP_USERNAME", "webadmin")
WP_APP_PASSWORD   = os.environ.get("WP_APP_PASSWORD", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# WhatJobs publisher config
WHATJOBS_PUBLISHER_ID    = "6687"   # US
WHATJOBS_PUBLISHER_ID_SG = "6770"   # Singapore
WHATJOBS_USER_IP      = "1.1.1.1"   # required by API; any public IP works
WHATJOBS_USER_AGENT   = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
WHATJOBS_API_URL    = "https://api.whatjobs.com/api/v1/jobs.xml"

# Search keywords (broad GTM/RevOps universe)
WHATJOBS_KEYWORDS = " OR ".join([
    # Revenue Operations
    "revenue operations", "revops", "revenue ops", "chief revenue officer",
    "revenue enablement", "revenue strategy", "revenue intelligence",
    # Sales Operations & Enablement
    "sales operations", "sales ops", "sales enablement", "deal desk",
    "sales analytics", "sales manager", "sales director", "VP sales",
    "head of sales", "inside sales", "outbound sales",
    "SDR manager", "BDR manager",
    # AE / AM
    "account executive", "account manager", "sales engineer", "solutions engineer",
    # Business Development
    "business development", "business development manager", "partnerships",
    "strategic partnerships",
    # CRM & Systems
    "CRM administrator", "CRM manager", "CRM analyst",
    "Salesforce administrator", "Salesforce developer",
    "HubSpot administrator", "GTM systems", "sales technology",
    "marketing technology manager",
    # Marketing Operations
    "marketing operations", "marketing automation", "marketing analytics",
    "martech", "marketing systems",
    # Demand Generation
    "demand generation", "demand gen", "pipeline marketing",
    "account-based marketing", "ABM",
    # Marketing Leadership & General
    "marketing manager", "marketing director", "VP marketing",
    "chief marketing officer", "growth marketing", "digital marketing",
    "content marketing", "email marketing manager", "lifecycle marketing",
    "retention marketing", "brand manager", "communications manager",
    "field marketing", "events manager", "PR manager",
    # Product Marketing
    "product marketing", "PMM", "competitive intelligence", "analyst relations",
    "solutions marketing",
    # Partner Marketing
    "partner marketing", "channel marketing", "alliance marketing", "co-marketing",
    # GTM
    "go-to-market", "GTM", "GTM engineer", "GTM strategy", "GTM operations",
    # Growth
    "growth marketing", "growth manager", "user acquisition",
    "growth hacker", "growth analyst",
    # Customer Success
    "customer success", "customer success manager", "customer success operations",
    "CS ops", "customer experience", "customer success director",
    "VP customer success", "account management", "implementation manager",
    "technical account manager", "customer onboarding", "customer retention",
    "solutions consultant", "professional services",
    # Data & Analytics
    "data analyst", "data scientist", "analytics manager",
    "business intelligence", "BI analyst", "BI engineer",
    "revenue analyst", "sales analyst", "marketing analyst",
    "data engineer", "analytics engineer", "data operations",
    "forecasting analyst", "SQL analyst", "Tableau developer",
    "Looker developer", "insights manager",
    # Finance Operations
    "financial operations", "finops", "FP&A", "financial planning",
    "financial analyst", "pricing analyst", "pricing manager",
    "revenue accounting", "billing operations", "quote to cash",
    "contract manager", "order management",
    # Web Operations
    "web operations", "SEO manager", "SEM manager",
    "conversion optimization", "web manager",
    # People Operations
    "people operations", "people ops", "VP people", "head of people",
    # Product Operations
    "product operations", "product ops",
    # Ad Operations
    "ad operations", "ad ops", "advertising operations",
    "programmatic operations", "campaign operations", "media operations",
])

STATE_FILE      = Path(__file__).parent / "imported_whatjobs.json"
STATE_FILE_SG   = Path(__file__).parent / "imported_whatjobs_sg.json"

# Region settings — overridden by --region arg in main()
_PUBLISHER_ID    = WHATJOBS_PUBLISHER_ID
_SLUG_PREFIX     = "whatjobs-us"   # WP post slug prefix
_COUNTRY_NAME    = "United States" # default country for location fields
_COUNTRY_SLUG    = "united-states" # used in slug generation
_SALARY_CURRENCY = "USD"
_API_URL         = WHATJOBS_API_URL
_STATE_FILE      = STATE_FILE
N8N_WEBHOOK_URL = "https://n8n.tigros.io/webhook/roc-insert-row"

PAGE_SIZE        = 50   # WhatJobs max per page
MAX_PAGES        = 10   # default cap for new-only runs
AGE_DAYS_MAX     = 2    # import jobs posted within last N days
REFRESH_DAYS_DEF = 30   # refresh jobs imported within last N days
EXPIRY_DAYS      = 60   # days until job expires on the site
FALLBACK_LOGO_ID = 184  # RevOpsCareers favicon — used when no company logo is found

WP_BATCH_SIZE  = 10
WP_BATCH_PAUSE = 2.0

ICON_HORSE_CDN = "https://icon.horse/icon/{domain}"
BRANDFETCH_CDN = "https://cdn.brandfetch.io/domain/{domain}"

# ---------------------------------------------------------------------------
# Blocklists
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

COMPANY_INDUSTRY_BLOCKLIST = [
    "hilton", "marriott", "hyatt", "sheraton", "westin", "waldorf",
    "radisson", "accor", "ihg", "intercontinental", "crowne plaza",
    "holiday inn", "four seasons", "ritz-carlton", "ritz carlton",
    "wyndham", "best western", "novotel", "sofitel", "ibis hotel",
    "kimpton", "loews hotel", "omni hotel", "fairmont",
    " resort", " resorts", " hotel", " hotels", " lodge", " inn ",
    " casino", " spa ", "cruise line", "hospitality",
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

# Job titles with these keywords are senior enough to override the industry blocklist
SENIOR_TITLE_KEYWORDS = [
    "manager", "director", "vp ", "vice president", "head of", "chief",
    "svp", "evp", "president", "principal", "senior", "lead", "sr.",
    "partner", "c-suite", "cmo", "cro", "coo", "cso",
]

# ---------------------------------------------------------------------------
# WordPress category IDs
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

JOB_TYPE_INPERSON = 43

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
# STATE — v2 format with per-job metadata for refresh
# =============================================================================

def load_state() -> dict:
    """Load state, migrating v1 (imported_urls list) to v2 (jobs dict) if needed."""
    if not _STATE_FILE.exists():
        return {"jobs": {}, "logo_ids": {}, "last_run": None}

    state = json.loads(_STATE_FILE.read_text())

    # Migrate v1 → v2: imported_urls was a flat list of URLs with no metadata
    if "imported_urls" in state and "jobs" not in state:
        jobs = {}
        for url in state.get("imported_urls", []):
            jobs[url] = {
                "wp_post_id":   None,   # unknown — can't update without ID
                "imported_at":  state.get("last_run", datetime.now().isoformat())[:10],
                "last_checked": state.get("last_run", datetime.now().isoformat())[:10],
                "title":        "",
                "snippet":      "",
                "salary":       "",
                "location":     "",
            }
        state["jobs"] = jobs
        del state["imported_urls"]
        print(f"  [state] Migrated {len(jobs)} URLs from v1 to v2 format.")

    if "logo_ids" not in state:
        state["logo_ids"] = {}

    return state

def save_state(state: dict) -> None:
    _STATE_FILE.write_text(json.dumps(state, indent=2))

def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")

# =============================================================================
# IMAGE UTILITIES
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
# WP MEDIA LIBRARY
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
# N8N DATA TABLE SYNC
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
# LOGO RESOLUTION PIPELINE
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
        if not data or mime == "image/svg+xml":
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
# DOMAIN EXTRACTION
# =============================================================================

def extract_domain(company_name: str) -> str:
    name = re.sub(r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|group|holdings|technologies?)\b",
                  "", company_name, flags=re.IGNORECASE)
    name = re.sub(r"[^a-z0-9]+", "", name.lower())
    return f"{name}.com" if name else ""

# =============================================================================
# WHATJOBS API FETCH
# =============================================================================

def fetch_whatjobs_page(page: int) -> ET.Element:
    params = {
        "publisher":  _PUBLISHER_ID,
        "user_ip":    WHATJOBS_USER_IP,
        "user_agent": WHATJOBS_USER_AGENT,
        "keyword":    WHATJOBS_KEYWORDS,
        "limit":      str(PAGE_SIZE),
        "page":       str(page),
    }
    url = _API_URL + "?" + urllib.parse.urlencode(params)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return ET.fromstring(r.text)

def parse_job(el: ET.Element) -> dict:
    return {
        "title":    normalize_title((el.findtext("title") or "").strip()),
        "company":  (el.findtext("company") or "").strip(),
        "location": (el.findtext("location") or "").strip(),
        "snippet":  (el.findtext("snippet") or "").strip(),
        "url":      (el.findtext("url") or "").strip(),
        "salary":   (el.findtext("salary") or "").strip(),
        "age_days": int(el.findtext("age_days") or 0),
    }

# =============================================================================
# WP JOB CREATION & UPDATE
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
    country = "" if "remote" in location.lower() else _COUNTRY_SLUG
    parts = [_slugify(company), _slugify(title), _slugify(location), country]
    slug = "-".join(p for p in parts if p)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return (_SLUG_PREFIX + "-" + slug)[:200]

def build_expiry() -> str:
    return (datetime.now() + timedelta(days=EXPIRY_DAYS)).strftime("%Y-%m-%d")

def build_salary(salary_str: str) -> str:
    try:
        low_str = salary_str.split("-")[0].strip().replace(".000000", "")
        val = int(low_str)
        return str(val) if val > 0 else ""
    except Exception:
        return ""

def _build_loc_str(location: str) -> str:
    if not location:
        return _COUNTRY_NAME
    if "remote" in location.lower():
        return location
    return f"{location}, {_COUNTRY_NAME}"

def create_wp_job(job: dict, media_id: int | None, dry_run: bool) -> dict | None:
    category_ids = assign_categories(job["title"])
    salary       = build_salary(job.get("salary", ""))

    payload: dict = {
        "title":   job["title"],
        "slug":    build_slug(job["title"], job["company"], job["location"]),
        "content": job.get("snippet", ""),
        "status":  "publish",
        "meta": {
            "_job_location":         _build_loc_str(job["location"]),
            "_application":          job["url"],
            "_company_name":         job["company"],
            "_company_website":      "",
            "_company_linkedin":     "",
            "_company_tagline":      "",
            "_company_twitter":      "",
            "_company_video":        "",
            "_company_facebook":     "",
            "_remote_position":      0,
            "_job_expires":          build_expiry(),
            "_application_deadline": "",
            "_featured":             1 if 21 in category_ids else 0,
            "_filled":               0,
            "_promoted":             "",
            "_job_salary":           salary,
            "_job_salary_currency":  _SALARY_CURRENCY if salary else "",
            "_job_salary_unit":      "year" if salary else "",
        },
    }

    if category_ids:
        payload["job-categories"] = category_ids
    payload["job-types"] = [JOB_TYPE_INPERSON]
    payload["featured_media"] = media_id or FALLBACK_LOGO_ID

    tag_term_ids = assign_tags(
        job["title"], job.get("snippet", ""), job["company"],
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

def update_wp_job(post_id: int, changes: dict, dry_run: bool) -> bool:
    """PATCH an existing WP job_listing with only the changed fields."""
    if dry_run:
        return True

    payload: dict = {}
    if "title" in changes:
        payload["title"] = changes["title"]
    if "snippet" in changes:
        payload["content"] = changes["snippet"]

    meta = {}
    if "salary" in changes:
        salary = build_salary(changes["salary"])
        meta["_job_salary"]          = salary
        meta["_job_salary_currency"] = _SALARY_CURRENCY if salary else ""
        meta["_job_salary_unit"]     = "year" if salary else ""
    if "location" in changes:
        meta["_job_location"] = _build_loc_str(changes["location"])
    if meta:
        payload["meta"] = meta

    if not payload:
        return True   # nothing to send

    resp = wp.post(f"{WP_API}/job-listings/{post_id}", json=payload, timeout=30)
    if resp.status_code in (200, 201):
        return True
    print(f"    [wp] Update failed ({resp.status_code}): {resp.text[:300]}")
    return False

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
            slug = post.get("slug", "")
            if company and slug:
                slug_norm = slug.replace("-", " ")
                company_slug = re.sub(r"[^\w\s]", "", company)
                if slug_norm.startswith(company_slug):
                    slug_norm = slug_norm[len(company_slug):].strip()
                if location:
                    loc_slug = re.sub(r"[^\w\s]", "", location)
                    if slug_norm.endswith(loc_slug):
                        slug_norm = slug_norm[: -len(loc_slug)].strip()
                if slug_norm:
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
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync WhatJobs RevOps/GTM jobs → RevOpsCareers WordPress"
    )
    parser.add_argument("--region", choices=["us", "sg"], default="us",
                        help="WhatJobs region to sync (default: us)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only — no writes to WordPress or media library")
    parser.add_argument("--pages", type=int, default=None, metavar="N",
                        help="Cap on WhatJobs pages to fetch")
    parser.add_argument("--max-age", type=int, default=AGE_DAYS_MAX, metavar="DAYS",
                        help=f"Import jobs posted within last N days (default: {AGE_DAYS_MAX})")
    parser.add_argument("--refresh-days", type=int, default=REFRESH_DAYS_DEF, metavar="DAYS",
                        help=f"Refresh jobs imported within last N days (default: {REFRESH_DAYS_DEF}, 0=off)")
    parser.add_argument("--reset", action="store_true",
                        help="Clear local state file and re-import all")
    parser.add_argument("--skip-logo", action="store_true",
                        help="Skip logo resolution (faster, no media uploads)")
    args = parser.parse_args()

    # Apply region-specific settings
    global _PUBLISHER_ID, _SLUG_PREFIX, _COUNTRY_NAME, _COUNTRY_SLUG, _SALARY_CURRENCY, _API_URL, _STATE_FILE
    if args.region == "sg":
        _PUBLISHER_ID    = WHATJOBS_PUBLISHER_ID_SG
        _SLUG_PREFIX     = "whatjobs-sg"
        _COUNTRY_NAME    = "Singapore"
        _COUNTRY_SLUG    = "singapore"
        _SALARY_CURRENCY = "SGD"
        _API_URL         = WHATJOBS_API_URL
        _STATE_FILE      = STATE_FILE_SG
    else:  # us (default)
        _PUBLISHER_ID    = WHATJOBS_PUBLISHER_ID
        _SLUG_PREFIX     = "whatjobs-us"
        _COUNTRY_NAME    = "United States"
        _COUNTRY_SLUG    = "united-states"
        _SALARY_CURRENCY = "USD"
        _API_URL         = WHATJOBS_API_URL
        _STATE_FILE      = STATE_FILE

    if not WP_APP_PASSWORD:
        sys.exit("ERROR: WP_APP_PASSWORD not set in .env")

    state = load_state()
    if args.reset:
        state = {"jobs": {}, "logo_ids": {}, "last_run": None}
        print("Local state reset.\n")

    jobs_state: dict[str, dict] = state.get("jobs", {})
    refresh_cutoff = (datetime.now() - timedelta(days=args.refresh_days)).strftime("%Y-%m-%d")

    # Page cap: if refresh is on, scan more pages to reach older jobs
    if args.pages:
        page_cap = args.pages
    elif args.refresh_days > 0:
        page_cap = max(MAX_PAGES, args.refresh_days * 2)   # rough heuristic
    else:
        page_cap = MAX_PAGES

    print(f"RevOpsCareers ← WhatJobs sync  [{args.region.upper()}]")
    print(f"  Site:          {SITE_URL}")
    print(f"  Dry run:       {args.dry_run}")
    print(f"  Import:        jobs posted in last {args.max_age} day(s)")
    print(f"  Refresh:       jobs imported in last {args.refresh_days} day(s)"
          if args.refresh_days else "  Refresh:       off")
    print(f"  Page cap:      {page_cap} ({PAGE_SIZE} jobs/page)")
    print(f"  Skip logos:    {args.skip_logo}")
    print(f"  Known jobs:    {len(jobs_state)}")
    print()

    existing_urls, existing_keys = load_existing_application_urls()

    global tag_ids
    tag_ids = fetch_tag_ids(wp, WP_API)

    new_count     = 0
    refresh_count = 0
    skip_count    = 0
    error_count   = 0
    stale_pages   = 0

    logo_cache: dict[str, int | None] = {
        name: mid for name, mid in state.get("logo_ids", {}).items()
    }

    today = _today()

    for page in range(1, page_cap + 1):
        print(f"[Page {page}/{page_cap}] Fetching from WhatJobs...")
        try:
            root = fetch_whatjobs_page(page)
        except Exception as e:
            print(f"  WhatJobs error: {e}")
            break

        total     = root.findtext("total") or "?"
        last_page = root.findtext("last_page") or "?"
        job_els   = root.findall(".//job")
        print(f"  {len(job_els)} jobs (total: {total}, last page: {last_page})")

        if not job_els:
            print("  No jobs returned — done.")
            break

        page_activity = 0   # new imports + refreshes this page

        for job in [parse_job(el) for el in job_els]:
            app_url = job["url"]
            title   = job["title"]
            company = job["company"]

            # ----------------------------------------------------------------
            # KNOWN JOB — check for refresh
            # ----------------------------------------------------------------
            if app_url in jobs_state:
                rec = jobs_state[app_url]

                # Only refresh if within refresh window and wp_post_id is known
                if (args.refresh_days > 0
                        and rec.get("wp_post_id")
                        and rec.get("imported_at", "") >= refresh_cutoff):

                    changes = {}
                    if job["title"]   != rec.get("title", ""):
                        changes["title"]   = job["title"]
                    if job["snippet"] != rec.get("snippet", ""):
                        changes["snippet"] = job["snippet"]
                    if job["salary"]  != rec.get("salary", ""):
                        changes["salary"]  = job["salary"]
                    if job["location"] != rec.get("location", ""):
                        changes["location"] = job["location"]

                    if changes:
                        fields = ", ".join(changes.keys())
                        print(f"  ~ {title} @ {company}  [refresh: {fields}]")
                        ok = update_wp_job(rec["wp_post_id"], changes, args.dry_run)
                        if ok:
                            if not args.dry_run:
                                rec.update(changes)
                            refresh_count += 1
                            page_activity += 1
                        else:
                            error_count += 1
                    else:
                        skip_count += 1

                    # Always update last_checked
                    if not args.dry_run:
                        rec["last_checked"] = today
                else:
                    skip_count += 1
                continue

            # ----------------------------------------------------------------
            # KNOWN IN WP (imported by another script) — just skip
            # ----------------------------------------------------------------
            if app_url in existing_urls:
                skip_count += 1
                continue

            job_key = (
                _norm_key(company),
                _norm_key(title),
                _norm_key(_build_loc_str(job.get("location", ""))),
            )
            if job_key in existing_keys:
                skip_count += 1
                continue

            # ----------------------------------------------------------------
            # NEW JOB — import if within max_age
            # ----------------------------------------------------------------
            if job["age_days"] > args.max_age:
                skip_count += 1
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
                if not any(s in title_lower for s in SENIOR_TITLE_KEYWORDS):
                    skip_count += 1
                    continue

            category_ids = assign_categories(title)
            if not category_ids:
                skip_count += 1
                continue

            cat_name = _CAT_NAMES.get(category_ids[0], "?")
            print(f"  + {title} @ {company}  [{cat_name}]  {job['location']}, {_COUNTRY_NAME}")

            # Logo
            media_id: int | None = None
            if not args.skip_logo and not args.dry_run:
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

            result = create_wp_job(job, media_id, args.dry_run)
            if result:
                if not args.dry_run:
                    post_id = result.get("id", "?")
                    print(f"    [wp] post ID {post_id}")
                    jobs_state[app_url] = {
                        "wp_post_id":   post_id,
                        "imported_at":  today,
                        "last_checked": today,
                        "title":        job["title"],
                        "snippet":      job["snippet"],
                        "salary":       job["salary"],
                        "location":     job["location"],
                    }
                    existing_urls.add(app_url)
                    existing_keys.add(job_key)
                    notify_n8n("jobs", {
                        "job_title":        title,
                        "company_name":     company,
                        "job_url":          f"{SITE_URL}/jobs/{_slugify(title)}/",
                        "application_link": app_url,
                        "posted_at":        today,
                    })
                new_count    += 1
                page_activity += 1
            else:
                error_count += 1

            if new_count > 0 and new_count % WP_BATCH_SIZE == 0 and not args.dry_run:
                print(f"  [batch] {new_count} posts published — pausing {WP_BATCH_PAUSE}s...")
                time.sleep(WP_BATCH_PAUSE)

        # Stale page: no new imports or refreshes
        if page_activity == 0:
            stale_pages += 1
            if stale_pages >= 3:
                print("  Too many consecutive inactive pages — stopping.")
                break
        else:
            stale_pages = 0

        try:
            if page >= int(last_page):
                print("  Reached last page.")
                break
        except (ValueError, TypeError):
            pass

    # Persist state
    if not args.dry_run:
        state["jobs"]     = jobs_state
        state["last_run"] = datetime.now().isoformat()
        save_state(state)

    print()
    print("Done.")
    print(f"  Imported: {new_count}")
    print(f"  Refreshed:{refresh_count}")
    print(f"  Skipped:  {skip_count}  (already imported, too old, or off-topic)")
    print(f"  Errors:   {error_count}")


if __name__ == "__main__":
    main()
