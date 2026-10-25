#!/usr/bin/env python3
"""
Role reads for featured Revenue Operations jobs
===============================================
A short, candidate-focused read of each featured RevOps job ad, shown on the job page
under the description. Method: role_read/METHODOLOGY.md (based on ReLoop.me's published
Fair Read method; links to ReLoop.me's full Role Intel).

  1. GET  /roc/v1/role-read-queue   featured, published category-21 jobs without a read
  2. Skip ads under MIN_CHARS of text (stored as skipped so they aren't re-checked)
  3. Claude Haiku 5.5 with a JSON schema → the read
  4. Drop any item whose quote isn't in the ad word for word
  5. Add pay / experience context from RevOps Careers 2026 data (computed here, not by the model)
  6. POST /roc/v1/role-read/{id}    stored as post meta _roc_role_read

Usage:
    python sync_role_reads.py              # live run
    python sync_role_reads.py --dry-run    # print reads, no writes
    python sync_role_reads.py --limit 5    # at most N jobs this run (default 60)
    python sync_role_reads.py --days 5 --limit 500   # backfill: all RevOps jobs from the last 5 days
    python sync_role_reads.py --ids 174292           # specific jobs (any category, even if already read)
"""

import argparse
import hashlib
import html
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import requests

import job_identity

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

API_BASE  = SITE_URL.rstrip("/") + "/wp-json"
MODEL     = "claude-haiku-5-5"
MIN_CHARS = 1500
PRICE_IN, PRICE_OUT = 0.10 / 1e6, 0.50 / 1e6   # Claude Haiku 5.5, USD per token
USAGE = {"input": 0, "output": 0}

wp = requests.Session()
wp.auth = (WP_USERNAME, WP_APP_PASSWORD.replace(" ", ""))
wp.headers.update({"Content-Type": "application/json"})

LEVELS = ["Analyst / Specialist", "Senior IC", "Lead", "Manager", "Senior Manager",
          "Director", "Head of", "VP / Chief"]

# RevOps Careers 2026 RevOps ads (Jan–Oct): median minimum years asked, and median
# midpoint of posted US pay ranges (USD). Source: the 2026 job-ads articles.
TYPICAL = {
    "Analyst / Specialist": (3, 100_000), "Senior IC": (4, 126_000), "Lead": (5, 140_000),
    "Manager": (5, 127_000), "Senior Manager": (6, 178_000), "Director": (7, 185_000),
    "Head of": (5, 235_000), "VP / Chief": (8, 275_000),
}

FLAGS = ["one-person function", "several jobs in one", "title broader than the work",
         "title narrower than the work", "unclear success measures",
         "requirements broad for the level"]
AXES  = ["hands-on vs strategic", "maintain vs build", "specialist vs generalist",
         "clear vs open-ended scope"]

_quoted = {"type": "object", "additionalProperties": False, "required": ["item", "quote"],
           "properties": {"item": {"type": "string"}, "quote": {"type": "string"}}}

SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["summary", "level", "manages_people", "title_match", "level_reason",
                 "must_haves", "nice_to_haves", "checks", "leans", "ai_in_role", "ai_quote",
                 "apply_if", "questions"],
    "properties": {
        "summary":        {"type": "string"},
        "level":          {"type": "string", "enum": LEVELS},
        "manages_people": {"type": "string", "enum": ["yes", "no", "not stated"]},
        "title_match":    {"type": "string", "enum": ["matches", "title overstates the work",
                                                      "title understates the work", "unclear"]},
        "level_reason":   {"type": "string"},
        "must_haves":     {"type": "array", "items": _quoted},
        "nice_to_haves":  {"type": "array", "items": _quoted},
        "checks": {"type": "array", "items": {
            "type": "object", "additionalProperties": False, "required": ["flag", "note", "quote"],
            "properties": {"flag": {"type": "string", "enum": FLAGS},
                           "note": {"type": "string"}, "quote": {"type": "string"}}}},
        "leans": {"type": "array", "items": {
            "type": "object", "additionalProperties": False, "required": ["axis", "lean", "reason"],
            "properties": {"axis": {"type": "string", "enum": AXES},
                           "lean": {"type": "string", "enum": ["mostly the first", "balanced",
                                                               "mostly the second"]},
                           "reason": {"type": "string"}}}},
        "ai_in_role": {"type": "string", "enum": ["none stated", "uses AI tools",
                                                  "builds AI workflows"]},
        "ai_quote":   {"type": "string"},
        "apply_if":   {"type": "array", "items": {"type": "string"}},
        "questions":  {"type": "array", "items": {"type": "string"}},
    },
}

SYSTEM = """You read job ads for candidates on RevOps Careers, a job board for Revenue Operations and go-to-market roles. You write a short, plain-English read of one job ad that helps a candidate decide whether to apply and how to prepare.

Rules:
- Evidence over titles. The title is a claim; the duties, authority and reporting line are the evidence. Judge the level from what the person will own and decide, not from the title.
- Unknown is not zero. If the ad doesn't say something (team size, reporting line, pay, success measures), say "not stated" or leave the list empty. Never guess or fill in typical values.
- Describe the job; never rank or judge candidates or the employer. Keep a neutral, factual tone.
- Show the why: give a short reason for every judgement.
- Quotes must be copied word for word from the ad, 3 to 15 words, with no changes. If you can't support an item with a quote, leave it out.

What to write:
- summary: 1-2 sentences on what the person will actually spend their time on. Refer to the company as "the employer".
- level: the level the work implies. manages_people: whether the ad says the person will manage people. title_match: compare the title with the level the work implies. level_reason: one sentence on why.
- must_haves: up to 5 load-bearing requirements (the ad says required, must, or makes them central). nice_to_haves: up to 5 preferences (preferred, bonus, nice to have, plus).
- checks: up to 3, only when the ad clearly shows them; otherwise an empty list. "one-person function" means no team or peers are mentioned for an area the person owns end to end.
- leans: exactly one entry per axis (4 entries), each with a one-line reason.
- ai_in_role with ai_quote (empty string when "none stated").
- apply_if: 2-3 concrete things a candidate should be able to show on a resume or in an interview, written as "You can show ...".
- questions: 3 questions to ask the employer that fill the ad's most important gaps.
Write in plain English, short sentences, no marketing language."""


def plain(text: str) -> str:
    text = re.sub(r"<(br|/p|/li|/h\d)[^>]*>", "\n", text or "", flags=re.I)
    text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    return re.sub(r"[ \t]+", " ", re.sub(r"\n\s*\n+", "\n", text)).strip()


def _norm(s: str) -> str:
    s = s.lower().replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = s.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", s).strip(" .,;:")


def keep_quoted(items: list[dict], ad: str, limit: int) -> list[dict]:
    """Items whose quote appears in the ad word for word (case/whitespace-insensitive)."""
    ad_n = _norm(ad)
    return [i for i in items if i.get("quote") and _norm(i["quote"]) in ad_n][:limit]


_RANGE = re.compile(r"\$\s?(\d[\d,.]*)\s?([kK])?\s*(?:-|–|—|to)\s*\$?\s?(\d[\d,.]*)\s?([kK])?")


def pay_context(ad: str, salary_meta: str, location: str, level: str) -> dict:
    m = _RANGE.search(ad) or _RANGE.search(salary_meta or "")
    if not m:
        return {"stated": bool(salary_meta), "text": salary_meta or ""}
    k_lo, k_hi = bool(m.group(2)), bool(m.group(4) or m.group(2))
    lo = float(m.group(1).replace(",", "")) * (1000 if k_lo or (k_hi and float(m.group(1).replace(",", "")) < 1000) else 1)
    hi = float(m.group(3).replace(",", "")) * (1000 if k_hi else 1)
    out = {"stated": True, "text": m.group(0).strip()}
    if 20_000 <= lo <= hi <= 900_000 and job_identity.country_of(location) == "United States":
        out["midpoint"] = round((lo + hi) / 2)
        out["typical_midpoint"] = TYPICAL[level][1]
    return out


def years_context(ad: str, level: str) -> dict:
    found = [int(x) for x in re.findall(r"(\d{1,2})\s*\+?\s*(?:-|–|to)?\s*(?:\d{1,2}\s*)?\+?\s*years", ad.lower())
             if 0 < int(x) <= 20]
    return {"asked": min(found) if found else None, "typical": TYPICAL[level][0]}


def read_job(client: anthropic.Anthropic, job: dict, ad: str) -> dict | None:
    prompt = (f"Job title: {html.unescape(job['title'])}\n"
              f"Location: {job.get('location') or 'not stated'}\n\n"
              f"Job ad:\n{ad}")
    try:
        resp = client.messages.create(
            model=MODEL, max_tokens=8000, system=SYSTEM,
            output_config={"effort": "low", "format": {"type": "json_schema", "schema": SCHEMA}},
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.RateLimitError:
        print("    rate limited — stopping this run")
        raise
    except anthropic.APIStatusError as e:
        print(f"    API error {e.status_code}: {e.message}")
        return None
    except anthropic.APIConnectionError:
        print("    network error")
        return None
    USAGE["input"] += resp.usage.input_tokens
    USAGE["output"] += resp.usage.output_tokens
    if resp.stop_reason != "end_turn":
        print(f"    no read (stop_reason={resp.stop_reason})")
        return None
    text = next((b.text for b in resp.content if b.type == "text"), "")
    return json.loads(text)


def build_read(raw: dict, job: dict, ad: str) -> dict:
    level = raw["level"]
    return {
        "summary": raw["summary"].strip(),
        "level": level,
        "manages_people": raw["manages_people"],
        "title_match": raw["title_match"],
        "level_reason": raw["level_reason"].strip(),
        "must_haves": keep_quoted(raw["must_haves"], ad, 5),
        "nice_to_haves": keep_quoted(raw["nice_to_haves"], ad, 5),
        "checks": keep_quoted(raw["checks"], ad, 3),
        "leans": [l for l in raw["leans"] if l["axis"] in AXES][:4],
        "ai_in_role": raw["ai_in_role"],
        "ai_quote": raw["ai_quote"] if raw["ai_quote"] and _norm(raw["ai_quote"]) in _norm(ad) else "",
        "apply_if": [s.strip() for s in raw["apply_if"]][:3],
        "questions": [s.strip() for s in raw["questions"]][:3],
        "pay": pay_context(ad, job.get("salary", ""), job.get("location", ""), level),
        "years": years_context(ad, level),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Role reads for featured RevOps jobs")
    parser.add_argument("--dry-run", action="store_true", help="Print reads, no writes")
    parser.add_argument("--limit", type=int, default=60, help="Max jobs per run (default 60)")
    parser.add_argument("--days", type=int, default=0,
                        help="Backfill: all RevOps jobs from the last N days, not only featured ones")
    parser.add_argument("--ids", type=str, default="", help="Comma-separated job IDs to read")
    args = parser.parse_args()

    if not WP_APP_PASSWORD:
        sys.exit("ERROR: WP_APP_PASSWORD not set in .env")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        sys.exit("ERROR: ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic()

    if args.ids:
        queue = []
        for job_id in [int(x) for x in args.ids.split(",") if x.strip()]:
            j = wp.get(f"{API_BASE}/wp/v2/job-listings/{job_id}", params={"context": "edit"}, timeout=60).json()
            queue.append({"id": job_id, "title": j["title"]["raw"], "content": j["content"]["raw"],
                          "location": j["meta"].get("_job_location", ""), "salary": j["meta"].get("_job_salary", "")})
    else:
        r = wp.get(f"{API_BASE}/roc/v1/role-read-queue", params={"limit": args.limit, "days": args.days}, timeout=60)
        r.raise_for_status()
        queue = r.json()
    scope = f"RevOps jobs from the last {args.days} days" if args.days else "featured RevOps jobs"
    print(f"Role reads — {len(queue)} {scope} without a read (dry run: {args.dry_run})")

    done = skipped = failed = 0
    for job in queue:
        ad = plain(job["content"])
        title = html.unescape(job["title"])
        meta = {"model": MODEL, "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "ad_hash": hashlib.sha1(ad.encode()).hexdigest()[:12]}
        if len(ad) < MIN_CHARS:
            payload, skipped = {**meta, "skipped": "short_ad"}, skipped + 1
            print(f"  - {title} (#{job['id']}): ad too short ({len(ad)} chars), skipped")
        else:
            try:
                raw = read_job(client, job, ad)
            except anthropic.RateLimitError:
                break
            if raw is None:
                failed += 1
                continue
            payload = {**meta, **build_read(raw, job, ad)}
            done += 1
            print(f"  + {title} (#{job['id']}): {payload['level']}, {payload['title_match']}, "
                  f"{len(payload['must_haves'])} must-haves, {len(payload['checks'])} checks")
        if args.dry_run:
            print(json.dumps(payload, indent=1)[:1500] if "summary" in payload else "")
            continue
        w = wp.post(f"{API_BASE}/roc/v1/role-read/{job['id']}", json={"read": payload}, timeout=60)
        if w.status_code != 200:
            print(f"    save failed ({w.status_code}): {w.text[:200]}")
            failed += 1

    cost = USAGE["input"] * PRICE_IN + USAGE["output"] * PRICE_OUT
    print(f"Done. Read: {done}  Skipped (short ad): {skipped}  Failed: {failed}")
    print(f"Tokens: {USAGE['input']} in / {USAGE['output']} out — ${cost:.4f}"
          + (f" (${cost / done:.5f} per read)" if done else ""))


if __name__ == "__main__":
    main()
