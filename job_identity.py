"""
Shared job identity rules for the sync scripts.

- job_key(): dedup identity = (company_key, title, country). The same job posted in
  several cities of one country is a single listing; the same job in another
  country is a separate listing.
- country_of(): ports roc_extract_country() from the child theme's functions.php.
  countries.json holds roc_normalize_country()'s static lookup tables, exported via
  ReflectionFunction::getStaticVariables(); re-export it if the PHP table changes.
- company_key(): ports roc_company_key() from functions.php ("Xendit Inc." == "Xendit").
- is_aggregator(): job boards, talent platforms and agencies that post on behalf
  of an unnamed employer, plus labels a feed attaches to other companies' ads.
  We only list jobs from actual employers.
"""

import json
import re
from pathlib import Path

_COUNTRIES = json.loads(Path(__file__).with_name("countries.json").read_text(encoding="utf-8"))
_LOOKUP = _COUNTRIES["lookup"]
_ISO2 = _COUNTRIES["iso2"]
_PART_SPLIT = re.compile(r"\s*(?:,|\s-\s|-\s|/|\||\(|\)|\bor\b|_)\s*", re.I)

# Normalized company names (see norm()). Matched exactly, except "confidential*".
AGGREGATORS = {
    "virtualvocations", "jobgether", "lifelancer", "huzzle", "weekday ai", "weekday",
    "mercor", "pavago", "scalejet", "trailhead",
    "the supreme hr advisory pte ltd", "intalent asia",
    "dealhub ltd",   # WhatJobs SG labels other companies' ads (Mistral, Cloudflare, Xendit...) "DealHub Ltd."
}

_LEGAL_SUFFIX = re.compile(r"([\s,-]+(inc|incorporated|llc|l\.l\.c|ltd|limited|corp|corporation|co|gmbh|ag|plc|pty|bv|b\.v"
                           r"|sa|s\.a|srl|s\.r\.l|lp|llp|nv|oy|ab|as|spa|s\.p\.a|sas|kg|pte)\.?)+$")


def norm(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace (same as the scripts' _norm_key)."""
    if not s:
        return ""
    s = re.sub(r"[^\w\s]", "", s.lower().strip())
    return re.sub(r"\s+", " ", s)


def company_key(name: str) -> str:
    """Company grouping key: lowercase, no legal suffix or ".com", letters and digits only."""
    k = re.sub(r"\.com$", "", (name or "").strip().lower())
    for _ in range(2):
        k = _LEGAL_SUFFIX.sub("", k).strip(" ,.-")
    return re.sub(r"[^a-z0-9]+", "", k)


def _normalize_country(raw: str) -> str:
    key = raw.strip(" \t\n\r\0\x0b.*").lower()
    if not key:
        return ""
    if key in _LOOKUP:
        return _LOOKUP[key]
    if len(key) == 2 and key in _ISO2 and raw.strip() == raw.strip().upper():
        return _ISO2[key]
    return ""


def country_of(location: str) -> str:
    """Canonical country name of a _job_location string, or "" if none is found."""
    for segment in re.split(r"\s*;\s*", (location or "").strip()):
        parts = [p for p in _PART_SPLIT.split(segment) if p]
        for part in reversed(parts):
            country = _normalize_country(part)
            if country:
                return country
    return ""


def job_key(company: str, title: str, location: str) -> tuple[str, str, str]:
    return (company_key(company), norm(title), country_of(location))


def is_aggregator(company: str) -> bool:
    n = norm(company)
    return n in AGGREGATORS or n.startswith("confidential")
