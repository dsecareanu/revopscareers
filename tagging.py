#!/usr/bin/env python3
"""
Job tagging helper — shared by all sync scripts.

Matches job title + description against a canonical tag list using keyword
lookup. No external API calls — fast, free, and deterministic.

Usage:
    from tagging import fetch_tag_ids, assign_tags

    # Once at startup:
    tag_ids = fetch_tag_ids(session, WP_API)

    # Per job:
    term_ids = assign_tags(title, description, company, tag_ids)
    # Pass term_ids as "job_listing_tag" in the WP post payload.
"""

import re
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Keyword map: tag name → list of substrings to search for in lowercased text.
# Matches are checked against: "{title} {description}" (lowercased).
# Use word-boundary tokens (\b) for short/ambiguous terms.
# ---------------------------------------------------------------------------

_TAG_KEYWORDS: dict[str, list[str]] = {
    # ── CRM ─────────────────────────────────────────────────────────────────
    "Salesforce": [
        "salesforce", "sfdc", "sales cloud", "service cloud",
        "salesforce.com", "force.com",
    ],
    "HubSpot": ["hubspot", "hub spot"],
    "Microsoft Dynamics": [
        "microsoft dynamics", "ms dynamics", "dynamics 365",
        "dynamics crm", "dynamics nav",
    ],
    "Pipedrive": ["pipedrive"],

    # ── Sales Engagement ────────────────────────────────────────────────────
    "Outreach": ["outreach.io", "outreach platform", "outreach sequence",
                 "outreach cadence", "using outreach", "via outreach"],
    "Salesloft": ["salesloft", "sales loft"],
    "Apollo":    ["apollo.io", "apollo platform", "apollo sequences"],

    # ── Revenue Intelligence ─────────────────────────────────────────────────
    "Gong":  ["gong.io", "gong call", "gong revenue", "gong platform",
              "gong insights", "using gong"],
    "Clari": ["clari"],

    # ── Marketing Automation ─────────────────────────────────────────────────
    "Marketo":  ["marketo"],
    "Pardot":   ["pardot", "marketing cloud account engagement", "mcae"],
    "Eloqua":   ["eloqua", "oracle eloqua"],

    # ── Analytics & BI ───────────────────────────────────────────────────────
    "Tableau":  ["tableau"],
    "Looker":   ["looker", "looker studio", "lookml"],
    "Power BI": ["power bi", "powerbi", "microsoft power bi"],

    # ── Data Warehouse ───────────────────────────────────────────────────────
    "Snowflake": ["snowflake"],
    "BigQuery":  ["bigquery", "big query", "google bigquery"],

    # ── CPQ & Billing ────────────────────────────────────────────────────────
    "Salesforce CPQ": [
        "salesforce cpq", "cpq", "configure price quote",
        "configure, price", "configure-price-quote",
    ],
    "Zuora":   ["zuora"],
    "DealHub": ["dealhub", "deal hub"],

    # ── Customer Success ─────────────────────────────────────────────────────
    "Gainsight":  ["gainsight"],
    "ChurnZero":  ["churnzero", "churn zero"],

    # ── Prospecting ──────────────────────────────────────────────────────────
    "ZoomInfo":                 ["zoominfo", "zoom info"],
    "LinkedIn Sales Navigator": ["sales navigator", "linkedin sales navigator"],
    "Cognism":                  ["cognism"],
    "Demandbase":               ["demandbase", "demand base"],

    # ── Automation ───────────────────────────────────────────────────────────
    "Workato":       ["workato"],
    "Zapier":        ["zapier"],
    "Make":          ["make.com", "make automation", "integromat"],
    "n8n":           ["n8n"],
    "Clay":          ["clay.com", "clay enrichment", "clay platform",
                      "using clay", "clay for "],
    "Power Automate": ["power automate", "microsoft flow", "ms flow"],
    "Airtable":      ["airtable", "air table"],

    # ── Skills ───────────────────────────────────────────────────────────────
    "SQL": [r"\bsql\b", "structured query language"],
    "Python": [r"\bpython\b"],
    "Data Analysis": [
        "data analysis", "data analytics", "data-driven", "analyze data",
        "data analyst", "analytical skills",
    ],
    "Forecasting": [
        "forecasting", "revenue forecast", "sales forecast",
        "demand forecast", "forecast accuracy",
    ],
    "Pipeline Management": [
        "pipeline management", "pipeline review", "manage.*pipeline",
        "pipeline development", "pipeline reporting",
    ],
    "Revenue Modeling": [
        "revenue model", "revenue modeling", "revenue projection",
        "revenue planning",
    ],
    "Territory Planning": [
        "territory planning", "territory management", "territory design",
        "territory assignment",
    ],
    "Quota Setting": [
        "quota setting", "quota design", "quota planning",
        "quota management", "quota methodology",
    ],
    "CRM Administration": [
        "crm admin", "crm administration", "crm management",
        "crm implementation", "crm configuration", "crm optimization",
        "salesforce admin", "sfdc admin",
    ],
    "Dashboard & Reporting": [
        "dashboard", "kpi reporting", "executive reporting",
        "build.*report", "reporting.*dashboard", "metrics reporting",
        "business intelligence report",
    ],
    "Lead Scoring": [
        "lead scoring", "lead qualification", "lead prioritization",
        "lead grading",
    ],
    "Attribution Modeling": [
        "attribution model", "attribution modeling", "multi-touch attribution",
        r"\bmta\b", "marketing attribution",
    ],
    "Compensation Design": [
        "compensation design", "compensation plan", r"\bcomp plan\b",
        "incentive compensation", "sales compensation",
        "commission plan", "variable compensation", "spiff",
    ],
    "Contract Management": [
        "contract management", "contract review", "contract negotiation",
        "contract lifecycle", "contract operations",
    ],
    "Account-Based Marketing": [
        r"\babm\b", "account-based marketing", "account based marketing",
        "account-based sales",
    ],
    "Excel": [
        "microsoft excel", r"\bexcel\b", "vlookup", "pivot table",
        "spreadsheet",
    ],
    "Google Analytics": [
        "google analytics", r"\bga4\b", "google analytics 4",
        "google tag manager",
    ],
    "Project Management": [
        "project management", r"\bpmp\b", "project planning",
        "project coordination", "cross-functional project",
    ],
    "Deal Desk": [
        "deal desk", "deal review", "deal structuring",
        "pricing approval", "deal approval",
    ],
    "Financial Modeling": [
        "financial model", "financial modeling", "financial analysis",
        r"\bp&l\b", "budget model", "financial planning",
    ],
    "Data Visualization": [
        "data visualization", "data viz", "visualize data",
        "visualization tool", "charting",
    ],
    "Change Management": [
        "change management", "organizational change", "change enablement",
        "change adoption", "managing change",
    ],
}

# Pre-compile patterns for speed
_COMPILED: dict[str, list[re.Pattern]] = {
    tag: [
        re.compile(kw, re.IGNORECASE)
        if re.escape(kw) != kw or kw.startswith(r"\b")
        else re.compile(re.escape(kw), re.IGNORECASE)
        for kw in keywords
    ]
    for tag, keywords in _TAG_KEYWORDS.items()
}


def _matches(text: str, tag: str) -> bool:
    patterns = _COMPILED.get(tag, [])
    return any(p.search(text) for p in patterns)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_tag_ids(wp_session: requests.Session, wp_api: str) -> dict[str, int]:
    """Fetch job_listing_tag term IDs from WordPress. Call once at startup.

    Returns dict keyed by lowercase tag name → WP term ID.
    """
    r = wp_session.get(f"{wp_api}/job_listing_tag", params={"per_page": 100}, timeout=15)
    r.raise_for_status()
    return {t["name"].lower(): t["id"] for t in r.json()}


def assign_tags(
    title: str,
    description: str,
    company: str,
    tag_ids: dict[str, int],
    api_key: str = "",   # unused — kept for interface compatibility
) -> list[int]:
    """Return WP term IDs for tags matching the job title + description.

    Keyword-based matching — no external API calls.
    """
    if not tag_ids:
        return []

    text = f"{title} {description}".lower()
    matched = []
    for tag_name, term_id in tag_ids.items():
        # Look up by canonical name (tag_ids keys are lowercase)
        canonical = next(
            (k for k in _TAG_KEYWORDS if k.lower() == tag_name), None
        )
        if canonical and _matches(text, canonical):
            matched.append(term_id)
    return matched
