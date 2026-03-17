"""
CRISIL scraper — uses CRISIL's internal JSON suggest API.

The endpoint returns all 38k+ ratings in a single call, no pagination needed.
Each record is: {"value": "Company Name:Rating action text"}

Usage:
    python run_scraper.py --crisil
    python run_scraper.py --crisil --limit 100
"""

import json
import logging
import re
import urllib.parse
from pathlib import Path
from typing import Optional

import requests

from database.models import get_connection, init_db, upsert_company, insert_rating
from parsers.rating import normalize_rating

logger = logging.getLogger(__name__)

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
INDEX_PATH     = PROJECT_ROOT / "data" / "crisil_index.json"
RATIONALE_BASE = "https://www.crisilratings.com/mnt/winshare/Ratings/RatingList/RatingDocs/"
GENERIC_URL    = (
    "https://www.crisilratings.com/en/home/our-business/"
    "ratings/rating-rationale.html"
)

SUGGEST_URL = (
    "https://www.crisilratings.com/content/crisilratings/en/home/our-business/"
    "ratings/rating-rationale/_jcr_content/wrapper_100_par/"
    "ratingresultlisting.suggest.RR.rating_rr.json"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.crisilratings.com/",
    "Accept": "application/json, */*",
}

# ------------------------------------------------------------------ #
# Rating action text parser                                           #
# ------------------------------------------------------------------ #
# Patterns like: "Crisil BB- / Stable", "Crisil AAA (Stable)",
#                "Crisil A1+", "Crisil BB+/Negative"
_RATING_RE = re.compile(
    r"['\"]?(CRISIL\s+[A-Z]{1,3}[+\-]?(?:\s*/\s*[A-Za-z]+)?)['\"]?",
    re.IGNORECASE,
)

_OUTLOOK_RE = re.compile(
    r"\b(Stable|Positive|Negative|Watch|Developing)\b",
    re.IGNORECASE,
)

_DATE_RE = re.compile(
    r"\b(\d{1,2}[\s\-/][A-Za-z]{3}[\s\-/]\d{2,4}"
    r"|\d{4}[\-/]\d{2}[\-/]\d{2}"
    r"|\d{1,2}/\d{1,2}/\d{2,4})\b"
)

# Sector keywords — ordered longest-first so more-specific phrases take priority
# (e.g. "housing finance" before "finance", "real estate" before "estate")
_SECTOR_HINTS = {
    # Financial Sector
    "housing finance": "Financial Sector",
    "asset management": "Financial Sector",
    "microfinance": "Financial Sector",
    "stock broker": "Capital Markets",
    "securities": "Capital Markets",
    "broking": "Capital Markets",
    "financial": "Financial Sector",
    "finance": "Financial Sector",
    "lending": "Financial Sector",
    "nbfc": "Financial Sector",
    "mfin": "Financial Sector",
    "leasing": "Financial Sector",
    "wealth": "Financial Sector",
    "invest": "Financial Sector",
    "bank": "Financial Sector",
    "insurance": "Insurance",
    "credit": "Financial Sector",
    # Infrastructure / Power / Gas
    "infrastructure": "Infrastructure",
    "wind energy": "Power",
    "solar": "Power",
    "thermal power": "Power",
    "hydropower": "Power",
    "hydro power": "Power",
    "transmission": "Infrastructure",
    "highway": "Infrastructure",
    "airport": "Infrastructure",
    "seaport": "Infrastructure",
    "metro rail": "Infrastructure",
    "railway": "Infrastructure",
    "gas pipeline": "Gas",
    "city gas": "Gas",
    "natural gas": "Gas",
    "pipeline": "Infrastructure",
    "water treatment": "Other Utilities",
    "sewage": "Other Utilities",
    "infra": "Infrastructure",
    "power": "Power",
    "electric": "Power",
    "gas": "Gas",
    "port": "Infrastructure",
    # Healthcare
    "pharma": "Pharmaceuticals",
    "pharmaceutical": "Pharmaceuticals",
    "hospital": "Healthcare",
    "diagnostic": "Healthcare",
    "pathology": "Healthcare",
    "clinic": "Healthcare",
    "medic": "Healthcare",
    "health": "Healthcare",
    "ayurved": "Healthcare",
    "biotech": "Pharmaceuticals",
    # Auto / Transport
    "automobile": "Auto",
    "vehicle": "Auto",
    "tractor": "Auto",
    "two-wheel": "Auto",
    "tyres": "Auto",
    "tyre": "Auto",
    "auto component": "Auto",
    "auto": "Auto",
    # Textile
    "apparel": "Textile",
    "garment": "Textile",
    "textile": "Textile",
    "spinning": "Textile",
    "weaving": "Textile",
    "fabric": "Textile",
    "denim": "Textile",
    "hosiery": "Textile",
    "yarn": "Textile",
    "cotton": "Textile",
    # Cement
    "cement": "Cement",
    # Metals & Mining
    "aluminium": "Metals",
    "aluminum": "Metals",
    "copper": "Metals",
    "ferrous": "Metals",
    "non-ferrous": "Metals",
    "steel": "Metals",
    "metal": "Metals",
    "iron": "Metals",
    "zinc": "Metals",
    "minerals": "Mining",
    "mining": "Mining",
    "granite": "Mining",
    "coal": "Mining",
    "mine": "Mining",
    # Technology / IT
    "software": "Technology",
    "it services": "IT Services",
    "information technology": "Technology",
    "tech": "Technology",
    "it ": "Technology",
    # Telecom
    "telecom": "Telecom",
    "telecommunication": "Telecom",
    # Retail
    "supermarket": "Retail",
    "retail": "Retail",
    "retailing": "Retail",
    # Real Estate
    "real estate": "Real Estate",
    "realty": "Real Estate",
    "township": "Real Estate",
    "developer": "Real Estate",
    "property": "Real Estate",
    # Hotels / Hospitality
    "hospitality": "Hotels",
    "hotel": "Hotels",
    "resort": "Hotels",
    "restaurant": "Hotels",
    "catering": "Hotels",
    # Food & Beverages / Agriculture
    "distillery": "Food & Beverages",
    "brewery": "Food & Beverages",
    "beverage": "Food & Beverages",
    "food": "Food & Beverages",
    "dairy": "Food & Beverages",
    "poultry": "Agriculture",
    "agro": "Agriculture",
    "agriculture": "Agriculture",
    "fertilizer": "Agriculture",
    "pesticide": "Agriculture",
    "seed": "Agriculture",
    # Education
    "university": "Education",
    "education": "Education",
    "school": "Education",
    "college": "Education",
    "coaching": "Education",
    "institute": "Education",
    # Paper
    "paper": "Paper",
    "pulp": "Paper",
    "printing": "Printing",
    # Chemicals
    "petrochemical": "Chemicals",
    "chemical": "Chemicals",
    # Media
    "media": "Media",
    "publishing": "Media",
    # Logistics
    "warehousing": "Logistics",
    "logistics": "Logistics",
    "freight": "Logistics",
    "courier": "Logistics",
    "shipping": "Logistics",
    "transport": "Logistics",
    # Construction
    "construction": "Construction",
    "builder": "Construction",
    # Energy / Oil
    "petroleum": "Energy",
    "refinery": "Energy",
    "energy": "Energy",
    "oil": "Energy",
    # Trading
    "trading": "Trading",
    "merchant": "Trading",
}


def _parse_record(value: str) -> Optional[dict]:
    """
    Parse a single suggest record string.
    Format: "Company Name:Action text mentioning rating"
    Returns dict or None if company name can't be extracted.
    """
    if ":" not in value:
        return None

    colon_idx = value.index(":")
    company_name = value[:colon_idx].strip()
    action_text = value[colon_idx + 1:].strip()

    if not company_name:
        return None

    # Extract rating symbol
    raw_rating = None
    m = _RATING_RE.search(action_text)
    if m:
        raw_rating = m.group(1).strip()

    # Extract outlook
    outlook = ""
    m = _OUTLOOK_RE.search(action_text)
    if m:
        outlook = m.group(1).capitalize()

    # If rating contains slash + outlook (e.g. "CRISIL BB+/Stable"), split it
    if raw_rating and "/" in raw_rating:
        parts = raw_rating.split("/")
        raw_rating = parts[0].strip()
        if not outlook and len(parts) > 1:
            outlook = parts[1].strip().capitalize()

    # Extract date
    rating_date = None
    m = _DATE_RE.search(action_text)
    if m:
        rating_date = m.group(1)

    # Rough sector from company name / action text
    sector = ""
    combined = (company_name + " " + action_text).lower()
    for kw, label in _SECTOR_HINTS.items():
        if kw in combined:
            sector = label
            break

    normalized = normalize_rating(raw_rating or "")

    return {
        "company_name": company_name,
        "raw_rating": raw_rating or "",
        "rating_grade": normalized["grade"],
        "outlook": outlook or normalized["outlook"],
        "rating_date": rating_date,
        "sector": sector,
        "action_text": action_text,
    }


def run(limit: Optional[int] = None, dry_run: bool = False) -> dict:
    """
    Main CRISIL scrape entry point.

    Args:
        limit:   Max records to process. None = no limit.
        dry_run: Print first 5 parsed records without writing to DB.
    """
    counts = {"records_scraped": 0, "companies_upserted": 0, "errors": 0}

    # Load rating_file_name index for direct rationale URLs
    _fname_map: dict = {}
    if INDEX_PATH.exists():
        try:
            idx = json.loads(INDEX_PATH.read_text(encoding="utf-8")).get("index", {})
            _fname_map = {k: v["rating_file_name"] for k, v in idx.items() if v.get("rating_file_name")}
            logger.info("Loaded %d file entries from index", len(_fname_map))
        except Exception as exc:
            logger.warning("Could not load crisil_index for file URLs: %s", exc)

    def _norm(s: str) -> str:
        s = s.lower().strip()
        s = re.sub(r'\b(limited|ltd|private|pvt|llp)\b', '', s)
        return re.sub(r'\s+', ' ', s).strip()

    logger.info("Fetching CRISIL suggest API...")
    try:
        resp = requests.get(SUGGEST_URL, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        raw_records = resp.json()
    except Exception as exc:
        logger.error("Failed to fetch CRISIL data: %s", exc)
        return counts

    logger.info("CRISIL: %d raw records fetched", len(raw_records))

    if dry_run:
        print(f"\n=== DRY RUN: {len(raw_records)} records total, showing first 5 parsed ===\n")
        for item in raw_records[:10]:
            parsed = _parse_record(item.get("value", ""))
            if parsed:
                print(parsed)
        return {"dry_run": True, "total_records": len(raw_records)}

    if not dry_run:
        init_db()
        conn = get_connection()

    total_processed = 0
    for item in raw_records:
        if limit is not None and total_processed >= limit:
            break

        value = item.get("value", "")
        if not value:
            continue

        parsed = _parse_record(value)
        if not parsed or not parsed["company_name"]:
            counts["errors"] += 1
            continue

        try:
            company_id = upsert_company(conn, parsed["company_name"])
            counts["companies_upserted"] += 1

            fname = _fname_map.get(_norm(parsed["company_name"]))
            insert_rating(
                conn,
                company_id,
                agency="CRISIL",
                rating_symbol=parsed["raw_rating"],
                rating_grade=parsed["rating_grade"],
                outlook=parsed["outlook"],
                rating_date=parsed["rating_date"],
                sector=parsed["sector"],
                rationale_url=(
                    RATIONALE_BASE + urllib.parse.quote(fname) if fname else GENERIC_URL
                ),
            )
            counts["records_scraped"] += 1

        except Exception as exc:
            logger.error("DB error for %s: %s", parsed["company_name"], exc)
            counts["errors"] += 1
            continue

        total_processed += 1
        if total_processed % 1000 == 0:
            logger.info("CRISIL: processed %d records...", total_processed)

    conn.close()
    logger.info(
        "CRISIL scrape complete. Records: %d, Companies: %d, Errors: %d",
        counts["records_scraped"],
        counts["companies_upserted"],
        counts["errors"],
    )
    return counts
