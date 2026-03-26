"""
ICRA scraper — uses ICRA's JSON API to fetch corporate ratings.

Usage (via run_scraper.py):
    python run_scraper.py --icra
    python run_scraper.py --icra --limit 50
    python run_scraper.py --icra-scan               # scan ID range for missing companies
    python run_scraper.py --icra-scan --id-start 1 --id-end 40000
"""

import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm

from database.models import get_connection, init_db, upsert_company, insert_rating, insert_financial
from parsers.rating import normalize_rating
from parsers.pdf import extract_financials

logger = logging.getLogger(__name__)

BASE_URL = "https://www.icra.in"
LIST_ENDPOINT = "/Rating/GetPaginationData"
PDF_ENDPOINT = "/Rating/GetRationalReportFilePdf"

# Category IDs: 5=Corporate, 7=Infrastructure, 8=Financial Sector
CATEGORY_IDS = [5, 7, 8]
CATEGORY_NAMES = {5: "Corporate", 7: "Infrastructure", 8: "Financial Sector"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.icra.in/Rating/RatingCategory?RatingType=CR&RatingCategoryId=5",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _build_session(category_id: int = 5) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    # Prime session with cookies by visiting the main page first
    try:
        session.get(f"{BASE_URL}/Rating/RatingCategory?RatingType=CR&RatingCategoryId={category_id}", timeout=15)
    except Exception:
        pass
    return session


def _fetch_page(session: requests.Session, category_id: int, page: int) -> list:
    """Fetch one page of ratings for a category. Returns list of records or []."""
    import re, json as _json
    # Page 1 uses the main category page (has full HTML + embedded data)
    # Pages 2+ use the GetPaginationData endpoint
    if page == 1:
        url = f"{BASE_URL}/Rating/RatingCategory?RatingType=CR&RatingCategoryId={category_id}"
    else:
        url = f"{BASE_URL}{LIST_ENDPOINT}?RatingType=CR&RatingCategoryId={category_id}&page={page}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        # Data is embedded as: var RatingBaseModel = [...];
        m = re.search(r'var\s+RatingBaseModel\s*=\s*(\[.*?\]);', resp.text, re.DOTALL)
        if m:
            return _json.loads(m.group(1))
        # If no RatingBaseModel found, page is empty (end of pagination)
        return []
    except Exception as exc:
        logger.error("Error fetching ICRA page cat=%d page=%d: %s", category_id, page, exc)
        return []


def _fetch_pdf_bytes(session: requests.Session, rationale_id, company_name: str) -> Optional[bytes]:
    """Fetch the rationale PDF into memory and return bytes, or None on failure."""
    url = BASE_URL + PDF_ENDPOINT
    params = {"id": rationale_id}
    try:
        resp = session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "")
        if "pdf" not in content_type.lower() and "octet" not in content_type.lower():
            logger.debug("PDF response content-type unexpected: %s for id=%s", content_type, rationale_id)
        if len(resp.content) < 1000:
            logger.warning("PDF too small (%d bytes) for id=%s", len(resp.content), rationale_id)
            return None
        logger.debug("Fetched PDF in memory: id=%s (%d bytes)", rationale_id, len(resp.content))
        return resp.content
    except Exception as exc:
        logger.error("Failed to fetch PDF id=%s for %s: %s", rationale_id, company_name, exc)
        return None


def _parse_record(record: dict) -> dict:
    """
    Normalise a raw ICRA API record into our internal format.

    ICRA API field names can vary; we try multiple known variants.
    """
    def get(keys, default=None):
        for k in keys if isinstance(keys, list) else [keys]:
            if k in record and record[k] not in (None, ""):
                return record[k]
        return default

    company_name = get(["CompanyName", "companyName", "Company", "company"], "")
    raw_rating = get([
        "Ratings", "Rating", "rating", "RatingSymbol", "ratingSymbol",
        "CurrentRating", "currentRating",
    ], "")
    sector = get(["SectorName", "sectorName", "Sector", "sector"], "")
    sub_sector = get(["SubSectorName", "subSectorName", "SubSector", "subSector"], "")
    instrument = get([
        "Instrument1", "Instrument", "instrument", "InstrumentType",
        "instrumentType", "FacilityType",
    ], "")
    rating_date = get([
        "RatingDate", "ratingDate", "Date", "date",
        "EffectiveDate", "effectiveDate", "ActionDate",
    ], "")
    rationale_id = get([
        "RationaleId", "rationaleId", "RatId", "ratId", "Id", "id",
    ], "")
    company_id_icra = get(["CompanyId", "companyId"], "")
    rated_amount = get([
        "RatedAmount", "ratedAmount", "Amount", "amount",
        "LimitSanctioned", "FacilitySize",
    ], None)

    # Try to parse rated_amount as float (strip non-numeric)
    if rated_amount is not None:
        try:
            # Remove commas and units
            import re
            amount_str = re.sub(r"[^\d.]", "", str(rated_amount))
            rated_amount = float(amount_str) if amount_str else None
        except Exception:
            rated_amount = None

    normalized = normalize_rating(str(raw_rating))

    return {
        "company_name": str(company_name).strip(),
        "raw_rating": str(raw_rating).strip(),
        "rating_base": normalized["base"],
        "rating_grade": normalized["grade"],
        "outlook": normalized["outlook"],
        "sector": str(sector).strip(),
        "sub_sector": str(sub_sector).strip(),
        "instrument": str(instrument).strip(),
        "rating_date": str(rating_date).strip() if rating_date else None,
        "rationale_id": str(rationale_id).strip() if rationale_id else None,
        "company_id_icra": str(company_id_icra).strip() if company_id_icra else None,
        "rated_amount_cr": rated_amount,
    }


def run(limit: Optional[int] = None) -> dict:
    """
    Main ICRA scrape entry point — fetches ratings list only, no PDFs.
    Financial data is sourced via BSE XBRL (listed) and run_pdf_pass() (unlisted).

    Args:
        limit: Max records to process (for testing). None = no limit.

    Returns:
        dict with counts: {records_scraped, companies_upserted, errors}
    """
    init_db()
    conn = get_connection()

    counts = {"records_scraped": 0, "companies_upserted": 0, "errors": 0}
    total_processed = 0
    stop_flag = False

    for cat_id in CATEGORY_IDS:
        if stop_flag:
            break
        cat_name = CATEGORY_NAMES[cat_id]
        logger.info("Scraping ICRA category: %s (id=%d)", cat_name, cat_id)
        session = _build_session(cat_id)

        page = 1
        with tqdm(desc=f"ICRA {cat_name}", unit="records") as pbar:
            while True:
                records = _fetch_page(session, cat_id, page)

                if not records:
                    logger.debug("Empty response at page %d for category %s — stopping", page, cat_name)
                    break

                for record in records:
                    if limit is not None and total_processed >= limit:
                        stop_flag = True
                        break

                    try:
                        parsed = _parse_record(record)
                    except Exception as exc:
                        logger.error("Error parsing record: %s — %s", record, exc)
                        counts["errors"] += 1
                        continue

                    if not parsed["company_name"]:
                        counts["errors"] += 1
                        continue

                    try:
                        company_id = upsert_company(conn, parsed["company_name"])
                        counts["companies_upserted"] += 1

                        insert_rating(
                            conn,
                            company_id,
                            agency="ICRA",
                            rating_symbol=parsed["raw_rating"],
                            rating_grade=parsed["rating_grade"],
                            outlook=parsed["outlook"],
                            instrument_type=parsed["instrument"],
                            rated_amount_cr=parsed["rated_amount_cr"],
                            rating_date=parsed["rating_date"],
                            sector=parsed["sector"],
                            sub_sector=parsed["sub_sector"],
                            source_id=parsed["rationale_id"],
                            rationale_url=(
                                "https://www.icra.in/Rating/RatingDetails"
                                f"?CompanyId={parsed['company_id_icra']}"
                                f"&CompanyName={urllib.parse.quote(parsed['company_name'])}"
                                if parsed.get("company_id_icra") else None
                            ),
                        )
                        counts["records_scraped"] += 1

                    except Exception as exc:
                        logger.error("DB error for company %s: %s", parsed["company_name"], exc)
                        counts["errors"] += 1
                        continue

                    total_processed += 1
                    pbar.update(1)

                if stop_flag:
                    break

                page += 1
                time.sleep(0.5)  # polite delay between pages

    conn.close()
    logger.info(
        "ICRA scrape complete. Records: %d, Companies: %d, Errors: %d",
        counts["records_scraped"],
        counts["companies_upserted"],
        counts["errors"],
    )
    return counts


def run_pdf_pass(limit: Optional[int] = None, min_grade: int = 5) -> dict:
    """
    Secondary pass: fetch ICRA rationale PDFs in-memory for unlisted companies
    that don't have financial data yet. Runs after BSE XBRL so we only hit
    companies BSE couldn't cover.

    Args:
        limit:     Max companies to process. None = no limit.
        min_grade: Only fetch PDFs for companies rated this grade or better (default 5 = A+).
    """
    conn = get_connection()
    session = _build_session()

    counts = {"processed": 0, "financials_extracted": 0, "errors": 0}

    # Unlisted companies with a good rating and a rationale_url, no financials yet
    candidates = conn.execute("""
        SELECT DISTINCT
            c.id AS company_id,
            c.name,
            r.source_id AS rationale_id,
            r.rating_date
        FROM companies c
        JOIN ratings r ON r.company_id = c.id
        LEFT JOIN financials f ON f.company_id = c.id
        WHERE c.is_listed = 0
          AND r.agency = 'ICRA'
          AND r.rating_grade <= ?
          AND r.source_id IS NOT NULL
          AND f.id IS NULL
        ORDER BY r.rating_grade ASC, c.name ASC
    """, (min_grade,)).fetchall()

    logger.info("PDF pass: %d unlisted companies without financials", len(candidates))

    for row in candidates:
        if limit is not None and counts["processed"] >= limit:
            break

        company_id = row["company_id"]
        company_name = row["name"]
        rationale_id = row["rationale_id"]

        time.sleep(1.0)
        pdf_bytes = _fetch_pdf_bytes(session, rationale_id, company_name)
        if not pdf_bytes:
            counts["errors"] += 1
            counts["processed"] += 1
            continue

        try:
            fin_data = extract_financials(pdf_bytes)
            if fin_data and fin_data.get("confidence", 0) > 0:
                insert_financial(
                    conn,
                    company_id,
                    fiscal_year=fin_data.get("fiscal_year"),
                    revenue_cr=fin_data.get("revenue_cr"),
                    ebitda_cr=fin_data.get("ebitda_cr"),
                    ebitda_margin_pct=fin_data.get("ebitda_margin_pct"),
                    pat_cr=fin_data.get("pat_cr"),
                    total_debt_cr=fin_data.get("total_debt_cr"),
                    cash_cr=fin_data.get("cash_cr"),
                    net_debt_cr=fin_data.get("net_debt_cr"),
                    capex_cr=fin_data.get("capex_cr"),
                    interest_coverage=fin_data.get("interest_coverage"),
                    net_debt_ebitda=fin_data.get("net_debt_ebitda"),
                    data_source="icra_pdf",
                    extraction_confidence=fin_data.get("confidence"),
                )
                counts["financials_extracted"] += 1
        except Exception as exc:
            logger.error("Financials error for %s: %s", company_name, exc)
            counts["errors"] += 1

        counts["processed"] += 1

    conn.close()
    logger.info(
        "PDF pass complete. Processed: %d, Financials: %d, Errors: %d",
        counts["processed"], counts["financials_extracted"], counts["errors"],
    )
    return counts


# ------------------------------------------------------------------ #
# Discover missing companies via ICRA search API + detail pages      #
# ------------------------------------------------------------------ #

DISCOVER_CHECKPOINT = PROJECT_ROOT / "data" / "icra_discover_checkpoint.txt"
DETAIL_ENDPOINT = "/Rating/RatingDetails"
SEARCH_ENDPOINT = "/Rating/GetRatingCompanys"


def _load_known_icra_ids(conn) -> set:
    """Return set of ICRA company IDs (as strings) already in the DB."""
    rows = conn.execute(
        "SELECT rationale_url FROM ratings WHERE agency='ICRA' AND rationale_url LIKE '%CompanyId=%'"
    ).fetchall()
    ids = set()
    for row in rows:
        m = re.search(r"CompanyId=(\d+)", row["rationale_url"] or "")
        if m:
            ids.add(m.group(1))
    return ids


def _get_all_icra_ids(session: requests.Session) -> dict:
    """
    Query ICRA's search API with a-z + 0-9 to discover all rated companies.
    Returns {company_id_str: company_name} for the full ICRA universe.
    """
    import string
    all_companies = {}
    terms = list(string.ascii_lowercase) + list(string.digits)
    for term in tqdm(terms, desc="ICRA search API", unit="terms"):
        try:
            resp = session.post(
                f"{BASE_URL}{SEARCH_ENDPOINT}",
                data={"Term": term},
                timeout=15,
            )
            resp.raise_for_status()
            for item in resp.json():
                cid = str(item.get("id", "")).strip()
                name = str(item.get("label", "")).strip()
                if not cid or not name:
                    continue
                # Some structured finance entries encode multiple companies as
                # "id1#id2#..." with "name1#name2#..." — split and handle each
                if "#" in cid or "#" in name:
                    ids_parts = cid.split("#")
                    names_parts = name.split("#")
                    for i, sub_id in enumerate(ids_parts):
                        sub_id = sub_id.strip()
                        sub_name = names_parts[i].strip() if i < len(names_parts) else ""
                        if sub_id.isdigit() and sub_name:
                            all_companies[sub_id] = sub_name
                else:
                    if cid.isdigit():
                        all_companies[cid] = name
        except Exception as exc:
            logger.warning("Search API error for term %r: %s", term, exc)
        time.sleep(0.2)
    return all_companies


def _fetch_and_parse_detail(session: requests.Session, company_id: str, company_name: str) -> Optional[dict]:
    """
    Fetch the RatingDetails page for a company and parse its rating data.
    The company name must be correct — ICRA uses it server-side to load data.
    Returns a list of rating record dicts, or None on failure.
    """
    import datetime as dt
    url = (
        f"{BASE_URL}{DETAIL_ENDPOINT}"
        f"?CompanyId={company_id}"
        f"&CompanyName={urllib.parse.quote(company_name)}"
    )
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        html = resp.text
    except Exception as exc:
        logger.debug("Fetch error for ID %s (%s): %s", company_id, company_name, exc)
        return None

    # Extract embedded JS: var RatingDetails = {...};
    m = re.search(r"var\s+RatingDetails\s*=\s*(\{.*?\});\s*\n", html, re.DOTALL)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
    except Exception:
        return None

    instruments = data.get("LstRatingInstrumentData") or []
    if not instruments:
        return None

    records = []
    for item in instruments:
        raw = (item.get("Long") or "").strip()
        outlook_str = (item.get("LongOutlook") or "").strip()
        combined = f"{raw} {outlook_str}".strip() if outlook_str else raw

        instrument = " / ".join(filter(None, [
            (item.get("Instrument1") or "").strip(),
            (item.get("Instrument2") or "").strip(),
            (item.get("Instrument3") or "").strip(),
        ])).strip(" /")

        rating_date = item.get("RatingDate") or item.get("RatingDateString")
        if rating_date:
            # ISO format: "2025-03-31T00:00:00+05:30"
            try:
                rating_date = rating_date[:10]  # keep YYYY-MM-DD
            except Exception:
                rating_date = None

        sector = (item.get("SectorName") or "").strip()
        sub_sector = (item.get("SubSectorName") or "").strip()
        rated_amount = item.get("RatingAmount") or item.get("RatedAmount")
        if rated_amount is not None:
            try:
                rated_amount = float(str(rated_amount)) or None
            except Exception:
                rated_amount = None

        normalized = normalize_rating(combined)
        if not normalized.get("grade"):
            continue

        records.append({
            "raw_rating": combined,
            "rating_grade": normalized["grade"],
            "outlook": normalized["outlook"],
            "instrument": instrument,
            "rating_date": rating_date,
            "sector": sector,
            "sub_sector": sub_sector,
            "rated_amount_cr": rated_amount,
        })

    return records if records else None


def run_id_scan(limit: Optional[int] = None, delay: float = 0.5) -> dict:
    """
    Discover ICRA-rated companies missing from the paginated listing by:
      1. Querying the ICRA search API (a–z, 0–9) to get all ~14k company IDs
      2. Diffing against what's already in the DB
      3. Fetching the RatingDetails page for each missing company

    Resumes from checkpoint (data/icra_discover_checkpoint.txt) if present.
    """
    init_db()
    conn = get_connection()
    session = _build_session()

    counts = {"discovered": 0, "added": 0, "no_rating": 0, "errors": 0}

    # Step 1: get full ICRA universe from search API
    logger.info("Querying ICRA search API to build full company universe...")
    all_icra = _get_all_icra_ids(session)
    logger.info("ICRA universe: %d companies", len(all_icra))

    # Step 2: diff against DB
    known_ids = _load_known_icra_ids(conn)
    missing = {cid: name for cid, name in all_icra.items() if cid not in known_ids}
    logger.info("Missing from DB: %d companies", len(missing))
    counts["discovered"] = len(missing)

    # Resume from checkpoint (sorted list so order is stable)
    checkpoint_id = None
    if DISCOVER_CHECKPOINT.exists():
        try:
            checkpoint_id = DISCOVER_CHECKPOINT.read_text().strip()
            logger.info("Resuming from checkpoint company_id=%s", checkpoint_id)
        except Exception:
            pass

    sorted_missing = sorted(missing.items(), key=lambda x: int(x[0]))
    if checkpoint_id:
        sorted_missing = [(cid, name) for cid, name in sorted_missing if int(cid) > int(checkpoint_id)]
        logger.info("%d companies remaining after checkpoint", len(sorted_missing))

    # Step 3: fetch detail pages for missing companies
    added_count = 0
    with tqdm(total=len(sorted_missing), desc="ICRA discover", unit="companies") as pbar:
        for company_id, company_name in sorted_missing:
            if limit is not None and added_count >= limit:
                break

            records = _fetch_and_parse_detail(session, company_id, company_name)

            if records is None:
                counts["no_rating"] += 1
            else:
                try:
                    cid = upsert_company(conn, company_name)
                    rationale_url = (
                        f"{BASE_URL}{DETAIL_ENDPOINT}"
                        f"?CompanyId={company_id}"
                        f"&CompanyName={urllib.parse.quote(company_name)}"
                    )
                    for idx, rec in enumerate(records):
                        insert_rating(
                            conn,
                            cid,
                            agency="ICRA",
                            rating_symbol=rec["raw_rating"],
                            rating_grade=rec["rating_grade"],
                            outlook=rec["outlook"],
                            instrument_type=rec["instrument"],
                            rated_amount_cr=rec["rated_amount_cr"],
                            rating_date=rec["rating_date"],
                            sector=rec["sector"],
                            sub_sector=rec["sub_sector"],
                            source_id=f"{company_id}_{idx}",
                            rationale_url=rationale_url,
                        )
                    counts["added"] += 1
                    added_count += 1
                    logger.debug("Added %r (ID %s, %d instruments)", company_name, company_id, len(records))
                except Exception as exc:
                    logger.error("DB error for ID %s (%s): %s", company_id, company_name, exc)
                    counts["errors"] += 1

            try:
                DISCOVER_CHECKPOINT.write_text(str(company_id))
            except Exception:
                pass

            pbar.update(1)
            time.sleep(delay)

    conn.close()
    logger.info(
        "ICRA discover complete. Discovered: %d, Added: %d, No rating data: %d, Errors: %d",
        counts["discovered"], counts["added"], counts["no_rating"], counts["errors"],
    )
    return counts
