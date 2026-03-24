"""
ICRA scraper — uses ICRA's JSON API to fetch corporate ratings.

Usage (via run_scraper.py):
    python run_scraper.py --icra
    python run_scraper.py --icra --limit 50
"""

import logging
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
