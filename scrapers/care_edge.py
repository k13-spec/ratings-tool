"""
CareEdge (CARE Ratings) scraper.

Three modes:
  run(conn, limit)          — fetch CARE Edge ratings for DB companies without one,
                              plus extract brief financials from the PR PDF.
  run_financials(conn, limit)— extract brief financials from PDFs for companies that
                              already have a CARE Edge rating but no financials yet.
  run_discover(conn, limit) — alphabetical prefix search to find net-new companies
                              not yet in the DB, then add them with ratings+financials.

API endpoints (discovered via JS inspection of careratings.com):
  GET /header/searchlist?cinput={query}
      → {"data": [{"CompanyID": "...", "CompanyName": "..."}], "report": [...]}
  GET /rrcompany?companyName={name}&YearID=0&fdate=&tdate=
      → [{"CompanyID", "CompanyName", "FileTitle", "FileType", "FileURL", "PublishedDate"}]
  PDF: GET /upload/CompanyFiles/PR/{FileURL}

PDF structure:
  Page 1  – rating header table (Instruments | Amount | Rating | Action)
  Last pages – Brief Financials table (TOI, PBILDT, PAT, gearing, interest coverage)
               Industry classification table (sector)
               Annexure-1 (detailed instrument list)
"""

import io
import logging
import re
import socket
import string
import time
from typing import Optional

import pdfplumber
import requests
from tqdm import tqdm

from database.models import (
    get_connection,
    init_db,
    insert_financial,
    insert_rating,
    upsert_company,
)
from parsers.rating import normalize_rating

logger = logging.getLogger(__name__)

BASE_URL = "https://www.careratings.com"
SEARCH_URL = f"{BASE_URL}/header/searchlist"
COMPANY_URL = f"{BASE_URL}/rrcompany"
PDF_BASE_URL = f"{BASE_URL}/upload/CompanyFiles/PR"
AGENCY = "CARE Edge"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.careratings.com/find-ratings",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
}

# ------------------------------------------------------------------ #
# Session & HTTP helpers                                               #
# ------------------------------------------------------------------ #

def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(f"{BASE_URL}/find-ratings", timeout=15)
    except Exception:
        pass
    return session


def _search_companies(session: requests.Session, query: str) -> list:
    """Search CareEdge for companies matching query. Returns list of dicts."""
    try:
        resp = session.get(SEARCH_URL, params={"cinput": query}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except Exception as exc:
        logger.debug("CE search failed for %r: %s", query, exc)
        return []


def _get_company_prs(session: requests.Session, company_name: str) -> list:
    """Get press releases for a company, sorted newest first."""
    try:
        resp = session.get(
            COMPANY_URL,
            params={"companyName": company_name, "YearID": "0", "fdate": "", "tdate": ""},
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        # API returns {"data": [...]} or a bare list
        if isinstance(payload, dict):
            data = payload.get("data", [])
        elif isinstance(payload, list):
            data = payload
        else:
            data = []
        data.sort(key=lambda x: x.get("PublishedDate", ""), reverse=True)
        return data
    except Exception as exc:
        logger.debug("CE PR list failed for %r: %s", company_name, exc)
        return []


def _fetch_pdf_bytes(session: requests.Session, file_url: str) -> Optional[bytes]:
    """Download a CARE Edge press release PDF. Returns bytes or None."""
    url = f"{PDF_BASE_URL}/{file_url}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "pdf" not in ct and len(resp.content) < 1000:
            return None
        return resp.content
    except Exception as exc:
        logger.debug("CE PDF fetch failed for %r: %s", file_url, exc)
        return None


# ------------------------------------------------------------------ #
# Name normalisation (mirrors database.models._normalize_name)        #
# ------------------------------------------------------------------ #

def _norm(name: str) -> str:
    n = name.lower().strip()
    n = n.replace("limited", "ltd")
    n = n.replace("private", "pvt")
    n = re.sub(r"\s+", " ", n)
    return n


def _find_match(search_results: list, db_name: str) -> Optional[dict]:
    """
    Return the first search result that is a good match for db_name.
    Tries exact normalized match, then prefix match.
    """
    db_norm = _norm(db_name)
    for result in search_results:
        ce_name = result.get("CompanyName", "")
        ce_norm = _norm(ce_name)
        if ce_norm == db_norm:
            return result
        if ce_norm.startswith(db_norm) or db_norm.startswith(ce_norm):
            if min(len(ce_norm), len(db_norm)) >= 10:   # avoid spurious short prefix matches
                return result
    return None


# ------------------------------------------------------------------ #
# PDF parsers                                                          #
# ------------------------------------------------------------------ #

def _extract_rating_date(pdf_bytes: bytes) -> Optional[str]:
    """Extract the press release date from page 1 text."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return None
            text = pdf.pages[0].extract_text() or ""
            m = re.search(
                r"(January|February|March|April|May|June|July|August|September"
                r"|October|November|December)\s+\d{1,2},?\s+\d{4}",
                text, re.IGNORECASE,
            )
            if m:
                return m.group(0).strip()
    except Exception:
        pass
    return None


def _extract_sector(pdf_bytes: bytes) -> Optional[str]:
    """
    Extract sector from the 'Industry classification' table.
    The table has a header row with 'Macroeconomic indicator / Sector / Industry / Basic industry'
    and a data row below it.
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                if "industry classification" not in text.lower():
                    continue
                for table in page.extract_tables():
                    for i, row in enumerate(table):
                        row_text = " ".join(str(c or "") for c in row).lower()
                        if "macroeconomic" in row_text or ("sector" in row_text and "industry" in row_text):
                            # The data row is right below this header
                            if i + 1 < len(table):
                                data_row = table[i + 1]
                                for cell in data_row:
                                    val = str(cell or "").strip()
                                    if val and val.lower() not in ("", "none"):
                                        return val
    except Exception:
        pass
    return None


def _parse_rating_table(pdf_bytes: bytes) -> list:
    """
    Parse the Facilities/Instruments table on page 1.

    Returns list of dicts:
      instrument_name, instrument_type, rated_amount_cr,
      rating_symbol, rating_grade, outlook, rating_action
    """
    results = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return []
            page = pdf.pages[0]
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue
                header = [str(c or "").lower() for c in table[0]]
                if not any("facilities" in h or "instruments" in h for h in header):
                    continue

                for row in table[1:]:
                    if not row or not any(row):
                        continue
                    instrument_raw = str(row[0] or "").strip()
                    amount_raw    = str(row[1] or "").strip() if len(row) > 1 else ""
                    rating_raw    = str(row[2] or "").strip() if len(row) > 2 else ""
                    action_raw    = str(row[3] or "").strip() if len(row) > 3 else ""

                    if not rating_raw or instrument_raw.lower().startswith("facilities"):
                        continue

                    # Amount
                    amount_cr = None
                    try:
                        cleaned = re.sub(r"[^\d.]", "", amount_raw.replace(",", ""))
                        if cleaned:
                            amount_cr = float(cleaned)
                    except Exception:
                        pass

                    # Instrument type
                    il = instrument_raw.lower()
                    if "short" in il and "long" in il:
                        instr_type = "LT/ST"
                    elif "short" in il or "commercial paper" in il:
                        instr_type = "ST"
                    else:
                        instr_type = "LT"

                    # Rating: "CARE AA+; Stable / CARE A1+"  → take LT part
                    lt_part = rating_raw.split(" / ")[0].strip() if " / " in rating_raw else rating_raw
                    normalized = normalize_rating(lt_part)

                    results.append({
                        "instrument_name": instrument_raw,
                        "instrument_type": instr_type,
                        "rated_amount_cr": amount_cr,
                        "rating_symbol": normalized.get("base"),
                        "rating_grade": normalized.get("grade"),
                        "outlook": normalized.get("outlook") or "",
                        "rating_action": action_raw,
                    })

                if results:
                    break   # use first matching table only
    except Exception as exc:
        logger.debug("CE rating table parse error: %s", exc)
    return results


def _extract_fiscal_year(text: str) -> Optional[int]:
    """Parse fiscal year from various formats: 'March 31, 2024 (A)', 'FY25', 'FY2025'."""
    # "March 31, 2024 (A)" → 2024
    m = re.search(r"march\s+\d{1,2},?\s*(\d{4})", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # "FY2025" → 2025
    m = re.search(r"\bFY\s*(20\d{2})\b", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # "FY25" → 2025
    m = re.search(r"\bFY\s*(\d{2})\b(?!\d)", text, re.IGNORECASE)
    if m:
        return 2000 + int(m.group(1))
    return None


def _map_label(label: str) -> Optional[str]:
    """Map a Brief Financials row label to a DB field name."""
    l = label.lower()
    if any(x in l for x in ["total operating income", "net sales", "turnover",
                              "total revenue", "revenue from operations", "total income"]):
        return "revenue_cr"
    if any(x in l for x in ["pbildt", "ebitda", "pbdit", "operating profit",
                              "profit before interest", "ebit"]):
        return "ebitda_cr"
    if any(x in l for x in ["profit after tax", "pat", "net profit", "profit for the year"]):
        return "pat_cr"
    if any(x in l for x in ["interest coverage", "icr", "dscr", "debt service"]):
        return "interest_coverage"
    if "total debt" in l or "total borrowing" in l:
        return "total_debt_cr"
    if "net debt" in l:
        return "net_debt_cr"
    if "cash" in l and "equivalent" in l:
        return "cash_cr"
    if "capex" in l or "capital expenditure" in l:
        return "capex_cr"
    # skip: gearing (ratio), debt/ebitda (ratio), net worth, etc.
    return None


def _parse_value(cell) -> Optional[float]:
    """Parse a numeric value from a table cell."""
    if cell is None:
        return None
    text = str(cell).strip().replace(",", "")
    if text in ("-", "--", "na", "n.a.", "n/a", ""):
        return None
    text = re.sub(r"\*+$", "", text).strip()  # strip trailing asterisks
    try:
        return float(text)
    except ValueError:
        return None


def _parse_brief_financials(pdf_bytes: bytes) -> list:
    """
    Parse the 'Brief Financials' table from a CARE Edge PDF.

    Returns list of dicts per audited fiscal year:
      {fiscal_year, revenue_cr, ebitda_cr, ebitda_margin_pct, pat_cr,
       interest_coverage, data_source, extraction_confidence}

    Skips partial-year columns (9M, 6M, Q).
    """
    all_years: dict = {}

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for table in page.extract_tables():
                    if not table:
                        continue

                    # Find the header row containing "Brief Financials" or year patterns
                    hdr_idx = None
                    for i, row in enumerate(table):
                        row_text = " ".join(str(c or "") for c in row)
                        if "brief financials" in row_text.lower():
                            hdr_idx = i
                            break

                    if hdr_idx is None:
                        continue

                    header = table[hdr_idx]

                    # Map column index → fiscal year (skip partial years)
                    year_cols: dict = {}
                    for j, cell in enumerate(header):
                        cell_str = str(cell or "").strip()
                        cl = cell_str.lower()
                        # Skip partial-year labels
                        if re.match(r"\d?[hHqQmM]fy", cl) or cl.startswith("9m") or cl.startswith("6m") or cl.startswith("q"):
                            continue
                        fy = _extract_fiscal_year(cell_str)
                        if fy:
                            year_cols[j] = fy

                    if not year_cols:
                        continue

                    for fy in year_cols.values():
                        all_years.setdefault(fy, {})

                    # Parse data rows
                    for row in table[hdr_idx + 1:]:
                        if not row:
                            continue
                        # Label: first non-empty cell in first 3 cols
                        label = ""
                        for cell in row[:3]:
                            if cell and str(cell).strip():
                                label = str(cell).strip()
                                break
                        if not label:
                            continue

                        field = _map_label(label)
                        if not field:
                            continue

                        for col_idx, fy in year_cols.items():
                            if col_idx < len(row):
                                val = _parse_value(row[col_idx])
                                if val is not None:
                                    all_years[fy][field] = val

    except Exception as exc:
        logger.debug("CE brief financials parse error: %s", exc)

    # Build result list
    result = []
    for fy, fields in sorted(all_years.items(), reverse=True):
        if not fields.get("revenue_cr"):
            continue
        rec = {"fiscal_year": fy, **fields}

        # Derive EBITDA margin
        if rec.get("revenue_cr") and rec.get("ebitda_cr"):
            rec["ebitda_margin_pct"] = round(rec["ebitda_cr"] / rec["revenue_cr"] * 100, 2)

        # Confidence: proportion of key fields populated
        key_fields = ["revenue_cr", "ebitda_cr", "pat_cr", "interest_coverage"]
        confidence = sum(1 for f in key_fields if rec.get(f)) / len(key_fields)
        rec["extraction_confidence"] = round(confidence, 2)
        rec["data_source"] = "care_edge_pdf"

        result.append(rec)

    return result


# ------------------------------------------------------------------ #
# Shared helper: process one company's PDF                            #
# ------------------------------------------------------------------ #

def _process_pdf(
    session: requests.Session,
    conn,
    company_id: int,
    company_name: str,
    file_url: str,
    stats: dict,
    insert_ratings: bool = True,
):
    """Download PDF for file_url, parse rating + financials, upsert to DB."""
    pdf_bytes = _fetch_pdf_bytes(session, file_url)
    if not pdf_bytes:
        return

    pdf_url = f"{PDF_BASE_URL}/{file_url}"

    if insert_ratings:
        rating_date = _extract_rating_date(pdf_bytes)
        sector      = _extract_sector(pdf_bytes)
        instruments = _parse_rating_table(pdf_bytes)

        # Use the best (lowest grade = best quality) long-term instrument
        best = None
        for instr in instruments:
            if instr.get("rating_grade") is not None:
                if best is None or instr["rating_grade"] < best["rating_grade"]:
                    best = instr

        if best:
            insert_rating(
                conn, company_id,
                agency=AGENCY,
                rating_symbol=best["rating_symbol"],
                rating_grade=best["rating_grade"],
                outlook=best["outlook"],
                instrument_type=best["instrument_type"],
                instrument_name=best["instrument_name"],
                rated_amount_cr=best["rated_amount_cr"],
                rating_date=rating_date,
                sector=sector,
                rationale_url=pdf_url,
                source_id=file_url,        # filename is unique per press release
            )
            stats["ratings_added"] = stats.get("ratings_added", 0) + 1
        else:
            logger.debug("CE no parseable LT rating in PDF for %r", company_name)

    # Financials
    fin_records = _parse_brief_financials(pdf_bytes)
    for fin in fin_records:
        fy = fin.pop("fiscal_year", None)
        if fy:
            insert_financial(conn, company_id, fiscal_year=fy, **fin)
            stats["financials_added"] = stats.get("financials_added", 0) + 1


# ------------------------------------------------------------------ #
# Public entry points                                                  #
# ------------------------------------------------------------------ #

def run(conn=None, limit: int = None) -> dict:
    """
    For each company in DB without a CARE Edge rating:
      1. Search CareEdge by company name (fallback: first significant word)
      2. If match found, fetch most recent PR PDF
      3. Parse rating + brief financials → upsert to DB
    """
    if conn is None:
        init_db()
        conn = get_connection()

    rows = conn.execute("""
        SELECT DISTINCT c.id, c.name
        FROM companies c
        WHERE NOT EXISTS (
            SELECT 1 FROM ratings r
            WHERE r.company_id = c.id AND r.agency = ?
        )
        AND NOT EXISTS (
            SELECT 1 FROM financials f
            WHERE f.company_id = c.id
        )
        ORDER BY c.name
    """, (AGENCY,)).fetchall()

    if limit:
        rows = rows[:limit]

    logger.info("CareEdge ratings: %d companies to search", len(rows))
    socket.setdefaulttimeout(30)
    session = _build_session()
    stats = {"searched": 0, "matched": 0, "ratings_added": 0, "financials_added": 0, "errors": 0}

    for row in tqdm(rows, desc="CareEdge", unit="co"):
        company_id   = row["id"]
        company_name = row["name"]
        try:
            stats["searched"] += 1

            # Primary search: full name
            results = _search_companies(session, company_name)
            match   = _find_match(results, company_name)

            # Fallback: first significant word (>3 chars)
            if not match:
                words = [w for w in company_name.split() if len(w) > 3]
                if words:
                    results = _search_companies(session, words[0])
                    match   = _find_match(results, company_name)

            if not match:
                time.sleep(0.3)
                continue

            stats["matched"] += 1
            ce_name = match["CompanyName"]

            prs = _get_company_prs(session, ce_name)
            if not prs:
                time.sleep(0.3)
                continue

            file_url = prs[0].get("FileURL", "")
            if not file_url:
                continue

            _process_pdf(session, conn, company_id, company_name, file_url, stats,
                         insert_ratings=True)
            time.sleep(0.5)

        except Exception as exc:
            logger.error("CE error for %r: %s", company_name, exc)
            stats["errors"] += 1

        if stats["searched"] % 500 == 0:
            logger.info(
                "CareEdge progress: %d searched, %d matched, %d ratings, %d financials, %d errors",
                stats["searched"], stats["matched"],
                stats.get("ratings_added", 0), stats.get("financials_added", 0),
                stats["errors"],
            )

    logger.info("CareEdge ratings done: %s", stats)
    return stats


def run_financials(conn=None, limit: int = None) -> dict:
    """
    For companies that already have a CARE Edge rating but no financials:
    fetch their stored PDF URL and extract brief financials.
    """
    if conn is None:
        init_db()
        conn = get_connection()

    rows = conn.execute("""
        SELECT DISTINCT c.id, c.name, r.rationale_url
        FROM companies c
        JOIN ratings r ON r.company_id = c.id
        WHERE r.agency = ?
          AND r.rationale_url IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM financials f WHERE f.company_id = c.id
          )
        ORDER BY c.name
    """, (AGENCY,)).fetchall()

    if limit:
        rows = rows[:limit]

    logger.info("CareEdge financials: %d companies to process", len(rows))
    socket.setdefaulttimeout(30)
    session = _build_session()
    stats = {"processed": 0, "financials_added": 0, "errors": 0}

    for row in tqdm(rows, desc="CareEdge Financials", unit="co"):
        company_id   = row["id"]
        company_name = row["name"]
        rationale_url = row["rationale_url"]
        try:
            file_url = rationale_url.split("/")[-1] if rationale_url else None
            if not file_url:
                continue

            _process_pdf(session, conn, company_id, company_name, file_url, stats,
                         insert_ratings=False)
            stats["processed"] += 1
            time.sleep(0.4)

        except Exception as exc:
            logger.error("CE financials error for %r: %s", company_name, exc)
            stats["errors"] += 1

    logger.info("CareEdge financials done: %s", stats)
    return stats


def run_discover(conn=None, limit: int = None) -> dict:
    """
    Alphabetical prefix search (aa, ab, ..., zz) to discover companies on CareEdge
    that are not yet in the DB.  For each new company found, adds it with rating +
    brief financials extracted from the most recent press release PDF.

    limit controls the number of 2-letter prefixes searched (max 676).
    """
    if conn is None:
        init_db()
        conn = get_connection()

    queries = [a + b for a in string.ascii_lowercase for b in string.ascii_lowercase]
    if limit:
        queries = queries[:limit]

    logger.info("CareEdge discover: %d prefix queries to run", len(queries))
    socket.setdefaulttimeout(30)
    session = _build_session()
    stats = {
        "queries": 0, "new_companies": 0,
        "ratings_added": 0, "financials_added": 0, "errors": 0,
    }
    seen_ce_names: set = set()

    for query in tqdm(queries, desc="CareEdge Discover", unit="prefix"):
        try:
            results = _search_companies(session, query)
            stats["queries"] += 1

            for ce_result in results:
                ce_name = ce_result.get("CompanyName", "").strip()
                if not ce_name or ce_name in seen_ce_names:
                    continue
                seen_ce_names.add(ce_name)

                # Check if already in DB by normalized name
                norm = _norm(ce_name)
                existing = conn.execute(
                    "SELECT id FROM companies WHERE name_normalized = ?", (norm,)
                ).fetchone()
                if existing:
                    continue

                # New company — fetch most recent PR PDF
                prs = _get_company_prs(session, ce_name)
                if not prs:
                    continue
                file_url = prs[0].get("FileURL", "")
                if not file_url:
                    continue

                company_id = upsert_company(conn, ce_name)
                stats["new_companies"] += 1

                _process_pdf(session, conn, company_id, ce_name, file_url, stats,
                             insert_ratings=True)
                time.sleep(0.5)

        except Exception as exc:
            logger.error("CE discover error for query=%r: %s", query, exc)
            stats["errors"] += 1

        time.sleep(0.3)

        if stats["queries"] % 50 == 0:
            logger.info(
                "CareEdge discover progress: %d queries, %d new companies, %d ratings, %d financials",
                stats["queries"], stats["new_companies"],
                stats.get("ratings_added", 0), stats.get("financials_added", 0),
            )

    logger.info("CareEdge discover done: %s", stats)
    return stats
