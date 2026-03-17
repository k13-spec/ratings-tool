"""
CRISIL Financials scraper — fetches rationale HTML pages for CRISIL-rated companies
and extracts Key Financial Indicators tables.

Two-phase approach:
  Phase 1  build_index()  — crawl CRISIL listing API by date ranges to build a
             local company → ratingFileName index (saved to data/crisil_index.json).
             Uses date-chunked queries (daily for high-volume months, monthly for older).
             Much faster than one API call per company.

  Phase 2  run()          — for each DB company without financials, look up the
             index, fetch the rationale HTML, parse KFI table, insert into DB.

Usage:
    python run_scraper.py --crisil-index        # Phase 1: build/refresh index
    python run_scraper.py --crisil-financials   # Phase 2: extract financials
    python run_scraper.py --crisil-financials --limit 100
"""

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

from database.models import get_connection, init_db, insert_financial

logger = logging.getLogger(__name__)

BASE_URL         = "https://www.crisilratings.com"
LISTING_API      = (
    BASE_URL
    + "/content/crisilratings/en/home/our-business/ratings/rating-rationale"
    "/_jcr_content/wrapper_100_par/ratingresultlisting.results.json"
)
RATIONALE_BASE   = BASE_URL + "/mnt/winshare/Ratings/RatingList/RatingDocs/"
LISTING_REFERER  = BASE_URL + "/en/home/our-business/ratings/rating-rationale.html"
INDEX_PATH       = Path(__file__).resolve().parent.parent / "data" / "crisil_index.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*",
    "Referer": LISTING_REFERER,
}


# ---------------------------------------------------------------------- #
# Name normalisation                                                      #
# ---------------------------------------------------------------------- #
_SUFFIX_RE = re.compile(
    r"\b(limited|ltd|private|pvt|llp|llc|co\.?|corp\.?|inc\.?|plc)\b\.?",
    re.IGNORECASE,
)


def _norm(name: str) -> str:
    n = _SUFFIX_RE.sub("", str(name)).lower()
    return re.sub(r"\s+", " ", n).strip()


# ---------------------------------------------------------------------- #
# Phase 1: Build listing index                                            #
# ---------------------------------------------------------------------- #
def _fetch_date_range(from_d: str, to_d: str) -> list:
    """
    Fetch up to 100 listing records for a date range (creates its own session).
    Returns list of doc dicts or [] on failure.
    """
    filters = json.dumps({"fromDate": from_d, "toDate": to_d})
    try:
        resp = requests.get(
            LISTING_API,
            params={"cmd": "RR", "start": "0", "limit": "100", "filters": filters},
            headers=HEADERS,
            timeout=25,
        )
        if not resp.content:
            return []
        data = resp.json()
        return data.get("docs", [])
    except Exception as exc:
        logger.debug("Date-range fetch error %s–%s: %s", from_d, to_d, exc)
        return []


def _monthly_chunks(start: date, end: date) -> list:
    """Generate monthly (from_str, to_str) chunks covering start..end."""
    chunks = []
    cur = start.replace(day=1)
    while cur <= end:
        if cur.month == 12:
            month_end = date(cur.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(cur.year, cur.month + 1, 1) - timedelta(days=1)
        chunk_end = min(month_end, end)
        chunks.append((cur.strftime("%m/%d/%Y"), chunk_end.strftime("%m/%d/%Y")))
        cur = month_end + timedelta(days=1)
    return chunks


def _daily_chunks(start: date, end: date) -> list:
    """Generate daily (from_str, to_str) chunks covering start..end."""
    chunks = []
    cur = start
    while cur <= end:
        ds = cur.strftime("%m/%d/%Y")
        chunks.append((ds, ds))
        cur += timedelta(days=1)
    return chunks


def build_index(
    years_back: int = 3,
    force_rebuild: bool = False,
    workers: int = 8,
    daily: bool = True,
) -> dict:
    """
    Build or incrementally update the local company→ratingFileName index.
    Uses parallel HTTP requests for speed.

    Args:
        years_back:    How many years of history to crawl.
        force_rebuild: Ignore existing index and start fresh.
        workers:       Concurrent HTTP threads.
        daily:         Use daily chunks (more complete) vs monthly (faster bootstrap).

    Returns:
        The index dict {norm_name: {company_name, rating_file_name, ...}}.
    """
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Load existing index
    if not force_rebuild and INDEX_PATH.exists():
        with open(INDEX_PATH, encoding="utf-8") as f:
            saved = json.load(f)
        index     = saved.get("index", {})
        last_date = saved.get("last_date")
        logger.info("Loaded existing index: %d entries, last_date=%s", len(index), last_date)
    else:
        index     = {}
        last_date = None

    today      = date.today()
    end_date   = today
    start_date = today.replace(year=today.year - years_back)

    # Incremental: only fetch after last saved date
    if last_date and not force_rebuild:
        try:
            start_date = date.fromisoformat(last_date) + timedelta(days=1)
        except Exception:
            pass

    if start_date > end_date:
        logger.info("Index is up to date.")
        return index

    chunks = _daily_chunks(start_date, end_date) if daily else _monthly_chunks(start_date, end_date)
    mode   = "daily" if daily else "monthly"
    logger.info(
        "Building CRISIL index: %d %s chunks from %s to %s (workers=%d)",
        len(chunks), mode, start_date, end_date, workers,
    )

    new_entries = 0
    completed   = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_date_range, f, t): (f, t) for f, t in chunks}
        for fut in as_completed(futures):
            from_d, to_d = futures[fut]
            try:
                docs = fut.result()
            except Exception as exc:
                logger.debug("Chunk error %s–%s: %s", from_d, to_d, exc)
                docs = []

            for doc in docs:
                name  = doc.get("companyName", "")
                fname = doc.get("ratingFileName", "")
                if not name or not fname:
                    continue
                key = _norm(name)
                if key not in index:
                    index[key] = {
                        "company_name":     name,
                        "company_code":     doc.get("companyCode", ""),
                        "rating_file_name": fname,
                        "rating_date":      doc.get("ratingDate", ""),
                        "pr_id":            doc.get("prId", ""),
                    }
                    new_entries += 1

            completed += 1
            if completed % 10 == 0:
                logger.info(
                    "Index progress: %d/%d chunks done, %d entries (%d new)",
                    completed, len(chunks), len(index), new_entries,
                )

    _save_index(index, end_date.isoformat())
    logger.info(
        "Index build complete: %d total entries, %d new this run",
        len(index), new_entries,
    )
    return index


def _save_index(index: dict, last_date: str):
    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump({"last_date": last_date, "index": index}, f, ensure_ascii=False)


def load_index() -> dict:
    """Load the saved index, or return empty dict if not found."""
    if not INDEX_PATH.exists():
        return {}
    with open(INDEX_PATH, encoding="utf-8") as f:
        return json.load(f).get("index", {})


# ---------------------------------------------------------------------- #
# Phase 2: KFI HTML parsing                                               #
# ---------------------------------------------------------------------- #
_YEAR_RE = re.compile(
    r"\bFY\s*(\d{4})\b|\bMar[- ](\d{2,4})\b|\b(\d{4})\b",
    re.IGNORECASE,
)

# Keys longest-first so specific phrases match before short ones
_CORPORATE_MAP = {
    "revenue from operations": "revenue_cr",
    "operating income":        "revenue_cr",
    "total income":            "revenue_cr",
    "net revenue":             "revenue_cr",
    "net sales":               "revenue_cr",
    "revenue":                 "revenue_cr",
    "turnover":                "revenue_cr",
    "reported profit after tax": "pat_cr",
    "profit after tax":        "pat_cr",
    "net profit":              "pat_cr",
    "ebitda margin":           "ebitda_margin_pct",
    "opbdit/oi":               "ebitda_margin_pct",
    "pat margins":             "ebitda_margin_pct",
    "pat margin":              "ebitda_margin_pct",
    "ebitda":                  "ebitda_cr",
    "opbdit":                  "ebitda_cr",
    "pat":                     "pat_cr",
    "interest coverage":       "interest_coverage",
    "total debt":              "total_debt_cr",
    "net debt":                "net_debt_cr",
    "cash":                    "cash_cr",
    "capex":                   "capex_cr",
}

_NBFC_MAP = {
    "total assets":     "total_assets_cr",   # not in DB schema, silently dropped
    "total income":     "revenue_cr",
    "profit after tax": "pat_cr",
    "return on assets": "roa_pct",           # not in DB schema, silently dropped
    "net worth":        "net_worth_cr",      # not in DB schema, silently dropped
}


def _parse_year_header(header: str) -> Optional[int]:
    m = _YEAR_RE.search(header)
    if not m:
        return None
    yr_str = m.group(1) or m.group(2) or m.group(3)
    if not yr_str:
        return None
    yr = int(yr_str)
    if yr < 100:
        yr = 2000 + yr if yr < 50 else 1900 + yr
    return yr if 2000 <= yr <= 2030 else None


def _parse_value(val: str) -> Optional[float]:
    val = val.strip()
    if not val or val in ("-", "N/A", "NA", "Nil", "--"):
        return None
    negative = val.startswith("(") and val.endswith(")")
    val = re.sub(r"[^0-9.]", "", val)
    try:
        f = float(val)
        return -f if negative else f
    except ValueError:
        return None


def _find_kfi_elements(soup) -> list:
    """
    Find all elements that introduce a KFI table.

    CRISIL often splits the heading across multiple <span> tags, e.g.
      <span>Key Financial Indic</span><span>ators</span>
    so soup.find_all(string=regex) returns nothing.  Instead we search
    block-level elements by their combined get_text().
    """
    seen = set()
    found = []

    # Strategy 1: direct NavigableString match (works when text is unsplit)
    for ns in soup.find_all(string=re.compile(r"Key\s*Financial\s*Indicator", re.IGNORECASE)):
        el = ns.find_parent()
        if el and id(el) not in seen:
            seen.add(id(el))
            found.append(el)

    # Strategy 2: block/inline elements whose *combined* text matches
    # (handles split-span case)
    for tag in soup.find_all(["p", "div", "h1", "h2", "h3", "h4", "h5", "b", "strong", "u"]):
        if id(tag) in seen:
            continue
        if re.search(r"Key\s*Financial\s*Indicator", tag.get_text(separator=""), re.IGNORECASE):
            seen.add(id(tag))
            found.append(tag)

    return found


def _parse_kfi_table(html: str) -> list:
    """
    Parse all 'Key Financial Indicators' tables in the HTML.
    First-wins per (fiscal_year, field) — Combined/consolidated entity comes first.
    Returns list of dicts, one per fiscal year.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []
    year_data: dict[int, dict] = {}

    for el in _find_kfi_elements(soup):
        table = el.find_next("table")
        if not table:
            continue

        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Find year columns (check rows 0 and 1)
        year_cols: dict[int, int] = {}
        for row_idx in range(min(2, len(rows))):
            cells = [c.get_text(separator=" ", strip=True) for c in rows[row_idx].find_all(["td", "th"])]
            for i, h in enumerate(cells[2:], start=2):
                yr = _parse_year_header(h)
                if yr and yr not in year_cols.values():
                    year_cols[i] = yr

        if not year_cols:
            continue

        # Ensure year buckets exist
        for yr in year_cols.values():
            if yr not in year_data:
                year_data[yr] = {}

        # Parse data rows
        for row in rows[1:]:
            cells = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) < 3:
                continue
            label = cells[0].lower().strip()
            if not label:
                continue

            db_field = None
            for kw, field in {**_CORPORATE_MAP, **_NBFC_MAP}.items():
                if kw in label:
                    db_field = field
                    break
            if not db_field:
                continue

            for col_idx, yr in year_cols.items():
                if col_idx < len(cells):
                    val = _parse_value(cells[col_idx])
                    # First-wins: don't overwrite (Combined entity comes first)
                    if val is not None and db_field not in year_data[yr]:
                        year_data[yr][db_field] = val

    for yr, data in year_data.items():
        if any(v is not None for v in data.values()):
            data["fiscal_year"] = yr
            results.append(data)

    return results


# ---------------------------------------------------------------------- #
# Phase 2: Main extraction run                                            #
# ---------------------------------------------------------------------- #
def run(limit: Optional[int] = None) -> dict:
    """
    Extract financials for CRISIL-rated DB companies using the local index.

    Args:
        limit: Max companies to process. None = no limit.

    Returns:
        Count dict.
    """
    init_db()
    conn = get_connection()

    counts = {
        "processed": 0,
        "financials_extracted": 0,
        "not_found": 0,
        "errors": 0,
    }

    # Load index
    index = load_index()
    if not index:
        logger.warning(
            "CRISIL index is empty. Run --crisil-index first to build the listing index."
        )
        conn.close()
        return counts

    logger.info("CRISIL financials: loaded index with %d entries", len(index))

    # Companies with CRISIL ratings but no financial data.
    # Priority: corporate sectors first, then infrastructure, then financial.
    candidates = conn.execute("""
        SELECT DISTINCT
            c.id   AS company_id,
            c.name,
            r.rating_grade,
            r.sector
        FROM companies c
        JOIN ratings r ON r.company_id = c.id
        LEFT JOIN financials f ON f.company_id = c.id
        WHERE r.agency = 'CRISIL'
          AND f.id IS NULL
        ORDER BY
            CASE r.sector
                WHEN 'Financial Sector' THEN 3
                WHEN 'Infrastructure'   THEN 2
                ELSE 1
            END ASC,
            r.rating_grade ASC,
            c.name ASC
    """).fetchall()

    logger.info(
        "CRISIL financials: %d companies without financial data to process",
        len(candidates),
    )

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(LISTING_REFERER, timeout=15)
    except Exception:
        pass

    for row in candidates:
        if limit is not None and counts["processed"] >= limit:
            break

        company_id   = row["company_id"]
        company_name = row["name"]

        # Look up in index — try exact norm first, then prefix match
        norm_name = _norm(company_name)
        entry = index.get(norm_name)

        if not entry:
            # Try prefix: first 30 chars
            prefix = norm_name[:30]
            for key, val in index.items():
                if key.startswith(prefix) or prefix in key:
                    entry = val
                    break

        if not entry:
            counts["not_found"] += 1
            counts["processed"] += 1
            continue

        rating_file = entry.get("rating_file_name", "")
        if not rating_file:
            counts["not_found"] += 1
            counts["processed"] += 1
            continue

        # Fetch rationale HTML
        try:
            resp = session.get(RATIONALE_BASE + rating_file, timeout=20)
            resp.raise_for_status()
            html = resp.text
        except Exception as exc:
            logger.debug("HTML fetch error for %s: %s", company_name, exc)
            counts["errors"] += 1
            counts["processed"] += 1
            time.sleep(0.3)
            continue

        # Parse KFI
        financials = _parse_kfi_table(html)
        if not financials:
            counts["not_found"] += 1
            counts["processed"] += 1
            time.sleep(0.2)
            continue

        # Insert most recent 2 fiscal years
        financials_sorted = sorted(
            financials, key=lambda x: x.get("fiscal_year", 0) or 0, reverse=True
        )
        inserted = 0
        for fin in financials_sorted[:2]:
            try:
                insert_financial(
                    conn,
                    company_id,
                    fiscal_year=fin.get("fiscal_year"),
                    revenue_cr=fin.get("revenue_cr"),
                    ebitda_cr=fin.get("ebitda_cr"),
                    ebitda_margin_pct=fin.get("ebitda_margin_pct"),
                    pat_cr=fin.get("pat_cr"),
                    total_debt_cr=fin.get("total_debt_cr"),
                    cash_cr=fin.get("cash_cr"),
                    net_debt_cr=fin.get("net_debt_cr"),
                    capex_cr=fin.get("capex_cr"),
                    interest_coverage=fin.get("interest_coverage"),
                    net_debt_ebitda=None,
                    data_source="crisil_html",
                    extraction_confidence=0.85,
                )
                inserted += 1
            except Exception as exc:
                logger.error("DB error for %s: %s", company_name, exc)
                counts["errors"] += 1

        if inserted:
            counts["financials_extracted"] += inserted
        else:
            counts["not_found"] += 1

        counts["processed"] += 1
        time.sleep(0.15)  # gentle rate limit for HTML fetches

        if counts["processed"] % 100 == 0:
            logger.info(
                "CRISIL financials progress: %d processed, %d extracted, %d not found",
                counts["processed"],
                counts["financials_extracted"],
                counts["not_found"],
            )

    conn.close()
    logger.info(
        "CRISIL financials complete. Processed: %d, Extracted: %d, Not found: %d, Errors: %d",
        counts["processed"],
        counts["financials_extracted"],
        counts["not_found"],
        counts["errors"],
    )
    return counts
