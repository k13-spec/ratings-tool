"""
Rationale ND/EBITDA Scraper
============================
For each company in the DB, fetches the latest rating rationale page/PDF
from one agency (priority: CRISIL > ICRA > India Ratings > CARE Edge),
searches for the agency-specific net-debt/EBITDA phrase, extracts the
ratio value, and writes it to financials.nd_ebitda_text + nd_ebitda_source.

Agency-specific search phrases:
  CRISIL        -> "net debt to EBITDA" / "net debt-to-EBITDA"
  ICRA          -> "net debt/OPBDITA"
  India Ratings -> "net adjusted debt/EBITDA"
  CARE Edge     -> "net debt/PBILDT"

Usage:
    python run_scraper.py --rationale-nd-ebitda
    python run_scraper.py --rationale-nd-ebitda --limit 100 --force
"""

import io
import logging
import re
import time
from pathlib import Path
from typing import Optional, Tuple

import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
}

# ---------------------------------------------------------------------------
# Agency config: (phrase_patterns, is_pdf_url_hint)
# ---------------------------------------------------------------------------
AGENCY_CONFIG = {
    "CRISIL": {
        "phrases": [
            r"net\s+debt[\s\-]+to[\s\-]+EBITDA",
            r"net\s+debt[\s/]+EBITDA",
        ],
        "is_html": True,   # CRISIL rationale pages are HTML
    },
    "ICRA": {
        "phrases": [
            r"net\s+debt\s*/\s*OPBDITA",
            r"net\s+debt[\s/]+OPBDIT(?:A)?",
        ],
        "is_html": False,  # ICRA rationale pages are PDFs
    },
    "India Ratings": {
        "phrases": [
            r"net\s+adjusted\s+debt\s*/\s*EBITDA",
            r"net\s+adj(?:usted)?\s+debt[\s/]+EBITDA",
        ],
        "is_html": False,
    },
    "CARE Edge": {
        "phrases": [
            r"net\s+debt\s*/\s*PBILDT",
            r"net\s+debt[\s/]+PBILDT",
        ],
        "is_html": False,
    },
}

# Priority order
AGENCY_PRIORITY = ["CRISIL", "ICRA", "India Ratings", "CARE Edge"]

# Value extraction: look for a number like 1.2x or 1.2 times within ~200 chars
# of the phrase match
VALUE_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:x|times|X)\b"
    r"|"
    r"(\d+(?:\.\d+)?)\s*(?=\s+(?:as\s+of|in\s+FY|for\s+FY|\())",
    re.IGNORECASE,
)
# Also try "was X.Xx" or ": X.Xx" or "of X.Xx" within the window
VALUE_NEAR_RE = re.compile(
    r"(?:was|is|at|of|:)\s+(\d+(?:\.\d+)?)\s*(?:x|times)?",
    re.IGNORECASE,
)


def _extract_value_near(text: str, match_start: int, window: int = 300) -> Optional[str]:
    """
    Extract a ratio value near a phrase match.
    Returns a string like "1.2x" or None.
    """
    snippet = text[match_start:match_start + window]
    m = VALUE_RE.search(snippet)
    if m:
        val = m.group(1) or m.group(2)
        if val:
            return val + "x"
    m = VALUE_NEAR_RE.search(snippet)
    if m:
        return m.group(1) + "x"
    return None


def _fetch_html_text(url: str, session: requests.Session) -> Optional[str]:
    """Fetch HTML rationale page and return plain text."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            # Remove script/style noise
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            return soup.get_text(" ", strip=True)
        except ImportError:
            return resp.text
    except Exception as exc:
        logger.debug("HTML fetch failed %s: %s", url, exc)
        return None


def _fetch_pdf_text(url: str, session: requests.Session) -> Optional[str]:
    """Download PDF bytes and extract text using pdfplumber."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=45)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
            # Might be HTML — try HTML extraction instead
            logger.debug("URL %s returned content-type %s, trying as HTML", url, content_type)
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                for tag in soup(["script", "style"]):
                    tag.decompose()
                return soup.get_text(" ", strip=True)
            except Exception:
                return resp.text
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        pages.append(t)
                return "\n".join(pages)
        except ImportError:
            logger.warning("pdfplumber not installed — PDF text extraction disabled")
            return None
    except Exception as exc:
        logger.debug("PDF fetch failed %s: %s", url, exc)
        return None


def _search_phrase(text: str, patterns: list) -> Optional[Tuple[str, int]]:
    """
    Search text for any of the compiled patterns.
    Returns (matched_text, start_position) or None.
    """
    for pat_str in patterns:
        m = re.search(pat_str, text, re.IGNORECASE)
        if m:
            return (m.group(0), m.start())
    return None


def _try_agency(
    agency: str,
    url: str,
    session: requests.Session,
) -> Optional[str]:
    """
    Fetch rationale for one agency and return the ratio value string like "1.2x",
    or None if not found.
    """
    cfg = AGENCY_CONFIG[agency]

    if cfg["is_html"]:
        text = _fetch_html_text(url, session)
    else:
        text = _fetch_pdf_text(url, session)

    if not text:
        return None

    hit = _search_phrase(text, cfg["phrases"])
    if not hit:
        return None

    phrase_text, pos = hit
    value = _extract_value_near(text, pos, window=350)
    if value:
        logger.debug("  Agency=%s phrase=%r value=%s", agency, phrase_text, value)
    return value


def _ensure_financials_row(conn, company_id: int):
    """Create a stub financials row if none exists."""
    conn.execute(
        "INSERT OR IGNORE INTO financials (company_id) VALUES (?)",
        (company_id,),
    )


def _update_nd_ebitda(conn, company_id: int, text_val: str, source: str):
    """Upsert nd_ebitda_text + nd_ebitda_source for a company."""
    _ensure_financials_row(conn, company_id)
    conn.execute(
        """
        UPDATE financials
           SET nd_ebitda_text = ?,
               nd_ebitda_source = ?
         WHERE company_id = ?
        """,
        (text_val, source, company_id),
    )
    conn.commit()


def run(limit: Optional[int] = None, force: bool = False) -> dict:
    """
    Main entry point.

    Args:
        limit: cap number of companies to process (for testing).
        force: re-process companies that already have nd_ebitda_text.

    Returns dict with summary stats.
    """
    from database.models import get_connection
    conn = get_connection()

    # --- build pivot of (company_id, agency_url) for each agency in priority order ---
    # Get one row per company with all 4 agency URLs
    rows = conn.execute(
        """
        WITH per_agency AS (
            SELECT r.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY r.company_id, r.agency
                       ORDER BY r.rating_date DESC NULLS LAST, r.id DESC
                   ) AS rn
            FROM ratings r
        ),
        latest AS (SELECT * FROM per_agency WHERE rn = 1),
        pivot AS (
            SELECT
                company_id,
                MAX(CASE WHEN agency = 'CRISIL'         THEN rationale_url END) AS crisil_url,
                MAX(CASE WHEN agency = 'ICRA'           THEN rationale_url END) AS icra_url,
                MAX(CASE WHEN agency = 'India Ratings'  THEN rationale_url END) AS ind_url,
                MAX(CASE WHEN agency = 'CARE Edge'      THEN rationale_url END) AS care_url
            FROM latest
            WHERE rationale_url IS NOT NULL AND rationale_url != ''
            GROUP BY company_id
        )
        SELECT
            c.id AS company_id,
            c.name AS company_name,
            p.crisil_url, p.icra_url, p.ind_url, p.care_url,
            f.nd_ebitda_text
        FROM companies c
        JOIN pivot p ON p.company_id = c.id
        LEFT JOIN (
            SELECT company_id, nd_ebitda_text
            FROM financials
        ) f ON f.company_id = c.id
        ORDER BY c.name
        """
    ).fetchall()

    if not force:
        # Skip companies that already have a value
        rows = [r for r in rows if not r["nd_ebitda_text"]]

    if limit:
        rows = rows[:limit]

    logger.info(
        "Rationale ND/EBITDA: %d companies to process%s",
        len(rows),
        " (force mode)" if force else "",
    )

    stats = {"processed": 0, "found": 0, "not_found": 0, "errors": 0}

    session = requests.Session()
    session.headers.update(HEADERS)

    for row in rows:
        company_id = row["company_id"]
        company_name = row["company_name"]
        stats["processed"] += 1

        url_map = {
            "CRISIL":        row["crisil_url"],
            "ICRA":          row["icra_url"],
            "India Ratings": row["ind_url"],
            "CARE Edge":     row["care_url"],
        }

        found = False
        for agency in AGENCY_PRIORITY:
            url = url_map.get(agency)
            if not url:
                continue

            logger.debug(
                "[%d/%d] %s — trying %s (%s)",
                stats["processed"], len(rows), company_name, agency, url[:80],
            )

            try:
                value = _try_agency(agency, url, session)
            except Exception as exc:
                logger.warning("Error processing %s / %s: %s", company_name, agency, exc)
                stats["errors"] += 1
                continue

            if value:
                logger.info(
                    "  FOUND  %-45s  %s  %s",
                    company_name[:45], agency, value,
                )
                _update_nd_ebitda(conn, company_id, value, agency)
                found = True
                stats["found"] += 1
                break

            # Small delay between requests
            time.sleep(0.5)

        if not found:
            stats["not_found"] += 1
            if stats["processed"] % 50 == 0:
                logger.info(
                    "  Progress: %d/%d  found=%d  not_found=%d  errors=%d",
                    stats["processed"], len(rows),
                    stats["found"], stats["not_found"], stats["errors"],
                )

    conn.close()
    logger.info(
        "Rationale ND/EBITDA done: %s",
        ", ".join(f"{k}={v}" for k, v in stats.items()),
    )
    return stats
