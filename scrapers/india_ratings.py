"""
India Ratings and Research scraper.

Enumerates issuer IDs (1 → MAX_ISSUER_ID) via the /home/GetIssuerDetails JSON API,
upserts companies and inserts ratings.  Saves a checkpoint so interrupted runs resume
from where they left off.

Usage via run_scraper.py:
    python run_scraper.py --india-ratings
    python run_scraper.py --india-ratings --limit 200   # test run
"""

import logging
import time
from pathlib import Path

import requests
from tqdm import tqdm

from database.models import get_connection, init_db, upsert_company, insert_rating
from parsers.rating import normalize_rating

logger = logging.getLogger(__name__)

AGENCY         = "India Ratings"
BASE_URL       = "https://www.indiaratings.co.in"
API_BASE       = f"{BASE_URL}/home"
MAX_ISSUER_ID  = 15000
CHECKPOINT     = Path(__file__).parent.parent / "data" / "india_ratings_checkpoint.txt"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":  "application/json, text/plain, */*",
    "Referer": "https://www.indiaratings.co.in/",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _get_issuer(session: requests.Session, issuer_id: int) -> dict | None:
    url = f"{API_BASE}/GetIssuerDetails?issuerId={issuer_id}"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data:
            return None
        return data[0] if isinstance(data, list) else data
    except Exception as exc:
        logger.debug("Issuer %d fetch error: %s", issuer_id, exc)
        return None


def _parse_amount(raw) -> float | None:
    if not raw:
        return None
    try:
        return float(str(raw).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _instrument_type(instrument_name: str, raw_symbol: str) -> str:
    name = (instrument_name or "").lower()
    sym  = (raw_symbol or "").upper()
    if any(x in name for x in ["commercial paper", " cp", "short term", "short-term"]):
        return "ST"
    if "A1" in sym or "A2" in sym or "A3" in sym or "A4" in sym:
        return "ST"
    return "LT"


def _load_checkpoint() -> int:
    if CHECKPOINT.exists():
        try:
            return int(CHECKPOINT.read_text().strip())
        except Exception:
            pass
    return 0


def _save_checkpoint(issuer_id: int):
    CHECKPOINT.write_text(str(issuer_id))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(conn=None, limit: int = None, reset: bool = False) -> dict:
    """
    Enumerate India Ratings issuer IDs and upsert companies + ratings.
    Resumes from checkpoint unless reset=True.
    """
    if conn is None:
        init_db()
        conn = get_connection()

    last_done = 0 if reset else _load_checkpoint()
    start_id  = last_done + 1
    end_id    = MAX_ISSUER_ID

    if limit:
        end_id = min(start_id + limit - 1, MAX_ISSUER_ID)

    id_range = range(start_id, end_id + 1)
    if last_done:
        logger.info("India Ratings: resuming from issuer ID %d (checkpoint)", start_id)
    logger.info("India Ratings: scanning IDs %d to %d", start_id, end_id)

    session = _build_session()
    stats = {
        "processed": 0, "found": 0, "empty": 0,
        "companies_upserted": 0, "ratings_added": 0, "errors": 0,
    }

    for issuer_id in tqdm(id_range, desc="India Ratings", unit="id"):
        try:
            issuer = _get_issuer(session, issuer_id)
            stats["processed"] += 1

            if not issuer:
                stats["empty"] += 1
                time.sleep(0.15)
                continue

            name = (issuer.get("name") or "").strip()
            if not name:
                stats["empty"] += 1
                continue

            stats["found"] += 1
            sector      = issuer.get("sector")    or None
            sub_sector  = issuer.get("subSector") or None
            eff_date    = issuer.get("effectiveDate") or None

            company_id = upsert_company(conn, name)
            stats["companies_upserted"] += 1

            ratings_list = issuer.get("ratingsList") or []
            for idx, r in enumerate(ratings_list):
                raw_rating      = (r.get("rating")         or "").strip()
                instrument_name = (r.get("instrumentName") or "").strip()
                amount          = _parse_amount(r.get("amount"))

                # "IND AA+ / Stable"  →  symbol + outlook
                if " / " in raw_rating:
                    raw_symbol, raw_outlook = raw_rating.split(" / ", 1)
                    raw_symbol  = raw_symbol.strip()
                    raw_outlook = raw_outlook.strip()
                else:
                    raw_symbol  = raw_rating
                    raw_outlook = None

                norm    = normalize_rating(raw_symbol)
                outlook = raw_outlook or norm.get("outlook")

                insert_rating(
                    conn, company_id,
                    agency          = AGENCY,
                    rating_symbol   = norm.get("base") or raw_symbol,
                    rating_grade    = norm.get("grade"),
                    outlook         = outlook,
                    instrument_type = _instrument_type(instrument_name, raw_symbol),
                    instrument_name = instrument_name,
                    rated_amount_cr = amount,
                    rating_date     = eff_date,
                    sector          = sector,
                    sub_sector      = sub_sector,
                    rationale_url   = f"{BASE_URL}/Issuers?issuerID={issuer_id}",
                    source_id       = f"{issuer_id}_{idx}",
                )
                stats["ratings_added"] += 1

            _save_checkpoint(issuer_id)

            if stats["processed"] % 500 == 0:
                logger.info(
                    "India Ratings progress: %d processed, %d found, "
                    "%d ratings added, %d errors",
                    stats["processed"], stats["found"],
                    stats["ratings_added"], stats["errors"],
                )

            time.sleep(0.3)

        except Exception as exc:
            logger.error("Error on issuer_id=%d: %s", issuer_id, exc)
            stats["errors"] += 1
            time.sleep(1)

    logger.info("India Ratings complete: %s", stats)
    return stats
