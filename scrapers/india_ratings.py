"""
India Ratings and Research scraper.

Enumerates issuer IDs (1 → MAX_ISSUER_ID) via the /home/GetIssuerDetails JSON API,
upserts companies and inserts ratings.  Saves a checkpoint so interrupted runs resume
from where they left off.

Usage via run_scraper.py:
    python run_scraper.py --india-ratings
    python run_scraper.py --india-ratings --limit 200   # test run
"""

import hashlib
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from tqdm import tqdm

from database.models import get_connection, init_db, upsert_company, insert_rating
from parsers.rating import normalize_rating
from parsers.dates import to_iso

logger = logging.getLogger(__name__)

AGENCY         = "India Ratings"
BASE_URL       = "https://www.indiaratings.co.in"
API_BASE       = f"{BASE_URL}/home"
# 2026-10-05: raised 15000 -> 18000; the audit tracks the highest live ID and
# flags "ceiling pressure" when new issuers approach the scan ceiling.
MAX_ISSUER_ID  = 18000
FETCH_WORKERS  = 4      # concurrent issuer fetches
FETCH_CHUNK    = 20     # IDs per batch (checkpoint granularity)
CHUNK_PAUSE_S  = 0.5    # pause between batches
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


_tls = threading.local()


def _thread_session() -> requests.Session:
    """One requests.Session per worker thread."""
    if not hasattr(_tls, "session"):
        _tls.session = _build_session()
    return _tls.session


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

def _store_issuer(conn, issuer_id: int, issuer: dict, stats: dict) -> None:
    """Upsert one issuer and its ratings (main thread only: owns the DB)."""
    name = (issuer.get("name") or "").strip()
    if not name:
        stats["empty"] += 1
        return

    stats["found"] += 1
    sector      = issuer.get("sector")    or None
    sub_sector  = issuer.get("subSector") or None
    eff_date    = issuer.get("effectiveDate") or None
    # 2026-10-05: normalize to ISO so "latest per agency" text
    # ordering is chronological across all agencies' rows.
    eff_date    = to_iso(eff_date) or eff_date

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
        symbol  = norm.get("base") or raw_symbol

        # Content-hash source_id: the old positional "{issuer_id}_{idx}"
        # was stable regardless of rating content, so re-scans could
        # never record a rating CHANGE (dedupe skipped the new state).
        # Hashing instrument+symbol+outlook means unchanged rows dedupe
        # and changed ratings insert a fresh row.
        # Must stay in sync with the backfill formula in db_maintenance.py.
        _sid = f"{issuer_id}_h" + hashlib.sha1(
            f"{instrument_name}|{symbol}|{outlook or ''}".encode()
        ).hexdigest()[:10]

        insert_rating(
            conn, company_id,
            agency          = AGENCY,
            rating_symbol   = symbol,
            rating_grade    = norm.get("grade"),
            outlook         = outlook,
            instrument_type = _instrument_type(instrument_name, raw_symbol),
            instrument_name = instrument_name,
            rated_amount_cr = amount,
            rating_date     = eff_date,
            sector          = sector,
            sub_sector      = sub_sector,
            rationale_url   = f"{BASE_URL}/Issuers?issuerID={issuer_id}",
            source_id       = _sid,
        )
        stats["ratings_added"] += 1



def run(conn=None, limit: int = None, reset: bool = False,
        max_seconds: float = None) -> dict:
    """
    Enumerate India Ratings issuer IDs and upsert companies + ratings.
    Resumes from checkpoint unless reset=True.
    """
    if conn is None:
        init_db()
        conn = get_connection()

    last_done = 0 if reset else _load_checkpoint()
    if last_done >= MAX_ISSUER_ID:
        # Previous scan finished: start a fresh pass (rolling full rescan).
        logger.info("India Ratings: checkpoint at ceiling, starting a new pass")
        last_done = 0
    start_id  = last_done + 1
    end_id    = MAX_ISSUER_ID

    if limit:
        end_id = min(start_id + limit - 1, MAX_ISSUER_ID)

    id_range = range(start_id, end_id + 1)
    if last_done:
        logger.info("India Ratings: resuming from issuer ID %d (checkpoint)", start_id)
    logger.info("India Ratings: scanning IDs %d to %d", start_id, end_id)

    stats = {
        "processed": 0, "found": 0, "empty": 0,
        "companies_upserted": 0, "ratings_added": 0, "errors": 0,
    }

    # 2026-11-12: fetch concurrently, write serially. The sequential scan of
    # 18k IDs took ~4h and, with everything else, overran the workflow's
    # 350-minute window. A small pool keeps the request rate modest (a few
    # per second) while the DB stays single-writer in this thread.
    t_start = time.time()
    ids = list(id_range)
    stopped_early = False

    def _fetch(issuer_id):
        try:
            return issuer_id, _get_issuer(_thread_session(), issuer_id), None
        except Exception as exc:          # network / JSON errors
            return issuer_id, None, exc

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool, \
            tqdm(total=len(ids), desc="India Ratings", unit="id") as pbar:
        for i in range(0, len(ids), FETCH_CHUNK):
            if max_seconds is not None and time.time() - t_start >= max_seconds:
                logger.info("India Ratings: time budget (%.0fs) reached at ID %d; "
                            "next run resumes from the checkpoint",
                            max_seconds, ids[i] - 1)
                stopped_early = True
                break
            chunk = ids[i:i + FETCH_CHUNK]
            for issuer_id, issuer, exc in pool.map(_fetch, chunk):
                stats["processed"] += 1
                if exc is not None:
                    logger.error("Error on issuer_id=%d: %s", issuer_id, exc)
                    stats["errors"] += 1
                    continue
                if not issuer:
                    stats["empty"] += 1
                    continue
                try:
                    _store_issuer(conn, issuer_id, issuer, stats)
                except Exception as exc2:
                    logger.error("Error on issuer_id=%d: %s", issuer_id, exc2)
                    stats["errors"] += 1
            _save_checkpoint(chunk[-1])
            pbar.update(len(chunk))
            if (i // FETCH_CHUNK) % 25 == 24:
                logger.info(
                    "India Ratings progress: %d processed, %d found, "
                    "%d ratings added, %d errors",
                    stats["processed"], stats["found"],
                    stats["ratings_added"], stats["errors"],
                )
            time.sleep(CHUNK_PAUSE_S)

    stats["stopped_early"] = int(stopped_early)
    logger.info("India Ratings complete: %s", stats)
    return stats
