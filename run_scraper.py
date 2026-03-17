#!/usr/bin/env python3
"""
CLI entry point for the ratings-tool scrapers.

Recommended flow (fastest, no local PDF storage):
    python run_scraper.py --icra          # Step 1: ratings list only (fast)
    python run_scraper.py --bse           # Step 2: financials for listed companies
    python run_scraper.py --icra-pdfs     # Step 3: financials for unlisted via PDF (optional)

Other:
    python run_scraper.py --crisil        # CRISIL ratings
    python run_scraper.py --all           # ICRA + CRISIL + BSE (steps 1+2 only)
    python run_scraper.py --icra --limit 50
"""

import argparse
import logging
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

# Ensure project root on path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Create data directory
(PROJECT_ROOT / "data").mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------ #
# Logging setup                                                        #
# ------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROJECT_ROOT / "data" / "scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("run_scraper")


# ------------------------------------------------------------------ #
# CLI argument parsing                                                 #
# ------------------------------------------------------------------ #
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Indian Credit Ratings Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--icra", action="store_true", help="Run ICRA scraper"
    )
    parser.add_argument(
        "--crisil", action="store_true", help="Run CRISIL scraper"
    )
    parser.add_argument(
        "--nse", action="store_true", help="Run NSE/yfinance financial data loader"
    )
    parser.add_argument(
        "--bse", action="store_true", help="Run BSE XBRL financial data loader (legacy, use --nse)"
    )
    parser.add_argument(
        "--icra-pdfs", action="store_true",
        help="PDF pass: fetch ICRA rationale PDFs in-memory for unlisted companies without financials"
    )
    parser.add_argument(
        "--crisil-index", action="store_true",
        help="Build/refresh the local CRISIL listing index (run before --crisil-financials)"
    )
    parser.add_argument(
        "--force-rebuild", action="store_true",
        help="Force full rebuild of CRISIL index (ignore existing index/last_date)"
    )
    parser.add_argument(
        "--crisil-financials", action="store_true",
        help="Fetch CRISIL rationale HTML pages and extract Key Financial Indicators for CRISIL-rated companies"
    )
    parser.add_argument(
        "--all", action="store_true", help="Run ICRA + CRISIL + NSE (ratings + listed financials)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Dry run mode (CRISIL: dumps card HTML for selector debugging)"
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Maximum number of records to process (useful for testing)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG logging"
    )
    parser.add_argument(
        "--push", action="store_true",
        help="After scraping, checkpoint WAL and git commit+push data/ratings.db to GitHub"
    )
    return parser


# ------------------------------------------------------------------ #
# Runner functions                                                     #
# ------------------------------------------------------------------ #
def run_icra(limit=None) -> dict:
    logger.info("=" * 60)
    logger.info("Starting ICRA scraper%s", f" (limit={limit})" if limit else "")
    logger.info("=" * 60)
    from scrapers.icra import run
    t0 = time.time()
    result = run(limit=limit)
    elapsed = time.time() - t0
    logger.info("ICRA done in %.1fs: %s", elapsed, ", ".join(f"{k}={v}" for k, v in result.items()))
    return result


def run_icra_pdfs(limit=None) -> dict:
    logger.info("=" * 60)
    logger.info("Starting ICRA PDF pass (unlisted companies)%s", f" (limit={limit})" if limit else "")
    logger.info("=" * 60)
    from scrapers.icra import run_pdf_pass
    t0 = time.time()
    result = run_pdf_pass(limit=limit)
    elapsed = time.time() - t0
    logger.info("ICRA PDF pass done in %.1fs: %s", elapsed, ", ".join(f"{k}={v}" for k, v in result.items()))
    return result


def run_crisil(limit=None, dry_run=False) -> dict:
    logger.info("=" * 60)
    logger.info(
        "Starting CRISIL scraper%s%s",
        f" (limit={limit})" if limit else "",
        " [DRY RUN]" if dry_run else "",
    )
    logger.info("=" * 60)
    from scrapers.crisil import run
    t0 = time.time()
    result = run(limit=limit, dry_run=dry_run)
    elapsed = time.time() - t0
    logger.info(
        "CRISIL done in %.1fs: %s",
        elapsed,
        ", ".join(f"{k}={v}" for k, v in result.items()),
    )
    return result


def run_nse(limit=None) -> dict:
    logger.info("=" * 60)
    logger.info("Starting NSE/yfinance financial loader%s", f" (limit={limit})" if limit else "")
    logger.info("=" * 60)
    from scrapers.nse_yfinance import run
    t0 = time.time()
    result = run(limit=limit)
    elapsed = time.time() - t0
    logger.info("NSE done in %.1fs: %s", elapsed, ", ".join(f"{k}={v}" for k, v in result.items()))
    return result


def run_crisil_index(force_rebuild=False) -> dict:
    logger.info("=" * 60)
    logger.info("Building CRISIL listing index%s", " (FORCE REBUILD)" if force_rebuild else "")
    logger.info("=" * 60)
    from scrapers.crisil_financials import build_index
    t0 = time.time()
    result = build_index(force_rebuild=force_rebuild)
    elapsed = time.time() - t0
    logger.info("CRISIL index done in %.1fs: %d entries", elapsed, len(result))
    return {"entries": len(result)}


def run_crisil_financials(limit=None) -> dict:
    logger.info("=" * 60)
    logger.info("Starting CRISIL financials scraper%s", f" (limit={limit})" if limit else "")
    logger.info("=" * 60)
    from scrapers.crisil_financials import run
    t0 = time.time()
    result = run(limit=limit)
    elapsed = time.time() - t0
    logger.info("CRISIL financials done in %.1fs: %s", elapsed, ", ".join(f"{k}={v}" for k, v in result.items()))
    return result


def run_bse(limit=None) -> dict:
    logger.info("=" * 60)
    logger.info("Starting BSE XBRL scraper%s", f" (limit={limit})" if limit else "")
    logger.info("=" * 60)
    from scrapers.bse_xbrl import run
    t0 = time.time()
    result = run(limit=limit)
    elapsed = time.time() - t0
    logger.info("BSE done in %.1fs: %s", elapsed, ", ".join(f"{k}={v}" for k, v in result.items()))
    return result


# ------------------------------------------------------------------ #
# Git auto-push                                                        #
# ------------------------------------------------------------------ #
def git_push_db() -> bool:
    """
    Checkpoint the SQLite WAL, then git add/commit/push data/ratings.db.
    Returns True on success, False if push was skipped or failed.
    """
    db_path = PROJECT_ROOT / "data" / "ratings.db"

    # 1. Checkpoint WAL so all scraper writes land in the main DB file
    try:
        conn = sqlite3.connect(str(db_path))
        result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        conn.close()
        logger.info("WAL checkpoint: busy=%s log=%s checkpointed=%s", *result)
    except Exception as exc:
        logger.error("WAL checkpoint failed: %s", exc)
        return False

    # 2. Stage data/ratings.db
    def _git(*args):
        return subprocess.run(
            ["git", "-C", str(PROJECT_ROOT), *args],
            capture_output=True, text=True,
        )

    _git("add", "data/ratings.db")

    # 3. Check if there's actually anything to commit
    diff = _git("diff", "--cached", "--quiet")
    if diff.returncode == 0:
        logger.info("git push: no changes to data/ratings.db, skipping")
        return True

    # 4. Commit with stats in message
    try:
        conn = sqlite3.connect(str(db_path))
        n = conn.execute("SELECT COUNT(DISTINCT company_id) FROM financials").fetchone()[0]
        conn.close()
    except Exception:
        n = "?"

    from datetime import datetime
    msg = f"Auto-sync: {n} companies with financials ({datetime.now().strftime('%Y-%m-%d')})"
    commit = _git("commit", "-m", msg)
    if commit.returncode != 0:
        logger.error("git commit failed: %s", commit.stderr.strip())
        return False
    logger.info("git commit: %s", commit.stdout.strip())

    # 5. Push
    push = _git("push")
    if push.returncode != 0:
        logger.error("git push failed: %s", push.stderr.strip())
        return False
    logger.info("git push: %s", push.stdout.strip() or push.stderr.strip())
    return True


# ------------------------------------------------------------------ #
# Main                                                                 #
# ------------------------------------------------------------------ #
def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Require at least one action
    if not any([args.icra, args.crisil, args.bse, args.nse, args.icra_pdfs, args.crisil_index, args.crisil_financials, args.all]):
        parser.print_help()
        print("\nError: specify at least one of --icra, --crisil, --nse, --bse, --icra-pdfs, --crisil-index, --crisil-financials, or --all")
        sys.exit(1)

    all_results = {}
    total_start = time.time()

    try:
        if args.icra or args.all:
            all_results["ICRA"] = run_icra(limit=args.limit)

        if args.crisil or args.all:
            all_results["CRISIL"] = run_crisil(limit=args.limit, dry_run=args.dry_run)

        if args.nse or args.all:
            all_results["NSE"] = run_nse(limit=args.limit)

        if args.bse:
            all_results["BSE"] = run_bse(limit=args.limit)

        if args.icra_pdfs:
            all_results["ICRA-PDFs"] = run_icra_pdfs(limit=args.limit)

        if args.crisil_index:
            all_results["CRISIL-Index"] = run_crisil_index(force_rebuild=args.force_rebuild)

        if args.crisil_financials:
            all_results["CRISIL-Financials"] = run_crisil_financials(limit=args.limit)

    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(130)
    except Exception as exc:
        logger.exception("Unhandled error: %s", exc)
        sys.exit(1)

    total_elapsed = time.time() - total_start
    logger.info("")
    logger.info("=" * 60)
    logger.info("All scrapers complete in %.1fs", total_elapsed)
    for scraper, result in all_results.items():
        logger.info("  %s: %s", scraper, result)
    logger.info("=" * 60)

    # Print summary to stdout for Streamlit capture
    print("\n--- Summary ---")
    for scraper, result in all_results.items():
        print(f"{scraper}: {result}")
    print(f"Total time: {total_elapsed:.1f}s")

    # Auto-push DB to GitHub if requested
    if args.push:
        logger.info("")
        logger.info("=" * 60)
        logger.info("Pushing updated DB to GitHub")
        logger.info("=" * 60)
        ok = git_push_db()
        print(f"Git push: {'OK' if ok else 'FAILED (see log)'}")


if __name__ == "__main__":
    main()
