#!/usr/bin/env python3
"""
Post-scrape maintenance + export for ratings-tool. Idempotent — safe to run
after every scraper pass (the fortnightly ratings-refresh workflow does).

Steps:
  0. Backfill content-hash source_id on legacy CRISIL / India Ratings rows
     (same formulas as scrapers/crisil.py and scrapers/india_ratings.py),
     then remove exact-duplicate rating rows sharing (company, agency,
     source_id) — keeps the newest.
  1. Realign rating_grade to the symbol map wherever they disagree
     (recurring CRISIL D/C artifact, agencies lagging symbol updates).
  2. NULL the outlook='Crisil' regex artifact.
  3. Reclassify renewable-energy power producers to sector 'Renewable Energy'
     (fresh scraper rows come in as Power/Energy again — this re-applies the
     rule each run; financial-sector lenders are skipped).
  4. Export data/ratings_current.csv — latest rating per (company, agency),
     matching the app's own "latest" semantics (rating_date DESC, id DESC,
     with the ICRA LT and CARE Edge LT preferences). NOTE: the previous
     export preferred the BEST-EVER grade per company/agency, which
     under-reported downgrades once re-scraping began.

Safety: refuses to write the CSV if the export shrinks below MIN_ROWS or by
more than 20% vs the existing file.

Usage (from the ratings-tool repo root):
    python db_maintenance.py            # apply + export
    python db_maintenance.py --dry-run  # report only, write nothing
"""
import csv
import hashlib
import os
import re
import sqlite3
import sys

DRY = "--dry-run" in sys.argv
DB = os.path.join("data", "ratings.db")
CSV_OUT = os.path.join("data", "ratings_current.csv")
MIN_ROWS = 20000

GRADE_MAP = {"AAA": 1, "AA+": 2, "AA": 3, "AA-": 4, "A+": 5, "A": 6, "A-": 7,
             "BBB+": 8, "BBB": 9, "BBB-": 10, "BB+": 11, "BB": 12, "BB-": 13,
             "B+": 14, "B": 15, "B-": 16, "C+": 17, "C": 18, "C-": 19, "D": 20}

RENEW_RE = re.compile(
    r"renewab|solar|\bwind\b|windpower|wind power|windfarm|green energy|"
    r"green power|greenko|clean ?energy|cleantech|clean ?max|hydro ?power|"
    r"hydroelectric|photovolta|\brenew\b|suzlon|inox wind|avaada|"
    r"azure power|juniper green|serentica|ayana renewable|amp energy|"
    r"fourth partner|o2 power|radiance renew|virescent|adani green|"
    r"tata power renewable|continuum green|vena energy|sael\b",
    re.I,
)
FINANCIAL_RE = re.compile(
    r"financ|bank|nbfc|insur|invest|capital market|stockbrok|"
    r"asset management|fintech|housing finance", re.I)

# Latest rating per (company, agency) — same ordering as database/queries.py
EXPORT_QUERY = """
WITH ranked AS (
    SELECT c.name AS company_name, r.agency, r.rating_symbol AS rating,
           r.rating_grade AS grade, r.outlook, r.rating_date,
           ROW_NUMBER() OVER (
               PARTITION BY r.company_id, r.agency
               ORDER BY
                 CASE WHEN r.agency='ICRA' AND (r.rating_symbol LIKE '--%'
                       OR r.rating_symbol LIKE 'Withdrawn%'
                       OR r.rating_symbol LIKE '*%') THEN 1 ELSE 0 END,
                 CASE WHEN r.agency='CARE Edge' AND r.instrument_type NOT IN ('LT','LT/ST') THEN 1 ELSE 0 END,
                 r.rating_grade IS NULL,
                 r.rating_date IS NULL, r.rating_date DESC, r.id DESC
           ) AS rn
    FROM ratings r JOIN companies c ON c.id = r.company_id
)
SELECT company_name, agency, rating, grade, outlook, rating_date
FROM ranked WHERE rn = 1 ORDER BY company_name, agency
"""


def step0_backfill_source_ids(cur):
    print("=" * 62)
    print("STEP 0 — backfill content-hash source_id (CRISIL / India Ratings)")
    print("=" * 62)
    # CRISIL rows without a source_id
    rows = cur.execute(
        "SELECT id, rating_symbol, outlook, rating_date FROM ratings "
        "WHERE agency='CRISIL' AND (source_id IS NULL OR source_id='')").fetchall()
    cr = [( "h" + hashlib.sha1(
            f"{r[1] or ''}|{r[2] or ''}|{r[3] or ''}".encode()).hexdigest()[:12], r[0])
          for r in rows]
    print(f"  CRISIL rows to backfill: {len(cr)}")
    # India Ratings rows with the legacy positional id ("123_0")
    rows = cur.execute(
        "SELECT id, instrument_name, rating_symbol, outlook, rationale_url, source_id "
        "FROM ratings WHERE agency='India Ratings' "
        "AND source_id IS NOT NULL AND source_id GLOB '*_[0-9]*' "
        "AND source_id NOT GLOB '*_h*'").fetchall()
    ir = []
    for rid, instr, sym, outlook, url, sid in rows:
        m = re.search(r"issuerID=(\d+)", url or "")
        issuer_id = m.group(1) if m else (sid or "").split("_")[0]
        h = hashlib.sha1(f"{instr or ''}|{sym or ''}|{outlook or ''}".encode()).hexdigest()[:10]
        ir.append((f"{issuer_id}_h{h}", rid))
    print(f"  India Ratings rows to backfill: {len(ir)}")
    if not DRY:
        cur.executemany("UPDATE ratings SET source_id=? WHERE id=?", cr + ir)

    # CRISIL noise rows: the suggest feed is a rolling ACTIONS feed and some
    # entries carry no parseable rating at all — such rows hold no information
    # (the scraper no longer inserts them; this cleans up past runs).
    noise = cur.execute(
        "SELECT COUNT(*) FROM ratings WHERE agency='CRISIL' "
        "AND (rating_symbol IS NULL OR rating_symbol='')").fetchone()[0]
    print(f"  CRISIL empty-symbol noise rows to delete: {noise}")
    if not DRY and noise:
        cur.execute("DELETE FROM ratings WHERE agency='CRISIL' "
                    "AND (rating_symbol IS NULL OR rating_symbol='')")
    # Orphan companies (created for noise records): no ratings, no financials,
    # no notes — safe to remove.
    orphans = cur.execute(
        "SELECT COUNT(*) FROM companies c WHERE "
        "NOT EXISTS (SELECT 1 FROM ratings r WHERE r.company_id=c.id) "
        "AND NOT EXISTS (SELECT 1 FROM financials f WHERE f.company_id=c.id) "
        "AND NOT EXISTS (SELECT 1 FROM notes n WHERE n.company_id=c.id)").fetchone()[0]
    print(f"  orphan companies to delete: {orphans}")
    if not DRY and orphans:
        cur.execute(
            "DELETE FROM companies WHERE id IN (SELECT c.id FROM companies c WHERE "
            "NOT EXISTS (SELECT 1 FROM ratings r WHERE r.company_id=c.id) "
            "AND NOT EXISTS (SELECT 1 FROM financials f WHERE f.company_id=c.id) "
            "AND NOT EXISTS (SELECT 1 FROM notes n WHERE n.company_id=c.id))")

    # Dedupe: identical (company_id, agency, source_id) — keep newest id
    dup = cur.execute(
        "SELECT COUNT(*) FROM ratings r WHERE r.source_id IS NOT NULL AND EXISTS ("
        "  SELECT 1 FROM ratings r2 WHERE r2.company_id=r.company_id AND r2.agency=r.agency "
        "  AND r2.source_id=r.source_id AND r2.id > r.id)").fetchone()[0]
    print(f"  exact-duplicate rows to prune: {dup}")
    if not DRY and dup:
        cur.execute(
            "DELETE FROM ratings WHERE source_id IS NOT NULL AND id IN ("
            "  SELECT r.id FROM ratings r JOIN ratings r2 "
            "  ON r2.company_id=r.company_id AND r2.agency=r.agency "
            "  AND r2.source_id=r.source_id AND r2.id > r.id)")


def step1_realign_grades(cur):
    print("=" * 62)
    print("STEP 1 — realign rating_grade to symbol")
    print("=" * 62)
    strip_prefix = re.compile(
        r"^(?:\[ICRA\]|ICRA\s+|CRISIL\s+|Crisil\s+|CARE\s+EDGE\s+|CARE\s+|IND\s+)+", re.I)
    rows = cur.execute(
        "SELECT id, rating_symbol, rating_grade FROM ratings "
        "WHERE rating_symbol IS NOT NULL AND rating_grade IS NOT NULL").fetchall()
    fixes = []
    for rid, sym, g in rows:
        base = sym.split(",")[0]
        base = re.sub(r"\(.*?\)", "", base)
        base = strip_prefix.sub("", base.strip()).strip().upper()
        base = re.sub(r"\s+ISSUER NOT COOPERATING.*$", "", base).strip()
        exp = GRADE_MAP.get(base)
        if exp is not None and exp != g:
            fixes.append((exp, rid))
    print(f"  grade fixes: {len(fixes)}")
    if not DRY and fixes:
        cur.executemany("UPDATE ratings SET rating_grade=? WHERE id=?", fixes)


def step2_outlook_artifact(cur):
    print("=" * 62)
    print("STEP 2 — NULL the outlook='Crisil' regex artifact")
    print("=" * 62)
    n = cur.execute("SELECT COUNT(*) FROM ratings "
                    "WHERE agency='CRISIL' AND LOWER(outlook)='crisil'").fetchone()[0]
    print(f"  rows to NULL: {n}")
    if not DRY and n:
        cur.execute("UPDATE ratings SET outlook=NULL "
                    "WHERE agency='CRISIL' AND LOWER(outlook)='crisil'")


def step3_renewables(cur):
    print("=" * 62)
    print("STEP 3 — Renewable Energy sector reclassification")
    print("=" * 62)
    rows = cur.execute(
        "SELECT DISTINCT c.id, c.name, COALESCE(r.sector,'') "
        "FROM companies c JOIN ratings r ON r.company_id = c.id").fetchall()
    by_company = {}
    for cid, name, sector in rows:
        by_company.setdefault((cid, name), set()).add(sector)
    targets = []
    for (cid, name), sectors in by_company.items():
        if not name or not RENEW_RE.search(name):
            continue
        if any(FINANCIAL_RE.search(s) for s in sectors if s):
            continue
        if sectors == {"Renewable Energy"}:
            continue
        targets.append(cid)
    print(f"  companies to (re)classify: {len(targets)}")
    if not DRY and targets:
        cur.executemany(
            "UPDATE ratings SET sector='Renewable Energy' WHERE company_id=?",
            [(c,) for c in targets])


def step4_export(cur):
    print("=" * 62)
    print("STEP 4 — export data/ratings_current.csv (latest per company/agency)")
    print("=" * 62)
    out = cur.execute(EXPORT_QUERY).fetchall()
    print(f"  export rows: {len(out)}")
    if len(out) < MIN_ROWS:
        print(f"  ABORT: fewer than {MIN_ROWS} rows — leaving existing CSV untouched.",
              file=sys.stderr)
        return False
    if os.path.exists(CSV_OUT):
        with open(CSV_OUT, encoding="utf-8") as f:
            existing = max(sum(1 for _ in f) - 1, 0)
        if existing and len(out) < existing * 0.8:
            print(f"  ABORT: export shrank {existing} -> {len(out)} (>20%) — "
                  "leaving existing CSV untouched.", file=sys.stderr)
            return False
    if DRY:
        print("  (dry run — CSV not written)")
        return True
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_name", "agency", "rating", "grade", "outlook", "rating_date"])
        w.writerows(out)
    print(f"  wrote {CSV_OUT}")
    return True


def main() -> int:
    if not os.path.exists(DB):
        sys.exit(f"ERROR: {DB} not found — run from the ratings-tool repo root.")
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    step0_backfill_source_ids(cur)
    step1_realign_grades(cur)
    step2_outlook_artifact(cur)
    step3_renewables(cur)
    if not DRY:
        conn.commit()
    ok = step4_export(cur)
    if not DRY:
        cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    print("\nDONE." if ok else "\nDONE (export skipped — see ABORT above).")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
