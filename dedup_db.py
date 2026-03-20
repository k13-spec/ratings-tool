"""
dedup_db.py — one-shot database deduplication migration for ratings-tool.

Run from project root:
    python dedup_db.py

Steps:
  1. Remove duplicate financials (keep MIN(id) per group)
  2. Remove empty CRISIL rating shells (NULL source_id + NULL grade)
  3. Remove placeholder companies (IDs 726, 23412)
  4. Remove orphan companies (no remaining ratings AND no financials)
  5. Add UNIQUE(company_id, fiscal_year, data_source) to financials via table recreation
  6. WAL checkpoint and print final stats
"""

import shutil
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "ratings.db"
BACKUP_PATH = PROJECT_ROOT / "data" / "ratings.db.bak"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def print_counts(conn, label: str):
    c = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    r = conn.execute("SELECT COUNT(*) FROM ratings").fetchone()[0]
    f = conn.execute("SELECT COUNT(*) FROM financials").fetchone()[0]
    print(f"  [{label}] companies={c:,}  ratings={r:,}  financials={f:,}")


def step1_remove_duplicate_financials(conn):
    print("\nStep 1: Remove duplicate financials")
    before = conn.execute("SELECT COUNT(*) FROM financials").fetchone()[0]
    print(f"  Before: {before:,} rows")

    to_delete = conn.execute("""
        SELECT COUNT(*) FROM financials
        WHERE id NOT IN (
            SELECT MIN(id) FROM financials
            GROUP BY company_id, fiscal_year, data_source
        )
    """).fetchone()[0]
    print(f"  Duplicates to delete: {to_delete:,}")

    conn.execute("""
        DELETE FROM financials
        WHERE id NOT IN (
            SELECT MIN(id) FROM financials
            GROUP BY company_id, fiscal_year, data_source
        )
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM financials").fetchone()[0]
    print(f"  After: {after:,} rows  (deleted {before - after:,})")

    remaining_dupes = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT company_id, fiscal_year, data_source
            FROM financials
            GROUP BY company_id, fiscal_year, data_source
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    assert remaining_dupes == 0, f"Still {remaining_dupes} duplicate groups remaining!"
    print(f"  Verification: 0 duplicate groups remain. ✓")


def step2_remove_empty_crisil_ratings(conn):
    print("\nStep 2: Remove empty CRISIL rating shells")
    before = conn.execute("SELECT COUNT(*) FROM ratings").fetchone()[0]
    print(f"  Before: {before:,} ratings")

    to_delete = conn.execute("""
        SELECT COUNT(*) FROM ratings
        WHERE agency = 'CRISIL'
          AND source_id IS NULL
          AND rating_grade IS NULL
    """).fetchone()[0]
    print(f"  Shells to delete: {to_delete:,}")

    conn.execute("""
        DELETE FROM ratings
        WHERE agency = 'CRISIL'
          AND source_id IS NULL
          AND rating_grade IS NULL
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM ratings").fetchone()[0]
    print(f"  After: {after:,} ratings  (deleted {before - after:,})")
    print(f"  Verification: {after:,} ratings remain. ✓")


def step3_remove_placeholder_companies(conn):
    print("\nStep 3: Remove placeholder companies (IDs 726, 23412)")

    for cid in [726, 23412]:
        row = conn.execute("SELECT name FROM companies WHERE id=?", (cid,)).fetchone()
        if not row:
            print(f"  Company {cid}: not found, skipping")
            continue
        n_ratings = conn.execute(
            "SELECT COUNT(*) FROM ratings WHERE company_id=?", (cid,)
        ).fetchone()[0]
        print(f"  Deleting company {cid} ({row['name']!r}): {n_ratings} ratings")
        conn.execute("DELETE FROM ratings WHERE company_id=?", (cid,))
        conn.execute("DELETE FROM companies WHERE id=?", (cid,))

    conn.commit()
    print("  Done. ✓")


def step4_remove_orphan_companies(conn):
    print("\nStep 4: Remove orphan companies (no ratings, no financials)")

    orphan_count = conn.execute("""
        SELECT COUNT(*) FROM companies
        WHERE id NOT IN (SELECT DISTINCT company_id FROM ratings)
          AND id NOT IN (SELECT DISTINCT company_id FROM financials)
    """).fetchone()[0]
    print(f"  Orphans to delete: {orphan_count:,}")

    notes_deleted = conn.execute("""
        DELETE FROM notes
        WHERE company_id NOT IN (SELECT DISTINCT company_id FROM ratings)
          AND company_id NOT IN (SELECT DISTINCT company_id FROM financials)
    """).rowcount
    if notes_deleted:
        print(f"  Also deleted {notes_deleted} orphaned notes row(s)")

    conn.execute("""
        DELETE FROM companies
        WHERE id NOT IN (SELECT DISTINCT company_id FROM ratings)
          AND id NOT IN (SELECT DISTINCT company_id FROM financials)
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    print(f"  After: {after:,} companies. ✓")


def step5_add_unique_constraint(conn):
    print("\nStep 5: Add UNIQUE(company_id, fiscal_year, data_source) to financials")

    fin_count = conn.execute("SELECT COUNT(*) FROM financials").fetchone()[0]
    print(f"  Financials rows to migrate: {fin_count:,}")

    conn.executescript("""
        CREATE TABLE financials_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL REFERENCES companies(id),
            fiscal_year INTEGER,
            revenue_cr REAL,
            ebitda_cr REAL,
            ebitda_margin_pct REAL,
            pat_cr REAL,
            total_debt_cr REAL,
            cash_cr REAL,
            net_debt_cr REAL,
            capex_cr REAL,
            interest_coverage REAL,
            net_debt_ebitda REAL,
            data_source TEXT,
            extraction_confidence REAL,
            scraped_at TEXT DEFAULT (datetime('now')),
            UNIQUE(company_id, fiscal_year, data_source)
        );

        INSERT INTO financials_new SELECT * FROM financials;

        DROP TABLE financials;

        ALTER TABLE financials_new RENAME TO financials;

        CREATE INDEX IF NOT EXISTS idx_financials_company ON financials(company_id);
    """)
    # executescript issues implicit COMMIT

    new_count = conn.execute("SELECT COUNT(*) FROM financials").fetchone()[0]
    assert new_count == fin_count, f"Row count mismatch after migration: {new_count} != {fin_count}"
    print(f"  Migrated {new_count:,} rows. UNIQUE constraint added. ✓")


def step6_checkpoint_and_stats(conn):
    print("\nStep 6: WAL checkpoint + final stats")
    result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    print(f"  WAL checkpoint: busy={result[0]}, log={result[1]}, checkpointed={result[2]}")

    print("\n=== Final Database State ===")
    for table in ["companies", "ratings", "financials", "notes"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {n:,}")

    agencies = conn.execute(
        "SELECT agency, COUNT(*) as n FROM ratings GROUP BY agency ORDER BY n DESC"
    ).fetchall()
    print("\n  Ratings by agency:")
    for row in agencies:
        print(f"    {row['agency']}: {row['n']:,}")

    fin_sources = conn.execute(
        "SELECT data_source, COUNT(*) as n, COUNT(DISTINCT company_id) as cos "
        "FROM financials GROUP BY data_source ORDER BY n DESC"
    ).fetchall()
    print("\n  Financials by source:")
    for row in fin_sources:
        print(f"    {row['data_source']}: {row['n']:,} rows, {row['cos']:,} companies")


def main():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)

    # Backup first
    print(f"Backing up {DB_PATH.name} → {BACKUP_PATH.name} ...")
    shutil.copy2(str(DB_PATH), str(BACKUP_PATH))
    print(f"Backup written to {BACKUP_PATH}")

    conn = get_conn()
    print("\n=== Initial State ===")
    print_counts(conn, "start")

    step1_remove_duplicate_financials(conn)
    step2_remove_empty_crisil_ratings(conn)
    step3_remove_placeholder_companies(conn)
    step4_remove_orphan_companies(conn)
    step5_add_unique_constraint(conn)
    step6_checkpoint_and_stats(conn)

    conn.close()
    print("\nMigration complete.")
    print(f"Backup preserved at: {BACKUP_PATH}")


if __name__ == "__main__":
    main()
