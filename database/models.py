"""
SQLite schema, connection, and upsert helpers for ratings-tool.
DB file lives at data/ratings.db relative to this project root.
"""

import os
import sqlite3
from pathlib import Path

# Resolve project root (two levels up from this file: database/ -> project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "ratings.db"

# Ensure data directory exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_connection(db_path: str = None) -> sqlite3.Connection:
    """Return a sqlite3 connection with row_factory set to sqlite3.Row."""
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode for better concurrent read performance
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


DDL = """
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    name_normalized TEXT,
    cin TEXT,
    isin TEXT,
    bse_code TEXT,
    nse_symbol TEXT,
    is_listed INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ratings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    agency TEXT NOT NULL,
    rating_symbol TEXT,
    rating_grade INTEGER,
    outlook TEXT,
    instrument_type TEXT,
    instrument_name TEXT,
    rated_amount_cr REAL,
    rating_date TEXT,
    sector TEXT,
    sub_sector TEXT,
    rationale_url TEXT,
    rationale_pdf_path TEXT,
    source_id TEXT,
    scraped_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS financials (
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

CREATE TABLE IF NOT EXISTS capex_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    amount_cr REAL,
    timeframe_years INTEGER,
    description TEXT,
    source_text TEXT,
    fiscal_year_extracted INTEGER
);

CREATE TABLE IF NOT EXISTS notes (
    company_id INTEGER PRIMARY KEY REFERENCES companies(id),
    note TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ratings_company ON ratings(company_id);
CREATE INDEX IF NOT EXISTS idx_ratings_grade ON ratings(rating_grade);
CREATE INDEX IF NOT EXISTS idx_financials_company ON financials(company_id);
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name_normalized);
"""


def init_db(db_path: str = None) -> None:
    """Create all tables and indexes if they don't already exist."""
    conn = get_connection(db_path)
    try:
        conn.executescript(DDL)
        conn.commit()
    finally:
        conn.close()


def _normalize_name(name: str) -> str:
    """Lowercase, strip, and normalize common suffixes for dedup matching."""
    n = name.lower().strip()
    n = n.replace("limited", "ltd")
    n = n.replace("private", "pvt")
    n = n.replace("  ", " ")
    return n


def upsert_company(conn: sqlite3.Connection, name: str, **kwargs) -> int:
    """
    Insert a company if it doesn't exist (matched on name_normalized),
    otherwise update mutable fields and return the existing id.

    Keyword args correspond to optional company columns:
    cin, isin, bse_code, nse_symbol, is_listed
    """
    name_normalized = _normalize_name(name)

    row = conn.execute(
        "SELECT id FROM companies WHERE name_normalized = ?", (name_normalized,)
    ).fetchone()

    if row:
        company_id = row["id"]
        # Update any provided kwargs that are not None
        updatable = {k: v for k, v in kwargs.items() if v is not None and k in
                     ("cin", "isin", "bse_code", "nse_symbol", "is_listed")}
        if updatable:
            set_clause = ", ".join(f"{k} = ?" for k in updatable)
            set_clause += ", updated_at = datetime('now')"
            values = list(updatable.values()) + [company_id]
            conn.execute(
                f"UPDATE companies SET {set_clause} WHERE id = ?", values
            )
            conn.commit()
        return company_id

    # Insert new company
    columns = ["name", "name_normalized"]
    values = [name, name_normalized]
    allowed = ("cin", "isin", "bse_code", "nse_symbol", "is_listed")
    for col in allowed:
        if col in kwargs and kwargs[col] is not None:
            columns.append(col)
            values.append(kwargs[col])

    placeholders = ", ".join("?" for _ in values)
    col_str = ", ".join(columns)
    cursor = conn.execute(
        f"INSERT INTO companies ({col_str}) VALUES ({placeholders})", values
    )
    conn.commit()
    return cursor.lastrowid


def insert_rating(conn: sqlite3.Connection, company_id: int, **kwargs) -> int:
    """
    Insert a rating row. Expected kwargs: agency, rating_symbol, rating_grade,
    outlook, instrument_type, instrument_name, rated_amount_cr, rating_date,
    sector, sub_sector, rationale_url, rationale_pdf_path, source_id.

    Skips insert if a rating with the same (company_id, agency, source_id) already exists.
    """
    agency = kwargs.get("agency", "")
    source_id = kwargs.get("source_id")

    if source_id:
        existing = conn.execute(
            "SELECT id FROM ratings WHERE company_id=? AND agency=? AND source_id=?",
            (company_id, agency, str(source_id)),
        ).fetchone()
        if existing:
            return existing["id"]

    allowed_cols = [
        "agency", "rating_symbol", "rating_grade", "outlook",
        "instrument_type", "instrument_name", "rated_amount_cr",
        "rating_date", "sector", "sub_sector", "rationale_url",
        "rationale_pdf_path", "source_id",
    ]
    columns = ["company_id"]
    values = [company_id]
    for col in allowed_cols:
        if col in kwargs and kwargs[col] is not None:
            columns.append(col)
            val = kwargs[col]
            # Ensure source_id stored as string
            if col == "source_id":
                val = str(val)
            values.append(val)

    placeholders = ", ".join("?" for _ in values)
    col_str = ", ".join(columns)
    cursor = conn.execute(
        f"INSERT INTO ratings ({col_str}) VALUES ({placeholders})", values
    )
    conn.commit()
    return cursor.lastrowid


def insert_financial(conn: sqlite3.Connection, company_id: int, **kwargs) -> int:
    """
    Insert a financials row. If (company_id, fiscal_year, data_source) already
    exists, keeps whichever row has the higher extraction_confidence.

    Uses INSERT OR IGNORE + the UNIQUE constraint to prevent race-condition
    duplicates from ThreadPoolExecutor workers.
    """
    fiscal_year = kwargs.get("fiscal_year")
    data_source = kwargs.get("data_source")
    new_conf    = kwargs.get("extraction_confidence", 0) or 0

    allowed_cols = [
        "fiscal_year", "revenue_cr", "ebitda_cr", "ebitda_margin_pct",
        "pat_cr", "total_debt_cr", "cash_cr", "net_debt_cr", "capex_cr",
        "interest_coverage", "net_debt_ebitda", "data_source",
        "extraction_confidence",
    ]
    columns = ["company_id"]
    values  = [company_id]
    for col in allowed_cols:
        if col in kwargs and kwargs[col] is not None:
            columns.append(col)
            values.append(kwargs[col])

    placeholders = ", ".join("?" for _ in values)
    col_str      = ", ".join(columns)

    if fiscal_year and data_source:
        # Atomic insert guarded by UNIQUE constraint — eliminates SELECT→INSERT race
        cursor = conn.execute(
            f"INSERT OR IGNORE INTO financials ({col_str}) VALUES ({placeholders})",
            values,
        )
        conn.commit()
        if cursor.rowcount == 1:
            return cursor.lastrowid  # new row inserted successfully

        # Row already existed — upgrade only if new confidence is strictly higher
        existing = conn.execute(
            "SELECT id, extraction_confidence FROM financials "
            "WHERE company_id=? AND fiscal_year=? AND data_source=?",
            (company_id, fiscal_year, data_source),
        ).fetchone()
        if existing:
            old_conf = existing["extraction_confidence"] or 0
            if new_conf <= old_conf:
                return existing["id"]
            # Higher confidence: replace old row
            conn.execute("DELETE FROM financials WHERE id=?", (existing["id"],))
            # Fall through to plain INSERT below

    cursor = conn.execute(
        f"INSERT INTO financials ({col_str}) VALUES ({placeholders})", values
    )
    conn.commit()
    return cursor.lastrowid


def insert_capex_plan(conn: sqlite3.Connection, company_id: int, **kwargs) -> int:
    """Insert a capex_plans row."""
    allowed_cols = [
        "amount_cr", "timeframe_years", "description",
        "source_text", "fiscal_year_extracted",
    ]
    columns = ["company_id"]
    values = [company_id]
    for col in allowed_cols:
        if col in kwargs and kwargs[col] is not None:
            columns.append(col)
            values.append(kwargs[col])

    placeholders = ", ".join("?" for _ in values)
    col_str = ", ".join(columns)
    cursor = conn.execute(
        f"INSERT INTO capex_plans ({col_str}) VALUES ({placeholders})", values
    )
    conn.commit()
    return cursor.lastrowid
