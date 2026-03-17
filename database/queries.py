"""
Query helpers that return pandas DataFrames for the Streamlit UI.
"""

import sqlite3
from typing import List, Optional

import pandas as pd


def get_filtered_companies(
    conn: sqlite3.Connection,
    min_grade: int = 1,
    max_grade: int = 20,
    agencies: Optional[List[str]] = None,
    outlooks: Optional[List[str]] = None,
    sectors: Optional[List[str]] = None,
    listed_only: bool = False,
    unlisted_only: bool = False,
    min_revenue_cr: Optional[float] = None,
    max_revenue_cr: Optional[float] = None,
    min_ebitda_cr: Optional[float] = None,
    min_ebitda_margin_pct: Optional[float] = None,
    max_net_debt_ebitda: Optional[float] = None,
    min_total_debt_cr: Optional[float] = None,
) -> pd.DataFrame:
    """
    Return a DataFrame of companies matching all provided filters.

    Uses the most recent rating per (company, agency) and the most recent
    financials row per company.
    """

    params: list = []

    # ------------------------------------------------------------------ #
    # Best (most recent) rating per company per agency                     #
    # ------------------------------------------------------------------ #
    best_rating_cte = """
    WITH best_rating AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.company_id, r.agency
                ORDER BY r.rating_date DESC NULLS LAST, r.id DESC
            ) AS rn
        FROM ratings r
    ),
    latest_rating AS (
        SELECT * FROM best_rating WHERE rn = 1
    ),
    -- pick a single agency row per company: lowest grade (best rating) first
    top_rating AS (
        SELECT
            lr.*,
            ROW_NUMBER() OVER (
                PARTITION BY lr.company_id
                ORDER BY lr.rating_grade ASC NULLS LAST, lr.rating_date DESC NULLS LAST
            ) AS top_rn
        FROM latest_rating lr
    ),
    company_rating AS (
        SELECT * FROM top_rating WHERE top_rn = 1
    ),
    """

    # ------------------------------------------------------------------ #
    # Most recent financials per company                                   #
    # ------------------------------------------------------------------ #
    financials_cte = """
    recent_financials AS (
        SELECT
            f.*,
            ROW_NUMBER() OVER (
                PARTITION BY f.company_id
                ORDER BY f.fiscal_year DESC NULLS LAST, f.id DESC
            ) AS fn_rn
        FROM financials f
    ),
    latest_financials AS (
        SELECT * FROM recent_financials WHERE fn_rn = 1
    )
    """

    select_clause = """
    SELECT
        c.id                        AS company_id,
        c.name                      AS "Company Name",
        cr.agency                   AS "Agency",
        cr.rating_symbol            AS "Rating",
        cr.rating_grade             AS "Grade",
        cr.outlook                  AS "Outlook",
        cr.sector                   AS "Sector",
        c.is_listed                 AS "Listed",
        lf.revenue_cr               AS "Revenue (Cr)",
        lf.ebitda_cr                AS "EBITDA (Cr)",
        lf.ebitda_margin_pct        AS "EBITDA Margin %",
        lf.total_debt_cr            AS "Total Debt (Cr)",
        lf.net_debt_cr              AS "Net Debt (Cr)",
        lf.net_debt_ebitda          AS "Net Debt/EBITDA",
        cr.rating_date              AS "Rating Date",
        cr.rationale_url            AS "Rationale URL",
        c.bse_code                  AS "BSE Code",
        c.isin                      AS "ISIN"
    """

    from_clause = """
    FROM companies c
    JOIN company_rating cr ON cr.company_id = c.id
    LEFT JOIN latest_financials lf ON lf.company_id = c.id
    """

    where_conditions = []

    # Rating grade range
    where_conditions.append("cr.rating_grade >= ?")
    params.append(min_grade)
    where_conditions.append("cr.rating_grade <= ?")
    params.append(max_grade)

    # Agency filter
    if agencies:
        placeholders = ", ".join("?" for _ in agencies)
        where_conditions.append(f"cr.agency IN ({placeholders})")
        params.extend(agencies)

    # Outlook filter
    if outlooks:
        placeholders = ", ".join("?" for _ in outlooks)
        where_conditions.append(f"cr.outlook IN ({placeholders})")
        params.extend(outlooks)

    # Sector filter
    if sectors:
        placeholders = ", ".join("?" for _ in sectors)
        where_conditions.append(f"cr.sector IN ({placeholders})")
        params.extend(sectors)

    # Listed / unlisted
    if listed_only:
        where_conditions.append("c.is_listed = 1")
    elif unlisted_only:
        where_conditions.append("c.is_listed = 0")

    # Financial filters
    if min_revenue_cr is not None:
        where_conditions.append("lf.revenue_cr >= ?")
        params.append(min_revenue_cr)

    if max_revenue_cr is not None:
        where_conditions.append("lf.revenue_cr <= ?")
        params.append(max_revenue_cr)

    if min_ebitda_cr is not None:
        where_conditions.append("lf.ebitda_cr >= ?")
        params.append(min_ebitda_cr)

    if min_ebitda_margin_pct is not None and min_ebitda_margin_pct > 0:
        where_conditions.append("lf.ebitda_margin_pct >= ?")
        params.append(min_ebitda_margin_pct)

    if max_net_debt_ebitda is not None and max_net_debt_ebitda < 20.0:
        where_conditions.append("lf.net_debt_ebitda <= ?")
        params.append(max_net_debt_ebitda)

    if min_total_debt_cr is not None and min_total_debt_cr > 0:
        where_conditions.append("lf.total_debt_cr >= ?")
        params.append(min_total_debt_cr)

    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)

    order_clause = "ORDER BY cr.rating_grade ASC, c.name ASC"

    full_query = (
        best_rating_cte
        + financials_cte
        + select_clause
        + from_clause
        + where_clause
        + " "
        + order_clause
    )

    df = pd.read_sql_query(full_query, conn, params=params)
    return df


def get_all_sectors(conn: sqlite3.Connection) -> List[str]:
    """Return sorted list of distinct sectors present in the ratings table."""
    rows = conn.execute(
        "SELECT DISTINCT sector FROM ratings WHERE sector IS NOT NULL AND sector != '' ORDER BY sector"
    ).fetchall()
    return [r["sector"] for r in rows]


def get_all_agencies(conn: sqlite3.Connection) -> List[str]:
    """Return sorted list of distinct agencies present in the ratings table."""
    rows = conn.execute(
        "SELECT DISTINCT agency FROM ratings WHERE agency IS NOT NULL ORDER BY agency"
    ).fetchall()
    return [r["agency"] for r in rows]


def get_db_stats(conn: sqlite3.Connection) -> dict:
    """Return summary statistics about the database contents."""
    total_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    rated_companies = conn.execute(
        "SELECT COUNT(DISTINCT company_id) FROM ratings"
    ).fetchone()[0]
    with_financials = conn.execute(
        "SELECT COUNT(DISTINCT company_id) FROM financials"
    ).fetchone()[0]
    last_scraped_row = conn.execute(
        "SELECT MAX(scraped_at) FROM ratings"
    ).fetchone()
    last_scraped = last_scraped_row[0] if last_scraped_row else None

    total_ratings = conn.execute("SELECT COUNT(*) FROM ratings").fetchone()[0]
    with_capex_plans = conn.execute(
        "SELECT COUNT(DISTINCT company_id) FROM capex_plans"
    ).fetchone()[0]

    return {
        "total_companies": total_companies,
        "rated_companies": rated_companies,
        "with_financials": with_financials,
        "with_capex_plans": with_capex_plans,
        "total_ratings": total_ratings,
        "last_scraped": last_scraped,
    }
