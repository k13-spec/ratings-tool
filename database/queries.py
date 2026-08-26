"""
Query helpers that return pandas DataFrames for the Streamlit UI.
Each company is ONE row; CRISIL / ICRA / CARE Edge / India Ratings are
separate columns so the dashboard can show all agency ratings side-by-side.
"""

import sqlite3
from typing import List, Optional

import pandas as pd

# UI pseudo-sector: selects companies whose ratings carry no sector value.
UNCLASSIFIED_SECTOR = "(Unclassified)"


def get_filtered_companies(
    conn: sqlite3.Connection,
    min_grade: int = 1,
    max_grade: int = 20,
    agencies: Optional[List[str]] = None,   # kept for filter compat; now means "must have at least one of these"
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
    Return a DataFrame with one row per company.
    Rating columns per agency: <Agency> Rating, <Agency> Outlook,
    <Agency> Grade, <Agency> URL.
    Best grade across agencies used for grade-range filter.
    """

    params: list = []

    # ── Latest rating per (company, agency) ──────────────────────────────
    cte = """
    WITH per_agency AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.company_id, r.agency
                ORDER BY
                    -- ICRA: prefer rows with a LT rating (symbol not starting with '--').
                    -- The combined LT,ST format means '--, [ICRA]A1+' has no LT component.
                    -- Prioritise the row that carries the actual LT rating.
                    CASE
                        WHEN r.agency = 'ICRA'
                         AND r.rating_symbol NOT LIKE '--%'
                         AND r.rating_symbol NOT LIKE 'Withdrawn%'
                         AND r.rating_symbol NOT LIKE '*%'
                        THEN 0 ELSE 1
                    END,
                    -- CARE Edge: prefer LT or LT/ST rows over ST-only rows.
                    CASE
                        WHEN r.agency = 'CARE Edge'
                         AND r.instrument_type IN ('LT', 'LT/ST')
                        THEN 0 ELSE 1
                    END,
                    -- Prefer rows that actually carry a parsed grade: the
                    -- CRISIL suggest feed is a rolling ACTIONS feed and some
                    -- entries have no parseable rating — without this, an
                    -- ungraded newer row shadows the real graded rating.
                    r.rating_grade IS NULL,
                    r.rating_date DESC NULLS LAST,
                    r.id DESC
            ) AS rn
        FROM ratings r
    ),
    latest AS (
        SELECT * FROM per_agency WHERE rn = 1
    ),
    -- Pivot: one row per company with all 4 agencies as columns
    agency_pivot AS (
        SELECT
            company_id,
            -- CRISIL
            MAX(CASE WHEN agency = 'CRISIL' THEN rating_symbol  END) AS crisil_rating,
            MAX(CASE WHEN agency = 'CRISIL' THEN outlook        END) AS crisil_outlook,
            MAX(CASE WHEN agency = 'CRISIL' THEN rating_grade   END) AS crisil_grade,
            MAX(CASE WHEN agency = 'CRISIL' THEN rationale_url  END) AS crisil_url,
            -- ICRA
            MAX(CASE WHEN agency = 'ICRA'   THEN rating_symbol  END) AS icra_rating,
            MAX(CASE WHEN agency = 'ICRA'   THEN outlook        END) AS icra_outlook,
            MAX(CASE WHEN agency = 'ICRA'   THEN rating_grade   END) AS icra_grade,
            MAX(CASE WHEN agency = 'ICRA'   THEN rationale_url  END) AS icra_url,
            -- CARE Edge
            MAX(CASE WHEN agency = 'CARE Edge' THEN rating_symbol END) AS care_rating,
            MAX(CASE WHEN agency = 'CARE Edge' THEN outlook       END) AS care_outlook,
            MAX(CASE WHEN agency = 'CARE Edge' THEN rating_grade  END) AS care_grade,
            MAX(CASE WHEN agency = 'CARE Edge' THEN rationale_url END) AS care_url,
            -- India Ratings
            MAX(CASE WHEN agency = 'India Ratings' THEN rating_symbol END) AS ind_rating,
            MAX(CASE WHEN agency = 'India Ratings' THEN outlook       END) AS ind_outlook,
            MAX(CASE WHEN agency = 'India Ratings' THEN rating_grade  END) AS ind_grade,
            MAX(CASE WHEN agency = 'India Ratings' THEN rationale_url END) AS ind_url,
            -- Cross-agency aggregates for filtering / sorting
            MIN(rating_grade)  AS best_grade,
            MAX(rating_date)   AS latest_date,
            MAX(sector)        AS sector
        FROM latest
        GROUP BY company_id
    ),
    -- Most recent financials per company
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
        c.id                            AS company_id,
        c.name                          AS "Company Name",
        ap.sector                       AS "Sector",
        c.is_listed                     AS "Listed",
        -- Agency rating columns
        ap.crisil_rating                AS "CRISIL Rating",
        ap.crisil_outlook               AS "CRISIL Outlook",
        ap.crisil_grade                 AS "CRISIL Grade",
        ap.crisil_url                   AS "CRISIL URL",
        ap.icra_rating                  AS "ICRA Rating",
        ap.icra_outlook                 AS "ICRA Outlook",
        ap.icra_grade                   AS "ICRA Grade",
        ap.icra_url                     AS "ICRA URL",
        ap.care_rating                  AS "Care Edge Rating",
        ap.care_outlook                 AS "Care Edge Outlook",
        ap.care_grade                   AS "Care Edge Grade",
        ap.care_url                     AS "Care Edge URL",
        ap.ind_rating                   AS "India Ratings Rating",
        ap.ind_outlook                  AS "India Ratings Outlook",
        ap.ind_grade                    AS "India Ratings Grade",
        ap.ind_url                      AS "India Ratings URL",
        -- Best grade for sorting / filtering
        ap.best_grade                   AS "Grade",
        ap.latest_date                  AS "Rating Date",
        -- Financials
        lf.revenue_cr                   AS "Revenue (Cr)",
        lf.ebitda_cr                    AS "EBITDA (Cr)",
        lf.ebitda_margin_pct            AS "EBITDA Margin %",
        lf.total_debt_cr                AS "Total Debt (Cr)",
        lf.net_debt_cr                  AS "Net Debt (Cr)",
        lf.net_debt_ebitda              AS "Net Debt/EBITDA",
        lf.nd_ebitda_text               AS "ND/EBITDA (Rationale)",
        lf.nd_ebitda_source             AS "ND/EBITDA Source",
        c.bse_code                      AS "BSE Code",
        c.isin                          AS "ISIN"
    """

    from_clause = """
    FROM companies c
    JOIN agency_pivot ap ON ap.company_id = c.id
    LEFT JOIN latest_financials lf ON lf.company_id = c.id
    """

    where_conditions: list = []

    # Grade range (best grade across agencies)
    where_conditions.append("ap.best_grade >= ?")
    params.append(min_grade)
    where_conditions.append("ap.best_grade <= ?")
    params.append(max_grade)

    # Agency filter: company must have a rating from at least one of these agencies
    if agencies:
        sub_conditions = []
        for ag in agencies:
            if ag == "CRISIL":
                sub_conditions.append("ap.crisil_rating IS NOT NULL")
            elif ag == "ICRA":
                sub_conditions.append("ap.icra_rating IS NOT NULL")
            elif ag == "CARE Edge":
                sub_conditions.append("ap.care_rating IS NOT NULL")
            elif ag == "India Ratings":
                sub_conditions.append("ap.ind_rating IS NOT NULL")
        if sub_conditions:
            where_conditions.append("(" + " OR ".join(sub_conditions) + ")")

    # Outlook filter (any agency matches)
    if outlooks:
        ph = ", ".join("?" for _ in outlooks)
        where_conditions.append(
            f"(ap.crisil_outlook IN ({ph}) OR ap.icra_outlook IN ({ph}) "
            f"OR ap.care_outlook IN ({ph}) OR ap.ind_outlook IN ({ph}))"
        )
        params.extend(outlooks * 4)

    # Sector filter. "(Unclassified)" is a UI pseudo-sector meaning
    # "companies with no sector at all" — a large share of the DB (the CRISIL
    # suggest feed and several other sources carry no sector), so a plain
    # IN (...) filter would silently hide them (e.g. Britannia Industries).
    if sectors:
        include_null = UNCLASSIFIED_SECTOR in sectors
        named = [s for s in sectors if s != UNCLASSIFIED_SECTOR]
        sector_conds = []
        if named:
            ph = ", ".join("?" for _ in named)
            sector_conds.append(f"ap.sector IN ({ph})")
            params.extend(named)
        if include_null:
            sector_conds.append("ap.sector IS NULL OR ap.sector = ''")
        if sector_conds:
            where_conditions.append("(" + " OR ".join(sector_conds) + ")")

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

    order_clause = "ORDER BY ap.best_grade ASC NULLS LAST, c.name ASC"

    full_query = cte + select_clause + from_clause + where_clause + " " + order_clause

    df = pd.read_sql_query(full_query, conn, params=params)
    return df


def get_all_sectors(conn: sqlite3.Connection) -> List[str]:
    """Return sorted list of distinct sectors present in the ratings table."""
    rows = conn.execute(
        "SELECT DISTINCT sector FROM ratings WHERE sector IS NOT NULL AND sector != '' ORDER BY sector"
    ).fetchall()
    return [r[0] for r in rows]


def get_all_agencies(conn: sqlite3.Connection) -> List[str]:
    """Return sorted list of distinct agencies present in the ratings table."""
    rows = conn.execute(
        "SELECT DISTINCT agency FROM ratings WHERE agency IS NOT NULL ORDER BY agency"
    ).fetchall()
    return [r[0] for r in rows]


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

    return {
        "total_companies": total_companies,
        "rated_companies": rated_companies,
        "with_financials": with_financials,
        "total_ratings": total_ratings,
        "last_scraped": last_scraped,
    }
