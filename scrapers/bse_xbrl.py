"""
BSE XBRL / financial results scraper.

Fetches:
1. List of all active equity scrips from BSE
2. Annual financial results for companies already in the DB
3. Matches BSE scrips to our companies by ISIN (primary) or name (fallback)
4. Normalises all monetary values to Crores

Usage:
    python run_scraper.py --bse
    python run_scraper.py --bse --limit 50
"""

import logging
import re
import time
from typing import Optional

import requests

from database.models import get_connection, init_db, insert_financial, upsert_company

logger = logging.getLogger(__name__)

BSE_BASE = "https://api.bseindia.com/BseIndiaAPI/api"
SCRIP_LIST_URL = f"{BSE_BASE}/ListofScripData/w"
FINANCIALS_URL = f"{BSE_BASE}/FinancialResultsNew/w"
XBRL_URL = f"{BSE_BASE}/Xbrlnotice/w"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
    "Accept": "application/json, text/plain, */*",
}


def _normalize_name(name: str) -> str:
    """Same normalisation used in models.py for dedup matching."""
    n = str(name).lower().strip()
    n = n.replace("limited", "ltd")
    n = n.replace("private", "pvt")
    n = re.sub(r"\s+", " ", n)
    return n


def _get(session: requests.Session, url: str, params: dict = None, retries: int = 3) -> dict:
    """GET with retries and error handling."""
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.JSONDecodeError:
            logger.warning("Non-JSON response from %s params=%s", url, params)
            return {}
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait = 2 ** attempt
                logger.warning("Rate limited, waiting %ds", wait)
                time.sleep(wait)
            else:
                logger.error("HTTP error %s for %s: %s", e.response.status_code, url, e)
                return {}
        except Exception as exc:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                logger.error("Failed after %d attempts for %s: %s", retries, url, exc)
    return {}


def _fetch_scrip_list(session: requests.Session) -> list:
    """Fetch all active equity scrips from BSE."""
    logger.info("Fetching BSE scrip list...")
    params = {"status": "Active", "scriptype": "EQ"}
    data = _get(session, SCRIP_LIST_URL, params)

    scrips = []
    # BSE returns various JSON structures
    if isinstance(data, list):
        scrips = data
    elif isinstance(data, dict):
        for key in ("Table", "table", "data", "Data", "Scrips", "scrips", "result"):
            if key in data and isinstance(data[key], list):
                scrips = data[key]
                break

    logger.info("Fetched %d scrips from BSE", len(scrips))
    return scrips


def _build_scrip_maps(scrips: list) -> tuple:
    """
    Build lookup maps:
    - isin_map: {isin_upper: scrip_dict}
    - name_map: {name_normalized: scrip_dict}
    """
    isin_map = {}
    name_map = {}

    for scrip in scrips:
        if not isinstance(scrip, dict):
            continue
        # Field name variations
        scrip_cd = (
            scrip.get("SCRIP_CD") or scrip.get("scripCd") or
            scrip.get("ScripCd") or scrip.get("scrip_cd") or ""
        )
        ticker = (
            scrip.get("SCRIP_ID") or scrip.get("scripId") or
            scrip.get("ScripId") or scrip.get("symbol") or ""
        )
        name = (
            scrip.get("SCRIP_NAME") or scrip.get("scripName") or
            scrip.get("ScripName") or scrip.get("companyName") or ""
        )
        isin = (
            scrip.get("ISIN_NUMBER") or scrip.get("isinNo") or
            scrip.get("ISIN") or scrip.get("isin") or ""
        )

        entry = {
            "scrip_cd": str(scrip_cd).strip(),
            "ticker": str(ticker).strip(),
            "name": str(name).strip(),
            "isin": str(isin).strip(),
        }

        if isin:
            isin_map[isin.upper().strip()] = entry
        if name:
            name_map[_normalize_name(name)] = entry

    return isin_map, name_map


# ------------------------------------------------------------------ #
# Field name mappings for BSE financial result records                #
# BSE uses inconsistent naming; we try multiple variants.            #
# ------------------------------------------------------------------ #

def _get_field(record: dict, *keys):
    """Return the first non-None, non-empty value from the given keys."""
    for k in keys:
        v = record.get(k)
        if v is not None and str(v).strip() not in ("", "null", "NULL", "N.A.", "NA", "-"):
            return v
    return None


def _parse_amount(value) -> Optional[float]:
    """Convert BSE amount field to float, stripping commas/spaces."""
    if value is None:
        return None
    s = str(value).replace(",", "").strip()
    # Remove trailing non-numeric
    s = re.sub(r"[A-Za-z%\s]+$", "", s)
    # Handle negative in parentheses
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def _detect_unit_multiplier(record: dict, company_name: str = "") -> float:
    """
    BSE reports in Lakhs for most companies, Crores for larger ones.
    Try to detect from the record itself or default to Lakhs (divide by 100 = Cr).
    """
    # Check for explicit unit field
    unit_keys = ["Unit", "unit", "FaceValue", "ReportingUnit", "reportingUnit"]
    for k in unit_keys:
        v = str(record.get(k, "")).lower()
        if "crore" in v or "cr" in v:
            return 1.0
        if "lakh" in v or "lac" in v:
            return 0.01
        if "million" in v:
            return 0.1

    # Heuristic: if revenue looks very large (>100000), likely in Lakhs
    for rev_key in ("SALES_AMT", "SalesAmt", "Net Sales", "Revenue", "NetSales"):
        v = _parse_amount(record.get(rev_key))
        if v is not None:
            if v > 100_000:
                return 0.01  # Lakhs to Crores
            return 1.0  # Already in Crores

    # Default: assume Lakhs (most common for BSE API)
    return 0.01


def _extract_annual_financials(records: list) -> list:
    """
    Parse BSE financial result records into annual financials.

    BSE typically returns quarterly records; we look for full-year entries
    (period type = A or records with a March year-end date that span 12 months).
    Returns list of dicts, one per fiscal year.
    """
    annual_records = []

    for record in records:
        if not isinstance(record, dict):
            continue

        # Filter for annual/full-year records
        period_type = str(_get_field(record, "PeriodType", "periodType", "Period", "period") or "").upper()
        period_end = str(_get_field(record, "Date", "date", "PeriodEnd", "periodEnd", "ToDate") or "")
        months = _get_field(record, "NofMonths", "noOfMonths", "Months", "months")
        months_int = int(months) if months and str(months).isdigit() else 0

        is_annual = (
            period_type in ("A", "ANNUAL", "YEARLY") or
            months_int == 12 or
            (period_end and re.search(r"(Mar|March)\s*\d{4}", period_end, re.I) and months_int >= 9)
        )
        if not is_annual:
            # If no period_type info, include all records and aggregate later
            if period_type and period_type not in ("A", "", "ANNUAL"):
                continue

        # Extract fiscal year from date
        fiscal_year = None
        if period_end:
            m = re.search(r"(\d{4})", period_end)
            if m:
                year = int(m.group(1))
                # If March year-end, fiscal year = that year
                if re.search(r"Mar|March", period_end, re.I):
                    fiscal_year = year
                else:
                    fiscal_year = year

        unit = _detect_unit_multiplier(record)

        def amt(field, *alt_fields):
            v = _parse_amount(_get_field(record, field, *alt_fields))
            if v is not None:
                return v * unit
            return None

        revenue = amt(
            "SALES_AMT", "SalesAmt", "Net Sales/Income from operations",
            "NetSales", "Revenue", "TotalRevenue", "TotalIncome",
            "Income From Operations", "Gross Sales",
        )
        pbt = amt("PBT", "Pbt", "ProfitBeforeTax", "Profit Before Tax")
        pat = amt("PAT", "Pat", "ProfitAfterTax", "NetProfit", "Net Profit", "Profit For Period")
        depreciation = amt("Depreciation", "DepreciationAmt", "D&A")
        finance_costs = amt("FinanceCost", "Interest", "FinanceCosts", "Finance Cost")
        operating_profit = amt("PBDIT", "Pbdit", "OperatingProfit", "Operating Profit", "EBITDA", "EBIDTA")

        # Compute EBITDA if not directly available
        ebitda = operating_profit
        if ebitda is None and pbt is not None:
            if depreciation is not None and finance_costs is not None:
                ebitda = pbt + depreciation + finance_costs
            elif depreciation is not None:
                ebitda = pbt + depreciation

        # Balance sheet items
        total_debt = amt(
            "TotalDebt", "TotalBorrowings", "Borrowings",
            "TotalOutstandingDebt", "LTBorrowings",
        )
        cash = amt("Cash", "CashAndBankBalance", "CashAndEquivalents", "CashBankBalance")
        capex = amt("Capex", "CapitalExpenditure", "AdditionsToFA", "NetCapex")

        record_data = {
            "fiscal_year": fiscal_year,
            "revenue_cr": revenue,
            "ebitda_cr": ebitda,
            "pat_cr": pat,
            "total_debt_cr": total_debt,
            "cash_cr": cash,
            "capex_cr": capex,
        }

        # Compute derived metrics
        if total_debt is not None and cash is not None:
            record_data["net_debt_cr"] = total_debt - cash
        if revenue and revenue > 0 and ebitda is not None:
            record_data["ebitda_margin_pct"] = round((ebitda / revenue) * 100, 2)
        nd = record_data.get("net_debt_cr")
        if nd is not None and ebitda and ebitda > 0:
            record_data["net_debt_ebitda"] = round(nd / ebitda, 2)
        if finance_costs and finance_costs > 0 and ebitda is not None:
            record_data["interest_coverage"] = round(ebitda / finance_costs, 2)

        # Only add record if we have at least some financial data
        has_data = any(
            v is not None for k, v in record_data.items() if k != "fiscal_year"
        )
        if has_data:
            annual_records.append(record_data)

    # Sort by fiscal year desc, take most recent 2
    annual_records.sort(key=lambda x: x.get("fiscal_year") or 0, reverse=True)
    return annual_records[:2]


def _fetch_financials(session: requests.Session, scrip_cd: str) -> list:
    """Fetch annual financial results for a given scrip code."""
    params = {
        "scrip_cd": scrip_cd,
        "period": "Annual",
        "isConsolidated": "C",
    }
    data = _get(session, FINANCIALS_URL, params)

    records = []
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        for key in ("Table", "table", "data", "Data", "result", "Result", "Results"):
            if key in data and isinstance(data[key], list):
                records = data[key]
                break

    return _extract_annual_financials(records)


def run(limit: Optional[int] = None) -> dict:
    """
    Main BSE XBRL scrape entry point.

    1. Fetch all active BSE equity scrips
    2. For each company already in the DB, try to match by ISIN or name
    3. Fetch annual financials for matched companies
    4. Insert into financials table

    Args:
        limit: Max number of companies to process. None = no limit.

    Returns:
        dict with counts.
    """
    init_db()
    conn = get_connection()
    session = requests.Session()
    session.headers.update(HEADERS)

    counts = {
        "companies_checked": 0,
        "companies_matched": 0,
        "financial_records_inserted": 0,
        "errors": 0,
    }

    # Fetch BSE scrip list
    scrips = _fetch_scrip_list(session)
    if not scrips:
        logger.error("Failed to fetch BSE scrip list")
        conn.close()
        return counts

    isin_map, name_map = _build_scrip_maps(scrips)
    logger.info(
        "BSE maps built: %d ISINs, %d names",
        len(isin_map), len(name_map),
    )

    # Fetch all companies from DB
    db_companies = conn.execute(
        "SELECT id, name, name_normalized, isin, bse_code FROM companies"
    ).fetchall()
    logger.info("Processing %d DB companies against BSE data...", len(db_companies))

    total_processed = 0
    for company_row in db_companies:
        if limit is not None and total_processed >= limit:
            break

        company_id = company_row["id"]
        company_name = company_row["name"]
        db_isin = (company_row["isin"] or "").upper().strip()
        db_bse_code = company_row["bse_code"] or ""

        # Try to match: ISIN first, then name
        scrip_entry = None
        if db_isin and db_isin in isin_map:
            scrip_entry = isin_map[db_isin]
        else:
            # Name-based match
            norm_name = company_row["name_normalized"] or _normalize_name(company_name)
            if norm_name in name_map:
                scrip_entry = name_map[norm_name]
            else:
                # Fuzzy: try prefix match (first 20 chars)
                prefix = norm_name[:20]
                for nm, entry in name_map.items():
                    if nm.startswith(prefix) or prefix in nm:
                        scrip_entry = entry
                        break

        counts["companies_checked"] += 1

        if not scrip_entry:
            logger.debug("No BSE match for: %s", company_name)
            total_processed += 1
            continue

        # Update company record with BSE data
        scrip_cd = scrip_entry["scrip_cd"]
        bse_isin = scrip_entry["isin"]
        try:
            upsert_company(
                conn,
                company_name,
                isin=bse_isin or db_isin or None,
                bse_code=scrip_cd or db_bse_code or None,
                is_listed=1,
            )
        except Exception as exc:
            logger.error("Failed to update company %s: %s", company_name, exc)

        counts["companies_matched"] += 1
        logger.debug("Matched: %s → scrip_cd=%s", company_name, scrip_cd)

        # Fetch financials
        if not scrip_cd:
            total_processed += 1
            continue

        try:
            financials_list = _fetch_financials(session, scrip_cd)
            for fin in financials_list:
                if not any(fin.get(k) for k in ("revenue_cr", "ebitda_cr", "pat_cr")):
                    continue
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
                        net_debt_ebitda=fin.get("net_debt_ebitda"),
                        data_source="bse_xbrl",
                        extraction_confidence=0.85,  # BSE structured data is high confidence
                    )
                    counts["financial_records_inserted"] += 1
                except Exception as exc:
                    logger.error("DB error inserting financials for %s: %s", company_name, exc)
                    counts["errors"] += 1

        except Exception as exc:
            logger.error("Error fetching financials for %s (scrip=%s): %s", company_name, scrip_cd, exc)
            counts["errors"] += 1

        total_processed += 1
        time.sleep(0.3)  # polite delay

        if total_processed % 50 == 0:
            logger.info(
                "BSE progress: %d checked, %d matched, %d financials inserted",
                counts["companies_checked"],
                counts["companies_matched"],
                counts["financial_records_inserted"],
            )

    conn.close()
    logger.info(
        "BSE scrape complete. Checked: %d, Matched: %d, Financials: %d, Errors: %d",
        counts["companies_checked"],
        counts["companies_matched"],
        counts["financial_records_inserted"],
        counts["errors"],
    )
    return counts
