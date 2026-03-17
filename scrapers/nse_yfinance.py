"""
NSE / yfinance financial data loader.

Flow:
1. Download NSE equity list (CSV) → company name + ISIN + NSE symbol
2. Match DB companies by ISIN (primary) or normalised name (fallback)
3. Fetch annual financials via yfinance (income statement + balance sheet)
4. Normalise to Crores and insert into DB

Usage:
    python run_scraper.py --nse
    python run_scraper.py --nse --limit 50
"""

import io
import logging
import re
import time
from typing import Optional

import pandas as pd
import requests
import yfinance as yf

from database.models import get_connection, init_db, insert_financial, upsert_company

logger = logging.getLogger(__name__)

NSE_EQUITY_CSV = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
INR_TO_CR = 1e-7  # raw INR values from yfinance → Crores


def _normalize_name(name: str) -> str:
    n = str(name).lower().strip()
    n = n.replace("limited", "ltd").replace("private", "pvt")
    n = re.sub(r"\s+", " ", n)
    return n


def _fetch_nse_list() -> pd.DataFrame:
    """Download NSE equity list CSV. Returns DataFrame with SYMBOL, NAME, ISIN."""
    logger.info("Fetching NSE equity list...")
    try:
        resp = requests.get(
            NSE_EQUITY_CSV,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv"},
            timeout=30,
        )
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        # Normalise column names
        df.columns = [c.strip() for c in df.columns]
        rename = {}
        for col in df.columns:
            cl = col.lower().strip()
            if "symbol" in cl:
                rename[col] = "symbol"
            elif "name" in cl and "company" in cl:
                rename[col] = "name"
            elif "isin" in cl:
                rename[col] = "isin"
        df = df.rename(columns=rename)
        df = df[["symbol", "name", "isin"]].dropna(subset=["symbol"])
        df["isin"] = df["isin"].str.strip().str.upper()
        df["symbol"] = df["symbol"].str.strip()
        df["name_norm"] = df["name"].apply(_normalize_name)
        logger.info("NSE list: %d equities", len(df))
        return df
    except Exception as exc:
        logger.error("Failed to fetch NSE equity list: %s", exc)
        return pd.DataFrame(columns=["symbol", "name", "isin", "name_norm"])


def _get_financials(symbol: str) -> list:
    """
    Fetch annual financials for an NSE symbol via yfinance.
    Returns list of dicts (one per fiscal year, most recent first), values in Crores.
    """
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        inc = ticker.financials      # income statement, columns = fiscal year dates
        bs = ticker.balance_sheet    # balance sheet
        cf = ticker.cashflow         # cash flow

        if inc is None or inc.empty:
            return []

        results = []
        for col in inc.columns[:2]:  # most recent 2 fiscal years
            year = col.year if hasattr(col, "year") else None

            def get_row(df, *keys):
                if df is None or df.empty:
                    return None
                for k in keys:
                    if k in df.index:
                        v = df.loc[k, col]
                        try:
                            f = float(v)
                            if not pd.isna(f):
                                return f * INR_TO_CR
                        except Exception:
                            pass
                return None

            revenue = get_row(inc,
                "Total Revenue", "Operating Revenue", "Net Revenue")
            ebitda = get_row(inc,
                "EBITDA", "Normalized EBITDA", "Operating Income")
            ebit = get_row(inc, "EBIT")
            dep = get_row(inc, "Reconciled Depreciation", "Depreciation And Amortization")
            # Derive EBITDA from EBIT + D&A if not directly available
            if ebitda is None and ebit is not None and dep is not None:
                ebitda = ebit + dep
            pat = get_row(inc,
                "Net Income", "Net Income Common Stockholders",
                "Net Income From Continuing Operation Net Minority Interest")
            total_debt = get_row(bs,
                "Total Debt", "Long Term Debt And Capital Lease Obligation",
                "Total Long Term Debt")
            cash = get_row(bs,
                "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments",
                "Cash And Short Term Investments")
            net_debt = get_row(bs, "Net Debt")
            capex_raw = get_row(cf,
                "Capital Expenditure", "Purchase Of PPE", "Capital Expenditures")
            capex = abs(capex_raw) if capex_raw is not None else None

            # Compute derived
            if net_debt is None and total_debt is not None and cash is not None:
                net_debt = total_debt - cash
            ebitda_margin = None
            if revenue and revenue > 0 and ebitda is not None:
                ebitda_margin = round((ebitda / revenue) * 100, 2)
            net_debt_ebitda = None
            if net_debt is not None and ebitda and ebitda > 0:
                net_debt_ebitda = round(net_debt / ebitda, 2)

            has_data = any(v is not None for v in [revenue, ebitda, pat, total_debt])
            if not has_data:
                continue

            results.append({
                "fiscal_year": year,
                "revenue_cr": round(revenue, 2) if revenue is not None else None,
                "ebitda_cr": round(ebitda, 2) if ebitda is not None else None,
                "ebitda_margin_pct": ebitda_margin,
                "pat_cr": round(pat, 2) if pat is not None else None,
                "total_debt_cr": round(total_debt, 2) if total_debt is not None else None,
                "cash_cr": round(cash, 2) if cash is not None else None,
                "net_debt_cr": round(net_debt, 2) if net_debt is not None else None,
                "capex_cr": round(capex, 2) if capex is not None else None,
                "net_debt_ebitda": net_debt_ebitda,
            })

        return results

    except Exception as exc:
        logger.debug("yfinance error for %s: %s", symbol, exc)
        return []


def run(limit: Optional[int] = None) -> dict:
    """
    Match DB companies to NSE symbols, fetch financials via yfinance.

    Args:
        limit: Max companies to process. None = no limit.
    """
    init_db()
    conn = get_connection()

    counts = {
        "companies_checked": 0,
        "companies_matched": 0,
        "financial_records_inserted": 0,
        "errors": 0,
    }

    nse_df = _fetch_nse_list()
    if nse_df.empty:
        logger.error("NSE list empty — aborting")
        conn.close()
        return counts

    # Build lookup maps
    isin_map = dict(zip(nse_df["isin"], nse_df["symbol"]))
    name_map = dict(zip(nse_df["name_norm"], nse_df["symbol"]))

    db_companies = conn.execute(
        "SELECT id, name, name_normalized, isin, bse_code FROM companies"
    ).fetchall()
    logger.info("Matching %d DB companies against %d NSE symbols", len(db_companies), len(nse_df))

    total_processed = 0
    for row in db_companies:
        if limit is not None and total_processed >= limit:
            break

        company_id = row["id"]
        company_name = row["name"]
        db_isin = (row["isin"] or "").upper().strip()
        counts["companies_checked"] += 1

        # Match: ISIN first, then normalised name
        symbol = None
        if db_isin and db_isin in isin_map:
            symbol = isin_map[db_isin]
        else:
            norm = row["name_normalized"] or _normalize_name(company_name)
            symbol = name_map.get(norm)
            if not symbol:
                # Prefix match
                prefix = norm[:25]
                for nm, sym in name_map.items():
                    if nm.startswith(prefix) or prefix in nm:
                        symbol = sym
                        break

        if not symbol:
            total_processed += 1
            continue

        counts["companies_matched"] += 1

        # Update company as listed + store NSE symbol as bse_code if not set
        try:
            upsert_company(conn, company_name, is_listed=1,
                           isin=db_isin or None,
                           bse_code=row["bse_code"] or symbol)
        except Exception:
            pass

        # Fetch financials
        financials = _get_financials(symbol)
        for fin in financials:
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
                    net_debt_ebitda=fin.get("net_debt_ebitda"),
                    data_source="nse_yfinance",
                    extraction_confidence=0.9,
                )
                counts["financial_records_inserted"] += 1
            except Exception as exc:
                logger.error("DB error for %s: %s", company_name, exc)
                counts["errors"] += 1

        total_processed += 1
        time.sleep(0.2)  # gentle rate limit

        if total_processed % 100 == 0:
            logger.info(
                "NSE progress: %d checked, %d matched, %d financials",
                counts["companies_checked"],
                counts["companies_matched"],
                counts["financial_records_inserted"],
            )

    conn.close()
    logger.info(
        "NSE/yfinance complete. Checked: %d, Matched: %d, Financials: %d, Errors: %d",
        counts["companies_checked"],
        counts["companies_matched"],
        counts["financial_records_inserted"],
        counts["errors"],
    )
    return counts
