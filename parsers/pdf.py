"""
pdfplumber-based extractor for financial tables and capex plan mentions
from rating-agency rationale PDFs.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import pdfplumber
    _HAS_PDFPLUMBER = True
except ImportError:
    _HAS_PDFPLUMBER = False
    logger.warning("pdfplumber not installed — PDF extraction disabled")

# ------------------------------------------------------------------ #
# Keyword maps                                                         #
# ------------------------------------------------------------------ #
_REVENUE_KEYS = {
    "revenue", "net sales", "income from operations",
    "total revenue", "net revenue", "gross revenue",
    "total income", "net turnover", "turnover",
}
_EBITDA_KEYS = {
    "ebitda", "operating profit", "pbdit", "ebit",
    "operating income", "op profit", "ebidta",
}
_PAT_KEYS = {
    "pat", "profit after tax", "net profit",
    "profit for the year", "net income",
}
_DEBT_KEYS = {
    "total debt", "total borrowings", "debt",
    "total financial debt", "gross debt",
    "total outstanding debt",
}
_CASH_KEYS = {
    "cash and equivalents", "cash & equivalents",
    "cash and bank", "cash & bank balances",
    "cash", "liquid investments",
}
_NET_DEBT_KEYS = {"net debt", "net financial debt"}
_CAPEX_KEYS = {
    "capex", "capital expenditure", "additions to fixed assets",
    "purchase of fixed assets", "net capex",
}
_INTEREST_COVERAGE_KEYS = {"interest coverage", "icr", "dscr", "interest service"}

# ------------------------------------------------------------------ #
# Unit multipliers (normalise to Crores)                              #
# ------------------------------------------------------------------ #
_UNIT_MAP = {
    "cr":  1.0,
    "crs": 1.0,
    "crore": 1.0,
    "crores": 1.0,
    "mn":  0.1,        # 1 Mn = 0.1 Cr (assuming Indian millions = 10 lakhs)
    "million": 0.1,
    "millions": 0.1,
    "lakh": 0.01,
    "lakhs": 0.01,
    "lacs": 0.01,
    "lac": 0.01,
    "bn":  100.0,      # 1 Bn = 100 Cr
    "billion": 100.0,
    "billions": 100.0,
}

# Capex plan regex
_CAPEX_PLAN_RE = re.compile(
    r"[Cc]apex\s+(?:of\s+)?(?:Rs\.?\s*)?(\d[\d,\.]*)\s*"
    r"(crore|cr|lakh|mn|bn|million|billion)?",
    re.IGNORECASE,
)
_FORWARD_LOOKING_RE = re.compile(
    r"over\s+the\s+next|planned|expected|FY2[0-9]|upcoming|envisaged|proposed",
    re.IGNORECASE,
)


def _parse_value(cell: str) -> Optional[float]:
    """Convert a table cell string to a float, returning None if unparseable."""
    if cell is None:
        return None
    s = str(cell).strip()
    # Remove commas, spaces
    s = s.replace(",", "").replace(" ", "")
    # Handle negative in parentheses: (123) => -123
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    # Remove trailing non-numeric suffixes like "Cr", "%"
    s = re.sub(r"[A-Za-z%]+$", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _apply_unit(value: float, unit_hint: str) -> float:
    """Apply unit multiplier to convert to Crores."""
    if not unit_hint:
        return value
    multiplier = _UNIT_MAP.get(unit_hint.lower().strip(), 1.0)
    return value * multiplier


def _detect_unit_from_header(text: str) -> float:
    """
    Scan a text block for unit declarations like 'Rs. Crore', 'in Lakhs', etc.
    Return the multiplier to apply to all values.
    """
    text_lower = text.lower()
    if re.search(r"in\s+lakh|rs\.?\s*lakh|\(lakh", text_lower):
        return 0.01
    if re.search(r"in\s+million|rs\.?\s*million|\(mn\)", text_lower):
        return 0.1
    if re.search(r"in\s+billion|rs\.?\s*billion|\(bn\)", text_lower):
        return 100.0
    # Default: assume Crores
    return 1.0


def _keyword_match(cell_text: str, keyword_set: set) -> bool:
    """Return True if cell_text (lowercased) contains any keyword."""
    t = str(cell_text).lower().strip()
    return any(kw in t for kw in keyword_set)


def _find_year_columns(header_row: List) -> List[int]:
    """
    Given a header row, return column indices that look like fiscal years
    (e.g. FY25, FY2025, 2024-25, Mar-25).  Rightmost first (most recent).
    """
    year_pattern = re.compile(
        r"(?:FY\s*\d{2,4}|\d{4}-\d{2,4}|Mar-\d{2}|March\s*\d{4}|\d{4})",
        re.IGNORECASE,
    )
    candidates = []
    for idx, cell in enumerate(header_row):
        if cell and year_pattern.search(str(cell)):
            candidates.append(idx)
    return list(reversed(candidates))  # most recent first


def _extract_year_from_header(cell_text: str) -> Optional[int]:
    """Try to extract a 4-digit fiscal year from a column header."""
    m = re.search(r"20(\d{2})", str(cell_text))
    if m:
        yy = int(m.group(1))
        return 2000 + yy
    m = re.search(r"FY\s*(\d{2})(?!\d)", str(cell_text), re.IGNORECASE)
    if m:
        yy = int(m.group(1))
        base = 2000 if yy < 50 else 1900
        return base + yy + 1  # FY25 → 2025
    return None


def _process_table(
    table: List[List], unit_multiplier: float
) -> Dict[str, Optional[float]]:
    """
    Scan a 2-D table (list of lists) for financial keyword rows.
    Returns a dict of extracted values + the best fiscal_year found.
    """
    if not table or len(table) < 2:
        return {}

    # First row as header
    header = table[0]
    year_col_indices = _find_year_columns(header)

    # Use most recent year column (first in reversed list), fallback to last column
    target_col = year_col_indices[0] if year_col_indices else (len(header) - 1)
    fiscal_year = None
    if year_col_indices:
        fiscal_year = _extract_year_from_header(header[year_col_indices[0]])

    result: Dict[str, Optional[float]] = {}
    if fiscal_year:
        result["fiscal_year"] = fiscal_year

    for row in table[1:]:
        if not row or len(row) < 2:
            continue
        label_cell = row[0]
        if label_cell is None:
            continue

        def get_val(col_idx: int) -> Optional[float]:
            if col_idx < len(row):
                raw = _parse_value(row[col_idx])
                if raw is not None:
                    return raw * unit_multiplier
            return None

        if "revenue_cr" not in result and _keyword_match(label_cell, _REVENUE_KEYS):
            v = get_val(target_col)
            if v is not None:
                result["revenue_cr"] = v

        if "ebitda_cr" not in result and _keyword_match(label_cell, _EBITDA_KEYS):
            v = get_val(target_col)
            if v is not None:
                result["ebitda_cr"] = v

        if "pat_cr" not in result and _keyword_match(label_cell, _PAT_KEYS):
            v = get_val(target_col)
            if v is not None:
                result["pat_cr"] = v

        if "total_debt_cr" not in result and _keyword_match(label_cell, _DEBT_KEYS):
            v = get_val(target_col)
            if v is not None:
                result["total_debt_cr"] = v

        if "cash_cr" not in result and _keyword_match(label_cell, _CASH_KEYS):
            v = get_val(target_col)
            if v is not None:
                result["cash_cr"] = v

        if "net_debt_cr" not in result and _keyword_match(label_cell, _NET_DEBT_KEYS):
            v = get_val(target_col)
            if v is not None:
                result["net_debt_cr"] = v

        if "capex_cr" not in result and _keyword_match(label_cell, _CAPEX_KEYS):
            v = get_val(target_col)
            if v is not None:
                result["capex_cr"] = v

        if "interest_coverage" not in result and _keyword_match(
            label_cell, _INTEREST_COVERAGE_KEYS
        ):
            v = get_val(target_col)
            if v is not None:
                result["interest_coverage"] = v

    return result


def _compute_derived(data: dict) -> dict:
    """Compute net_debt, ebitda_margin, net_debt_ebitda from extracted values."""
    # Net debt
    if "net_debt_cr" not in data:
        if "total_debt_cr" in data and "cash_cr" in data:
            data["net_debt_cr"] = data["total_debt_cr"] - data["cash_cr"]

    # EBITDA margin
    if "ebitda_margin_pct" not in data:
        rev = data.get("revenue_cr")
        ebitda = data.get("ebitda_cr")
        if rev and ebitda and rev > 0:
            data["ebitda_margin_pct"] = round((ebitda / rev) * 100, 2)

    # Net debt / EBITDA
    if "net_debt_ebitda" not in data:
        nd = data.get("net_debt_cr")
        ebitda = data.get("ebitda_cr")
        if nd is not None and ebitda and ebitda > 0:
            data["net_debt_ebitda"] = round(nd / ebitda, 2)

    return data


def _confidence_score(data: dict) -> float:
    """Score 0-1 based on how many key financial fields were extracted."""
    key_fields = [
        "revenue_cr", "ebitda_cr", "pat_cr",
        "total_debt_cr", "cash_cr", "capex_cr",
    ]
    found = sum(1 for f in key_fields if f in data and data[f] is not None)
    return round(found / len(key_fields), 2)


def extract_financials(pdf_source) -> dict:
    """
    Extract key financial metrics from a rating rationale PDF.

    Args:
        pdf_source: file path (str/Path) or in-memory bytes/BytesIO object.

    Returns a dict with keys:
        revenue_cr, ebitda_cr, ebitda_margin_pct, pat_cr,
        total_debt_cr, cash_cr, net_debt_cr, capex_cr,
        interest_coverage, net_debt_ebitda, fiscal_year, confidence
    Returns empty dict on failure.
    """
    if not _HAS_PDFPLUMBER:
        return {}

    import io as _io
    if isinstance(pdf_source, (str, Path)):
        path = Path(pdf_source)
        if not path.exists():
            logger.warning("PDF not found: %s", pdf_source)
            return {}
        open_arg = str(path)
    elif isinstance(pdf_source, (bytes, bytearray)):
        open_arg = _io.BytesIO(pdf_source)
    else:
        open_arg = pdf_source  # assume file-like BytesIO

    aggregated: dict = {}

    try:
        with pdfplumber.open(open_arg) as pdf:
            full_text = ""
            for page in pdf.pages:
                # Detect unit from page text
                page_text = page.extract_text() or ""
                full_text += page_text + "\n"
                unit_multiplier = _detect_unit_from_header(page_text)

                tables = page.extract_tables()
                for table in tables:
                    partial = _process_table(table, unit_multiplier)
                    # Merge — earlier pages / higher-confidence values kept
                    for k, v in partial.items():
                        if k not in aggregated and v is not None:
                            aggregated[k] = v

            # Try ICRA-style plain-text table (most reliable for ICRA PDFs)
            text_table_data = _extract_from_text_table(full_text)
            for k, v in text_table_data.items():
                if k not in aggregated and v is not None:
                    aggregated[k] = v

            # If still missing fields, try regex text-based extraction as fallback
            if "revenue_cr" not in aggregated or "ebitda_cr" not in aggregated:
                text_data = _extract_from_text(full_text)
                for k, v in text_data.items():
                    if k not in aggregated and v is not None:
                        aggregated[k] = v

    except Exception as exc:
        logger.error("Error extracting financials from %s: %s", pdf_path, exc)
        return {}

    aggregated = _compute_derived(aggregated)
    aggregated["confidence"] = _confidence_score(aggregated)
    return aggregated


def _extract_from_text_table(text: str) -> dict:
    """
    Parse ICRA's plain-text 'Key financial indicators' section.

    Format:
        Key financial indicators (audited)
        Company Name FY2024 FY2025
        Operating income 4269.1 4329.8
        PAT 446.4 341.6
        OPBDIT/OI 13.9% 10.4%
        Total debt/OPBDIT (times) 0.0 0.2
        Interest coverage (times) 103.7 26.2
    """
    result: dict = {}

    # Find the section
    m = re.search(
        r"Key\s+financial\s+indicators",
        text,
        re.IGNORECASE,
    )
    if not m:
        return result

    # Grab a generous block after the header (up to 3000 chars)
    block = text[m.start(): m.start() + 3000]
    lines = block.splitlines()

    # Find the year-header line (contains FY\d{2,4} or \d{4}-\d{2})
    year_pat = re.compile(
        r"(?:FY\s*\d{2,4}|\d{4}-\d{2,4}|Mar[-\s]\d{2,4})",
        re.IGNORECASE,
    )
    year_line_idx = None
    year_cols: List[str] = []
    for i, line in enumerate(lines[1:], start=1):
        years_found = year_pat.findall(line)
        if len(years_found) >= 1:
            year_line_idx = i
            year_cols = years_found
            break

    if not year_cols:
        return result

    # Most recent year = last entry in year_cols
    most_recent_header = year_cols[-1]
    fiscal_year = _extract_year_from_header(most_recent_header)
    if fiscal_year:
        result["fiscal_year"] = fiscal_year

    # Number of value columns per data row matches len(year_cols)
    n_cols = len(year_cols)
    # We want the last (most recent) value column
    val_idx = n_cols - 1  # 0-based index within the trailing values

    # Data label → schema field mapping
    LABEL_MAP = [
        ({"operating income", "net sales", "revenue", "income from operations",
          "total income", "total revenue", "net revenue", "turnover"}, "revenue_cr"),
        ({"pat", "profit after tax", "net profit", "profit for the year"}, "pat_cr"),
        ({"interest coverage", "icr", "interest service coverage"}, "interest_coverage"),
    ]
    # Ratio labels (value is a multiplier, not crores)
    RATIO_MAP = {
        "total debt/opbdit": "total_debt_opbdit",
        "total debt/ebitda": "total_debt_opbdit",
        "net debt/opbdit": "net_debt_opbdit",
        "net debt/ebitda": "net_debt_opbdit",
    }
    # Percentage labels
    PCT_MAP = {
        "opbdit/oi": "ebitda_margin_pct",
        "ebitda/revenue": "ebitda_margin_pct",
        "ebitda margin": "ebitda_margin_pct",
        "operating margin": "ebitda_margin_pct",
        "pbdit/oi": "ebitda_margin_pct",
        "pat/oi": "pat_margin_pct",
    }

    # Parse each data line after year_line_idx
    for line in lines[year_line_idx + 1:]:
        line = line.strip()
        if not line:
            continue
        # Stop if we hit another section header
        if re.match(r"^[A-Z][A-Za-z\s]+:$", line) or \
                re.match(r"^(Note|Rating|Instrument|Outlook)", line, re.I):
            break

        # Extract trailing numeric tokens (possibly with %)
        tokens = re.findall(r"-?[\d,]+\.?\d*%?|\([\d,]+\.?\d*\)", line)
        if len(tokens) < n_cols:
            continue  # not a data row with enough values

        # Get the target value (most recent column)
        raw_token = tokens[-(n_cols - val_idx)]  # from end
        is_pct = "%" in raw_token
        raw_val = _parse_value(raw_token.replace("%", ""))
        if raw_val is None:
            continue

        # Derive label: everything before the first numeric-looking token
        first_tok_pos = line.find(tokens[0])
        label = line[:first_tok_pos].strip().rstrip("(").strip().lower()
        # Remove trailing "(times)", "(x)", etc.
        label = re.sub(r"\s*\(times?\)", "", label, flags=re.I).strip()

        # Match label to a field
        matched = False

        # Check percentage fields first
        for pct_label, field in PCT_MAP.items():
            if pct_label in label:
                if field not in result:
                    result[field] = round(raw_val, 2)
                matched = True
                break

        if not matched:
            # Check ratio fields
            for ratio_label, field in RATIO_MAP.items():
                if ratio_label in label:
                    if field not in result:
                        result[field] = raw_val
                    matched = True
                    break

        if not matched:
            # Check absolute value fields
            for kw_set, field in LABEL_MAP:
                if any(kw in label for kw in kw_set):
                    if field not in result:
                        # Apply unit multiplier (default Crores for ICRA)
                        unit_mult = _detect_unit_from_header(block)
                        result[field] = round(raw_val * unit_mult, 2)
                    matched = True
                    break

    # Derive EBITDA absolute from revenue × margin%
    if "ebitda_cr" not in result:
        rev = result.get("revenue_cr")
        margin = result.get("ebitda_margin_pct")
        if rev is not None and margin is not None and rev > 0:
            result["ebitda_cr"] = round(rev * margin / 100, 2)

    # Derive total_debt_cr from ratio × EBITDA
    if "total_debt_cr" not in result:
        ratio = result.get("total_debt_opbdit")
        ebitda = result.get("ebitda_cr")
        if ratio is not None and ebitda is not None and ebitda > 0:
            result["total_debt_cr"] = round(ratio * ebitda, 2)

    if "net_debt_cr" not in result:
        ratio = result.get("net_debt_opbdit")
        ebitda = result.get("ebitda_cr")
        if ratio is not None and ebitda is not None and ebitda > 0:
            result["net_debt_cr"] = round(ratio * ebitda, 2)

    # Clean up helper fields not in schema
    for tmp in ("total_debt_opbdit", "net_debt_opbdit", "pat_margin_pct"):
        result.pop(tmp, None)

    return result


def _extract_from_text(text: str) -> dict:
    """
    Fallback: use regex to pull financial values from plain text when tables
    aren't cleanly parsed.
    """
    result = {}
    unit_multiplier = _detect_unit_from_header(text)

    # Pattern: "Revenue: Rs. 1,234 Cr" or "Revenue Rs 1234.5"
    def find_value(keywords: list, text_block: str) -> Optional[float]:
        for kw in keywords:
            pattern = re.compile(
                rf"{re.escape(kw)}\s*[:\-]?\s*(?:Rs\.?\s*)?(\d[\d,\.]*)"
                r"\s*(crore|cr|lakh|mn|bn|million|billion)?",
                re.IGNORECASE,
            )
            m = pattern.search(text_block)
            if m:
                raw = _parse_value(m.group(1))
                unit = m.group(2)
                if raw is not None:
                    if unit:
                        return _apply_unit(raw, unit)
                    return raw * unit_multiplier
        return None

    mapping = [
        ("revenue_cr", ["Revenue", "Net Sales", "Income from Operations", "Total Revenue"]),
        ("ebitda_cr", ["EBITDA", "Operating Profit", "PBDIT"]),
        ("pat_cr", ["PAT", "Profit After Tax", "Net Profit"]),
        ("total_debt_cr", ["Total Debt", "Total Borrowings"]),
        ("cash_cr", ["Cash and Equivalents", "Cash & Equivalents"]),
        ("net_debt_cr", ["Net Debt"]),
        ("capex_cr", ["Capex", "Capital Expenditure"]),
    ]
    for field, keywords in mapping:
        v = find_value(keywords, text)
        if v is not None:
            result[field] = v

    return result


def extract_capex_plans(pdf_source) -> List[dict]:
    """
    Extract forward-looking capex plan mentions from a PDF.

    Args:
        pdf_source: file path (str/Path) or in-memory bytes/BytesIO object.

    Returns list of dicts with: amount_cr, description, source_text
    """
    if not _HAS_PDFPLUMBER:
        return []

    import io as _io
    if isinstance(pdf_source, (str, Path)):
        path = Path(pdf_source)
        if not path.exists():
            return []
        open_arg = str(path)
    elif isinstance(pdf_source, (bytes, bytearray)):
        open_arg = _io.BytesIO(pdf_source)
    else:
        open_arg = pdf_source

    plans = []
    try:
        with pdfplumber.open(open_arg) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                sentences = re.split(r"(?<=[.!?])\s+", text)
                for sentence in sentences:
                    if not _FORWARD_LOOKING_RE.search(sentence):
                        continue
                    for m in _CAPEX_PLAN_RE.finditer(sentence):
                        raw_val = _parse_value(m.group(1))
                        unit = m.group(2) or ""
                        if raw_val is None:
                            continue
                        amount_cr = _apply_unit(raw_val, unit) if unit else raw_val * _detect_unit_from_header(text)
                        # Extract rough timeframe
                        tf_match = re.search(r"(\d+)\s*[-–]\s*(\d+)\s*years?", sentence, re.I)
                        tf_match2 = re.search(r"next\s+(\d+)\s+years?", sentence, re.I)
                        timeframe = None
                        if tf_match:
                            timeframe = int(tf_match.group(2))
                        elif tf_match2:
                            timeframe = int(tf_match2.group(1))

                        plans.append({
                            "amount_cr": round(amount_cr, 2),
                            "timeframe_years": timeframe,
                            "description": sentence[:200],
                            "source_text": sentence[:500],
                        })
    except Exception as exc:
        logger.error("Error extracting capex plans from %s: %s", pdf_path, exc)

    return plans


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        result = extract_financials(sys.argv[1])
        print("Financials:", result)
        plans = extract_capex_plans(sys.argv[1])
        print("Capex plans:", plans)
    else:
        print("Usage: python pdf.py <path_to_pdf>")
