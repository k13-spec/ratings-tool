"""
Normalize credit rating symbols from any Indian agency into:
  - base: clean symbol string (e.g. "AA+")
  - grade: integer 1-20
  - outlook: standardized outlook string
"""

import re

# ------------------------------------------------------------------ #
# Grade mapping: lower number = better credit quality                 #
# ------------------------------------------------------------------ #
_GRADE_MAP = {
    # Long-term
    "AAA":   1,
    "AA+":   2,
    "AA":    3,
    "AA-":   4,
    "A+":    5,
    "A":     6,
    "A-":    7,
    "BBB+":  8,
    "BBB":   9,
    "BBB-": 10,
    "BB+":  11,
    "BB":   12,
    "BB-":  13,
    "B+":   14,
    "B":    15,
    "B-":   16,
    "C+":   17,
    "C":    18,
    "C-":   19,
    "D":    20,
}

_GRADE_LABEL = {v: k for k, v in _GRADE_MAP.items()}

# ------------------------------------------------------------------ #
# Agency prefixes to strip                                             #
# ------------------------------------------------------------------ #
_AGENCY_PREFIXES = [
    r"\[ICRA\]",
    r"ICRA\s+",
    r"CRISIL\s+",
    r"CARE\s+EDGE\s+",
    r"CARE\s+",
    r"IND\s+",
    r"FITCH\s+",
    r"INDIA\s+RATINGS\s+AND\s+RESEARCH\s+",
    r"ACUITE\s+",
    r"BRICKWORK\s+",
    r"BWR\s+",
    r"SMERA\s+",
    r"INFOMERICS\s+",
]

# Compiled pattern to strip agency prefixes (case-insensitive)
_PREFIX_PATTERN = re.compile(
    "^(?:" + "|".join(_AGENCY_PREFIXES) + ")+",
    re.IGNORECASE,
)

# ------------------------------------------------------------------ #
# Outlook keywords                                                     #
# ------------------------------------------------------------------ #
_OUTLOOK_PATTERNS = [
    (re.compile(r"watch\s+develop", re.I), "Watch Developing"),
    (re.compile(r"watch\s+neg",     re.I), "Watch Negative"),
    (re.compile(r"watch\s+pos",     re.I), "Watch Positive"),
    (re.compile(r"\bwatch\b",       re.I), "Watch"),
    (re.compile(r"credit\s+watch",  re.I), "Watch"),
    (re.compile(r"review\s+for\s+downgrade", re.I), "Watch Negative"),
    (re.compile(r"review\s+for\s+upgrade",   re.I), "Watch Positive"),
    (re.compile(r"negative",        re.I), "Negative"),
    (re.compile(r"positive",        re.I), "Positive"),
    (re.compile(r"stable",          re.I), "Stable"),
    (re.compile(r"developing",      re.I), "Developing"),
]

# ------------------------------------------------------------------ #
# Rating base symbols to search for, ordered longest-first to avoid   #
# partial matches (e.g. "BBB+" before "BB+")                          #
# ------------------------------------------------------------------ #
_SYMBOLS_ORDERED = sorted(_GRADE_MAP.keys(), key=len, reverse=True)
_SYMBOL_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])(" + "|".join(re.escape(s) for s in _SYMBOLS_ORDERED) + r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)


def _extract_outlook(text: str) -> str:
    """Return the first outlook keyword found in text, or empty string."""
    for pattern, label in _OUTLOOK_PATTERNS:
        if pattern.search(text):
            return label
    return ""


def normalize_rating(raw_symbol: str) -> dict:
    """
    Parse a raw agency rating string and return::

        {
            "base":    "AA+",
            "grade":   2,
            "outlook": "Stable",
        }

    Returns grade=None and base=None if the symbol cannot be parsed.
    """
    if not raw_symbol or not isinstance(raw_symbol, str):
        return {"base": None, "grade": None, "outlook": ""}

    text = raw_symbol.strip()

    # Extract outlook from the full raw string first
    outlook = _extract_outlook(text)

    # Handle comma/semicolon-separated multi-instrument strings like
    # "--, [ICRA]A4 ISSUER NOT COOPERATING" — try each segment, use first hit
    segments = re.split(r"[,;]", text)
    for segment in segments:
        result = _normalize_single(segment.strip(), outlook)
        if result["grade"] is not None:
            return result

    # All segments failed — return with whatever outlook we found
    return {"base": None, "grade": None, "outlook": outlook}


def _normalize_single(text: str, outlook: str) -> dict:
    """Normalize a single (non-comma-separated) rating segment."""
    # Strip agency prefix
    cleaned = _PREFIX_PATTERN.sub("", text).strip()

    # Remove parenthesised outlook / watch text: e.g. "(Stable)", "(Watch)"
    cleaned_no_paren = re.sub(r"\([^)]*\)", "", cleaned).strip()

    # Remove common suffixes that confuse the symbol match
    cleaned_no_paren = re.sub(
        r"\b(ISSUER\s+NOT\s+COOPERATING|INC|REAFFIRMED|UPGRADED|DOWNGRADED"
        r"|ASSIGNED|SUSPENDED|WITHDRAWN|SO|CE|PP-MLD)\b.*",
        "", cleaned_no_paren, flags=re.IGNORECASE,
    ).strip()

    # Strip trailing / leading punctuation that's not part of the symbol
    cleaned_no_paren = re.sub(r"[^A-Za-z0-9+\-]", " ", cleaned_no_paren).strip()

    # Try to find a known rating symbol
    match = _SYMBOL_PATTERN.search(cleaned_no_paren)
    if not match:
        match = _SYMBOL_PATTERN.search(cleaned)

    if not match:
        return {"base": None, "grade": None, "outlook": outlook}

    base = match.group(1).upper()
    grade = _GRADE_MAP.get(base)

    return {"base": base, "grade": grade, "outlook": outlook}


def grade_label(grade: int) -> str:
    """Reverse lookup: grade integer → symbol string (e.g. 2 → 'AA+')."""
    return _GRADE_LABEL.get(grade, f"Grade {grade}")


# ------------------------------------------------------------------ #
# Quick self-test                                                      #
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    tests = [
        "[ICRA]AA+(Stable)",
        "CRISIL AA+",
        "CARE AAA; Stable",
        "IND A-/Stable",
        "CRISIL D",
        "CARE EDGE BBB-; Negative",
        "[ICRA]BB+ (Watch Negative)",
        "AA+",
        "BBB",
        "--, [ICRA]A4 ISSUER NOT COOPERATING",  # short-term → should return None
    ]
    for t in tests:
        result = normalize_rating(t)
        print(f"{t!r:45s} -> {result}")
