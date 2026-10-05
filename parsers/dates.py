"""
Date normalization for rating_date values (2026-10-05 data overhaul).

Every scraper historically stored rating_date as whatever text its source
emitted — "12-Jan-24" (CRISIL action text), "September 25, 2026" (CARE Edge
press releases), "2025-03-31T00:00:00+05:30" (ICRA detail pages),
"/Date(1690848000000)/" (ASP.NET JSON), "05/01/2026" (dd/mm), etc.

Both the app (database/queries.py) and the export (db_maintenance.py) pick
the "latest" rating per (company, agency) with ORDER BY rating_date DESC on
that TEXT column, i.e. lexicographically. Mixed formats make that ordering
meaningless — "September 25, 2024" sorts AFTER "2026-03-31" — so a stale row
can shadow a fresh one and the portal shows dated ratings even when current
rows exist. Normalizing everything to ISO YYYY-MM-DD makes text ordering
equal chronological ordering.

Ambiguous numeric dates (05/01/2026) are read dd/mm per Indian convention —
every source here is Indian.

to_iso(raw) -> "YYYY-MM-DD" or None if unparseable / implausible.
"""
import re
from datetime import date, datetime, timedelta, timezone

_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
for _m in list(_MONTHS):
    _MONTHS[_m[:3]] = _MONTHS[_m]
# common scrape artifacts
_MONTHS["sept"] = 9

_ISO_RE      = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ].*)?$")
_ASPNET_RE   = re.compile(r"/Date\((\-?\d+)(?:[+\-]\d{4})?\)/")
_DMY_TXT_RE  = re.compile(r"^(\d{1,2})[\s\-/.]*([A-Za-z]{3,9})[\s\-/.,]*(\d{2,4})$")
_MDY_TXT_RE  = re.compile(r"^([A-Za-z]{3,9})[\s\-/.]*(\d{1,2})(?:st|nd|rd|th)?[\s,\-/.]+(\d{4})$")
_DMY_NUM_RE  = re.compile(r"^(\d{1,2})[\-/.](\d{1,2})[\-/.](\d{2,4})$")
_YMD_NUM_RE  = re.compile(r"^(\d{4})[\-/.](\d{1,2})[\-/.](\d{1,2})$")

_MIN_YEAR = 1990


def _plausible(y: int, m: int, d: int):
    if not (_MIN_YEAR <= y <= date.today().year + 1):
        return None
    try:
        dt = date(y, m, d)
    except ValueError:
        return None
    if dt > date.today() + timedelta(days=366):
        return None
    return dt.isoformat()


def _fix_year(y: int) -> int:
    if y >= 100:
        return y
    # two-digit years: 00-89 -> 2000s, 90-99 -> 1990s
    return 2000 + y if y < 90 else 1900 + y


def to_iso(raw):
    """Normalize a rating date string to ISO YYYY-MM-DD, or None."""
    if raw is None:
        return None
    s = str(raw).strip().strip("'\"").strip()
    if not s or s.lower() in ("none", "null", "na", "n.a.", "n/a", "-", "--"):
        return None

    m = _ISO_RE.match(s)
    if m:
        return _plausible(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    m = _ASPNET_RE.search(s)
    if m:
        try:
            dt = datetime.fromtimestamp(int(m.group(1)) / 1000.0, tz=timezone.utc)
            return _plausible(dt.year, dt.month, dt.day)
        except (ValueError, OverflowError, OSError):
            return None

    m = _DMY_TXT_RE.match(s)                      # 12-Jan-24 / 12 January 2024
    if m:
        mon = _MONTHS.get(m.group(2).lower())
        if mon:
            return _plausible(_fix_year(int(m.group(3))), mon, int(m.group(1)))

    m = _MDY_TXT_RE.match(s)                      # September 25, 2026 / Jan 5 2024
    if m:
        mon = _MONTHS.get(m.group(1).lower())
        if mon:
            return _plausible(int(m.group(3)), mon, int(m.group(2)))

    m = _YMD_NUM_RE.match(s)                      # 2026/01/05
    if m:
        return _plausible(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    m = _DMY_NUM_RE.match(s)                      # 05/01/2026 == 5 Jan (dd/mm)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), _fix_year(int(m.group(3)))
        if mo > 12 and d <= 12:                   # clearly mm/dd — swap
            d, mo = mo, d
        return _plausible(y, mo, d)

    return None
