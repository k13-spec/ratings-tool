#!/usr/bin/env python3
"""
Build data/issuer_prefix_map.csv — ISIN issuer-prefix → ratings-DB company.

The bond tracker joins NSDL bonds to ratings by normalized issuer NAME, which
is fuzzy and misses spelling variants. Indian corporate ISINs embed a stable
issuer code in their first 7 characters (e.g. INE002A* = Reliance Industries),
so a curated prefix→company map turns the join into an exact ISIN lookup.

Matching is deliberately conservative (verified 2026-08-05 on the full NSDL
file): exact match on an aggressive normalization (legal suffixes, '&'→and,
"(formerly …)" stripped, single-letter runs merged), plus a strict fuzzy pass
(token_sort ≥96) that additionally requires identical "distinguishing tokens"
(numerals, roman numerals, number words, short initials) on both sides — this
blocks sibling-SPV mismatches like "Project 2" → "Project 1" or "SPR" → "S R".

Usage (from the ratings-tool repo root):
    python build_isin_map.py                    # fetch NSDL live, write map
    python build_isin_map.py path/to/nsdl.xlsx  # use a local NSDL export
On any fetch/parse failure the existing map is left untouched.
"""
import collections
import io
import re
import sqlite3
import sys
import time
import warnings
from datetime import date, datetime
from pathlib import Path

import openpyxl
import pandas as pd
import requests

try:
    from rapidfuzz import process, fuzz
    HAVE_FUZZ = True
except ImportError:
    HAVE_FUZZ = False

ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "ratings.db"
OUT = ROOT / "data" / "issuer_prefix_map.csv"
MIN_MAPPED = 1000   # refuse to overwrite the map with a suspiciously small one

NSDL_API = ("https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo"
            "/listofsecurities?type=Active")
NSDL_HOME = "https://www.indiabondinfo.nsdl.com/CBDServices/"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
      "Referer": NSDL_HOME, "Accept": "*/*"}

_STOP = {"limited", "ltd", "private", "pvt", "llp", "corporation", "corp",
         "inc", "co", "company", "the", "and", "of"}
_ROMAN = re.compile(r"^(?=[ivxl])(x{0,3})(ix|iv|v?i{0,3}|l?x{0,3})$")
_NUMWORDS = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
             "one", "two", "three", "four", "five", "six", "seven", "eight",
             "nine", "ten", "eleven", "twelve"}


def norm(n: str) -> str:
    n = str(n).lower()
    n = re.sub(r"\((formerly|erstwhile)[^)]*\)", " ", n)
    n = n.replace("&", " and ")
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    words = [w for w in n.split() if w not in _STOP]
    out = []
    for w in words:  # merge runs of single letters: "m p" -> "mp"
        if out and len(w) == 1 and len(out[-1]) == 1 and w.isalpha() and out[-1].isalpha():
            out[-1] += w
        else:
            out.append(w)
    return " ".join(out).strip()


def _short_alpha_tokens(k: str) -> list:
    return sorted(t for t in k.split() if t.isalpha() and len(t) <= 3)


def distinguishers(k: str) -> list:
    d = []
    for t in k.split():
        if t.isdigit() or _ROMAN.match(t) or t in _NUMWORDS or len(t) == 1 \
                or re.search(r"\d", t):
            d.append(t)
    return sorted(d)


def fetch_nsdl(local_path: str | None) -> bytes:
    if local_path:
        return Path(local_path).read_bytes()
    s = requests.Session()
    s.headers.update(UA)
    s.get(NSDL_HOME, timeout=20)
    time.sleep(0.3)
    r = s.get(NSDL_API, timeout=180)
    r.raise_for_status()
    return r.content


def main() -> int:
    if not DB.exists():
        sys.exit(f"ERROR: {DB} not found — run from the ratings-tool repo root.")
    local = sys.argv[1] if len(sys.argv) > 1 else None

    try:
        data = fetch_nsdl(local)
        warnings.filterwarnings("ignore")
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        hdr = {h: i for i, h in enumerate(rows[0]) if h}
        assert "ISIN" in hdr and "Name of Issuer" in hdr
    except Exception as exc:
        print(f"NSDL fetch/parse failed ({exc}) — keeping existing map.", file=sys.stderr)
        return 0   # non-fatal: the workflow continues with the old map

    def pdte(s):
        try:
            return datetime.strptime(str(s).strip(), "%d-%m-%Y").date()
        except Exception:
            return None

    today = date.today()
    issuer_prefixes = collections.defaultdict(set)
    for r in rows[1:]:
        if not r or not r[1]:
            continue
        md = pdte(r[hdr["Date of Redemption/Conversion"]])
        if not md or md < today:
            continue
        nm, isin = r[hdr["Name of Issuer"]], str(r[hdr["ISIN"]]).strip()
        if nm and len(isin) == 12 and isin.startswith("IN"):
            issuer_prefixes[str(nm).strip()].add(isin[:7])
    print(f"NSDL: {len(issuer_prefixes)} active issuers")

    conn = sqlite3.connect(str(DB))
    companies = [r[0] for r in conn.execute(
        "SELECT DISTINCT c.name FROM companies c JOIN ratings r ON r.company_id=c.id "
        "WHERE c.name IS NOT NULL")]
    conn.close()
    db_by_norm = {}
    for c in companies:
        db_by_norm.setdefault(norm(c), c)
    print(f"DB: {len(companies)} rated companies")

    matches = {}
    unmatched = []
    for n in issuer_prefixes:
        k = norm(n)
        if k and k in db_by_norm:
            matches[n] = (db_by_norm[k], "exact", 100.0)
        else:
            unmatched.append(n)

    fuzzy_ct = 0
    if HAVE_FUZZ:
        keys = list(db_by_norm.keys())
        for n in unmatched:
            k = norm(n)
            if len(k) < 8:
                continue
            res = process.extract(k, keys, scorer=fuzz.token_sort_ratio,
                                  limit=2, score_cutoff=93)
            if not res or res[0][1] < 96:
                continue
            cand_key = res[0][0]
            cand = db_by_norm[cand_key]
            if distinguishers(k) != distinguishers(cand_key):
                continue
            if _short_alpha_tokens(k) != _short_alpha_tokens(cand_key):
                continue
            second = res[1][1] if len(res) > 1 else 0
            if res[0][1] - second < 1.5 and res[0][1] < 100:
                continue
            matches[n] = (cand, "fuzzy", round(res[0][1], 1))
            fuzzy_ct += 1

    print(f"matched: {len(matches)} issuers ({fuzzy_ct} via strict fuzzy)")
    if len(matches) < MIN_MAPPED:
        print(f"ABORT: only {len(matches)} matches (<{MIN_MAPPED}) — keeping existing map.",
              file=sys.stderr)
        return 1

    out = []
    for n, (c, method, score) in matches.items():
        for p in issuer_prefixes[n]:
            out.append({"isin_prefix": p, "company_name": c,
                        "nsdl_issuer": n, "match_method": method, "score": score})
    df = pd.DataFrame(out).sort_values("isin_prefix")
    # a prefix must map to exactly one company
    df = df.drop_duplicates(subset="isin_prefix", keep="first")
    df.to_csv(OUT, index=False)
    print(f"wrote {OUT}: {len(df)} prefixes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
