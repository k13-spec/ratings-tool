#!/usr/bin/env python3
"""
Ratings data audit (2026-10-05 overhaul) — "is what's here active and
current, and what should be here but isn't?"

Runs automatically at the end of db_maintenance.py (and standalone:
    python audit_ratings.py [--no-network]
).

Writes data/audit_report.json (machine-readable) and data/AUDIT.md
(human-readable), then git-STAGES both so the refresh workflow's existing
commit step publishes them — it must never commit or push itself.

Checks:
  STALENESS  per agency, age of the latest (current) rating per company:
             fresh <=6m | aging 6-12m | dated 12-18m | stale >18m | undated.
             The 6-month bar is the maintainer's chosen re-verification bar
             (2026-10-05); agencies review annually, so 6-12m is "aging",
             not necessarily wrong.
  HYGIENE    current rows that are Withdrawn / Issuer Not Cooperating /
             ungraded — shown as if live.
  COVERAGE   per-agency company counts; cross-agency matrix; ICRA universe
             diff via the ICRA search API (network, optional); CRISIL index
             entries vs DB; India Ratings issuer-ID ceiling pressure; CARE
             discover rotation state; NSDL active-issuer gaps from
             data/missing_issuers.csv (written by build_isin_map.py).
"""
import json
import os
import re
import sqlite3
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "ratings.db"
OUT_JSON = ROOT / "data" / "audit_report.json"
OUT_MD = ROOT / "data" / "AUDIT.md"

FRESH_D, AGING_D, DATED_D = 183, 365, 548   # bucket edges in days

# Same "current rating" ranking as db_maintenance.EXPORT_QUERY / the app.
LATEST_QUERY = """
WITH ranked AS (
    SELECT c.name AS company_name, r.company_id, r.agency,
           r.rating_symbol AS rating, r.rating_grade AS grade,
           r.outlook, r.rating_date,
           ROW_NUMBER() OVER (
               PARTITION BY r.company_id, r.agency
               ORDER BY
                 CASE WHEN r.agency='ICRA' AND (r.rating_symbol LIKE '--%'
                       OR r.rating_symbol LIKE 'Withdrawn%'
                       OR r.rating_symbol LIKE '*%') THEN 1 ELSE 0 END,
                 CASE WHEN r.agency='CARE Edge' AND r.instrument_type NOT IN ('LT','LT/ST') THEN 1 ELSE 0 END,
                 r.rating_grade IS NULL,
                 r.rating_date IS NULL, r.rating_date DESC, r.id DESC
           ) AS rn
    FROM ratings r JOIN companies c ON c.id = r.company_id
)
SELECT company_name, company_id, agency, rating, grade, outlook, rating_date
FROM ranked WHERE rn = 1
"""

_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_WITHDRAWN_RE = re.compile(r"withdraw", re.I)
_INC_RE = re.compile(r"not\s*co-?operating|issuer not cooperating|\bINC\b", re.I)


def _age_days(iso: str, today: date):
    try:
        d = datetime.strptime(iso, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    return (today - d).days


def collect(conn, network: bool = True) -> dict:
    today = date.today()
    cur = conn.cursor()
    rows = cur.execute(LATEST_QUERY).fetchall()

    report = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "staleness_bar_days": FRESH_D,
        "totals": {}, "staleness": {}, "hygiene": {}, "coverage": {},
        "worst_stale_examples": {},
    }

    # ---- totals ------------------------------------------------------------
    report["totals"]["companies"] = cur.execute(
        "SELECT COUNT(*) FROM companies").fetchone()[0]
    report["totals"]["rated_companies"] = cur.execute(
        "SELECT COUNT(DISTINCT company_id) FROM ratings").fetchone()[0]
    report["totals"]["rating_rows"] = cur.execute(
        "SELECT COUNT(*) FROM ratings").fetchone()[0]
    report["totals"]["current_rows"] = len(rows)
    graded_companies = {r[1] for r in rows if r[4] is not None}
    report["totals"]["companies_with_graded_current"] = len(graded_companies)
    rated = {r[1] for r in rows}
    report["totals"]["companies_with_no_graded_current"] = len(rated - graded_companies)

    # ---- staleness & hygiene per agency -------------------------------------
    per_agency = {}
    examples = {}
    for name, cid, agency, rating, grade, outlook, rdate in rows:
        a = per_agency.setdefault(agency, {
            "companies": 0, "fresh_0_6m": 0, "aging_6_12m": 0,
            "dated_12_18m": 0, "stale_18m_plus": 0, "undated_or_unparseable": 0,
            "withdrawn_as_current": 0, "not_cooperating": 0, "ungraded_current": 0,
        })
        a["companies"] += 1
        sym = rating or ""
        if _WITHDRAWN_RE.search(sym):
            a["withdrawn_as_current"] += 1
        if _INC_RE.search(sym) or _INC_RE.search(outlook or ""):
            a["not_cooperating"] += 1
        if grade is None:
            a["ungraded_current"] += 1
        iso = rdate if (rdate and _ISO_RE.match(str(rdate))) else None
        age = _age_days(iso, today) if iso else None
        if age is None:
            a["undated_or_unparseable"] += 1
        elif age <= FRESH_D:
            a["fresh_0_6m"] += 1
        elif age <= AGING_D:
            a["aging_6_12m"] += 1
        elif age <= DATED_D:
            a["dated_12_18m"] += 1
        else:
            a["stale_18m_plus"] += 1
            ex = examples.setdefault(agency, [])
            if len(ex) < 15:
                ex.append({"company": name, "rating": sym,
                           "rating_date": iso, "age_days": age})
    report["staleness"] = per_agency
    report["worst_stale_examples"] = examples

    needs = {ag: (v["aging_6_12m"] + v["dated_12_18m"] + v["stale_18m_plus"]
                  + v["undated_or_unparseable"])
             for ag, v in per_agency.items()}
    report["hygiene"]["needs_reverification_by_agency"] = needs
    report["hygiene"]["needs_reverification_total"] = sum(needs.values())

    # ---- cross-agency coverage ----------------------------------------------
    by_company = {}
    for _, cid, agency, _, grade, _, _ in rows:
        if grade is not None:
            by_company.setdefault(cid, set()).add(agency)
    dist = {}
    for agencies in by_company.values():
        dist[len(agencies)] = dist.get(len(agencies), 0) + 1
    report["coverage"]["graded_companies_by_agency_count"] = {
        str(k): v for k, v in sorted(dist.items())}

    # ---- CRISIL index vs DB --------------------------------------------------
    try:
        idx_path = ROOT / "data" / "crisil_index.json"
        if idx_path.exists():
            idx = json.loads(idx_path.read_text(encoding="utf-8")).get("index", {})
            db_crisil = cur.execute(
                "SELECT COUNT(DISTINCT company_id) FROM ratings WHERE agency='CRISIL'"
            ).fetchone()[0]
            report["coverage"]["crisil"] = {
                "index_entries": len(idx), "db_companies": db_crisil}
    except Exception as exc:
        report["coverage"]["crisil"] = {"error": str(exc)[:200]}

    # ---- India Ratings ceiling -------------------------------------------------
    try:
        from scrapers.india_ratings import MAX_ISSUER_ID
        max_seen = 0
        for (url,) in cur.execute(
                "SELECT DISTINCT rationale_url FROM ratings "
                "WHERE agency='India Ratings' AND rationale_url LIKE '%issuerID=%'"):
            m = re.search(r"issuerID=(\d+)", url or "")
            if m:
                max_seen = max(max_seen, int(m.group(1)))
        report["coverage"]["india_ratings"] = {
            "max_issuer_id_seen": max_seen, "scan_ceiling": MAX_ISSUER_ID,
            "ceiling_pressure": bool(max_seen >= MAX_ISSUER_ID - 500),
        }
    except Exception as exc:
        report["coverage"]["india_ratings"] = {"error": str(exc)[:200]}

    # ---- CARE discover rotation state -----------------------------------------
    try:
        ckpt = ROOT / "data" / "care_discover_checkpoint.txt"
        report["coverage"]["care_edge"] = {
            "db_companies": cur.execute(
                "SELECT COUNT(DISTINCT company_id) FROM ratings WHERE agency='CARE Edge'"
            ).fetchone()[0],
            "discover_prefix_checkpoint": ckpt.read_text().strip() if ckpt.exists() else None,
        }
    except Exception as exc:
        report["coverage"]["care_edge"] = {"error": str(exc)[:200]}

    # ---- ICRA universe (network) ------------------------------------------------
    if network:
        try:
            from scrapers.icra import _build_session, _get_all_icra_ids, _load_known_icra_ids
            session = _build_session()
            universe = _get_all_icra_ids(session)
            known = _load_known_icra_ids(conn)
            missing = {cid: nm for cid, nm in universe.items() if cid not in known}
            report["coverage"]["icra"] = {
                "universe_companies": len(universe),
                "db_companies_with_icra_id": len(known),
                "missing_from_db": len(missing),
                "missing_sample": sorted(missing.values())[:25],
            }
        except Exception as exc:
            report["coverage"]["icra"] = {"error": str(exc)[:200]}

    # ---- NSDL active-issuer gaps (from build_isin_map step) ----------------------
    try:
        mi = ROOT / "data" / "missing_issuers.csv"
        if mi.exists():
            lines = mi.read_text(encoding="utf-8").splitlines()
            report["coverage"]["nsdl_missing_active_issuers"] = {
                "count": max(len(lines) - 1, 0), "sample": lines[1:16]}
    except Exception as exc:
        report["coverage"]["nsdl_missing_active_issuers"] = {"error": str(exc)[:200]}

    return report


def write_md(report: dict) -> str:
    t = report["totals"]
    lines = [
        "# Ratings data audit",
        "",
        f"Generated {report['generated_at']} by the fortnightly refresh. "
        f"Staleness bar: {report['staleness_bar_days']} days "
        "(ratings older than this are queued for re-verification; agencies "
        "formally review annually, so 6-12m is \"aging\", not necessarily wrong).",
        "",
        f"Companies: {t['companies']} total, {t['rated_companies']} rated, "
        f"{t['companies_with_graded_current']} with at least one graded current "
        f"rating ({t['companies_with_no_graded_current']} rated but nothing "
        f"graded-current). Current (company, agency) rows: {t['current_rows']}.",
        "",
        "## Staleness of current ratings, by agency",
        "",
        "| Agency | Companies | <=6m | 6-12m | 12-18m | >18m | Undated | Withdrawn-as-current | Not cooperating | Ungraded |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for ag in sorted(report["staleness"]):
        v = report["staleness"][ag]
        lines.append(
            f"| {ag} | {v['companies']} | {v['fresh_0_6m']} | {v['aging_6_12m']} "
            f"| {v['dated_12_18m']} | {v['stale_18m_plus']} "
            f"| {v['undated_or_unparseable']} | {v['withdrawn_as_current']} "
            f"| {v['not_cooperating']} | {v['ungraded_current']} |")
    lines += [
        "",
        f"Needs re-verification (older than 6m or undated): "
        f"{report['hygiene']['needs_reverification_total']} "
        f"({', '.join(f'{a}: {n}' for a, n in sorted(report['hygiene']['needs_reverification_by_agency'].items()))}).",
        "",
        "## Coverage",
        "",
    ]
    cov = report["coverage"]
    dist = cov.get("graded_companies_by_agency_count", {})
    if dist:
        lines.append("Graded companies by number of agencies covering them: "
                     + ", ".join(f"{k} agency(ies): {v}" for k, v in dist.items()) + ".")
    ic = cov.get("icra")
    if ic and "error" not in ic:
        lines.append(f"ICRA: universe {ic['universe_companies']}, in DB "
                     f"{ic['db_companies_with_icra_id']}, missing "
                     f"{ic['missing_from_db']}.")
    cr = cov.get("crisil")
    if cr and "error" not in cr:
        lines.append(f"CRISIL: index entries {cr['index_entries']}, DB companies "
                     f"{cr['db_companies']}.")
    ir = cov.get("india_ratings")
    if ir and "error" not in ir:
        lines.append(f"India Ratings: max issuer ID seen {ir['max_issuer_id_seen']} "
                     f"of scan ceiling {ir['scan_ceiling']}"
                     + (" — CEILING PRESSURE, raise MAX_ISSUER_ID." if ir["ceiling_pressure"] else "."))
    ce = cov.get("care_edge")
    if ce and "error" not in ce:
        lines.append(f"CARE Edge: DB companies {ce['db_companies']}, discover "
                     f"rotation checkpoint {ce.get('discover_prefix_checkpoint')}.")
    nm = cov.get("nsdl_missing_active_issuers")
    if nm and "error" not in nm:
        lines.append(f"NSDL active bond issuers with no ratings-DB match: "
                     f"{nm['count']} (full list: data/missing_issuers.csv).")
    lines += ["", "## Worst stale examples (>18m), per agency", ""]
    for ag in sorted(report.get("worst_stale_examples", {})):
        lines.append(f"**{ag}**")
        lines.append("")
        for ex in report["worst_stale_examples"][ag]:
            lines.append(f"- {ex['company']} — {ex['rating']} "
                         f"({ex['rating_date']}, {ex['age_days']}d)")
        lines.append("")
    return "\n".join(lines) + "\n"


def stage_outputs():
    """git add the audit outputs so the workflow's commit step publishes them.
    Never commits or pushes. Silently a no-op outside a git checkout."""
    try:
        subprocess.run(
            ["git", "-C", str(ROOT), "add",
             "data/audit_report.json", "data/AUDIT.md"],
            capture_output=True, text=True, timeout=30)
    except Exception:
        pass


def run_audit(network: bool = True) -> dict:
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row   # scrapers.icra._load_known_icra_ids needs it
    try:
        report = collect(conn, network=network)
    finally:
        conn.close()
    OUT_JSON.write_text(json.dumps(report, indent=1), encoding="utf-8")
    OUT_MD.write_text(write_md(report), encoding="utf-8")
    stage_outputs()
    print(f"audit: wrote {OUT_JSON.name} and {OUT_MD.name} — "
          f"{report['hygiene']['needs_reverification_total']} current ratings "
          "need re-verification (>6m or undated)")
    return report


if __name__ == "__main__":
    if not DB.exists():
        sys.exit(f"ERROR: {DB} not found — run from the ratings-tool repo root.")
    sys.path.insert(0, str(ROOT))
    run_audit(network="--no-network" not in sys.argv)
