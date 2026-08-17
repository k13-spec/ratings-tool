"""
Streamlit UI for the Indian Credit Ratings Tool.

Run with:
    streamlit run app.py --server.headless true --browser.gatherUsageStats false
"""

import re
import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
import urllib.parse

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH    = PROJECT_ROOT / "data" / "ratings.db"
NOTES_PATH = PROJECT_ROOT / "data" / "notes.json"  # legacy; migrated into DB on first load

# ------------------------------------------------------------------ #
# Sector grouping                                                      #
# ------------------------------------------------------------------ #
_SECTOR_GROUPS = {
    "Corporate": [
        # Basic industrials / materials
        "Manufacturing", "Chemicals", "Chemicals & Petrochemicals",
        "Specialty Chemicals", "Commodity Chemicals", "Basic Chemicals",
        "Fertilizers & Agrochemicals", "Petrochemicals", "Petroleum Products",
        "Consumable Fuels", "Carbon Black", "Dyes And Pigments", "Explosives",
        "Metals", "Ferrous Metals", "Diversified Metals", "Metals & Minerals Trading",
        "Minerals & Mining", "Mining", "Non - Ferrous Metals",
        "Iron & Steel", "Iron & Steel Products", "Sponge Iron",
        "Ferro & Silica Manganese", "Aluminium, Copper & Zinc Products",
        "Precious Metals", "Copper", "Zinc",
        "Cement", "Cement & Cement Products", "Other Construction Materials",
        "Refractories", "Glass - Industrial", "Castings & Forgings",
        "Paper", "Paper & Paper Products", "Paper, Forest & Jute Products",
        "Forest Products", "Printing", "Printing & Publication",
        # Auto & engineering
        "Auto", "Auto Components", "Automobiles",
        "Agricultural, Commercial & Construction Vehicles",
        "Passenger Cars & Utility Vehicles", "Commercial Vehicles",
        "2/3 Wheelers", "Tractors", "Trading - Automobiles",
        "Tyres & Rubber Products", "Batteries - Automobile",
        "Bearings", "Compressors & Pumps", "Electrodes",
        "Heavy Electrical Equipment", "Electrical Equipment", "Other Electrical Equipment",
        "Industrial Electronics", "Industrial Equipments", "Industrial Machinery",
        "Industrial Products", "Industrial Manufacturing",
        "Engineering & Construction products", "Engineering, Designing & Construction",
        "Other Industrial Products", "Packaging", "Industrial Gas", "Industrial Gases",
        "Aerospace & Defense", "Industrial",
        # FMCG / consumer
        "FMCG", "Diversified FMCG", "Fast Moving Consumer Goods",
        "Consumer Durables", "Consumer Goods including FMGC",
        "Consumer Electronics", "Household Appliances", "Household Products",
        "Personal Care", "Personal Products", "Houseware",
        "Furniture, Home Furnishing, Flooring", "Plastic Products - Consumer",
        "Gems, Jewellery And Watches", "Leather And Leather Products",
        "Toys", "Diversified Consumer Products",
        # Food & beverages
        "Food & Beverages", "Food Products", "Beverages",
        "Agricultural Food & other Products", "Other Agricultural Products",
        "Agriculture", "Dairy Products", "Packaged Foods", "Other Food Products",
        "Edible Oil", "Sugar", "Tea & Coffee", "Other Beverages",
        "Breweries & Distilleries", "Cigarettes & Tobacco Products",
        "Animal Feed",
        # Healthcare & pharma
        "Healthcare", "Pharmaceuticals", "Pharmaceuticals & Biotechnology",
        "Healthcare Services", "Healthcare Equipment & Supplies",
        "Medical Equipment & Supplies", "Hospital",
        "Healthcare Research, Analytics & Technology", "Biotechnology",
        "Pharmacy Retail", "Wellness",
        # Technology & media
        "Technology", "Information Technology",
        "IT Services", "IT - Services", "IT - Software", "IT - Hardware",
        "IT Enabled Services", "Computers - Software & Consulting",
        "Computers Hardware & Equipments", "Software Products",
        "Data Processing Services", "E-Learning", "Business Process Outsourcing (BPO) / Knowledge Process Outsourcing (KPO)",
        "Digital Entertainment", "Media", "Media & Entertainment",
        "Entertainment", "Film Production, Distribution & Exhibition",
        "TV Broadcasting & Software Production", "Electronic Media",
        "Advertising & Media Agencies",
        # Retail, consumer services, education
        "Retail", "Retailing", "Diversified Retail",
        "Speciality Retail", "E-Retail/ E- Commerce", "Pharmacy Retail",
        "Distributors", "Trading & Distributors",
        "Education", "Food Storage Facilities",
        "Other Consumer Services", "Consumer Services",
        "Leisure Services", "Hotels", "Hotels & Resorts",
        "Restaurants", "Amusement Parks/ Other Recreation",
        "Tour, Travel Related Services",
        # Construction & real estate
        "Construction", "Civil Construction",
        "Real Estate", "Realty", "Residential, Commercial Projects",
        "Real Estate Investment Trusts (REITs)", "Real Estate related services",
        # Textile
        "Textile", "Textiles & Apparels",
        "Garments & Apparels", "Other Textile Products",
        "Other Textiles", "Cotton Textiles - Composite",
        "Trading - Textile Products",
        # Other trading
        "Trading", "Trading - Chemicals", "Trading - Gas",
        "Trading - Metals", "Trading - Minerals",
        # Misc
        "Consumer Discretionary", "Services", "Diversified",
        "Multi-Product Companies", "Holding Company",
        "Commodities", "Basic Materials", "Industrials",
        "Rubber And Plastics Products", "Plastic Products - Industrial",
        "Plywood Boards/ Laminates", "Packaging",
        "Printing & Publication",
        "Not Mapped", "",
    ],
    "Infrastructure": [
        "Infrastructure", "Transport Infrastructure", "Transport Services",
        "Renewable Energy", "Renewables", "Solar", "Wind", "Green Energy",
        "Power - Renewable", "Energy",
        "Other Utilities", "Public Services", "Gas",
        "Gas Transmission/ Marketing", "LPG/CNG/PN G/LNG Supplier",
        "Industrial Gas", "Trading - Gas",
        "Power", "Power Trading", "Power - Transmission",
        "Electric Utilities", "Electricity Generation", "Utilities",
        "Oil", "Oil Exploration & Production", "Oil Storage & Transportation",
        "Refineries & Marketing", "Petro Products",
        "Airport & Airport services", "Airline",
        "Railways", "Road Transport", "Shipping",
        "Port & Port services", "Dredging",
        "Logistics Solution Provider", "Transport Related Services",
        "Toll bridge operator",
        "Waste Management", "Water Supply & Management", "Multi Utilities",
        "Telecom", "Telecom - Services", "Telecom - Equipment & Accessories",
        "Telecom - Cellular & Fixed line services",
        "Telecom - Infrastructure", "Other Telecom Services",
        "Telecommunication",
    ],
    "Financial Institutions": [
        "Financial Sector", "Financial Services",
        "Banks", "Private Sector Bank", "Public Sector Bank", "Other Bank",
        "Capital Markets", "Finance", "Insurance",
        "Financial Institution",
        "Financial Technology (Fintech)", "Financial Technology", "Fintech",
        "Non-Banking Financial Company (NBFC)", "Non-Banking Financial Company",
        "NBFC", "Housing Finance Company", "Housing Finance",
        "Investment Company", "Other Financial Services",
        "Life Insurance", "General Insurance", "Other Insurance Companies",
        "Insurance Distributors",
        "Asset Management Company", "Stockbroking & Allied",
        "Depositories, Clearing Houses and Other Intermediaries",
        "Other Capital Market related Services",
    ],
}

def _group_of(sector: str) -> str:
    for grp, members in _SECTOR_GROUPS.items():
        if sector in members:
            return grp
    return "Corporate"


# ------------------------------------------------------------------ #
# PSU / sovereign detection (name-based heuristic)                    #
# ------------------------------------------------------------------ #
_PSU_FRAGMENTS = [
    "ntpc ", "bhel ", " sail ", "ongc", "iocl", "gail ",
    "nalco", "nmdc", "nhpc", "npcil", "powergrid", "seci ",
    "irfc", "nhai ", "hudco", "sidbi", "nabard",
    "coal india", "indian oil", "bharat petroleum",
    "hindustan petroleum", "oil and natural gas",
    "gas authority", "steel authority of india",
    "national aluminium", "national mineral development",
    "national thermal power", "national highways authority",
    "national fertilizers",
    "bharat heavy electricals", "bharat electronics",
    "bharat dynamics", "hindustan aeronautics",
    "hindustan copper", "mazagon dock",
    "garden reach ship", "goa shipyard",
    "rites ", "ircon", "nbcc ", "moil ", "mtnl", "bsnl",
    "balmer lawrie", "mmtc", "mecon", "engineers india",
    "rashtriya chemicals",
    "state bank of india", "punjab national bank",
    "bank of baroda", "bank of india", "bank of maharashtra",
    "canara bank", "union bank of india", "central bank of india",
    "indian bank ", "uco bank", "jammu and kashmir bank",
    "life insurance corporation",
    "power finance corp", "rural electrification corp",
    "housing and urban development", "national bank for agriculture",
    "export import bank of india", "exim bank",
    "rec limited", "pfc limited",
    "food corporation of india",
    "oil india", "mrpl", "bpcl", "hpcl",
    # verified against NSDL's Type of Issuer-Ownership field, 2026-08-05
    "power grid", "indian railway finance", "india infrastructure finance",
    "indian renewable energy development", "nuclear power corporation",
    "national housing bank", "financing infrastructure and development",
    "thdc ", "bharat sanchar", "mahanagar telephone", "pnb housing",
    "solar energy corporation",
    # state-government entities: discoms/transcos, state FIs, civic bodies
    "power corporation", "energy corporation", "electricity board",
    "state electricity", "rajya vidyut", "prasaran nigam", "vidyut nigam",
    "power distribution company", "power generation co",
    "municipal corporation", "nagar nigam",
    "metropolitan development authority", "capital region development",
    "infrastructure development board", "state beverages",
    "kerala financial corporation", "kerala infrastructure investment",
    "mineral development corporation", "industrial infrastructure corporation",
]

def _is_psu(name) -> bool:
    # NaN / None / non-string company names must not crash the filter
    if not isinstance(name, str):
        return False
    n = (" " + name.lower() + " ")
    return any(frag in n for frag in _PSU_FRAGMENTS)


# ------------------------------------------------------------------ #
# Notes persistence                                                    #
# ------------------------------------------------------------------ #
def _ensure_notes_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            company_id INTEGER PRIMARY KEY REFERENCES companies(id),
            note TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def _load_notes() -> dict:
    if not DB_PATH.exists():
        return {}
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(str(DB_PATH))
    try:
        _ensure_notes_table(conn)
        rows = conn.execute("SELECT company_id, note FROM notes").fetchall()
        notes = {str(r[0]): r[1] for r in rows}
        # One-time migration from legacy notes.json
        if not notes and NOTES_PATH.exists():
            try:
                legacy = json.loads(NOTES_PATH.read_text(encoding="utf-8"))
                if legacy:
                    conn.executemany(
                        "INSERT OR REPLACE INTO notes (company_id, note) VALUES (?, ?)",
                        [(int(k), v) for k, v in legacy.items() if v and v.strip()],
                    )
                    conn.commit()
                    notes = {str(k): v for k, v in legacy.items() if v and v.strip()}
            except Exception:
                pass
        return notes
    finally:
        conn.close()


def _save_notes(notes: dict):
    if not DB_PATH.exists():
        return
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(str(DB_PATH))
    try:
        _ensure_notes_table(conn)
        conn.execute("DELETE FROM notes")
        if notes:
            conn.executemany(
                "INSERT INTO notes (company_id, note, updated_at) VALUES (?, ?, datetime('now'))",
                [(int(k), v) for k, v in notes.items() if v and v.strip()],
            )
        conn.commit()
    finally:
        conn.close()


# ------------------------------------------------------------------ #
# Sector override persistence                                          #
# ------------------------------------------------------------------ #

def _save_sector_override(company_id: int, sector: str):
    """Update sector for all ratings of a company, then push DB to git for cross-instance sync."""
    if not DB_PATH.exists():
        return
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(str(DB_PATH))
    try:
        conn.execute(
            "UPDATE ratings SET sector=? WHERE company_id=?",
            (sector.strip() if sector else "", int(company_id)),
        )
        conn.commit()
    finally:
        conn.close()
    _push_db_to_git(f"Manual sector edit: company_id={company_id}")


def _push_db_to_git(message: str = "Manual edit from dashboard"):
    """Checkpoint WAL + git add/commit/push data/ratings.db for cross-instance sync."""
    import sqlite3 as _sqlite3
    from datetime import datetime as _dt
    try:
        conn = _sqlite3.connect(str(DB_PATH))
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except Exception:
        return
    try:
        def _git(*args):
            return subprocess.run(
                ["git", "-C", str(PROJECT_ROOT), *args],
                capture_output=True, text=True,
            )
        _git("add", "data/ratings.db")
        diff = _git("diff", "--cached", "--quiet")
        if diff.returncode == 0:
            return  # nothing changed
        _git("commit", "-m", f"{message} ({_dt.now().strftime('%Y-%m-%d %H:%M')})")
        _git("push", "origin", "master")
    except Exception:
        pass  # don't fail the UI if git push fails


# sorted list of all known sectors for the edit dropdown
_ALL_SECTOR_OPTIONS = [""] + sorted(
    {s for members in _SECTOR_GROUPS.values() for s in members if s}
)


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #
def _db_exists() -> bool:
    return DB_PATH.exists() and DB_PATH.stat().st_size > 0


def _get_conn():
    from database.models import get_connection, init_db
    init_db()
    return get_connection()


@st.cache_data(ttl=300, show_spinner=False)
def _cached_sectors() -> list:
    from database.queries import get_all_sectors
    conn = _get_conn()
    try:
        return get_all_sectors(conn)
    finally:
        conn.close()


@st.cache_data(ttl=300, show_spinner=False)
def _cached_agencies() -> list:
    from database.queries import get_all_agencies
    conn = _get_conn()
    try:
        return get_all_agencies(conn)
    finally:
        conn.close()


@st.cache_data(ttl=30, show_spinner=False)
def _cached_stats() -> dict:
    from database.queries import get_db_stats
    conn = _get_conn()
    try:
        return get_db_stats(conn)
    finally:
        conn.close()


@st.cache_data(ttl=300, show_spinner=False)
def _cached_query(
    min_grade, max_grade, agencies_tuple, outlooks_tuple, sectors_tuple,
    listed_only, unlisted_only, min_revenue_cr, max_revenue_cr,
    min_ebitda_cr, min_ebitda_margin_pct, max_net_debt_ebitda,
    min_total_debt_cr,
) -> pd.DataFrame:
    from database.queries import get_filtered_companies
    conn = _get_conn()
    try:
        return get_filtered_companies(
            conn,
            min_grade=min_grade,
            max_grade=max_grade,
            agencies=list(agencies_tuple) if agencies_tuple else None,
            outlooks=list(outlooks_tuple) if outlooks_tuple else None,
            sectors=list(sectors_tuple) if sectors_tuple else None,
            listed_only=listed_only,
            unlisted_only=unlisted_only,
            min_revenue_cr=min_revenue_cr or None,
            max_revenue_cr=max_revenue_cr or None,
            min_ebitda_cr=min_ebitda_cr or None,
            min_ebitda_margin_pct=min_ebitda_margin_pct or None,
            max_net_debt_ebitda=float(max_net_debt_ebitda) if max_net_debt_ebitda < 20.0 else None,
            min_total_debt_cr=min_total_debt_cr or None,
        )
    finally:
        conn.close()


def _run_scraper(flags: list) -> str:
    cmd = [sys.executable, str(PROJECT_ROOT / "run_scraper.py")] + flags
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
    return (result.stdout + ("\n" + result.stderr if result.stderr else "")).strip()


def _df_to_excel(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Ratings")
    return buf.getvalue()


# ------------------------------------------------------------------ #
# Rating grade colour helpers                                          #
# ------------------------------------------------------------------ #
_GRADE_COLOUR_MAP = {
    range(1, 3):   "#1a7a1a",
    range(3, 5):   "#2e9e2e",
    range(5, 8):   "#f0c040",
    range(8, 11):  "#e08020",
    range(11, 14): "#c04020",
    range(14, 17): "#a02010",
    range(17, 21): "#6b0000",
}

def _grade_color(grade) -> str:
    if grade is None:
        return "#888888"
    try:
        g = int(grade)
    except Exception:
        return "#888888"
    for r, color in _GRADE_COLOUR_MAP.items():
        if g in r:
            return color
    return "#888888"


# ------------------------------------------------------------------ #
# Sidebar: sector checkbox panel                                       #
# ------------------------------------------------------------------ #
def _is_valid_sector(s: str) -> bool:
    """Return False for sentence fragments or obvious scrape artifacts."""
    if not s or len(s) > 80:
        return False
    # Reject if it looks like a sentence (ends with full stop or contains common prose words)
    prose_markers = (" the ", " in the ", " of the ", " is ", " are ", " have ", " has ",
                     " remains ", " which ", " further ", " especially ", " among ")
    sl = s.lower()
    if any(m in sl for m in prose_markers):
        return False
    if s.endswith("."):
        return False
    return True


def _sector_checkbox_panel(available_sectors: list) -> list:
    available_sectors = [s for s in available_sectors if _is_valid_sector(s)]
    grouped: dict[str, list] = {"Corporate": [], "Infrastructure": [],
                                "Financial Institutions": []}
    for s in available_sectors:
        grouped[_group_of(s)].append(s)

    for s in available_sectors:
        wkey = f"chk_{s}"
        if wkey not in st.session_state:
            st.session_state[wkey] = True

    st.markdown("**Sectors**")

    qc1, qc2, qc3 = st.columns(3)
    if qc1.button("All",  key="sec_all",  use_container_width=True):
        for s in available_sectors:
            st.session_state[f"chk_{s}"] = True
        st.rerun()
    if qc2.button("None", key="sec_none", use_container_width=True):
        for s in available_sectors:
            st.session_state[f"chk_{s}"] = False
        st.rerun()
    if qc3.button("Corp", key="sec_corp", use_container_width=True,
                  help="Corporate sectors only"):
        for s in available_sectors:
            st.session_state[f"chk_{s}"] = (_group_of(s) == "Corporate")
        st.rerun()

    selected = []
    for grp in ["Corporate", "Infrastructure", "Financial Institutions"]:
        members = grouped.get(grp, [])
        if not members:
            continue
        with st.expander(grp, expanded=(grp == "Corporate")):
            ga1, ga2 = st.columns(2)
            if ga1.button("All",  key=f"grp_all_{grp}",  use_container_width=True):
                for s in members:
                    st.session_state[f"chk_{s}"] = True
                st.rerun()
            if ga2.button("None", key=f"grp_none_{grp}", use_container_width=True):
                for s in members:
                    st.session_state[f"chk_{s}"] = False
                st.rerun()

            for sector in sorted(members):
                label = sector if sector else "(unclassified)"
                checked = st.checkbox(label, key=f"chk_{sector}")
                if checked:
                    selected.append(sector)

    return selected




# ------------------------------------------------------------------ #
# Main app                                                             #
# ------------------------------------------------------------------ #
_CSS = """
<style>
/* ═══════════════════════════════════════════════
   Snazzy Indigo design system — DM Sans · Indigo · Frosted Glass
   ═══════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap');

/* ── Design tokens ── */
:root {
    --bg:           #F8F9FF;
    --surface:      #FFFFFF;
    --surface-glass:rgba(255,255,255,0.72);
    --border:       #E0E2EF;
    --border-soft:  #ECEEFF;
    --text:         #111827;
    --text-muted:   #6B7280;
    --accent:       #6366F1;
    --accent-hov:   #4F46E5;
    --accent-light: #EEF2FF;
    --accent-dim:   rgba(99,102,241,0.12);
    --secondary:    #F3F4F6;
    --secondary-hov:#E5E7EB;
    --shadow-xs:    0 1px 3px rgba(99,102,241,0.08);
    --shadow-sm:    0 4px 16px rgba(99,102,241,0.12);
    --shadow-md:    0 8px 32px rgba(99,102,241,0.16);
    --radius-sm:    8px;
    --radius-md:    12px;
    --radius-lg:    16px;
    --font:         'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, .stDeployButton { visibility: hidden; }

/* ── Global background & font ── */
html, body {
    background-color: var(--bg) !important;
    font-family: var(--font) !important;
    color: var(--text);
}
[data-testid="stApp"],
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
[data-testid="stMainBlockContainer"],
[data-testid="stVerticalBlock"],
.main, .main .block-container,
section.main > div,
.stMainBlockContainer {
    background-color: var(--bg) !important;
    font-family: var(--font) !important;
}
/* Markdown text */
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] span {
    color: var(--text) !important;
    font-family: var(--font) !important;
}

/* ── Top header bar ── */
[data-testid="stHeader"] {
    background-color: var(--bg) !important;
    border-bottom: 1px solid var(--border);
}

/* ═══════════════ SIDEBAR — FROSTED GLASS ═══════════════ */
[data-testid="stSidebar"],
[data-testid="stSidebarContent"],
section[data-testid="stSidebar"] > div {
    background: var(--surface-glass) !important;
    backdrop-filter: blur(24px) saturate(180%) !important;
    -webkit-backdrop-filter: blur(24px) saturate(180%) !important;
    border-right: 1px solid rgba(99,102,241,0.15) !important;
}
/* Sidebar section header */
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color: var(--accent) !important;
    font-family: var(--font) !important;
    font-size: 0.68rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
}
/* All sidebar text */
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label {
    color: var(--text) !important;
    font-family: var(--font) !important;
}
/* Sidebar widget labels */
[data-testid="stSidebar"] .stTextInput label,
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stNumberInput label,
[data-testid="stSidebar"] .stRadio label,
[data-testid="stSidebar"] .stCheckbox label {
    color: var(--text-muted) !important;
    font-size: 0.73rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.02em;
}
/* Sidebar buttons */
[data-testid="stSidebar"] [data-testid="stBaseButton-secondary"],
[data-testid="stSidebar"] button {
    border-radius: var(--radius-sm) !important;
    font-size: 0.8rem !important;
    background: rgba(255,255,255,0.8) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    font-weight: 500 !important;
    font-family: var(--font) !important;
    transition: all 0.15s ease;
}
[data-testid="stSidebar"] [data-testid="stBaseButton-secondary"]:hover,
[data-testid="stSidebar"] button:hover {
    background: white !important;
    border-color: var(--accent) !important;
    color: var(--accent) !important;
    box-shadow: 0 0 0 3px var(--accent-dim) !important;
}
/* Sidebar dividers */
[data-testid="stSidebar"] hr { border-color: var(--border-soft) !important; }

/* ═══════════════ METRIC CARDS ═══════════════ */
[data-testid="stMetric"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-lg) !important;
    padding: 1.1rem 1.4rem !important;
    box-shadow: var(--shadow-xs) !important;
    transition: box-shadow 0.2s ease, transform 0.2s ease;
    position: relative;
    overflow: hidden;
}
[data-testid="stMetric"]:hover {
    box-shadow: var(--shadow-sm) !important;
    transform: translateY(-1px);
}
[data-testid="stMetric"]::before {
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 3px; height: 100%;
    background: linear-gradient(180deg, var(--accent), #818CF8);
    border-radius: 2px 0 0 2px;
}
[data-testid="stMetricLabel"] p {
    font-size: 0.67rem !important;
    color: var(--text-muted) !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    font-family: var(--font) !important;
}
[data-testid="stMetricValue"] {
    font-size: 1.6rem !important;
    font-weight: 700 !important;
    color: var(--text) !important;
    letter-spacing: -0.025em !important;
    font-family: var(--font) !important;
}
[data-testid="stMetricDelta"] { font-size: 0.78rem; }

/* ═══════════════ EXPANDERS ═══════════════ */
/* Streamlit 1.35+ structure */
[data-testid="stExpander"],
[data-testid="stExpanderDetails"] {
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-md) !important;
    background: var(--surface) !important;
    overflow: hidden;
}
/* Legacy structure (details/summary) */
[data-testid="stExpander"] details {
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-md) !important;
    background: var(--surface) !important;
    overflow: hidden;
}
[data-testid="stExpanderToggle"],
[data-testid="stExpander"] summary {
    color: var(--text) !important;
    font-weight: 500 !important;
    font-size: 0.83rem !important;
    font-family: var(--font) !important;
    padding: 0.65rem 1rem !important;
    background: var(--surface) !important;
}
[data-testid="stExpanderToggle"]:hover,
[data-testid="stExpander"] summary:hover { background: var(--secondary) !important; }
[data-testid="stExpanderToggle"] p,
[data-testid="stExpander"] summary > span,
[data-testid="stExpander"] summary p { color: var(--text) !important; }

/* ═══════════════ DIVIDERS ═══════════════ */
hr { border-color: var(--border) !important; opacity: 1 !important; }
[data-testid="stDivider"] hr { border-color: var(--border) !important; }

/* ═══════════════ BUTTONS ═══════════════ */
/* Primary — indigo fill */
[data-testid="stBaseButton-primary"],
[data-testid="stDownloadButton"] button {
    border-radius: var(--radius-sm) !important;
    background: linear-gradient(135deg, var(--accent) 0%, #818CF8 100%) !important;
    color: #FFFFFF !important;
    border: none !important;
    font-weight: 600 !important;
    font-family: var(--font) !important;
    letter-spacing: 0.01em !important;
    box-shadow: 0 2px 8px var(--accent-dim) !important;
    transition: all 0.2s ease !important;
}
[data-testid="stBaseButton-primary"]:hover,
[data-testid="stDownloadButton"] button:hover {
    background: linear-gradient(135deg, var(--accent-hov) 0%, var(--accent) 100%) !important;
    box-shadow: 0 4px 16px rgba(99,102,241,0.35) !important;
    transform: translateY(-1px);
}
/* Secondary */
[data-testid="stBaseButton-secondary"] {
    border-radius: var(--radius-sm) !important;
    background: var(--accent-light) !important;
    color: var(--accent) !important;
    border: 1px solid rgba(99,102,241,0.25) !important;
    font-weight: 600 !important;
    font-family: var(--font) !important;
    transition: all 0.15s ease !important;
}
[data-testid="stBaseButton-secondary"]:hover {
    background: #E0E7FF !important;
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 3px var(--accent-dim) !important;
}
/* Minimal */
[data-testid="stBaseButton-minimal"],
[data-testid="stBaseButton-borderless"] {
    border-radius: var(--radius-sm) !important;
    background: transparent !important;
    color: var(--accent) !important;
    border: none !important;
    font-weight: 500 !important;
    font-family: var(--font) !important;
}
[data-testid="stBaseButton-minimal"]:hover,
[data-testid="stBaseButton-borderless"]:hover {
    background: var(--accent-light) !important;
}

/* ═══════════════ DATA TABLE ═══════════════ */
[data-testid="stDataEditor"],
[data-testid="stDataFrame"] {
    border-radius: var(--radius-lg) !important;
    border: 1px solid rgba(99,102,241,0.2) !important;
    box-shadow: var(--shadow-xs) !important;
    overflow: hidden;
    background: var(--surface) !important;
}

/* ═══════════════ INPUTS ═══════════════ */
/* Text input */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-sm) !important;
    color: var(--text) !important;
    font-family: var(--font) !important;
    font-size: 0.85rem !important;
    transition: border-color 0.15s ease, box-shadow 0.15s ease;
}
[data-testid="stTextInput"] input:focus,
[data-testid="stNumberInput"] input:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 3px var(--accent-dim) !important;
    outline: none;
}
/* Selectbox */
[data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child,
[data-testid="stMultiSelect"] [data-baseweb="select"] > div:first-child {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-sm) !important;
    font-family: var(--font) !important;
    font-size: 0.85rem !important;
}
/* Dropdown menus */
[data-baseweb="popover"] ul,
[data-baseweb="menu"],
[data-baseweb="popover"] [data-baseweb="menu-item"] {
    background: var(--surface) !important;
    font-family: var(--font) !important;
    font-size: 0.83rem !important;
    color: var(--text) !important;
}
[data-baseweb="option"]:hover,
[role="option"]:hover {
    background: var(--secondary) !important;
}
/* Multiselect tags — indigo pill */
[data-baseweb="tag"] {
    background: linear-gradient(135deg, var(--accent), #818CF8) !important;
    border-radius: 20px !important;
    border: none !important;
}
[data-baseweb="tag"] span { color: #FFFFFF !important; font-size: 0.78rem !important; font-weight: 500 !important; }

/* ═══════════════ SLIDERS ═══════════════ */
[data-testid="stSlider"] label,
[data-testid="stSlider"] p {
    color: var(--text-muted) !important;
    font-size: 0.75rem !important;
    font-weight: 500 !important;
}
[data-testid="stSlider"] [data-testid="stTickBarMin"],
[data-testid="stSlider"] [data-testid="stTickBarMax"] {
    color: var(--text-muted) !important;
    font-size: 0.7rem !important;
}

/* ═══════════════ CHECKBOXES & RADIOS ═══════════════ */
[data-testid="stCheckbox"] label p,
[data-testid="stCheckbox"] span {
    color: var(--text) !important;
    font-size: 0.83rem !important;
    font-family: var(--font) !important;
}
[data-testid="stRadio"] label p,
[data-testid="stRadio"] > div > label {
    color: var(--text) !important;
    font-size: 0.83rem !important;
    font-family: var(--font) !important;
}
[data-testid="stRadio"] > div > label > div {
    color: var(--text) !important;
}

/* ═══════════════ TOGGLE ═══════════════ */
[data-testid="stToggle"] label,
[data-testid="stToggle"] p {
    color: var(--text) !important;
    font-size: 0.83rem !important;
    font-family: var(--font) !important;
}

/* ═══════════════ TYPOGRAPHY ═══════════════ */
h1, h2, h3, h4 {
    color: var(--text) !important;
    font-family: var(--font) !important;
    font-weight: 700 !important;
    letter-spacing: -0.025em !important;
}
h1 { font-size: 1.65rem !important; line-height: 1.25; }
h2 { font-size: 1.15rem !important; }
h3 { font-size: 0.97rem !important; }
p, li { font-family: var(--font) !important; }

/* Caption / muted text */
[data-testid="stCaptionContainer"] p,
.stCaption p, small {
    color: var(--text-muted) !important;
    font-size: 0.76rem !important;
    font-family: var(--font) !important;
}

/* ═══════════════ ALERTS / INFO ═══════════════ */
[data-testid="stAlert"],
[data-testid="stNotification"],
[data-testid="stAlertContentInfo"],
[data-testid="stAlertContentWarning"],
[data-testid="stAlertContentError"],
[data-testid="stAlertContentSuccess"] {
    border-radius: var(--radius-md) !important;
    font-family: var(--font) !important;
}
[data-testid="stAlert"] p,
[data-testid="stAlertContentInfo"] p { color: var(--text) !important; }

/* ═══════════════ SUBHEADER ═══════════════ */
[data-testid="stSubheader"] h2,
[data-testid="stSubheader"] p {
    color: var(--text) !important;
    font-family: var(--font) !important;
    font-weight: 600 !important;
    font-size: 1.0rem !important;
    letter-spacing: -0.015em !important;
}

/* ═══════════════ SPINNER ═══════════════ */
.stSpinner > div { border-top-color: var(--primary) !important; }

/* ═══════════════ CODE BLOCKS ═══════════════ */
[data-testid="stCode"],
.stCode { background: var(--secondary) !important; border: 1px solid var(--border) !important; border-radius: var(--radius-sm) !important; }

/* ═══════════════ TOAST ═══════════════ */
[data-testid="stToast"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-md) !important;
    color: var(--text) !important;
    box-shadow: var(--shadow-sm) !important;
    font-family: var(--font) !important;
}

/* ═══════════════ CHART ═══════════════ */
[data-testid="stVegaLiteChart"] { border-radius: var(--radius-md); }

/* ═══════════════ SORT ROW ═══════════════ */
div[data-testid="stHorizontalBlock"] [data-testid="stSelectbox"] label,
div[data-testid="stHorizontalBlock"] [data-testid="stToggle"] label {
    font-size: 0.78rem !important;
    color: var(--text-muted) !important;
}

/* ═══════════════ MATERIAL ICON GLYPHS ═══════════════
   Streamlit renders expander arrows / widget icons via the Material Symbols
   ligature font. The DM Sans overrides above must NOT apply to them, or the
   ligature text ("keyboard_arrow_right", …) renders as literal characters
   overlapping the labels. */
[data-testid="stIconMaterial"],
span[data-testid="stIconMaterial"],
[data-testid="stExpanderToggleIcon"],
span[class*="material-symbols"],
i[class*="material-symbols"] {
    font-family: "Material Symbols Rounded" !important;
    font-weight: normal !important;
    letter-spacing: normal !important;
    text-transform: none !important;
}
</style>
"""




# ------------------------------------------------------------------ #
# NSDL bond overlay helpers (cached, optional)                       #
# ------------------------------------------------------------------ #
_CO_STOP = {"limited", "ltd", "private", "pvt", "llp",
           "corporation", "corp", "inc", "co", "bank",
           "finance", "financial", "india", "and"}


def _normalize_co(name: str) -> str:
    # Strip common legal suffixes so 'HDFC Bank Ltd' matches 'HDFC BANK LIMITED'
    words = [w.strip(".,&") for w in name.lower().split()]
    return " ".join(w for w in words if w not in _CO_STOP).strip()


@st.cache_data(ttl=86400, show_spinner=False)
def _load_nsdl_bonds():
    # Download active bond list from NSDL. Cached 24h.
    import time as _time, warnings as _warnings, openpyxl, requests
    try:
        hdrs = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.indiabondinfo.nsdl.com/CBDServices/",
        }
        s = requests.Session()
        s.headers.update(hdrs)
        s.get("https://www.indiabondinfo.nsdl.com/CBDServices/", timeout=15)
        _time.sleep(0.3)
        r = s.get(
            "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo"
            "/listofsecurities?type=Active",
            timeout=120, stream=True,
        )
        r.raise_for_status()
        _warnings.filterwarnings("ignore", category=UserWarning)
        wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
        ws = wb.active
        all_rows = list(ws.iter_rows(values_only=True))
        col = {h: i for i, h in enumerate(all_rows[0]) if h}
        today = pd.Timestamp.today().normalize()
        records = []
        for row in all_rows[1:]:
            if not row:
                continue
            issuer = row[col.get("Name of Issuer", 1)]
            mat_raw = row[col.get("Date of Redemption/Conversion", 0)]
            if not issuer or not mat_raw:
                continue
            issuer_str = str(issuer).strip()
            if not issuer_str:
                continue
            try:
                mat = pd.Timestamp(str(mat_raw).strip().replace("/", "-"))
            except Exception:
                continue
            if mat < today:
                continue
            records.append({
                "Issuer": issuer_str,
                "Issuer_norm": _normalize_co(issuer_str),
                "Days": int((mat - today).days),
            })
        return pd.DataFrame(records)
    except Exception:
        return pd.DataFrame(columns=["Issuer", "Issuer_norm", "Days"])


def _add_bond_counts(display_df, bonds_df):
    # Append Bonds 30d / 90d / 1yr columns to display_df.
    display_df = display_df.copy()
    zeros = [0] * len(display_df)
    if bonds_df.empty:
        display_df["Bonds 30d"] = zeros
        display_df["Bonds 90d"] = zeros
        display_df["Bonds 1yr"] = zeros
        display_df["Bonds total"] = zeros
        return display_df
    issuer_norms = bonds_df["Issuer_norm"].values
    days_arr = bonds_df["Days"].values
    b30, b90, b365, b_total = [], [], [], []
    for cname in display_df["Company Name"]:
        key = _normalize_co(str(cname))
        if len(key) < 4:
            b30.append(0); b90.append(0); b365.append(0); b_total.append(0)
            continue
        key_short = key[:18]
        idx = [i for i, n in enumerate(issuer_norms) if key_short in n]
        d = days_arr[idx] if idx else []
        import numpy as np
        d = np.array(d)
        b30.append(int((d <= 30).sum()))
        b90.append(int((d <= 90).sum()))
        b365.append(int((d <= 365).sum()))
        b_total.append(len(idx))
    display_df["Bonds 30d"] = b30
    display_df["Bonds 90d"] = b90
    display_df["Bonds 1yr"] = b365
    display_df["Bonds total"] = b_total
    return display_df

@st.cache_data(ttl=3600, show_spinner=False)
def _recent_rating_actions(days: int = 7):
    """Upgrades / downgrades in the trailing `days` window ending at the most
    recent dated rating in the DB. The window is data-relative (not calendar)
    because scrapes are fortnightly — a calendar week would read empty just
    before each refresh. Compares each (company, agency)'s latest dated+graded
    rating with its previous one on the normalized 1-20 grade scale
    (lower = better). Dates are parsed per-unique-value: formats are mixed
    (ISO + "Month D, YYYY") and pandas 3.0 rejects vectorised mixed parsing.
    """
    import sqlite3 as _sq
    conn = _sq.connect(str(DB_PATH))
    try:
        raw = pd.read_sql_query(
            """SELECT r.id, r.company_id, c.name, r.agency, r.rating_symbol,
                      r.rating_grade, r.rating_date, r.rationale_url
               FROM ratings r JOIN companies c ON c.id = r.company_id
               WHERE r.rating_grade IS NOT NULL
                 AND r.rating_date IS NOT NULL""",
            conn)
    finally:
        conn.close()
    if raw.empty:
        return [], None, None
    _dcache = {}
    def _pdate(s):
        if s not in _dcache:
            try:
                _ts = pd.to_datetime(s, errors="coerce")
                if _ts is not pd.NaT and getattr(_ts, "tzinfo", None) is not None:
                    _ts = _ts.tz_localize(None)
                _dcache[s] = _ts
            except (ValueError, TypeError):
                _dcache[s] = pd.NaT
        return _dcache[s]
    raw["dt"] = pd.Series([_pdate(s) for s in raw["rating_date"]], index=raw.index)
    raw = raw.dropna(subset=["dt"])
    if raw.empty:
        return [], None, None
    end = raw["dt"].max()
    start = end - pd.Timedelta(days=days - 1)
    actions = []
    for (_cid, _agency), g in raw.groupby(["company_id", "agency"]):
        g = g.sort_values(["dt", "id"])
        cur = g.iloc[-1]
        if cur["dt"] < start:
            continue
        prev_rows = g[g["dt"] < cur["dt"]]
        if prev_rows.empty:
            continue
        prev = prev_rows.iloc[-1]
        if int(cur["rating_grade"]) == int(prev["rating_grade"]):
            continue
        actions.append({
            "company": str(cur["name"]),
            "agency":  str(_agency),
            "from":    _fmt_action_symbol(prev["rating_symbol"]),
            "to":      _fmt_action_symbol(cur["rating_symbol"]),
            "up":      int(cur["rating_grade"]) < int(prev["rating_grade"]),
            "date":    cur["dt"],
            "url":     str(cur["rationale_url"] or ""),
        })
    actions.sort(key=lambda a: a["date"], reverse=True)
    return actions, start, end


def _fmt_action_symbol(sym) -> str:
    """Compact a raw agency symbol for the actions strip: drop empty '--'
    components of combined LT,ST symbols and shorten the INC boilerplate."""
    s = str(sym or "").strip()
    parts = [p.strip() for p in s.split(",") if p.strip() and p.strip() != "--"]
    s = ", ".join(parts) if parts else s
    s = re.sub(r"\s*ISSUER NOT COOPERATING\s*", " (INC)", s, flags=re.I)
    return re.sub(r"\s{2,}", " ", s).strip()


def main():
    st.set_page_config(
        page_title="Indian Credit Ratings",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(_CSS, unsafe_allow_html=True)
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #6366F1 0%, #818CF8 50%, #A5B4FC 100%);
            border-radius: 16px;
            padding: 28px 32px 22px 32px;
            margin-bottom: 8px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            box-shadow: 0 8px 32px rgba(99,102,241,0.22);
        ">
            <div>
                <div style="font-family:'DM Sans',sans-serif;font-size:1.65rem;font-weight:700;
                            color:#FFFFFF;letter-spacing:-0.025em;line-height:1.2;margin-bottom:6px;">
                    Indian Credit Ratings Dashboard
                </div>
                <div style="font-family:'DM Sans',sans-serif;font-size:0.83rem;font-weight:400;
                            color:rgba(255,255,255,0.8);letter-spacing:0.01em;">
                    ICRA &middot; CRISIL &middot; CARE Edge &middot; India Ratings &nbsp;&nbsp;|&nbsp;&nbsp;
                    Financials from NSE / yfinance &amp; CRISIL
                </div>
            </div>
            <div>
                <a href="https://creditnexus.streamlit.app/Financing_Ideas" target="_self"
                   style="font-family:'DM Sans',sans-serif;font-size:13px;font-weight:600;
                          color:#6366F1;background:#FFFFFF;border-radius:8px;
                          padding:7px 16px;text-decoration:none;white-space:nowrap;
                          box-shadow:0 2px 8px rgba(0,0,0,0.12);display:inline-block;
                          margin-right:8px;">
                    💡 Financing Ideas
                </a><a href="https://creditnexus-bonds.streamlit.app" target="_blank"
                   style="font-family:'DM Sans',sans-serif;font-size:13px;font-weight:600;
                          color:#6366F1;background:#FFFFFF;border-radius:8px;
                          padding:7px 16px;text-decoration:none;white-space:nowrap;
                          box-shadow:0 2px 8px rgba(0,0,0,0.12);display:inline-block;">
                    ↗ Bond Tracker
                </a>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Load notes into session state once
    if "notes" not in st.session_state:
        st.session_state.notes = _load_notes()

    if not _db_exists():
        st.warning("Database not found. Run the scrapers to populate data.")
        with st.sidebar:
            st.header("Run Scrapers")
            if st.button("Run ICRA (test, 50 records)", type="primary"):
                with st.spinner("Running ICRA scraper..."):
                    out = _run_scraper(["--icra", "--limit", "50"])
                st.code(out)
                st.rerun()
        return

    # --------------------------------------------------------- #
    # Sidebar                                                    #
    # --------------------------------------------------------- #
    with st.sidebar:
        st.header("Filters")

        if st.button("↺  Refresh Data", type="secondary", use_container_width=True):
            _cached_sectors.clear()
            _cached_agencies.clear()
            _cached_stats.clear()
            _cached_query.clear()
            st.rerun()

        st.divider()

        # ---- Scraper controls ----
        with st.expander("▶  Run Scrapers", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                run_icra   = st.button("ICRA",   use_container_width=True)
                run_nse    = st.button("NSE",    use_container_width=True)
            with col2:
                run_crisil = st.button("CRISIL", use_container_width=True)
                run_all    = st.button("All",    use_container_width=True, type="primary")
            run_crisil_fin = st.button(
                "CRISIL Financials", use_container_width=True,
                help="Fetch Key Financial Indicators from CRISIL rationale HTML pages"
            )

            for flag, label, spinner_label in [
                (run_icra,       ["--icra"],              "ICRA ratings..."),
                (run_crisil,     ["--crisil"],            "CRISIL ratings..."),
                (run_nse,        ["--nse"],               "NSE/yfinance financials..."),
                (run_crisil_fin, ["--crisil-financials"], "CRISIL HTML financials..."),
                (run_all,        ["--all"],               "All scrapers (this may take a while)..."),
            ]:
                if flag:
                    with st.spinner(f"Running {spinner_label}"):
                        out = _run_scraper(label)
                    st.code(out[:2000])
                    _cached_stats.clear()
                    _cached_query.clear()

        st.divider()

        # Pre-fill from deep-link: ?company=Tata
        _qp_company = st.query_params.get("company", "")
        # ---- Company Name Search ----
        company_search = st.text_input(
            "Search Company Name",
            placeholder="e.g. Tata, Reliance…",
            value=_qp_company,
        ).strip()

        st.divider()

        # ---- Rating Agency ----
        available_agencies = _cached_agencies()
        selected_agencies = st.multiselect(
            "Rating Agency",
            options=available_agencies or ["ICRA", "CRISIL"],
            default=available_agencies or ["ICRA", "CRISIL"],
        )

        # ---- Rating Grade ----
        grade_options = {
            "AAA only":       (1, 1),
            "AA+ or better":  (1, 2),
            "AA or better":   (1, 3),
            "AA- or better":  (1, 4),
            "A+ or better":   (1, 5),
            "A or better":    (1, 6),
            "A- or better":   (1, 7),
            "BBB+ or better": (1, 8),
            "All":            (1, 20),
        }
        grade_choice = st.selectbox(
            "Minimum Rating",
            options=list(grade_options.keys()),
            index=list(grade_options.keys()).index("A- or better"),
        )
        min_grade, max_grade = grade_options[grade_choice]

        # ---- Exact ratings (tick to include only these grades) ----
        _SYM2GRADE = {
            "AAA": 1, "AA+": 2, "AA": 3, "AA-": 4,
            "A+": 5, "A": 6, "A-": 7,
            "BBB+": 8, "BBB": 9, "BBB-": 10,
            "BB+": 11, "BB": 12, "BB-": 13,
            "B+": 14, "B": 15, "B-": 16,
            "C+": 17, "C": 18, "C-": 19, "D": 20,
        }
        exact_ratings = st.multiselect(
            "Exact Ratings (tick to include only these)",
            options=list(_SYM2GRADE.keys()),
            default=[],
            placeholder="e.g. AA-",
            help="Show only companies whose best grade is one of the ticked "
                 "grades (e.g. tick just AA-). Overrides Minimum Rating.",
        )
        if exact_ratings:
            # Query the full grade range, then filter to the ticked grades
            min_grade, max_grade = 1, 20

        # ---- Outlook ----
        outlook_options = ["Stable", "Positive", "Negative", "Watch",
                           "Watch Negative", "Watch Positive", "Watch Developing"]
        selected_outlooks = st.multiselect(
            "Outlook",
            options=outlook_options,
            default=[],
            placeholder="All outlooks",
        )

        st.divider()

        # ---- Sector checkboxes ----
        available_sectors = _cached_sectors()
        selected_sectors = _sector_checkbox_panel(available_sectors)

        st.divider()

        # ---- Listed Status ----
        listed_choice = st.radio(
            "Listed Status",
            options=["All", "Listed only", "Unlisted only"],
            index=0,
            horizontal=True,
        )
        listed_only   = listed_choice == "Listed only"
        unlisted_only = listed_choice == "Unlisted only"

        # ---- Sovereign / PSU ----
        exclude_psu = st.checkbox(
            "Exclude Sovereign / PSU",
            help="Hide government-owned / public sector entities (name-based detection)",
        )

        st.divider()

        # ---- Financial Filters ----
        with st.expander("Financial Filters", expanded=False):
            st.caption("Applies only to companies with financial data available.")
            min_revenue_cr = st.number_input(
                "Min Revenue (₹ Cr)", min_value=0.0, value=0.0, step=100.0
            )
            max_revenue_cr = st.number_input(
                "Max Revenue (₹ Cr)", min_value=0.0, value=0.0, step=1000.0,
                help="0 = no upper limit",
            )
            min_ebitda_cr = st.number_input(
                "Min EBITDA (₹ Cr)", min_value=0.0, value=0.0, step=50.0
            )
            min_ebitda_margin_pct = st.slider(
                "Min EBITDA Margin %", min_value=0, max_value=50, value=0
            )
            max_net_debt_ebitda = st.slider(
                "Max Net Debt / EBITDA",
                min_value=-5.0, max_value=20.0, value=20.0, step=0.5,
            )
            min_total_debt_cr = st.number_input(
                "Min Total Debt (₹ Cr)", min_value=0.0, value=0.0, step=100.0,
                help="Filter to companies with at least this much total debt",
            )

    # --------------------------------------------------------- #

        st.divider()
        # ---- Bond Maturity Overlay ----
        show_bond_overlay = st.checkbox(
            "Show bond maturities",
            value=False,
            help="Adds Bonds 30d/90d/1yr from NSDL. First load ~30s; cached 24h.",
        )
    # Main area                                                  #
    # --------------------------------------------------------- #
    stats = _cached_stats()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Companies",  f"{stats.get('total_companies', 0):,}")
    m2.metric("Rated Companies",  f"{stats.get('rated_companies', 0):,}")
    m3.metric("With Financials",  f"{stats.get('with_financials', 0):,}")
    last = stats.get("last_scraped") or "Never"
    m4.metric("Last Scraped", last[:16] if len(str(last)) > 16 else last)

    # ---- Rating actions: upgrades / downgrades this week ----
    _actions, _act_start, _act_end = _recent_rating_actions()
    st.markdown(
        "<div style='font-family:DM Sans,sans-serif;font-weight:700;font-size:1.05rem;"
        "margin:10px 0 2px 2px;'>Rating Actions — week to "
        f"{_act_end.strftime('%d %b %Y') if _act_end is not None else '—'}"
        "</div>", unsafe_allow_html=True)
    if not _actions:
        st.caption("No upgrades or downgrades in the latest week of data.")
    else:
        st.caption(f"{sum(a['up'] for a in _actions)} upgrades · "
                   f"{sum(not a['up'] for a in _actions)} downgrades · latest "
                   "dated action per company/agency vs its previous rating")
        _ups   = [a for a in _actions if a["up"]]
        _downs = [a for a in _actions if not a["up"]]
        import html as _html

        def _action_row(a, up):
            bg, bd, arrow, col = (("#F0FDF4", "#BBF7D0", "▲", "#059669") if up
                                  else ("#FEF2F2", "#FECACA", "▼", "#DC2626"))
            name = _html.escape(a["company"])
            if a["url"]:
                name = (f"<a href='{_html.escape(a['url'], quote=True)}' target='_blank' "
                        f"style='color:#1F2937;text-decoration:none;'>{name}</a>")
            return (
                f"<div style='padding:7px 12px;border-radius:8px;background:{bg};"
                f"border:1px solid {bd};margin-bottom:6px;"
                f"font-family:DM Sans,sans-serif;font-size:0.82rem;'>"
                f"<div style='display:flex;justify-content:space-between;gap:8px;'>"
                f"<span style='min-width:0;overflow:hidden;text-overflow:ellipsis;"
                f"white-space:nowrap;'><span style='color:{col};font-weight:700;'>{arrow}</span> "
                f"<b>{name}</b></span>"
                f"<span style='white-space:nowrap;color:#6B7280;'>"
                f"{_html.escape(a['agency'])} · {a['date'].strftime('%d %b')}</span></div>"
                f"<div style='color:#374151;margin-top:2px;'>"
                f"{_html.escape(a['from'])} → <b>{_html.escape(a['to'])}</b></div>"
                f"</div>")

        _cu, _cd = st.columns(2)
        _ACT_CAP = 6
        with _cu:
            st.markdown(f"**▲ Upgrades ({len(_ups)})**")
            if not _ups:
                st.caption("None this week.")
            for a in _ups[:_ACT_CAP]:
                st.markdown(_action_row(a, True), unsafe_allow_html=True)
            if len(_ups) > _ACT_CAP:
                with st.expander(f"+{len(_ups) - _ACT_CAP} more upgrades"):
                    for a in _ups[_ACT_CAP:]:
                        st.markdown(_action_row(a, True), unsafe_allow_html=True)
        with _cd:
            st.markdown(f"**▼ Downgrades ({len(_downs)})**")
            if not _downs:
                st.caption("None this week.")
            for a in _downs[:_ACT_CAP]:
                st.markdown(_action_row(a, False), unsafe_allow_html=True)
            if len(_downs) > _ACT_CAP:
                with st.expander(f"+{len(_downs) - _ACT_CAP} more downgrades"):
                    for a in _downs[_ACT_CAP:]:
                        st.markdown(_action_row(a, False), unsafe_allow_html=True)

    st.divider()

    # ---- DB query ----
    with st.spinner("Querying database..."):
        try:
            df = _cached_query(
                min_grade=min_grade,
                max_grade=max_grade,
                agencies_tuple=tuple(selected_agencies) if selected_agencies else tuple(available_agencies),
                outlooks_tuple=tuple(selected_outlooks) if selected_outlooks else (),
                sectors_tuple=tuple(selected_sectors) if selected_sectors else (),
                listed_only=listed_only,
                unlisted_only=unlisted_only,
                min_revenue_cr=float(min_revenue_cr) if min_revenue_cr else None,
                max_revenue_cr=float(max_revenue_cr) if max_revenue_cr else None,
                min_ebitda_cr=float(min_ebitda_cr) if min_ebitda_cr else None,
                min_ebitda_margin_pct=float(min_ebitda_margin_pct) if min_ebitda_margin_pct else None,
                max_net_debt_ebitda=float(max_net_debt_ebitda),
                min_total_debt_cr=float(min_total_debt_cr) if min_total_debt_cr else None,
            )
        except Exception as exc:
            st.error(f"Query error: {exc}")
            df = pd.DataFrame()

    if df is not None and not df.empty:
        display_df = df.copy()
        if "Listed" in display_df.columns:
            display_df["Listed"] = display_df["Listed"].map(lambda x: "Yes" if x == 1 else "No")

        # Exact-ratings filter (in-memory) — only ticked grades
        if exact_ratings and "Grade" in display_df.columns:
            _sel_grades = {_SYM2GRADE[s] for s in exact_ratings}
            display_df = display_df[
                display_df["Grade"].isin(_sel_grades)
            ].reset_index(drop=True)

        # Sovereign filter (in-memory) — exclude PSUs when checked
        # (astype(bool) keeps the mask boolean even on empty results)
        if exclude_psu:
            mask = display_df["Company Name"].apply(_is_psu).astype(bool)
            display_df = display_df[~mask].reset_index(drop=True)

        # Company name search (in-memory)
        if company_search:
            mask = display_df["Company Name"].str.contains(company_search, case=False, na=False)
            display_df = display_df[mask].reset_index(drop=True)
    else:
        display_df = df if df is not None else pd.DataFrame()


    # ---- Bond data (always load for link filtering; cached 24h) ----
    if display_df is not None and not display_df.empty:
        with st.spinner("Checking bond data..."):
            _bonds_df = _load_nsdl_bonds()
        display_df = _add_bond_counts(display_df, _bonds_df)
        if not show_bond_overlay:
            # Hide time-windowed columns; keep Bonds total for link visibility
            display_df.drop(columns=["Bonds 30d", "Bonds 90d", "Bonds 1yr"],
                            errors="ignore", inplace=True)
    # Reset index so positional lookups are safe
    if display_df is not None and not display_df.empty:
        display_df = display_df.reset_index(drop=True)

    result_count = len(display_df) if display_df is not None else 0
    st.subheader(f"{result_count:,} companies match your filters")

    if display_df is not None and not display_df.empty:
        # Default sort: Grade ascending
        if "Grade" in display_df.columns:
            display_df = display_df.sort_values("Grade", ascending=True, na_position="last").reset_index(drop=True)

        # Add Notes column from persisted store
        notes = st.session_state.notes
        display_df = display_df.copy()
        display_df["Notes"] = display_df["company_id"].astype(str).map(notes).fillna("")
        # View Bonds link — only populate if company has active NSDL bonds
        def _bonds_url(row):
            cname = row.get("Company Name", "")
            if not pd.notna(cname):
                return ""
            # Only show link if company has any active NSDL bonds (any maturity)
            if "Bonds total" in row.index and (row.get("Bonds total") or 0) == 0:
                return ""
            return "https://creditnexus-bonds.streamlit.app/?issuer=" + urllib.parse.quote(str(cname))

        display_df["View Bonds"] = display_df.apply(_bonds_url, axis=1)

        # Show as editable table — Notes and Sector columns are editable
        # ---- ND/EBITDA: prefer rationale-sourced text, fall back to numeric ----
        def _nd_display(row):
            txt = row.get("ND/EBITDA (Rationale)", None)
            if txt and str(txt).strip() and str(txt).strip() != "nan":
                src_lbl = row.get("ND/EBITDA Source", "") or ""
                return str(txt).strip() + (" (" + src_lbl + ")" if src_lbl else "")
            num = row.get("Net Debt/EBITDA", None)
            if num is not None and str(num) not in ("", "nan", "None"):
                try:
                    return f"{float(num):.1f}x"
                except Exception:
                    pass
            return ""

        display_df["ND/EBITDA"] = display_df.apply(_nd_display, axis=1)

        # Fill URL columns so LinkColumn shows blank instead of "None"
        for _url_col in ["CRISIL URL", "ICRA URL", "Care Edge URL", "India Ratings URL"]:
            if _url_col in display_df.columns:
                display_df[_url_col] = display_df[_url_col].fillna("")

        # ── ICRA pre-processing ────────────────────────────────────────────────────
        # ICRA stores combined "LT_RATING, ST_RATING" in rating_symbol.
        # Extract only the long-term portion and parse its embedded outlook.
        #   "[ICRA]AAA (Stable), --"       -> AAA / Stable
        #   "--, [ICRA]A1+"                -> (blank – no LT rating)
        #   "[ICRA]A- (Stable), [ICRA]A2+" -> A- / Stable
        _ICRA_SKIP = {'--', 'withdrawn', '*', 'n.a.', 'na', ''}

        def _parse_icra_lt(raw):
            """Return (lt_symbol, lt_outlook) from ICRA combined rating_symbol."""
            if not raw or str(raw).strip().lower() in ('nan', 'none', ''):
                return '', ''
            lt = str(raw).strip().split(', ')[0].strip()   # first token = LT
            if lt.lower().rstrip('* ') in _ICRA_SKIP:
                return '', ''
            lt = re.sub(r'^\[?icra\]?\s*', '', lt, flags=re.IGNORECASE)  # strip [ICRA]
            if re.match(r'^A[1-4]', lt, re.IGNORECASE):    # skip short-term symbols
                return '', ''
            lt = re.sub(r'\s*ISSUER\s+NOT\s+COOPERATING\s*', ' INC',
                        lt, flags=re.IGNORECASE).strip()
            m = re.search(r'\s*\(([^)]+)\)\s*$', lt)
            if m:
                return lt[:m.start()].strip(), m.group(1)
            return lt, ''

        if 'ICRA Rating' in display_df.columns:
            _icra_lt = display_df['ICRA Rating'].apply(_parse_icra_lt)
            display_df['ICRA Rating']  = _icra_lt.apply(lambda x: x[0])
            display_df['ICRA Outlook'] = _icra_lt.apply(lambda x: x[1])

        # ── Rating link formatting (all 4 agencies) ────────────────────────────────
        # Builds "url##AAA / Stable" for rated companies, "" for not-rated.
        # LinkColumn display_text=r"##(.+)$" shows the rating text as a hyperlink.
        _PREFIX_RE = {
            "CRISIL":        re.compile(r"^crisil\s+", re.IGNORECASE),
            "ICRA":          re.compile(r"^\[?icra\]?\s*", re.IGNORECASE),  # \s* not \s+
            "Care Edge":     re.compile(r"^care\s*edge\s+|^care\s+", re.IGNORECASE),
            "India Ratings": re.compile(r"^india\s+ratings?\s+|^ind\s+", re.IGNORECASE),
        }
        for _ag, _rat, _out, _ucol in [
            ("CRISIL",        "CRISIL Rating",        "CRISIL Outlook",       "CRISIL URL"),
            ("ICRA",          "ICRA Rating",          "ICRA Outlook",         "ICRA URL"),
            ("Care Edge",     "Care Edge Rating",     "Care Edge Outlook",    "Care Edge URL"),
            ("India Ratings", "India Ratings Rating", "India Ratings Outlook","India Ratings URL"),
        ]:
            if _rat in display_df.columns:
                def _fmt_link_rating(row, r=_rat, o=_out, u=_ucol, p=_PREFIX_RE[_ag]):
                    sym = row.get(r, None)
                    if not sym or str(sym).strip() in ("", "nan", "None"):
                        return ""  # not rated -> blank
                    sym = p.sub("", str(sym).strip())  # strip any remaining prefix
                    out_v = row.get(o, None)
                    if out_v and str(out_v).strip() not in ("", "nan", "None"):
                        display = sym + " / " + str(out_v).strip()
                    else:
                        display = sym
                    url_v = row.get(u, None)
                    if url_v and str(url_v).strip() not in ("", "nan", "None"):
                        return str(url_v).strip() + "##" + display
                    return display  # rated but no URL -> plain text (rare)
                display_df[_ag] = display_df.apply(_fmt_link_rating, axis=1)

        # ── Rating badge legend ──────────────────────────────────────────
        st.markdown(
            """
            <div style="display:flex;flex-wrap:wrap;gap:6px;align-items:center;
                        margin-bottom:10px;padding:10px 14px;
                        background:#F8F9FF;border:1px solid #E0E2EF;border-radius:10px;">
                <span style="font-family:'DM Sans',sans-serif;font-size:0.7rem;font-weight:700;
                             color:#6B7280;letter-spacing:0.08em;text-transform:uppercase;
                             margin-right:6px;">Rating scale →</span>
                <span style="background:#0F766E;color:#fff;border-radius:6px;
                             padding:2px 9px;font-size:0.72rem;font-weight:600;
                             font-family:'DM Sans',sans-serif;">AAA</span>
                <span style="background:#0369A1;color:#fff;border-radius:6px;
                             padding:2px 9px;font-size:0.72rem;font-weight:600;
                             font-family:'DM Sans',sans-serif;">AA</span>
                <span style="background:#6366F1;color:#fff;border-radius:6px;
                             padding:2px 9px;font-size:0.72rem;font-weight:600;
                             font-family:'DM Sans',sans-serif;">A</span>
                <span style="background:#7C3AED;color:#fff;border-radius:6px;
                             padding:2px 9px;font-size:0.72rem;font-weight:600;
                             font-family:'DM Sans',sans-serif;">BBB</span>
                <span style="background:#B45309;color:#fff;border-radius:6px;
                             padding:2px 9px;font-size:0.72rem;font-weight:600;
                             font-family:'DM Sans',sans-serif;">BB</span>
                <span style="background:#DC2626;color:#fff;border-radius:6px;
                             padding:2px 9px;font-size:0.72rem;font-weight:600;
                             font-family:'DM Sans',sans-serif;">B &amp; below</span>
                <span style="background:#6B7280;color:#fff;border-radius:6px;
                             padding:2px 9px;font-size:0.72rem;font-weight:600;
                             font-family:'DM Sans',sans-serif;">D / NR</span>
                <span style="font-family:'DM Sans',sans-serif;font-size:0.7rem;color:#6B7280;
                             margin-left:auto;">Click a rating to open the agency rationale</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        non_note_cols = [c for c in display_df.columns if c not in ("Notes", "Sector")]
        editor_df = display_df.drop(columns=["company_id"], errors="ignore")

        # Column order: agency cols are now combined url##text (no separate URL col)
        _col_order = [
            "Company Name",
            "View Bonds", "Bonds 30d", "Bonds 90d", "Bonds 1yr",
            "CRISIL", "ICRA", "Care Edge", "India Ratings",
            "Sector",
            "Revenue (Cr)", "EBITDA (Cr)", "EBITDA Margin %",
            "Total Debt (Cr)", "Net Debt (Cr)", "ND/EBITDA",
            "Rating Date", "Notes",
        ]
        _present = [c for c in _col_order if c in editor_df.columns]
        editor_df = editor_df[_present]  # only show columns in _col_order
        non_note_cols = [c for c in editor_df.columns if c not in ("Notes", "Sector")]
        edited_df = st.data_editor(
            editor_df,
            use_container_width=True,
            height=620,
            hide_index=True,
            disabled=non_note_cols,
            column_config={
                "Company Name":  st.column_config.TextColumn("Company",       width="medium"),
                # --- Agency rating cols: value = "url##AAA/Stable" or "" ---
                "CRISIL":        st.column_config.LinkColumn("CRISIL",        display_text=r"##(.+)$", width="medium"),
                "ICRA":          st.column_config.LinkColumn("ICRA",          display_text=r"##(.+)$", width="medium"),
                "Care Edge":     st.column_config.LinkColumn("Care Edge",     display_text=r"##(.+)$", width="medium"),
                "India Ratings": st.column_config.LinkColumn("India Ratings", display_text=r"##(.+)$", width="medium"),
                # --- Financials ---
                "Sector":             st.column_config.SelectboxColumn(
                    "Sector 📝", width="medium", options=_ALL_SECTOR_OPTIONS,
                    help="Click to change sector. Changes save automatically and sync to company record.",
                ),
                "Revenue (Cr)":       st.column_config.NumberColumn("Revenue (Cr)",  format="%.0f", width="small"),
                "EBITDA (Cr)":        st.column_config.NumberColumn("EBITDA (Cr)",   format="%.0f", width="small"),
                "EBITDA Margin %":    st.column_config.NumberColumn("EBITDA %",      format="%.1f%%", width="small"),
                "Total Debt (Cr)":    st.column_config.NumberColumn("Total Debt",    format="%.0f", width="small"),
                "Net Debt (Cr)":      st.column_config.NumberColumn("Net Debt",      format="%.0f", width="small"),
                "ND/EBITDA":          st.column_config.TextColumn("ND/EBITDA",       width="small"),
                "Rating Date":        st.column_config.TextColumn("Rating Date",     width="small"),
                "View Bonds":         st.column_config.LinkColumn("Bonds ↗", display_text="↗", width="small"),
                "Bonds 30d":          st.column_config.NumberColumn("Bonds 30d",     format="%d", width="small"),
                "Bonds 90d":          st.column_config.NumberColumn("Bonds 90d",     format="%d", width="small"),
                "Bonds 1yr":          st.column_config.NumberColumn("Bonds 1yr",     format="%d", width="small"),
                "Notes":              st.column_config.TextColumn("Notes",           width="large"),
            },
            key="main_table",
        )

        # Persist note and sector edits — only process rows the user actually changed
        edit_delta = st.session_state.get("main_table") or {}
        edited_rows = edit_delta.get("edited_rows", {})
        if edited_rows:
            notes_changed = False
            sector_saves = []
            for row_idx, changes in edited_rows.items():
                idx = int(row_idx)
                if idx >= len(display_df):
                    continue
                cid = str(display_df.iloc[idx]["company_id"])

                # ---- Notes ----
                if "Notes" in changes:
                    note = str(changes["Notes"] or "").strip()
                    if note:
                        if notes.get(cid) != note:
                            notes[cid] = note
                            notes_changed = True
                    elif cid in notes:
                        del notes[cid]
                        notes_changed = True

                # ---- Sector ----
                if "Sector" in changes:
                    new_sector = str(changes["Sector"] or "").strip()
                    old_sector = str(display_df.iloc[idx].get("Sector", "") or "").strip()
                    if new_sector != old_sector:
                        sector_saves.append((int(cid), new_sector))

            if notes_changed:
                st.session_state.notes = notes
                _save_notes(notes)

            for cid_int, new_sector in sector_saves:
                _save_sector_override(cid_int, new_sector)
                _cached_sectors.clear()
            if sector_saves:
                st.toast(
                    f"✅ Sector updated for {len(sector_saves)} company"
                    + ("" if len(sector_saves) == 1 else "s")
                    + " — syncing to cloud…",
                    icon="✏️",
                )
                st.rerun()

        # ---- Export ----
        st.divider()
        c_exp, c_info = st.columns([1, 3])
        with c_exp:
            export_df = display_df.drop(columns=["company_id"], errors="ignore")
            excel_bytes = _df_to_excel(export_df)
            st.download_button(
                label="Export to Excel",
                data=excel_bytes,
                file_name="ratings_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
            )
        with c_info:
            st.caption(
                f"Exporting {result_count:,} rows. "
                "'—' means financial data not yet available for that company."
            )

        # ---- Grade distribution chart ----
        if df is not None and "Grade" in df.columns and not df["Grade"].isna().all():
            st.divider()
            st.subheader("Rating Distribution")
            from parsers.rating import grade_label
            grade_counts = (
                df["Grade"].dropna().astype(int)
                .value_counts().sort_index().reset_index()
            )
            grade_counts.columns = ["Grade", "Count"]
            grade_counts["Symbol"] = grade_counts["Grade"].apply(grade_label)
            grade_counts["Label"]  = grade_counts["Symbol"] + " (G" + grade_counts["Grade"].astype(str) + ")"

            c_chart, c_table = st.columns([2, 1])
            with c_chart:
                st.bar_chart(grade_counts.set_index("Label")["Count"], use_container_width=True)
            with c_table:
                st.dataframe(
                    grade_counts[["Symbol", "Count"]].rename(columns={"Symbol": "Rating"}),
                    use_container_width=True,
                    hide_index=True,
                )


        # ---- Contact footer ----
        st.markdown(
            '<div style="text-align:center;margin-top:40px;padding-bottom:16px;'
            'font-size:12px;font-family:\'DM Sans\',sans-serif;color:var(--text-muted,#6B7280)">'
            '<a href="https://www.linkedin.com/in/saxenakriti/" target="_blank"'
            ' style="color:#6366F1;text-decoration:none;font-weight:500">Contact</a></div>',
            unsafe_allow_html=True,
        )
    elif display_df is not None and display_df.empty:
        st.info("No companies match the current filters. Try relaxing your criteria.")
    else:
        st.error("Failed to load data.")


if __name__ == "__main__":
    main()
