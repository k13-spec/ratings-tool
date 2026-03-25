"""
Streamlit UI for the Indian Credit Ratings Tool.

Run with:
    streamlit run app.py --server.headless true --browser.gatherUsageStats false
"""

import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

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
        # CRISIL-style labels
        "Manufacturing", "Auto", "Healthcare", "Technology", "Textile",
        "Energy", "Retail", "Real Estate", "Telecom", "Chemicals",
        "Pharmaceuticals", "Consumer Durables", "Media", "Education",
        "Agriculture", "Food & Beverages", "Logistics", "Hotels",
        "Construction", "Cement", "Metals", "Mining", "Paper", "Printing",
        "Trading", "Services", "IT Services", "FMCG", "Diversified", "",
        # ICRA-style labels
        "Realty", "Textiles & Apparels", "Auto Components", "Automobiles",
        "Agricultural, Commercial & Construction Vehicles",
        "Chemicals & Petrochemicals", "Fertilizers & Agrochemicals",
        "Industrial Products", "Industrial Manufacturing",
        "Agricultural Food & other Products", "Food Products", "Beverages",
        "Ferrous Metals", "Diversified Metals", "Metals & Minerals Trading",
        "Minerals & Mining", "Pharmaceuticals & Biotechnology",
        "Commercial Services & Supplies", "Other Consumer Services",
        "Leisure Services", "Telecom - Services", "Telecom - Equipment & Accessories",
        "Healthcare Services", "Healthcare Equipment & Supplies",
        "Paper, Forest & Jute Products", "IT - Services", "IT - Software", "IT - Hardware",
        "Electrical Equipment", "Cement & Cement Products", "Other Construction Materials",
        "Consumable Fuels", "Petroleum Products", "Diversified FMCG",
        "Household Products", "Entertainment", "Cigarettes & Tobacco Products",
        "Retailing", "Not Mapped",
    ],
    "Infrastructure": [
        "Infrastructure", "Transport Infrastructure", "Transport Services",
        "Other Utilities", "Public Services", "Gas", "Power",
        "Oil",
    ],
    "Financial": [
        "Financial Sector", "Banks", "Capital Markets", "Finance", "Insurance",
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
    "indian bank ", "uco bank",
    "life insurance corporation",
    "power finance corp", "rural electrification corp",
    "housing and urban development", "national bank for agriculture",
    "export import bank of india", "exim bank",
    "rec limited", "pfc limited",
    "food corporation of india",
    "oil india", "mrpl", "bpcl", "hpcl",
]

def _is_psu(name: str) -> bool:
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
def _sector_checkbox_panel(available_sectors: list) -> list:
    grouped: dict[str, list] = {"Corporate": [], "Infrastructure": [], "Financial": []}
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
    for grp in ["Corporate", "Infrastructure", "Financial"]:
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
/* ── Hide Streamlit chrome ── */
#MainMenu, footer { visibility: hidden; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #0f1117;
    border-right: 1px solid #1e2130;
}
[data-testid="stSidebar"] .stButton > button {
    border-radius: 6px;
    font-size: 0.82rem;
}

/* ── Metric cards ── */
[data-testid="stMetric"] {
    background: #1a1d2e;
    border: 1px solid #2a2d40;
    border-radius: 10px;
    padding: 0.6rem 1rem;
}
[data-testid="stMetricLabel"] { font-size: 0.78rem; color: #9098b0; }
[data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 600; }

/* ── Expanders ── */
[data-testid="stExpander"] details {
    border: 1px solid #2a2d40;
    border-radius: 8px;
    background: #13161f;
}

/* ── Sort controls row ── */
div[data-testid="stHorizontalBlock"] [data-testid="stSelectbox"] label,
div[data-testid="stHorizontalBlock"] [data-testid="stToggle"] label {
    font-size: 0.8rem;
    color: #9098b0;
}

/* ── Dividers ── */
hr { border-color: #1e2130 !important; }

/* ── Download button ── */
[data-testid="stDownloadButton"] > button {
    border-radius: 8px;
}
</style>
"""


def main():
    st.set_page_config(
        page_title="Indian Credit Ratings",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(_CSS, unsafe_allow_html=True)
    st.title("Indian Credit Ratings Dashboard")
    st.caption("ICRA · CRISIL · CARE · IND Ratings  |  Financials from NSE/yfinance & CRISIL")

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

        # ---- Company Name Search ----
        company_search = st.text_input(
            "Search Company Name",
            placeholder="e.g. Tata, Reliance…",
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
    # Main area                                                  #
    # --------------------------------------------------------- #
    stats = _cached_stats()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Companies",  f"{stats.get('total_companies', 0):,}")
    m2.metric("Rated Companies",  f"{stats.get('rated_companies', 0):,}")
    m3.metric("With Financials",  f"{stats.get('with_financials', 0):,}")
    last = stats.get("last_scraped") or "Never"
    m4.metric("Last Scraped", last[:16] if len(str(last)) > 16 else last)

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

        # Sovereign filter (in-memory) — exclude PSUs when checked
        if exclude_psu:
            mask = display_df["Company Name"].apply(_is_psu)
            display_df = display_df[~mask].reset_index(drop=True)

        # Company name search (in-memory)
        if company_search:
            mask = display_df["Company Name"].str.contains(company_search, case=False, na=False)
            display_df = display_df[mask].reset_index(drop=True)
    else:
        display_df = df if df is not None else pd.DataFrame()

    # Reset index so positional lookups are safe
    if display_df is not None and not display_df.empty:
        display_df = display_df.reset_index(drop=True)

    result_count = len(display_df) if display_df is not None else 0
    st.subheader(f"{result_count:,} companies match your filters")

    if display_df is not None and not display_df.empty:
        # ---- Sort controls ----
        _sortable = [
            "Grade", "Company Name", "Revenue (Cr)", "EBITDA (Cr)",
            "Total Debt (Cr)", "Net Debt/EBITDA", "EBITDA Margin %", "Rating Date",
        ]
        _sort_cols_avail = [c for c in _sortable if c in display_df.columns]
        sc1, sc2, sc3 = st.columns([3, 1, 4])
        with sc1:
            sort_col = st.selectbox(
                "Sort by", options=_sort_cols_avail,
                index=_sort_cols_avail.index("Grade") if "Grade" in _sort_cols_avail else 0,
                key="sort_col", label_visibility="collapsed",
            )
        with sc2:
            sort_asc = st.toggle("↑ Asc", value=True, key="sort_asc")
        display_df = display_df.sort_values(
            sort_col, ascending=sort_asc, na_position="last"
        ).reset_index(drop=True)

        # Add Notes column from persisted store
        notes = st.session_state.notes
        display_df = display_df.copy()
        display_df["Notes"] = display_df["company_id"].astype(str).map(notes).fillna("")

        # Show as editable table — Notes and Sector columns are editable
        non_note_cols = [c for c in display_df.columns if c not in ("Notes", "Sector")]
        editor_df = display_df.drop(columns=["company_id"], errors="ignore")

        # Replace NaN in URL column so LinkColumn shows blank instead of "None"
        if "Rationale URL" in editor_df.columns:
            editor_df["Rationale URL"] = editor_df["Rationale URL"].fillna("")

        # Reorder: put Rationale URL as 3rd column (after Company Name, Agency)
        _col_order = [
            "Company Name", "Agency", "Rationale URL",
            "Rating", "Grade", "Outlook", "Sector", "Listed",
            "Revenue (Cr)", "EBITDA (Cr)", "EBITDA Margin %",
            "Total Debt (Cr)", "Net Debt (Cr)", "Net Debt/EBITDA",
            "Rating Date", "BSE Code", "ISIN", "Notes",
        ]
        _present = [c for c in _col_order if c in editor_df.columns]
        _extra   = [c for c in editor_df.columns if c not in _present]
        editor_df = editor_df[_present + _extra]
        edited_df = st.data_editor(
            editor_df,
            use_container_width=True,
            height=620,
            hide_index=True,
            disabled=non_note_cols,
            column_config={
                "Company Name":    st.column_config.TextColumn("Company",           width="large", pinned=True),
                "Agency":          st.column_config.TextColumn("Agency",            width="small"),
                "Rating":          st.column_config.TextColumn("Rating",            width="small"),
                "Grade":           st.column_config.NumberColumn("Grade",           width="small", format="%d"),
                "Outlook":         st.column_config.TextColumn("Outlook",           width="medium"),
                "Sector":          st.column_config.SelectboxColumn(
                    "Sector ✏️", width="medium", options=_ALL_SECTOR_OPTIONS,
                    help="Click to change sector. Changes save automatically and sync to cloud.",
                ),
                "Listed":          st.column_config.TextColumn("Listed",            width="small"),
                "Revenue (Cr)":    st.column_config.NumberColumn("Revenue (Cr)",    format="₹%,.0f", width="medium"),
                "EBITDA (Cr)":     st.column_config.NumberColumn("EBITDA (Cr)",     format="₹%,.0f", width="medium"),
                "EBITDA Margin %": st.column_config.NumberColumn("EBITDA %",        format="%.1f%%", width="small"),
                "Total Debt (Cr)": st.column_config.NumberColumn("Total Debt (Cr)", format="₹%,.0f", width="medium"),
                "Net Debt (Cr)":   st.column_config.NumberColumn("Net Debt (Cr)",   format="₹%,.0f", width="medium"),
                "Net Debt/EBITDA": st.column_config.NumberColumn("ND/EBITDA",       format="%.1fx",  width="small"),
                "Rating Date":     st.column_config.TextColumn("Rating Date",       width="medium"),
                "Rationale URL":   st.column_config.LinkColumn(
                    "Rationale", display_text="↗", width="small",
                ),
                "BSE Code":        st.column_config.TextColumn("BSE Code",          width="small"),
                "ISIN":            st.column_config.TextColumn("ISIN",              width="medium"),
                "Notes":           st.column_config.TextColumn("Notes",             width="large"),
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

    elif display_df is not None and display_df.empty:
        st.info("No companies match the current filters. Try relaxing your criteria.")
    else:
        st.error("Failed to load data.")


if __name__ == "__main__":
    main()
