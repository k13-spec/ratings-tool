"""
Company Deep Dive — combines current ratings + history (from the ratings DB)
with bond maturity schedule (from NSDL) for a single issuer.

URL: /Company_Deep_Dive?company=<name>
"""
from pathlib import Path
import sys
import urllib.parse

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "data" / "ratings.db"

st.set_page_config(
    page_title="Company Deep Dive | Ratings",
    page_icon="magnifying_glass",
    layout="wide",
)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _db_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    import sqlite3
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()
        return df
    except Exception as exc:
        st.error(f"DB error: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=86400, show_spinner=False)
def _load_nsdl() -> pd.DataFrame:
    """Download active bonds from NSDL. Cached 24h."""
    import io, time, warnings
    import openpyxl, requests

    try:
        hdrs = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.indiabondinfo.nsdl.com/CBDServices/",
        }
        s = requests.Session()
        s.headers.update(hdrs)
        s.get("https://www.indiabondinfo.nsdl.com/CBDServices/", timeout=15)
        time.sleep(0.3)
        r = s.get(
            "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo"
            "/listofsecurities?type=Active",
            timeout=120, stream=True,
        )
        r.raise_for_status()
        warnings.filterwarnings("ignore", category=UserWarning)
        wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        col = {h: i for i, h in enumerate(rows[0]) if h}
        today = pd.Timestamp.today().normalize()
        records = []
        for row in rows[1:]:
            if not row:
                continue
            issuer  = row[col.get("Name of Issuer", 1)]
            mat_raw = row[col.get("Date of Redemption/Conversion", 0)]
            isin    = row[col.get("ISIN", 0)] if col.get("ISIN") is not None else ""
            coupon  = row[col.get("Coupon Rate (%)", 0)] if col.get("Coupon Rate (%)") is not None else ""
            size_r  = row[col.get("Issue Size(in Rs.)", 0)] if col.get("Issue Size(in Rs.)") is not None else None
            itype   = row[col.get("Type of Instrument", 0)] if col.get("Type of Instrument") is not None else ""
            rating  = row[col.get("Credit Rating", 0)] if col.get("Credit Rating") is not None else ""
            if not issuer or not mat_raw:
                continue
            try:
                mat = pd.Timestamp(str(mat_raw).strip().replace("/", "-"))
            except Exception:
                continue
            if mat < today:
                continue
            try:
                size_cr = round(float(str(size_r).replace(",", "").strip()) / 1e7, 2) if size_r else None
            except Exception:
                size_cr = None
            records.append({
                "ISIN":          str(isin or "").strip(),
                "Issuer":        str(issuer).strip(),
                "Type":          str(itype or "").strip(),
                "Coupon %":      str(coupon or "").strip(),
                "Maturity Date": mat,
                "Days Left":     int((mat - today).days),
                "Size (Cr)":     size_cr,
                "Rating":        str(rating or "").strip(),
            })
        return pd.DataFrame(records)
    except Exception:
        return pd.DataFrame()


_CO_STOP_DD = {"limited","ltd","private","pvt","llp","corporation",
               "corp","inc","co","bank","finance","financial"}

def _key(name: str) -> str:
    words = [w.strip(".,&") for w in name.lower().split()]
    return " ".join(w for w in words if w not in _CO_STOP_DD).strip()


def _filter_bonds(bonds_df: pd.DataFrame, company_name: str) -> pd.DataFrame:
    if bonds_df.empty or not company_name:
        return pd.DataFrame()
    k = _key(company_name)
    if len(k) < 4:
        return pd.DataFrame()
    k18 = k[:18]
    mask = bonds_df["Issuer"].apply(lambda n: k18 in _key(n))
    return bonds_df[mask].sort_values("Days Left").reset_index(drop=True)


# ------------------------------------------------------------------
# UI
# ------------------------------------------------------------------
st.title("Company Deep Dive")

qp = st.query_params.get("company", "")
company_name = st.text_input(
    "Company name",
    value=qp,
    placeholder="e.g. HDFC Bank, Tata Motors, Reliance Industries",
).strip()

if not company_name:
    st.info("Enter a company name above to see ratings, bond maturities, and financials.")
    st.stop()

pattern = f"%{company_name}%"

# DB queries
ratings_df = _db_query(
    """
    SELECT c.name AS Company,
           r.agency AS Agency,
           r.rating_symbol AS Rating,
           r.rating_grade AS Grade,
           r.outlook AS Outlook,
           r.instrument_type AS Instrument,
           r.rated_amount_cr AS "Amount (Cr)",
           r.rating_date AS Date,
           r.rationale_url AS "Rationale URL"
    FROM ratings r
    JOIN companies c ON c.id = r.company_id
    WHERE c.name LIKE ?
    ORDER BY r.rating_date DESC, r.agency
    """,
    (pattern,),
)

fin_df = _db_query(
    """
    SELECT f.fiscal_year AS Year,
           f.revenue_cr AS "Revenue (Cr)",
           f.ebitda_cr AS "EBITDA (Cr)",
           f.ebitda_margin_pct AS "EBITDA %",
           f.total_debt_cr AS "Total Debt (Cr)",
           f.net_debt_cr AS "Net Debt (Cr)",
           f.net_debt_ebitda AS "ND/EBITDA"
    FROM financials f
    JOIN companies c ON c.id = f.company_id
    WHERE c.name LIKE ?
    ORDER BY f.fiscal_year DESC
    """,
    (pattern,),
)

# NSDL bonds
with st.spinner("Loading bond data (cached 24h — first load ~30s)..."):
    all_bonds = _load_nsdl()

company_bonds = _filter_bonds(all_bonds, company_name)

if ratings_df.empty and fin_df.empty and company_bonds.empty:
    st.warning(f"No data found for **{company_name}**. Try a shorter or different name fragment.")
    st.stop()

# Matched company name from DB
matched_name = ratings_df["Company"].iloc[0] if not ratings_df.empty else company_name
st.subheader(matched_name)

col_hdr1, col_hdr2 = st.columns(2)
bond_url   = f"https://creditnexus-bonds.streamlit.app/?issuer={urllib.parse.quote(matched_name)}"
ratings_url = f"https://creditnexus.streamlit.app/?company={urllib.parse.quote(matched_name)}"
with col_hdr1:
    st.markdown(f"[View all bonds maturing →]({bond_url})")
with col_hdr2:
    st.markdown(f"[Back to ratings dashboard →]({ratings_url})")

st.divider()

left, right = st.columns([1, 1])

# ---- Left: Ratings ----
with left:
    if not ratings_df.empty:
        # Latest per agency
        latest = (
            ratings_df
            .sort_values("Date", ascending=False)
            .groupby("Agency")
            .first()
            .reset_index()
        )
        latest["Rationale URL"] = latest["Rationale URL"].fillna("")
        st.subheader("Current Ratings")
        st.dataframe(
            latest[["Agency", "Rating", "Grade", "Outlook", "Date", "Rationale URL"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Grade": st.column_config.NumberColumn(format="%d", width="small"),
                "Rationale URL": st.column_config.LinkColumn(
                    "Rationale", display_text="↗", width="small"
                ),
            },
        )
        # Rating history table
        history = ratings_df[["Date", "Agency", "Rating", "Grade", "Outlook", "Instrument"]].copy()
        history["Date"] = pd.to_datetime(history["Date"], errors="coerce")
        history = history.dropna(subset=["Date"]).sort_values("Date", ascending=False)
        if len(history) > len(latest):
            with st.expander(f"Rating history ({len(history)} entries)"):
                st.dataframe(
                    history,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Date": st.column_config.DateColumn(format="DD/MM/YYYY"),
                        "Grade": st.column_config.NumberColumn(format="%d"),
                    },
                )
    else:
        st.info("No ratings found in database for this company.")

# ---- Right: Bonds ----
with right:
    if not company_bonds.empty:
        n = len(company_bonds)
        m30  = int((company_bonds["Days Left"] <= 30).sum())
        m90  = int((company_bonds["Days Left"] <= 90).sum())
        m365 = int((company_bonds["Days Left"] <= 365).sum())
        st.subheader(f"Bond Maturities ({n} active bonds)")
        c1, c2, c3 = st.columns(3)
        c1.metric("Maturing ≤30d", m30)
        c2.metric("Maturing ≤90d", m90)
        c3.metric("Maturing ≤1yr", m365)
        disp = company_bonds.copy()
        disp["Maturity Date"] = disp["Maturity Date"].dt.strftime("%d/%m/%Y")
        disp["Size (Cr)"] = disp["Size (Cr)"].apply(
            lambda x: f"{x:,.2f}" if pd.notna(x) else "—"
        )
        st.dataframe(
            disp[["ISIN","Type","Coupon %","Maturity Date","Days Left","Size (Cr)","Rating"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Days Left": st.column_config.NumberColumn(format="%d", width="small"),
                "Size (Cr)": st.column_config.TextColumn(width="small"),
            },
        )
    else:
        st.subheader("Bond Maturities")
        st.info("No active bonds found in NSDL data for this company.")

# ---- Financials ----
if not fin_df.empty:
    st.divider()
    st.subheader("Financials")
    st.dataframe(
        fin_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Revenue (Cr)":    st.column_config.NumberColumn(format="%.0f"),
            "EBITDA (Cr)":     st.column_config.NumberColumn(format="%.0f"),
            "EBITDA %":        st.column_config.NumberColumn(format="%.1f%%"),
            "Total Debt (Cr)": st.column_config.NumberColumn(format="%.0f"),
            "Net Debt (Cr)":   st.column_config.NumberColumn(format="%.0f"),
            "ND/EBITDA":       st.column_config.NumberColumn(format="%.1fx"),
        },
    )
