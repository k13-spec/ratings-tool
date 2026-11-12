"""
Financing Ideas — pre-screened INR debt financing candidates, priority-ranked.

Static research snapshot (compiled 7 Jul 2026) rendered from
assets/debt_financing_ideas.html. Each candidate card deep-links back to
Company_Deep_Dive (?company=) and the bond tracker (?issuer=).

To refresh the underlying research or add a company, regenerate the HTML
via Claude ("refresh the financing ideas page" / "run a workup on X") and
re-upload assets/debt_financing_ideas.html.

The embedded HTML posts `streamlit:setFrameHeight` so the iframe grows to fit
all cards (the old fixed 2600px frame + scroll-sentinel lazy-load stalled after
~12 cards on Streamlit Cloud). `height` below is only the pre-resize fallback.
"""
import datetime as _dt
import json as _json
import re as _re
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Financing Ideas | CreditNexus",
    page_icon="bulb",
    layout="wide",
    initial_sidebar_state="collapsed",
)

_HTML_PATH = Path(__file__).resolve().parent.parent / "assets" / "debt_financing_ideas.html"

# Trim default Streamlit chrome so the embedded page reads full-bleed
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.2rem; padding-bottom: 0; max-width: 1240px;}
      header[data-testid="stHeader"] {background: transparent;}
      iframe {border: none;}
      [data-testid="stToolbarActions"], [data-testid="stMainMenu"], [data-testid="stAppDeployButton"] {display: none !important;}
      [data-testid="stSidebarNav"] li:first-child a p {font-size: 0 !important;}
      [data-testid="stSidebarNav"] li:first-child a p::after {content: "Ratings Dashboard"; font-size: 0.875rem; line-height: 1.6;}
    </style>
    """,
    unsafe_allow_html=True,
)

if not _HTML_PATH.exists():
    st.error("assets/debt_financing_ideas.html not found in the repo.")
    st.stop()

_PATCH_DIR = _HTML_PATH.parent / "dfi_patches"

_MONTHS = {m: i + 1 for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}


def _hero_date(html):
    """Date in the hero's 'compiled D Mon YYYY' line, if present."""
    m = _re.search(r"compiled (\d{1,2}) ([A-Za-z]{3})[a-z]* (\d{4})", html)
    if not m:
        return None
    try:
        return _dt.date(int(m.group(3)), _MONTHS[m.group(2).title()], int(m.group(1)))
    except Exception:
        return None


def _load_page():
    """Base page + the assets/dfi_patches find/replace series.

    Weekly cloud news-refresh sessions cannot push the full ~1 MB page
    (repo not in their authorized push set), so they publish small patch
    files through the GitHub API instead; this applies them at render
    time, and db_maintenance.py folds them back into the page on the
    fortnightly ratings-refresh workflow. A full re-land of the page with
    a newer hero 'compiled' date retires the series automatically.
    Fail-soft: any problem falls back to the committed page as-is.
    """
    html = _HTML_PATH.read_text(encoding="utf-8")
    try:
        man_path = _PATCH_DIR / "manifest.json"
        if not man_path.exists():
            return html
        man = _json.loads(man_path.read_text(encoding="utf-8"))
        series_date = _dt.date.fromisoformat(str(man.get("compiled", ""))[:10])
        base_date = _hero_date(html)
        if base_date and base_date > series_date:
            return html  # page re-landed in full after the series was cut
        for fname in man.get("series", []):
            if not _re.fullmatch(r"patch_\d{3}\.json", str(fname)):
                continue
            fpath = _PATCH_DIR / fname
            if not fpath.exists():
                continue
            for pair in _json.loads(fpath.read_text(encoding="utf-8")):
                find = pair.get("find", "")
                if find and html.count(find) == 1:
                    html = html.replace(find, pair.get("replace", ""))
        return html
    except Exception:
        return _HTML_PATH.read_text(encoding="utf-8")


components.html(
    _load_page(),
    height=2600,   # fallback only; iframe auto-resizes via setFrameHeight
    scrolling=True,
)
