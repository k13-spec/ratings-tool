"""
Financing Ideas — pre-screened INR debt financing candidates, priority-ranked.

Static research snapshot (compiled 7 Jul 2026) rendered from
assets/debt_financing_ideas.html. Each candidate card deep-links back to
Company_Deep_Dive (?company=) and the bond tracker (?issuer=).

To refresh the underlying research or add a company, regenerate the HTML
via Claude ("refresh the financing ideas page" / "run a workup on X") and
re-upload assets/debt_financing_ideas.html.
"""
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
    </style>
    """,
    unsafe_allow_html=True,
)

if not _HTML_PATH.exists():
    st.error("assets/debt_financing_ideas.html not found in the repo.")
    st.stop()

components.html(
    _HTML_PATH.read_text(encoding="utf-8"),
    height=2600,
    scrolling=True,
)
