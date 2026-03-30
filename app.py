import streamlit as st

from modules.ui_text import t
from modules.incidents import render_incidents
from modules.diagnostics import render_diagnostics
from modules.settings import render_settings
from modules.today import render_daily_report
from modules.analytics import render_analytics
from modules.reports import render_reports

st.set_page_config(page_title="Aquantis — Farm Intelligence (DEMO)", page_icon="🐟", layout="wide")

st.title("🐟 Aquantis — Farm Intelligence (DEMO)")
st.caption("Recommendations are for decision support only. Always verify measurements and confirm actions before execution.")

tab_daily, tab_analytics, tab_event, tab_inc, tab_reports, tab_settings = st.tabs(
    [
        t("tab.daily"),
        t("tab.analytics"),
        t("tab.new_event"),
        t("tab.incidents"),
        t("tab.reports"),
        t("tab.settings"),
    ]
)

with tab_daily:
    render_daily_report()

with tab_analytics:
    render_analytics()

with tab_event:
    render_diagnostics()

with tab_inc:
    render_incidents()

with tab_reports:
    render_reports()

with tab_settings:
    render_settings()