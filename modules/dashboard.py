import streamlit as st
import pandas as pd

import aquantis_storage
from modules.dt import to_utc_datetime, utc_days_ago
import os, inspect
import streamlit as st

st.write("aquantis_storage file:", getattr(aquantis_storage, "__file__", "NO_FILE"))
st.write("cwd:", os.getcwd())
st.write("attrs:", [x for x in dir(aquantis_storage) if "incident" in x.lower() or "load" in x.lower()])

def render_dashboard():
    st.subheader("📊 Dashboard")

    records = aquantis_storage.load_incidents()
    df = aquantis_storage.records_to_df(records)
    if df.empty:
        st.info("No incidents yet.")
        return

    if "timestamp_utc" in df.columns:
        df["timestamp_dt"] = to_utc_datetime(df["timestamp_utc"])
    else:
        df["timestamp_dt"] = pd.NaT

    if "status" not in df.columns:
        df["status"] = "OPEN"

    open_df = df[df["status"].fillna("OPEN") == "OPEN"].copy()

    days_30_ts = utc_days_ago(30)
    recent_30 = df[df["timestamp_dt"].notna() & (df["timestamp_dt"] >= days_30_ts)]

    k1, k2, k3 = st.columns(3)
    k1.metric("OPEN incidents", int(len(open_df)))
    k2.metric("Total incidents", int(len(df)))
    k3.metric("Last 30 days", int(len(recent_30)))

    st.divider()

    st.markdown("### Top triggers (OPEN only)")
    trig_counts = {}
    for _, r in open_df.iterrows():
        tb = r.get("triggered_by", None)

        # triggered_by can be: list, comma-separated string, NaN/None, or other junk
        if tb is None or (isinstance(tb, float) and pd.isna(tb)):
            tb = []
        elif isinstance(tb, str):
            tb = [x.strip() for x in tb.split(",") if x.strip()]
        elif isinstance(tb, (list, tuple, set)):
            tb = list(tb)
        else:
            tb = []

        for t in tb:
            trig_counts[t] = trig_counts.get(t, 0) + 1

    if not trig_counts:
        st.write("No triggers logged.")
    else:
        items = sorted(trig_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        st.dataframe(pd.DataFrame(items, columns=["trigger", "count"]), use_container_width=True)

    if "resolved_at_utc" in df.columns:
        df["resolved_at_dt"] = to_utc_datetime(df["resolved_at_utc"])
        resolved = df[df["status"].fillna("OPEN") == "RESOLVED"].copy()
        if not resolved.empty and resolved["timestamp_dt"].notna().any() and resolved["resolved_at_dt"].notna().any():
            resolved["ttr_hours"] = (resolved["resolved_at_dt"] - resolved["timestamp_dt"]).dt.total_seconds() / 3600.0
            ttr = resolved["ttr_hours"].dropna()
            if len(ttr) > 0:
                st.markdown("### Time-to-resolve (hours)")
                st.metric("Median TTR (h)", float(ttr.median()))
                st.metric("Avg TTR (h)", float(ttr.mean()))
