from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

import ops_storage as ops
from modules.dt import to_utc_datetime
from modules.settings import load_farm_profiles, get_active_farm
from modules.ui_text import t


# -----------------------------
# Helpers
# -----------------------------
PARAM_OPTIONS = {
    "ph": "pH",
    "temp": "Temp",
    "salinity": "Salinity",
    "no2": "NO2",
    "no3": "NO3",
    "nh3": "NH3",
    "nh4": "NH4",
    "tan": "TAN",
    "unit_o2_min": "O2 min",
    "unit_o2_avg": "O2 avg",
    "unit_o2_spread": "O2 spread",
    "planned_feed_kg_total": "Planned feed total",
    "mortality_total_n": "Mortality total",
}

RANGE_OPTIONS = {
    "7 days": 7,
    "30 days": 30,
    "All": None,
}


def _safe_dict(x):
    return x if isinstance(x, dict) else {}


def _safe_list(x):
    return x if isinstance(x, list) else []


def _p(text: Any) -> str:
    if text is None:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br/>")
    )


def _farm_options(db: dict) -> List[dict]:
    farms = db.get("farms") or []
    return [f for f in farms if isinstance(f, dict)]


def _farm_label(f: dict) -> str:
    return f"{f.get('farm_name','Farm')} ({f.get('farm_id','')})"


def _get_unit_options(farm: dict) -> List[dict]:
    units = farm.get("units") or []
    return [u for u in units if isinstance(u, dict)]


def _flatten_daily_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "timestamp_utc" in df.columns:
        df["timestamp_dt"] = to_utc_datetime(df["timestamp_utc"])
        try:
            df["day"] = df["timestamp_dt"].dt.strftime("%Y-%m-%d")
        except Exception:
            df["day"] = df["timestamp_utc"].astype(str).str[:10]
    else:
        df["timestamp_dt"] = pd.NaT
        df["day"] = ""

    for k in ["ph", "temp", "salinity", "no2", "no3", "nh3", "nh4", "tan"]:
        col = f"values.{k}"
        if col in df.columns and k not in df.columns:
            df[k] = pd.to_numeric(df[col], errors="coerce")

    for col in ["unit_o2_min", "unit_o2_avg", "unit_o2_spread", "planned_feed_kg_total", "mortality_total_n"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _filter_range(df: pd.DataFrame, days: Optional[int]) -> pd.DataFrame:
    if df.empty or days is None or "timestamp_dt" not in df.columns:
        return df
    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(days=days)
    return df[(df["timestamp_dt"].notna()) & (df["timestamp_dt"] >= start) & (df["timestamp_dt"] <= end)]


def _daily_last_value_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep last record per day for parameter charts.
    """
    if df.empty:
        return df
    dfx = df.copy()
    if "timestamp_dt" in dfx.columns and dfx["timestamp_dt"].notna().any():
        dfx = dfx.sort_values("timestamp_dt", ascending=True)
    if "day" not in dfx.columns:
        dfx["day"] = dfx["timestamp_utc"].astype(str).str[:10]
    return dfx.drop_duplicates(subset=["day"], keep="last")


def _daily_total_mortality_df(records: List[dict], farm_id: str, unit_id: str) -> pd.DataFrame:
    rows = []
    for rec in records:
        if str(rec.get("farm_id")) != str(farm_id):
            continue
        if str(rec.get("unit_id")) != str(unit_id):
            continue
        ts = rec.get("timestamp_utc")
        day = str(ts)[:10]
        rows.append(
            {
                "timestamp_utc": ts,
                "day": day,
                "mortality_total_n": rec.get("mortality_total_n"),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["mortality_total_n"] = pd.to_numeric(df["mortality_total_n"], errors="coerce").fillna(0)
    return df.groupby("day", as_index=False)["mortality_total_n"].sum()


def _extract_tank_daily_df(records: List[dict], farm_id: str, unit_id: str) -> pd.DataFrame:
    rows = []
    for rec in records:
        if str(rec.get("farm_id")) != str(farm_id):
            continue
        if str(rec.get("unit_id")) != str(unit_id):
            continue

        ts = rec.get("timestamp_utc")
        day = str(ts)[:10]
        tank_entries = _safe_list(rec.get("tank_entries"))
        for tnk in tank_entries:
            td = _safe_dict(tnk)
            rows.append(
                {
                    "timestamp_utc": ts,
                    "day": day,
                    "tank_id": td.get("tank_id"),
                    "tank_name": td.get("tank_name"),
                    "mortality_n": td.get("mortality_n"),
                    "o2_mg_l": td.get("o2_mg_l"),
                    "biomass_kg": td.get("biomass_kg"),
                    "avg_weight_g": td.get("avg_weight_g"),
                    "feed_rate_pct_day": td.get("feed_rate_pct_day"),
                    "planned_feed_kg_day": td.get("planned_feed_kg_day"),
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp_dt"] = to_utc_datetime(df["timestamp_utc"])
    for c in ["mortality_n", "o2_mg_l", "biomass_kg", "avg_weight_g", "feed_rate_pct_day", "planned_feed_kg_day"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _filter_tank_range(df: pd.DataFrame, days: Optional[int]) -> pd.DataFrame:
    if df.empty or days is None or "timestamp_dt" not in df.columns:
        return df
    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(days=days)
    return df[(df["timestamp_dt"].notna()) & (df["timestamp_dt"] >= start) & (df["timestamp_dt"] <= end)]


def _tank_mortality_daily(tank_df: pd.DataFrame, tank_name: Optional[str] = None) -> pd.DataFrame:
    if tank_df.empty:
        return pd.DataFrame()
    dfx = tank_df.copy()
    if tank_name:
        dfx = dfx[dfx["tank_name"].astype(str) == str(tank_name)]
    if dfx.empty:
        return pd.DataFrame()
    return dfx.groupby("day", as_index=False)["mortality_n"].sum()


def _tank_o2_daily_last(tank_df: pd.DataFrame, tank_name: Optional[str] = None) -> pd.DataFrame:
    if tank_df.empty:
        return pd.DataFrame()
    dfx = tank_df.copy()
    if tank_name:
        dfx = dfx[dfx["tank_name"].astype(str) == str(tank_name)]
    if dfx.empty:
        return pd.DataFrame()
    dfx = dfx.sort_values("timestamp_dt", ascending=True)
    dfx = dfx.drop_duplicates(subset=["day"], keep="last")
    return dfx[["day", "o2_mg_l"]].copy()


def _first_non_empty(*values):
    for v in values:
        if v is None:
            continue
        if isinstance(v, str):
            if v.strip():
                return v.strip()
        else:
            return v
    return None


def _fmt_value(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        f = float(v)
        if abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        if abs(f) >= 100:
            return f"{f:.1f}"
        if abs(f) >= 10:
            return f"{f:.2f}".rstrip("0").rstrip(".")
        return f"{f:.3f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)


def _extract_record_values(record: dict) -> dict:
    """
    Returns measurement values for both raw records and flattened DataFrame rows.
    Priority:
    1) nested record["values"]
    2) flat keys on record (ph, temp, salinity, no2, no3, nh3, nh4, tan)
    """
    nested = _safe_dict(record.get("values"))
    out: dict[str, Any] = {}

    if nested:
        out.update(nested)

    for key in ["ph", "temp", "salinity", "no2", "no3", "nh3", "nh4", "tan"]:
        if key in record:
            value = record.get(key)
            if value is None:
                continue
            if isinstance(value, float) and pd.isna(value):
                continue
            out[key] = value

    return out


def _build_daily_log_pdf(record: dict) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=50,
        bottomMargin=40,
    )

    styles = getSampleStyleSheet()
    normal = ParagraphStyle("NormalX", parent=styles["Normal"], fontSize=10, leading=14)
    heading = ParagraphStyle("HeadingX", parent=styles["Heading2"], fontSize=14, leading=18, spaceAfter=10)
    subheading = ParagraphStyle("SubHeadingX", parent=styles["Heading3"], fontSize=11, leading=15, spaceAfter=6)

    farm_name = _first_non_empty(record.get("farm_name"), "Farm")
    unit_name = _first_non_empty(record.get("unit_name"), "Unit")
    operator = _first_non_empty(record.get("operator"))
    timestamp_utc = _first_non_empty(record.get("timestamp_utc"))
    day_txt = _first_non_empty(record.get("day"), str(timestamp_utc)[:10] if timestamp_utc else None)
    risk_level = _first_non_empty(record.get("risk_level"), "—")
    planned_feed = record.get("planned_feed_kg_total")
    mortality_total = record.get("mortality_total_n")
    note = _first_non_empty(record.get("note"))

    values = _extract_record_values(record)
    tank_entries = _safe_list(record.get("tank_entries"))
    top_risks = _safe_list(record.get("top_risks"))
    checklist = _safe_list(record.get("checklist"))
    signals = _safe_list(record.get("signals"))

    unit_o2_min = record.get("unit_o2_min")
    unit_o2_avg = record.get("unit_o2_avg")
    unit_o2_spread = record.get("unit_o2_spread")

    header_title = f"{farm_name} - {unit_name} - {t('analytics.pdf.daily_report', 'Daily report')}"

    main_issues = ", ".join([str(x) for x in top_risks[:3] if str(x).strip()]) if top_risks else t(
        "analytics.pdf.no_major_issues",
        "No major issues flagged.",
    )
    recommendation = _first_non_empty(
        checklist[0] if checklist else None,
        t("analytics.pdf.continue_monitoring", "Continue routine monitoring."),
    )

    elements = []
    elements.append(Paragraph(_p(header_title), heading))
    elements.append(Spacer(1, 10))

    # 1) Executive summary
    elements.append(Paragraph(t("analytics.pdf.executive_summary", "Executive summary"), subheading))
    elements.append(Paragraph(f"<b>{_p(t('label.date', 'Date'))}:</b> {_p(day_txt)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('label.operator', 'Operator'))}:</b> {_p(operator or '—')}", normal))
    elements.append(Paragraph(f"<b>{_p(t('analytics.pdf.risk_level', 'Risk level'))}:</b> {_p(risk_level)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('analytics.pdf.main_issues', 'Main issues'))}:</b> {_p(main_issues)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('analytics.metric.mortality_total', 'Mortality total'))}:</b> {_p(_fmt_value(mortality_total))}", normal))
    elements.append(
        Paragraph(
            f"<b>{_p(t('analytics.metric.planned_feed_total', 'Planned feed total'))}:</b> {_p(_fmt_value(planned_feed))} kg/day",
            normal,
        )
    )
    elements.append(Paragraph(f"<b>{_p(t('analytics.pdf.recommendation', 'Recommendation'))}:</b> {_p(recommendation)}", normal))
    elements.append(Spacer(1, 8))

    # 2) Operational snapshot
    elements.append(Paragraph(t("analytics.pdf.operational_snapshot", "Operational snapshot"), subheading))
    snapshot_items = {
        t("label.farm", "Farm"): farm_name,
        t("today.field.unit", "Unit"): unit_name,
        t("analytics.metric.tanks", "Tanks"): len(tank_entries),
        t("analytics.metric.planned_feed_total_kg_day", "Planned feed total (kg/day)"): _fmt_value(planned_feed),
        t("analytics.metric.mortality_total", "Mortality total"): _fmt_value(mortality_total),
        t("today.metric.lowest_o2", "Lowest O2"): _fmt_value(unit_o2_min),
        t("today.metric.average_o2", "Average O2"): _fmt_value(unit_o2_avg),
        t("today.metric.o2_spread", "O2 spread"): _fmt_value(unit_o2_spread),
        t("analytics.pdf.timestamp_utc", "Timestamp (UTC)"): timestamp_utc,
    }
    for k, v in snapshot_items.items():
        if v is None or str(v).strip() == "":
            continue
        elements.append(Paragraph(f"<b>{_p(k)}:</b> {_p(v)}", normal))
    elements.append(Spacer(1, 8))

    # 3) Water quality summary
    elements.append(Paragraph(t("analytics.pdf.water_quality_summary", "Water quality summary"), subheading))
    if values:
        param_order = ["ph", "temp", "salinity", "no2", "no3", "nh3", "nh4", "tan"]
        printed = 0
        for k in param_order:
            if k not in values:
                continue
            label = PARAM_OPTIONS.get(k, k)
            elements.append(Paragraph(f"- {_p(label)}: {_p(_fmt_value(values.get(k)))}", normal))
            printed += 1
        for k, v in values.items():
            if k in param_order:
                continue
            elements.append(Paragraph(f"- {_p(PARAM_OPTIONS.get(k, k))}: {_p(_fmt_value(v))}", normal))
            printed += 1
        if printed == 0:
            elements.append(Paragraph("- —", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 8))

    # 4) Tank summary
    if tank_entries:
        elements.append(Paragraph(t("analytics.pdf.tank_summary", "Tank summary"), subheading))
        for tnk in tank_entries:
            td = _safe_dict(tnk)
            tank_title = _first_non_empty(td.get("tank_name"), td.get("tank_id"), t("label.tank", "Tank"))
            elements.append(Paragraph(f"<b>{_p(tank_title)}</b>", normal))
            tank_lines = [
                f"{t('analytics.table.biomass_kg', 'Biomass (kg)')}: {_fmt_value(td.get('biomass_kg'))} kg",
                f"{t('analytics.table.avg_weight_g', 'Avg wt (g)')}: {_fmt_value(td.get('avg_weight_g'))} g",
                f"{t('analytics.table.feed_kg_day', 'Feed - kg/day')}: {_fmt_value(td.get('planned_feed_kg_day'))}",
                f"{t('analytics.table.o2', 'O2')}: {_fmt_value(td.get('o2_mg_l'))}",
                f"{t('analytics.table.mortality', 'Mortality')}: {_fmt_value(td.get('mortality_n'))}",
            ]
            for line in tank_lines:
                elements.append(Paragraph(f"- {_p(line)}", normal))
            elements.append(Spacer(1, 4))
        elements.append(Spacer(1, 4))

    # 5) Top risks and signals
    elements.append(Paragraph(t("analytics.pdf.top_risks_and_signals", "Top risks and signals"), subheading))
    elements.append(Paragraph(f"<b>{_p(t('today.top_risks.title', 'Top risks'))}:</b>", normal))
    if top_risks:
        for x in top_risks:
            elements.append(Paragraph(f"- {_p(x)}", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph(f"<b>{_p(t('today.signals.title', 'Signals'))}:</b>", normal))
    if signals:
        for x in signals[:30]:
            elements.append(Paragraph(f"- {_p(x)}", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 8))

    # 6) Checklist / recommended follow-up
    elements.append(Paragraph(t("analytics.pdf.checklist_followup", "Checklist / recommended follow-up"), subheading))
    if checklist:
        for x in checklist:
            elements.append(Paragraph(f"- {_p(x)}", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 8))

    # 7) Notes
    elements.append(Paragraph(t("label.notes", "Notes"), subheading))
    if note:
        elements.append(Paragraph(_p(note), normal))
    else:
        elements.append(Paragraph("—", normal))
    elements.append(Spacer(1, 8))

    # 8) Appendix
    elements.append(Paragraph(t("analytics.pdf.appendix", "Appendix"), subheading))
    elements.append(
        Paragraph(
            f"{_p(t('analytics.pdf.generated_utc', 'Generated (UTC)'))}: {_p(datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))}",
            normal,
        )
    )

    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()


# -----------------------------
# Main
# -----------------------------
def render_analytics():
    st.subheader(t("analytics.title", "Analytics"))

    db = load_farm_profiles()
    farms = _farm_options(db)
    active_farm = get_active_farm(db)

    if not farms:
        st.warning(t("analytics.msg.no_farms", "No farms found."))
        st.info(t("analytics.msg.go_settings_create_farm", "Go to Settings → create farm."))
        return

    records = ops.load_daily_logs()
    df = ops.logs_to_df(records)
    if df.empty:
        st.info(t("analytics.msg.no_daily_logs", "No daily logs yet."))
        return

    df = _flatten_daily_df(df)

    # -----------------------------
    # Filters
    # -----------------------------
    c1, c2, c3, c4 = st.columns([2.0, 1.6, 1.0, 1.4])

    farm_labels = [_farm_label(f) for f in farms]
    default_idx = 0
    if active_farm:
        for i, f in enumerate(farms):
            if str(f.get("farm_id")) == str(active_farm.get("farm_id")):
                default_idx = i
                break

    with c1:
        farm_label = st.selectbox(t("label.farm", "Farm"), farm_labels, index=default_idx, key="analytics_farm")
    farm = farms[farm_labels.index(farm_label)]
    farm_id = farm.get("farm_id")

    units = _get_unit_options(farm)
    if not units:
        st.warning(t("analytics.msg.no_units", "No units in this farm."))
        return

    unit_labels = [(u.get("unit_name") or t("settings.unit.fallback_name", "Unit")).strip() or t("settings.unit.fallback_name", "Unit") for u in units]
    with c2:
        unit_label = st.selectbox(t("today.field.unit", "Unit"), unit_labels, index=0, key="analytics_unit")
    unit = units[unit_labels.index(unit_label)]
    unit_id = unit.get("unit_id")

    with c3:
        range_label = st.selectbox(t("analytics.field.range", "Range"), list(RANGE_OPTIONS.keys()), index=0, key="analytics_range")
    days = RANGE_OPTIONS[range_label]

    with c4:
        param_key = st.selectbox(
            t("analytics.field.parameter", "Parameter"),
            options=list(PARAM_OPTIONS.keys()),
            format_func=lambda x: PARAM_OPTIONS.get(x, x),
            index=0,
            key="analytics_param",
        )

    # filter daily df
    if "farm_id" in df.columns:
        df = df[df["farm_id"].astype(str) == str(farm_id)]
    if "unit_id" in df.columns:
        df = df[df["unit_id"].astype(str) == str(unit_id)]

    df = _filter_range(df, days)

    if df.empty:
        st.info(t("analytics.msg.no_logs_for_selection", "No daily logs for selected farm/unit."))
        return

    dfx = df.sort_values("timestamp_dt")

    # Prepare reusable datasets
    daily_last = _daily_last_value_df(dfx)

    total_mort_df = _daily_total_mortality_df(records, str(farm_id), str(unit_id))
    total_mort_df = total_mort_df[total_mort_df["day"].isin(dfx["day"].astype(str).unique())] if not total_mort_df.empty else total_mort_df

    tank_df = _extract_tank_daily_df(records, str(farm_id), str(unit_id))
    tank_df = _filter_tank_range(tank_df, days)

    tank_names = []
    selected_tank = None
    if not tank_df.empty:
        tank_names = sorted([x for x in tank_df["tank_name"].dropna().astype(str).unique().tolist() if x.strip()])
        if tank_names:
            selected_tank = tank_names[0]

    # -----------------------------
    # Single parameter chart
    # -----------------------------
    param_label = PARAM_OPTIONS.get(param_key, param_key)
    st.markdown(f"### {t('analytics.parameter_chart.title', 'Parameter chart')} - {param_label}")

    if param_key in daily_last.columns:
        chart_df = daily_last[["day", param_key]].copy().dropna(subset=[param_key])
        if chart_df.empty:
            st.info(t("analytics.msg.no_data_for_parameter", "No data for selected parameter."))
        else:
            chart_df = chart_df.set_index("day")
            st.line_chart(chart_df, use_container_width=True)
    else:
        st.info(t("analytics.msg.parameter_not_found", "Selected parameter not found in daily logs."))

    # -----------------------------
    # Mortality
    # -----------------------------
    st.markdown(f"### {t('analytics.mortality.title', 'Mortality')}")

    if total_mort_df.empty:
        st.info(t("analytics.msg.no_mortality_data", "No mortality data yet."))
    else:
        st.markdown(f"**{t('analytics.total_mortality_unit.title', 'Total mortality (unit)')}**")
        st.line_chart(total_mort_df.set_index("day")[["mortality_total_n"]], use_container_width=True)

    if tank_df.empty:
        st.info(t("analytics.msg.no_tank_level_data", "No tank-level data yet."))
    else:
        if tank_names:
            selected_tank = st.selectbox(t("label.tank", "Tank"), tank_names, index=0, key="analytics_tank_select")

        if selected_tank:
            tank_mort = _tank_mortality_daily(tank_df, selected_tank)
            if not tank_mort.empty:
                st.markdown(f"**{t('analytics.mortality_tank.title', 'Mortality')} – {selected_tank}**")
                st.line_chart(tank_mort.set_index("day")[["mortality_n"]], use_container_width=True)

            tank_o2 = _tank_o2_daily_last(tank_df, selected_tank)
            if not tank_o2.empty:
                st.markdown(f"**{t('analytics.o2_tank.title', 'O2')} – {selected_tank}**")
                st.line_chart(tank_o2.set_index("day")[["o2_mg_l"]], use_container_width=True)