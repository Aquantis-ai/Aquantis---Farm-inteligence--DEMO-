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


def _filter_last_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty or "timestamp_dt" not in df.columns:
        return df
    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(days=days)
    return df[(df["timestamp_dt"].notna()) & (df["timestamp_dt"] >= start) & (df["timestamp_dt"] <= end)]


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    try:
        return bool(pd.isna(v))
    except Exception:
        return False


def _first_non_empty(*values):
    for v in values:
        if _is_missing(v):
            continue
        if isinstance(v, str):
            return v.strip()
        return v
    return None


def _fmt_value(v: Any) -> str:
    if v is None:
        return "—"
    try:
        if pd.isna(v):
            return "—"
    except Exception:
        pass

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


def _latest_row_per_day(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    dfx = df.copy()
    if "timestamp_dt" in dfx.columns:
        dfx = dfx.sort_values("timestamp_dt", ascending=True)
    return dfx.drop_duplicates(subset=["day"], keep="last").reset_index(drop=True)


def _daily_day_level_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    dfx = _latest_row_per_day(df)

    if "mortality_total_n" not in dfx.columns:
        dfx["mortality_total_n"] = 0
    if "risk_level" not in dfx.columns:
        dfx["risk_level"] = "—"

    dfx["mortality_total_n"] = pd.to_numeric(dfx["mortality_total_n"], errors="coerce").fillna(0)
    dfx["risk_rank_worst"] = dfx["risk_level"].astype(str).str.upper().map({"HIGH": 2, "WATCH": 1, "OK": 0}).fillna(0)
    dfx["risk_rank_best"] = dfx["risk_level"].astype(str).str.upper().map({"OK": 0, "WATCH": 1, "HIGH": 2}).fillna(3)

    return dfx


def _extract_record_values(record: dict) -> dict:
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

    header_title = f"{farm_name} - {unit_name} - {t('reports.pdf.daily_report', 'Daily report')}"

    main_issues = ", ".join([str(x) for x in top_risks[:3] if str(x).strip()]) if top_risks else t(
        "reports.pdf.no_major_issues",
        "No major issues flagged.",
    )
    recommendation = _first_non_empty(
        checklist[0] if checklist else None,
        t("reports.pdf.continue_monitoring", "Continue routine monitoring."),
    )

    elements = []
    elements.append(Paragraph(_p(header_title), heading))
    elements.append(Spacer(1, 10))

    elements.append(Paragraph(t("reports.pdf.executive_summary", "Executive summary"), subheading))
    elements.append(Paragraph(f"<b>{_p(t('label.date', 'Date'))}:</b> {_p(day_txt)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('label.operator', 'Operator'))}:</b> {_p(operator or '—')}", normal))
    elements.append(Paragraph(f"<b>{_p(t('reports.pdf.risk_level', 'Risk level'))}:</b> {_p(risk_level)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('reports.pdf.main_issues', 'Main issues'))}:</b> {_p(main_issues)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('reports.metric.mortality_total', 'Mortality total'))}:</b> {_p(_fmt_value(mortality_total))}", normal))
    elements.append(
        Paragraph(
            f"<b>{_p(t('reports.metric.planned_feed', 'Planned feed'))}:</b> {_p(_fmt_value(planned_feed))} kg/day",
            normal,
        )
    )
    elements.append(Paragraph(f"<b>{_p(t('reports.pdf.recommendation', 'Recommendation'))}:</b> {_p(recommendation)}", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.operational_snapshot", "Operational snapshot"), subheading))
    snapshot_items = {
        t("label.farm", "Farm"): farm_name,
        t("today.field.unit", "Unit"): unit_name,
        t("reports.metric.tanks", "Tanks"): len(tank_entries),
        t("reports.metric.planned_feed_total_kg_day", "Planned feed total (kg/day)"): _fmt_value(planned_feed),
        t("reports.metric.mortality_total", "Mortality total"): _fmt_value(mortality_total),
        t("today.metric.lowest_o2", "Lowest O2"): _fmt_value(unit_o2_min),
        t("today.metric.average_o2", "Average O2"): _fmt_value(unit_o2_avg),
        t("today.metric.o2_spread", "O2 spread"): _fmt_value(unit_o2_spread),
        t("reports.pdf.timestamp_utc", "Timestamp (UTC)"): timestamp_utc,
    }
    for k, v in snapshot_items.items():
        if v is None or str(v).strip() == "":
            continue
        elements.append(Paragraph(f"<b>{_p(k)}:</b> {_p(v)}", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.water_quality_summary", "Water quality summary"), subheading))
    if values:
        param_order = ["ph", "temp", "salinity", "no2", "no3", "nh3", "nh4", "tan"]
        printed = 0
        for k in param_order:
            if k not in values:
                continue
            label = {
                "ph": "pH",
                "temp": "Temp",
                "salinity": "Salinity",
                "no2": "NO2",
                "no3": "NO3",
                "nh3": "NH3",
                "nh4": "NH4",
                "tan": "TAN",
            }.get(k, k)
            elements.append(Paragraph(f"- {_p(label)}: {_p(_fmt_value(values.get(k)))}", normal))
            printed += 1
        for k, v in values.items():
            if k in param_order:
                continue
            elements.append(Paragraph(f"- {_p(k)}: {_p(_fmt_value(v))}", normal))
            printed += 1
        if printed == 0:
            elements.append(Paragraph("- —", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 8))

    if tank_entries:
        elements.append(Paragraph(t("reports.pdf.tank_summary", "Tank summary"), subheading))
        for tank in tank_entries:
            td = _safe_dict(tank)
            tank_title = _first_non_empty(td.get("tank_name"), td.get("tank_id"), t("label.tank", "Tank"))
            elements.append(Paragraph(f"<b>{_p(tank_title)}</b>", normal))
            tank_lines = [
                f"{t('reports.table.biomass', 'Biomass')}: {_fmt_value(td.get('biomass_kg'))} kg",
                f"{t('reports.table.avg_weight', 'Avg weight')}: {_fmt_value(td.get('avg_weight_g'))} g",
                f"{t('reports.table.feed_kg_day', 'Feed - kg/day')}: {_fmt_value(td.get('planned_feed_kg_day'))}",
                f"{t('reports.table.o2', 'O2')}: {_fmt_value(td.get('o2_mg_l'))}",
                f"{t('reports.table.mortality', 'Mortality')}: {_fmt_value(td.get('mortality_n'))}",
            ]
            for line in tank_lines:
                elements.append(Paragraph(f"- {_p(line)}", normal))
            elements.append(Spacer(1, 4))
        elements.append(Spacer(1, 4))

    elements.append(Paragraph(t("reports.pdf.top_risks_and_signals", "Top risks and signals"), subheading))
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

    elements.append(Paragraph(t("reports.pdf.checklist_followup", "Checklist / recommended follow-up"), subheading))
    if checklist:
        for x in checklist:
            elements.append(Paragraph(f"- {_p(x)}", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("label.notes", "Notes"), subheading))
    if note:
        elements.append(Paragraph(_p(note), normal))
    else:
        elements.append(Paragraph("—", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.appendix", "Appendix"), subheading))
    elements.append(
        Paragraph(
            f"{_p(t('reports.pdf.generated_utc', 'Generated (UTC)'))}: {_p(datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))}",
            normal,
        )
    )

    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()


def _build_weekly_report_pdf(summary: dict) -> bytes:
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

    farm_name = _first_non_empty(summary.get("farm_name"), "Farm")
    unit_name = _first_non_empty(summary.get("unit_name"), "Unit")
    period = _first_non_empty(summary.get("period"), t("reports.pdf.selected_period", "Selected period"))

    metrics = _safe_dict(summary.get("metrics"))
    top_risks = _safe_list(summary.get("top_risks"))
    notes = _safe_list(summary.get("notes"))
    daily_rows = _safe_list(summary.get("daily_rows"))

    logs_n = metrics.get("Logs")
    mortality_total = metrics.get("Mortality total")
    avg_o2_per_unit = metrics.get("Average O2 per unit")
    avg_feed_day = metrics.get("Average feed/day")
    total_feed_period = metrics.get("Total feed period")
    high_risk_days = metrics.get("High-risk days")
    overall_status = metrics.get("Overall status")
    worst_day = metrics.get("Worst day")
    best_day = metrics.get("Best day")

    main_issue = _first_non_empty(
        top_risks[0] if top_risks else None,
        t("reports.pdf.no_major_repeated_issue", "No major repeated issue identified."),
    )
    management_takeaway = _first_non_empty(
        notes[0] if notes else None,
        t(
            "reports.pdf.management_takeaway_default",
            "Continue routine monitoring and maintain current operating discipline.",
        ),
    )
    recommendation = _first_non_empty(
        notes[1] if len(notes) > 1 else None,
        t(
            "reports.pdf.recommendation_next_period",
            "Review the main repeated issue and confirm actions for the next reporting period.",
        ),
    )

    timeline_rows = []
    for row in daily_rows:
        rd = _safe_dict(row)
        risk = str(rd.get("risk_level") or "").strip().upper()
        mortality = rd.get("mortality_total_n")
        try:
            mortality_n = float(mortality) if mortality is not None else 0.0
        except Exception:
            mortality_n = 0.0

        if risk in {"HIGH", "WATCH"} or mortality_n > 0:
            timeline_rows.append(rd)

    header_title = f"{farm_name} - {unit_name} - {t('reports.pdf.weekly_performance_report', 'Weekly performance report')}"

    elements = []
    elements.append(Paragraph(_p(header_title), heading))
    elements.append(Spacer(1, 10))

    elements.append(Paragraph(t("reports.pdf.executive_summary", "Executive summary"), subheading))
    elements.append(Paragraph(f"<b>{_p(t('reports.pdf.period', 'Period'))}:</b> {_p(period)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('reports.pdf.main_issue', 'Main issue'))}:</b> {_p(main_issue)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('reports.metric.mortality_total', 'Mortality total'))}:</b> {_p(_fmt_value(mortality_total))}", normal))
    elements.append(
        Paragraph(
            f"<b>{_p(t('reports.metric.total_feed_for_period', 'Total feed for period'))}:</b> {_p(_fmt_value(total_feed_period))} kg",
            normal,
        )
    )
    elements.append(Paragraph(f"<b>{_p(t('reports.metric.overall_status', 'Overall status'))}:</b> {_p(_first_non_empty(overall_status, '—'))}", normal))
    elements.append(Paragraph(f"<b>{_p(t('reports.pdf.management_takeaway', 'Management takeaway'))}:</b> {_p(management_takeaway)}", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.kpi_snapshot", "KPI snapshot"), subheading))
    snapshot = {
        t("label.farm", "Farm"): farm_name,
        t("today.field.unit", "Unit"): unit_name,
        t("reports.metric.logs_reviewed", "Logs reviewed"): logs_n,
        t("reports.metric.high_risk_days", "High-risk days"): high_risk_days,
        t("reports.metric.avg_o2_per_unit", "Average O2 per unit"): avg_o2_per_unit,
        t("reports.metric.avg_feed_day", "Average feed/day"): avg_feed_day,
        t("reports.metric.total_feed_for_period", "Total feed for period"): total_feed_period,
        t("reports.metric.worst_day", "Worst day"): worst_day,
        t("reports.metric.best_day", "Best day"): best_day,
    }
    for k, v in snapshot.items():
        if _is_missing(v):
            continue
        elements.append(Paragraph(f"<b>{_p(k)}:</b> {_p(_fmt_value(v))}", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.losses_and_production_impact", "Losses and production impact"), subheading))
    if mortality_total is not None and str(mortality_total).strip() != "":
        elements.append(
            Paragraph(
                f"- {t('reports.pdf.total_mortality_period_line', 'Total mortality recorded during the period')}: {_p(_fmt_value(mortality_total))}.",
                normal,
            )
        )
    else:
        elements.append(
            Paragraph(
                f"- {t('reports.pdf.total_mortality_period_line', 'Total mortality recorded during the period')}: —",
                normal,
            )
        )

    if total_feed_period is not None and str(total_feed_period).strip() != "":
        elements.append(
            Paragraph(
                f"- {t('reports.pdf.total_planned_feed_period_line', 'Total planned feed during the period')}: {_p(_fmt_value(total_feed_period))} kg.",
                normal,
            )
        )
    else:
        elements.append(
            Paragraph(
                f"- {t('reports.pdf.total_planned_feed_period_line', 'Total planned feed during the period')}: —",
                normal,
            )
        )

    if avg_feed_day is not None and str(avg_feed_day).strip() != "":
        elements.append(
            Paragraph(
                f"- {t('reports.pdf.avg_daily_feed_line', 'Average daily feed')}: {_p(_fmt_value(avg_feed_day))} kg/day.",
                normal,
            )
        )

    if avg_o2_per_unit is not None and str(avg_o2_per_unit).strip() != "":
        elements.append(
            Paragraph(
                f"- {t('reports.pdf.avg_o2_period_line', 'Average O2 per unit during the period')}: {_p(_fmt_value(avg_o2_per_unit))}.",
                normal,
            )
        )

    if notes:
        for x in notes[:3]:
            elements.append(Paragraph(f"- {_p(x)}", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.weekly_operational_interpretation", "Weekly operational interpretation"), subheading))
    if top_risks:
        elements.append(Paragraph(f"<b>{_p(t('reports.pdf.repeated_issues_observed', 'Repeated issues observed'))}:</b>", normal))
        for x in top_risks[:5]:
            elements.append(Paragraph(f"- {_p(x)}", normal))
    else:
        elements.append(
            Paragraph(
                f"- {t('reports.pdf.no_repeated_high_priority_issue', 'No repeated high-priority issue was identified from the selected logs.')}",
                normal,
            )
        )
    elements.append(Spacer(1, 4))

    if notes:
        elements.append(Paragraph(f"<b>{_p(t('reports.pdf.interpretation', 'Interpretation'))}:</b>", normal))
        for x in notes[:5]:
            elements.append(Paragraph(f"- {_p(x)}", normal))
    else:
        elements.append(Paragraph(f"- {t('reports.pdf.no_additional_interpretation', 'No additional interpretation available.')}", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.action_plan_next_week", "Action plan for next week"), subheading))
    elements.append(Paragraph(f"<b>{_p(t('reports.pdf.recommended_priorities', 'Recommended priorities'))}:</b>", normal))
    if notes:
        for x in notes[:5]:
            elements.append(Paragraph(f"- {_p(x)}", normal))
    elif top_risks:
        for x in top_risks[:3]:
            elements.append(
                Paragraph(
                    f"- {t('reports.pdf.review_and_address_recurring_issue', 'Review and address recurring issue')}: {_p(x)}",
                    normal,
                )
            )
    else:
        elements.append(Paragraph(f"- {t('reports.pdf.continue_monitoring_weekly_review', 'Continue routine monitoring and weekly review.')}", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.key_events_timeline", "Key events timeline"), subheading))
    if timeline_rows:
        for row in timeline_rows[:15]:
            rd = _safe_dict(row)
            day = _first_non_empty(rd.get("day"), "—")
            risk = _first_non_empty(rd.get("risk_level"), "—")
            mortality = _fmt_value(rd.get("mortality_total_n"))
            o2_min = _fmt_value(rd.get("unit_o2_min"))
            feed = _fmt_value(rd.get("planned_feed_kg_total"))
            note = _first_non_empty(rd.get("note"))
            base_line = f"- {day} | risk={risk} | mortality={mortality} | O2 min={o2_min} | feed={feed}"
            elements.append(Paragraph(_p(base_line), normal))
            if note:
                elements.append(Paragraph(f"  {t('label.notes', 'Notes').lower()}: {_p(note)}", normal))
    else:
        elements.append(Paragraph(f"- {t('reports.pdf.no_key_events', 'No key events identified for the selected period.')}", normal))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph(t("reports.pdf.appendix", "Appendix"), subheading))
    if daily_rows:
        for row in daily_rows[:31]:
            rd = _safe_dict(row)
            day = _first_non_empty(rd.get("day"), "—")
            operator = _first_non_empty(rd.get("operator"), "—")
            risk = _first_non_empty(rd.get("risk_level"), "—")
            mortality = _fmt_value(rd.get("mortality_total_n"))
            o2_min = _fmt_value(rd.get("unit_o2_min"))
            feed = _fmt_value(rd.get("planned_feed_kg_total"))
            elements.append(
                Paragraph(
                    f"- {_p(day)} | operator={_p(operator)} | risk={_p(risk)} | mortality={_p(mortality)} | O2 min={_p(o2_min)} | feed={_p(feed)}",
                    normal,
                )
            )
    else:
        elements.append(Paragraph("- —", normal))

    elements.append(Spacer(1, 8))
    elements.append(Paragraph(f"<b>{t('reports.pdf.export_type', 'Export type')}:</b> {t('reports.pdf.weekly_performance_report', 'Weekly performance report')}", normal))
    elements.append(
        Paragraph(
            f"<b>{t('reports.pdf.generated_utc', 'Generated (UTC)')}:</b> {_p(datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))}",
            normal,
        )
    )

    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()


def render_reports():
    st.subheader(t("reports.title", "Reports"))

    db = load_farm_profiles()
    farms = _farm_options(db)
    active_farm = get_active_farm(db)

    if not farms:
        st.warning(t("reports.msg.no_farms", "No farms found."))
        st.info(t("reports.msg.go_settings_create_farm", "Go to Settings → create farm."))
        return

    records = ops.load_daily_logs()
    df = ops.logs_to_df(records)
    if df.empty:
        st.info(t("reports.msg.no_daily_logs", "No daily logs yet."))
        return

    df = _flatten_daily_df(df)

    c1, c2, c3 = st.columns([2.0, 1.6, 1.0])

    farm_labels = [_farm_label(f) for f in farms]
    default_idx = 0
    if active_farm:
        for i, f in enumerate(farms):
            if str(f.get("farm_id")) == str(active_farm.get("farm_id")):
                default_idx = i
                break

    with c1:
        farm_label = st.selectbox(t("label.farm", "Farm"), farm_labels, index=default_idx, key="reports_farm")
    farm = farms[farm_labels.index(farm_label)]
    farm_id = farm.get("farm_id")
    farm_name = farm.get("farm_name") or "Farm"

    units = _get_unit_options(farm)
    if not units:
        st.warning(t("reports.msg.no_units", "No units in this farm."))
        return

    unit_labels = [(u.get("unit_name") or t("settings.unit.fallback_name", "Unit")).strip() or t("settings.unit.fallback_name", "Unit") for u in units]
    with c2:
        unit_label = st.selectbox(t("today.field.unit", "Unit"), unit_labels, index=0, key="reports_unit")
    unit = units[unit_labels.index(unit_label)]
    unit_id = unit.get("unit_id")
    unit_name = unit.get("unit_name") or "Unit"

    period_options = {
        t("reports.period.7_days", "7 days"): 7,
        t("reports.period.30_days", "30 days"): 30,
    }
    with c3:
        period_label = st.selectbox(t("reports.field.period", "Period"), list(period_options.keys()), index=0, key="reports_period")
    period_days = period_options[period_label]

    if "farm_id" in df.columns:
        df = df[df["farm_id"].astype(str) == str(farm_id)]
    if "unit_id" in df.columns:
        df = df[df["unit_id"].astype(str) == str(unit_id)]

    df = _filter_last_days(df, int(period_days))

    if df.empty:
        st.info(t("reports.msg.no_logs_for_selection", "No daily logs for selected farm/unit and period."))
        return

    dfx = df.sort_values("timestamp_dt", ascending=False).reset_index(drop=True)
    dfx_day = _daily_day_level_df(df)

    total_logs = len(dfx)
    total_mortality = float(dfx["mortality_total_n"].fillna(0).sum()) if "mortality_total_n" in dfx.columns else 0.0
    avg_o2_per_unit = float(dfx["unit_o2_avg"].dropna().mean()) if "unit_o2_avg" in dfx.columns and dfx["unit_o2_avg"].notna().any() else None
    avg_feed = float(dfx["planned_feed_kg_total"].dropna().mean()) if "planned_feed_kg_total" in dfx.columns and dfx["planned_feed_kg_total"].notna().any() else None
    total_feed_period = float(dfx["planned_feed_kg_total"].dropna().sum()) if "planned_feed_kg_total" in dfx.columns and dfx["planned_feed_kg_total"].notna().any() else None

    risk_counts = dfx["risk_level"].fillna("—").value_counts().to_dict() if "risk_level" in dfx.columns else {}

    high_risk_days_n = 0
    if not dfx_day.empty and "risk_level" in dfx_day.columns:
        high_risk_days_n = int(dfx_day["risk_level"].astype(str).str.upper().isin(["HIGH"]).sum())

    worst_day = None
    if not dfx_day.empty:
        tmp = dfx_day.copy()
        tmp = tmp.sort_values(["mortality_total_n", "risk_rank_worst", "timestamp_dt"], ascending=[False, False, True])
        if not tmp.empty:
            worst_day = tmp.iloc[0].get("day")

    best_day = None
    if not dfx_day.empty:
        tmpb = dfx_day.copy()
        tmpb = tmpb.sort_values(["risk_rank_best", "mortality_total_n", "timestamp_dt"], ascending=[True, True, True])
        if not tmpb.empty:
            best_day = tmpb.iloc[0].get("day")

    if high_risk_days_n > 0:
        overall_status = t("reports.status.elevated_operational_risk", "Elevated operational risk") + f" ({high_risk_days_n} {t('reports.metric.high_risk_days', 'high-risk days').lower()})"
    elif total_mortality > 0:
        overall_status = t("reports.status.watch_mortality_recorded", "Watch - mortality recorded during the period")
    else:
        overall_status = t("reports.status.stable_operations", "Stable operations")

    top_risks_series = pd.Series(dtype=object)
    if "top_risks" in dfx.columns:
        expanded = []
        for item in dfx["top_risks"].tolist():
            if isinstance(item, list):
                expanded.extend(item)
        if expanded:
            top_risks_series = pd.Series(expanded).value_counts()

    top_risks = top_risks_series.head(5).index.tolist() if not top_risks_series.empty else []

    s1, s2, s3, s4 = st.columns(4)
    with s1:
        st.metric(t("reports.metric.logs", "Logs"), total_logs)
    with s2:
        st.metric(t("reports.metric.mortality_total", "Mortality total"), int(total_mortality))
    with s3:
        st.metric(t("reports.metric.avg_o2_per_unit", "Avg O2 per unit"), f"{avg_o2_per_unit:.2f}" if avg_o2_per_unit is not None else "—")
    with s4:
        st.metric(t("reports.metric.avg_feed_day", "Avg feed/day"), f"{avg_feed:.2f}" if avg_feed is not None else "—")

    st.markdown(f"### {t('reports.risk_overview.title', 'Risk overview')}")
    if risk_counts:
        for k, v in risk_counts.items():
            st.write(f"- {k}: {v}")

    st.markdown(f"### {t('reports.top_repeated_risks.title', 'Top repeated risks')}")
    if top_risks:
        for x in top_risks:
            st.write(f"- {x}")
    else:
        st.write("—")

    observations = []
    if total_mortality > 0:
        observations.append(
            f"{t('reports.obs.mortality_recorded_last_days', 'Mortality recorded during the last')} {period_days} {t('reports.obs.days', 'days')}: {int(total_mortality)}."
        )
    if avg_o2_per_unit is not None and avg_o2_per_unit < 6.5:
        observations.append(t("reports.obs.avg_o2_low", "Average O2 per unit is low and should be reviewed."))
    if top_risks:
        observations.append(f"{t('reports.obs.most_frequent_issue', 'Most frequent repeated issue this period')}: {top_risks[0]}.")
    if high_risk_days_n > 0:
        observations.append(f"{t('reports.obs.high_risk_days_recorded', 'High-risk days recorded during the period')}: {high_risk_days_n}.")
    if total_feed_period is not None:
        observations.append(f"{t('reports.obs.total_planned_feed_period', 'Total planned feed during the period')}: {total_feed_period:.2f} kg.")

    st.markdown(f"### {t('reports.observations.title', 'Observations')}")
    if observations:
        for x in observations:
            st.write(f"- {x}")
    else:
        st.write("—")

    pdf_daily_rows = dfx_day[
        [c for c in ["day", "operator", "risk_level", "unit_o2_min", "planned_feed_kg_total", "mortality_total_n", "note"] if c in dfx_day.columns]
    ].to_dict(orient="records") if not dfx_day.empty else []

    summary = {
        "farm_name": farm_name,
        "unit_name": unit_name,
        "period": period_label,
        "metrics": {
            "Logs": total_logs,
            "Mortality total": int(total_mortality),
            "Average O2 per unit": avg_o2_per_unit,
            "Average feed/day": avg_feed,
            "Total feed period": total_feed_period,
            "High-risk days": high_risk_days_n,
            "Overall status": overall_status,
            "Worst day": worst_day,
            "Best day": best_day,
            "Risk counts": ", ".join([f"{k}={v}" for k, v in risk_counts.items()]) if risk_counts else "—",
        },
        "top_risks": top_risks,
        "notes": observations,
        "daily_rows": pdf_daily_rows,
    }

    pdf_bytes = _build_weekly_report_pdf(summary)
    st.download_button(
        t("reports.button.generate_weekly_pdf", "Generate Weekly PDF"),
        data=pdf_bytes,
        file_name=f"weekly_report_{farm_id}_{unit_id}_{period_days}d.pdf",
        mime="application/pdf",
        key="reports_weekly_pdf",
    )

    st.markdown("---")
    st.markdown(f"### {t('reports.daily_log_database.title', 'Daily log database')}")

    list_df = dfx.copy().sort_values("timestamp_dt", ascending=False).reset_index(drop=True)

    daily_show_cols = [
        "day",
        "timestamp_utc",
        "operator",
        "unit_name",
        "risk_level",
        "planned_feed_kg_total",
        "mortality_total_n",
        "note",
    ]
    daily_existing = [c for c in daily_show_cols if c in list_df.columns]
    st.dataframe(list_df[daily_existing], use_container_width=True, hide_index=True)

    options = []
    for i, row in list_df.iterrows():
        day_txt = str(row.get("day") or "")
        rl = str(row.get("risk_level") or "")
        options.append(f"{i+1}. {day_txt} | {rl}")

    selected_label = st.selectbox(t("reports.field.open_daily_record", "Open daily record"), options, index=0, key="reports_day_select")
    selected_idx = options.index(selected_label)
    selected_row = list_df.iloc[selected_idx].to_dict()

    st.markdown(f"### {t('reports.day_detail.title', 'Day detail')}")

    meta1, meta2, meta3 = st.columns(3)
    with meta1:
        st.write(f"**{t('reports.meta.date_utc', 'Date (UTC)')}:** {selected_row.get('timestamp_utc','')[:19]}")
        st.write(f"**{t('reports.meta.day', 'Day')}:** {selected_row.get('day') or '—'}")
        st.write(f"**{t('label.operator', 'Operator')}:** {selected_row.get('operator') or '—'}")
    with meta2:
        st.write(f"**{t('reports.meta.risk', 'Risk')}:** {selected_row.get('risk_level') or '—'}")
        st.write(f"**{t('reports.meta.feed_total', 'Feed total')}:** {selected_row.get('planned_feed_kg_total') if selected_row.get('planned_feed_kg_total') is not None else '—'}")
        st.write(f"**{t('today.field.unit', 'Unit')}:** {selected_row.get('unit_name') or '—'}")
    with meta3:
        st.write(f"**{t('reports.meta.mortality_total', 'Mortality total')}:** {selected_row.get('mortality_total_n') if selected_row.get('mortality_total_n') is not None else '—'}")
        st.write(f"**{t('reports.meta.o2_min', 'O2 min')}:** {selected_row.get('unit_o2_min') if selected_row.get('unit_o2_min') is not None else '—'}")
        st.write(f"**{t('reports.meta.o2_avg', 'O2 avg')}:** {selected_row.get('unit_o2_avg') if selected_row.get('unit_o2_avg') is not None else '—'}")

    values = _extract_record_values(selected_row)
    if values:
        with st.expander(t("reports.measurements.title", "Measurements"), expanded=True):
            for k, v in values.items():
                st.write(f"**{k}:** {v}")

    o2_summary = {
        "unit_o2_min": selected_row.get("unit_o2_min"),
        "unit_o2_avg": selected_row.get("unit_o2_avg"),
        "unit_o2_spread": selected_row.get("unit_o2_spread"),
    }
    if any(v is not None for v in o2_summary.values()):
        with st.expander(t("today.o2_summary.title", "O2 summary"), expanded=True):
            for k, v in o2_summary.items():
                st.write(f"**{k}:** {v}")

    tank_entries = _safe_list(selected_row.get("tank_entries"))
    if tank_entries:
        st.markdown(f"**{t('today.tanks.title', 'Tanks')}**")
        for i, tank in enumerate(tank_entries):
            td = _safe_dict(tank)
            tank_title = str(td.get("tank_name") or td.get("tank_id") or f"{t('label.tank', 'Tank')} {i+1}")
            with st.expander(tank_title, expanded=False):
                st.write(f"**{t('reports.table.o2', 'O2')}:** {td.get('o2_mg_l') if td.get('o2_mg_l') is not None else '—'}")
                st.write(f"**{t('reports.table.mortality', 'Mortality')}:** {td.get('mortality_n') if td.get('mortality_n') is not None else '—'}")
                st.write(f"**{t('reports.table.biomass', 'Biomass')}:** {td.get('biomass_kg') if td.get('biomass_kg') is not None else '—'}")
                st.write(f"**{t('reports.table.avg_weight', 'Avg weight')}:** {td.get('avg_weight_g') if td.get('avg_weight_g') is not None else '—'}")
                st.write(f"**{t('reports.table.feed_pct_day', 'Feed %/day')}:** {td.get('feed_rate_pct_day') if td.get('feed_rate_pct_day') is not None else '—'}")
                st.write(f"**{t('reports.table.planned_feed_kg_day', 'Planned feed kg/day')}:** {td.get('planned_feed_kg_day') if td.get('planned_feed_kg_day') is not None else '—'}")

    selected_top_risks = _safe_list(selected_row.get("top_risks"))
    if selected_top_risks:
        st.markdown(f"**{t('today.top_risks.title', 'Top risks')}**")
        for x in selected_top_risks:
            st.write(f"- {x}")

    checklist = _safe_list(selected_row.get("checklist"))
    if checklist:
        st.markdown(f"**{t('today.checklist.title', 'Checklist')}**")
        for x in checklist:
            st.write(f"- {x}")

    signals = _safe_list(selected_row.get("signals"))
    if signals:
        with st.expander(t("today.signals.title", "Signals"), expanded=False):
            for x in signals:
                st.write(f"- {x}")

    if selected_row.get("note"):
        with st.expander(t("label.notes", "Notes"), expanded=True):
            st.write(selected_row.get("note"))

    daily_pdf_bytes = _build_daily_log_pdf(selected_row)
    ts_txt = str(selected_row.get("timestamp_utc") or "daily_report").replace(":", "-")
    st.download_button(
        t("reports.button.generate_daily_pdf", "Generate Daily PDF"),
        data=daily_pdf_bytes,
        file_name=f"daily_report_{ts_txt}.pdf",
        mime="application/pdf",
        key="reports_daily_pdf",
    )