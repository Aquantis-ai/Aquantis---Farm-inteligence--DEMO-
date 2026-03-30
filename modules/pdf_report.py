from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.pdfmetrics import registerFontFamily
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

from modules.ui_text import t


_FONT_CACHE: dict[str, Any] = {}


def _find_first_existing(paths: list[str]) -> str | None:
    for p in paths:
        try:
            if Path(p).exists():
                return p
        except Exception:
            continue
    return None


def _setup_pdf_font_family() -> dict[str, str]:
    """
    Returns concrete font face names for ReportLab Paragraph styles.

    Priority:
    1) app-local fonts/DejaVuSans*.ttf
    2) common Linux DejaVu paths
    3) Windows Arial family

    Falls back to Helvetica faces if nothing usable is found.
    """
    cached = _FONT_CACHE.get("faces")
    if isinstance(cached, dict) and cached:
        return cached

    local_fonts = Path(__file__).resolve().parent / "fonts"
    root_fonts = Path(__file__).resolve().parent.parent / "fonts"

    regular_candidates = [
        str(local_fonts / "DejaVuSans.ttf"),
        str(root_fonts / "DejaVuSans.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/local/share/fonts/DejaVuSans.ttf",
        "C:/Windows/Fonts/arial.ttf",
    ]
    bold_candidates = [
        str(local_fonts / "DejaVuSans-Bold.ttf"),
        str(root_fonts / "DejaVuSans-Bold.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/local/share/fonts/DejaVuSans-Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    italic_candidates = [
        str(local_fonts / "DejaVuSans-Oblique.ttf"),
        str(root_fonts / "DejaVuSans-Oblique.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf",
        "/usr/local/share/fonts/DejaVuSans-Oblique.ttf",
        "C:/Windows/Fonts/ariali.ttf",
    ]
    bold_italic_candidates = [
        str(local_fonts / "DejaVuSans-BoldOblique.ttf"),
        str(root_fonts / "DejaVuSans-BoldOblique.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-BoldOblique.ttf",
        "/usr/local/share/fonts/DejaVuSans-BoldOblique.ttf",
        "C:/Windows/Fonts/arialbi.ttf",
    ]

    regular_path = _find_first_existing(regular_candidates)
    bold_path = _find_first_existing(bold_candidates)
    italic_path = _find_first_existing(italic_candidates)
    bold_italic_path = _find_first_existing(bold_italic_candidates)

    if not regular_path:
        faces = {
            "normal": "Helvetica",
            "bold": "Helvetica-Bold",
            "italic": "Helvetica-Oblique",
            "bold_italic": "Helvetica-BoldOblique",
        }
        _FONT_CACHE["faces"] = faces
        return faces

    family_name = "AquantisUnicode"
    normal_name = f"{family_name}-Regular"
    bold_name = f"{family_name}-Bold"
    italic_name = f"{family_name}-Italic"
    bold_italic_name = f"{family_name}-BoldItalic"

    try:
        pdfmetrics.registerFont(TTFont(normal_name, regular_path))

        if bold_path:
            pdfmetrics.registerFont(TTFont(bold_name, bold_path))
        else:
            bold_name = normal_name

        if italic_path:
            pdfmetrics.registerFont(TTFont(italic_name, italic_path))
        else:
            italic_name = normal_name

        if bold_italic_path:
            pdfmetrics.registerFont(TTFont(bold_italic_name, bold_italic_path))
        else:
            bold_italic_name = bold_name

        registerFontFamily(
            family_name,
            normal=normal_name,
            bold=bold_name,
            italic=italic_name,
            boldItalic=bold_italic_name,
        )

        faces = {
            "normal": normal_name,
            "bold": bold_name,
            "italic": italic_name,
            "bold_italic": bold_italic_name,
        }
        _FONT_CACHE["faces"] = faces
        return faces

    except Exception:
        faces = {
            "normal": "Helvetica",
            "bold": "Helvetica-Bold",
            "italic": "Helvetica-Oblique",
            "bold_italic": "Helvetica-BoldOblique",
        }
        _FONT_CACHE["faces"] = faces
        return faces


def _normalize_pdf_text(text: str) -> str:
    """
    Normalize characters that often break in basic PDF font setups.
    Even with Unicode fonts, using plain chemical notation is safer.
    """
    replacements = {
        "₀": "0",
        "₁": "1",
        "₂": "2",
        "₃": "3",
        "₄": "4",
        "₅": "5",
        "₆": "6",
        "₇": "7",
        "₈": "8",
        "₉": "9",
        "⁰": "0",
        "¹": "1",
        "²": "2",
        "³": "3",
        "⁴": "4",
        "⁵": "5",
        "⁶": "6",
        "⁷": "7",
        "⁸": "8",
        "⁹": "9",
        "→": "->",
        "–": "-",
        "—": "-",
    }
    out = str(text)
    for old, new in replacements.items():
        out = out.replace(old, new)
    return out


def _p(text: str) -> str:
    if text is None:
        return ""
    safe = _normalize_pdf_text(str(text))
    return (
        safe
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br/>")
    )


def _as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        return [p.strip() for p in x.split(",") if p.strip()]
    return [str(x)]


def _safe_dict(x):
    return x if isinstance(x, dict) else {}


def _safe_list(x):
    return x if isinstance(x, list) else []


def _task_done(t: dict) -> bool:
    td = _safe_dict(t)
    state = td.get("state")
    if isinstance(state, str):
        return state.upper() == "DONE"
    if isinstance(state, bool):
        return bool(state)
    done = td.get("done")
    if isinstance(done, bool):
        return done
    return False


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


def _fmt_measurement_value(v) -> str:
    if v is None:
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


def _measurement_lines(inputs: dict, measurement_units: dict) -> list[str]:
    meas = _safe_dict(inputs.get("measurements_struct") or inputs.get("measurements") or {})
    if not meas:
        return []

    order = [
        ("pH", "pH", measurement_units.get("ph", "pH")),
        ("temp", t("diagnostics.measurement.temp", "Temp"), measurement_units.get("temp", "")),
        ("salinity", t("diagnostics.measurement.salinity", "Salinity"), measurement_units.get("salinity", "")),
        ("O2", "O2", measurement_units.get("o2", "")),
        ("NO2", "NO2", measurement_units.get("no2", "")),
        ("NO3", "NO3", measurement_units.get("no3", "")),
        ("TAN", "TAN", measurement_units.get("tan", "")),
        ("NH3", "NH3", measurement_units.get("nh3", "")),
        ("NH4", "NH4", measurement_units.get("nh4", "")),
    ]

    out: list[str] = []
    for key, label, unit_txt in order:
        v = meas.get(key)
        if v is None:
            continue
        val = _fmt_measurement_value(v)
        if unit_txt and key != "pH":
            out.append(f"{label}: {val} {unit_txt}".strip())
        else:
            out.append(f"{label}: {val}")
    return out


def _block(elements, title: str, items, style_normal, spacer_after: int = 4):
    elements.append(Paragraph(f"<b>{_p(title)}:</b>", style_normal))
    items = _safe_list(items)
    if not items:
        elements.append(Paragraph("- —", style_normal))
        elements.append(Spacer(1, spacer_after))
        return

    printed = 0
    for x in items[:30]:
        if isinstance(x, str) and x.strip():
            elements.append(Paragraph(f"- {_p(x.strip())}", style_normal))
            printed += 1

    if printed == 0:
        elements.append(Paragraph("- —", style_normal))

    elements.append(Spacer(1, spacer_after))


def build_pdf_from_incident(incident: dict) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=50,
        bottomMargin=40,
    )

    font_faces = _setup_pdf_font_family()

    styles = getSampleStyleSheet()
    normal = ParagraphStyle(
        "NormalX",
        parent=styles["Normal"],
        fontName=font_faces["normal"],
        fontSize=10,
        leading=14,
    )
    heading = ParagraphStyle(
        "HeadingX",
        parent=styles["Heading2"],
        fontName=font_faces["bold"],
        fontSize=14,
        leading=18,
        spaceAfter=10,
    )
    subheading = ParagraphStyle(
        "SubHeadingX",
        parent=styles["Heading3"],
        fontName=font_faces["bold"],
        fontSize=11,
        leading=15,
        spaceAfter=6,
    )

    rb = _safe_dict(incident.get("risk_bundle"))
    diag = _safe_dict(rb.get("diagnostics"))
    proto = _safe_dict(diag.get("protocol"))
    inputs = _safe_dict(incident.get("inputs"))

    farm_name = _first_non_empty(inputs.get("farm_name"), inputs.get("farm"), t("label.farm", "Farm"))
    unit_name = _first_non_empty(inputs.get("unit"), t("today.field.unit", "Unit"))
    operator = _first_non_empty(inputs.get("operator"))
    species = _first_non_empty(inputs.get("species"))
    duration = _first_non_empty(inputs.get("duration"))
    mortality = _first_non_empty(inputs.get("mortality"))
    timestamp_utc = _first_non_empty(incident.get("timestamp_utc"), diag.get("ts_utc"))
    measurement_units = _safe_dict(inputs.get("measurement_units"))

    causes = _safe_list(proto.get("probable_causes"))
    primary_cause = _safe_dict(causes[0]) if causes else {}
    contributing_causes = [_safe_dict(c) for c in causes[1:3]]

    immediate_actions = _safe_list(proto.get("immediate_actions_0_30m"))
    stabilization = _safe_list(proto.get("stabilization_2_24h"))
    tests_to_verify = _safe_list(proto.get("tests_to_verify"))
    escalation_thresholds = _safe_list(proto.get("escalation_thresholds"))

    title = _first_non_empty(incident.get("title"))
    severity = _first_non_empty(incident.get("severity"), "—")
    status = _first_non_empty(incident.get("status"), "—")
    symptoms = _first_non_empty(inputs.get("symptoms"))

    main_issue = _first_non_empty(
        primary_cause.get("name"),
        title,
        t("pdf.incident.main_issue_fallback", "Incident under review"),
    )

    impact_parts = []
    if mortality:
        impact_parts.append(f"{t('label.mortality', 'Mortality')}: {mortality}")
    if symptoms:
        impact_parts.append(f"{t('label.symptoms', 'Symptoms')}: {symptoms}")
    impact = " | ".join(impact_parts) if impact_parts else t(
        "pdf.incident.impact_fallback",
        "Operational impact not specified.",
    )

    current_recommendation = None
    for candidate in immediate_actions + stabilization + tests_to_verify:
        if isinstance(candidate, str) and candidate.strip():
            current_recommendation = candidate.strip()
            break
    if not current_recommendation:
        current_recommendation = t(
            "pdf.incident.current_recommendation_fallback",
            "Review incident details and continue monitoring.",
        )

    header_title = f"{farm_name} - {unit_name} - {t('pdf.incident.title', 'Incident report')}"

    tasks = rb.get("tasks")
    timeline = rb.get("timeline")
    ws = rb.get("workspace")
    if not isinstance(tasks, list) and isinstance(ws, dict):
        tasks = ws.get("tasks")
    if not isinstance(timeline, list) and isinstance(ws, dict):
        timeline = ws.get("timeline")

    tasks = _safe_list(tasks)
    timeline = _safe_list(timeline)
    audit = _safe_list(incident.get("audit_trail"))

    handled_by = _first_non_empty(incident.get("handled_by"))
    actions_taken = _first_non_empty(incident.get("actions_taken"))
    note = _first_non_empty(incident.get("note"))
    resolved_at = _first_non_empty(incident.get("resolved_at_utc"))

    elements = []
    elements.append(Paragraph(_p(header_title), heading))
    elements.append(Spacer(1, 10))

    # 1) Executive summary
    elements.append(Paragraph(t("pdf.executive_summary", "Executive summary"), subheading))
    elements.append(Paragraph(f"<b>{_p(t('label.severity', 'Severity'))}:</b> {_p(severity)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('label.status', 'Status'))}:</b> {_p(status)}", normal))
    if title:
        elements.append(Paragraph(f"<b>{_p(t('incidents.field.title', 'Title'))}:</b> {_p(title)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('reports.pdf.main_issue', 'Main issue'))}:</b> {_p(main_issue)}", normal))
    elements.append(Paragraph(f"<b>{_p(t('pdf.incident.impact', 'Impact'))}:</b> {_p(impact)}", normal))
    elements.append(
        Paragraph(
            f"<b>{_p(t('pdf.incident.current_recommendation', 'Current recommendation'))}:</b> {_p(current_recommendation)}",
            normal,
        )
    )
    elements.append(Spacer(1, 8))

    # 2) Incident context
    elements.append(Paragraph(t("pdf.incident.context", "Incident context"), subheading))
    context_items = {
        t("label.farm", "Farm"): farm_name,
        t("today.field.unit", "Unit"): unit_name,
        t("label.operator", "Operator"): operator,
        t("label.species", "Species"): species,
        t("label.duration", "Duration"): duration,
        t("label.mortality", "Mortality"): mortality,
        t("pdf.timestamp_utc", "Timestamp (UTC)"): timestamp_utc,
        t("pdf.incident.incident_id", "Incident ID"): incident.get("incident_id"),
    }
    for k, v in context_items.items():
        if v is None or str(v).strip() == "":
            continue
        elements.append(Paragraph(f"<b>{_p(k)}:</b> {_p(v)}", normal))
    elements.append(Spacer(1, 8))

    # 3) Symptoms and measurements
    elements.append(Paragraph(t("pdf.incident.symptoms_measurements", "Symptoms and measurements"), subheading))
    elements.append(Paragraph(f"<b>{_p(t('label.symptoms', 'Symptoms'))}:</b>", normal))
    if symptoms:
        elements.append(Paragraph(_p(symptoms), normal))
    else:
        elements.append(Paragraph("—", normal))
    elements.append(Spacer(1, 4))

    measurement_lines = _measurement_lines(inputs, measurement_units)
    elements.append(Paragraph(f"<b>{_p(t('today.measurements.title', 'Measurements'))}:</b>", normal))
    if measurement_lines:
        for line in measurement_lines:
            elements.append(Paragraph(f"- {_p(line)}", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 8))

    # 4) Diagnostic conclusion
    elements.append(Paragraph(t("pdf.incident.diagnostic_conclusion", "Diagnostic conclusion"), subheading))
    elements.append(Paragraph(f"<b>{_p(t('pdf.incident.primary_probable_cause', 'Primary probable cause'))}:</b>", normal))
    if primary_cause:
        pc_name = _first_non_empty(primary_cause.get("name"), "—")
        pc_conf = primary_cause.get("confidence")
        pc_why = _first_non_empty(primary_cause.get("why"), "")
        if pc_conf is None:
            elements.append(Paragraph(f"- {_p(pc_name)} — {_p(pc_why)}", normal))
        else:
            elements.append(Paragraph(f"- {_p(pc_name)} ({_p(pc_conf)}%) — {_p(pc_why)}", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 4))

    elements.append(Paragraph(f"<b>{_p(t('diagnostics.result.secondary_factors', 'Contributing factors'))}:</b>", normal))
    if contributing_causes:
        for c in contributing_causes:
            name = _first_non_empty(c.get("name"), "—")
            conf = c.get("confidence")
            why = _first_non_empty(c.get("why"), "")
            if conf is None:
                elements.append(Paragraph(f"- {_p(name)} — {_p(why)}", normal))
            else:
                elements.append(Paragraph(f"- {_p(name)} ({_p(conf)}%) — {_p(why)}", normal))
    else:
        elements.append(Paragraph("- —", normal))
    elements.append(Spacer(1, 8))

    # 5) Actions
    elements.append(Paragraph(t("pdf.incident.actions", "Actions"), subheading))
    _block(elements, t("diagnostics.result.immediate_actions", "Immediate actions (0–30 min)"), immediate_actions, normal)
    _block(elements, t("diagnostics.result.stabilization", "Stabilization (2–24 h)"), stabilization, normal)
    _block(elements, t("pdf.incident.verify_tests", "Verify / tests"), tests_to_verify, normal)
    _block(elements, t("pdf.incident.escalation", "Escalation"), escalation_thresholds, normal)

    # 6) Incident workspace
    if tasks or timeline:
        elements.append(Paragraph(t("pdf.incident.workspace", "Incident workspace"), subheading))

        if tasks:
            done_n = sum(1 for tsk in tasks if _task_done(_safe_dict(tsk)))
            total_n = len(tasks)
            elements.append(Paragraph(f"<b>{_p(t('incidents.tasks.title', 'Tasks'))}:</b> {done_n}/{total_n} {t('incidents.tasks.done', 'done')}", normal))
            for tsk in tasks[:60]:
                td = _safe_dict(tsk)
                task_title = str(td.get("title") or "").strip()
                if not task_title:
                    continue
                state = "DONE" if _task_done(td) else "OPEN"
                group = td.get("group")
                prefix = f"[{_p(group)}] " if isinstance(group, str) and group.strip() else ""
                elements.append(Paragraph(f"- {prefix}{_p(task_title)} <i>({state})</i>", normal))
            elements.append(Spacer(1, 6))

        if timeline:
            elements.append(Paragraph(f"<b>{_p(t('incidents.timeline.title', 'Timeline'))}:</b>", normal))
            tl_sorted = sorted(timeline, key=lambda x: str(_safe_dict(x).get("ts_utc") or ""))
            for e in tl_sorted[-80:]:
                ed = _safe_dict(e)
                ts = str(ed.get("ts_utc") or "")
                who = ed.get("who") or "—"
                ev = ed.get("event") or ""
                tl_note = ed.get("note") or ""
                elements.append(Paragraph(f"- {_p(ts)} | {_p(who)} | {_p(ev)} — {_p(tl_note)}", normal))
            elements.append(Spacer(1, 8))

    # 7) Workflow and resolution
    if handled_by or actions_taken or note or resolved_at:
        elements.append(Paragraph(t("pdf.incident.workflow_resolution", "Workflow and resolution"), subheading))
        if handled_by:
            elements.append(Paragraph(f"<b>{_p(t('incidents.workflow.who_handled', 'Handled by'))}:</b> {_p(handled_by)}", normal))
        if actions_taken:
            elements.append(
                Paragraph(
                    f"<b>{_p(t('incidents.workflow.what_was_done', 'What was done'))}:</b><br/>{_p(actions_taken)}",
                    normal,
                )
            )
        if note:
            elements.append(Paragraph(f"<b>{_p(t('label.notes', 'Note'))}:</b><br/>{_p(note)}", normal))
        if resolved_at:
            elements.append(Paragraph(f"<b>{_p(t('pdf.incident.resolved_at_utc', 'Resolved at (UTC)'))}:</b> {_p(resolved_at)}", normal))
        elements.append(Spacer(1, 8))

    # 8) Audit appendix
    if audit:
        elements.append(Paragraph(t("pdf.incident.audit_appendix", "Audit appendix"), subheading))
        elements.append(Paragraph(f"<b>{_p(t('pdf.incident.audit_trail', 'Audit trail'))}:</b>", normal))
        for a in audit[-80:]:
            ad = _safe_dict(a)
            ts = ad.get("ts_utc", "")
            field = ad.get("field", "")
            old = ad.get("old", "")
            new = ad.get("new", "")
            elements.append(Paragraph(f"- {_p(ts)} | {_p(field)}: {_p(old)} -> {_p(new)}", normal))
        elements.append(Spacer(1, 8))

    elements.append(Spacer(1, 10))
    elements.append(
        Paragraph(
            f"{t('pdf.generated_utc', 'Generated (UTC)')}: {_p(datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))}",
            normal,
        )
    )

    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()