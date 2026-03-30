import streamlit as st
import pandas as pd

import aquantis_storage as storage
from modules.dt import to_utc_datetime
from modules.pdf_report import build_pdf_from_incident
from modules.ui_text import t


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def _load_df():
    records = storage.load_incidents()
    df = storage.records_to_df(records)

    if df.empty:
        return df, records

    if "timestamp_utc" in df.columns:
        df["timestamp_dt"] = to_utc_datetime(df["timestamp_utc"])
    else:
        df["timestamp_dt"] = pd.NaT

    if "status" not in df.columns:
        df["status"] = "OPEN"

    if "severity" not in df.columns:
        df["severity"] = df.get("risk_status", "OK")

    if "incident_id" not in df.columns:
        df["incident_id"] = df.index.astype(str)

    return df, records


def _dedupe_latest(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only latest snapshot per incident_id (append-only full snapshots)."""
    if df.empty:
        return df
    if "timestamp_dt" in df.columns and df["timestamp_dt"].notna().any():
        df = df.sort_values("timestamp_dt", ascending=False)
    return df.drop_duplicates(subset=["incident_id"], keep="first")


def _latest_snapshot(records, incident_id):
    for r in reversed(records):
        if str(r.get("incident_id")) == str(incident_id):
            return r
    return None


def _incident_history(records, incident_id):
    hist = [r for r in records if str(r.get("incident_id")) == str(incident_id)]
    hist.sort(key=lambda r: str(r.get("timestamp_utc") or ""))
    return hist


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


def _now_utc_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _task_done(t: dict) -> bool:
    td = _safe_dict(t)
    state = td.get("state")
    if isinstance(state, str):
        return state.upper() == "DONE"
    if isinstance(state, bool):
        return bool(state)
    # legacy
    done = td.get("done")
    if isinstance(done, bool):
        return done
    return False


def _task_group(t: dict) -> str:
    td = _safe_dict(t)
    g = td.get("group")
    if isinstance(g, str) and g.strip():
        return g.strip()
    # fallback bucket
    return t("incidents.tasks.fallback_group", "Tasks")


def _group_tasks(tasks: list) -> dict:
    groups = {}
    for tsk in _safe_list(tasks):
        td = _safe_dict(tsk)
        title = str(td.get("title") or "").strip()
        if not title:
            continue
        g = _task_group(td)
        groups.setdefault(g, []).append(td)
    return groups


# -------------------------------------------------
# Main Page
# -------------------------------------------------

def render_incidents():
    st.subheader(t("incidents.title", "📁 Incidents"))

    df, records = _load_df()
    if df.empty:
        st.info(t("incidents.msg.no_incidents", "No incidents yet."))
        return

    # -----------------------
    # Filters
    # -----------------------
    st.markdown(f"### {t('incidents.filters.title', 'Filters')}")
    c1, c2, c3 = st.columns([1, 1, 2])

    with c1:
        only_open = st.checkbox(
            t("incidents.filters.only_open", "Show only OPEN"),
            value=False,
            key="inc_filter_only_open",
        )

    with c2:
        sev = st.multiselect(
            t("label.severity", "Severity"),
            ["OK", "WATCH", "WARNING", "STOP"],
            default=["OK", "WATCH", "WARNING", "STOP"],
            key="inc_filter_sev",
        )

    with c3:
        search = st.text_input(
            t("incidents.filters.search", "Search (ID/title/trigger)"),
            key="inc_filter_search",
        )

    df_f = df.copy()

    if only_open:
        df_f = df_f[df_f["status"].fillna("OPEN") == "OPEN"]

    if sev:
        df_f = df_f[df_f["severity"].fillna("").isin(sev)]

    if search:
        q = search.lower()
        mask = None

        for col in ["incident_id", "title"]:
            if col in df_f.columns:
                s = df_f[col].fillna("").astype(str).str.lower().str.contains(q, na=False)
                mask = s if mask is None else (mask | s)

        if "triggered_by" in df_f.columns:
            s = df_f["triggered_by"].fillna("").astype(str).str.lower().str.contains(q, na=False)
            mask = s if mask is None else (mask | s)

        if mask is not None:
            df_f = df_f[mask]

    df_list = _dedupe_latest(df_f)

    # -----------------------
    # List
    # -----------------------
    st.markdown(f"### {t('incidents.list.title', 'List')}")

    if df_list.empty:
        st.info(t("incidents.msg.no_match_filters", "No incidents match filters."))
    else:
        header = st.columns([2.2, 1.2, 1.2, 2.8, 1])
        header[0].markdown(f"**{t('incidents.table.created_utc', 'Created (UTC)')}**")
        header[1].markdown(f"**{t('label.status', 'Status')}**")
        header[2].markdown(f"**{t('label.severity', 'Severity')}**")
        header[3].markdown(f"**{t('incidents.table.id_title', 'ID / Title')}**")
        header[4].markdown(f"**{t('incidents.table.action', 'Action')}**")

        for idx, row in df_list.iterrows():
            inc_id = str(row.get("incident_id"))
            ts_txt = str(row.get("timestamp_utc", ""))

            cols = st.columns([2.2, 1.2, 1.2, 2.8, 1])
            cols[0].write(ts_txt[:19])
            cols[1].write(f"**{row.get('status','OPEN')}**")
            cols[2].write(row.get("severity", ""))
            cols[3].write(f"`{inc_id}`  {row.get('title','') or ''}")

            open_key = f"open::list::{inc_id}::{ts_txt}::{idx}"
            if cols[4].button(t("incidents.button.open", "Open"), key=open_key):
                st.session_state["selected_incident_id"] = inc_id
                st.session_state["auto_expand_inputs"] = True
                st.rerun()

    st.divider()
    _render_detail(records)


# -------------------------------------------------
# Detail View (Workspace)
# -------------------------------------------------

def _render_detail(records):
    inc_id = st.session_state.get("selected_incident_id")
    if not inc_id:
        st.info(t("incidents.msg.click_open_detail", "Click **Open** to view detail."))
        return

    if st.button(t("incidents.button.back_to_list", "← Back to list"), key=f"back::{inc_id}"):
        st.session_state["selected_incident_id"] = None
        st.rerun()

    rec = _latest_snapshot(records, inc_id)
    if not rec:
        st.warning(t("incidents.msg.not_found", "Incident not found."))
        return

    rb = _safe_dict(rec.get("risk_bundle"))
    diag = _safe_dict(rb.get("diagnostics"))

    st.markdown(f"## `{inc_id}`")

    # -------------------------------------------------
    # Header + workspace status
    # -------------------------------------------------
    h1, h2, h3 = st.columns([1.1, 1.1, 2.2])
    with h1:
        st.write(f"**{t('label.severity', 'Severity')}:** {rec.get('severity','') or '—'}")
    with h2:
        st.write(f"**{t('label.status', 'Status')}:** {rec.get('status','OPEN')}")
    with h3:
        if rec.get("title"):
            st.write(f"**{t('incidents.field.title', 'Title')}:** {rec.get('title')}")

    ws_status_options = ["OPEN", "MITIGATING", "MONITORING", "CLOSED"]
    current_status = str(rec.get("status") or "OPEN").upper()
    current_ws = "CLOSED" if current_status == "RESOLVED" else (current_status if current_status in ws_status_options else "OPEN")

    new_ws_status = st.selectbox(
        t("incidents.field.workspace_status", "Workspace status"),
        options=ws_status_options,
        index=ws_status_options.index(current_ws),
        key=f"ws::status::{inc_id}",
    )

    if new_ws_status != current_ws:
        storage.set_workspace_status(inc_id, new_ws_status)
        st.rerun()

    st.divider()

    # -------------------------------------------------
    # Diagnostics (structured protocol)
    # -------------------------------------------------
    if diag:
        proto = diag.get("protocol")
        st.markdown(f"### {t('incidents.diagnostics.title', 'Diagnostics')}")

        if isinstance(proto, dict) and proto:
            causes = _safe_list(proto.get("probable_causes"))
            ia = _safe_list(proto.get("immediate_actions_0_30m"))
            stz = _safe_list(proto.get("stabilization_2_24h"))
            tests = _safe_list(proto.get("tests_to_verify"))
            esc = _safe_list(proto.get("escalation_thresholds"))

            st.markdown(f"#### {t('incidents.diagnostics.probable_cause', 'Pravděpodobná příčina (top 3) + jistota')}")
            if causes:
                for c in causes[:3]:
                    if not isinstance(c, dict):
                        continue
                    name = c.get("name") or "—"
                    conf = c.get("confidence")
                    why = c.get("why") or ""
                    if conf is None:
                        st.write(f"- **{name}** — {why}".strip())
                    else:
                        st.write(f"- **{name}** ({conf}%) — {why}".strip())
            else:
                st.write("- —")

            def _bul(title, items):
                st.markdown(f"#### {title}")
                if not items:
                    st.write("- —")
                    return
                for x in items:
                    if isinstance(x, str) and x.strip():
                        st.write(f"- {x.strip()}")

            _bul(t("incidents.diagnostics.immediate_actions", "Okamžitá opatření (0–30 min)"), ia)
            _bul(t("incidents.diagnostics.stabilization", "Stabilizace (2–24 h)"), stz)
            _bul(t("incidents.diagnostics.tests_to_verify", "Co ověřit / jaké testy doplnit"), tests)
            _bul(t("incidents.diagnostics.escalation_thresholds", "Kdy eskalovat (thresholdy)"), esc)

            kb_sources = _as_list(diag.get("kb_sources"))
            if kb_sources:
                with st.expander(t("incidents.diagnostics.kb_sources", "KB sources"), expanded=False):
                    for s in kb_sources:
                        st.write(f"- {s}")
                    if diag.get("rag_best_score") is not None:
                        st.write(f"{t('incidents.diagnostics.rag_score', 'RAG score')}: {diag.get('rag_best_score')}")
        else:
            # legacy
            short_answer = diag.get("short_answer")
            detailed_report = diag.get("detailed_report")
            if short_answer:
                st.markdown(short_answer)
            if detailed_report:
                st.markdown(detailed_report)

    # -------------------------------------------------
    # Workspace: Tasks (grouped) + Done %
    # -------------------------------------------------
    st.markdown(f"### {t('incidents.workspace.title', 'Workspace')}")

    tasks = rb.get("tasks")
    timeline = rb.get("timeline")
    if not isinstance(tasks, list) and isinstance(rb.get("workspace"), dict):
        tasks = rb["workspace"].get("tasks")
    if not isinstance(timeline, list) and isinstance(rb.get("workspace"), dict):
        timeline = rb["workspace"].get("timeline")

    tasks = _safe_list(tasks)
    timeline = _safe_list(timeline)

    with st.expander(t("incidents.tasks.title", "Tasks"), expanded=True):
        if not tasks:
            st.info(t("incidents.msg.no_tasks", "No tasks yet."))
        else:
            total = 0
            done_n = 0
            for task in tasks:
                total += 1
                if _task_done(_safe_dict(task)):
                    done_n += 1
            pct = int(round((done_n / total) * 100)) if total else 0
            st.progress(pct / 100.0)
            st.write(f"**{t('incidents.tasks.done', 'Done')}:** {done_n}/{total} ({pct}%)")

            groups = _group_tasks(tasks)

            new_tasks = []
            for gname, items in groups.items():
                st.markdown(f"**{gname}**")
                for i, task in enumerate(items):
                    td = _safe_dict(task)
                    title = str(td.get("title") or "").strip()
                    if not title:
                        continue

                    done = _task_done(td)
                    cb_key = f"task::cb::{inc_id}::{gname}::{i}::{title[:12]}"
                    val = st.checkbox(title, value=done, key=cb_key)

                    td2 = dict(td)
                    td2["group"] = gname
                    td2["state"] = "DONE" if val else "OPEN"
                    new_tasks.append(td2)

            if st.button(t("incidents.button.save_tasks", "Save tasks"), key=f"task::save::{inc_id}"):
                storage.update_tasks(inc_id, new_tasks, audit_note="tasks checklist updated")
                st.rerun()

    # -------------------------------------------------
    # Workspace: Timeline + quick buttons
    # -------------------------------------------------
    with st.expander(t("incidents.timeline.title", "Timeline"), expanded=True):
        # quick buttons
        qb1, qb2, qb3, qb4 = st.columns(4)
        if qb1.button(t("incidents.timeline.quick.o2_checked", "O2 checked"), key=f"tl::qb::o2::{inc_id}"):
            storage.append_timeline_entry(inc_id, {"event": "CHECK", "note": "O2 checked"}, audit_note="O2 checked")
            st.rerun()
        if qb2.button(t("incidents.timeline.quick.no2_retest", "NO2 re-test"), key=f"tl::qb::no2::{inc_id}"):
            storage.append_timeline_entry(inc_id, {"event": "TEST", "note": "NO2 re-test"}, audit_note="NO2 re-test")
            st.rerun()
        if qb3.button(t("incidents.timeline.quick.feeding_reduced", "Feeding reduced"), key=f"tl::qb::feed::{inc_id}"):
            storage.append_timeline_entry(inc_id, {"event": "ACTION", "note": "Feeding reduced"}, audit_note="Feeding reduced")
            st.rerun()
        if qb4.button(t("incidents.timeline.quick.biofilter_inspected", "Biofilter inspected"), key=f"tl::qb::bf::{inc_id}"):
            storage.append_timeline_entry(inc_id, {"event": "CHECK", "note": "Biofilter inspected"}, audit_note="Biofilter inspected")
            st.rerun()

        # manual add
        t1, t2, t3 = st.columns([1.2, 2.2, 1])
        with t1:
            who = st.text_input(
                t("incidents.timeline.who", "Who"),
                value=str(rec.get("handled_by") or ""),
                key=f"tl::who::{inc_id}",
            )
        with t2:
            note = st.text_input(
                t("incidents.timeline.log_entry", "Log entry"),
                value="",
                key=f"tl::note::{inc_id}",
            )
        with t3:
            add = st.button(t("action.add", "Add"), key=f"tl::add::{inc_id}")

        if add and note.strip():
            entry = {
                "ts_utc": _now_utc_iso(),
                "who": who.strip() or "—",
                "unit": (rec.get("inputs") or {}).get("unit") or "—",
                "event": "SHIFT_LOG",
                "note": note.strip(),
            }
            storage.append_timeline_entry(inc_id, entry, audit_note=entry.get("note", "timeline"))
            st.rerun()

        # show timeline
        if not timeline:
            st.info(t("incidents.msg.no_timeline_entries", "No timeline entries."))
        else:
            tl_sorted = sorted(timeline, key=lambda x: str(_safe_dict(x).get("ts_utc") or ""), reverse=True)
            for e in tl_sorted:
                ed = _safe_dict(e)
                ts = str(ed.get("ts_utc") or "")[:19]
                who2 = ed.get("who") or "—"
                ev = ed.get("event") or ""
                nt = ed.get("note") or ""
                st.write(f"- {ts} | **{who2}** | {ev} — {nt}")

    st.divider()

    # -------------------------------------------------
    # Inputs + workflow + PDF
    # -------------------------------------------------
    expand_inputs = bool(st.session_state.get("auto_expand_inputs", False))
    with st.expander(t("incidents.inputs.title", "Inputs"), expanded=expand_inputs):
        st.json(rec.get("inputs", {}))
        if rb:
            st.markdown(f"**{t('incidents.inputs.risk_bundle_snapshot', 'Risk bundle snapshot')}**")
            st.json({k: rb.get(k) for k in ["parsed", "missing", "confidence", "thresholds_applied"] if k in rb})
    st.session_state["auto_expand_inputs"] = False

    with st.expander(t("incidents.workflow.title", "Workflow"), expanded=False):
        handled_by = st.text_input(
            t("incidents.workflow.who_handled", "Who handled"),
            value=rec.get("handled_by", ""),
            key=f"wf_handled_{inc_id}",
        )
        actions = st.text_area(
            t("incidents.workflow.what_was_done", "What was done"),
            value=rec.get("actions_taken", ""),
            key=f"wf_actions_{inc_id}",
            height=120,
        )
        note2 = st.text_area(
            t("incidents.workflow.note", "Note"),
            value=rec.get("note", ""),
            key=f"wf_note_{inc_id}",
            height=90,
        )

        c1, c2, c3, c4 = st.columns(4)

        if c1.button(t("incidents.button.save_workflow", "Save workflow"), key=f"wf_save_{inc_id}"):
            storage.update_workflow_fields(inc_id, handled_by, actions, note2)
            st.success(t("msg.saved", "Saved."))
            st.rerun()

        if c2.button(t("incidents.button.resolve", "Resolve"), key=f"wf_resolve_{inc_id}"):
            storage.resolve_incident(inc_id)
            st.success(t("incidents.msg.resolved", "Resolved."))
            st.rerun()

        if c3.button(t("incidents.button.reopen", "Re-open"), key=f"wf_reopen_{inc_id}"):
            storage.reopen_incident(inc_id)
            st.success(t("incidents.msg.reopened", "Re-opened."))
            st.rerun()

        if c4.button(t("action.export_pdf", "Export PDF"), key=f"wf_pdf_{inc_id}"):
            pdf_bytes = build_pdf_from_incident(rec)
            st.download_button(
                t("incidents.button.download_pdf", "Download PDF"),
                data=pdf_bytes,
                file_name=f"{inc_id}_report.pdf",
                mime="application/pdf",
                key=f"wf_dl_{inc_id}",
            )

    with st.expander(t("incidents.audit.title", "Audit / History"), expanded=False):
        hist = _incident_history(records, inc_id)
        if not hist:
            st.info(t("incidents.msg.no_history_records", "No history records."))
        else:
            for i, h in enumerate(hist):
                ts = str(h.get("timestamp_utc", ""))
                et = h.get("event_type", "")
                with st.expander(f"{i+1}. {ts[:19]} | {et}", expanded=False):
                    if h.get("audit_trail"):
                        st.json(h["audit_trail"])
                    else:
                        st.info(t("incidents.msg.no_audit_trail", "No audit trail."))