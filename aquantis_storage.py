from __future__ import annotations

from datetime import datetime, timezone
import uuid
from pathlib import Path
import json
import pandas as pd

from modules.workspace import get_workspace_id

INCIDENTS_DIR = Path("incidents")
INCIDENTS_DIR.mkdir(parents=True, exist_ok=True)


def _get_incidents_log_path() -> Path:
    workspace_id = get_workspace_id()
    return INCIDENTS_DIR / f"incidents_log_{workspace_id}.jsonl"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str = "INC") -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


def append_record(record: dict):
    record = dict(record or {})
    record.setdefault("timestamp_utc", _now_utc_iso())
    incidents_log_path = _get_incidents_log_path()
    with open(incidents_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_incidents() -> list[dict]:
    incidents_log_path = _get_incidents_log_path()
    if not incidents_log_path.exists():
        return []
    out = []
    with open(incidents_log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = (line or "").strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def records_to_df(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records)


def _append_audit(rec: dict, field: str, old, new):
    rec.setdefault("audit_trail", [])
    rec["audit_trail"].append({
        "ts_utc": _now_utc_iso(),
        "field": field,
        "old": old,
        "new": new,
    })


def create_incident(*, title: str, severity: str, status: str, triggered_by: list[str], inputs: dict,
                    risk_bundle: dict | None = None) -> dict:
    rec = {
        "incident_id": _new_id("INC"),
        "event_type": "INCIDENT",
        "title": title,
        "severity": severity,
        "status": status,
        "triggered_by": triggered_by or [],
        "inputs": inputs or {},
        "risk_bundle": risk_bundle or {},
        "handled_by": "",
        "actions_taken": "",
        "note": "",
        "resolved_at_utc": None,
        "audit_trail": [],
        "timestamp_utc": _now_utc_iso(),
    }
    append_record(rec)
    return rec


def _latest_snapshot(records: list[dict], incident_id: str) -> dict | None:
    for r in reversed(records):
        if str(r.get("incident_id")) == str(incident_id):
            return r
    return None


def update_workflow_fields(incident_id: str, handled_by: str, actions_taken: str, note: str):
    records = load_incidents()
    base = _latest_snapshot(records, incident_id)
    if not base:
        return None

    upd = dict(base)
    upd["event_type"] = "INCIDENT_UPDATE"
    upd["timestamp_utc"] = _now_utc_iso()

    if handled_by != (base.get("handled_by") or ""):
        _append_audit(upd, "handled_by", base.get("handled_by"), handled_by)
        upd["handled_by"] = handled_by

    if actions_taken != (base.get("actions_taken") or ""):
        _append_audit(upd, "actions_taken", base.get("actions_taken"), actions_taken)
        upd["actions_taken"] = actions_taken

    if note != (base.get("note") or ""):
        _append_audit(upd, "note", base.get("note"), note)
        upd["note"] = note

    append_record(upd)
    return upd


def resolve_incident(incident_id: str):
    records = load_incidents()
    base = _latest_snapshot(records, incident_id)
    if not base:
        return None

    upd = dict(base)
    upd["event_type"] = "INCIDENT_UPDATE"
    upd["timestamp_utc"] = _now_utc_iso()

    if (base.get("status") or "OPEN") != "RESOLVED":
        _append_audit(upd, "status", base.get("status"), "RESOLVED")

    upd["status"] = "RESOLVED"
    upd["resolved_at_utc"] = _now_utc_iso()

    append_record(upd)
    return upd


def reopen_incident(incident_id: str):
    records = load_incidents()
    base = _latest_snapshot(records, incident_id)
    if not base:
        return None

    upd = dict(base)
    upd["event_type"] = "INCIDENT_UPDATE"
    upd["timestamp_utc"] = _now_utc_iso()

    if (base.get("status") or "OPEN") != "OPEN":
        _append_audit(upd, "status", base.get("status"), "OPEN")

    upd["status"] = "OPEN"
    upd["resolved_at_utc"] = None

    append_record(upd)
    return upd


# ---------------------------------------------------------
# Workspace helpers (append-only risk_bundle updates)
# ---------------------------------------------------------

def _deep_merge(dst: dict, src: dict) -> dict:
    """
    Deep-merge src into dst (dicts only). Returns dst.
    Lists are replaced (not merged).
    """
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst


def update_risk_bundle(incident_id: str, patch: dict, audit_note: str = "risk_bundle_update"):
    """
    Append-only snapshot update:
    - clones latest snapshot for incident_id
    - deep-merges `patch` into `risk_bundle`
    - appends audit_trail entry (field='risk_bundle')
    """
    records = load_incidents()
    base = _latest_snapshot(records, incident_id)
    if not base:
        return None

    upd = dict(base)
    upd["event_type"] = "INCIDENT_UPDATE"
    upd["timestamp_utc"] = _now_utc_iso()

    old_rb = dict(base.get("risk_bundle") or {})
    new_rb = dict(old_rb)
    if isinstance(patch, dict):
        _deep_merge(new_rb, patch)

    _append_audit(upd, "risk_bundle", "(patched)", audit_note)
    upd["risk_bundle"] = new_rb

    append_record(upd)
    return upd


def append_timeline_entry(incident_id: str, entry: dict, audit_note: str = "timeline_append"):
    """
    Append a single timeline entry into risk_bundle.timeline (append-only snapshot).
    """
    entry = dict(entry or {})
    entry.setdefault("ts_utc", _now_utc_iso())
    entry.setdefault("event", "SHIFT_LOG")

    records = load_incidents()
    base = _latest_snapshot(records, incident_id)
    if not base:
        return None

    rb = dict(base.get("risk_bundle") or {})
    tl = rb.get("timeline")
    if not isinstance(tl, list):
        tl = []
    tl = list(tl)
    tl.append(entry)
    rb["timeline"] = tl

    upd = dict(base)
    upd["event_type"] = "INCIDENT_UPDATE"
    upd["timestamp_utc"] = _now_utc_iso()
    _append_audit(upd, "timeline", "(append)", audit_note)
    upd["risk_bundle"] = rb

    append_record(upd)
    return upd


def update_tasks(incident_id: str, tasks: list, audit_note: str = "tasks_update"):
    """
    Replace risk_bundle.tasks with provided list (append-only snapshot).
    """
    if not isinstance(tasks, list):
        tasks = []

    records = load_incidents()
    base = _latest_snapshot(records, incident_id)
    if not base:
        return None

    rb = dict(base.get("risk_bundle") or {})
    rb["tasks"] = list(tasks)

    upd = dict(base)
    upd["event_type"] = "INCIDENT_UPDATE"
    upd["timestamp_utc"] = _now_utc_iso()
    _append_audit(upd, "tasks", "(replace)", audit_note)
    upd["risk_bundle"] = rb

    append_record(upd)
    return upd


def set_workspace_status(incident_id: str, status: str):
    """
    Set incident status for workspace flow (OPEN/MITIGATING/MONITORING/CLOSED).
    Uses append-only snapshot.
    """
    status = (status or "").upper().strip()
    allowed = {"OPEN", "MITIGATING", "MONITORING", "CLOSED"}
    if status not in allowed:
        return None

    records = load_incidents()
    base = _latest_snapshot(records, incident_id)
    if not base:
        return None

    old = base.get("status")
    if str(old).upper() == status:
        return base

    upd = dict(base)
    upd["event_type"] = "INCIDENT_UPDATE"
    upd["timestamp_utc"] = _now_utc_iso()
    _append_audit(upd, "status", old, status)
    upd["status"] = status

    append_record(upd)
    return upd
 