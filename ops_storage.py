from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import pandas as pd

from modules.workspace import get_workspace_id

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _get_daily_log_path() -> Path:
    workspace_id = get_workspace_id()
    return DATA_DIR / f"daily_log_{workspace_id}.jsonl"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _save_daily_logs(records: list[dict]) -> None:
    daily_log_path = _get_daily_log_path()
    with open(daily_log_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def append_daily_log(record: dict) -> dict:
    rec = dict(record or {})
    rec.setdefault("event_type", "DAILY_LOG")
    rec.setdefault("timestamp_utc", _now_utc_iso())

    if not rec.get("date"):
        try:
            rec["date"] = str(rec["timestamp_utc"])[:10]
        except Exception:
            rec["date"] = _now_utc_iso()[:10]

    farm_id = str(rec.get("farm_id") or "")
    unit_id = str(rec.get("unit_id") or "")
    date_key = str(rec.get("date") or "")
    daily_log_path = _get_daily_log_path()

    if not farm_id or not unit_id or not date_key:
        with open(daily_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return rec

    records = load_daily_logs()
    updated = False

    for i, existing in enumerate(records):
        if (
            str(existing.get("farm_id") or "") == farm_id
            and str(existing.get("unit_id") or "") == unit_id
            and str(existing.get("date") or "") == date_key
        ):
            old_timestamp = existing.get("timestamp_utc")
            rec["timestamp_utc"] = old_timestamp or rec.get("timestamp_utc") or _now_utc_iso()
            rec["updated_at_utc"] = _now_utc_iso()
            records[i] = rec
            updated = True
            break

    if not updated:
        records.append(rec)

    _save_daily_logs(records)
    return rec


def load_daily_logs() -> list[dict]:
    daily_log_path = _get_daily_log_path()
    if not daily_log_path.exists():
        return []
    out: list[dict] = []
    with open(daily_log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = (line or "").strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def logs_to_df(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records)
    return df