from __future__ import annotations

from datetime import datetime, timezone
import streamlit as st
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

import ops_storage as ops
from modules.dt import to_utc_datetime
from modules.settings import load_farm_profiles, get_active_farm
from modules.ui_text import t


# -----------------------------
# Helpers
# -----------------------------
# Full possible unit-level params; Daily report will show only active ones
PARAM_ORDER = [
    ("ph", "pH"),
    ("temp", "Temp"),
    ("salinity", "Salinity"),
    ("no2", "NO2"),
    ("no3", "NO3"),
    ("nh3", "NH3"),
    ("nh4", "NH4"),
    ("tan", "TAN"),
]

# Trend keys over last 7 days
TREND_KEYS = ["no2", "tan", "unit_o2_min"]

DEFAULT_UNITS = {
    "ph": "pH",
    "temp": "°C",
    "salinity": "ppt",
    "o2": "mg/L",
    "no2": "mg/L",
    "no3": "mg/L",
    "nh3": "mg/L",
    "nh4": "mg/L",
    "tan": "mg/L",
}

# For these parameters, lower values are generally better.
# We should not penalize "near min" or "below target" states.
LOW_IS_GOOD_KEYS = {"no2", "no3", "nh3", "nh4", "tan"}

NUM_CFG = {
    "ph": dict(min_value=0.0, max_value=14.0, step=0.01, fmt="%.2f"),
    "temp": dict(min_value=0.0, max_value=40.0, step=0.1, fmt="%.1f"),
    "salinity": dict(min_value=0.0, max_value=50.0, step=0.1, fmt="%.1f"),
    "o2": dict(min_value=0.0, max_value=30.0, step=0.1, fmt="%.2f"),
    "no2": dict(min_value=0.0, max_value=10.0, step=0.01, fmt="%.3f"),
    "no3": dict(min_value=0.0, max_value=500.0, step=1.0, fmt="%.1f"),
    "nh3": dict(min_value=0.0, max_value=5.0, step=0.001, fmt="%.4f"),
    "nh4": dict(min_value=0.0, max_value=50.0, step=0.01, fmt="%.3f"),
    "tan": dict(min_value=0.0, max_value=50.0, step=0.01, fmt="%.3f"),
}


def _safe_dict(x):
    return x if isinstance(x, dict) else {}


def _safe_list(x):
    return x if isinstance(x, list) else []


def _today_utc_date_iso() -> str:
    return datetime.now().date().isoformat()


def _farm_options(db: dict) -> List[dict]:
    farms = db.get("farms") or []
    return [f for f in farms if isinstance(f, dict)]


def _farm_label(f: dict) -> str:
    return f"{f.get('farm_name','Farm')} ({f.get('farm_id','')})"


def _get_measurement_units(farm: dict) -> dict:
    mu = farm.get("measurement_units")
    if isinstance(mu, dict):
        out = dict(DEFAULT_UNITS)
        out.update({k: v for k, v in mu.items() if v})
        return out
    return dict(DEFAULT_UNITS)


def _get_unit_options(farm: dict) -> List[dict]:
    units = farm.get("units") or []
    return [u for u in units if isinstance(u, dict)]


def _active_unit_params(unit: dict) -> List[Tuple[str, str]]:
    active = _safe_dict(unit.get("param_active"))
    out: List[Tuple[str, str]] = []
    for k, label in PARAM_ORDER:
        if bool(active.get(k, False)):
            out.append((k, label))
    return out


def _flatten_values_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    for k, _ in PARAM_ORDER:
        col = f"values.{k}"
        if col in df.columns and k not in df.columns:
            df[k] = pd.to_numeric(df[col], errors="coerce")

    for col in ["planned_feed_kg_total", "mortality_total_n", "unit_o2_min", "unit_o2_avg", "unit_o2_spread"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _filter_last_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty:
        return df
    if "timestamp_dt" not in df.columns:
        return df

    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(days=days)
    return df[(df["timestamp_dt"].notna()) & (df["timestamp_dt"] >= start) & (df["timestamp_dt"] <= end)]


def _find_today_record(records: List[dict], farm_id: Any, unit_id: Any, date_iso: str) -> Optional[dict]:
    if not records:
        return None

    candidates: List[dict] = []
    for rec in records:
        if (
            str(rec.get("farm_id") or "") == str(farm_id)
            and str(rec.get("unit_id") or "") == str(unit_id)
        ):
            rec_date = str(rec.get("date") or "")
            if not rec_date:
                ts = str(rec.get("timestamp_utc") or "")
                if ts:
                    rec_date = ts[:10]
            if rec_date == date_iso:
                candidates.append(rec)

    if not candidates:
        return None

    candidates.sort(
        key=lambda r: str(r.get("updated_at_utc") or r.get("timestamp_utc") or ""),
        reverse=True,
    )
    return candidates[0]


def _load_daily_form_state(
    farm_id: Any,
    unit_id: Any,
    active_unit_params: List[Tuple[str, str]],
    tanks: List[dict],
    record: Optional[dict],
):
    operator_key = f"daily_operator::{farm_id}::{unit_id}"
    note_key = f"daily_note::{farm_id}::{unit_id}"

    if record:
        values = _safe_dict(record.get("values"))
        tank_entries = _safe_list(record.get("tank_entries"))

        st.session_state[operator_key] = str(record.get("operator") or "")
        st.session_state[note_key] = str(record.get("note") or "")

        for k, _ in active_unit_params:
            cfg = NUM_CFG.get(k, dict(min_value=0.0))
            widget_key = f"daily_val::{farm_id}::{unit_id}::{k}"
            raw_val = values.get(k)
            try:
                st.session_state[widget_key] = float(raw_val) if raw_val is not None else float(cfg["min_value"])
            except Exception:
                st.session_state[widget_key] = float(cfg["min_value"])

        tank_map = {}
        for te in tank_entries:
            ted = _safe_dict(te)
            tank_map[str(ted.get("tank_id") or "")] = ted

        for ti, tank in enumerate(tanks):
            t = _safe_dict(tank)
            tank_id = str(t.get("tank_id") or f"TNK-{ti}")
            entry = _safe_dict(tank_map.get(tank_id))

            mort_key = f"daily_mort::{farm_id}::{unit_id}::{tank_id}"
            st.session_state[mort_key] = int(entry.get("mortality_n") or 0)

            o2_key = f"daily_o2::{farm_id}::{unit_id}::{tank_id}"
            if bool(t.get("o2_active", False)):
                raw_o2 = entry.get("o2_mg_l")
                try:
                    st.session_state[o2_key] = float(raw_o2) if raw_o2 is not None else float(NUM_CFG["o2"]["min_value"])
                except Exception:
                    st.session_state[o2_key] = float(NUM_CFG["o2"]["min_value"])
    else:
        st.session_state[operator_key] = ""
        st.session_state[note_key] = ""

        for k, _ in active_unit_params:
            cfg = NUM_CFG.get(k, dict(min_value=0.0))
            st.session_state[f"daily_val::{farm_id}::{unit_id}::{k}"] = float(cfg["min_value"])

        for ti, tank in enumerate(tanks):
            t = _safe_dict(tank)
            tank_id = str(t.get("tank_id") or f"TNK-{ti}")
            st.session_state[f"daily_mort::{farm_id}::{unit_id}::{tank_id}"] = 0
            if bool(t.get("o2_active", False)):
                st.session_state[f"daily_o2::{farm_id}::{unit_id}::{tank_id}"] = float(NUM_CFG["o2"]["min_value"])


def _queue_daily_form_restore(farm_id: Any, unit_id: Any, date_iso: str):
    st.session_state["_daily_restore_request"] = {
        "farm_id": str(farm_id),
        "unit_id": str(unit_id),
        "date": str(date_iso),
    }


def _apply_queued_daily_form_restore(active_unit_params: List[Tuple[str, str]], tanks: List[dict]) -> bool:
    req = st.session_state.pop("_daily_restore_request", None)
    if not req:
        return False

    farm_id = str(req.get("farm_id") or "")
    unit_id = str(req.get("unit_id") or "")
    date_iso = str(req.get("date") or "")

    records = ops.load_daily_logs()
    rec = _find_today_record(records, farm_id, unit_id, date_iso)
    _load_daily_form_state(farm_id, unit_id, active_unit_params, tanks, rec)
    st.session_state["_daily_loaded_signature"] = f"{farm_id}::{unit_id}::{date_iso}"
    return True


def _risk_bump(cur: str, new: str) -> str:
    order = {"OK": 0, "WATCH": 1, "HIGH": 2}
    return new if order[new] > order[cur] else cur


def _risk_priority(reason_type: str) -> int:
    priorities = {
        "below_min": 1,
        "above_max": 1,
        "mortality": 1,
        "o2_low_tank": 1,
        "trend_critical": 2,
        "near_limit": 3,
        "off_target": 4,
        "trend_watch": 4,
    }
    return priorities.get(reason_type, 9)


def _compare_value_to_limits(
    *,
    key: str,
    label: str,
    value: Optional[float],
    mins: dict,
    targets: dict,
    maxs: dict,
    units: dict,
) -> Tuple[str, List[str], List[dict]]:
    if value is None:
        return "OK", [], []

    level = "OK"
    signals: List[str] = []
    risk_items: List[dict] = []

    mn = mins.get(key)
    tgt = targets.get(key)
    mx = maxs.get(key)
    unit_txt = units.get(key, "")
    low_is_good = key in LOW_IS_GOOD_KEYS

    if mn is not None and value < float(mn):
        level = _risk_bump(level, "HIGH")
        signals.append(f"{label} below min ({value} {unit_txt} < {mn} {unit_txt})".strip())
        risk_items.append({"label": f"{label} low", "kind": "below_min"})

    if mx is not None and value > float(mx):
        level = _risk_bump(level, "HIGH")
        signals.append(f"{label} above max ({value} {unit_txt} > {mx} {unit_txt})".strip())
        risk_items.append({"label": f"{label} high", "kind": "above_max"})

    if mn is not None and mx is not None and float(mx) > float(mn) and level != "HIGH":
        span = float(mx) - float(mn)
        band = span * 0.10
        zero_baseline_case = (float(mn) == 0.0 and float(value) == 0.0)

        if not zero_baseline_case:
            if not low_is_good and value <= float(mn) + band:
                level = _risk_bump(level, "WATCH")
                signals.append(f"{label} near min")
                risk_items.append({"label": f"{label} near min", "kind": "near_limit"})
            elif value >= float(mx) - band:
                level = _risk_bump(level, "WATCH")
                signals.append(f"{label} near max")
                risk_items.append({"label": f"{label} near max", "kind": "near_limit"})

    if tgt is not None and level != "HIGH":
        try:
            tgt_f = float(tgt)
            if not (tgt_f == 0.0 and float(value) == 0.0):
                dev = abs(float(value) - tgt_f)
                tol = max(abs(tgt_f) * 0.25, 0.10)

                if low_is_good:
                    if float(value) > tgt_f and dev > tol:
                        level = _risk_bump(level, "WATCH")
                        signals.append(f"{label} off target")
                        risk_items.append({"label": f"{label} off target", "kind": "off_target"})
                else:
                    if dev > tol:
                        level = _risk_bump(level, "WATCH")
                        signals.append(f"{label} off target")
                        risk_items.append({"label": f"{label} off target", "kind": "off_target"})
        except Exception:
            pass

    return level, signals, risk_items


def _risk_from_unit(values: dict, unit_cfg: dict, units: dict, mortality_total: int) -> Tuple[str, List[str], List[dict]]:
    mins = _safe_dict(unit_cfg.get("mins"))
    targets = _safe_dict(unit_cfg.get("targets"))
    maxs = _safe_dict(unit_cfg.get("maxs"))
    active_params = _active_unit_params(unit_cfg)

    level = "OK"
    signals: List[str] = []
    risk_items: List[dict] = []

    for key, label in active_params:
        sub_level, sub_signals, sub_items = _compare_value_to_limits(
            key=key,
            label=label,
            value=values.get(key),
            mins=mins,
            targets=targets,
            maxs=maxs,
            units=units,
        )
        level = _risk_bump(level, sub_level)
        signals.extend(sub_signals)
        risk_items.extend(sub_items)

    if mortality_total > 0:
        level = _risk_bump(level, "WATCH")
        signals.append(f"Mortality reported ({mortality_total})")
        risk_items.append({"label": "Mortality present", "kind": "mortality"})

    seen = set()
    uniq_items = []
    for item in risk_items:
        ikey = (item.get("label"), item.get("kind"))
        if ikey not in seen:
            uniq_items.append(item)
            seen.add(ikey)

    signals = list(dict.fromkeys(signals))
    return level, signals, uniq_items


def _o2_summary_from_tanks(
    tanks: List[dict],
    tank_o2_values: Dict[str, Optional[float]],
    active_tank_ids: List[str],
    o2_unit_txt: str,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[dict]]:
    vals: List[Tuple[str, str, float]] = []

    for ti, tank in enumerate(tanks):
        t = _safe_dict(tank)
        tank_id = str(t.get("tank_id") or f"TNK-{ti}")
        if tank_id not in active_tank_ids:
            continue
        tank_name = str(t.get("tank_name") or f"Tank {ti+1}")
        v = tank_o2_values.get(tank_id)
        if v is None:
            continue
        vals.append((tank_id, tank_name, float(v)))

    if not vals:
        return None, None, None, None

    only_vals = [x[2] for x in vals]
    min_v = min(only_vals)
    avg_v = sum(only_vals) / len(only_vals)
    spread_v = max(only_vals) - min(only_vals)

    worst = min(vals, key=lambda x: x[2])
    worst_dict = {
        "tank_id": worst[0],
        "tank_name": worst[1],
        "o2": worst[2],
        "unit": o2_unit_txt,
    }
    return min_v, avg_v, spread_v, worst_dict


def _risk_from_tank_o2(
    *,
    measurement_units: dict,
    tank_o2_values: Dict[str, Optional[float]],
    tanks: List[dict],
) -> Tuple[str, List[str], List[dict], Dict[str, Optional[float]]]:
    level = "OK"
    signals: List[str] = []
    risk_items: List[dict] = []

    active_tank_ids: List[str] = []
    for ti, tank in enumerate(tanks):
        t = _safe_dict(tank)
        if bool(t.get("o2_active", False)):
            active_tank_ids.append(str(t.get("tank_id") or f"TNK-{ti}"))

    o2_min, o2_avg, o2_spread, worst = _o2_summary_from_tanks(
        tanks=tanks,
        tank_o2_values=tank_o2_values,
        active_tank_ids=active_tank_ids,
        o2_unit_txt=measurement_units.get("o2", "mg/L"),
    )

    if worst is None:
        return level, signals, risk_items, {
            "unit_o2_min": None,
            "unit_o2_avg": None,
            "unit_o2_spread": None,
        }

    worst_tank_id = worst["tank_id"]
    worst_tank_name = worst["tank_name"]
    v = worst["o2"]
    unit_txt = measurement_units.get("o2", "")

    tank_cfg = None
    for ti, tank in enumerate(tanks):
        t = _safe_dict(tank)
        tank_id = str(t.get("tank_id") or f"TNK-{ti}")
        if tank_id == worst_tank_id:
            tank_cfg = t
            break

    tank_cfg = _safe_dict(tank_cfg)
    mn = tank_cfg.get("o2_min")
    tgt = tank_cfg.get("o2_target")
    mx = tank_cfg.get("o2_max")

    if mn is not None and v < float(mn):
        level = _risk_bump(level, "HIGH")
        signals.append(f"O2 below min in {worst_tank_name} ({v} {unit_txt} < {mn} {unit_txt})".strip())
        risk_items.append({"label": "O2 low", "kind": "o2_low_tank"})

    if mx is not None and v > float(mx):
        level = _risk_bump(level, "HIGH")
        signals.append(f"O2 above max in {worst_tank_name} ({v} {unit_txt} > {mx} {unit_txt})".strip())
        risk_items.append({"label": "O2 high", "kind": "above_max"})

    if mn is not None and mx is not None and float(mx) > float(mn) and level != "HIGH":
        span = float(mx) - float(mn)
        band = span * 0.10
        if v <= float(mn) + band:
            level = _risk_bump(level, "WATCH")
            signals.append(f"O2 near min in {worst_tank_name}")
            risk_items.append({"label": "O2 near min", "kind": "near_limit"})
        elif v >= float(mx) - band:
            level = _risk_bump(level, "WATCH")
            signals.append(f"O2 near max in {worst_tank_name}")
            risk_items.append({"label": "O2 near max", "kind": "near_limit"})

    if tgt is not None and level != "HIGH":
        try:
            tgt_f = float(tgt)
            dev = abs(float(v) - tgt_f)
            tol = max(abs(tgt_f) * 0.25, 0.10)
            if dev > tol:
                level = _risk_bump(level, "WATCH")
                signals.append(f"O2 off target in {worst_tank_name}")
                risk_items.append({"label": "O2 off target", "kind": "off_target"})
        except Exception:
            pass

    if o2_spread is not None and o2_spread >= 1.0:
        level = _risk_bump(level, "WATCH")
        signals.append(f"O2 variability across unit high (spread {o2_spread:.2f} {unit_txt})")
        risk_items.append({"label": "O2 variability high", "kind": "trend_watch"})

    seen = set()
    uniq_items = []
    for item in risk_items:
        ikey = (item.get("label"), item.get("kind"))
        if ikey not in seen:
            uniq_items.append(item)
            seen.add(ikey)

    return level, signals, uniq_items, {
        "unit_o2_min": o2_min,
        "unit_o2_avg": o2_avg,
        "unit_o2_spread": o2_spread,
    }


def _trend_flags(df7: pd.DataFrame) -> Tuple[List[dict], List[str]]:
    trend_items: List[dict] = []
    checklist: List[str] = []

    if df7.empty or "timestamp_dt" not in df7.columns:
        return trend_items, checklist

    dfx = df7.sort_values("timestamp_dt")

    for k in TREND_KEYS:
        if k not in dfx.columns:
            continue
        s = pd.to_numeric(dfx[k], errors="coerce").dropna()
        if len(s) < 3:
            continue

        slope = float(s.iloc[-1] - s.iloc[0])

        if k in ("no2", "tan") and slope > 0:
            trend_items.append({"label": f"{k.upper()} trend up", "kind": "trend_watch"})
        elif k == "unit_o2_min" and slope < 0:
            trend_items.append({"label": "O2 trend down", "kind": "trend_watch"})

    for item in trend_items[:3]:
        label = item["label"]
        if "NO2" in label:
            checklist.append("Check biofilter aeration / loading")
        elif "TAN" in label:
            checklist.append("Review feeding and verify TAN + pH")
        elif "O2" in label:
            checklist.append("Check aeration/blower and verify O2 at fish level")

    return trend_items[:3], checklist[:5]


def _prioritize_top_risks(base_items: List[dict], trend_items: List[dict]) -> List[str]:
    combined = _safe_list(base_items) + _safe_list(trend_items)

    combined_sorted = sorted(
        combined,
        key=lambda x: (_risk_priority(str(x.get("kind"))), str(x.get("label"))),
    )

    out = []
    seen = set()
    for item in combined_sorted:
        label = str(item.get("label") or "").strip()
        if not label or label in seen:
            continue
        out.append(label)
        seen.add(label)
        if len(out) >= 3:
            break

    return out


def _summary_checklist(level: str, top_risks: List[str], trend_checks: List[str], mortality_total: int) -> List[str]:
    checks: List[str] = []

    for r in top_risks[:3]:
        r_low = r.lower()
        if "o2" in r_low:
            checks.append("Check aeration / blower / circulation")
        elif "no2" in r_low:
            checks.append("Check biofilter and repeat NO2")
        elif "tan" in r_low or "nh3" in r_low or "nh4" in r_low:
            checks.append("Verify TAN/NH3 and review feeding")
        elif "temp" in r_low:
            checks.append("Check temperature stability")
        elif "salinity" in r_low:
            checks.append("Verify salinity and water source")
        elif "mortality" in r_low:
            checks.append("Log affected tanks and inspect fish behavior")
        elif "ph" in r_low:
            checks.append("Verify pH and review recent water chemistry changes")

    checks.extend(trend_checks)

    if mortality_total > 0:
        checks.append("Inspect tanks with mortality first")

    checks = list(dict.fromkeys(checks))
    return checks[:5]


def _risk_text(level: str) -> str:
    return level


# -----------------------------
# Main
# -----------------------------
def render_daily_report():
    st.subheader(t("today.title", "Daily report"))

    db = load_farm_profiles()
    farms = _farm_options(db)
    active_farm = get_active_farm(db)

    if not farms:
        st.warning(t("today.msg.no_farms", "No farms found."))
        st.info(t("today.msg.go_settings_create_farm", "Go to Settings → create farm."))
        return

    farm_labels = [_farm_label(f) for f in farms]
    default_idx = 0
    if active_farm:
        for i, f in enumerate(farms):
            if str(f.get("farm_id")) == str(active_farm.get("farm_id")):
                default_idx = i
                break

    farm_label = st.selectbox(
        t("label.farm", "Farm"),
        farm_labels,
        index=default_idx,
        key="daily_farm_select",
    )
    farm = farms[farm_labels.index(farm_label)]
    farm_id = farm.get("farm_id")
    farm_name = farm.get("farm_name") or "Farm"

    units = _get_unit_options(farm)
    if not units:
        st.warning(t("today.msg.no_units", "No units in this farm."))
        st.info(t("today.msg.go_settings_create_unit", "Go to Settings → create unit."))
        return

    unit_labels = [(u.get("unit_name") or t("settings.unit.fallback_name", "Unit")).strip() or t("settings.unit.fallback_name", "Unit") for u in units]
    unit_label = st.selectbox(
        t("today.field.unit", "Unit"),
        unit_labels,
        index=0,
        key=f"daily_unit_select::{farm_id}",
    )
    unit = units[unit_labels.index(unit_label)]
    unit_id = unit.get("unit_id")
    unit_name = unit.get("unit_name") or "Unit"

    measurement_units = _get_measurement_units(farm)
    tanks = _safe_list(unit.get("tanks"))
    active_unit_params = _active_unit_params(unit)

    records = ops.load_daily_logs()
    df = ops.logs_to_df(records)
    if not df.empty:
        if "timestamp_utc" in df.columns:
            df["timestamp_dt"] = to_utc_datetime(df["timestamp_utc"])
        else:
            df["timestamp_dt"] = pd.NaT
        df = _flatten_values_columns(df)

    today_iso = _today_utc_date_iso()
    load_signature = f"{farm_id}::{unit_id}::{today_iso}"

    if not _apply_queued_daily_form_restore(active_unit_params, tanks):
        today_record = _find_today_record(records, farm_id, unit_id, today_iso)
        if st.session_state.get("_daily_loaded_signature") != load_signature:
            _load_daily_form_state(farm_id, unit_id, active_unit_params, tanks, today_record)
            st.session_state["_daily_loaded_signature"] = load_signature
    else:
        today_record = _find_today_record(ops.load_daily_logs(), farm_id, unit_id, today_iso)

    if today_record:
        st.info(
            t(
                "today.msg.today_loaded",
                "Today's report loaded. You can edit values and press Save again to update today's report.",
            )
        )

    operator_key = f"daily_operator::{farm_id}::{unit_id}"
    operator = st.text_input(t("label.operator", "Operator"), key=operator_key)

    # -----------------------------
    # Measurements (unit-level)
    # -----------------------------
    st.markdown(f"### {t('today.measurements.title', 'Measurements')}")

    values: Dict[str, Optional[float]] = {}
    if not active_unit_params:
        st.info(t("today.msg.no_active_unit_params", "No active unit parameters. Go to Settings → Unit → Set active."))
    else:
        cols = st.columns(3)
        for i, (k, label) in enumerate(active_unit_params):
            cfg = NUM_CFG.get(k, dict(min_value=0.0, max_value=1000.0, step=0.1, fmt="%.2f"))
            widget_key = f"daily_val::{farm_id}::{unit_id}::{k}"

            if widget_key not in st.session_state:
                st.session_state[widget_key] = float(cfg["min_value"])

            with cols[i % 3]:
                values[k] = float(
                    st.number_input(
                        f"{label} ({measurement_units.get(k,'')})",
                        min_value=float(cfg["min_value"]),
                        max_value=float(cfg["max_value"]),
                        step=float(cfg["step"]),
                        format=str(cfg["fmt"]),
                        key=widget_key,
                    )
                )

    st.divider()

    # -----------------------------
    # Tanks: O2 per tank + mortality per tank + feed plan
    # -----------------------------
    st.markdown(f"### {t('today.tanks.title', 'Tanks')}")

    tank_entries: List[dict] = []
    total_mortality = 0
    total_planned_feed = 0.0
    tank_o2_values: Dict[str, Optional[float]] = {}

    if not tanks:
        st.info(t("today.msg.no_tanks", "No tanks in this unit."))
    else:
        h = st.columns([1.2, 1.0, 1.0, 1.1, 1.0, 1.0])
        h[0].markdown(f"**{t('label.tank', 'Tank')}**")
        h[1].markdown(f"**{t('today.table.biomass_kg', 'Biomass (kg)')}**")
        h[2].markdown(f"**{t('today.table.avg_weight_g', 'Avg wt (g)')}**")
        h[3].markdown(f"**{t('today.table.feed_kg_day', 'Feed - kg/day')}**")
        h[4].markdown(f"**{t('today.table.o2', 'O2')}**")
        h[5].markdown(f"**{t('today.table.mortality', 'Mortality')}**")

        for ti, tank in enumerate(tanks):
            tnk = _safe_dict(tank)
            tank_id = str(tnk.get("tank_id") or f"TNK-{ti}")
            tank_name = str(tnk.get("tank_name") or f"Tank {ti+1}")

            biomass_kg = tnk.get("biomass_kg")
            avg_weight_g = tnk.get("avg_weight_g")
            feed_kg_day = tnk.get("feed_kg_day")

            if feed_kg_day is not None:
                total_planned_feed += float(feed_kg_day)

            row = st.columns([1.2, 1.0, 1.0, 1.1, 1.0, 1.0])
            row[0].write(tank_name)
            row[1].write(f"{float(biomass_kg):.1f}" if biomass_kg is not None else "—")
            row[2].write(f"{float(avg_weight_g):.1f}" if avg_weight_g is not None else "—")
            row[3].write(f"{float(feed_kg_day):.2f}" if feed_kg_day is not None else "—")

            o2_active = bool(tnk.get("o2_active", False))
            tank_o2: Optional[float] = None
            if o2_active:
                o2_key = f"daily_o2::{farm_id}::{unit_id}::{tank_id}"
                if o2_key not in st.session_state:
                    st.session_state[o2_key] = float(NUM_CFG["o2"]["min_value"])

                tank_o2 = float(
                    row[4].number_input(
                        "",
                        min_value=float(NUM_CFG["o2"]["min_value"]),
                        max_value=float(NUM_CFG["o2"]["max_value"]),
                        step=float(NUM_CFG["o2"]["step"]),
                        format=str(NUM_CFG["o2"]["fmt"]),
                        key=o2_key,
                    )
                )
                tank_o2_values[tank_id] = tank_o2
            else:
                row[4].write("—")

            mort_key = f"daily_mort::{farm_id}::{unit_id}::{tank_id}"
            if mort_key not in st.session_state:
                st.session_state[mort_key] = 0

            mort = int(
                row[5].number_input(
                    "",
                    min_value=0,
                    step=1,
                    key=mort_key,
                )
            )
            total_mortality += mort

            tank_entries.append({
                "tank_id": tank_id,
                "tank_name": tank_name,
                "biomass_kg": biomass_kg,
                "avg_weight_g": avg_weight_g,
                "feed_kg_day": feed_kg_day,
                "o2_active": o2_active,
                "o2_mg_l": tank_o2,
                "o2_min": tnk.get("o2_min"),
                "o2_target": tnk.get("o2_target"),
                "o2_max": tnk.get("o2_max"),
                "mortality_n": mort,
            })

    # -----------------------------
    # O2 summary for unit
    # -----------------------------
    o2_level, o2_signals, o2_risk_items, o2_summary = _risk_from_tank_o2(
        measurement_units=measurement_units,
        tank_o2_values=tank_o2_values,
        tanks=tanks,
    )

    st.markdown(f"### {t('today.o2_summary.title', 'O2 summary')}")
    o1, o2c, o3 = st.columns(3)
    with o1:
        st.metric(
            t("today.metric.lowest_o2", "Lowest O2"),
            f"{o2_summary.get('unit_o2_min'):.2f}" if o2_summary.get("unit_o2_min") is not None else "—",
        )
    with o2c:
        st.metric(
            t("today.metric.average_o2", "Average O2"),
            f"{o2_summary.get('unit_o2_avg'):.2f}" if o2_summary.get("unit_o2_avg") is not None else "—",
        )
    with o3:
        st.metric(
            t("today.metric.o2_spread", "O2 spread"),
            f"{o2_summary.get('unit_o2_spread'):.2f}" if o2_summary.get("unit_o2_spread") is not None else "—",
        )

    note_key = f"daily_note::{farm_id}::{unit_id}"
    if note_key not in st.session_state:
        st.session_state[note_key] = ""

    note = st.text_area(t("label.notes", "Notes"), height=120, key=note_key)

    b1, b2 = st.columns([1, 1])
    save = b1.button(t("action.save", "Save"), key=f"daily_save::{farm_id}::{unit_id}")
    reset = b2.button(t("today.button.reset_form", "Reset form"), key=f"daily_reset::{farm_id}::{unit_id}")

    if reset:
        _queue_daily_form_restore(farm_id, unit_id, today_iso)
        st.rerun()

    # -----------------------------
    # Compute risk
    # -----------------------------
    base_level, base_signals, base_risk_items = _risk_from_unit(values, unit, measurement_units, total_mortality)
    level = _risk_bump(base_level, o2_level)

    signals = list(dict.fromkeys(base_signals + o2_signals))
    base_risk_items = _safe_list(base_risk_items) + _safe_list(o2_risk_items)

    df7 = pd.DataFrame()
    if not df.empty:
        dfx = df.copy()
        if "farm_id" in dfx.columns:
            dfx = dfx[dfx["farm_id"].astype(str) == str(farm_id)]
        if "unit_id" in dfx.columns:
            dfx = dfx[dfx["unit_id"].astype(str) == str(unit_id)]
        df7 = _filter_last_days(dfx, 7)

    trend_items, trend_checks = _trend_flags(df7)

    if trend_items and level == "OK":
        level = "WATCH"

    combined_top = _prioritize_top_risks(base_risk_items, trend_items)
    checklist = _summary_checklist(level, combined_top, trend_checks, total_mortality)

    # -----------------------------
    # Save
    # -----------------------------
    if save:
        rec = {
            "date": today_iso,
            "farm_id": farm_id,
            "farm_name": farm_name,
            "unit_id": unit_id,
            "unit_name": unit_name,
            "operator": (operator or "").strip() or None,
            "values": values,
            "tank_entries": tank_entries,
            "unit_o2_min": o2_summary.get("unit_o2_min"),
            "unit_o2_avg": o2_summary.get("unit_o2_avg"),
            "unit_o2_spread": o2_summary.get("unit_o2_spread"),
            "planned_feed_kg_total": total_planned_feed,
            "mortality_total_n": total_mortality,
            "risk_level": level,
            "top_risks": combined_top,
            "signals": signals,
            "checklist": checklist,
            "note": (note or "").strip() or None,
        }
        ops.append_daily_log(rec)

        st.success(t("msg.saved", "Saved."))
        st.rerun()

    st.divider()

    # -----------------------------
    # Output
    # -----------------------------
    st.markdown(f"### {t('today.risk_level.title', 'Risk Level')}: {_risk_text(level)}")

    if combined_top:
        st.markdown(f"**{t('today.top_risks.title', 'Top risks')}**")
        for item in combined_top:
            st.write(f"- {item}")

    if checklist:
        st.markdown(f"**{t('today.checklist.title', 'Checklist')}**")
        for item in checklist:
            st.write(f"- {item}")

    if signals:
        with st.expander(t("today.signals.title", "Signals"), expanded=False):
            for s in signals[:20]:
                st.write(f"- {s}")

    s1, s2, s3 = st.columns(3)
    with s1:
        st.metric(t("today.metric.planned_feed_kg_day", "Planned feed kg/day"), f"{total_planned_feed:.2f}")
    with s2:
        st.metric(t("today.metric.mortality_total", "Mortality total"), int(total_mortality))
    with s3:
        st.metric(t("today.metric.tanks", "Tanks"), len(tank_entries))

    # -----------------------------
    # Last 7 days
    # -----------------------------
    with st.expander(t("today.last_7_days.title", "Last 7 days"), expanded=False):
        if df7.empty:
            st.info(t("today.msg.no_daily_logs", "No daily logs yet."))
        else:
            show_cols = [
                "timestamp_utc",
                "operator",
                "unit_name",
                "unit_o2_min",
                "unit_o2_avg",
                "unit_o2_spread",
                "planned_feed_kg_total",
                "mortality_total_n",
                "risk_level",
                "note",
            ]

            for k, _ in active_unit_params:
                if k not in show_cols:
                    show_cols.insert(3, k)

            existing = [c for c in show_cols if c in df7.columns]
            dshow = df7.sort_values("timestamp_dt", ascending=False)
            st.dataframe(dshow[existing], use_container_width=True, hide_index=True)