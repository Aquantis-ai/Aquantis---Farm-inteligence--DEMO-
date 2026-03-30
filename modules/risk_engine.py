from __future__ import annotations

from typing import Any

def parse_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            s = x.strip().replace(",", ".")
            if s == "" or s.lower() in {"none", "null", "nan"}:
                return None
            return float(s)
        return float(x)
    except Exception:
        return None

def nh3_fraction(ph: float, temp_c: float) -> float:
    t_k = temp_c + 273.15
    pka = 0.09018 + (2729.92 / t_k)
    frac = 1.0 / (1.0 + (10 ** (pka - ph)))
    return max(0.0, min(1.0, float(frac)))

def estimate_nh3_nh4_from_tan(tan: float, ph: float, temp_c: float):
    if tan is None or ph is None or temp_c is None:
        return None, None
    try:
        frac = nh3_fraction(ph, temp_c)
        nh3 = float(tan) * frac
        nh4 = float(tan) - nh3
        return nh3, nh4
    except Exception:
        return None, None

def estimate_tan_from_nh3_nh4(nh3: float | None, nh4: float | None) -> float | None:
    if nh3 is None and nh4 is None:
        return None
    return float(nh3 or 0.0) + float(nh4 or 0.0)

REQUIRED_FOR_CONF = ["o2", "no2", "ph", "temp_c"]

def compute_missing_and_confidence(parsed: dict):
    missing = [k for k in REQUIRED_FOR_CONF if parsed.get(k) is None]
    present = len(REQUIRED_FOR_CONF) - len(missing)
    conf = present / float(len(REQUIRED_FOR_CONF))
    if parsed.get("tan") is not None or parsed.get("nh3_explicit") is not None or parsed.get("nh4_explicit") is not None:
        conf = min(1.0, conf + 0.15)
    if parsed.get("no3") is not None:
        conf = min(1.0, conf + 0.05)
    return missing, float(conf)

STATUS_ORDER = ["OK", "WATCH", "WARNING", "STOP"]

def _update_status(cur: str, new: str) -> str:
    return new if STATUS_ORDER.index(new) > STATUS_ORDER.index(cur) else cur

def evaluate_risk_from_thresholds(parsed: dict, thresholds: dict) -> dict:
    status = "OK"
    triggered = []

    def check_lower_is_bad(val, warn, stop, label):
        nonlocal status, triggered
        if val is None:
            return
        if stop is not None and val <= stop:
            status = _update_status(status, "STOP")
            triggered.append(f"{label}<=stop")
        elif warn is not None and val <= warn:
            status = _update_status(status, "WARNING")
            triggered.append(f"{label}<=warn")

    def check_higher_is_bad(val, warn, stop, label):
        nonlocal status, triggered
        if val is None:
            return
        if stop is not None and val >= stop:
            status = _update_status(status, "STOP")
            triggered.append(f"{label}>=stop")
        elif warn is not None and val >= warn:
            status = _update_status(status, "WARNING")
            triggered.append(f"{label}>=warn")

    check_lower_is_bad(parsed.get("o2"), thresholds.get("o2_warn"), thresholds.get("o2_stop"), "o2")
    check_higher_is_bad(parsed.get("no2"), thresholds.get("no2_warn"), thresholds.get("no2_stop"), "no2")

    nh3 = parsed.get("nh3_explicit") or parsed.get("nh3_est")
    nh4 = parsed.get("nh4_explicit") or parsed.get("nh4_est")
    check_higher_is_bad(nh3, thresholds.get("nh3_warn"), thresholds.get("nh3_stop"), "nh3")
    check_higher_is_bad(nh4, thresholds.get("nh4_warn"), thresholds.get("nh4_stop"), "nh4")

    check_higher_is_bad(parsed.get("no3"), thresholds.get("no3_warn"), thresholds.get("no3_stop"), "no3")

    ph = parsed.get("ph")
    if ph is not None:
        check_lower_is_bad(ph, thresholds.get("ph_low_warn"), thresholds.get("ph_low_stop"), "ph_low")
        check_higher_is_bad(ph, thresholds.get("ph_high_warn"), thresholds.get("ph_high_stop"), "ph_high")

    t = parsed.get("temp_c")
    if t is not None:
        check_lower_is_bad(t, thresholds.get("temp_low_warn"), thresholds.get("temp_low_stop"), "temp_low")
        check_higher_is_bad(t, thresholds.get("temp_high_warn"), thresholds.get("temp_high_stop"), "temp_high")

    sal = parsed.get("salinity_ppt")
    if sal is not None:
        check_higher_is_bad(sal, thresholds.get("sal_high_warn"), thresholds.get("sal_high_stop"), "sal_high")

    if status == "OK" and triggered:
        status = "WATCH"

    return {"status": status, "triggered_by": triggered, "thresholds_applied": dict(thresholds)}

def build_risk_bundle(*, ph, temp_c, o2, no2, no3, tan, nh3_explicit, nh4_explicit, salinity_ppt, thresholds: dict) -> dict:
    tan_est = tan
    if tan_est is None and (nh3_explicit is not None or nh4_explicit is not None):
        tan_est = estimate_tan_from_nh3_nh4(nh3_explicit, nh4_explicit)

    nh3_est = nh4_est = None
    if tan_est is not None and ph is not None and temp_c is not None:
        nh3_est, nh4_est = estimate_nh3_nh4_from_tan(tan_est, ph, temp_c)

    parsed = {
        "ph": ph, "temp_c": temp_c, "o2": o2, "no2": no2, "no3": no3,
        "tan": tan_est, "salinity_ppt": salinity_ppt,
        "nh3_explicit": nh3_explicit, "nh4_explicit": nh4_explicit,
        "nh3_est": nh3_est, "nh4_est": nh4_est,
    }
    missing, confidence = compute_missing_and_confidence(parsed)
    risk = evaluate_risk_from_thresholds(parsed, thresholds)
    return {"parsed": parsed, "missing": missing, "confidence": confidence, "risk_result": risk}
