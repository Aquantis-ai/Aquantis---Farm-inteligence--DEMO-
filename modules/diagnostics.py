from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

import aquantis_storage as storage
from rag import retrieve  # ✅ uses your rag.py
from modules.ui_text import t


# -----------------------------
# Farm profile loader (MVP)
# -----------------------------
FARM_PROFILES_PATH = Path("data") / "farm_profiles.json"


def _load_farm_profiles() -> dict:
    if not FARM_PROFILES_PATH.exists():
        return {"active_farm_id": None, "farms": []}
    try:
        return json.loads(FARM_PROFILES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"active_farm_id": None, "farms": []}


def _list_farms() -> List[dict]:
    db = _load_farm_profiles()
    farms = db.get("farms") or []
    return farms if isinstance(farms, list) else []


def _get_active_farm() -> Optional[dict]:
    db = _load_farm_profiles()
    active_id = db.get("active_farm_id")
    farms = db.get("farms") or []
    if not farms:
        return None
    if active_id:
        for f in farms:
            if str(f.get("farm_id")) == str(active_id):
                return f
    return farms[0]


def _get_farm_by_id(farm_id: Any) -> Optional[dict]:
    farms = _list_farms()
    for f in farms:
        if str(f.get("farm_id")) == str(farm_id):
            return f
    return None


# -----------------------------
# Farm fields
# -----------------------------
DEFAULT_MEASUREMENT_UNITS = {
    "temp": "°C",
    "o2": "mg/L",
    "tan": "mg/L",
    "nh3": "mg/L",
    "nh4": "mg/L",
    "no2": "mg/L",
    "no3": "mg/L",
    "salinity": "ppt",
    "ph": "pH",
}


def _farm_unit_names(farm: Optional[dict]) -> List[str]:
    u = (farm or {}).get("units")
    if isinstance(u, list) and u:
        out = []
        for x in u:
            xd = x if isinstance(x, dict) else {}
            name = str(xd.get("unit_name") or "").strip()
            if name:
                out.append(name)
        if out:
            return out
    return [t("settings.unit.fallback_name", "Unit")]


def _farm_measurement_units(farm: Optional[dict]) -> dict:
    mu = (farm or {}).get("measurement_units")
    if isinstance(mu, dict):
        out = dict(DEFAULT_MEASUREMENT_UNITS)
        out.update({k: v for k, v in mu.items() if v})
        return out
    return dict(DEFAULT_MEASUREMENT_UNITS)


def _farm_thresholds(farm: Optional[dict]) -> dict:
    tdata = (farm or {}).get("thresholds") or {}
    return dict(tdata) if isinstance(tdata, dict) else {}


# -----------------------------
# Species list (KB list)
# -----------------------------
KB_SPECIES_LIST = [
    "Pike-perch - Sander lucioperca",
    "European perch - Perca fluviatilis",
    "Rainbow trout - Oncorhynchus mykiss",
    "Brook trout - Salvelinus fontinalis",
    "Atlantic salmon - Salmo salar",
    "Nile tilapia - Oreochromis niloticus",
    "Common carp - Cyprinus carpio",
    'Koi carp - Cyprinus rubrofuscus "koi"',
    "Wels catfish - Silurus glanis",
    "African catfish - Clarias gariepinus",
    "Pangasius - Pangasianodon hypophthalmus",
    "Siberian sturgeon - Acipenser baerii",
    "Russian sturgeon - Acipenser gueldenstaedtii",
    "Sterlet - Acipenser ruthenus",
    "Beluga sturgeon - Huso huso",
    "Grass carp - Ctenopharyngodon idella",
    "Silver carp - Hypophthalmichthys molitrix",
    "Tench - Tinca tinca",
    "Golden tench - Tinca tinca gold",
    "Goldfish - Carassius auratus",
    "Burbot - Lota lota",
]

CUSTOM_SPECIES_OPTION = "Other / Custom species"


# -----------------------------
# Session state
# -----------------------------
def _ensure_state():
    st.session_state.setdefault("diag_result", None)         # includes protocol + context + kb meta
    st.session_state.setdefault("diag_md", None)             # rendered output
    st.session_state.setdefault("diag_fingerprint", None)
    st.session_state.setdefault("diag_last_fp", None)
    st.session_state.setdefault("diag_last_incident_id", None)
    st.session_state.setdefault("diag_reset_nonce", 0)


def _clear_diag_output():
    st.session_state["diag_result"] = None
    st.session_state["diag_md"] = None
    st.session_state["diag_fingerprint"] = None


def _reset_diag_form(farm_id: Any):
    st.session_state["diag_result"] = None
    st.session_state["diag_md"] = None
    st.session_state["diag_fingerprint"] = None

    current_nonce = int(st.session_state.get("diag_reset_nonce", 0))

    keys_to_clear = [
        f"diag_operator_{current_nonce}",
        f"diag_mortality_{current_nonce}",
        f"diag_species_kb_{current_nonce}",
        f"diag_species_custom_{current_nonce}",
        f"diag_duration_{current_nonce}",
        f"diag_symptoms_{current_nonce}",
        f"diag_unit_name_{farm_id}_{current_nonce}",
        f"diag_ph_{farm_id}_{current_nonce}",
        f"diag_temp_{farm_id}_{current_nonce}",
        f"diag_sal_{farm_id}_{current_nonce}",
        f"diag_no2_{farm_id}_{current_nonce}",
        f"diag_no3_{farm_id}_{current_nonce}",
        f"diag_o2_{farm_id}_{current_nonce}",
        f"diag_tan_{farm_id}_{current_nonce}",
        f"diag_nh3_{farm_id}_{current_nonce}",
        f"diag_nh4_{farm_id}_{current_nonce}",
    ]

    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]

    st.session_state["diag_reset_nonce"] = current_nonce + 1


def _fingerprint(payload: dict) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(payload)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -----------------------------
# Helpers: species
# -----------------------------
def _extract_species_selection(species_data: Any) -> tuple[list[str], str]:
    """
    Supports both old format:
      species_pack: list[str]
    and new format:
      species_pack: {"selected": [...], "custom": "..."}
    """
    if isinstance(species_data, dict):
        selected = species_data.get("selected") or []
        custom = str(species_data.get("custom") or "").strip()
    else:
        selected = species_data if isinstance(species_data, list) else []
        custom = ""

    selected_clean = [v for v in selected if v in KB_SPECIES_LIST]
    if custom:
        selected_clean = [v for v in selected_clean if v != CUSTOM_SPECIES_OPTION]
        selected_clean.append(CUSTOM_SPECIES_OPTION)

    return selected_clean, custom


def _resolve_species_value(selected_species: str, custom_species: str) -> str:
    custom_clean = str(custom_species or "").strip()
    if selected_species == CUSTOM_SPECIES_OPTION and custom_clean:
        return custom_clean
    return selected_species


# -----------------------------
# Helpers: measurement struct + canonical text
# -----------------------------
def _nz(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _canonical_measurements_text(meas: dict, mu: dict) -> str:
    order = ["pH", "temp", "salinity", "O2", "NO2", "NO3", "TAN", "NH3", "NH4"]
    parts = []
    for k in order:
        v = meas.get(k)
        if v is None:
            continue
        if k == "pH":
            parts.append(f"pH={v}")
        elif k == "temp":
            parts.append(f"temp={v} {mu.get('temp','')}".strip())
        elif k == "salinity":
            parts.append(f"salinity={v} {mu.get('salinity','')}".strip())
        elif k == "O2":
            parts.append(f"O2={v} {mu.get('o2','')}".strip())
        elif k == "NO2":
            parts.append(f"NO2={v} {mu.get('no2','')}".strip())
        elif k == "NO3":
            parts.append(f"NO3={v} {mu.get('no3','')}".strip())
        elif k == "TAN":
            parts.append(f"TAN={v} {mu.get('tan','')}".strip())
        elif k == "NH3":
            parts.append(f"NH3={v} {mu.get('nh3','')}".strip())
        elif k == "NH4":
            parts.append(f"NH4={v} {mu.get('nh4','')}".strip())
    return ", ".join(parts) if parts else t("diagnostics.value.not_provided", "Not provided")


# -----------------------------
# Heuristics / safety rails
# -----------------------------
CORE_MEASUREMENT_KEYS = ["pH", "temp", "O2", "NO2", "TAN", "NH3"]
FILLER_CAUSE_PATTERNS = [
    "environmental stress",
    "low activity",
    "fluctuating feed intake",
    "flow-related stress",
    "flow",
    "lethargy",
    "restlessness",
]


def _norm_text(s: Any) -> str:
    return str(s or "").strip().lower()


def _contains_any(text: str, needles: list[str]) -> bool:
    ttxt = _norm_text(text)
    return any(n in ttxt for n in needles)


def _dedupe_keep_order(items: list[str]) -> list[str]:
    out = []
    seen = set()
    for x in items:
        if not isinstance(x, str):
            continue
        x2 = x.strip()
        if not x2:
            continue
        key = x2.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(x2)
    return out


def _measurement_present(v: Any) -> bool:
    return v is not None


def _count_missing_core(meas: dict) -> tuple[int, list[str]]:
    missing = []
    for k in CORE_MEASUREMENT_KEYS:
        if not _measurement_present(meas.get(k)):
            missing.append(k)
    return len(missing), missing


def _symptoms_too_vague(symptoms: str) -> bool:
    s = _norm_text(symptoms)
    if not s:
        return True
    if len(s) < 18:
        return True
    vague_patterns = [
        "fish look strange",
        "weird",
        "not feeding",
        "something is wrong",
        "problem",
    ]
    strong_patterns = [
        "gasping at the surface",
        "rapid breathing",
        "at the surface",
        "near the inlet",
        "rubbing",
        "loss of coordination",
        "apathy",
        "darkening",
        "part of the system",
        "one section",
        "section",
    ]
    return _contains_any(s, vague_patterns) and not _contains_any(s, strong_patterns)


def _severity_from_context(duration: str, mortality: str) -> str:
    if str(mortality).startswith("Yes, massive"):
        return "critical"
    if str(mortality).startswith("Yes, tens"):
        return "high"
    if str(mortality).startswith("Yes"):
        return "moderate"
    if duration == "< 6 hours":
        return "moderate"
    return "routine"


def _build_rule_hints(
    *,
    species: str,
    symptoms: str,
    duration: str,
    mortality: str,
    measurements: dict,
) -> dict:
    s = _norm_text(symptoms)
    o2 = _nz(measurements.get("O2"))
    no2 = _nz(measurements.get("NO2"))
    no3 = _nz(measurements.get("NO3"))
    tan = _nz(measurements.get("TAN"))
    nh3 = _nz(measurements.get("NH3"))
    ph = _nz(measurements.get("pH"))
    temp = _nz(measurements.get("temp"))

    missing_count, missing_items = _count_missing_core(measurements)
    insufficient_data = missing_count >= 3 or _symptoms_too_vague(symptoms)

    acute = duration in ("< 6 hours", "6–24 hours")
    chronic = duration == "More than 3 days"

    symptom_flags = {
        "surface_gasping": _contains_any(s, ["gasping at the surface", "gasping", "at the surface", "coming up to the surface"]),
        "rapid_breathing": _contains_any(s, ["rapid breathing", "fast breathing", "increased respiration"]),
        "near_inflow": _contains_any(s, ["near the inlet", "by the inlet"]),
        "apathy": _contains_any(s, ["apat", "slower response", "less active", "reduced activity"]),
        "rubbing": _contains_any(s, ["rubbing", "flashing"]),
        "local_section": _contains_any(s, ["part of the system", "one section", "section", "only in one part"]),
        "coordination_loss": _contains_any(s, ["loss of coordination", "lying on the bottom", "collapse"]),
        "poor_feeding": _contains_any(s, ["poor feed intake", "reduced feed intake", "not feeding"]),
    }

    rule_scores: dict[str, dict] = {}

    def push(name: str, score: int, why: str):
        prev = rule_scores.get(name)
        if not prev or score > prev.get("score", 0):
            rule_scores[name] = {"score": score, "why": why}

    # Missing / uncertainty guardrail
    if insufficient_data:
        push(
            "Insufficient data / low confidence",
            95,
            "Multiple key measurements are missing or the symptoms are too vague, so a dominant cause cannot be identified reliably.",
        )

    # Acute oxygen stress
    if o2 is not None:
        if o2 <= 4.5:
            push(
                "Acute oxygen stress / hypoxia",
                96 if acute else 90,
                f"O2 {o2} mg/L is severely low and, together with the clinical signs, can explain acute stress or collapse.",
            )
        elif o2 <= 5.5 and (symptom_flags["surface_gasping"] or symptom_flags["rapid_breathing"]):
            push(
                "Acute oxygen stress / hypoxia",
                92,
                f"O2 {o2} mg/L together with surface gasping / respiratory distress strongly points to oxygen stress.",
            )
        elif o2 <= 6.5 and symptom_flags["surface_gasping"]:
            push(
                "Oxygen limitation / low O2",
                84,
                f"O2 {o2} mg/L is borderline and, together with the symptoms, may be the main stressor.",
            )

    # Local hydraulic / distribution issue
    if symptom_flags["local_section"]:
        loc_score = 88 if o2 is not None and o2 <= 6.5 else 78
        push(
            "Local hydraulic / oxygen distribution issue",
            loc_score,
            "The problem appears limited to one part of the system, which fits a local issue with flow, water distribution, or oxygenation more than whole-system chemistry.",
        )

    # Nitrite / biofilter stress
    if no2 is not None:
        if no2 >= 0.30:
            push(
                "Nitrite spike / biofilter stress",
                91,
                f"NO2 {no2} mg/L is clearly elevated and, together with a 1–3 day course, suggests a nitrite spike or nitrification stress.",
            )
        elif no2 >= 0.15:
            push(
                "Nitrite stress",
                78,
                f"NO2 {no2} mg/L is elevated and may be contributing significantly to fish stress.",
            )
        elif no2 >= 0.05 and acute is False and not insufficient_data:
            push(
                "Mild nitrite elevation / secondary stressor",
                52,
                f"NO2 {no2} mg/L may be a secondary stressor, but alone may not explain the full picture.",
            )

    if (no2 is not None and no2 >= 0.20) and (tan is not None and tan >= 0.8):
        push(
            "Nitrite spike / biofilter stress",
            93,
            f"The combination of NO2 {no2} mg/L and TAN {tan} mg/L fits a nitrification or biofilter overload problem more than an isolated single-parameter deviation.",
        )

    # Ammonia / TAN / NH3
    if nh3 is not None and nh3 >= 0.03:
        score = 88
        if ph is not None and ph >= 7.8:
            score += 4
        if temp is not None and temp >= 20:
            score += 3
        push(
            "Ammonia risk / NH3 toxicity",
            min(score, 95),
            f"NH3 {nh3} mg/L is elevated and the toxicity risk increases further at higher pH / temperature.",
        )
    elif tan is not None and tan >= 1.0 and (ph is not None and ph >= 7.8) and (temp is not None and temp >= 20):
        push(
            "Ammonia risk / TAN-driven NH3 concern",
            82,
            f"TAN {tan} mg/L together with higher pH and temperature increases suspicion of ammonia stress.",
        )
    elif tan is not None and tan >= 1.5:
        push(
            "Elevated TAN / ammonia load",
            70,
            f"TAN {tan} mg/L is elevated and may contribute to stress or biofilter loading.",
        )

    # Chronic nitrate / long-term water quality issue
    if chronic and no3 is not None and no3 >= 100 and (o2 is None or o2 >= 7.0) and (no2 is None or no2 < 0.10) and (nh3 is None or nh3 <= 0.02):
        push(
            "Chronic water quality issue / nitrate accumulation",
            86,
            f"NO3 {no3} mg/L is high while the acute parameters are otherwise relatively stable, which fits a long-term water quality issue more than an acute emergency.",
        )

    # Chronic uncertain override
    if (
        chronic
        and not insufficient_data
        and (no2 is None or no2 <= 0.08)
        and (nh3 is None or nh3 <= 0.02)
        and (o2 is None or o2 >= 6.5)
        and (not str(mortality).startswith("Yes"))
        and (symptom_flags["rubbing"] or symptom_flags["apathy"] or symptom_flags["poor_feeding"])
    ):
        push(
            "Chronic non-specific stress / further diagnostics required",
            76,
            "The course is chronic and the data do not point to one clearly dominant acute cause; broader differential diagnostics are appropriate.",
        )

    # Chronic non-specific issue fallback
    if chronic and not insufficient_data and len(rule_scores) == 0:
        push(
            "Chronic non-specific stress / further diagnostics required",
            68,
            "The course is chronic and the available data do not indicate one clearly dominant acute cause; broader diagnostics should be added.",
        )

    # Confidence cap
    confidence_cap = 95
    if insufficient_data:
        confidence_cap = 35
    elif chronic and len(rule_scores) <= 1:
        confidence_cap = 70

    ranked = sorted(
        [{"name": k, "confidence": v["score"], "why": v["why"]} for k, v in rule_scores.items()],
        key=lambda x: x["confidence"],
        reverse=True,
    )

    return {
        "insufficient_data": insufficient_data,
        "missing_core_count": missing_count,
        "missing_core_items": missing_items,
        "acute": acute,
        "chronic": chronic,
        "severity_band": _severity_from_context(duration, mortality),
        "symptom_flags": symptom_flags,
        "ranked_rule_causes": ranked[:4],
        "confidence_cap": confidence_cap,
    }


def _safe_protocol_for_insufficient_data(rule_hints: dict) -> dict:
    missing = rule_hints.get("missing_core_items") or []
    missing_txt = ", ".join(missing) if missing else "O2, pH, temp, NO2, TAN, NH3"

    return {
        "probable_causes": [
            {
                "name": "Insufficient data / low confidence",
                "confidence": min(35, int(rule_hints.get("confidence_cap", 35))),
                "why": "Multiple key measurements are missing or the symptoms are too general, so a dominant cause cannot be identified safely without more data.",
            }
        ],
        "immediate_actions_0_30m": [
            "Add the core measurements before interpreting the case: O2, pH, temperature, NO2, TAN, and NH3.",
            "Check whether any zero values are actually unfilled fields or measurement errors.",
            "Observe whether the problem affects all tanks or only a specific section / tank.",
            "If fish are breathing rapidly, staying at the surface, or mortality is starting, treat the case as a possible O2 emergency and recheck O2 immediately.",
        ],
        "stabilization_2_24h": [
            "Repeat the diagnostics once the missing data are added and the parameter set is complete.",
            "Add a more precise behavior description: gasping, apathy, rubbing, localization of the problem, changes in feed intake.",
        ],
        "tests_to_verify": [
            f"Add the missing core measurements: {missing_txt}.",
            "Verify O2 both in the tank and at the inlet / other parts of the system.",
            "Review recent changes in feeding, biomass, biofilter status, cleaning, or system interventions.",
        ],
        "escalation_thresholds": [
            "Escalate immediately if rapid breathing, surface gasping, loss of coordination, or mortality begins.",
            "Escalate if the basic measurements cannot be obtained quickly and the clinical signs are worsening.",
        ],
    }


def _clean_list_items(items: list, *, max_items: int = 8) -> list[str]:
    out: list[str] = []
    if not isinstance(items, list):
        return out
    for x in items:
        if not isinstance(x, str):
            continue
        txt = re.sub(r"\s+", " ", x).strip(" -•\n\t")
        if not txt:
            continue
        out.append(txt)
        if len(out) >= max_items:
            break
    return _dedupe_keep_order(out)


def _normalize_cause_name(name: str) -> str:
    txt = _norm_text(name)
    if "insufficient data" in txt or "low confidence" in txt:
        return "Insufficient data / low confidence"
    if "oxygen" in txt or "hypoxi" in txt or "o₂" in txt or "o2" in txt:
        if "local" in txt or "distribution" in txt or "hydraulic" in txt:
            return "Local hydraulic / oxygen distribution issue"
        return "Acute oxygen stress / hypoxia"
    if "nitrite" in txt or "no₂" in txt or "no2" in txt or "nitroz" in txt:
        if "biofilter" in txt or "nitrification" in txt:
            return "Nitrite spike / biofilter stress"
        if "secondary" in txt or "mild" in txt:
            return "Mild nitrite elevation / secondary stressor"
        return "Nitrite stress"
    if "ammon" in txt or "nh₃" in txt or "nh3" in txt or "tan" in txt:
        if "risk" in txt or "tox" in txt:
            return "Ammonia risk / NH3 toxicity"
        return "Elevated TAN / ammonia load"
    if "nitrate" in txt or "no₃" in txt or "no3" in txt:
        return "Chronic water quality issue / nitrate accumulation"
    if "hydraulic" in txt or "distribution" in txt:
        return "Local hydraulic / oxygen distribution issue"
    if "chronic non-specific" in txt or "further diagnostics" in txt:
        return "Chronic non-specific stress / further diagnostics required"
    return name.strip() if isinstance(name, str) else "Unspecified cause"


def _cause_is_filler(name: str, rule_hints: dict) -> bool:
    n = _norm_text(name)
    if not n:
        return True
    if any(p in n for p in FILLER_CAUSE_PATTERNS):
        if "flow" in n:
            return not rule_hints.get("symptom_flags", {}).get("local_section")
        return True
    return False


def _spread_cause_confidences(causes: list[dict]) -> list[dict]:
    if not causes:
        return []
    out = sorted(causes, key=lambda x: int(x.get("confidence", 0)), reverse=True)
    primary = int(out[0].get("confidence", 0))
    if len(out) >= 2:
        out[1]["confidence"] = min(int(out[1].get("confidence", 0)), max(primary - 15, 0))
    if len(out) >= 3:
        out[2]["confidence"] = min(int(out[2].get("confidence", 0)), max(primary - 22, 0))
    return out


def _merge_causes(llm_causes: list, rule_hints: dict) -> list[dict]:
    merged: list[dict] = []

    for c in rule_hints.get("ranked_rule_causes") or []:
        merged.append(
            {
                "name": _normalize_cause_name(c.get("name", "")),
                "confidence": int(c.get("confidence", 50)),
                "why": str(c.get("why") or "").strip(),
            }
        )

    for c in llm_causes or []:
        if not isinstance(c, dict):
            continue
        name = _normalize_cause_name(str(c.get("name") or ""))
        if _cause_is_filler(name, rule_hints):
            continue
        why = str(c.get("why") or "").strip()
        try:
            conf = int(c.get("confidence", 50))
        except Exception:
            conf = 50
        merged.append({"name": name, "confidence": conf, "why": why})

    best: dict[str, dict] = {}
    for c in merged:
        name = str(c.get("name") or "").strip()
        if not name:
            continue
        cur = best.get(name)
        if cur is None or int(c.get("confidence", 0)) > int(cur.get("confidence", 0)):
            best[name] = c
        elif cur and not cur.get("why") and c.get("why"):
            cur["why"] = c.get("why")

    out = list(best.values())
    out.sort(key=lambda x: int(x.get("confidence", 0)), reverse=True)

    cap = int(rule_hints.get("confidence_cap", 95))
    for c in out:
        c["confidence"] = max(0, min(cap, int(c.get("confidence", 50))))

    out = _spread_cause_confidences(out)
    return out[:3]


def _prepend_priority_actions(base: list[str], extra: list[str]) -> list[str]:
    return _dedupe_keep_order((extra or []) + (base or []))


def _semantic_key(text: str) -> str:
    txt = _norm_text(text)
    replacements = {
        "oxygenation": "aeration",
        "oxygen supply": "aeration",
        "re-measure": "check",
        "measure again": "check",
        "verify": "check",
        "partial water exchange": "water exchange",
        "water exchange": "water exchange",
        "water dilution": "water exchange",
        "concentration reduction": "reduction",
        "in different parts of the system": "in system",
        "in other points of the system": "in system",
    }
    for old, new in replacements.items():
        txt = txt.replace(old, new)
    txt = re.sub(r"[^a-z0-9₂₃₄₅₆₇₈₉₀\s/+-]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _semantic_dedupe(items: list[str], *, max_items: int = 6) -> list[str]:
    out = []
    seen = set()
    for item in items:
        if not isinstance(item, str) or not item.strip():
            continue
        key = _semantic_key(item)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item.strip())
        if len(out) >= max_items:
            break
    return out


def _filter_generic_fillers(items: list[str], *, allow_generic: bool = False) -> list[str]:
    generic_patterns = [
        "monitor water parameters",
        "maintain stable conditions",
        "maintain stable temperature",
        "maintain a stable feeding regime",
        "perform filter system maintenance",
        "check and optimize mechanical filtration",
    ]
    out = []
    for item in items:
        it = _norm_text(item)
        if not allow_generic and any(p in it for p in generic_patterns):
            continue
        out.append(item)
    return out


def _filter_actions_by_top_cause(items: list[str], top_name: str, flags: dict) -> list[str]:
    out = []
    top = _norm_text(top_name)
    has_resp_distress = bool(flags.get("surface_gasping") or flags.get("rapid_breathing"))

    for item in items:
        it = _norm_text(item)

        if "chronic water quality issue / nitrate accumulation" in top:
            if any(x in it for x in ["oxygenation", "aeration", "oxygen supply", "blowers", "o₂", "o2"]) and not has_resp_distress:
                continue
            if "stop" in it and "feed" in it:
                continue
            if "immediately" in it and ("re-measure" in it or "check" in it) and ("o₂" in it or "o2" in it):
                continue

        if "chronic non-specific" in top:
            if any(x in it for x in ["oxygenation", "aeration", "oxygen supply", "blowers"]) and not has_resp_distress:
                continue
            if "stop" in it and "feed" in it:
                continue

        if "ammonia risk" in top:
            if "oxygen shortfall" in it:
                continue
            if "oxygen supply" in it or "blowers" in it:
                continue

        if "nitrite spike / biofilter stress" in top:
            if any(x in it for x in ["oxygen supply", "blowers"]) and not has_resp_distress:
                continue

        if "local hydraulic / oxygen distribution issue" in top:
            pass

        if "acute oxygen stress / hypoxia" in top:
            pass

        out.append(item)

    return out


def _inject_stabilization_defaults(stz: list[str], top_name: str) -> list[str]:
    top = _norm_text(top_name)

    if stz:
        return stz

    if "ammonia risk" in top:
        return [
            "Track NH3, TAN, pH, and temperature trends at shorter intervals until stabilization is confirmed.",
            "Keep feed load reduced until NH3 / TAN decline is confirmed.",
            "Review biofilter recovery and overall system loading.",
        ]

    if "nitrite spike / biofilter stress" in top:
        return [
            "Track NO2 and TAN trends at shorter intervals until stabilization is visible.",
            "Keep system loading and feed load lower until nitrification recovery is confirmed.",
            "Review biofilter stability and hydraulics through the biofilter.",
        ]

    if "chronic water quality issue / nitrate accumulation" in top:
        return [
            "Track NO3 trends over time and verify that the situation is gradually improving after water-management adjustments.",
            "Review water exchange / purge strategy and long-term system loading.",
            "Monitor changes in fish behavior and feed intake over the following days.",
        ]

    if "chronic non-specific" in top:
        return [
            "Track symptom trends over time instead of making a one-time conclusion.",
            "Add broader differential diagnostics based on gill / skin findings and system history.",
            "Review recent husbandry, feeding, and water-quality changes.",
        ]

    if "acute oxygen stress" in top or "oxygen limitation" in top or "local hydraulic / oxygen distribution issue" in top:
        return [
            "Track O2 trends at shorter intervals and verify the root cause of the oxygen shortfall instead of only increasing aeration once.",
            "Confirm that fish behavior improves after intervention and that O2 is not dropping further.",
            "Review whether the issue was local or system-wide.",
        ]

    return stz


def _apply_rule_overrides(proto: dict, rule_hints: dict) -> dict:
    if rule_hints.get("insufficient_data"):
        return _safe_protocol_for_insufficient_data(rule_hints)

    proto2 = dict(proto)
    proto2["probable_causes"] = _merge_causes(proto.get("probable_causes") or [], rule_hints)

    ia = _clean_list_items(proto.get("immediate_actions_0_30m") or [], max_items=8)
    stz = _clean_list_items(proto.get("stabilization_2_24h") or [], max_items=8)
    tests = _clean_list_items(proto.get("tests_to_verify") or [], max_items=8)
    esc = _clean_list_items(proto.get("escalation_thresholds") or [], max_items=8)

    causes = proto2.get("probable_causes") or []
    top_name = _norm_text(causes[0].get("name", "")) if causes else ""
    flags = rule_hints.get("symptom_flags") or {}
    severity_band = str(rule_hints.get("severity_band") or "")
    chronic = bool(rule_hints.get("chronic"))

    # Strong O2 logic
    if "oxygen" in top_name or "hypoxi" in top_name or flags.get("surface_gasping") or flags.get("rapid_breathing"):
        ia = _prepend_priority_actions(
            ia,
            [
                "Immediately increase aeration / oxygenation and verify that the system is actually delivering O2.",
                "Check flow, oxygen supply, blowers / diffusers, and any possible hydraulic failure.",
                "Temporarily stop or significantly reduce feeding until the situation stabilizes.",
                "Recheck O2 directly in the affected tank and at the inlet / other points in the system.",
            ],
        )
        stz = _prepend_priority_actions(
            stz,
            [
                "Track O2 trends at shorter intervals and verify the cause of the oxygen shortfall instead of only increasing aeration once.",
            ],
        )
        esc = _prepend_priority_actions(
            esc,
            [
                "Escalate immediately if fish behavior does not improve quickly after increasing O2.",
                "Escalate if mortality continues or the problem spreads to additional tanks / sections.",
            ],
        )

    # Local section logic
    if flags.get("local_section"):
        ia = _prepend_priority_actions(
            ia,
            [
                "Verify O2 and flow separately in individual tanks / sections, not only globally for the whole system.",
                "Check local water distribution, valves, blockages, and differences between the affected and unaffected section.",
            ],
        )
        tests = _prepend_priority_actions(
            tests,
            [
                "Compare O2, flow, and fish behavior between the affected and unaffected part of the system.",
            ],
        )

    # Nitrite / biofilter logic
    if "nitrite" in top_name or "biofilter" in top_name:
        ia = _prepend_priority_actions(
            ia,
            [
                "Immediately reduce feeding and lower further system loading.",
                "Consider partial water exchange / dilution according to the farm's operating conditions.",
                "Check the biofilter, flow through the biofilter, and recent changes in loading, cleaning, or system interventions.",
            ],
        )
        stz = _prepend_priority_actions(
            stz,
            [
                "Track NO2 and TAN trends at shorter intervals until stabilization becomes visible.",
                "Verify whether the issue is linked to nitrification failure or biofilter overload.",
            ],
        )

    # Ammonia logic
    if "ammonia" in top_name or "nh₃" in top_name or "nh3" in top_name or ("tan" in top_name and "nitrite" not in top_name):
        ia = _prepend_priority_actions(
            ia,
            [
                "Immediately reduce or stop feeding.",
                "Recheck TAN and NH3 and verify pH + temperature, because these directly affect NH3 toxicity.",
                "Review any recent increase in feed load, biomass, or biofilter loading.",
            ],
        )
        tests = _prepend_priority_actions(
            tests,
            [
                "Repeat TAN, NH3, pH, and temperature at the same time and at the same measurement point.",
            ],
        )

    # Chronic nitrate logic
    if "nitrate" in top_name or ("chronic" in top_name and chronic):
        ia = _prepend_priority_actions(
            ia,
            [
                "Do not treat the case as an acute emergency unless the clinical signs are worsening.",
                "Review long-term water exchange, NO3 accumulation, and overall system management.",
            ],
        )
        esc = _prepend_priority_actions(
            esc,
            [
                "Escalate if mild chronic signs shift into acute respiratory distress or if mortality begins.",
            ],
        )

    # Chronic uncertain logic
    if "chronic non-specific" in top_name:
        ia = _prepend_priority_actions(
            ia,
            [
                "Do not treat the case as an acute chemical emergency without further confirmation.",
                "Review husbandry changes, water quality over time, and overall system history.",
            ],
        )
        tests = _prepend_priority_actions(
            tests,
            [
                "Add broader differential diagnostics: water quality over time, gills / skin, parasites, system history, feeding changes.",
            ],
        )

    # Critical incident framing
    if severity_band == "critical":
        esc = _prepend_priority_actions(
            esc,
            [
                "Treat the case as a critical incident if mortality continues even after immediate intervention.",
                "Immediately verify whether a system-wide failure is affecting multiple parts of the operation.",
            ],
        )

    banned_fragments = [
        "chemic",
        "antibiotic",
        "medic",
        "dosing",
    ]

    def filter_lines(items: list[str]) -> list[str]:
        out = []
        for x in items:
            lx = _norm_text(x)
            if any(b in lx for b in banned_fragments):
                continue
            out.append(x)
        return out

    ia = filter_lines(ia)
    stz = filter_lines(stz)
    tests = filter_lines(tests)
    esc = filter_lines(esc)

    ia = _filter_generic_fillers(ia, allow_generic=False)
    stz = _filter_generic_fillers(stz, allow_generic=False)
    tests = _filter_generic_fillers(tests, allow_generic=True)
    esc = _filter_generic_fillers(esc, allow_generic=True)

    ia = _filter_actions_by_top_cause(ia, top_name, flags)
    stz = _filter_actions_by_top_cause(stz, top_name, flags)
    tests = _filter_actions_by_top_cause(tests, top_name, flags)
    esc = _filter_actions_by_top_cause(esc, top_name, flags)

    stz = _inject_stabilization_defaults(stz, top_name)

    proto2["immediate_actions_0_30m"] = _semantic_dedupe(ia, max_items=6)
    proto2["stabilization_2_24h"] = _semantic_dedupe(stz, max_items=6)
    proto2["tests_to_verify"] = _semantic_dedupe(tests, max_items=6)
    proto2["escalation_thresholds"] = _semantic_dedupe(esc, max_items=6)

    return proto2


# -----------------------------
# RAG: KB-RAS only
# -----------------------------
KB_RAS_DOMAIN = "KB-RAS"


def _rag_kb_ras_only(query: str, k: int = 6) -> Tuple[List[dict], List[str], float]:
    hits = retrieve(query=query, k=k, domains=[KB_RAS_DOMAIN])  # ✅ hard filter to KB-RAS
    sources = []
    best = 0.0
    if hits:
        sources = sorted(list({(h.get("source") or "unknown") for h in hits if (h.get("source") or "").strip()}))
        try:
            best = float(max(h.get("score", 0.0) for h in hits))
        except Exception:
            best = 0.0
    return hits, sources, best


def _format_kb_context(hits: List[dict], max_chars: int = 9000) -> str:
    """
    Compact KB context for the model. Keeps provenance.
    """
    if not hits:
        return ""
    blocks = []
    for h in hits:
        dom = h.get("domain") or "unknown"
        src = h.get("source") or "unknown"
        cid = h.get("chunk_id")
        score = h.get("score")
        txt = (h.get("text") or "").strip()
        if not txt:
            continue
        header = f"[{dom}] source={src} chunk={cid} score={score:.4f}"
        blocks.append(header + "\n" + txt)
    ctx = "\n\n---\n\n".join(blocks).strip()
    if len(ctx) > max_chars:
        ctx = ctx[:max_chars] + "\n\n[TRUNCATED]"
    return ctx


# -----------------------------
# LLM: JSON protocol (KB-only + rule hints)
# -----------------------------
def _call_llm_protocol_json(*, kb_context: str, user_payload: dict) -> dict:
    """
    Returns JSON:
    {
      probable_causes:[{name, confidence, why}],
      immediate_actions_0_30m: [...],
      stabilization_2_24h: [...],
      tests_to_verify: [...],
      escalation_thresholds: [...]
    }
    """
    from openai import OpenAI  # local import
    client = OpenAI()

    system = """
You are Aquantis. You create an actionable diagnostic protocol for RAS.

CRITICAL RULES:
- Respond ONLY from the "KB CONTEXT" and the provided input data / rule_hints below. Do not use outside knowledge.
- If rule_hints contains insufficient_data=true, you MUST NOT invent specific diagnoses with high confidence.
- Distinguish between:
  1) one primary probable cause,
  2) up to 2 contributing factors.
- Do not present mild secondary parameter deviations as equally important main causes unless they explain the symptoms equally well.
- Never rewrite symptoms as a diagnosis.
- Do not provide medication / antibiotic / chemical dosing (no treatment mg/L doses).
- Do not state claims like "X is above threshold Y" unless this is clearly supported by the input or rule_hints.
- Output MUST be a valid JSON object with the exact keys below.

JSON format:
{
  "probable_causes": [{"name": "...", "confidence": 0-100, "why": "..."}],
  "immediate_actions_0_30m": ["..."],
  "stabilization_2_24h": ["..."],
  "tests_to_verify": ["..."],
  "escalation_thresholds": ["..."]
}

Notes:
- probable_causes: max 3 items, confidence must be a number from 0–100.
- The 1st item should be the primary probable cause.
- If the issue is limited to one part of the system, consider a local hydraulic / distribution issue.
- If the issue is acute with respiratory distress, prioritize life-saving actions.
- If the KB does not contain specific escalation numbers, describe escalation qualitatively.
""".strip()

    user = {
        "kb_context": kb_context if kb_context else "(NO_KB_CONTEXT_FOUND)",
        "input": user_payload,
    }

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.15,
        max_tokens=900,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
    )
    txt = resp.choices[0].message.content or "{}"
    try:
        out = json.loads(txt)
        if isinstance(out, dict):
            return out
        return {"error": "non-dict-json", "raw": txt}
    except Exception:
        return {"error": "invalid-json", "raw": txt}


def _normalize_protocol(proto: dict) -> dict:
    """
    Ensure keys exist and are correct types (defensive).
    """
    def as_list(x):
        return x if isinstance(x, list) else []

    def as_causes(x):
        if not isinstance(x, list):
            return []
        out = []
        for c in x[:3]:
            if not isinstance(c, dict):
                continue
            name = c.get("name")
            why = c.get("why")
            conf = c.get("confidence")
            if not isinstance(name, str) or not name.strip():
                continue
            if not isinstance(why, str):
                why = ""
            try:
                conf_n = int(conf)
            except Exception:
                conf_n = 50
            conf_n = max(0, min(100, conf_n))
            out.append({"name": name.strip(), "confidence": conf_n, "why": why.strip()})
        return out

    return {
        "probable_causes": as_causes(proto.get("probable_causes")),
        "immediate_actions_0_30m": as_list(proto.get("immediate_actions_0_30m")),
        "stabilization_2_24h": as_list(proto.get("stabilization_2_24h")),
        "tests_to_verify": as_list(proto.get("tests_to_verify")),
        "escalation_thresholds": as_list(proto.get("escalation_thresholds")),
    }


# -----------------------------
# Rendering helpers
# -----------------------------
def _protocol_to_md(proto: dict) -> str:
    causes = proto.get("probable_causes") or []
    ia = proto.get("immediate_actions_0_30m") or []
    stz = proto.get("stabilization_2_24h") or []
    tests = proto.get("tests_to_verify") or []
    esc = proto.get("escalation_thresholds") or []

    def bullets(items: list) -> str:
        if not items:
            return "- —"
        out = []
        for x in items:
            if isinstance(x, str) and x.strip():
                out.append(f"- {x.strip()}")
        return "\n".join(out) if out else "- —"

    md = []

    primary = causes[:1]
    secondary = causes[1:3]

    md.append(f"### {t('diagnostics.result.primary_cause', 'Primary probable cause + confidence')}")
    if primary:
        c = primary[0]
        md.append(f"- **{c.get('name','—')}** ({c.get('confidence',50)}%) — {c.get('why','')}".strip())
    else:
        md.append("- —")

    md.append(f"\n### {t('diagnostics.result.secondary_factors', 'Contributing factors')}")
    if secondary:
        for c in secondary:
            md.append(f"- **{c.get('name','—')}** ({c.get('confidence',50)}%) — {c.get('why','')}".strip())
    else:
        md.append("- —")

    md.append(f"\n### {t('diagnostics.result.immediate_actions', 'Immediate actions (0–30 min)')}")
    md.append(bullets(ia))

    md.append(f"\n### {t('diagnostics.result.stabilization', 'Stabilization (2–24 h)')}")
    md.append(bullets(stz))

    md.append(f"\n### {t('diagnostics.result.tests_to_verify', 'What to verify / additional tests')}")
    md.append(bullets(tests))

    md.append(f"\n### {t('diagnostics.result.escalation_thresholds', 'When to escalate')}")
    md.append(bullets(esc))

    return "\n".join(md).strip()


def _protocol_to_tasks(proto: dict) -> List[dict]:
    """
    Simple checklist: immediate + tests (+ first few stabilization).
    """
    tasks: List[dict] = []

    def add(group: str, items: list, limit: int):
        if not isinstance(items, list):
            return
        n = 0
        for x in items:
            if not isinstance(x, str) or not x.strip():
                continue
            tasks.append({"title": x.strip(), "group": group, "state": "OPEN"})
            n += 1
            if n >= limit:
                break

    add(t("diagnostics.tasks.immediate_group", "Immediate (0–30 min)"), proto.get("immediate_actions_0_30m", []), 6)
    add(t("diagnostics.tasks.verify_group", "Verify / Tests"), proto.get("tests_to_verify", []), 4)
    add(t("diagnostics.tasks.stabilization_group", "Stabilization (2–24 h)"), proto.get("stabilization_2_24h", []), 3)

    return tasks


def _timeline_first_entry(*, farm_name: str, unit_name: str, operator: str, species: str) -> dict:
    return {
        "ts_utc": _now_utc_iso(),
        "who": operator.strip() or "—",
        "unit": unit_name,
        "event": "Created from Guided Diagnostics",
        "note": f"{farm_name} | {species}",
    }


def _append_farm_threshold_lines(thresholds: dict, mu: dict, proto: dict) -> dict:
    """
    Adds farm threshold lines to escalation list (configuration, not KB).
    Keeps KB-only LLM rule intact because this is deterministic farm config overlay.
    """
    esc = list(proto.get("escalation_thresholds") or [])
    extra = []

    def add_if(key: str, label: str, unit_key: str):
        v = thresholds.get(key)
        vv = _nz(v)
        if vv is None:
            return
        u = mu.get(unit_key, "")
        extra.append(f"{label}: {vv} {u}".strip())

    add_if("o2_warn", "O2 WARNING <=", "o2")
    add_if("o2_stop", "O2 STOP <=", "o2")
    add_if("no2_warn", "NO2 WARNING >=", "no2")
    add_if("no2_stop", "NO2 STOP >=", "no2")
    add_if("nh3_warn", "NH3 WARNING >=", "nh3")
    add_if("nh3_stop", "NH3 STOP >=", "nh3")
    add_if("tan_warn", "TAN target high >=", "tan")
    add_if("tan_stop", "TAN alarm >=", "tan")

    if extra:
        esc = (esc or [])
        esc = esc + ["— Farm thresholds —"] + extra

    proto2 = dict(proto)
    proto2["escalation_thresholds"] = esc
    return proto2


# -----------------------------
# UI
# -----------------------------
def render_diagnostics():
    _ensure_state()

    farms = _list_farms()
    if not farms:
        st.warning(t("diagnostics.msg.no_farms", "No farms found. Go to Settings → create farm."))
        return

    active_farm = _get_active_farm()
    active_farm_id = str((active_farm or {}).get("farm_id") or "")

    farm_options = [str(f.get("farm_id")) for f in farms]
    farm_labels = {str(f.get("farm_id")): str(f.get("farm_name") or f"Farm {i+1}") for i, f in enumerate(farms)}

    default_farm_index = 0
    if active_farm_id and active_farm_id in farm_options:
        default_farm_index = farm_options.index(active_farm_id)

    reset_nonce = int(st.session_state.get("diag_reset_nonce", 0))

    st.subheader(t("diagnostics.title", "Guided Diagnostics"))

    top = st.columns([1.2, 1.2, 1.0, 1.0])
    with top[0]:
        selected_farm_id = st.selectbox(
            t("label.farm", "Farm"),
            farm_options,
            index=default_farm_index,
            format_func=lambda fid: farm_labels.get(str(fid), str(fid)),
            key="diag_farm_id",
        )

    farm = _get_farm_by_id(selected_farm_id) or active_farm
    if not farm:
        st.warning(t("diagnostics.msg.selected_farm_not_found", "Selected farm not found."))
        return

    farm_id = farm.get("farm_id")
    farm_name = farm.get("farm_name") or "Farm"
    unit_names = _farm_unit_names(farm)
    mu = _farm_measurement_units(farm)
    thresholds = _farm_thresholds(farm)

    with top[1]:
        unit_name = st.selectbox(t("today.field.unit", "Unit"), unit_names, index=0, key=f"diag_unit_name_{farm_id}_{reset_nonce}")
    with top[2]:
        operator = st.text_input(t("label.operator", "Operator"), value="", key=f"diag_operator_{reset_nonce}")
    with top[3]:
        mortality = st.selectbox(
            t("label.mortality", "Mortality"),
            [
                t("diagnostics.mortality.none", "No"),
                t("diagnostics.mortality.few", "Yes, a few fish"),
                t("diagnostics.mortality.tens", "Yes, tens of fish"),
                t("diagnostics.mortality.massive", "Yes, massive mortality"),
            ],
            index=0,
            key=f"diag_mortality_{reset_nonce}",
        )

    species_options = KB_SPECIES_LIST + [CUSTOM_SPECIES_OPTION]

    c1, c2 = st.columns([2, 1])
    with c1:
        species_selected = st.selectbox(t("label.species", "Species"), species_options, index=0, key=f"diag_species_kb_{reset_nonce}")
        custom_species = ""
        if species_selected == CUSTOM_SPECIES_OPTION:
            custom_species = st.text_input(
                t("settings.field.custom_species", "Custom species"),
                value="",
                key=f"diag_species_custom_{reset_nonce}",
            )
    with c2:
        duration = st.selectbox(
            t("label.duration", "Duration"),
            [
                t("diagnostics.duration.lt_6h", "< 6 hours"),
                t("diagnostics.duration.6_24h", "6–24 hours"),
                t("diagnostics.duration.1_3d", "1–3 days"),
                t("diagnostics.duration.gt_3d", "More than 3 days"),
            ],
            index=1,
            key=f"diag_duration_{reset_nonce}",
        )

    species = _resolve_species_value(species_selected, custom_species)

    system_type = "RAS"
    symptoms = st.text_area(t("label.symptoms", "Symptoms"), value="", height=110, key=f"diag_symptoms_{reset_nonce}")

    st.markdown(f"### {t('diagnostics.measurements.title', 'Measurements')}")
    a, b, c = st.columns(3)

    with a:
        in_ph = st.number_input(f"pH ({mu['ph']})", key=f"diag_ph_{farm_id}_{reset_nonce}", min_value=0.0, max_value=14.0, value=0.0, step=0.01, format="%.2f")
        in_temp = st.number_input(f"{t('diagnostics.measurement.temp', 'Temp')} ({mu['temp']})", key=f"diag_temp_{farm_id}_{reset_nonce}", min_value=0.0, max_value=40.0, value=0.0, step=0.1, format="%.1f")
        in_sal = st.number_input(f"{t('diagnostics.measurement.salinity', 'Salinity')} ({mu['salinity']})", key=f"diag_sal_{farm_id}_{reset_nonce}", min_value=0.0, max_value=50.0, value=0.0, step=0.1, format="%.1f")

    with b:
        in_no2 = st.number_input(f"NO2 ({mu['no2']})", key=f"diag_no2_{farm_id}_{reset_nonce}", min_value=0.0, max_value=10.0, value=0.0, step=0.01, format="%.3f")
        in_no3 = st.number_input(f"NO3 ({mu['no3']})", key=f"diag_no3_{farm_id}_{reset_nonce}", min_value=0.0, max_value=500.0, value=0.0, step=1.0, format="%.1f")
        in_o2 = st.number_input(f"O2 ({mu['o2']})", key=f"diag_o2_{farm_id}_{reset_nonce}", min_value=0.0, max_value=30.0, value=0.0, step=0.1, format="%.2f")

    with c:
        in_tan = st.number_input(f"TAN ({mu['tan']})", key=f"diag_tan_{farm_id}_{reset_nonce}", min_value=0.0, max_value=50.0, value=0.0, step=0.01, format="%.3f")
        in_nh3 = st.number_input(f"NH3 ({mu['nh3']})", key=f"diag_nh3_{farm_id}_{reset_nonce}", min_value=0.0, max_value=5.0, value=0.0, step=0.001, format="%.4f")
        in_nh4 = st.number_input(f"NH4 ({mu['nh4']})", key=f"diag_nh4_{farm_id}_{reset_nonce}", min_value=0.0, max_value=50.0, value=0.0, step=0.01, format="%.3f")

    measurements_struct = {
        "pH": _nz(in_ph) if in_ph > 0 else None,
        "temp": _nz(in_temp) if in_temp > 0 else None,
        "salinity": _nz(in_sal) if in_sal > 0 else None,
        "NO2": _nz(in_no2) if in_no2 > 0 else None,
        "NO3": _nz(in_no3) if in_no3 > 0 else None,
        "O2": _nz(in_o2) if in_o2 > 0 else None,
        "TAN": _nz(in_tan) if in_tan > 0 else None,
        "NH3": _nz(in_nh3) if in_nh3 > 0 else None,
        "NH4": _nz(in_nh4) if in_nh4 > 0 else None,
    }
    measurements_text = _canonical_measurements_text(measurements_struct, mu)

    b1, b2, b3, b4 = st.columns([1, 1, 1, 1])
    analyze_clicked = b1.button(t("diagnostics.button.analyze", "Analyze"), key="diag_analyze")
    analyze_status = b2.empty()
    convert_clicked = b3.button(t("diagnostics.button.convert_to_incident", "Convert to Incident"), key="diag_convert")
    refresh_clicked = b4.button(t("action.refresh", "Refresh"), key="diag_refresh")

    if refresh_clicked:
        _reset_diag_form(farm_id)
        st.rerun()

    if analyze_clicked:
        if species_selected == CUSTOM_SPECIES_OPTION and not custom_species.strip():
            st.warning(t("diagnostics.msg.fill_custom_species", "Fill custom species."))
        elif not symptoms.strip():
            st.warning(t("diagnostics.msg.fill_symptoms", "Fill symptoms."))
        else:
            with analyze_status:
                with st.spinner(t("diagnostics.msg.analyzing", "Analyzing...")):
                    rule_hints = _build_rule_hints(
                        species=species,
                        symptoms=symptoms.strip(),
                        duration=duration,
                        mortality=mortality,
                        measurements=measurements_struct,
                    )

                    rag_query = (
                        f"RAS diagnostics protocol. Species: {species}. "
                        f"Symptoms: {symptoms.strip()}. Measurements: {measurements_text}. "
                        f"Duration: {duration}. Mortality: {mortality}. "
                        f"Rule hints: {json.dumps(rule_hints, ensure_ascii=False)}. "
                        f"System: RAS."
                    )

                    hits, kb_sources, rag_best = _rag_kb_ras_only(rag_query, k=6)
                    kb_context = _format_kb_context(hits)

                    user_payload = {
                        "farm": farm_name,
                        "unit": unit_name,
                        "operator": operator.strip() or None,
                        "system": system_type,
                        "species": species,
                        "species_selected": species_selected,
                        "species_custom": custom_species.strip() or None,
                        "duration": duration,
                        "mortality": mortality,
                        "symptoms": symptoms.strip(),
                        "measurements": measurements_struct,
                        "measurements_text": measurements_text,
                        "rule_hints": rule_hints,
                    }

                    proto_raw = _call_llm_protocol_json(kb_context=kb_context, user_payload=user_payload)
                    proto = _normalize_protocol(proto_raw)
                    proto = _apply_rule_overrides(proto, rule_hints)
                    proto = _append_farm_threshold_lines(thresholds, mu, proto)

                    fp_payload = {
                        "farm_id": str(farm_id),
                        "farm_name": farm_name,
                        "unit": unit_name,
                        "species": species,
                        "species_selected": species_selected,
                        "species_custom": custom_species.strip() or None,
                        "duration": duration,
                        "mortality": mortality,
                        "symptoms": symptoms.strip(),
                        "measurements": measurements_struct,
                    }
                    fp = _fingerprint(fp_payload)

                    st.session_state["diag_fingerprint"] = fp
                    st.session_state["diag_result"] = {
                        "protocol": proto,
                        "context": {
                            **user_payload,
                            "farm_id": farm_id,
                            "measurement_units": mu,
                            "thresholds": thresholds,
                            "ts_utc": _now_utc_iso(),
                        },
                        "kb": {
                            "domains": [KB_RAS_DOMAIN],
                            "sources": kb_sources,
                            "rag_best_score": rag_best,
                        },
                        "kb_hits": hits,
                    }
                    st.session_state["diag_md"] = _protocol_to_md(proto)

    if st.session_state.get("diag_md"):
        st.markdown(st.session_state["diag_md"])

    if convert_clicked:
        if not st.session_state.get("diag_result"):
            st.warning(t("diagnostics.msg.run_analyze_first", "Run Analyze first."))
        else:
            fp = st.session_state.get("diag_fingerprint")
            if fp and st.session_state.get("diag_last_fp") == fp:
                st.info(t("diagnostics.msg.already_converted", "Already converted."))
            else:
                data = st.session_state["diag_result"]
                ctx = data.get("context") or {}
                proto = data.get("protocol") or {}
                kb = data.get("kb") or {}

                severity = "WATCH"
                if mortality == t("diagnostics.mortality.massive", "Yes, massive mortality"):
                    severity = "STOP"
                elif mortality == t("diagnostics.mortality.tens", "Yes, tens of fish"):
                    severity = "STOP"
                elif mortality.startswith("Yes"):
                    severity = "WARNING"

                title = f"{ctx.get('farm','Farm')} — {ctx.get('unit','Unit')} — {t('diagnostics.incident.title_suffix', 'Diagnostics')}"

                risk_bundle = {
                    "diagnostics": {
                        "protocol": proto,
                        "kb_domains": kb.get("domains") or [KB_RAS_DOMAIN],
                        "kb_sources": kb.get("sources") or [],
                        "rag_best_score": kb.get("rag_best_score"),
                        "ts_utc": ctx.get("ts_utc"),
                    },
                    "tasks": _protocol_to_tasks(proto),
                    "timeline": [
                        _timeline_first_entry(
                            farm_name=str(ctx.get("farm") or "Farm"),
                            unit_name=str(ctx.get("unit") or "Unit"),
                            operator=str(ctx.get("operator") or ""),
                            species=str(ctx.get("species") or ""),
                        )
                    ],
                }

                rec = storage.create_incident(
                    title=title,
                    severity=severity,
                    status="MITIGATING",
                    triggered_by=[],
                    inputs={
                        "farm_id": ctx.get("farm_id"),
                        "farm_name": ctx.get("farm"),
                        "unit": ctx.get("unit"),
                        "operator": ctx.get("operator"),
                        "system_type": ctx.get("system"),
                        "species": ctx.get("species"),
                        "species_selected": ctx.get("species_selected"),
                        "species_custom": ctx.get("species_custom"),
                        "duration": ctx.get("duration"),
                        "mortality": ctx.get("mortality"),
                        "symptoms": ctx.get("symptoms"),
                        "measurements_struct": ctx.get("measurements"),
                        "measurements_text": ctx.get("measurements_text"),
                        "measurement_units": ctx.get("measurement_units"),
                    },
                    risk_bundle=risk_bundle,
                )

                inc_id = rec.get("incident_id")
                st.session_state["diag_last_incident_id"] = inc_id
                st.session_state["diag_last_fp"] = fp

                st.session_state["selected_incident_id"] = inc_id
                st.session_state["auto_expand_inputs"] = True

                st.success(f"{t('diagnostics.msg.incident_created', 'Incident created')}: {inc_id}")
                st.rerun()