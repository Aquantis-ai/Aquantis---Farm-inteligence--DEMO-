import streamlit as st
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

import aquantis_storage as storage


ISSUES = [
    "Water quality spike",
    "Oxygen issue",
    "Feeding / appetite",
    "Behavior / stress",
    "Mortality event",
    "Disease / parasites",
    "Mechanical / system failure",
    "Other",
]

PARAM_FIELDS = [
    ("temp_c", "Temp (°C)"),
    ("o2_mg_l", "O₂ (mg/L)"),
    ("ph", "pH"),
    ("tan_mg_l", "TAN (mg/L)"),
    ("nh3_mg_l", "NH₃ (mg/L)"),
    ("nh4_mg_l", "NH₄ (mg/L)"),
    ("no2_mg_l", "NO₂ (mg/L)"),
    ("no3_mg_l", "NO₃ (mg/L)"),
    ("salinity_ppt", "Salinity (ppt)"),
]


def _clean_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)) and pd.notna(x):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def _compose_title(issue: str, species: str, system: str) -> str:
    parts = [issue]
    if species:
        parts.append(species)
    if system:
        parts.append(system)
    return " — ".join([p for p in parts if p]).strip()


def _try_risk_engine(inputs: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[str], str]:
    """
    Plug-in point for your real risk engine.
    For now: small deterministic heuristic.
    Returns: risk_result, triggered_by, severity_guess
    """
    triggered_by: List[str] = []
    severity_guess = "WATCH"

    no2 = inputs.get("no2_mg_l")
    nh3 = inputs.get("nh3_mg_l")
    o2 = inputs.get("o2_mg_l")

    if isinstance(o2, (int, float)) and o2 > 0 and o2 < 4:
        triggered_by.append("O2_low")
        severity_guess = "WARNING"

    if isinstance(no2, (int, float)) and no2 >= 0.5:
        triggered_by.append("NO2_high")
        severity_guess = "STOP" if no2 >= 1.0 else "WARNING"

    if isinstance(nh3, (int, float)) and nh3 >= 0.02:
        triggered_by.append("NH3_high")
        severity_guess = "STOP" if nh3 >= 0.05 else "WARNING"

    return None, triggered_by, severity_guess


def render_intake():
    st.subheader("➕ Intake")

    # Session defaults
    st.session_state.setdefault("intake_status", "OPEN")
    st.session_state.setdefault("intake_severity", "WATCH")

    left, right = st.columns([1.15, 0.85])

    with left:
        with st.form("intake_form", clear_on_submit=False):

            r1 = st.columns([1, 1])
            issue = r1[0].selectbox("Issue", ISSUES, index=0)
            status = r1[1].selectbox("Status", ["OPEN", "RESOLVED"], index=0, key="intake_status")

            r2 = st.columns([1, 1, 1])
            species = r2[0].text_input("Species", value="")
            system = r2[1].text_input("System / Unit", value="")
            batch = r2[2].text_input("Batch / Tank", value="")

            st.markdown("##### Water")
            w1 = st.columns([1, 1, 1])
            temp_c = w1[0].text_input("Temp (°C)", value="")
            o2_mg_l = w1[1].text_input("O₂ (mg/L)", value="")
            ph = w1[2].text_input("pH", value="")

            w2 = st.columns([1, 1, 1])
            tan_mg_l = w2[0].text_input("TAN (mg/L)", value="")
            nh3_mg_l = w2[1].text_input("NH₃ (mg/L)", value="")
            no2_mg_l = w2[2].text_input("NO₂ (mg/L)", value="")

            w3 = st.columns([1, 1, 1])
            nh4_mg_l = w3[0].text_input("NH₄ (mg/L)", value="")
            no3_mg_l = w3[1].text_input("NO₃ (mg/L)", value="")
            salinity_ppt = w3[2].text_input("Salinity (ppt)", value="")

            st.markdown("##### Notes")
            obs = st.text_area("Notes", value="", height=120)

            r3 = st.columns([1, 1])
            severity = r3[0].selectbox(
                "Severity",
                ["OK", "WATCH", "WARNING", "STOP"],
                index=["OK", "WATCH", "WARNING", "STOP"].index(st.session_state["intake_severity"]),
                key="intake_severity",
            )
            autolog = r3[1].toggle("Run + log", value=True, key="intake_run_log")

            # Two actions in one place
            a1, a2 = st.columns([1, 1])
            create_clicked = a1.form_submit_button("Create incident")
            runlog_clicked = a2.form_submit_button("Run + log")

    # Build inputs
    inputs: Dict[str, Any] = {
        "species": species.strip() or None,
        "system": system.strip() or None,
        "batch": batch.strip() or None,
        "temp_c": _clean_float(temp_c),
        "o2_mg_l": _clean_float(o2_mg_l),
        "ph": _clean_float(ph),
        "tan_mg_l": _clean_float(tan_mg_l),
        "nh3_mg_l": _clean_float(nh3_mg_l),
        "nh4_mg_l": _clean_float(nh4_mg_l),
        "no2_mg_l": _clean_float(no2_mg_l),
        "no3_mg_l": _clean_float(no3_mg_l),
        "salinity_ppt": _clean_float(salinity_ppt),
    }

    title = _compose_title(issue, species.strip(), system.strip())

    # Right panel: only shows when an action happens
    with right:
        st.markdown("### Output")

        if not (create_clicked or runlog_clicked):
            st.empty()
            return

        if create_clicked:
            # No diagnostics; just log
            try:
                rec = storage.create_incident(
                    title=title,
                    severity=severity,
                    status=status,
                    triggered_by=[f"Batch:{batch.strip()}"] if batch.strip() else [],
                    inputs=inputs,
                    risk_bundle=None,
                )
                inc_id = rec.get("incident_id")
                st.session_state["selected_incident_id"] = inc_id
                st.session_state["auto_expand_inputs"] = True
                st.success("Incident created.")
                st.rerun()
            except Exception as e:
                st.error(f"Create failed: {e}")
            return

        # Run + log
        try:
            risk_result, triggered_by, severity_guess = _try_risk_engine(inputs)
            out_sev = severity if severity else severity_guess
            out_triggers = triggered_by[:] if triggered_by else []
            if batch.strip():
                out_triggers.append(f"Batch:{batch.strip()}")

            # Keep output minimal and obvious
            st.write(f"**Title:** {title}")
            st.write(f"**Severity:** {out_sev}")
            st.write(f"**Status:** {status}")
            if out_triggers:
                st.write("**Triggered by:**")
                for t in out_triggers:
                    st.write(f"- {t}")

            diag_note = (obs or "").strip()

            risk_bundle = {
                "risk_result": risk_result or {"status": out_sev, "triggered_by": out_triggers},
                "parsed": {k: v for k, v in inputs.items() if v is not None and k not in ("species", "system", "batch")},
                "missing": [k for k, _ in PARAM_FIELDS if inputs.get(k) is None],
                "confidence": None,
                "thresholds_applied": {},
                "diagnostic_note": diag_note if diag_note else None,
            }

            rec = storage.create_incident(
                title=title,
                severity=out_sev,
                status=status,
                triggered_by=out_triggers,
                inputs=inputs,
                risk_bundle=risk_bundle,
            )

            inc_id = rec.get("incident_id")
            st.session_state["selected_incident_id"] = inc_id
            st.session_state["auto_expand_inputs"] = True
            st.success("Logged.")
            st.rerun()

        except Exception as e:
            st.error(f"Run + log failed: {e}")