import streamlit as st

import aquantis_storage
from modules.guidelines import list_species, get_guidelines, clamp_thresholds_to_guidelines
from modules.risk_engine import build_risk_bundle

def render_new_incident():
    st.subheader("➕ New incident (structured inputs)")

    left, right = st.columns([1.2, 1])

    with left:
        species = st.selectbox("Species guideline profile", options=list_species(), index=0)
        title = st.text_input("Title", value="")

        st.markdown("### Structured inputs (enter what you have)")
        g1, g2, g3 = st.columns(3)

        with g1:
            ph = st.number_input("pH", min_value=0.0, max_value=14.0, value=0.0, step=0.1, format="%.2f", key="ni_ph")
            temp_c = st.number_input("Temp (°C)", min_value=0.0, max_value=40.0, value=0.0, step=0.5, format="%.1f", key="ni_temp")
            o2 = st.number_input("O2 (mg/L)", min_value=0.0, max_value=30.0, value=0.0, step=0.1, format="%.2f", key="ni_o2")
        with g2:
            no2 = st.number_input("NO2 (mg/L)", min_value=0.0, max_value=10.0, value=0.0, step=0.01, format="%.3f", key="ni_no2")
            no3 = st.number_input("NO3 (mg/L)", min_value=0.0, max_value=500.0, value=0.0, step=1.0, format="%.0f", key="ni_no3")
            sal = st.number_input("Salinity (ppt)", min_value=0.0, max_value=40.0, value=0.0, step=0.1, format="%.2f", key="ni_sal")
        with g3:
            tan = st.number_input("TAN (mg/L)", min_value=0.0, max_value=20.0, value=0.0, step=0.01, format="%.3f", key="ni_tan")
            nh3 = st.number_input("NH3 explicit (mg/L)", min_value=0.0, max_value=5.0, value=0.0, step=0.001, format="%.4f", key="ni_nh3")
            nh4 = st.number_input("NH4 explicit (mg/L)", min_value=0.0, max_value=50.0, value=0.0, step=0.01, format="%.3f", key="ni_nh4")

        st.caption("Tip: Pokud nemáš TAN, ale máš NH3 a/nebo NH4, Aquantis dopočítá TAN ≈ NH3 + NH4.")

        with st.expander("Farm-specific thresholds (optional)", expanded=False):
            st.caption("Pokud nevyplníš, použijí se guideline guardrails. Farm thresholds se automaticky omezí na bezpečné guardrails.")
            ft = {}
            cols = st.columns(4)
            ft["o2_warn"] = cols[0].number_input("o2_warn", value=0.0, step=0.1, key="ft_o2w")
            ft["o2_stop"] = cols[1].number_input("o2_stop", value=0.0, step=0.1, key="ft_o2s")
            ft["no2_warn"] = cols[2].number_input("no2_warn", value=0.0, step=0.01, key="ft_no2w")
            ft["no2_stop"] = cols[3].number_input("no2_stop", value=0.0, step=0.01, key="ft_no2s")

    guideline = get_guidelines(species)

    def nz(x):
        return None if (x is None or float(x) == 0.0) else float(x)

    farm_thresholds = {k: v for k, v in (locals().get("ft", {}) or {}).items() if v and float(v) != 0.0}
    thresholds_applied = clamp_thresholds_to_guidelines(farm_thresholds, guideline)

    bundle = build_risk_bundle(
        ph=nz(ph),
        temp_c=nz(temp_c),
        o2=nz(o2),
        no2=nz(no2),
        no3=nz(no3),
        tan=nz(tan),
        nh3_explicit=nz(nh3),
        nh4_explicit=nz(nh4),
        salinity_ppt=nz(sal),
        thresholds=thresholds_applied,
    )

    with right:
        st.markdown("### UX Risk panel")
        rr = bundle["risk_result"]
        st.metric("Risk status", rr.get("status", "—"))
        tb = rr.get("triggered_by", [])
        if tb:
            st.write("**Triggered by:**")
            for t in tb:
                st.write(f"- {t}")
        else:
            st.write("Triggered by: —")

        missing = bundle.get("missing", [])
        conf = float(bundle.get("confidence", 0.0))
        st.progress(min(1.0, max(0.0, conf)))
        st.caption(f"Confidence: {conf:.2f} | Missing: {', '.join(missing) if missing else '—'}")

        with st.expander("Guideline vs Farm vs Applied", expanded=False):
            st.write("**Guideline**", guideline)
            st.write("**Farm (provided)**", farm_thresholds if farm_thresholds else "—")
            st.write("**Applied (clamped)**", thresholds_applied)

        with st.expander("Risk engine snapshot", expanded=False):
            st.json({"parsed": bundle["parsed"], "risk_result": rr, "missing": missing, "confidence": conf})

    st.divider()
    if st.button("Create incident", type="primary"):
        rec = aquantis_storage.create_incident(
            title=title.strip(),
            severity=rr.get("status", "OK"),
            status="OPEN",
            triggered_by=rr.get("triggered_by", []),
            inputs={
                "species_guideline": species,
                "structured_inputs": {
                    "ph": nz(ph), "temp_c": nz(temp_c), "o2": nz(o2),
                    "no2": nz(no2), "no3": nz(no3), "salinity_ppt": nz(sal),
                    "tan": nz(tan), "nh3_explicit": nz(nh3), "nh4_explicit": nz(nh4),
                },
            },
            risk_bundle={
                "parsed": bundle["parsed"],
                "risk_result": rr,
                "missing": bundle["missing"],
                "confidence": bundle["confidence"],
                "thresholds_applied": thresholds_applied,
                "guideline": guideline,
                "farm_thresholds": farm_thresholds,
            }
        )
        st.success(f"Created incident: {rec.get('incident_id')}")
        st.session_state["selected_incident_id"] = rec.get("incident_id")
        st.session_state["auto_expand_inputs"] = True
