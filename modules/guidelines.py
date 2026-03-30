from __future__ import annotations

SPECIES_GUIDELINES = {
    "Generic RAS (freshwater)": {
        "o2_warn": 6.0, "o2_stop": 5.0,
        "no2_warn": 0.5, "no2_stop": 0.8,
        "nh3_warn": 0.02, "nh3_stop": 0.05,
        "nh4_warn": 1.0, "nh4_stop": 2.0,
        "no3_warn": 80.0, "no3_stop": 150.0,
        "ph_low_warn": 6.5, "ph_low_stop": 6.0,
        "ph_high_warn": 8.2, "ph_high_stop": 8.6,
        "temp_low_warn": 16.0, "temp_low_stop": 14.0,
        "temp_high_warn": 24.0, "temp_high_stop": 26.0,
        "sal_low_warn": 0.0, "sal_low_stop": 0.0,
        "sal_high_warn": 3.0, "sal_high_stop": 5.0,
    },
}

def list_species() -> list[str]:
    return sorted(SPECIES_GUIDELINES.keys())

def get_guidelines(species_label: str) -> dict:
    return dict(SPECIES_GUIDELINES.get(species_label) or SPECIES_GUIDELINES["Generic RAS (freshwater)"])

def clamp_thresholds_to_guidelines(farm_thresholds: dict | None, guideline: dict) -> dict:
    ft = dict(farm_thresholds or {})
    out = dict(guideline)

    higher_is_worse_prefixes = ("no2_", "nh3_", "nh4_", "no3_", "ph_high_", "temp_high_", "sal_high_")
    lower_is_worse_prefixes = ("o2_", "ph_low_", "temp_low_", "sal_low_")

    for k, gv in guideline.items():
        fv = ft.get(k, None)
        if fv is None:
            out[k] = gv
            continue
        try:
            fv = float(fv)
        except Exception:
            out[k] = gv
            continue

        if any(k.startswith(p) for p in higher_is_worse_prefixes):
            out[k] = min(fv, float(gv))
        elif any(k.startswith(p) for p in lower_is_worse_prefixes):
            out[k] = max(fv, float(gv))
        else:
            out[k] = fv

    return out
