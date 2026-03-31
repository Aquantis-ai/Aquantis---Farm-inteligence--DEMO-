from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import streamlit as st

from modules.ui_text import t
from modules.workspace import get_workspace_id


DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _get_farm_profiles_path() -> Path:
    workspace_id = get_workspace_id()
    return DATA_DIR / f"farm_profiles_{workspace_id}.json"


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


UNIT_PARAM_FIELDS = [
    ("ph", "pH"),
    ("temp", "Teplota"),
    ("salinity", "Salinita"),
    ("no2", "NO2"),
    ("no3", "NO3"),
    ("nh3", "NH3"),
    ("nh4", "NH4"),
    ("tan", "TAN"),
]

TANK_O2_FIELD = ("o2", "O2")


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


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:8]}"


def _load_db() -> dict:
    farm_profiles_path = _get_farm_profiles_path()
    if not farm_profiles_path.exists():
        return {"active_farm_id": None, "farms": []}
    try:
        return json.loads(farm_profiles_path.read_text(encoding="utf-8"))
    except Exception:
        return {"active_farm_id": None, "farms": []}


def _save_db(db: dict):
    farm_profiles_path = _get_farm_profiles_path()
    farm_profiles_path.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_active_farm(db: dict) -> Optional[dict]:
    farms = db.get("farms") or []
    if not farms:
        return None
    active_id = db.get("active_farm_id")
    if active_id:
        for f in farms:
            if str(f.get("farm_id")) == str(active_id):
                return f
    return farms[0]


def _set_active(db: dict, farm_id: str):
    db["active_farm_id"] = farm_id
    _save_db(db)


def _find_farm(db: dict, farm_id: str) -> Optional[dict]:
    for f in (db.get("farms") or []):
        if str(f.get("farm_id")) == str(farm_id):
            return f
    return None


def _safe_float(x: Any) -> Optional[float]:
    try:
        s = str(x).strip()
        if s == "":
            return None
        return float(s.replace(",", "."))
    except Exception:
        return None


def _safe_dict(x: Any) -> dict:
    return x if isinstance(x, dict) else {}


def _coerce_species_defaults(values: Any) -> List[str]:
    vals = values if isinstance(values, list) else []
    return [v for v in vals if v in KB_SPECIES_LIST]


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


def _pack_species_selection(selected_values: List[str], custom_value: str) -> dict:
    selected_clean = [v for v in (selected_values or []) if v in KB_SPECIES_LIST]
    custom_clean = str(custom_value or "").strip()

    if custom_clean and CUSTOM_SPECIES_OPTION not in selected_clean:
        selected_clean.append(CUSTOM_SPECIES_OPTION)

    if not custom_clean:
        selected_clean = [v for v in selected_clean if v != CUSTOM_SPECIES_OPTION]

    return {
        "selected": selected_clean,
        "custom": custom_clean or None,
    }


def _ensure_param_tables(unit: dict):
    unit.setdefault("param_active", {})
    unit.setdefault("mins", {})
    unit.setdefault("targets", {})
    unit.setdefault("maxs", {})

    for k, _ in UNIT_PARAM_FIELDS:
        unit["param_active"].setdefault(k, False)
        unit["mins"].setdefault(k, None)
        unit["targets"].setdefault(k, None)
        unit["maxs"].setdefault(k, None)


def _ensure_tank_o2_tables(tank: dict):
    tank.setdefault("o2_active", False)
    tank.setdefault("o2_min", None)
    tank.setdefault("o2_target", None)
    tank.setdefault("o2_max", None)


def render_settings():
    st.subheader(t("settings.title", "Settings"))

    db = _load_db()
    farms: List[dict] = db.get("farms") or []
    active = _get_active_farm(db)

    # -------------------------
    # Farm selector + create
    # -------------------------
    top = st.columns([2.2, 1, 1])
    with top[0]:
        options = [f"{f.get('farm_name','Farm')}  ({f.get('farm_id')})" for f in farms]
        idx = 0
        if active and farms:
            for i, f in enumerate(farms):
                if f.get("farm_id") == active.get("farm_id"):
                    idx = i
                    break
        selected_label = st.selectbox(
            t("settings.active_farm", "Active farm"),
            options=options if options else ["—"],
            index=idx if options else 0,
            key="settings_active_farm_select",
        )

    selected_farm = None
    if farms and options:
        selected_farm = farms[options.index(selected_label)]

    with top[1]:
        if st.button(
            t("settings.button.set_active", "Set active"),
            key="settings_btn_set_active",
            disabled=not bool(selected_farm),
        ):
            _set_active(db, selected_farm["farm_id"])
            st.rerun()

    with top[2]:
        if st.button(t("settings.button.new_farm", "New farm"), key="settings_btn_new_farm"):
            st.session_state["settings_mode"] = "NEW_FARM"
            st.rerun()

    st.divider()

    # -------------------------
    # New farm form
    # -------------------------
    mode = st.session_state.get("settings_mode", "EDIT")
    if mode == "NEW_FARM":
        st.markdown(f"### {t('settings.create_farm.title', 'Create farm')}")

        farm_name = st.text_input(
            t("settings.field.farm_name", "Farm name"),
            value="",
            key="new_farm_name",
        )
        st.text_input(
            t("settings.field.operation", "Operation"),
            value="RAS",
            disabled=True,
            key="new_farm_operation_fixed",
        )

        species_options = KB_SPECIES_LIST + [CUSTOM_SPECIES_OPTION]
        species_pack_selected = st.multiselect(
            t("settings.field.species_pack", "Species pack"),
            options=species_options,
            default=[],
            key="new_farm_species_pack",
        )

        custom_species = ""
        if CUSTOM_SPECIES_OPTION in species_pack_selected:
            custom_species = st.text_input(
                t("settings.field.custom_species", "Custom species"),
                value="",
                key="new_farm_species_custom",
            )

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button(t("settings.button.create", "Create"), key="new_farm_create"):
                if not farm_name.strip():
                    st.warning(t("settings.msg.fill_farm_name", "Fill Farm name."))
                else:
                    new_f = {
                        "farm_id": _new_id("FARM"),
                        "farm_name": farm_name.strip(),
                        "operation": "RAS",
                        "species_pack": _pack_species_selection(species_pack_selected, custom_species),
                        "units": [],
                        "measurement_units": dict(DEFAULT_UNITS),
                    }
                    db["farms"] = (db.get("farms") or []) + [new_f]
                    db["active_farm_id"] = new_f["farm_id"]
                    _save_db(db)
                    st.session_state["settings_mode"] = "EDIT"
                    st.session_state["settings_open_farm_id"] = new_f["farm_id"]
                    st.rerun()
        with c2:
            if st.button(t("action.cancel", "Cancel"), key="new_farm_cancel"):
                st.session_state["settings_mode"] = "EDIT"
                st.rerun()

        return

    # -------------------------
    # No farms yet
    # -------------------------
    if not farms:
        st.info(t("settings.msg.no_farms", "No farms yet. Click **New farm**."))
        return

    # -------------------------
    # Farm detail
    # -------------------------
    farm_id = st.session_state.get("settings_open_farm_id") or (
        active.get("farm_id") if active else farms[0].get("farm_id")
    )
    farm = _find_farm(db, farm_id) or active or farms[0]

    st.markdown(f"### {t('settings.farm_detail.title', 'Farm detail')}")

    f1, f2 = st.columns([2, 1])
    with f1:
        farm_name = st.text_input(
            t("settings.field.farm_name", "Farm name"),
            value=str(farm.get("farm_name") or ""),
            key=f"farm_name::{farm.get('farm_id')}",
        )
    with f2:
        st.text_input(
            t("settings.field.operation", "Operation"),
            value="RAS",
            disabled=True,
            key=f"farm_operation_fixed::{farm.get('farm_id')}",
        )

    save_row = st.columns([1, 1, 2])
    with save_row[0]:
        if st.button(t("settings.button.save_farm", "Save farm"), key=f"farm_save::{farm.get('farm_id')}"):
            farm["farm_name"] = farm_name.strip() or farm.get("farm_name") or "Farm"
            farm["operation"] = "RAS"
            farm.setdefault("units", [])
            farm.setdefault("measurement_units", dict(DEFAULT_UNITS))
            _save_db(db)
            st.success(t("settings.msg.farm_saved", "Farm saved."))
            st.rerun()

    with save_row[1]:
        if st.button(t("settings.button.delete_farm", "Delete farm"), key=f"farm_delete::{farm.get('farm_id')}"):
            db["farms"] = [f for f in (db.get("farms") or []) if f.get("farm_id") != farm.get("farm_id")]
            if db.get("active_farm_id") == farm.get("farm_id"):
                db["active_farm_id"] = (db["farms"][0]["farm_id"] if db["farms"] else None)
            _save_db(db)
            st.session_state.pop("settings_open_farm_id", None)
            st.success(t("settings.msg.deleted", "Deleted."))
            st.rerun()

    st.divider()

    # -------------------------
    # Units + Tanks
    # -------------------------
    units = farm.get("units")
    if not isinstance(units, list):
        units = []
        farm["units"] = units

    if st.button(t("settings.button.new_unit", "New unit"), key=f"unit_new::{farm.get('farm_id')}"):
        units.append({
            "unit_id": _new_id("UNIT"),
            "unit_name": "",
            "species_pack": {"selected": [], "custom": None},
            "param_active": {},
            "mins": {},
            "targets": {},
            "maxs": {},
            "tanks": [],
        })
        _save_db(db)
        st.rerun()

    if not units:
        st.info(t("settings.msg.no_units", "No units yet. Click **New unit**."))
        _save_db(db)
        return

    st.markdown(f"### {t('settings.units.title', 'Units')}")

    for ui, unit in enumerate(units):
        u = unit if isinstance(unit, dict) else {}
        unit_id = str(u.get("unit_id") or f"UNIT-{ui}")
        _ensure_param_tables(u)

        with st.expander(
            f"{(u.get('unit_name') or t('settings.unit.fallback_name', 'Unit')).strip() or t('settings.unit.fallback_name', 'Unit')}",
            expanded=False,
        ):
            urow = st.columns([2.2, 1])
            with urow[0]:
                u_name = st.text_input(
                    t("settings.field.unit_name", "Unit name"),
                    value=str(u.get("unit_name") or ""),
                    key=f"unit_name::{farm.get('farm_id')}::{unit_id}",
                )
            with urow[1]:
                if st.button(
                    t("settings.button.delete_unit", "Delete unit"),
                    key=f"unit_del::{farm.get('farm_id')}::{unit_id}",
                ):
                    farm["units"] = [x for x in farm["units"] if str(_safe_dict(x).get("unit_id")) != unit_id]
                    _save_db(db)
                    st.rerun()

            u["unit_name"] = u_name.strip()

            # Species per unit
            species_options = KB_SPECIES_LIST + [CUSTOM_SPECIES_OPTION]
            unit_species_selected_default, unit_species_custom_default = _extract_species_selection(u.get("species_pack"))

            unit_species_selected = st.multiselect(
                t("settings.field.species_pack", "Species pack"),
                options=species_options,
                default=unit_species_selected_default,
                key=f"unit_species::{farm.get('farm_id')}::{unit_id}",
            )

            unit_species_custom = unit_species_custom_default
            if CUSTOM_SPECIES_OPTION in unit_species_selected:
                unit_species_custom = st.text_input(
                    t("settings.field.custom_species", "Custom species"),
                    value=unit_species_custom_default,
                    key=f"unit_species_custom::{farm.get('farm_id')}::{unit_id}",
                )

            u["species_pack"] = _pack_species_selection(unit_species_selected, unit_species_custom)

            st.markdown(f"**{t('settings.water_parameters.title', 'Water parameters (per unit)')}**")
            st.caption(t("settings.water_parameters.caption", "Only active parameters will be used in Daily report."))

            mu = farm.get("measurement_units") if isinstance(farm.get("measurement_units"), dict) else dict(DEFAULT_UNITS)

            header = st.columns([1.6, 1.0, 1.0, 1.0, 1.0])
            header[0].markdown(f"**{t('settings.table.parameter', 'Parameter')}**")
            header[1].markdown(f"**{t('settings.table.set_active', 'Set active')}**")
            header[2].markdown(f"**{t('settings.table.min', 'Min')}**")
            header[3].markdown(f"**{t('settings.table.target', 'Target')}**")
            header[4].markdown(f"**{t('settings.table.max', 'Max')}**")

            for k, label in UNIT_PARAM_FIELDS:
                row = st.columns([1.6, 1.0, 1.0, 1.0, 1.0])

                row[0].markdown(f"**{label}**  \n{mu.get(k, '')}")
                u["param_active"][k] = row[1].checkbox(
                    "",
                    value=bool(u["param_active"].get(k, False)),
                    key=f"u_active::{farm.get('farm_id')}::{unit_id}::{k}",
                )

                min_default = 0.0 if u["mins"].get(k) is None else float(u["mins"].get(k))
                tgt_default = 0.0 if u["targets"].get(k) is None else float(u["targets"].get(k))
                max_default = 0.0 if u["maxs"].get(k) is None else float(u["maxs"].get(k))

                row[2].number_input(
                    "",
                    min_value=0.0,
                    value=min_default,
                    step=0.01,
                    format="%.3f",
                    key=f"u_min::{farm.get('farm_id')}::{unit_id}::{k}",
                )
                row[3].number_input(
                    "",
                    min_value=0.0,
                    value=tgt_default,
                    step=0.01,
                    format="%.3f",
                    key=f"u_tgt::{farm.get('farm_id')}::{unit_id}::{k}",
                )
                row[4].number_input(
                    "",
                    min_value=0.0,
                    value=max_default,
                    step=0.01,
                    format="%.3f",
                    key=f"u_max::{farm.get('farm_id')}::{unit_id}::{k}",
                )

                u["mins"][k] = float(st.session_state[f"u_min::{farm.get('farm_id')}::{unit_id}::{k}"]) if u["param_active"][k] else None
                u["targets"][k] = float(st.session_state[f"u_tgt::{farm.get('farm_id')}::{unit_id}::{k}"]) if u["param_active"][k] else None
                u["maxs"][k] = float(st.session_state[f"u_max::{farm.get('farm_id')}::{unit_id}::{k}"]) if u["param_active"][k] else None

            st.divider()

            tanks = u.get("tanks")
            if not isinstance(tanks, list):
                tanks = []
                u["tanks"] = tanks

            trow = st.columns([1, 3])
            with trow[0]:
                if st.button(
                    t("settings.button.new_tank", "New tank"),
                    key=f"tank_new::{farm.get('farm_id')}::{unit_id}",
                ):
                    tanks.append({
                        "tank_id": _new_id("TNK"),
                        "tank_name": "",
                        "biomass_kg": None,
                        "avg_weight_g": None,
                        "feed_kg_day": None,
                        "o2_active": False,
                        "o2_min": None,
                        "o2_target": None,
                        "o2_max": None,
                    })
                    _save_db(db)
                    st.rerun()

            if not tanks:
                st.info(t("settings.msg.no_tanks", "No tanks yet. Click **New tank**."))
            else:
                st.markdown(f"**{t('settings.tanks.title', 'Tanks')}**")

                for ti, tank in enumerate(tanks):
                    tnk = tank if isinstance(tank, dict) else {}
                    tank_id = str(tnk.get("tank_id") or f"TNK-{ti}")
                    _ensure_tank_o2_tables(tnk)

                    with st.expander(str(tnk.get("tank_name") or f"{t('settings.tank.fallback_name', 'Tank')} {ti+1}"), expanded=False):
                        tr1 = st.columns([2.0, 1.0])
                        with tr1[0]:
                            name = st.text_input(
                                t("settings.field.tank_name", "Tank name"),
                                value=str(tnk.get("tank_name") or ""),
                                key=f"tank_name::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                            )
                        with tr1[1]:
                            if st.button(
                                t("settings.button.delete_tank", "Delete tank"),
                                key=f"tank_del::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                            ):
                                u["tanks"] = [x for x in u["tanks"] if str(_safe_dict(x).get("tank_id")) != tank_id]
                                _save_db(db)
                                st.rerun()

                        tr2 = st.columns(3)
                        biomass = tr2[0].number_input(
                            t("settings.field.biomass_kg", "Biomass (kg)"),
                            min_value=0.0,
                            value=float(tnk.get("biomass_kg") or 0.0),
                            step=1.0,
                            key=f"tank_biomass::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                        )
                        avg_w = tr2[1].number_input(
                            t("settings.field.avg_weight_g", "Avg weight (g)"),
                            min_value=0.0,
                            value=float(tnk.get("avg_weight_g") or 0.0),
                            step=1.0,
                            key=f"tank_avgw::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                        )
                        feed_kg_day = tr2[2].number_input(
                            t("settings.field.feed_kg_day", "Feed kg per day"),
                            min_value=0.0,
                            value=float(tnk.get("feed_kg_day") or 0.0),
                            step=0.1,
                            key=f"tank_feedkg::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                        )

                        st.markdown(f"**{t('settings.o2_per_tank.title', 'O2 per tank')}**")
                        o2_header = st.columns([1.6, 1.0, 1.0, 1.0, 1.0])
                        o2_header[0].markdown(f"**{TANK_O2_FIELD[1]}**  \n{mu.get('o2', 'mg/L')}")
                        o2_header[1].markdown(f"**{t('settings.table.set_active', 'Set active')}**")
                        o2_header[2].markdown(f"**{t('settings.table.min', 'Min')}**")
                        o2_header[3].markdown(f"**{t('settings.table.target', 'Target')}**")
                        o2_header[4].markdown(f"**{t('settings.table.max', 'Max')}**")

                        o2row = st.columns([1.6, 1.0, 1.0, 1.0, 1.0])
                        o2row[0].write("")

                        tnk["o2_active"] = o2row[1].checkbox(
                            "",
                            value=bool(tnk.get("o2_active", False)),
                            key=f"tank_o2_active::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                        )

                        o2_min_default = 0.0 if tnk.get("o2_min") is None else float(tnk.get("o2_min"))
                        o2_tgt_default = 0.0 if tnk.get("o2_target") is None else float(tnk.get("o2_target"))
                        o2_max_default = 0.0 if tnk.get("o2_max") is None else float(tnk.get("o2_max"))

                        o2row[2].number_input(
                            "",
                            min_value=0.0,
                            value=o2_min_default,
                            step=0.1,
                            format="%.2f",
                            key=f"tank_o2_min::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                        )
                        o2row[3].number_input(
                            "",
                            min_value=0.0,
                            value=o2_tgt_default,
                            step=0.1,
                            format="%.2f",
                            key=f"tank_o2_tgt::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                        )
                        o2row[4].number_input(
                            "",
                            min_value=0.0,
                            value=o2_max_default,
                            step=0.1,
                            format="%.2f",
                            key=f"tank_o2_max::{farm.get('farm_id')}::{unit_id}::{tank_id}",
                        )

                        tnk["tank_name"] = name.strip()
                        tnk["biomass_kg"] = float(biomass) if biomass > 0 else None
                        tnk["avg_weight_g"] = float(avg_w) if avg_w > 0 else None
                        tnk["feed_kg_day"] = float(feed_kg_day) if feed_kg_day > 0 else None
                        tnk["o2_min"] = float(st.session_state[f"tank_o2_min::{farm.get('farm_id')}::{unit_id}::{tank_id}"]) if tnk["o2_active"] else None
                        tnk["o2_target"] = float(st.session_state[f"tank_o2_tgt::{farm.get('farm_id')}::{unit_id}::{tank_id}"]) if tnk["o2_active"] else None
                        tnk["o2_max"] = float(st.session_state[f"tank_o2_max::{farm.get('farm_id')}::{unit_id}::{tank_id}"]) if tnk["o2_active"] else None

            if st.button(t("settings.button.save_unit", "Save unit"), key=f"unit_save::{farm.get('farm_id')}::{unit_id}"):
                farm["operation"] = "RAS"
                farm["farm_name"] = (farm_name.strip() or farm.get("farm_name") or "Farm")
                _save_db(db)
                st.success(t("settings.msg.unit_saved", "Unit saved."))
                st.rerun()

    _save_db(db)


# -----------------------------
# Public helpers for other modules
# -----------------------------
def load_farm_profiles() -> dict:
    return _load_db()


def save_farm_profiles(db: dict):
    _save_db(db)


def get_active_farm(db: Optional[dict] = None) -> Optional[dict]:
    if db is None:
        db = _load_db()
    return _get_active_farm(db)