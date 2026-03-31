from __future__ import annotations

import re
from typing import Optional

import streamlit as st


_DEFAULT_WORKSPACE = "default"
_ALLOWED_PATTERN = re.compile(r"[^a-z0-9_-]")


def _normalize_workspace_id(value: Optional[str]) -> str:
    """
    Normalize workspace ID so it is safe for file names and URLs.

    Rules:
    - lowercase
    - trim whitespace
    - only keep a-z, 0-9, underscore, hyphen
    - collapse invalid characters to hyphen
    - fallback to "default" if empty
    """
    raw = str(value or "").strip().lower()
    if not raw:
        return _DEFAULT_WORKSPACE

    cleaned = _ALLOWED_PATTERN.sub("-", raw)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-_")

    return cleaned or _DEFAULT_WORKSPACE


def get_workspace_id() -> str:
    """
    Read workspace ID from Streamlit query params.

    Supported URL format:
    ?ws=farm-a

    Returns normalized workspace ID.
    Falls back to "default" if missing.
    """
    try:
        params = st.query_params
        value = params.get("ws", _DEFAULT_WORKSPACE)
    except Exception:
        return _DEFAULT_WORKSPACE

    if isinstance(value, list):
        value = value[0] if value else _DEFAULT_WORKSPACE

    return _normalize_workspace_id(value)


def get_workspace_label() -> str:
    """
    User-friendly label for display/debug purposes.
    """
    return get_workspace_id()