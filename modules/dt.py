from __future__ import annotations

from datetime import datetime, timezone
import pandas as pd

def to_utc_datetime(series_or_value):
    """Convert to timezone-aware UTC pandas datetime."""
    if isinstance(series_or_value, pd.Series):
        return pd.to_datetime(series_or_value, errors="coerce", utc=True)
    return pd.to_datetime(series_or_value, errors="coerce", utc=True)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_days_ago(days: int) -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
