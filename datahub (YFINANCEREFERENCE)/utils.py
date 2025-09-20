# utils.py
import pandas as pd
from typing import Union, Optional

DateLike = Union[str, pd.Timestamp, None]

def to_datestr(v: DateLike) -> Optional[str]:
    """
    Convert a date-like input into a YYYY-MM-DD string or None.
    Accepted: str (unchanged), pd.Timestamp, None.
    """
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, pd.Timestamp):
        return v.date().isoformat()
    raise TypeError(f"Unsupported date type: {type(v)}")

def to_timestamp(v: DateLike, tz: str = "UTC") -> Optional[pd.Timestamp]:
    """
    Convert input into a timezone-aware Timestamp.
    - str → parsed
    - pd.Timestamp → localize/convert to tz if needed
    - None → None
    """
    if v is None:
        return None
    if isinstance(v, str):
        return pd.to_datetime(v).tz_localize(tz) if pd.to_datetime(v).tzinfo is None else pd.to_datetime(v).tz_convert(tz)
    if isinstance(v, pd.Timestamp):
        return v.tz_localize(tz) if v.tzinfo is None else v.tz_convert(tz)
    raise TypeError(f"Unsupported date type: {type(v)}")

def date_range_days(start: DateLike, end: DateLike) -> int:
    """
    Compute number of days between start and end.
    """
    ts_start, ts_end = to_timestamp(start), to_timestamp(end)
    if ts_start is None or ts_end is None:
        raise ValueError("Both start and end must be provided")
    return (ts_end - ts_start).days
