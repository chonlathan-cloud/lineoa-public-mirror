

# core/utils.py
from __future__ import annotations
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def to_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def normalize_phone_th(s: str) -> Optional[str]:
    """Normalize Thai phone number. Return '0XXXXXXXXX' or None."""
    if not s:
        return None
    import re
    digits = re.sub(r"\D+", "", s)
    if digits.startswith("66") and len(digits) == 11:
        return "0" + digits[2:]
    if digits.startswith("0") and len(digits) in (9, 10):
        return digits
    return None

def previous_period(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    """Return the immediately preceding period with the same duration."""
    dur = end - start
    prev_end = start - timedelta(seconds=1)
    prev_start = prev_end - dur
    return to_utc(prev_start), to_utc(prev_end)

def log_ctx(**kwargs) -> str:
    parts = []
    for k, v in kwargs.items():
        if v is not None and v != "":
            parts.append(f"{k}={v}")
    return " ".join(parts)