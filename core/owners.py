

# core/owners.py
from __future__ import annotations
from typing import Dict, Any, Optional
import re, requests
try:
  from services.firestore_client import get_db
except Exception:
  from firestore_client import get_db

def normalize_th_phone(s: str) -> Optional[str]:
  if not s: return None
  digits = re.sub(r"\D+", "", s)
  if digits.startswith("66") and len(digits) == 11:
    return "0" + digits[2:]
  if digits.startswith("0") and len(digits) in (9,10):
    return digits
  return None

def upsert_owner_profile_from_text(shop_id: str, text: str) -> Dict[str, Any]:
  """
  Heuristics: try to extract display name and phone number from free-form text and upsert to owner_profile/default.
  """
  db = get_db()
  name = None
  phone = normalize_th_phone(text or "")
  if not name:
    # crude heuristic: anything before a comma or 'เบอร์/โทร' keyword
    m = re.search(r"^(.*?)(?:,|เบอร์|โทร)", (text or ""), flags=re.IGNORECASE)
    if m: name = m.group(1).strip() or None
  ref = db.collection("shops").document(shop_id).collection("owner_profile").document("default")
  payload = {}
  if name: payload["display_name"] = name
  if phone: payload["phone"] = phone
  if payload:
    ref.set(payload, merge=True)
  return {"saved": bool(payload), **payload}

def fetch_line_profile(access_token: str, user_id: str) -> Dict[str, Any]:
  """
  Call LINE profile API.
  """
  url = f"https://api.line.me/v2/bot/profile/{user_id}"
  headers = {"Authorization": f"Bearer {access_token}"}
  try:
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()
  except Exception:
    return {}