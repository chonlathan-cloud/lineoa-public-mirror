

# core/payments.py
from __future__ import annotations
from typing import Dict, Any, Optional, Tuple
import re, datetime

try:
  from services.firestore_client import get_db
except Exception:
  from firestore_client import get_db

def parse_payment_intent(text: str) -> Optional[Dict[str, Any]]:
  """
  Parse Thai/English amount like 'โอน 500', 'pay 1200', 'ชำระ 99.50'
  Returns dict {'amount': float, 'currency': 'THB'}
  """
  if not text:
    return None
  m = re.search(r"([0-9]+(?:\.[0-9]{1,2})?)", text.replace(",", ""))
  if not m:
    return None
  try:
    amt = float(m.group(1))
    if amt <= 0:
      return None
    return {"amount": amt, "currency": "THB"}
  except Exception:
    return None

def create_or_attach_intent(shop_id: str, user_id: str, text: str) -> Optional[str]:
  """
  Create a pending payment_intent from parsed text, or return None if not parsable.
  """
  info = parse_payment_intent(text)
  if not info:
    return None
  db = get_db()
  ref = db.collection("shops").document(shop_id).collection("payment_intents").document()
  now = datetime.datetime.utcnow()
  payload = {
    "user_id": user_id,
    "amount": info["amount"],
    "currency": info["currency"],
    "status": "pending",
    "created_at": now,
    "updated_at": now,
    "source": "message",
    "raw_text": text,
  }
  ref.set(payload, merge=False)
  return ref.id

def confirm_latest_pending_intent(shop_id: str, reviewer_id: str, note: str = "") -> Optional[str]:
  """
  Find latest pending intent, convert to a payment, and mark intent as confirmed.
  """
  db = get_db()
  col = db.collection("shops").document(shop_id).collection("payment_intents")
  q = col.where("status", "==", "pending").order_by("created_at", direction=__import__("google.cloud.firestore").firestore.Query.DESCENDING).limit(1)
  snaps = list(q.stream())
  if not snaps:
    return None
  intent = snaps[0]
  data = intent.to_dict() or {}
  now = datetime.datetime.utcnow()
  # mark intent
  intent.reference.set({"status": "confirmed", "updated_at": now, "reviewer_id": reviewer_id, "review_note": note}, merge=True)
  # create payment
  pref = db.collection("shops").document(shop_id).collection("payments").document()
  pref.set({
    "user_id": data.get("user_id"),
    "amount": data.get("amount"),
    "currency": data.get("currency", "THB"),
    "created_at": now,
    "intent_id": intent.id,
    "note": note,
  }, merge=False)
  return pref.id

def reject_latest_pending_intent(shop_id: str, reviewer_id: str, note: str = "") -> Optional[str]:
  db = get_db()
  col = db.collection("shops").document(shop_id).collection("payment_intents")
  q = col.where("status", "==", "pending").order_by("created_at", direction=__import__("google.cloud.firestore").firestore.Query.DESCENDING).limit(1)
  snaps = list(q.stream())
  if not snaps:
    return None
  intent = snaps[0]
  now = datetime.datetime.utcnow()
  intent.reference.set({"status": "rejected", "updated_at": now, "reviewer_id": reviewer_id, "review_note": note}, merge=True)
  return intent.id