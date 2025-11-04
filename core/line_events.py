

# core/line_events.py
from __future__ import annotations
import hmac, hashlib, base64, json
from typing import Dict, Any, Optional, Tuple

def verify_signature(channel_secret: str, body_bytes: bytes) -> bool:
  """
  Validate LINE webhook signature using channel_secret.
  Returns True if valid, False otherwise.
  """
  if not channel_secret:
    return False
  mac = hmac.new(channel_secret.encode("utf-8"), body_bytes, hashlib.sha256).digest()
  sig = base64.b64encode(mac).decode("utf-8")
  return sig

def check_signature(header_signature: str, channel_secret: str, body_bytes: bytes) -> bool:
  """
  Compare the request header 'X-Line-Signature' against a freshly computed one.
  """
  if not header_signature:
    return False
  try:
    expected = verify_signature(channel_secret, body_bytes)
    # constant-time compare
    return hmac.compare_digest(header_signature, expected)
  except Exception:
    return False

def extract_event_fields(event: Dict[str, Any]) -> Dict[str, Any]:
  """
  Normalize common fields from a LINE event.
  Returns dict with keys: type, event_id, reply_token, timestamp, user_id, message_type, text, message_id
  """
  e_type = event.get("type")
  eid = event.get("webhookEventId") or event.get("deliveryContext", {}).get("webhookEventId")
  reply = event.get("replyToken")
  ts = event.get("timestamp")
  src = (event.get("source") or {})
  user_id = src.get("userId") or src.get("senderId")

  msg = event.get("message") or {}
  mtype = msg.get("type")
  mid = msg.get("id")
  text = msg.get("text")

  return {
    "type": e_type,
    "event_id": eid,
    "reply_token": reply,
    "timestamp": ts,
    "user_id": user_id,
    "message_type": mtype,
    "message_id": mid,
    "text": text,
  }

def ensure_event_once(db, shop_id: str, event_id: Optional[str]) -> bool:
  """
  Best-effort idempotency guard. Returns True if event is new; False if duplicate.
  """
  try:
    if not event_id:
      return True
    ref = db.collection("shops").document(shop_id).collection("events_seen").document(event_id)
    snap = ref.get()
    if snap.exists:
      return False
    ref.set({"seen_at": __import__("datetime").datetime.utcnow()})
    return True
  except Exception:
    return True