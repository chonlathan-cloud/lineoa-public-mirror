# dao.py — Firestore data access layer (no Flask routes)
# Used by lineoa_frontend.py

from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from firebase_admin import firestore as fb
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists  # <-- สำหรับ idempotency

from firestore_client import get_db

# ---------- helpers ----------

def ts_now():
    return fb.SERVER_TIMESTAMP


def _ensure_aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ---------- shops / settings ----------

# Canonical: settings/default under each shop
def get_shop_settings(shop_id: str) -> Optional[Dict[str, Any]]:
    """Return shops/{shopId}/settings/default as a dict, or None if missing."""
    db = get_db()
    snap = (
        db.collection("shops").document(shop_id)
          .collection("settings").document("default")
          .get()
    )
    return snap.to_dict() if snap.exists else None

def set_shop_settings(shop_id: str, data: Dict[str, Any], merge: bool = True) -> None:
    """Upsert fields into shops/{shopId}/settings/default."""
    if not data:
        return
    db = get_db()
    (
        db.collection("shops").document(shop_id)
          .collection("settings").document("default")
          .set(data, merge=merge)
    )

def get_shop_settings_value(shop_id: str, key: str, default: Any = None) -> Any:
    """Convenience: read one key from settings/default."""
    s = get_shop_settings(shop_id) or {}
    return s.get(key, default)

def set_shop_bot_user_id(shop_id: str, bot_user_id: str) -> None:
    """Persist mapping bot_user_id -> shops/{shopId} at top-level shop document for fast lookup."""
    db = get_db()
    db.collection("shops").document(shop_id).set({"bot_user_id": bot_user_id}, merge=True)

def set_shop_line_oa_id(shop_id: str, line_oa_id: str) -> None:
    """Persist mapping line_oa_id -> shops/{shopId} at top-level shop document for fast lookup."""
    db = get_db()
    db.collection("shops").document(shop_id).set({"line_oa_id": line_oa_id}, merge=True)

def get_shop(shop_id: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    snap = db.collection("shops").document(shop_id).get()
    return snap.to_dict() if snap.exists else None


def get_shop_id_by_line_oa_id(line_oa_id: str) -> Optional[str]:
    """Map LINE OA channel ID -> shopId"""
    db = get_db()
    q = db.collection("shops").where("line_oa_id", "==", line_oa_id).limit(1)
    docs = list(q.stream())
    if not docs:
        return None
    return docs[0].id

def get_shop_id_by_bot_user_id(bot_user_id: str) -> Optional[str]:
    """Map LINE bot userId (webhook 'destination', starts with 'U') -> shopId"""
    db = get_db()
    q = db.collection("shops").where("bot_user_id", "==", bot_user_id).limit(1)
    docs = list(q.stream())
    if not docs:
        return None
    return docs[0].id

# ---------- events (idempotency) ----------

def ensure_event_once(shop_id: str, event_id: str) -> bool:
    """
    Mark event_id as seen under the shop.
    Returns True if this is the first time (new),
    or False if it was already processed.
    """
    if not event_id:
        # ถ้าไม่มี event_id ให้ถือว่าใหม่ (กันไม่ให้ drop ข้อความ)
        return True
    db = get_db()
    ref = (
        db.collection("shops").document(shop_id)
          .collection("events_seen").document(event_id)
    )
    try:
        ref.create({"seen_at": ts_now()})
        return True
    except AlreadyExists:
        return False

# ---------- customers ----------

def upsert_customer(shop_id: str, customer_line_user_id: str, display_name: Optional[str] = None) -> None:
    """Create/update customer basic fields and last_interaction_at."""
    db = get_db()
    ref = (
        db.collection("shops").document(shop_id)
          .collection("customers").document(customer_line_user_id)
    )
    data: Dict[str, Any] = {"last_interaction_at": ts_now()}
    if display_name:
        data["display_name"] = display_name
    ref.set(data, merge=True)

# ---------- messages ----------

def save_message(
    shop_id: str,
    customer_line_user_id: str,
    text: str,
    ts: Optional[datetime] = None,
    direction: str = "inbound",
    intent: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    """Persist a message and update customer's last_interaction_at. Returns doc id."""
    db = get_db()
    if ts is None:
        timestamp_value: Any = ts_now()
    else:
        timestamp_value = _ensure_aware_utc(ts)

    cust_ref = (
        db.collection("shops").document(shop_id)
          .collection("customers").document(customer_line_user_id)
    )
    msg_ref = cust_ref.collection("messages").document()

    msg_data: Dict[str, Any] = {
        "text": text,
        "timestamp": timestamp_value,
        "direction": direction,
    }
    if intent is not None:
        msg_data["intent"] = intent
    if extra is not None:
        msg_data["extra"] = extra

    # derive has_media flag for querying later
    try:
        has_media = bool(extra and isinstance(extra.get("media"), dict))
    except Exception:
        has_media = False
    msg_data["has_media"] = has_media

    msg_ref.set(msg_data, merge=False)
    cust_ref.set({"last_interaction_at": timestamp_value}, merge=True)

    # ensure first_interaction_at is set once (used for "new customers" KPI)
    try:
        snap = cust_ref.get()
        need_set_first = True
        if snap.exists:
            data0 = snap.to_dict() or {}
            if "first_interaction_at" in data0 and data0.get("first_interaction_at") is not None:
                need_set_first = False
        if need_set_first:
            cust_ref.set({"first_interaction_at": timestamp_value}, merge=True)
    except Exception:
        # best-effort; ignore failures so message persistence never fails
        pass

    return msg_ref.id


def list_messages(
    shop_id: str,
    user_id: Optional[str] = None,
    limit: int = 50,
    before: Optional[str] = None,
    has_media: Optional[bool] = None,
    since: Optional[str] = None,            # <--- NEW
    direction: Optional[str] = None,        # <--- NEW: "inbound" or "outbound"
) -> List[Dict[str, Any]]:
    db = get_db()
    if not user_id:
        raise ValueError("user_id is required")

    col = (
        db.collection("shops").document(shop_id)
          .collection("customers").document(user_id)
          .collection("messages")
    )
    from google.cloud import firestore as _fs
    q: _fs.Query = col.order_by("timestamp", direction=_fs.Query.DESCENDING)

    # optional filter: has_media True/False
    if has_media is True:
        q = q.where("has_media", "==", True)
    elif has_media is False:
        q = q.where("has_media", "==", False)

    # optional filter: direction
    if direction in ("inbound", "outbound"):
        q = q.where("direction", "==", direction)

    # cursor: before (lt)
    if before:
        try:
            from dateutil import parser as _dtparser
            dt = _dtparser.isoparse(before)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            q = q.where("timestamp", "<", dt)
        except Exception:
            pass

    # lower bound: since (gte)
    if since:
        try:
            from dateutil import parser as _dtparser
            sdt = _dtparser.isoparse(since)
            if sdt.tzinfo is None:
                sdt = sdt.replace(tzinfo=timezone.utc)
            q = q.where("timestamp", ">=", sdt)
        except Exception:
            pass

    q = q.limit(limit)
    items: List[Dict[str, Any]] = []
    for doc in q.stream():
        data = doc.to_dict() or {}
        data["_id"] = doc.id
        tsv = data.get("timestamp")
        if hasattr(tsv, "isoformat"):
            data["timestamp"] = tsv.isoformat()
        items.append(data)
    return items

# ---------- products ----------
def list_products(shop_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("products")
    items: List[Dict[str, Any]] = []

    # Primary: is_active == True
    try:
        q1 = (
            col.where("is_active", "==", True)
               .order_by("created_at", direction=firestore.Query.DESCENDING)
               .limit(limit)
        )
        docs1 = list(q1.stream())
    except Exception:
        docs1 = []

    if docs1:
        for d in docs1:
            items.append(d.to_dict() | {"_id": d.id})
        return items

    # Fallback: status == "active"
    try:
        q2 = (
            col.where("status", "==", "active")
               .order_by("created_at", direction=firestore.Query.DESCENDING)
               .limit(limit)
        )
        docs2 = list(q2.stream())
    except Exception:
        docs2 = []

    if docs2:
        for d in docs2:
            items.append(d.to_dict() | {"_id": d.id})
        return items

    # Last resort: recent docs, filter in memory
    try:
        q3 = col.order_by("created_at", direction=firestore.Query.DESCENDING).limit(limit * 2)
        for d in q3.stream():
            data = d.to_dict() or {}
            is_active = data.get("is_active")
            status = (data.get("status") or "").lower()
            if (is_active is True) or (status == "active"):
                items.append(data | {"_id": d.id})
                if len(items) >= limit:
                    break
    except Exception:
        pass

    return items

# ---------- promotions ----------

def list_promotions(shop_id: str, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("promotions")
    q: firestore.Query = col
    if status:
        q = q.where("status", "==", status)
    q = q.order_by("created_at", direction=firestore.Query.DESCENDING).limit(limit)

    items: List[Dict[str, Any]] = []
    for doc in q.stream():
        data = doc.to_dict() or {}
        data["_id"] = doc.id
        for k in ("created_at", "updated_at", "start_date", "end_date"):
            v = data.get(k)
            if hasattr(v, "isoformat"):
                data[k] = v.isoformat()
        items.append(data)
    return items
# ---------- customers (listing) ----------

def list_customers(shop_id: str, limit: int = 100, before: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return recent customers of a shop ordered by last_interaction_at desc.
       Supports cursor pagination with `before` and returns next_before at API layer."""
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("customers")
    from google.cloud import firestore as _fs
    q: _fs.Query = col.order_by("last_interaction_at", direction=_fs.Query.DESCENDING)

    if before:
        try:
            from dateutil import parser as _dtparser
            dt = _dtparser.isoparse(before)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            q = q.where("last_interaction_at", "<", dt)
        except Exception:
            pass

    q = q.limit(limit)
    items: List[Dict[str, Any]] = []
    for doc in q.stream():
        data = doc.to_dict() or {}
        data["user_id"] = doc.id
        v = data.get("last_interaction_at")
        if hasattr(v, "isoformat"):
            data["last_interaction_at"] = v.isoformat()
        items.append(data)
    return items

# ---------- owners / owner_profile ----------

def add_owner_user(shop_id: str, owner_user_id: str) -> None:
    db = get_db()
    ref = db.collection("shops").document(shop_id).collection("owners").document(owner_user_id)
    ref.set({"active": True}, merge=True)


def is_owner_user(shop_id: str, user_id: str) -> bool:
    db = get_db()
    ref = db.collection("shops").document(shop_id).collection("owners").document(user_id).get()
    return ref.exists and (ref.to_dict() or {}).get("active", False)


def list_owner_users(shop_id: str) -> List[str]:
    """Return active owner userIds for a shop."""
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("owners")
    owners: List[str] = []
    for doc in col.stream():
        data = doc.to_dict() or {}
        if data.get("active", False):
            owners.append(doc.id)
    return owners



def upsert_owner_profile(
    shop_id: str,
    full_name: Optional[str] = None,
    phone: Optional[str] = None,
    location: Optional[Dict[str, Any]] = None,
    line_display_name: Optional[str] = None,
    business_name: Optional[str] = None,
) -> None:
    db = get_db()
    data: Dict[str, Any] = {}
    if full_name is not None:
        data["full_name"] = full_name
    if phone is not None:
        data["phone"] = phone
    if location is not None:
        data["location"] = location
    if line_display_name is not None:
        data["line_display_name"] = line_display_name
    if business_name is not None:
        data["business_name"] = business_name
    if not data:
        return
    db.collection("shops").document(shop_id).collection("owner_profile").document("default").set(data, merge=True)


# Re-introduced get_owner_profile, placed immediately after upsert_owner_profile
def get_owner_profile(shop_id: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    snap = db.collection("shops").document(shop_id).collection("owner_profile").document("default").get()
    return snap.to_dict() if snap.exists else None


# ---------- owner_profile/information (bootstrap data for creating B's OA) ----------
def upsert_owner_information(
    shop_id: str,
    location: Optional[Dict[str, Any]] = None,
    phone: Optional[str] = None,
    **extra: Any,
) -> None:
    """
    Upsert to shops/{shopId}/owner_profile/information.
    Known fields: location, phone, and any extra metadata for provisioning OA B.
    """
    db = get_db()
    data: Dict[str, Any] = {}
    if location is not None:
        data["location"] = location
    if phone is not None:
        data["phone"] = phone
    # include other provided fields as-is (e.g., business_hours, notes)
    for k, v in (extra or {}).items():
        if v is not None:
            data[k] = v
    if not data:
        return
    (
        db.collection("shops").document(shop_id)
          .collection("owner_profile").document("information")
          .set(data, merge=True)
    )

def get_owner_information(shop_id: str) -> Optional[Dict[str, Any]]:
    """Return shops/{shopId}/owner_profile/information as a dict, or None if missing."""
    db = get_db()
    snap = (
        db.collection("shops").document(shop_id)
          .collection("owner_profile").document("information")
          .get()
    )
    return snap.to_dict() if snap.exists else None


# ---------- payments (manual + summary) ----------
from typing import Tuple


def record_manual_payment(
    shop_id: str,
    customer_user_id: str,
    amount: float,
    currency: str = "THB",
    paid_at: Optional[datetime] = None,
    slip_gcs_uri: Optional[str] = None,
    message_id: Optional[str] = None,
) -> str:
    """Create a manual payment record under shops/{shopId}/payments.
    Returns created payment document id.
    Status starts as "pending_review" to allow owner/admin confirmation.
    """
    db = get_db()
    pay_ref = (
        db.collection("shops").document(shop_id)
          .collection("payments").document()
    )
    doc: Dict[str, Any] = {
        "customer_user_id": customer_user_id,
        "amount": float(amount),
        "currency": currency or "THB",
        "paid_at": _ensure_aware_utc(paid_at) if paid_at else ts_now(),
        "method": "manual",
        "status": "pending_review",
        "slip_gcs_uri": slip_gcs_uri,
        "message_id": message_id,
        "created_at": ts_now(),
    }
    pay_ref.set(doc, merge=False)
    return pay_ref.id


def confirm_payment(shop_id: str, payment_id: str) -> None:
    """Mark a manual payment as confirmed."""
    db = get_db()
    (
        db.collection("shops").document(shop_id)
          .collection("payments").document(payment_id)
          .set({"status": "confirmed", "confirmed_at": ts_now()}, merge=True)
    )


def list_payments(
    shop_id: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    status: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """List payments optionally filtered by date range and status."""
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("payments")
    q: firestore.Query = col
    if start is not None:
        q = q.where("paid_at", ">=", _ensure_aware_utc(start))
    if end is not None:
        q = q.where("paid_at", "<=", _ensure_aware_utc(end))
    if status:
        q = q.where("status", "==", status)
    q = q.order_by("paid_at", direction=firestore.Query.DESCENDING).limit(limit)

    items: List[Dict[str, Any]] = []
    for doc in q.stream():
        d = doc.to_dict() or {}
        d["_id"] = doc.id
        for k in ("paid_at", "created_at", "confirmed_at"):
            v = d.get(k)
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        items.append(d)
    return items



def sum_payments_between(
    shop_id: str,
    start: datetime,
    end: datetime,
    statuses: Tuple[str, ...] = ("confirmed", "succeeded"),
) -> Dict[str, Any]:
    """Return aggregate count and sum of payments in [start, end] with given statuses."""
    db = get_db()
    total_amount = 0.0
    count = 0
    col = db.collection("shops").document(shop_id).collection("payments")
    try:
        q = (
            col.where("paid_at", ">=", _ensure_aware_utc(start))
               .where("paid_at", "<=", _ensure_aware_utc(end))
        )
        # Firestore does not support IN on array of statuses older SDKs; try IN else loop
        try:
            q2 = q.where("status", "in", list(statuses))
            docs = list(q2.stream())
        except Exception:
            docs = []
            for st in statuses:
                try:
                    docs.extend(list(q.where("status", "==", st).stream()))
                except Exception:
                    pass
        for doc in docs:
            d = doc.to_dict() or {}
            try:
                amt = float(d.get("amount", 0) or 0)
                total_amount += amt
                count += 1
            except Exception:
                pass
    except Exception:
        pass
    return {"count": count, "amount": total_amount}


# --- Attach/overwrite slip info to an existing payment doc (merge update) ---

def attach_payment_slip(
    shop_id: str,
    payment_id: str,
    slip_gcs_uri: Optional[str] = None,
    message_id: Optional[str] = None,
) -> None:
    """Attach/overwrite slip info to an existing payment doc (merge update)."""
    db = get_db()
    ref = (
        db.collection("shops").document(shop_id)
          .collection("payments").document(payment_id)
    )
    if not ref.get().exists:
        raise ValueError("payment_not_found")
    update: Dict[str, Any] = {"updated_at": ts_now()}
    if slip_gcs_uri:
        update["slip_gcs_uri"] = slip_gcs_uri
    if message_id:
        update["message_id"] = message_id
    ref.set(update, merge=True)


# ----- Owner confirmation via short code -----

def set_payment_confirm_code(shop_id: str, payment_id: str, code: str) -> None:
    db = get_db()
    ref = (
        db.collection("shops").document(shop_id)
          .collection("payments").document(payment_id)
    )
    if not ref.get().exists:
        raise ValueError("payment_not_found")
    ref.set({"confirm_code": code, "confirm_code_set_at": ts_now()}, merge=True)


def find_pending_payment_by_code(shop_id: str, code: str) -> Optional[str]:
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("payments")
    q = (
        col.where("status", "==", "pending_review")
           .where("confirm_code", "==", code)
           .limit(1)
    )
    docs = list(q.stream())
    return docs[0].id if docs else None


def confirm_payment_by_code(shop_id: str, code: str) -> Optional[str]:
    pid = find_pending_payment_by_code(shop_id, code)
    if not pid:
        return None
    confirm_payment(shop_id, pid)
    return pid


def reject_payment_by_code(shop_id: str, code: str) -> Optional[str]:
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("payments")
    q = (
        col.where("status", "==", "pending_review")
           .where("confirm_code", "==", code)
           .limit(1)
    )
    docs = list(q.stream())
    if not docs:
        return None
    pid = docs[0].id
    col.document(pid).set({"status": "rejected", "rejected_at": ts_now()}, merge=True)
    return pid

# ---------- payment intents (staging before owner confirmation) ----------

def create_payment_intent(
    shop_id: str,
    customer_user_id: str,
    amount: float,
    currency: str = "THB",
    slip_gcs_uri: Optional[str] = None,
    message_id: Optional[str] = None,
    expires_minutes: int = 120,
) -> str:
    """Create a staging intent that is NOT counted as a real payment until owner confirms."""
    db = get_db()
    ref = (
        db.collection("shops").document(shop_id)
          .collection("payment_intents").document()
    )
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    now = _dt.now(_tz.utc)
    data: Dict[str, Any] = {
        "customer_user_id": customer_user_id,
        "amount": float(amount),
        "currency": currency or "THB",
        "slip_gcs_uri": slip_gcs_uri,
        "message_id": message_id,
        "status": "awaiting_owner",
        "created_at": ts_now(),
        "expires_at": now + _td(minutes=expires_minutes),
    }
    ref.set(data, merge=False)
    return ref.id


def set_intent_confirm_code(shop_id: str, intent_id: str, code: str) -> None:
    db = get_db()
    ref = (
        db.collection("shops").document(shop_id)
          .collection("payment_intents").document(intent_id)
    )
    if not ref.get().exists:
        raise ValueError("intent_not_found")
    ref.set({"confirm_code": code, "confirm_code_set_at": ts_now()}, merge=True)


def find_pending_intent_by_code(shop_id: str, code: str) -> Optional[str]:
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("payment_intents")
    q = (
        col.where("status", "==", "awaiting_owner")
           .where("confirm_code", "==", code)
           .limit(1)
    )
    docs = list(q.stream())
    return docs[0].id if docs else None


def confirm_intent_to_payment(shop_id: str, code: str) -> Optional[str]:
    """Convert an intent to a real payment and mark intent as confirmed."""
    db = get_db()
    iid = find_pending_intent_by_code(shop_id, code)
    if not iid:
        return None
    iref = db.collection("shops").document(shop_id).collection("payment_intents").document(iid)
    isnap = iref.get()
    if not isnap.exists:
        return None
    i = isnap.to_dict() or {}
    # 1) create payment
    pid = record_manual_payment(
        shop_id=shop_id,
        customer_user_id=i.get("customer_user_id"),
        amount=float(i.get("amount")),
        currency=i.get("currency", "THB"),
        slip_gcs_uri=i.get("slip_gcs_uri"),
        message_id=i.get("message_id"),
    )
    # 2) confirm it
    confirm_payment(shop_id, pid)
    # 3) mark intent
    iref.set({"status": "confirmed", "confirmed_at": ts_now(), "payment_id": pid}, merge=True)
    return pid



def reject_intent_by_code(shop_id: str, code: str) -> Optional[str]:
    db = get_db()
    iid = find_pending_intent_by_code(shop_id, code)
    if not iid:
        return None
    iref = db.collection("shops").document(shop_id).collection("payment_intents").document(iid)
    iref.set({"status": "rejected", "rejected_at": ts_now()}, merge=True)
    return iid


# ---------- helpers for latest intent ops & slip attachment ----------

def find_latest_pending_intent(shop_id: str, within_minutes: int = 120) -> Optional[str]:
    """Return the most recent awaiting_owner intent id within a time window, without requiring composite indexes."""
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("payment_intents")
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    since = _dt.now(_tz.utc) - _td(minutes=within_minutes)
    # Avoid composite index by not combining where(status) + order_by(created_at)
    # Read recent docs by created_at and filter in memory.
    try:
        docs = list(col.order_by("created_at", direction=firestore.Query.DESCENDING).limit(50).stream())
    except Exception:
        # Fallback: no order, just limit
        docs = list(col.limit(50).stream())
    latest_id = None
    latest_ts = None
    for d in docs:
        data = d.to_dict() or {}
        if data.get("status") != "awaiting_owner":
            continue
        cat = data.get("created_at")
        try:
            # Firestore Timestamp -> datetime
            if hasattr(cat, "to_datetime"):
                cat_dt = cat.to_datetime()
            elif hasattr(cat, "isoformat"):
                cat_dt = cat
            else:
                cat_dt = None
        except Exception:
            cat_dt = None
        if cat_dt is None:
            continue
        if cat_dt < since:
            continue
        if latest_ts is None or cat_dt > latest_ts:
            latest_ts = cat_dt
            latest_id = d.id
    return latest_id


def confirm_latest_pending_intent_to_payment(shop_id: str, within_minutes: int = 120) -> Optional[str]:
    """Convert the most recent awaiting_owner intent to a confirmed payment and return payment_id."""
    db = get_db()
    iid = find_latest_pending_intent(shop_id, within_minutes=within_minutes)
    if not iid:
        return None
    iref = db.collection("shops").document(shop_id).collection("payment_intents").document(iid)
    isnap = iref.get()
    if not isnap.exists:
        return None
    i = isnap.to_dict() or {}
    pid = record_manual_payment(
        shop_id=shop_id,
        customer_user_id=i.get("customer_user_id"),
        amount=float(i.get("amount")),
        currency=i.get("currency", "THB"),
        slip_gcs_uri=i.get("slip_gcs_uri"),
        message_id=i.get("message_id"),
    )
    confirm_payment(shop_id, pid)
    iref.set({"status": "confirmed", "confirmed_at": ts_now(), "payment_id": pid}, merge=True)
    return pid


def reject_latest_pending_intent(shop_id: str, within_minutes: int = 120) -> Optional[str]:
    db = get_db()
    iid = find_latest_pending_intent(shop_id, within_minutes=within_minutes)
    if not iid:
        return None
    iref = db.collection("shops").document(shop_id).collection("payment_intents").document(iid)
    iref.set({"status": "rejected", "rejected_at": ts_now()}, merge=True)
    return iid


def attach_recent_intent_by_user(
    shop_id: str,
    customer_user_id: str,
    slip_gcs_uri: Optional[str],
    message_id: Optional[str],
    within_minutes: int = 30,
) -> Optional[str]:
    """Attach slip to the latest awaiting_owner intent by this user within a window. Returns intent_id if updated.
    Avoid composite indexes by fetching recent docs and filtering in memory.
    Additionally, if the intent is already converted to a payment (has payment_id),
    backfill the slip to that payment document too.
    """
    if not slip_gcs_uri and not message_id:
        return None
    db = get_db()
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    since = _dt.now(_tz.utc) - _td(minutes=within_minutes)
    col = db.collection("shops").document(shop_id).collection("payment_intents")

    # Prefer narrowing by customer_user_id alone (single equality), then sort client-side
    try:
        docs = list(
            col.where("customer_user_id", "==", customer_user_id)
               .order_by("created_at", direction=firestore.Query.DESCENDING)
               .limit(50)
               .stream()
        )
    except Exception:
        # Fallback if order_by also needs index: just fetch some docs and filter
        docs = list(col.where("customer_user_id", "==", customer_user_id).limit(50).stream())

    target_id = None
    target_ts = None
    target_was_confirmed = False
    target_payment_id = None

    for doc in docs:
        d = doc.to_dict() or {}
        # choose most recent intent in time window, regardless of status, but prefer awaiting_owner first
        cat = d.get("created_at")
        try:
            if hasattr(cat, "to_datetime"):
                cat_dt = cat.to_datetime()
            elif hasattr(cat, "isoformat"):
                cat_dt = cat
            else:
                cat_dt = None
        except Exception:
            cat_dt = None
        if cat_dt is None or cat_dt < since:
            continue

        # Skip ones that already have slip/message
        if d.get("slip_gcs_uri") or d.get("message_id"):
            continue

        # Pick first candidate: awaiting_owner gets priority
        if d.get("status") == "awaiting_owner":
            target_id = doc.id
            target_ts = cat_dt
            target_was_confirmed = False
            target_payment_id = None
            break
        # Otherwise, keep the most recent confirmed intent (for backfill)
        if d.get("status") == "confirmed" and d.get("payment_id"):
            if (target_ts is None) or (cat_dt > target_ts):
                target_id = doc.id
                target_ts = cat_dt
                target_was_confirmed = True
                target_payment_id = d.get("payment_id")

    if not target_id:
        return None

    ref = col.document(target_id)
    update: Dict[str, Any] = {"updated_at": ts_now()}
    if slip_gcs_uri:
        update["slip_gcs_uri"] = slip_gcs_uri
    if message_id:
        update["message_id"] = message_id
    ref.set(update, merge=True)

    # If this intent was already confirmed and has a payment_id, backfill payment
    try:
        if target_was_confirmed and target_payment_id:
            attach_payment_slip(
                shop_id=shop_id,
                payment_id=target_payment_id,
                slip_gcs_uri=slip_gcs_uri,
                message_id=message_id,
            )
    except Exception:
        # best-effort; do not fail message processing
        pass

    return target_id