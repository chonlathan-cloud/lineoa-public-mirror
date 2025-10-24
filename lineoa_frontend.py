# lineoa_frontend.py (multi-tenant for up to 10,000+ LINE OA)
# - Single webhook endpoint for all OAs
# - Dynamically loads LINE credentials per OA (by channelId a.k.a. destination)
# - Verifies signature per OA, routes events, and persists via dao.py
# - Exposes small REST endpoints for internal reads; supports media (image/video/audio)
# - connet URL for app cript by google chellane 
import os
import json
import hmac
import base64
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import urllib.request
from urllib.error import HTTPError, URLError
import re
import io
from report_renderer import _build_report_pdf_v3, _build_report_pdf_weasy
# Optional GCS for media upload
try:
    from google.cloud import storage
    _STORAGE_AVAILABLE = True
except Exception:
    _STORAGE_AVAILABLE = False

from flask import Flask, request, abort, jsonify
from flask_cors import CORS
from dao import (
    get_shop, get_shop_id_by_line_oa_id, get_shop_id_by_bot_user_id,
    upsert_customer, save_message,
    list_messages, list_products, list_promotions, list_customers,
    add_owner_user, is_owner_user, upsert_owner_profile, get_owner_profile,
    ensure_event_once,  # <-- idempotency
    list_owner_users,
    record_manual_payment, confirm_payment, list_payments, sum_payments_between,
    attach_payment_slip,
    set_payment_confirm_code, find_pending_payment_by_code, 
    confirm_payment_by_code, reject_payment_by_code,
    create_payment_intent, set_intent_confirm_code, 
    find_pending_intent_by_code, confirm_intent_to_payment, reject_intent_by_code,
    find_latest_pending_intent, confirm_latest_pending_intent_to_payment, reject_latest_pending_intent, attach_recent_intent_by_user,
)

from firestore_client import get_db
from admin.blueprint import admin_bp
from owner.blueprint import owner_bp

# Optional LINE reply (created per-tenant only when needed)
try:
    from linebot import LineBotApi
    from linebot.models import TextSendMessage, QuickReply, QuickReplyButton, MessageAction
except Exception:
    LineBotApi = None  # optional
    TextSendMessage = None
    QuickReply = None
    QuickReplyButton = None
    MessageAction = None

# --- LINE profile helper ---
from typing import Tuple

def _fetch_line_profile(access_token: Optional[str], user_id: str) -> Dict[str, Optional[str]]:
    """Fetch user's LINE profile; returns {display_name, picture_url}. Safe-fail."""
    if not access_token or not LineBotApi:
        return {}
    try:
        api = LineBotApi(access_token)
        prof = api.get_profile(user_id)
        return {
            "display_name": getattr(prof, "display_name", None),
            "picture_url": getattr(prof, "picture_url", None),
        }
    except Exception as e:
        logger.warning("get_profile failed: %s %s", e, _log_ctx(user_id=user_id))
        return {}

# Optional Secret Manager (if you store secrets there)
try:
    from google.cloud import secretmanager
    _SM_AVAILABLE = True
except Exception:
    _SM_AVAILABLE = False

# ---------- App ----------
app = Flask(__name__)
# ---- App bootstrap: secret key + blueprints ----
import os as _os_boot
if not getattr(app, "secret_key", None):
    app.secret_key = _os_boot.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

from admin.blueprint import admin_bp
from owner.blueprint import owner_bp

app.register_blueprint(admin_bp)
app.register_blueprint(owner_bp)
# ---- end bootstrap ----
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lineoa-frontend-mt")

API_BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN", "")

# Optional fallback for dev when mapping destination -> shop_id ยังไม่ครบ
DEFAULT_SHOP_ID = os.environ.get("DEFAULT_SHOP_ID", "").strip()

 # Media configs
MEDIA_BUCKET = os.environ.get("MEDIA_BUCKET", "").strip()            # e.g. lineoa-media-dev
MEDIA_PUBLIC_BASE = os.environ.get("MEDIA_PUBLIC_BASE", "").rstrip("/")  # optional public base URL
_storage_client = None

# Reports / Branding
REPORT_BUCKET = os.environ.get("REPORT_BUCKET", "lineoa-report-for-owner").strip()
REPORT_LOGO_PATH = os.environ.get("REPORT_LOGO_PATH", "/mnt/data/Logo.png").strip()
BRAND_PRIMARY_HEX = os.environ.get("BRAND_PRIMARY_HEX", "#2B5EA4").strip()  # light navy
BRAND_ACCENT_HEX = os.environ.get("BRAND_ACCENT_HEX", "#7FADEB").strip()    # soft light blue
REPORT_TITLE_TH = os.environ.get("REPORT_TITLE_TH", "รายงานสรุปข้อมูลลูกค้า").strip()
REPORT_TITLE_EN = os.environ.get("REPORT_TITLE_EN", "Customer Insight Report").strip()

def _get_storage():
    global _storage_client
    if not _STORAGE_AVAILABLE:
        return None
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client

# ---------- Helpers ----------

def _require_auth():
    token = request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not token:
        token = request.headers.get("X-Api-Token", "").strip()
    if not API_BEARER_TOKEN or token != API_BEARER_TOKEN:
        abort(401, "Unauthorized")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _detect_intent(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ["promo", "promotion", "โปร", "โปรฯ", "ส่วนลด"]):
        return "promotion"
    if any(k in t for k in ["สินค้า", "product", "รุ่น", "ราคา", "stock", "สต็อก"]):
        return "product"
    # payment intent keywords (Thai/EN)
    if any(k in t for k in ["โอน", "แจ้งโอน", "ชำระ", "ชำระเงิน", "จ่าย", "จ่ายเงิน", "สลิป", "payment", "paid", "transfer"]):
        return "payment"
    return "message"

# Helper: parse payment intent from message text
def _parse_payment_intent(text: str) -> Optional[Dict[str, Any]]:
    """Extract amount (float) and optional currency from a free-form text.
    Heuristics: look for the first decimal number; accept separators comma/dot.
    Default currency: THB.
    Example matches: "โอน 500", "ชำระ 1,250 บาท", "paid 300.50", "transfer 2000".
    """
    if not text:
        return None
    t = text.replace(",", "")
    # Try patterns like 1,234.56 or 500 or 500.5
    import re
    m = re.search(r"(\d+(?:[\.]\d{1,2})?)", t)
    if not m:
        return None
    try:
        amount = float(m.group(1))
    except Exception:
        return None
    # currency heuristic
    cur = "THB"
    if any(k in text for k in ["usd", "$"]):
        cur = "USD"
    elif any(k in text for k in ["eur", "€"]):
        cur = "EUR"
    elif any(k in text for k in ["฿", "บาท", "thb"]):
        cur = "THB"
    return {"amount": amount, "currency": cur}

def _normalize_phone_th(s: str) -> Optional[str]:
    digits = re.sub(r"\D", "", s or "")
    if len(digits) == 10 and digits.startswith("0"):
        return digits
    if digits.startswith("66") and len(digits) == 11:
        return "0" + digits[2:]
    return digits if len(digits) >= 9 else None

def _log_ctx(shop_id: Optional[str] = None, user_id: Optional[str] = None,
             event_id: Optional[str] = None, message_id: Optional[str] = None) -> str:
    parts = []
    if shop_id: parts.append(f"shop={shop_id}")
    if user_id: parts.append(f"user_id={user_id}")
    if event_id: parts.append(f"event_id={event_id}")
    if message_id: parts.append(f"message_id={message_id}")
    return " ".join(parts)
def _is_valid_line_user_id(uid: str) -> bool:
    """Valid LINE userId is 'U' + 32 hex chars."""
    try:
        return isinstance(uid, str) and len(uid) == 33 and uid.startswith("U") and all(c in "0123456789abcdef" for c in uid[1:].lower())
    except Exception:
        return False

def _get_settings_by_shop_id(shop_id: str) -> Dict[str, Any]:
    db = get_db()
    snap = db.collection("shops").document(shop_id).collection("settings").document("default").get()
    return snap.to_dict() if snap.exists else {}

def _ensure_shop_display_name(shop_id: str, access_token: Optional[str]) -> None:
    """Ensure the LINE OA display name is saved to owner_profile/default.line_display_name once."""
    if not access_token:
        return
    try:
        db = get_db()
        prof = db.collection("shops").document(shop_id).collection("owner_profile").document("default").get()
        if prof.exists and (prof.to_dict() or {}).get("line_display_name"):
            return
        req = urllib.request.Request("https://api.line.me/v2/bot/info")
        req.add_header("Authorization", f"Bearer {access_token}")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        display_name = data.get("displayName")
        if display_name:
            upsert_owner_profile(shop_id, line_display_name=display_name)
    except Exception as e:
        logger.warning("fetch bot info failed: %s %s", e, _log_ctx(shop_id=shop_id))

def _push_payment_review_to_owners(shop_id: str, access_token: Optional[str], user_id: str, amount: float, currency: str, payment_id: str, slip_gcs_uri: Optional[str], confirm_code: str) -> None:
    if not access_token or not LineBotApi or not TextSendMessage:
        return
    try:
        api = LineBotApi(access_token)
        owners = [u for u in list_owner_users(shop_id) if _is_valid_line_user_id(u)]
        logger.info("payment-review push: owners=%s", owners)
        if not owners:
            return
        amt_txt = f"{amount:.2f} {currency}"
        slip_txt = f"\nสลิป: {slip_gcs_uri}" if slip_gcs_uri else ""
        msg = (
            f"มีการแจ้งโอนจากลูกค้า\nยอด: {amt_txt}{slip_txt}\n"
            f"ยืนยัน: 1010\n"
            f"ปัดตก: 0011"
        )
        for oid in owners:
            try:
                qr = None
                if QuickReply and QuickReplyButton and MessageAction:
                    qr = QuickReply(items=[
                        QuickReplyButton(action=MessageAction(label="ยืนยัน", text="1010")),
                        QuickReplyButton(action=MessageAction(label="ปัดตก", text="0011")),
                    ])
                api.push_message(oid, TextSendMessage(text=msg, quick_reply=qr))
            except Exception as e:
                logger.warning("push to owner failed: %s %s uid=%s", e, _log_ctx(shop_id=shop_id), oid)
    except Exception as e:
        logger.warning("push owners review failed: %s %s", e, _log_ctx(shop_id=shop_id))
    if not access_token:
        return
    try:
        db = get_db()
        prof = db.collection("shops").document(shop_id).collection("owner_profile").document("default").get()
        if prof.exists and (prof.to_dict() or {}).get("line_display_name"):
            return
        req = urllib.request.Request("https://api.line.me/v2/bot/info")
        req.add_header("Authorization", f"Bearer {access_token}")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        display_name = data.get("displayName")
        if display_name:
            upsert_owner_profile(shop_id, line_display_name=display_name)
    except Exception as e:
        logger.warning("fetch bot info failed: %s %s", e, _log_ctx(shop_id=shop_id))

def _get_shop_and_settings_by_line_oa_id(line_oa_id: str) -> Optional[Dict[str, Any]]:
    destination = line_oa_id
    # 1) map แบบถูกต้อง: bot_user_id (destination จะเป็น U...)
    shop_id = get_shop_id_by_bot_user_id(destination)

    # 2) เผื่อกรณีทดสอบ/ค่าเป็นตัวเลข → ลอง map ด้วย Channel ID (line_oa_id) แบบเก่า
    if not shop_id and destination and destination.isdigit():
        shop_id = get_shop_id_by_line_oa_id(destination)

    # 3) dev fallback
    if not shop_id and DEFAULT_SHOP_ID:
        settings = _get_settings_by_shop_id(DEFAULT_SHOP_ID) or {}
        logger.warning("Fallback to DEFAULT_SHOP_ID=%s for destination=%s", DEFAULT_SHOP_ID, destination)
        return {"shop_id": DEFAULT_SHOP_ID, "settings": settings}

    if not shop_id:
        return None

    settings = _get_settings_by_shop_id(shop_id)
    return {"shop_id": shop_id, "settings": settings or {}}

def _resolve_secret_value(settings: Dict[str, Any], direct_key: str, sm_key: str) -> Optional[str]:
    val = settings.get(direct_key)
    if val:
        return val
    sm_res = settings.get(sm_key)
    if not sm_res:
        return None
    if not _SM_AVAILABLE:
        logger.error("Secret Manager referenced but package not available: %s", sm_res)
        return None
    try:
        sm = secretmanager.SecretManagerServiceClient()
        resp = sm.access_secret_version(name=sm_res)
        return resp.payload.data.decode("utf-8")
    except Exception as e:
        logger.exception("Secret Manager access failed for %s: %s", sm_res, e)
        return None

def _compute_signature(channel_secret: str, body: bytes) -> str:
    mac = hmac.new(channel_secret.encode("utf-8"), body, hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")

#
# ----- MIME map for media extension/content-type -----
MIME_MAP = {
    "image": ("image/jpeg", ".jpg"),
    "video": ("video/mp4", ".mp4"),
    "audio": ("audio/m4a", ".m4a"),
}
# Fallbacks by exact MIME if LINE returns header content-type
MIME_EXT_BY_CT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "video/mp4": ".mp4",
    "audio/m4a": ".m4a",
    "audio/aac": ".m4a",
}

# ----- Media helpers -----
LINE_CONTENT_URL_TMPL = "https://api-data.line.me/v2/bot/message/{message_id}/content"

def _download_line_content(access_token: Optional[str], message_id: str) -> Optional[tuple[bytes, Optional[str]]]:
    """
    Download binary media from LINE and return (content_bytes, content_type).
    content_type is taken from response header if present; may be None.
    """
    if not access_token:
        logger.warning("No access_token available; skip media download for %s", message_id)
        return None
    url = LINE_CONTENT_URL_TMPL.format(message_id=message_id)
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {access_token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
            # Try to extract MIME from header
            ct = None
            try:
                ct = resp.info().get_content_type()
            except Exception:
                pass
            return (data, ct)
    except HTTPError as e:
        logger.error("LINE content download HTTPError %s for message %s %s", e.code, message_id, _log_ctx())
    except URLError as e:
        logger.error("LINE content download URLError %s for message %s %s", e, message_id, _log_ctx())
    except Exception as e:
        logger.exception("LINE content download failed for %s: %s %s", message_id, e, _log_ctx())
    return None

def _store_media(shop_id: str, mtype: str, message_id: str, content: Optional[bytes], content_type: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Store media to GCS with correct content-type and file extension so it can be opened by players.
    - Infers content-type by header or message type.
    - Appends extension (.mp4/.jpg/.m4a) to the blob path.
    - Sets Cache-Control for better UX.
    """
    if not content:
        return None
    if not MEDIA_BUCKET:
        logger.warning("MEDIA_BUCKET not set; skipping upload for %s %s", message_id, _log_ctx(shop_id=shop_id))
        return None
    client = _get_storage()
    if client is None:
        logger.error("google-cloud-storage not available; cannot upload media %s", _log_ctx(shop_id=shop_id))
        return None

    # Infer final content-type and extension
    ct = (content_type or "").strip().lower() if content_type else None
    # If LINE did not give us a useful type, coerce by message type
    if not ct or ct == "application/octet-stream" or ct == "binary/octet-stream":
        ct = MIME_MAP.get(mtype, ("application/octet-stream", ""))[0]
    # Decide extension: prefer by exact MIME header if available (after coercion)
    ext = MIME_EXT_BY_CT.get(ct)
    if not ext:
        ext = MIME_MAP.get(mtype, ("", ""))[1] or ""

    try:
        bucket = client.bucket(MEDIA_BUCKET)
        base = f"shops/{shop_id}/media/{mtype}/{message_id}"
        blob_path = base + (ext or "")
        blob = bucket.blob(blob_path)
        # caching for public/static media
        try:
            blob.cache_control = "public, max-age=86400"
        except Exception:
            pass
        blob.upload_from_string(content, content_type=ct or "application/octet-stream")

        gcs_uri = f"gs://{MEDIA_BUCKET}/{blob_path}"
        public_url = f"{MEDIA_PUBLIC_BASE}/{blob_path}" if MEDIA_PUBLIC_BASE else None
        return {
            "gcs_uri": gcs_uri,
            "public_url": public_url,
            "content_type": ct,
            "size": len(content),
            "bucket": MEDIA_BUCKET,
            "path": blob_path,
        }
    except Exception as e:
        logger.exception("Upload to GCS failed for %s: %s %s", message_id, e, _log_ctx(shop_id=shop_id))
        return None

 # ----- Media URL augmentation -----
def _augment_media_urls(media: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Ensure media dict has a browser-accessible URL."""
    if not media:
        return None
    try:
        if media.get("public_url"):
            return media
        path = media.get("path")
        bucket_name = media.get("bucket")
        if MEDIA_PUBLIC_BASE and path:
            media["public_url"] = f"{MEDIA_PUBLIC_BASE}/{path}"
            return media
        if bucket_name and path:
            client = _get_storage()
            if client is not None:
                blob = client.bucket(bucket_name).blob(path)
                try:
                    url = blob.generate_signed_url(expiration=timedelta(hours=24), method="GET")
                    media["signed_url"] = url
                except Exception as e:
                    logger.warning("generate_signed_url failed for %s/%s: %s %s", bucket_name, path, e, _log_ctx())
    except Exception as e:
        logger.warning("_augment_media_urls error: %s %s", e, _log_ctx())
    return media


# ----- Media path lookup for slip by LINE message id -----

def _find_media_blob_path(shop_id: str, mtype: str, line_message_id: str) -> Optional[Dict[str, Any]]:
    """Given LINE message id and media type, try to locate the uploaded blob in GCS.
    We will probe common extensions and return dict with gcs_uri/public_url if found.
    """
    if not MEDIA_BUCKET or not line_message_id:
        return None
    client = _get_storage()
    if client is None:
        return None
    bucket = client.bucket(MEDIA_BUCKET)
    # Try known extensions by type
    exts = {
        "image": [".jpg", ".png"],
        "video": [".mp4"],
        "audio": [".m4a"],
    }.get(mtype, [".jpg", ".png", ".mp4", ".m4a"])
    base = f"shops/{shop_id}/media/{mtype}/{line_message_id}"
    for ext in exts:
        path = base + ext
        blob = bucket.blob(path)
        try:
            if blob.exists():
                gcs_uri = f"gs://{MEDIA_BUCKET}/{path}"
                public_url = f"{MEDIA_PUBLIC_BASE}/{path}" if MEDIA_PUBLIC_BASE else None
                return {"bucket": MEDIA_BUCKET, "path": path, "gcs_uri": gcs_uri, "public_url": public_url}
        except Exception:
            continue
    return None


# ---------- Reports / Owner PDF Helpers ----------

def _to_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def _previous_period(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    """Return the period immediately before [start, end] with the same duration."""
    dur = end - start
    prev_end = start - timedelta(seconds=1)
    prev_start = prev_end - dur
    return (_to_utc(prev_start), _to_utc(prev_end))

def _trend_daily_messages(shop_id: str, start: datetime, end: datetime) -> Dict[str, Dict[str, int]]:
    """Aggregate per-day inbound/outbound counts and active users for [start, end]. Keys are YYYY-MM-DD (TH)."""
    db = get_db()
    th_tz = timezone(timedelta(hours=7))
    buckets: Dict[str, Dict[str, int]] = {}
    cur = start.astimezone(th_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end_th = end.astimezone(th_tz)
    while cur <= end_th:
        key = cur.strftime("%Y-%m-%d")
        buckets[key] = {"inbound": 0, "outbound": 0, "active_users": 0}
        cur += timedelta(days=1)
    cust_col = db.collection("shops").document(shop_id).collection("customers")
    for cdoc in cust_col.stream():
        cid = cdoc.id
        seen_days = set()
        msgs = (
            cust_col.document(cid).collection("messages")
            .where("timestamp", ">=", start)
            .where("timestamp", "<=", end)
        )
        try:
            for mdoc in msgs.stream():
                m = mdoc.to_dict() or {}
                ts = m.get("timestamp")
                try:
                    if hasattr(ts, "to_datetime"):
                        dt = ts.to_datetime().astimezone(th_tz)
                    elif hasattr(ts, "astimezone"):
                        dt = ts.astimezone(th_tz)
                    else:
                        continue
                except Exception:
                    continue
                key = dt.strftime("%Y-%m-%d")
                if key not in buckets:
                    buckets[key] = {"inbound": 0, "outbound": 0, "active_users": 0}
                if m.get("direction") == "inbound":
                    buckets[key]["inbound"] += 1
                elif m.get("direction") == "outbound":
                    buckets[key]["outbound"] += 1
                if key not in seen_days:
                    buckets[key]["active_users"] += 1
                    seen_days.add(key)
        except Exception:
            pass
    return buckets

def _biweekly_period(now: Optional[datetime] = None) -> tuple[datetime, datetime]:
    """
    Return (start_utc, end_utc) for the previous half-month, assuming scheduler triggers on the 1st and 16th.
    If called on other days, returns the last 14 days.
    """
    th_tz = timezone(timedelta(hours=7))
    now_th = (now or datetime.now(timezone.utc)).astimezone(th_tz)

    if now_th.day == 1:
        # previous month: 16 -> end of month
        first_of_month = now_th.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        prev_month_last = first_of_month - timedelta(days=1)
        start = prev_month_last.replace(day=16, hour=0, minute=0, second=0, microsecond=0)
        end = prev_month_last.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif now_th.day == 16:
        # current month: 1 -> 15
        start = now_th.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now_th.replace(day=15, hour=23, minute=59, second=59, microsecond=999999)
    else:
        # fallback last 14 days
        end = now_th
        start = now_th - timedelta(days=14)

    return (_to_utc(start), _to_utc(end))

def _count_customers(db, shop_id: str) -> int:
    # Prefer aggregation count() if available; otherwise fallback to iterating.
    try:
        q = db.collection("shops").document(shop_id).collection("customers")
        agg = q.count()
        res = list(agg.get())
        if res and hasattr(res[0][0], "value"):
            return int(res[0][0].value)
    except Exception:
        pass
    # Fallback
    try:
        return sum(1 for _ in db.collection("shops").document(shop_id).collection("customers").stream())
    except Exception:
        return 0

def _daterange_filter(q, field: str, start: datetime, end: datetime):
    try:
        q = q.where(field, ">=", start).where(field, "<=", end)
    except Exception:
        pass
    return q

def _compute_kpis(shop_id: str, start: datetime, end: datetime) -> dict:
    """
    Computes KPIs by iterating customers/messages (sufficient for UAT and small-to-mid shops).
    """
    db = get_db()
    total_customers = _count_customers(db, shop_id)

    # list customers (ids + first_interaction_at)
    cust_col = db.collection("shops").document(shop_id).collection("customers")
    customers = []
    for doc in cust_col.stream():
        d = doc.to_dict() or {}
        customers.append({
            "id": doc.id,
            "first_interaction_at": d.get("first_interaction_at"),
            "last_interaction_at": d.get("last_interaction_at"),
        })

    new_customers = 0
    active_chat_users = set()
    inbound_msgs = 0
    outbound_msgs = 0

    for c in customers:
        # count "new" by first_interaction_at in period
        fi = c.get("first_interaction_at")
        try:
            if fi and hasattr(fi, "timestamp"):
                fi = fi  # Firestore Timestamp
                fi_dt = fi.to_datetime().astimezone(timezone.utc)
            elif hasattr(fi, "isoformat"):
                fi_dt = fi
            else:
                fi_dt = None
            if fi_dt and start <= fi_dt <= end:
                new_customers += 1
        except Exception:
            pass

        # messages within period
        msgs = (
            cust_col.document(c["id"]).collection("messages")
            .where("timestamp", ">=", start)
            .where("timestamp", "<=", end)
        )
        try:
            for mdoc in msgs.stream():
                m = mdoc.to_dict() or {}
                active_chat_users.add(c["id"])
                if m.get("direction") == "inbound":
                    inbound_msgs += 1
                elif m.get("direction") == "outbound":
                    outbound_msgs += 1
        except Exception:
            # ignore per-customer failures
            pass

    # promotions (active)
    try:
        promo_q = db.collection("shops").document(shop_id).collection("promotions").where("status", "==", "active")
        promo_active = sum(1 for _ in promo_q.stream())
    except Exception:
        promo_active = 0

    # payments summary (confirmed/succeeded) — compute from shops/{shop_id}/payments
    # Allow configurable statuses via env var (comma-separated), default to common "positive" statuses.
    pay_count = 0
    pay_amount = 0.0
    try:
        positive_statuses = os.environ.get("REPORT_PAYMENT_STATUSES", "confirmed,succeeded,paid,completed")
        allow = [s.strip().lower() for s in positive_statuses.split(",") if s.strip()]
        # Pull payments in date range (server-side filter by paid_at), then filter by status client-side
        pays = list_payments(shop_id, start=start, end=end, status=None, limit=2000)
        for pdoc in pays:
            st = (pdoc.get("status") or "").lower()
            if allow and st not in allow:
                continue
            try:
                pay_amount += float(pdoc.get("amount") or 0)
                pay_count += 1
            except Exception:
                pass
    except Exception:
        # fall back silently; revenue=0 if any error
        pass

    summary = {
        "total_customers": total_customers,
        "new_customers": new_customers,
        "reactivated_customers": None,  # TODO: add when we track inactivity windows
        "inbound_msgs": inbound_msgs,
        "outbound_msgs": outbound_msgs,
        "active_chat_users": len(active_chat_users),
        "avg_response_time_sec": None,
        "promo_active": promo_active,
        "payments_success": pay_count,
        "revenue": pay_amount,
    }
    return summary


def _upload_pdf_to_gcs(pdf_bytes: bytes, shop_id: str, report_id: str) -> dict:
    if not REPORT_BUCKET:
        raise RuntimeError("REPORT_BUCKET is not configured")
    client = _get_storage()
    if client is None:
        raise RuntimeError("google-cloud-storage is not available")
    bucket = client.bucket(REPORT_BUCKET)
    path = f"reports/{shop_id}/{report_id}.pdf"
    blob = bucket.blob(path)
    try:
        blob.cache_control = "public, max-age=86400"
    except Exception:
        pass
    blob.upload_from_string(pdf_bytes, content_type="application/pdf")
    public_url = f"https://storage.googleapis.com/{REPORT_BUCKET}/{path}"
    # Try to produce a signed URL for convenience
    pub = None
    try:
        pub = blob.generate_signed_url(expiration=timedelta(days=30), method="GET")
    except Exception:
        pub = None
    return {
    "bucket": REPORT_BUCKET,
    "path": path,
    "gcs_uri": f"gs://{REPORT_BUCKET}/{path}",
    "public_url": public_url,
    "signed_url": pub
    }
# ---------- Routes ----------

@app.get("/front/health")
def health():
    return {
        "ok": True,
        "service": "lineoa-frontend-mt",
        "time": _now_iso(),
        "media_bucket": MEDIA_BUCKET or None
    }, 200

@app.get("/line/webhook")
def line_webhook_verify():
    return "OK", 200

@app.get("/line/webhook/")
def line_webhook_verify_slash():
    return "OK", 200

@app.post("/line/webhook")
def line_webhook():
    signature = request.headers.get("X-Line-Signature", "")
    body_bytes: bytes = request.get_data()
    try:
        body = json.loads(body_bytes.decode("utf-8"))
    except Exception:
        abort(400, "Invalid JSON body")

    line_oa_id = body.get("destination")  # bot_user_id (ส่วนมากขึ้นต้นด้วย U...)
    if not line_oa_id:
        abort(400, "Missing destination (channelId)")

    ctx = _get_shop_and_settings_by_line_oa_id(line_oa_id)
    if not ctx:
        logger.error("Unknown LINE OA id: %s", line_oa_id)
        abort(404, "Unknown destination")

    shop_id = ctx["shop_id"]
    settings = ctx["settings"]
    logger.info("webhook route destination=%s shop=%s", line_oa_id, shop_id)

    channel_secret = _resolve_secret_value(settings, "line_channel_secret", "sm_line_channel_secret")
    access_token = _resolve_secret_value(settings, "line_channel_access_token", "sm_line_channel_access_token")

    if not channel_secret:
        abort(500, "LINE channel secret not configured for this shop")

    expected_sig = _compute_signature(channel_secret, body_bytes)
    if not hmac.compare_digest(signature, expected_sig):
        logger.warning("invalid signature %s", _log_ctx(shop_id=shop_id))
        abort(400, "Invalid signature")

    events: List[Dict[str, Any]] = body.get("events", [])
    for ev in events:
        user_id = None
        event_id = None
        message_id = None
        try:
            ev_type = ev.get("type")
            msg = ev.get("message", {}) or {}
            mtype = msg.get("type")
            user_id = (ev.get("source", {}) or {}).get("userId", "")
            event_id = ev.get("webhookEventId") or msg.get("id") or ev.get("replyToken")
            message_id = msg.get("id")

            if ev_type != "message":
                logger.info("skip non-message %s %s", ev_type, _log_ctx(shop_id, user_id, event_id, message_id))
                continue
            if not user_id:
                logger.warning("missing userId %s", _log_ctx(shop_id, None, event_id, message_id))
                continue

            # Idempotency per shop
            try:
                is_new = ensure_event_once(shop_id, event_id)
            except Exception as e:
                logger.warning("ensure_event_once failed: %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))
                is_new = True
            if not is_new:
                logger.info("duplicate event ignored %s", _log_ctx(shop_id, user_id, event_id, message_id))
                continue

            # owner?
            owner = False
            try:
                owner = is_owner_user(shop_id, user_id)
            except Exception as e:
                logger.warning("is_owner_user check failed: %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))

            # ensure customer exists/updated (also fetch LINE profile once)
            try:
                prof = _fetch_line_profile(access_token, user_id)
                display_name = prof.get("display_name")
                if display_name:
                    logger.info("fetched profile %s display_name=%s", _log_ctx(shop_id, user_id, event_id, message_id), display_name)
                upsert_customer(shop_id, user_id, display_name=display_name)
            except Exception as e:
                logger.warning("upsert_customer failed %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))

            if mtype == "text":
                text = (msg.get("text") or "").strip()
                if owner:
                    low = text.lower()
                    saved_owner_any = False
                    if low.startswith("ชื่อร้าน:") or low.startswith("ร้าน:") or low.startswith("ร้าน "):
                        if ":" in text:
                            business_name = text.split(":", 1)[1].strip()
                        else:
                            business_name = text.split(" ", 1)[1].strip() if " " in text else text
                        if business_name:
                            upsert_owner_profile(shop_id, business_name=business_name)
                            logger.info("owner business_name saved %s name=%s", _log_ctx(shop_id, user_id, event_id, message_id), business_name)
                            saved_owner_any = True
                    elif low.startswith("ชื่อ:") or low.startswith("ชื่อ "):
                        full_name = text.split(":",1)[-1].strip() if ":" in text else text[4:].strip()
                        if full_name:
                            upsert_owner_profile(shop_id, full_name=full_name)
                            logger.info("owner full_name saved %s name=%s", _log_ctx(shop_id, user_id, event_id, message_id), full_name)
                            saved_owner_any = True
                    elif low.startswith("เบอร์:") or low.startswith("เบอร์ "):
                        raw = text.split(":",1)[-1].strip() if ":" in text else text[5:].strip()
                        phone = _normalize_phone_th(raw)
                        if phone:
                            upsert_owner_profile(shop_id, phone=phone)
                            logger.info("owner phone saved %s phone=%s", _log_ctx(shop_id, user_id, event_id, message_id), phone)
                            saved_owner_any = True
                    if not saved_owner_any:
                        digits = re.sub(r"\D", "", text)
                        if (len(digits) >= 9 and len(digits) <= 11) and any(ch.isdigit() for ch in text):
                            phone = _normalize_phone_th(text)
                            if phone:
                                upsert_owner_profile(shop_id, phone=phone)
                                logger.info("owner phone(saved by heuristic) %s phone=%s", _log_ctx(shop_id, user_id, event_id, message_id), phone)
                                saved_owner_any = True
                        if not saved_owner_any:
                            if any('\u0E00' <= ch <= '\u0E7F' for ch in text) and (" " in text) and len(text) >= 4:
                                upsert_owner_profile(shop_id, full_name=text)
                                logger.info("owner full_name(saved by heuristic) %s name=%s", _log_ctx(shop_id, user_id, event_id, message_id), text)
                                saved_owner_any = True
                        if not saved_owner_any and text.startswith("ร้าน"):
                            bn = text
                            upsert_owner_profile(shop_id, business_name=bn)
                            logger.info("owner business_name(saved by heuristic) %s name=%s", _log_ctx(shop_id, user_id, event_id, message_id), bn)
                            saved_owner_any = True
                    _ensure_shop_display_name(shop_id, access_token)
                    # Owner review commands with fixed codes: Confirm=1010, Reject=0011
                    try:
                        # Normalize spaces
                        low_compact = re.sub(r"\s+", " ", low).strip()

                        # --- Reject first ---
                        if low_compact in ("0011", "reject 0011", "ปัดตก 0011", "ไม่ใช่ 0011", "ยกเลิก 0011") or re.match(r"^(ปัดตก|reject|ไม่ใช่|ยกเลิก)\s+0011$", low_compact):
                            iid = reject_latest_pending_intent(shop_id, within_minutes=120)
                            if iid and access_token and LineBotApi and TextSendMessage:
                                api = LineBotApi(access_token)
                                # Ack to owner
                                try:
                                    api.push_message(user_id, TextSendMessage(text="ปัดตกแล้ว (0011)"))
                                except Exception:
                                    logger.warning("push reject ack to owner failed %s", _log_ctx(shop_id, user_id, event_id, message_id))
                                # Notify customer (C) for the rejected intent
                                try:
                                    db = get_db()
                                    isnap = db.collection("shops").document(shop_id).collection("payment_intents").document(iid).get()
                                    idata = isnap.to_dict() or {}
                                    cus = idata.get("customer_user_id")
                                    if cus:
                                        api.push_message(cus, TextSendMessage(text="กรุณาส่งใหม่ เพื่อยืนยันการชำระเงินอีกครั้งครับ"))
                                except Exception as _pe:
                                    logger.warning("push reject-to-customer failed: %s %s", _pe, _log_ctx(shop_id, user_id, event_id, message_id))
                            logger.info("owner rejected latest intent iid=%s %s", iid, _log_ctx(shop_id, user_id, event_id, message_id))
                            continue

                        # --- Explicit confirm (fixed code 1010) ---
                        if low_compact in ("1010", "ยืนยัน 1010", "confirm 1010", "ok 1010", "ตกลง 1010", "approve 1010") or re.match(r"^(ยืนยัน|confirm|ok|ตกลง|approve)\s+1010$", low_compact):
                            pid = confirm_latest_pending_intent_to_payment(shop_id, within_minutes=120)
                            if pid and access_token and LineBotApi and TextSendMessage:
                                api = LineBotApi(access_token)
                                # Ack to owner
                                try:
                                    api.push_message(user_id, TextSendMessage(text="ยืนยันสำเร็จ (1010)"))
                                except Exception:
                                    logger.warning("push confirm ack to owner failed %s", _log_ctx(shop_id, user_id, event_id, message_id))
                                # Notify customer (C)
                                try:
                                    db = get_db()
                                    pdoc = db.collection("shops").document(shop_id).collection("payments").document(pid).get()
                                    pdata = pdoc.to_dict() or {}
                                    cus = pdata.get("customer_user_id")
                                    amt = pdata.get("amount")
                                    cur = pdata.get("currency") or "THB"
                                    if cus:
                                        txt = (f"ร้านยืนยันการชำระเงินเรียบร้อย จำนวน {float(amt):.2f} {cur} ขอบคุณครับ" if isinstance(amt, (int, float)) else "ร้านยืนยันการชำระเงินเรียบร้อย ขอบคุณครับ")
                                        api.push_message(cus, TextSendMessage(text=txt))
                                except Exception as _pe:
                                    logger.warning("push confirm-to-customer failed: %s %s", _pe, _log_ctx(shop_id, user_id, event_id, message_id))
                            logger.info("owner confirmed latest intent -> payment %s %s", pid, _log_ctx(shop_id, user_id, event_id, message_id))
                            continue
                    except Exception as _oe:
                        logger.warning("owner fixed-code review failed: %s %s", _oe, _log_ctx(shop_id, user_id, event_id, message_id))
                intent = _detect_intent(text)
                # Auto-create manual payment when message indicates payment intent
                auto_payment_created = False
                if intent == "payment":
                    parsed = _parse_payment_intent(text)
                    if parsed and parsed.get("amount"):
                        # Find recent inbound media (slip) within last 10 minutes
                        recent_slip_msg_id = None
                        slip_gcs_uri = None
                        try:
                            from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                            since_iso = (_dt.now(_tz.utc) - _td(minutes=10)).isoformat()
                            # Do not filter by has_media; we want to see image messages even if upload was skipped.
                            msgs = list_messages(
                                shop_id,
                                user_id=user_id,
                                limit=15,
                                since=since_iso,
                                direction="inbound",
                            )
                            # Prefer the most recent message whose extra.type == "image"
                            for im in msgs:
                                ex = im.get("extra") or {}
                                if (ex.get("type") == "image"):
                                    recent_slip_msg_id = (ex.get("raw") or {}).get("message_id")
                                    media = ex.get("media") or {}
                                    slip_gcs_uri = media.get("gcs_uri") or None
                                    break
                            # If still not found, accept any message that has media block
                            if not recent_slip_msg_id:
                                for im in msgs:
                                    ex = im.get("extra") or {}
                                    media = ex.get("media") or {}
                                    if isinstance(media, dict):
                                        recent_slip_msg_id = (ex.get("raw") or {}).get("message_id")
                                        slip_gcs_uri = media.get("gcs_uri") or None
                                        break
                        except Exception as e:
                            logger.warning("scan recent media failed: %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))

                        # If we only have message id, try to derive GCS path
                        if (not slip_gcs_uri) and recent_slip_msg_id:
                            info = _find_media_blob_path(shop_id, "image", recent_slip_msg_id)
                            if info:
                                slip_gcs_uri = info.get("gcs_uri")

                        try:
                            intent_id = create_payment_intent(
                                shop_id=shop_id,
                                customer_user_id=user_id,
                                amount=float(parsed["amount"]),
                                currency=parsed.get("currency", "THB"),
                                slip_gcs_uri=slip_gcs_uri,
                                message_id=recent_slip_msg_id or None,
                            )
                            code = (intent_id[-6:] if isinstance(intent_id, str) and len(intent_id) >= 6 else intent_id)
                            set_intent_confirm_code(shop_id, intent_id, code)
                            auto_payment_created = True
                            logger.info("payment intent created %s amount=%.2f slip=%s", _log_ctx(shop_id, user_id, event_id, message_id), float(parsed["amount"]), bool(slip_gcs_uri))
                            # Notify owner to confirm with code
                            try:
                                if access_token and LineBotApi and TextSendMessage:
                                    api = LineBotApi(access_token)
                                    amt_txt = f"{parsed['amount']:.2f} {parsed.get('currency','THB')}"
                                    slip_txt = f"\nสลิป: {slip_gcs_uri}" if slip_gcs_uri else ""
                                    msg_owner = (
                                        f"มีการแจ้งโอนจากลูกค้า\nยอด: {amt_txt}{slip_txt}\n"
                                        f"ยืนยัน: 1010\nปัดตก: 0011"
                                    )
                                    for oid in [u for u in list_owner_users(shop_id) if isinstance(u, str) and u.startswith("U")]:
                                        try:
                                            qr = None
                                            if QuickReply and QuickReplyButton and MessageAction:
                                                qr = QuickReply(items=[
                                                    QuickReplyButton(action=MessageAction(label="ยืนยัน", text="1010")),
                                                    QuickReplyButton(action=MessageAction(label="ปัดตก", text="0011")),
                                                ])
                                            api.push_message(oid, TextSendMessage(text=msg_owner, quick_reply=qr))
                                        except Exception as _pe:
                                            logger.warning("push to owner failed: %s %s uid=%s", _pe, _log_ctx(shop_id=shop_id), oid)
                            except Exception as _pe:
                                logger.warning("push owner-notify failed: %s %s", _pe, _log_ctx(shop_id, user_id))
                            # Acknowledge to customer C
                            try:
                                if access_token and LineBotApi and TextSendMessage:
                                    api = LineBotApi(access_token)
                                    api.push_message(user_id, TextSendMessage(text="รับคำขอชำระแล้ว รอร้านยืนยันค่ะ"))
                            except Exception as _pe:
                                logger.warning("push ack to customer failed: %s %s", _pe, _log_ctx(shop_id, user_id))
                        except Exception as _e:
                            logger.exception("create payment intent failed: %s %s", _e, _log_ctx(shop_id, user_id, event_id, message_id))
                extra = {"raw": {"message_id": msg.get("id")}}
                save_message(shop_id, user_id, text=text, ts=None, direction="inbound", intent=intent, extra=extra)
                logger.info("recv text %s text=%s", _log_ctx(shop_id, user_id, event_id, message_id), text[:200])
                continue

            if mtype == "location" and owner:
                loc = {
                    "lat": msg.get("latitude"),
                    "lng": msg.get("longitude"),
                    "address": msg.get("address"),
                }
                upsert_owner_profile(shop_id, location=loc)
                logger.info("owner location saved %s loc=%s", _log_ctx(shop_id, user_id, event_id, message_id), loc)
                extra = {"raw": {"message_id": msg.get("id")}, "location": loc, "type": "location"}
                save_message(shop_id, user_id, text="<owner location>", ts=None, direction="inbound", intent="owner_location", extra=extra)
                continue

            if mtype in ("image", "video", "audio"):
                dl = _download_line_content(access_token, message_id)
                content, content_type = (dl if isinstance(dl, tuple) else (dl, None))
                media_info = _store_media(shop_id, mtype, message_id, content, content_type)
                # --- Auto-attach slip to latest awaiting_owner intent by this user (image only) ---
                try:
                    if mtype == "image":
                        gcs_uri0 = (media_info or {}).get("gcs_uri")
                        iid_attached = attach_recent_intent_by_user(
                            shop_id=shop_id,
                            customer_user_id=user_id,
                            slip_gcs_uri=gcs_uri0,
                            message_id=message_id,
                            within_minutes=60,
                        )
                        if iid_attached:
                            logger.info("attached slip to pending intent %s %s", iid_attached, _log_ctx(shop_id, user_id, event_id, message_id))
                except Exception as e:
                    logger.warning("auto-attach slip to intent failed: %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))
                extra = {"raw": {"message_id": message_id}, "media": media_info, "type": mtype}
                placeholder = f"&lt;{mtype} message&gt;"
                save_message(shop_id, user_id, text=placeholder, ts=None, direction="inbound", intent=mtype, extra=extra)
                logger.info("recv %s %s stored=%s ct=%s", mtype, _log_ctx(shop_id, user_id, event_id, message_id), bool(media_info), content_type)
                continue

            # ignore other message types for now
            logger.info("skip message type=%s %s", mtype, _log_ctx(shop_id, user_id, event_id, message_id))

        except Exception as e:
            logger.exception("event processing error: %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))

    return "OK", 200

@app.post("/line/webhook/")
def line_webhook_slash():
    return line_webhook()

# ------------- Internal REST (protected) -------------

@app.get("/front/shops/<shop_id>/messages")
def front_list_messages(shop_id):
    _require_auth()
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "error": "user_id_required"}), 400

    limit = int(request.args.get("limit", "50"))
    before = request.args.get("before")
    since = request.args.get("since")  # NEW
    direction = request.args.get("direction")  # NEW: "inbound" or "outbound"

    has_media_param = request.args.get("has_media")
    has_media = None
    if has_media_param is not None:
        v = has_media_param.strip().lower()
        if v in ("1", "true", "yes"): has_media = True
        elif v in ("0", "false", "no"): has_media = False

    items = list_messages(
        shop_id,
        user_id=user_id,
        limit=limit,
        before=before,
        has_media=has_media,
        since=since,
        direction=direction,
    )

    for it in items:
        extra = it.get("extra") or {}
        media = extra.get("media")
        if isinstance(media, dict):
            _augment_media_urls(media)

    next_before = None
    if items:
        last_ts = items[-1].get("timestamp")
        if isinstance(last_ts, str):
            next_before = last_ts

    return jsonify({"ok": True, "items": items, "next_before": next_before}), 200

@app.get("/front/shops/<shop_id>/products")
def front_list_products(shop_id):
    _require_auth()
    items = list_products(shop_id)
    return jsonify({"ok": True, "items": items}), 200

@app.get("/front/shops/<shop_id>/promotions")
def front_list_promotions(shop_id):
    _require_auth()
    status = request.args.get("status")
    items = list_promotions(shop_id, status=status)
    return jsonify({"ok": True, "items": items}), 200

@app.get("/front/shops/<shop_id>/customers")
def front_list_customers_endpoint(shop_id):
    _require_auth()
    limit = int(request.args.get("limit", "100"))
    before = request.args.get("before")  # NEW
    items = list_customers(shop_id, limit=limit, before=before)

    next_before = None
    if items:
        last_ts = items[-1].get("last_interaction_at")
        if isinstance(last_ts, str):
            next_before = last_ts

    return jsonify({"ok": True, "items": items, "next_before": next_before}), 200

@app.post("/front/shops/<shop_id>/owners")
def front_add_owner(shop_id):
    _require_auth()
    data = request.get_json(silent=True) or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "error": "user_id_required"}), 400
    add_owner_user(shop_id, user_id)
    return jsonify({"ok": True}), 200


@app.get("/front/shops/<shop_id>/owner_profile")
def front_get_owner_profile(shop_id):
    _require_auth()
    prof = get_owner_profile(shop_id) or {}
    return jsonify({"ok": True, "profile": prof}), 200



@app.patch("/front/shops/<shop_id>/owner_profile")
def front_update_owner_profile(shop_id):
    _require_auth()
    data = request.get_json(silent=True) or {}
    upsert_owner_profile(
        shop_id,
        full_name=data.get("full_name"),
        phone=data.get("phone"),
        location=data.get("location"),
        line_display_name=data.get("line_display_name"),
        business_name=data.get("business_name"),
    )
    return jsonify({"ok": True}), 200


# ----- Payments: Manual + Aggregation Endpoints -----

@app.post("/front/shops/<shop_id>/payments/manual")
def front_create_manual_payment(shop_id):
    _require_auth()
    data = request.get_json(silent=True) or {}
    customer_user_id = (data.get("customer_user_id") or "").strip()
    amount = data.get("amount")
    currency = (data.get("currency") or "THB").strip() or "THB"
    paid_at_str = data.get("paid_at")
    slip_gcs_uri = (data.get("slip_gcs_uri") or "").strip() or None
    slip_public_url = (data.get("slip_public_url") or "").strip() or None

    if not customer_user_id:
        return jsonify({"ok": False, "error": "customer_user_id_required"}), 400
    try:
        amount = float(amount)
    except Exception:
        return jsonify({"ok": False, "error": "amount_invalid"}), 400

    paid_at = None
    if paid_at_str:
        try:
            from dateutil import parser as _dtparser
            paid_at = _dtparser.isoparse(paid_at_str)
        except Exception:
            paid_at = None

    # Optional: find slip by LINE message context
    if not slip_gcs_uri and data.get("line_message_id"):
        mtype = (data.get("media_type") or "image").strip()
        info = _find_media_blob_path(shop_id, mtype, data.get("line_message_id"))
        if info:
            slip_gcs_uri = info.get("gcs_uri")
            if not slip_public_url:
                slip_public_url = info.get("public_url")

    intent_id = create_payment_intent(
        shop_id=shop_id,
        customer_user_id=customer_user_id,
        amount=float(amount),
        currency=currency,
        slip_gcs_uri=slip_gcs_uri,
        message_id=data.get("line_message_id") or None,
    )
    code = intent_id[-6:] if isinstance(intent_id, str) and len(intent_id) >= 6 else intent_id
    set_intent_confirm_code(shop_id, intent_id, code)
    # push to owners
    try:
        ctx_settings = _get_settings_by_shop_id(shop_id) or {}
        access_token2 = _resolve_secret_value(ctx_settings, "line_channel_access_token", "sm_line_channel_access_token")
        if access_token2 and LineBotApi and TextSendMessage:
            api = LineBotApi(access_token2)
            amt_txt = f"{float(amount):.2f} {currency}"
            slip_txt = f"\nสลิป: {slip_gcs_uri}" if slip_gcs_uri else ""
            mmsg_owner = (
                f"มีการแจ้งโอนจากลูกค้า\nยอด: {amt_txt}{slip_txt}\n"
                f"ยืนยัน: 1010\nปัดตก: 0011"
            )
            for oid in [u for u in list_owner_users(shop_id) if isinstance(u, str) and u.startswith("U")]:
                try:
                    api.push_message(oid, TextSendMessage(text=msg_owner))
                except Exception as _pe:
                    logger.warning("push to owner failed: %s %s uid=%s", _pe, _log_ctx(shop_id=shop_id), oid)
    except Exception as _pe:
        logger.warning("push owner-notify failed: %s %s", _pe, _log_ctx(shop_id=shop_id))

    return jsonify({"ok": True, "intent_id": intent_id, "confirm_code": code, "slip_gcs_uri": slip_gcs_uri, "slip_public_url": slip_public_url}), 200


@app.patch("/front/shops/<shop_id>/payments/<payment_id>/confirm")
def front_confirm_payment(shop_id, payment_id):
    _require_auth()
    db = get_db()
    ref = db.collection("shops").document(shop_id).collection("payments").document(payment_id)
    snap = ref.get()
    if not snap.exists:
        return jsonify({"ok": False, "error": "payment_not_found", "payment_id": payment_id}), 404

    pdata = snap.to_dict() or {}
    cus = pdata.get("customer_user_id")
    amt = pdata.get("amount")
    cur = pdata.get("currency") or "THB"

    confirm_payment(shop_id, payment_id)

    # Push confirmation to customer C
    try:
        ctx_settings = _get_settings_by_shop_id(shop_id) or {}
        access_token2 = _resolve_secret_value(ctx_settings, "line_channel_access_token", "sm_line_channel_access_token")
        if cus and access_token2 and LineBotApi and TextSendMessage:
            api = LineBotApi(access_token2)
            if isinstance(amt, (int, float)):
                txt = f"ร้านยืนยันการชำระเงินเรียบร้อย จำนวน {amt:.2f} {cur} ขอบคุณครับ"
            else:
                txt = "ร้านยืนยันการชำระเงินเรียบร้อย ขอบคุณครับ"
            api.push_message(cus, TextSendMessage(text=txt))
    except Exception as e:
        logger.warning("push confirm-to-customer failed: %s %s", e, _log_ctx(shop_id=shop_id))

    return jsonify({"ok": True}), 200


# --- Attach slip to existing payment ---
@app.patch("/front/shops/<shop_id>/payments/<payment_id>/attach-slip")
def front_attach_payment_slip(shop_id, payment_id):
    _require_auth()
    data = request.get_json(silent=True) or {}
    line_message_id = (data.get("line_message_id") or "").strip() or None
    slip_gcs_uri = (data.get("slip_gcs_uri") or "").strip() or None
    mtype = (data.get("media_type") or "image").strip()

    if not line_message_id and not slip_gcs_uri:
        return jsonify({"ok": False, "error": "line_message_id_or_slip_gcs_uri_required"}), 400

    resolved_uri = slip_gcs_uri
    if not resolved_uri and line_message_id:
        info = _find_media_blob_path(shop_id, mtype, line_message_id)
        if info:
            resolved_uri = info.get("gcs_uri")
        if not resolved_uri:
            return jsonify({"ok": False, "error": "slip_not_found_in_gcs", "line_message_id": line_message_id}), 404

    try:
        attach_payment_slip(shop_id, payment_id, slip_gcs_uri=resolved_uri, message_id=line_message_id)
    except ValueError as e:
        if str(e) == "payment_not_found":
            return jsonify({"ok": False, "error": "payment_not_found", "payment_id": payment_id}), 404
        raise

    return jsonify({"ok": True, "slip_gcs_uri": resolved_uri, "message_id": line_message_id}), 200

@app.get("/front/shops/<shop_id>/payments")
def front_list_payments(shop_id):
    _require_auth()
    try:
        limit = int(request.args.get("limit", "20"))
    except Exception:
        limit = 20
    status = request.args.get("status") or None
    start = request.args.get("start")
    end = request.args.get("end")

    sdt = edt = None
    if start or end:
        try:
            from dateutil import parser as _dtparser
            if start:
                sdt = _dtparser.isoparse(start)
            if end:
                edt = _dtparser.isoparse(end)
        except Exception:
            return jsonify({"ok": False, "error": "invalid_datetime"}), 400

    items = list_payments(shop_id, start=sdt, end=edt, status=status, limit=limit)
    return jsonify({"ok": True, "items": items}), 200

@app.get("/front/shops/<shop_id>/payments/summary")
def front_payments_summary(shop_id):
    _require_auth()
    start_param = request.args.get("start")
    end_param = request.args.get("end")
    if not start_param or not end_param:
        return jsonify({"ok": False, "error": "start_and_end_required"}), 400
    try:
        from dateutil import parser as _dtparser
        ps = _dtparser.isoparse(start_param)
        pe = _dtparser.isoparse(end_param)
    except Exception:
        return jsonify({"ok": False, "error": "invalid_datetime"}), 400
    agg = sum_payments_between(shop_id, ps, pe)
    return jsonify({"ok": True, "summary": {"count": agg.get("count", 0), "amount": agg.get("amount", 0.0)}}), 200


@app.post("/tasks/generate-biwk-report")
def task_generate_biwk_report():
    # Protect with the same bearer token
    _require_auth()
    db = get_db()

    shop_id = request.args.get("shop_id", "").strip()
    if not shop_id:
        return jsonify({"ok": False, "error": "shop_id_required"}), 400

    # Resolve LINE access token for push + ensure shop display name
    ctx_settings = _get_settings_by_shop_id(shop_id) or {}
    access_token = _resolve_secret_value(ctx_settings, "line_channel_access_token", "sm_line_channel_access_token")
    if access_token:
        _ensure_shop_display_name(shop_id, access_token)

    # Period
    start_param = request.args.get("start")
    end_param = request.args.get("end")
    if start_param and end_param:
        try:
            from dateutil import parser as _dtparser
            ps = _dtparser.isoparse(start_param)
            pe = _dtparser.isoparse(end_param)
            period_start, period_end = _to_utc(ps), _to_utc(pe)
        except Exception:
            period_start, period_end = _biweekly_period()
    else:
        period_start, period_end = _biweekly_period()

    # Compute KPIs
    summary = _compute_kpis(shop_id, period_start, period_end)
    insights: list[str] = []  # simple v1, add heuristics later

    # previous period and trend for charts/%Δ
    prev_start, prev_end = _previous_period(period_start, period_end)
    prev_summary = _compute_kpis(shop_id, prev_start, prev_end)
    trend = _trend_daily_messages(shop_id, period_start, period_end)

    # Persist report shell
    report_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    report_doc = {
        "period_start": period_start,
        "period_end": period_end,
        "generated_at": datetime.now(timezone.utc),
        "summary": summary,
        "insights": [{"type": "info", "text": t} for t in insights],
        "status": "generating",
    }
    # Store under shops/{shop_id}/owner_reports/{report_id}
    rep_ref = db.collection("shops").document(shop_id).collection("owner_reports").document(report_id)
    rep_ref.set(report_doc, merge=False)

    # Build PDF
    renderer = os.environ.get("REPORT_RENDERER", "weasy").lower()
    if renderer in ("weasy", "html", "htmlpdf"):
        pdf_bytes = _build_report_pdf_weasy(shop_id, period_start, period_end, summary, prev_summary, trend)
    else:
        pdf_bytes = _build_report_pdf_v3(shop_id, period_start, period_end, summary, prev_summary, trend)
    # Fallback: ถ้า HTML renderer ล้มเหลว (None/empty) ให้ใช้ ReportLab V3
    if not pdf_bytes:
        logger.warning("HTML renderer failed or returned empty bytes; falling back to ReportLab V3 %s", _log_ctx(shop_id=shop_id))
        pdf_bytes = _build_report_pdf_v3(shop_id, period_start, period_end, summary, prev_summary, trend)
    # Upload to GCS
    upload = _upload_pdf_to_gcs(pdf_bytes, shop_id, report_id)

    # Update report with URL + mark ready
    rep_ref.set({
        "pdf": upload,
        "status": "ready",
    }, merge=True)

    # Push LINE to owners (summary + link)
    push_ok = False
    try:
        owners = [u for u in list_owner_users(shop_id) if _is_valid_line_user_id(u)]
        if owners and access_token and LineBotApi:
            # keep only real LINE userIds (start with 'U')
            owners = [u for u in owners if isinstance(u, str) and u.startswith("U")]
            api = LineBotApi(access_token)
            url_txt = upload.get("signed_url") or upload.get("public_url") #or upload.get("gcs_uri")
            msg = (f"{REPORT_TITLE_TH}\n"
                   f"ช่วงเวลา: {period_start.astimezone(timezone(timedelta(hours=7))).strftime('%d %b %Y')} – "
                   f"{period_end.astimezone(timezone(timedelta(hours=7))).strftime('%d %b %Y')}\n"
                   f"ลูกค้าทั้งหมด: {summary.get('total_customers')} | ลูกค้าใหม่: {summary.get('new_customers')}\n"
                   f"ดูรายงาน: {url_txt}")
            logger.info("pushing owner report to %d owners (shop=%s, token=%s...)", len(owners), shop_id, access_token[:8] if access_token else "NONE")
            for uid in owners:
                try:
                    api.push_message(uid, TextSendMessage(text=msg))
                    push_ok = True
                except Exception as e:
                    logger.warning("push to owner failed: %s %s uid=%s", e, _log_ctx(shop_id=shop_id), uid)
    except Exception as e:
        logger.warning("owner push failed: %s %s", e, _log_ctx(shop_id=shop_id))

    return jsonify({
        "ok": True,
        "shop_id": shop_id,
        "report_id": report_id,
        "summary": summary,
        "pdf": upload,
        "pushed": push_ok
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)