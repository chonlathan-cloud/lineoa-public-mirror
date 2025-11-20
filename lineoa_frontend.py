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
import base64, json, os, logging
from urllib.parse import quote as _q, parse_qs as _parse_qs
# Try to import heavy report renderer (uses matplotlib); fall back to a tiny ReportLab-only stub if unavailable
try:
    from report_renderer import build_report_pdf_v3, _build_report_pdf_weasy  # preferred (may import matplotlib)
    _build_report_pdf_v3 = build_report_pdf_v3
except Exception as _rr_err:
    import io as _io_fallback
    def _build_report_pdf_v3(shop_id, start_dt, end_dt, **kwargs):
        """
        Minimal fallback PDF generator (no charts) to avoid hard dependency on matplotlib.
        Returns PDF bytes.
        """
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.units import cm
        except Exception:
            # As a last resort, return a tiny placeholder PDF bytes
            return b"%PDF-1.3\n%\xe2\xe3\xcf\xd3\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 595 842]/Contents 4 0 R>>endobj\n4 0 obj<</Length 44>>stream\nBT /F1 12 Tf 72 770 Td (Report temporarily unavailable) Tj ET\nendstream endobj\ntrailer<</Root 1 0 R>>\n%%EOF"
        buf = _io_fallback.BytesIO()
        c = canvas.Canvas(buf, pagesize=A4)
        c.setFont("Helvetica", 16)
        c.drawString(2*cm, 27*cm, "Customer Insight Report (Fallback)")
        c.setFont("Helvetica", 11)
        c.drawString(2*cm, 25.8*cm, f"Shop: {shop_id}")
        try:
            c.drawString(2*cm, 25.0*cm, f"Period: {start_dt.isoformat()}  →  {end_dt.isoformat()}")
        except Exception:
            pass
        c.setFont("Helvetica", 10)
        c.drawString(2*cm, 23.5*cm, "Charts unavailable on this runtime (matplotlib not installed).")
        c.drawString(2*cm, 22.8*cm, "This is a lightweight PDF stub to keep the service healthy.")
        c.showPage()
        c.save()
        return buf.getvalue()
    def _build_report_pdf_weasy(*args, **kwargs):
        # Fallback to the same minimal PDF
        return _build_report_pdf_v3(*args, **kwargs)
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
    list_owner_users, get_default_owner_user_id,
    record_manual_payment, confirm_payment, list_payments, sum_payments_between,
    attach_payment_slip,
    set_payment_confirm_code, find_pending_payment_by_code, 
    confirm_payment_by_code, reject_payment_by_code,
    create_payment_intent, set_intent_confirm_code, 
    find_pending_intent_by_code, confirm_intent_to_payment, reject_intent_by_code,
    find_latest_pending_intent, confirm_latest_pending_intent_to_payment, reject_latest_pending_intent, attach_recent_intent_by_user,
    find_recent_pending_magic_link, bind_owner, mark_magic_link_used,
)
try:
    from admin.onboarding import (
        get_session, save_session, clear_session,
        upload_logo_bytes, upload_payment_qr_bytes,
        finalize_request_from_session, to_flex_summary,
    )
except Exception:
    from onboarding import (
        get_session, save_session, clear_session,
        upload_logo_bytes, upload_payment_qr_bytes,
        finalize_request_from_session, to_flex_summary,
    )

from firestore_client import get_db

# Optional LINE reply (created per-tenant only when needed)
try:
    from linebot import LineBotApi
    from linebot.models import TextSendMessage, QuickReply, QuickReplyButton, MessageAction, FlexSendMessage
except Exception:
    LineBotApi = None  # optional
    TextSendMessage = None
    QuickReply = None
    QuickReplyButton = None
    MessageAction = None
    FlexSendMessage = None

# --- LINE profile helper ---
from typing import Tuple

def _fetch_line_profile(access_token: Optional[str], user_id: str, shop_id: Optional[str] = None) -> Dict[str, Optional[str]]:
    """Fetch user's LINE profile; returns {display_name, picture_url}. Also persists display to shops/{shop}/customers/{user_id} when shop_id is provided."""
    if not access_token or not LineBotApi:
        return {}
    try:
        api = LineBotApi(access_token)
        prof = api.get_profile(user_id)
        result = {
            "display_name": getattr(prof, "display_name", None),
            "picture_url": getattr(prof, "picture_url", None),
        }
        # Persist display name for customer if we know the shop
        try:
            if shop_id and result.get("display_name"):
                db = get_db()
                ref = db.collection("shops").document(shop_id).collection("customers").document(user_id)
                ref.set({
                    "display_name": result["display_name"],
                    "updated_at": datetime.now(timezone.utc),
                    # keep existing first/last interaction fields untouched
                }, merge=True)
        except Exception as pe:
            logger.warning("persist display_name failed: %s %s", pe, _log_ctx(shop_id=shop_id, user_id=user_id))
        return result
    except Exception as e:
        logger.warning("get_profile failed: %s %s", e, _log_ctx(user_id=user_id))
        return {}

 # --- core shared (for credential loading two-mode) ---
from core.secrets import load_shop_context_by_destination as core_load_ctx, resolve_secret as core_resolve_secret

# ---------- App ----------
app = Flask(__name__)
import os as _os_boot
if not getattr(app, "secret_key", None):
    app.secret_key = _os_boot.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

from admin.blueprint import admin_bp, _sign_owner_invite, _build_owner_invite_url, _send_owner_invite_message# from owner.blueprint import owner_bp  # (optional; keep commented if not used)

app.register_blueprint(admin_bp)
# app.register_blueprint(owner_bp)  # (optional)
# ---- end bootstrap ----
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lineoa-frontend-mt")

# ---- Health check and root probe for Cloud Run ----
@app.get("/_ah/health")
def _health():
    return "ok", 200

@app.get("/")
def _root():
    return jsonify({"ok": True, "service": "lineoa-frontend", "ts": datetime.now(timezone.utc).isoformat()}), 200

API_BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN", "")
PUSH_ALL_OWNERS = (os.environ.get("PUSH_ALL_OWNERS", "false") or "false").strip().lower() in ("1", "true", "yes", "on")

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

def _resolve_owner_push_targets(shop_id: str) -> List[str]:
    """Return LINE user IDs that should receive owner notifications."""
    owners: List[str] = []
    try:
        owners = [u for u in list_owner_users(shop_id) if _is_valid_line_user_id(u)]
    except Exception as err:
        logger.warning("list_owner_users failed %s err=%s", _log_ctx(shop_id=shop_id), err)
        owners = []
    default_owner = None
    try:
        default_owner = get_default_owner_user_id(shop_id)
    except Exception as err:
        logger.warning("get_default_owner_user_id failed %s err=%s", _log_ctx(shop_id=shop_id), err)
    logger.info("push target default_owner_user_id=%s shop=%s push_all=%s", default_owner, shop_id, PUSH_ALL_OWNERS)
    if PUSH_ALL_OWNERS:
        return owners
    if default_owner and _is_valid_line_user_id(default_owner):
        return [default_owner]
    if owners:
        return owners[:1]
    if default_owner:
        return [default_owner]
    return []

def _mark_primary_owner_if_missing(shop_id: str, owner_user_id: str) -> None:
    """Mark the first active owner as primary when none exists."""
    if not (shop_id and owner_user_id):
        return
    try:
        db = get_db()
        col = db.collection("shops").document(shop_id).collection("owners")
        q = col.where("is_primary", "==", True).limit(1)
        docs = list(q.stream())
        if docs:
            return
        col.document(owner_user_id).set({"is_primary": True}, merge=True)
    except Exception as err:
        logger.warning("mark_primary_owner_if_missing failed %s err=%s", _log_ctx(shop_id=shop_id, user_id=owner_user_id), err)

def _get_settings_by_shop_id(shop_id: str) -> Dict[str, Any]:
    db = get_db()
    snap = db.collection("shops").document(shop_id).collection("settings").document("default").get()
    if not snap.exists:
        logger.error("missing settings/default for %s", shop_id)
        return {}
    data = snap.to_dict() or {}
    if not data:
        logger.error("empty settings/default for %s", shop_id)
    return data

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


def _auto_bind_owner_if_needed(shop_id: str, user_id: str, settings: Dict[str, Any]) -> bool:
    """
    Try to bind owners/{user_id} from a recent shops/{shop_id}/magic_links/* with status='pending'.
    Returns True if a new binding occurred, False otherwise.
    """
    if not shop_id or not user_id:
        return False

    try:
        magic_link = find_recent_pending_magic_link(shop_id, within_hours=48)
    except Exception:
        return False

    if not magic_link:
        return False

    liff_user_id = magic_link.get("liff_user_id")
    jti = magic_link.get("_id")
    if not liff_user_id or not jti:
        return False

    last_login_channel_id = None
    try:
        last_login_channel_id = (settings or {}).get("channel_id")
        if not last_login_channel_id:
            consumer = (settings or {}).get("oa_consumer")
            if isinstance(consumer, dict):
                last_login_channel_id = consumer.get("channel_id")
    except Exception:
        last_login_channel_id = None

    bind_owner(shop_id, user_id, liff_user_id, last_login_channel_id=last_login_channel_id)
    mark_magic_link_used(shop_id, jti)
    return True

def _push_payment_review_to_owners(shop_id: str, access_token: Optional[str], user_id: str, amount: float, currency: str, payment_id: str, slip_gcs_uri: Optional[str], confirm_code: str) -> None:
    if not access_token or not LineBotApi or not TextSendMessage:
        return
    try:
        api = LineBotApi(access_token)
        owners = _resolve_owner_push_targets(shop_id)
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
# lineoa_frontend.py
def _get_shop_and_settings_by_line_oa_id(line_oa_id: str) -> Optional[Dict[str, Any]]:
    destination = line_oa_id
    ctx = core_load_ctx(destination)
    if not ctx:
        return None

    # Start with whatever core gives us (may be root-level settings)
    base_settings = ctx.get("settings") or {}

    # Decide if we need to fetch the subcollection settings/default
    need_fallback = False
    try:
        consumer_cfg = (base_settings or {}).get("oa_consumer")
        consumer_bot = None
        if isinstance(consumer_cfg, dict):
            consumer_bot = (consumer_cfg.get("bot_user_id") or "").strip()
        # Fallback if consumer block missing or bot_user_id is empty
        need_fallback = not consumer_bot
    except Exception:
        need_fallback = True

    merged = dict(base_settings) if isinstance(base_settings, dict) else {}

    if need_fallback:
        try:
            sub_settings = _get_settings_by_shop_id(ctx["shop_id"]) or {}
            # Merge: subcollection values override base when present
            if isinstance(sub_settings, dict):
                merged.update(sub_settings)
        except Exception:
            pass

    try:
        cb = None
        oc = merged.get("oa_consumer") if isinstance(merged.get("oa_consumer"), dict) else {}
        for k in ("bot_user_id", "botUserId"):
            v = oc.get(k)
            if isinstance(v, str) and v.strip():
                cb = v.strip()
                break
        logger.info("settings-merged: shop=%s has_consumer=%s consumer.bot=%s keys=%s", ctx.get("shop_id"), isinstance(oc, dict), cb or "", list(oc.keys()) if isinstance(oc, dict) else [])
    except Exception:
        pass
    return {"shop_id": ctx["shop_id"], "settings": merged}

def _resolve_oa_context(line_oa_id: str, settings: Dict[str, Any]) -> str:
    """
    Return 'admin' or 'consumer' for the current webhook event.
    We use LINE destination (bot_user_id/channelId) vs settings.oa_consumer.bot_user_id if present.
    Fallback default is 'admin' to be safe for the A->B flow.
    """
    try:
        # Accept various shapes/names to be resilient with existing data
        s = (settings or {}) if isinstance(settings, dict) else {}
        consumer = s.get("oa_consumer") if isinstance(s.get("oa_consumer"), dict) else {}
        # Candidate fields for bot user id
        candidates = [
            (consumer.get("bot_user_id") if isinstance(consumer, dict) else None),
            (consumer.get("botUserId") if isinstance(consumer, dict) else None),
            s.get("oa_consumer_bot_user_id"),  # flattened style
            s.get("bot_user_id"),               # occasionally stored at root
        ]
        consumer_bot = None
        for v in candidates:
            if isinstance(v, str) and v.strip():
                consumer_bot = v.strip()
                break
        # Extra diagnostics so we can see why it still falls back to admin
        try:
            logger.info(
                "ctx-judge: dest=%s consumer.bot=%s has_oa_consumer=%s oa_consumer_keys=%s root_keys=%s",
                (line_oa_id or "").strip(),
                consumer_bot or "",
                isinstance(consumer, dict),
                list((consumer or {}).keys()),
                list(s.keys())
            )
        except Exception:
            pass
        # Primary comparison: destination (botUserId) vs consumer botUserId
        if consumer_bot and isinstance(line_oa_id, str) and line_oa_id.strip() == consumer_bot:
            return "consumer"
    except Exception:
        pass
    return "admin"

def _resolve_secret_value(settings: Dict[str, Any], direct_key: str, sm_key: str) -> Optional[str]:
    """
    Resolve a credential by reading *direct keys only* from settings/default.
    Priority:
      1) settings.oa_consumer.<key>
      2) settings.<key>
    We intentionally do NOT resolve any sm_* references here.
    """
    # Normalize to dicts
    settings = settings or {}
    consumer = (settings.get("oa_consumer") or {}) if isinstance(settings.get("oa_consumer"), dict) else {}

    # Map expected keys
    # direct_key will be one of: "line_channel_access_token" or "line_channel_secret"
    key = direct_key

    # Prefer consumer-scoped value
    val = consumer.get(key)
    if not val:
        val = settings.get(key)
    return val

def _store_customer_last_message(shop_id: str, user_id: str, message: str, ctx: str) -> None:
    if not shop_id or not user_id:
        return
    try:
        ref = (
            get_db().collection("shops")
            .document(shop_id)
            .collection("customers").document(user_id)
        )
        ref.set({
            "last_message": message,
            "last_interaction_at": datetime.now(timezone.utc),
        }, merge=True)
    except Exception as err:
        logger.warning("update customer last_message failed: %s %s", err, ctx)

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

# ---------- Pub/Sub push helpers ----------
def _verify_pubsub_token():
    """Verify a simple shared token for Pub/Sub push (query ?token=... or header X-PubSub-Token)."""
    want = os.environ.get("PUBSUB_TOKEN", "").strip()
    if not want:
        return True  # if not configured, allow (service may be publicly reachable)
    got = (request.args.get("token") or request.headers.get("X-PubSub-Token") or "").strip()
    return bool(got) and (got == want)

def _parse_pubsub_envelope():
    """Parse Pub/Sub push JSON envelope -> (attributes:dict, data:dict). Safe-fail."""
    try:
        env = request.get_json(force=True, silent=True) or {}
        msg = env.get("message") or {}
        attrs = msg.get("attributes") or {}
        data_raw = msg.get("data")
        data = {}
        if data_raw:
            try:
                data = json.loads(base64.b64decode(data_raw).decode("utf-8"))
            except Exception:
                try:
                    data = {"_raw": base64.b64decode(data_raw).decode("utf-8", "ignore")}
                except Exception:
                    data = {"_raw_b64": data_raw}
        return attrs, data
    except Exception as e:
        logger.warning("parse pubsub envelope failed: %s %s", e, _log_ctx())
        return {}, {}

@app.post("/pubsub/promotion-updated")
def pubsub_promotion_updated():
    if not _verify_pubsub_token():
        abort(401, "bad_token")
    attrs, data = _parse_pubsub_envelope()
    shop_id = attrs.get("shop_id") or (data.get("shop_id") if isinstance(data, dict) else None)
    promo_id = attrs.get("promotion_id") or (data.get("promotion_id") if isinstance(data, dict) else None)
    op = attrs.get("op") or (data.get("op") if isinstance(data, dict) else None)
    logger.info("promotion.updated received shop=%s promo=%s op=%s payload=%s", shop_id, promo_id, op, json.dumps(data)[:500])
    return ("", 204)

# --- Product updated Pub/Sub route ---
@app.post("/pubsub/product-updated")
def pubsub_product_updated():
    if not _verify_pubsub_token():
        abort(401, "bad_token")
    attrs, data = _parse_pubsub_envelope()
    shop_id = attrs.get("shop_id") or (data.get("shop_id") if isinstance(data, dict) else None)
    product_id = attrs.get("product_id") or (data.get("product_id") if isinstance(data, dict) else None)
    op = attrs.get("op") or (data.get("op") if isinstance(data, dict) else None)
    logger.info("product.updated received shop=%s product=%s op=%s payload=%s", shop_id, product_id, op, json.dumps(data)[:500])
    # TODO: refresh caches / invalidate storefront listings if needed
    return ("", 204)


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
    logger.info("WEBHOOK recv: headers=%s", dict(request.headers))
    logger.info("WEBHOOK body: %s", request.get_data(as_text=True))
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
    oa_ctx = _resolve_oa_context(line_oa_id, settings)  # 'admin' or 'consumer'
    logger.info("oa_ctx=%s destination=%s shop=%s", oa_ctx, line_oa_id, shop_id)

    # Resolve credentials per-context (prefer oa_consumer.* when present)
    access_token = _resolve_secret_value(settings, "line_channel_access_token", "sm_line_channel_access_token")
    channel_secret = _resolve_secret_value(settings, "line_channel_secret", "sm_line_channel_secret")
    logger.info(
        "cred pick: ctx=%s dest=%s shop=%s has_token=%s has_secret=%s",
        oa_ctx, line_oa_id, shop_id, bool(access_token), bool(channel_secret)
    )

    if not channel_secret:
        abort(500, "LINE channel secret not configured for this shop")

    expected_sig = _compute_signature(channel_secret, body_bytes)
    if not hmac.compare_digest(signature, expected_sig):
        logger.warning("invalid signature %s", _log_ctx(shop_id=shop_id))
        abort(400, "Invalid signature")

    events: List[Dict[str, Any]] = body.get("events", []) or []
    for ev in events:
        user_id = None
        event_id = None
        message_id = None

        ev_type = ev.get("type")
        msg = ev.get("message") or {}
        postback = ev.get("postback") or {}
        mtype = msg.get("type")
        user_id = (ev.get("source") or {}).get("userId", "")
        event_id = ev.get("webhookEventId") or msg.get("id") or ev.get("replyToken")
        message_id = msg.get("id")
        replyToken = ev.get("replyToken")
        onboarding_handled = False

        # --- Admin postback branch for Task 2 (confirm/edit) ---
        if ev_type == "postback":
            data = (postback.get("data") or "").strip()
            if not data:
                logger.warning(
                    "postback missing data %s",
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                continue
            try:
                is_new = ensure_event_once(shop_id, event_id)
            except Exception as e:
                logger.warning(
                    "ensure_event_once failed(postback): %s %s",
                    e,
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                is_new = True
            if not is_new:
                logger.info(
                    "duplicate postback ignored %s",
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                continue
            if oa_ctx != "admin":
                logger.info(
                    "postback skipped (ctx=%s) %s",
                    oa_ctx,
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                continue
            try:
                params = _parse_qs(data)
            except Exception as e:
                logger.warning(
                    "postback parse failed: %s %s",
                    e,
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                continue

            action = ""
            action_vals = params.get("action") or []
            if action_vals:
                action = (action_vals[0] or "").strip()
            shop_ref_vals = params.get("shop_id") or []
            target_shop = (shop_ref_vals[0] or "").strip() if shop_ref_vals else ""

            if action not in ("register_confirm", "register_edit"):
                logger.info(
                    "postback ignored action=%s %s",
                    action,
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                continue
            if not replyToken:
                logger.warning(
                    "postback missing replyToken %s",
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                continue
            if not access_token or not LineBotApi or not TextSendMessage:
                logger.warning(
                    "postback reply unavailable (token/sdk missing) %s",
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                continue
            try:
                api = LineBotApi(access_token)
            except Exception as e:
                logger.warning(
                    "postback api init failed: %s %s",
                    e,
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                continue

            if action == "register_confirm":
                reply_text = (
                    "ขอบคุณค่ะ 🎉 MIA ได้รับข้อมูลของร้านคุณเรียบร้อยแล้ว "
                    "กำลังดำเนินการเปิดใช้งานให้เร็วที่สุด"
                )
            else:
                reply_text = (
                    "หากต้องการแก้ไขข้อมูล แจ้งรายละเอียดที่ต้องการแก้ในแชตนี้ได้เลยค่ะ "
                    "ทีม MIA จะช่วยอัปเดตให้ 🙌"
                )

            try:
                api.reply_message(replyToken, TextSendMessage(text=reply_text))
                logger.info(
                    "admin register postback handled action=%s target_shop=%s %s",
                    action,
                    target_shop or shop_id,
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
            except Exception as e:
                logger.warning(
                    "postback reply failed action=%s err=%s %s",
                    action,
                    e,
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
            # postback already handled; go to next event
            continue

        # --- Non-postback branch; must be a message ---
        if ev_type != "message":
            logger.info(
                "skip non-message %s %s",
                ev_type,
                _log_ctx(shop_id, user_id, event_id, message_id),
            )
            continue

        # Must be a message event from this point forward
        if not user_id:
            logger.warning(
                "missing userId %s",
                _log_ctx(shop_id, None, event_id, message_id),
            )
            continue

        # Event idempotency check
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
        if not owner:
            try:
                bound = _auto_bind_owner_if_needed(shop_id, user_id, settings)
                if bound:
                    owner = True
                    logger.info("auto-bind owner success %s", _log_ctx(shop_id, user_id, event_id, message_id))
            except Exception as e:
                logger.warning("auto-bind failed: %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))

        api = None
        if access_token and LineBotApi:
            try:
                api = LineBotApi(access_token)
            except Exception as e:
                logger.warning("LineBotApi init failed: %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))

        def _reply_line(messages):
            if not replyToken or not api:
                logger.warning("skip reply (LINE client unavailable) %s", _log_ctx(shop_id, user_id, event_id))
                return False
            try:
                msg_list = messages if isinstance(messages, list) else [messages]
                api.reply_message(replyToken, msg_list)
                return True
            except Exception as err:
                logger.warning("reply failed: %s %s", err, _log_ctx(shop_id, user_id, event_id, message_id))
                return False

        def _qr_text(msg: str, add_online: bool = False):
            if not TextSendMessage:
                return None
            if QuickReply and QuickReplyButton and MessageAction:
                items = [QuickReplyButton(action=MessageAction(label="ยกเลิก", text="ยกเลิก"))]
                if add_online:
                    items.append(QuickReplyButton(action=MessageAction(label="ร้านออนไลน์", text="ร้านออนไลน์")))
                return TextSendMessage(text=msg, quick_reply=QuickReply(items=items))
            return TextSendMessage(text=msg)

        def _reply_text_simple(message: str) -> bool:
            if not TextSendMessage:
                return False
            return _reply_line(TextSendMessage(text=message))

        def _send_onboarding_summary(session: Dict[str, Any]) -> bool:
            try:
                summary = to_flex_summary(session)
            except Exception as err:
                logger.warning(
                    "build onboarding summary failed: %s %s",
                    err,
                    _log_ctx(shop_id, user_id, event_id, message_id),
                )
                summary = None
            if not summary:
                return False
            if FlexSendMessage and summary.get("contents"):
                try:
                    flex_msg = FlexSendMessage(
                        alt_text=summary.get("altText") or "สรุปข้อมูลร้านค้า",
                        contents=summary["contents"],
                    )
                    _reply_line(flex_msg)
                    return True
                except Exception as err:
                    logger.warning(
                        "send onboarding flex failed: %s %s",
                        err,
                        _log_ctx(shop_id, user_id, event_id, message_id),
                    )
            if TextSendMessage:
                fallback_msg = TextSendMessage(
                    text="รับข้อมูลครบแล้วค่ะ ตรวจสอบรายละเอียดให้เรียบร้อย จากนั้นกดปุ่ม “ยืนยันข้อมูล” หรือ “แก้ไขข้อมูล” ได้เลยนะคะ"
                )
                _reply_line(fallback_msg)
                return True
            return False

        # ensure customer exists/updated (also fetch LINE profile once)
        try:
            prof = _fetch_line_profile(access_token, user_id)
            display_name = prof.get("display_name")
            if display_name:
                logger.info("fetched profile %s display_name=%s", _log_ctx(shop_id, user_id, event_id, message_id), display_name)
            upsert_customer(shop_id, user_id, display_name=display_name)
        except Exception as e:
            logger.warning("upsert_customer failed %s %s", e, _log_ctx(shop_id, user_id, event_id, message_id))

        text = (msg.get("text") or "").strip()
        low_txt = text.lower()
        sess: Dict[str, Any] = {}
        step = 0
        if not owner:
            try:
                sess = get_session(user_id) or {}
                step = int(sess.get("step") or 0)
                if user_id and sess and not sess.get("messaging_user_id"):
                    sess["messaging_user_id"] = user_id
                    save_session(user_id, sess)
            except Exception as e:
                logger.warning("onboarding get_session failed: %s %s", e, _log_ctx(shop_id, user_id))
                sess = {}
                step = 0
        low_txt = (text or "").strip().lower()
        start_keywords = ("เริ่มต้นใช้งาน", "เริ่มต้นเปิดร้านของคุณ")
        is_admin_start = (oa_ctx == "admin") and any(k in low_txt for k in start_keywords)
        logger.info("DEBUG owner-flow ctx=%s owner=%s step=%s is_admin_start=%s",
                    oa_ctx, bool(owner), step, is_admin_start)
        owner_prompt_msg = None

        if (
            oa_ctx == "admin"
            and not owner
            and is_admin_start
            and ev_type == "message"
            and mtype == "text"
            and not onboarding_handled
        ):
            try:
                sess = get_session(user_id) or {}
            except Exception as err:
                logger.warning(
                    "get_session failed on admin onboarding start: %s %s",
                    err,
                    _log_ctx(shop_id=shop_id, user_id=user_id, event_id=event_id, message_id=message_id),
                )
                sess = {}

            sess.update({
                "step": 1,
                "messaging_user_id": user_id,
            })
            try:
                save_session(user_id, sess)
            except Exception as err:
                logger.warning(
                    "save_session failed on admin onboarding start: %s %s",
                    err,
                    _log_ctx(shop_id=shop_id, user_id=user_id, event_id=event_id, message_id=message_id),
                )

            start_text = (
                "ยินดีต้อนรับสู่ MIA ค่ะ 🎉\n"
                "เราจะช่วยเก็บข้อมูลร้านของคุณ 5 ขั้นตอนสั้น ๆ นะคะ\n\n"
                "ขั้นที่ 1/5: กรุณาพิมพ์ *ชื่อ - นามสกุลผู้ติดต่อ* ของคุณตอบกลับมาได้เลยค่ะ"
            )

            if TextSendMessage and replyToken:
                sent = _reply_line(TextSendMessage(text=start_text))
                if sent:
                    logger.info(
                        "admin onboarding start replied %s",
                        _log_ctx(shop_id=shop_id, user_id=user_id, event_id=event_id, message_id=message_id),
                    )
                else:
                    logger.warning(
                        "admin onboarding start reply helper failed %s",
                        _log_ctx(shop_id=shop_id, user_id=user_id, event_id=event_id, message_id=message_id),
                    )
            else:
                logger.warning(
                    "missing TextSendMessage or replyToken on admin onboarding start %s",
                    _log_ctx(shop_id=shop_id, user_id=user_id, event_id=event_id, message_id=message_id),
                )

            onboarding_handled = True
            continue

        # --- Auto-owner bootstrap via "เริ่มต้นใช้งาน" on consumer OA ---
        try:
            t_start = (text or "").strip()
        except Exception:
            t_start = ""
        try:
            is_admin_start = bool(is_admin_start)
        except Exception:
            is_admin_start = False

        if (oa_ctx == "consumer") and (t_start in ("เริ่มต้นใช้งาน", "เริ่มต้น", "start", "Start")) and (not is_admin_start):
                # 1) Upsert owners/{user_id} เป็น active owner
                try:
                    add_owner_user(
                        shop_id,
                        user_id,
                        roles=["owner"],
                        source="start_keyword",
                        local_owner_user_id=user_id,
                    )
                    _mark_primary_owner_if_missing(shop_id, user_id)
                except Exception as _own_err:
                    logger.warning("auto-add owner failed: %s %s", _own_err, _log_ctx(shop_id=shop_id, user_id=user_id))

                # 2) สร้างลิงก์ owner-invite (magic) แล้วห่อด้วย LIFF global
                try:
                    token, _jti, _exp = _sign_owner_invite(shop_id)
                    boot_url = _build_owner_invite_url(shop_id, token)  # {ADMIN_BASE_URL}/owner/auth/liff/boot?sid=...&token=...

                    global_liff_id = (os.getenv("GLOBAL_LIFF_ID") or "").strip()
                    deep_link = (f"https://liff.line.me/{global_liff_id}?next={_q(boot_url, safe='')}"
                                 if global_liff_id else boot_url)

                    # ใช้ access token ของ OA ฝั่ง consumer (ต้องตรงกับ destination ตอนนี้) ในการส่งข้อความ
                    token_for_push = _resolve_secret_value(settings, "line_channel_access_token", "sm_line_channel_access_token")
                    if token_for_push and LineBotApi and TextSendMessage:
                        api_tmp = LineBotApi(token_for_push)
                        msg_intro = TextSendMessage(text="✅ เชื่อมสิทธิ์เจ้าของร้านเรียบร้อยแล้วครับ\nกดลิงก์ด้านล่างเพื่อเปิดหน้าจัดการร้านของคุณ")
                        msg_link = TextSendMessage(text=deep_link)
                        # ถ้ามี replyToken ใช้ reply ก่อน เพื่อประหยัด quota; ถ้า fail ค่อย push
                        try:
                            if replyToken:
                                api_tmp.reply_message(replyToken, [msg_intro, msg_link])
                            else:
                                api_tmp.push_message(user_id, [msg_intro, msg_link])
                        except Exception:
                            api_tmp.push_message(user_id, [msg_intro, msg_link])
                        logger.info("pushed LIFF owner-invite link to %s shop=%s", user_id, shop_id)
                    else:
                        logger.warning("skip LIFF push: missing consumer token or LINE SDK %s", _log_ctx(shop_id=shop_id, user_id=user_id))
                except Exception as _push_err:
                    logger.warning("push LIFF owner-invite failed: %s %s", _push_err, _log_ctx(shop_id=shop_id, user_id=user_id))

                # จบเคสนี้เพื่อไม่ให้ไปชน handler อื่นซ้ำ
                return "ok", 200

        # fire consumer owner-binding prompt เฉพาะเมื่อ webhook มาจาก consumer OA จริงๆ
        _oc = (settings or {}).get("oa_consumer") or {}
        _consumer_ids = {
            str((_oc.get("bot_user_id") or "")).strip(),
            str((_oc.get("channel_id") or "")).strip(),
        }
        _consumer_ids = {x for x in _consumer_ids if x}
        _dest = (line_oa_id or "").strip()
        _is_consumer_dest = _dest in _consumer_ids

        if not onboarding_handled:
            logger.info(
                "owner_prompt gate: is_consumer_dest=%s oa_ctx=%s shop=%s dest=%s",
                _is_consumer_dest, oa_ctx, shop_id, _dest
            )
            # --- Owner invite flow when message comes via consumer OA ---
            try:
                _t = (text or "").strip()
            except Exception:
                _t = text if isinstance(text, str) else ""

            start_words = {"เริ่มต้นใช้งาน", "เริ่มต้นใช้ งาน", "start", "Start", "เริ่มต้น ใช้งาน"}

            if oa_ctx == "consumer" and _t in start_words:
                try:
                    # 1) สร้าง magic link สำหรับร้านนี้
                    token, jti, exp = _sign_owner_invite(shop_id)
                    invite_url = _build_owner_invite_url(shop_id, token)

                    # 2) ส่งลิงก์/QR ผ่าน ADMIN OA (MIA)
                    sent, err = _send_owner_invite_message(shop_id, settings, user_id, invite_url)
                    logger.info("owner-invite via consumer trigger shop=%s user=%s sent=%s err=%s",
                                shop_id, user_id, sent, err)

                    # 3) แจ้งยืนยันในห้องแชต consumer เพื่อให้ผู้ใช้รู้ว่ามีลิงก์ถูกส่งไปที่ MIA แล้ว
                    if LineBotApi and TextSendMessage and access_token:
                        try:
                            api = LineBotApi(access_token)  # token ของ OA ร้าน (consumer)
                            ack = (
                                "ลิงก์ยืนยันสิทธิ์เจ้าของร้าน:\n"
                                f"{invite_url}\n\n"
                                "ถ้ากดไม่ได้ ลองสแกน QR ด้านล่างได้ครับ"
                            )
                            api.push_message(user_id, TextSendMessage(text=ack))

                            # แนบ QR ของลิงก์ (ถ้ามี ImageSendMessage)
                            try:
                                from linebot.models import ImageSendMessage
                                qr = f"https://api.qrserver.com/v1/create-qr-code/?size=600x600&data={_q(invite_url, safe='')}"
                                api.push_message(user_id, ImageSendMessage(original_content_url=qr, preview_image_url=qr))
                            except Exception:
                                pass

                            logger.info("consumer fallback: pushed invite_url to user via consumer OA %s",
                                                            _log_ctx(shop_id=shop_id, user_id=user_id))
                        except Exception as _fb_err:
                            logger.warning("consumer fallback push failed: %s %s",
                                        _fb_err, _log_ctx(shop_id=shop_id, user_id=user_id))
                except Exception as _oi_err:
                    logger.warning("consumer owner-invite flow failed: %s %s",
                                _oi_err, _log_ctx(shop_id=shop_id, user_id=user_id))

            if _is_consumer_dest and (oa_ctx == "consumer") and (not owner) and (not is_admin_start):
                asked_flag = bool((sess or {}).get("_asked_owner_bind"))
                if (not asked_flag) and (step == 0) and (mtype == "text") and TextSendMessage:
                        logger.info(
                            "owner_prompt: firing (oa_ctx=%s, asked_flag=%s, step=%s, mtype=%s, text=%s)",
                            oa_ctx, asked_flag, step, mtype, (text or "")[:80]
                        )
                        prompt_txt = "ยืนยันสิทธิ์เจ้าของร้านเพื่อเริ่มใช้งานได้เลยครับ"
                        if QuickReply and QuickReplyButton and MessageAction:
                            qr_items = [
                                QuickReplyButton(action=MessageAction(label="ยืนยันเป็นเจ้าของร้าน", text="ยืนยันเป็นเจ้าของร้าน")),
                                QuickReplyButton(action=MessageAction(label="ยกเลิก", text="ยกเลิก")),
                            ]
                            owner_prompt_msg = TextSendMessage(text=prompt_txt, quick_reply=QuickReply(items=qr_items))
                        else:
                            owner_prompt_msg = TextSendMessage(text=prompt_txt)
                        sess["_asked_owner_bind"] = True
                        try:
                            save_session(user_id, sess)
                        except Exception as e:
                            logger.warning("save_session owner prompt failed: %s %s", e, _log_ctx(shop_id, user_id))
                if owner_prompt_msg and not owner:
                    _reply_line(owner_prompt_msg)

            start_keywords = ("เริ่มต้นใช้งาน", "เริ่มต้นเปิดร้านของคุณ")

            if not owner and mtype == "text":
                if (oa_ctx == "consumer") and _is_consumer_dest and (low_txt == "ยืนยันเป็นเจ้าของร้าน"):
                    try:
                        token, jti, _ = _sign_owner_invite(shop_id)
                        now_utc = datetime.now(timezone.utc)
                        db = get_db()
                        (
                            db.collection("shops").document(shop_id)
                              .collection("magic_links").document(jti)
                        ).set({
                            "scope": "owner_invite",
                            "target_user_id": user_id,
                            "source": "consumer",
                            "issued_at": now_utc,
                            "updated_at": now_utc,
                            "created_via": "consumer",
                        }, merge=True)
                        redirect_url = _build_owner_invite_url(shop_id, token)
                        logger.info(
                            "consumer-owner-bind: issued invite jti=%s shop=%s user=%s",
                            jti, shop_id, user_id
                        )
                        if TextSendMessage and api:
                            msg = TextSendMessage(
                                text=f"แตะลิงก์นี้เพื่อยืนยันสิทธิ์เจ้าของร้านของคุณ:\n{redirect_url}"
                            )
                            if replyToken:
                                _reply_line(msg)
                            else:
                                api.push_message(user_id, msg)
                        else:
                            logger.warning(
                                "consumer-owner-bind: invite jti=%s created but no LINE client available %s",
                                jti, _log_ctx(shop_id, user_id, event_id, message_id)
                            )
                    except Exception as e:
                        logger.warning(
                            "consumer-owner-bind: issue invite failed shop=%s user=%s err=%s",
                            shop_id, user_id, e
                        )
                    onboarding_handled = True

            if (oa_ctx == "admin") and not owner:
                stripped_text = (text or "").strip()

                if low_txt == "ยกเลิก" and step:
                    clear_session(user_id)
                    logger.info("onboarding cancel %s", _log_ctx(shop_id, user_id, event_id, message_id))
                    _reply_text_simple("🙅 ยกเลิกขั้นตอนแล้วค่ะ หากต้องการเริ่มใหม่พิมพ์ “เริ่มต้นใช้งาน” ได้เลยนะคะ")
                    onboarding_handled = True
                    continue

                if mtype == "text" and stripped_text == "แก้ไขข้อมูล":
                    _reply_text_simple("หากต้องการแก้ไขข้อมูล พิมพ์รายละเอียดที่ต้องการเปลี่ยนแปลงในแชทนี้ได้เลยค่ะ ทีม MIA จะช่วยอัปเดตให้ 🙌")
                    onboarding_handled = True
                    continue

                if mtype == "text" and low_txt in ("ยืนยันข้อมูล", "ยืนยัน"):
                    has_payment = bool(sess.get("payment_promptpay") or sess.get("payment_qr_url"))
                    if sess.get("name") and sess.get("phone") and sess.get("shop") and has_payment:
                        req_id = finalize_request_from_session(user_id)
                        logger.info("admin onboarding finalize req=%s %s", req_id, _log_ctx(shop_id, user_id, event_id, message_id))
                        if req_id:
                            _reply_text_simple("✅ ส่งคำขอเปิดร้านเรียบร้อยแล้วค่ะ! ทีมงาน MIA จะติดต่อกลับภายใน 1 วันทำการ 🙌")
                            clear_session(user_id)
                        else:
                            _reply_text_simple("ตอนนี้ยังเก็บข้อมูลไม่ครบค่ะ รบกวนลองเริ่มใหม่อีกครั้งโดยพิมพ์ “เริ่มต้นใช้งาน” ได้เลยนะคะ")
                    else:
                        _reply_text_simple("ตอนนี้ยังเก็บข้อมูลไม่ครบค่ะ รบกวนพิมพ์ “เริ่มต้นใช้งาน” เพื่อเริ่มต้นใหม่อีกครั้งนะคะ")
                    onboarding_handled = True
                    continue

                if step == 1 and mtype == "text" and stripped_text and stripped_text not in start_keywords:
                    sess["name"] = stripped_text
                    sess["step"] = 2
                    save_session(user_id, sess)
                    _reply_text_simple("เยี่ยมเลย! ขั้นที่ 2/5 กรุณาพิมพ์เบอร์มือถือ (เช่น 0812345678) ครับ")
                    onboarding_handled = True
                    continue

                if step == 2 and mtype == "text":
                    phone = _normalize_phone_th(text) or stripped_text
                    if phone and len(phone) >= 9:
                        sess["phone"] = phone
                        sess["step"] = 3
                        save_session(user_id, sess)
                        _reply_text_simple("เรียบร้อยครับ! ขั้นที่ 3/5 ต้องการใช้ชื่อร้านบน LINE OA ว่าอะไรครับ?")
                    else:
                        _reply_text_simple("ขอเป็นเบอร์มือถือ 10 หลักนะครับ เช่น 0812345678")
                    onboarding_handled = True
                    continue

                if step == 3 and mtype == "text" and stripped_text:
                    sess["shop"] = stripped_text
                    sess["step"] = 4
                    save_session(user_id, sess)
                    _reply_text_simple("ขั้นที่ 4/5 แชร์ตำแหน่งร้าน หรือพิมพ์ “ร้านออนไลน์” ถ้าไม่มีหน้าร้านครับ")
                    onboarding_handled = True
                    continue

                location_updated = False
                if step == 4 and mtype == "text" and stripped_text:
                    if low_txt == "ร้านออนไลน์":
                        sess["location"] = {"address": "ร้านออนไลน์", "lat": None, "lng": None}
                    else:
                        sess["location"] = {"address": stripped_text, "lat": None, "lng": None}
                    location_updated = True
                elif step == 4 and mtype == "location":
                    sess["location"] = {
                        "title": msg.get("title") or "ตำแหน่งร้าน",
                        "address": msg.get("address"),
                        "lat": msg.get("latitude"),
                        "lng": msg.get("longitude"),
                    }
                    location_updated = True

                if location_updated:
                    sess["step"] = 5
                    save_session(user_id, sess)
                    _reply_text_simple("รับทราบครับ ✨ ขั้นที่ 5/5 กรุณาส่งช่องทางรับเงินของร้าน สามารถพิมพ์หมายเลข PromptPay/บัญชีธนาคาร หรือส่งรูป QR code ก็ได้เลยครับ")
                    onboarding_handled = True
                    continue

                if step >= 5:
                    if mtype == "image":
                        media = _download_line_content(access_token, message_id)
                        if media:
                            content, ct = media
                            url = upload_payment_qr_bytes(user_id, content, ct)
                            if url:
                                sess["payment_qr_url"] = url
                                sess["step"] = 5
                                save_session(user_id, sess)
                                logger.info("admin onboarding payment_qr stored %s", _log_ctx(shop_id, user_id, event_id, message_id))
                                _send_onboarding_summary(sess)
                            else:
                                _reply_text_simple("อัปโหลดรูปไม่สำเร็จ ลองใหม่อีกครั้งนะครับ")
                        else:
                            _reply_text_simple("ดาวน์โหลดไฟล์ไม่สำเร็จ ลองส่งใหม่อีกครั้งนะครับ")
                        onboarding_handled = True
                        continue

                    if mtype == "text" and stripped_text:
                        stored_payment = False
                        lower_payment_text = stripped_text.lower()
                        if lower_payment_text.startswith(("note", "หมายเหตุ")):
                            payload = stripped_text.split(":", 1)
                            note_val = payload[1].strip() if len(payload) > 1 else stripped_text
                            sess["payment_note"] = note_val or stripped_text
                            stored_payment = True
                        elif not sess.get("payment_promptpay"):
                            sess["payment_promptpay"] = stripped_text
                            stored_payment = True
                        else:
                            sess["payment_note"] = stripped_text
                            stored_payment = True

                        if stored_payment:
                            sess["step"] = 5
                            save_session(user_id, sess)
                            if sess.get("payment_promptpay") or sess.get("payment_qr_url"):
                                if not _send_onboarding_summary(sess):
                                    _reply_text_simple("รับข้อมูลช่องทางรับเงินเรียบร้อยแล้วครับ ตรวจสอบและกดปุ่มยืนยันได้เลยนะครับ")
                            else:
                                _reply_text_simple("รับทราบหมายเหตุแล้วค่ะ รบกวนส่ง PromptPay หรือ QR code เพิ่มเติมด้วยนะคะ")
                        else:
                            _reply_text_simple("กรุณาพิมพ์ช่องทาง PromptPay หรือหมายเหตุเพิ่มเติมนะครับ")
                        onboarding_handled = True
                        continue

                if onboarding_handled:
                    continue

            if mtype == "text":
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
                                    for oid in _resolve_owner_push_targets(shop_id):
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
                _store_customer_last_message(shop_id, user_id, text, _log_ctx(shop_id, user_id, event_id, message_id))
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
                _store_customer_last_message(shop_id, user_id, "<owner location>", _log_ctx(shop_id, user_id, event_id, message_id))
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
                _store_customer_last_message(shop_id, user_id, placeholder, _log_ctx(shop_id, user_id, event_id, message_id))
                logger.info("recv %s %s stored=%s ct=%s", mtype, _log_ctx(shop_id, user_id, event_id, message_id), bool(media_info), content_type)
                continue

            # ignore other message types for now
            logger.info("skip message type=%s %s", mtype, _log_ctx(shop_id, user_id, event_id, message_id))

        #except Exception as e:
            # Ensure exception object is converted to string for formatting
            #logger.exception("event processing error: %s %s", str(e), _log_ctx(shop_id, user_id, event_id, message_id))

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
            for oid in _resolve_owner_push_targets(shop_id):
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
        owners = _resolve_owner_push_targets(shop_id)
        if owners and access_token and LineBotApi:
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
# === Pub/Sub push endpoints (consumer) ===
# These endpoints allow Cloud Pub/Sub to push changes from admin to consumer.
# They respond with 204 to prevent Pub/Sub retries once processed.

from flask import request, abort  # ensure available in this scope
import os as _os_pubsub
import json as _json_pubsub
import base64 as _b64_pubsub
import logging as _logging_pubsub
from firestore_client import get_db as _get_db_pubsub
from firebase_admin import firestore as _fb_pubsub

_pub_logger = _logging_pubsub.getLogger("lineoa-frontend-mt")

def _require_pubsub_token():
    want = _os_pubsub.environ.get("PUBSUB_TOKEN", "")
    got = (request.args.get("token") or request.headers.get("X-PubSub-Token") or "").strip()
    if not want or got != want:
        abort(401, "bad token")

def _parse_pubsub_envelope():
    env = request.get_json(silent=True) or {}
    msg = env.get("message") or {}
    attrs = msg.get("attributes") or {}
    payload = {}
    data_b64 = msg.get("data")
    if data_b64:
        try:
            payload = _json_pubsub.loads(_b64_pubsub.b64decode(data_b64).decode("utf-8"))
        except Exception as e:
            _pub_logger.warning("pubsub decode error: %s", e)
    return attrs, payload

@app.post("/pubsub/promotion-updated", endpoint="pubsub_promotion_updated_v2")
def pubsub_promotion_updated_v2():
    _require_pubsub_token()
    attrs, payload = _parse_pubsub_envelope()
    shop_id = attrs.get("shop_id") or _os_pubsub.getenv("DEFAULT_SHOP_ID", "")
    promo_id = attrs.get("promotion_id")
    op = (attrs.get("op") or "upsert").lower()

    if not shop_id or not promo_id:
        return ("", 204)

    db = _get_db_pubsub()
    ref = db.collection("shops").document(shop_id).collection("promotions").document(promo_id)
    try:
        if op == "delete":
            ref.delete()
        else:
            payload = (payload or {})
            payload["updated_at"] = _fb_pubsub.SERVER_TIMESTAMP
            # set merge=True to avoid overwriting existing fields unintentionally
            ref.set(payload, merge=True)
        _pub_logger.info("promotion.updated handled: shop=%s id=%s op=%s", shop_id, promo_id, op)
    except Exception as e:
        _pub_logger.error("promotion.updated error: %s shop=%s id=%s", e, shop_id, promo_id)
    return ("", 204)

@app.post("/pubsub/product-updated", endpoint="pubsub_product_updated_v2")
def pubsub_product_updated_v2():
    _require_pubsub_token()
    attrs, payload = _parse_pubsub_envelope()
    shop_id = attrs.get("shop_id") or _os_pubsub.getenv("DEFAULT_SHOP_ID", "")
    product_id = attrs.get("product_id")
    op = (attrs.get("op") or "upsert").lower()

    if not shop_id or not product_id:
        return ("", 204)

    db = _get_db_pubsub()
    ref = db.collection("shops").document(shop_id).collection("products").document(product_id)
    try:
        if op == "delete":
            ref.delete()
        else:
            payload = (payload or {})
            payload["updated_at"] = _fb_pubsub.SERVER_TIMESTAMP
            ref.set(payload, merge=True)
        _pub_logger.info("product.updated handled: shop=%s id=%s op=%s", shop_id, product_id, op)
    except Exception as e:
        _pub_logger.error("product.updated error: %s shop=%s id=%s", e, shop_id, product_id)
    return ("", 204)
