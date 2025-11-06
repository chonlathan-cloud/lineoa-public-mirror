# admin/blueprint.py — routes for B (owner) via OA ของ A
from flask import Blueprint, request, jsonify, render_template, abort
from jinja2 import TemplateNotFound
from flask import render_template_string
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List
import logging
import jwt
import time
from flask import make_response, redirect
import json
import requests
import re
try:
    from services.firestore_client import get_db
except Exception:
    from firestore_client import get_db
from google.cloud import firestore
try:
    # FieldPath อยู่ใน firestore_v1
    from google.cloud.firestore_v1 import FieldPath as _FieldPath
except Exception:
    _FieldPath = None
from io import BytesIO
from google.cloud import storage
from google.cloud import pubsub_v1
from dateutil import parser as _dtparser
import os
import traceback
import uuid
from urllib.parse import quote, urlparse, parse_qs
# --- core shared modules (do not import consumer/admin crosswise) ---
from core.line_events import check_signature as core_check_sig, extract_event_fields as core_extract, ensure_event_once as core_event_once
from core.secrets import load_shop_context_by_destination as core_load_ctx, resolve_secret as core_resolve_secret
from core.media import download_line_content as core_dl_content, store_media as core_store_media
from core.owners import upsert_owner_profile_from_text as core_owner_upsert, fetch_line_profile as core_fetch_profile
from core.payments import parse_payment_intent as core_parse_intent, create_or_attach_intent as core_create_intent, confirm_latest_pending_intent as core_confirm_intent, reject_latest_pending_intent as core_reject_intent
from werkzeug.utils import secure_filename
try:
    from linebot import LineBotApi
    from linebot.models import TextSendMessage, ImageSendMessage
except Exception:
    LineBotApi = None  # optional
    TextSendMessage = None
    ImageSendMessage = None
try:
    from dao import get_shop_settings, get_shop  # reuse DAO helpers when available
except Exception:
    get_shop_settings = None
    get_shop = None
# Resolve renderers: **lock to approved file** to guarantee data logic matches the meeting-approved version.
try:
    from report_renderer import build_mini_report_pdf as render_mini_report_pdf
    from report_renderer import build_report_pdf_v3 as render_full_report_pdf
except Exception:
    # If heavy deps (e.g., matplotlib) are missing, keep running and fall back later.
    render_mini_report_pdf = None
    render_full_report_pdf = None

_ADMIN_BASE_DEFAULT = "https://lineoa-admin-250878482242.asia-southeast1.run.app"
_CONSUMER_BASE_DEFAULT = "https://lineoa-consumer-250878482242.asia-southeast1.run.app"
ADMIN_BASE_URL = (os.getenv("ADMIN_BASE_URL") or os.getenv("OWNER_PORTAL_BASE_URL") or _ADMIN_BASE_DEFAULT).rstrip("/")
CONSUMER_BASE_URL = (os.getenv("CONSUMER_BASE_URL") or _CONSUMER_BASE_DEFAULT).rstrip("/")

CONSUMER_WEBHOOK_URL = f"{CONSUMER_BASE_URL}/line/webhook"

# --- Admin OA token for owner invite ---
ADMIN_LINE_TOKEN = (os.getenv("ADMIN_LINE_CHANNEL_ACCESS_TOKEN") or "").strip()

# --- Helper: fetch bot info v2 ---
def _fetch_bot_info_v2(access_token: str) -> Dict[str, Any]:
    """
    Call LINE Bot Info API using the given channel access token and return a dict.
    Returns {} on any failure.
    """
    if not access_token:
        return {}
    try:
        import urllib.request, json as _json
        req = urllib.request.Request("https://api.line.me/v2/bot/info")
        req.add_header("Authorization", f"Bearer {access_token}")
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return _json.loads(raw) if raw else {}
    except Exception as e:
        logging.getLogger("admin-oa-new").warning("bot info fetch failed: %s", e)
        return {}

# --- Fallback PDF stub ---
def _fallback_pdf_stub(shop_id: str, start_dt, end_dt) -> bytes:
    """Tiny PDF using ReportLab when WeasyPrint/Jinja2/matplotlib are unavailable."""
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import cm
    except Exception:
        # ultra-minimal PDF if ReportLab is not importable (should not happen; it's in requirements)
        return (b"%PDF-1.3\n%\xe2\xe3\xcf\xd3\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
                b"2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
                b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 595 842]/Contents 4 0 R>>endobj\n"
                b"4 0 obj<</Length 44>>stream\nBT /F1 12 Tf 72 770 Td (Report temporarily unavailable) Tj ET\n"
                b"endstream endobj\ntrailer<</Root 1 0 R>>\n%%EOF")
    import io
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    c.setFont("Helvetica", 16)
    c.drawString(2*cm, 27*cm, "Customer Insight Report (Fallback)")
    c.setFont("Helvetica", 11)
    try:
        period = f"{start_dt.isoformat()} \u2192 {end_dt.isoformat()}"
    except Exception:
        period = ""
    c.drawString(2*cm, 25.8*cm, f"Shop: {shop_id}")
    if period:
        c.drawString(2*cm, 25.0*cm, f"Period: {period}")
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, 23.5*cm, "Charts/WeasyPrint unavailable on this runtime.")
    c.drawString(2*cm, 22.8*cm, "This is a lightweight PDF stub so you can proceed.")
    c.showPage()
    c.save()
    return buf.getvalue()


def _resolve_shop_display_name(shop_id: Optional[str]) -> Optional[str]:
    """Return the human-friendly shop name for UI surfaces."""
    if not shop_id:
        return None

    settings: Dict[str, Any] = {}
    if callable(get_shop_settings):
        try:
            data = get_shop_settings(shop_id)
            if isinstance(data, dict):
                settings = data
        except Exception:
            settings = {}
    if not settings:
        try:
            snap = (
                get_db().collection("shops")
                .document(shop_id).collection("settings").document("default").get()
            )
            if snap.exists:
                data = snap.to_dict()
                if isinstance(data, dict):
                    settings = data
        except Exception:
            settings = {}

    def _pick_name(source: Optional[Dict[str, Any]], keys) -> Optional[str]:
        if not isinstance(source, dict):
            return None
        for key in keys:
            val = source.get(key)
            if isinstance(val, str):
                name = val.strip()
                if name:
                    return name
        return None

    name = _pick_name(settings, ("oa_display_name", "display_name", "name"))
    if name:
        return name
    consumer = settings.get("oa_consumer") if isinstance(settings, dict) else None
    name = _pick_name(consumer if isinstance(consumer, dict) else None, ("display_name", "oa_display_name", "name"))
    if name:
        return name

    shop_meta: Dict[str, Any] = {}
    if callable(get_shop):
        try:
            data = get_shop(shop_id)
            if isinstance(data, dict):
                shop_meta = data
        except Exception:
            shop_meta = {}
    if not shop_meta:
        try:
            snap = get_db().collection("shops").document(shop_id).get()
            if snap.exists:
                data = snap.to_dict()
                if isinstance(data, dict):
                    shop_meta = data
        except Exception:
            shop_meta = {}

    name = _pick_name(shop_meta, ("oa_display_name", "display_name", "name"))
    if name:
        return name
    return None

admin_bp = Blueprint("admin", __name__, template_folder="templates", static_folder="static")

# --- Magic Link (JWT) config ---
MAGIC_LINK_SECRET_ENV = "MAGIC_LINK_SECRET"
MAGIC_LINK_TTL_MIN_ENV = "MAGIC_LINK_TTL_MIN"
OWNER_SESSION_COOKIE = "owner_session_sid"
OWNER_SESSION_DAYS = 7  # cookie lifetime days

def _get_magic_secret() -> str:
    secret = os.getenv(MAGIC_LINK_SECRET_ENV)
    if not secret:
        # For dev, allow fallback to Flask secret; warn in logs
        try:
            logger = logging.getLogger("admin-magic")
            logger.warning("MAGIC_LINK_SECRET not set — falling back to FLASK_SECRET_KEY (dev only)")
        except Exception:
            pass
        secret = os.getenv("FLASK_SECRET_KEY", "")
        if not secret:
            raise RuntimeError("MAGIC_LINK_SECRET not configured")
    return secret

def _sign_magic_token(shop_id: str, scope: str = "owner_form", ttl_min: int | None = None) -> tuple[str, str, int]:
    secret = _get_magic_secret()
    if ttl_min is None:
        try:
            ttl_min = int(os.getenv(MAGIC_LINK_TTL_MIN_ENV, "10080"))  # default 7 days
        except Exception:
            ttl_min = 10080
    jti = uuid.uuid4().hex
    now = int(time.time())
    exp = now + max(1, int(ttl_min)) * 60
    payload = {"shop_id": shop_id, "scope": scope, "iat": now, "exp": exp, "jti": jti}
    token = jwt.encode(payload, secret, algorithm="HS256")
    # Optional: persist jti for revoke
    try:
        db = get_db()
        db.collection("shops").document(shop_id).collection("magic_links").document(jti).set({
            "scope": scope,
            "exp": exp,
            "created_at": datetime.now(timezone.utc),
            "revoked": False,
        }, merge=False)
    except Exception:
        pass
    return token, jti, exp

def _sign_owner_invite(shop_id: str, ttl_min: int | None = None) -> tuple[str, str, int]:
    token, jti, exp = _sign_magic_token(shop_id, scope="owner_invite", ttl_min=ttl_min)
    return token, jti, exp

def _line_bot_api_for_shop(shop_id: str, settings: Optional[Dict[str, Any]] | None = None):
    if not LineBotApi:
        return None
    try:
        if settings is None:
            snap = (
                get_db().collection("shops")
                .document(shop_id).collection("settings").document("default").get()
            )
            settings = snap.to_dict() if snap.exists else {}
        # Prefer direct values in settings/default (no Secret Manager indirection)
        consumer_cfg = (settings or {}).get("oa_consumer") or (settings or {})
        token = (
            (consumer_cfg.get("line_channel_access_token") or (settings or {}).get("line_channel_access_token"))
        )
        if not token:
            return None
        return LineBotApi(token)
    except Exception as e:
        logging.getLogger("admin-invite").warning("line bot init failed shop=%s err=%s", shop_id, e)
        return None

def _build_owner_invite_url(shop_id: str, token: str) -> str:
    return f"{ADMIN_BASE_URL}/owner/auth/liff/boot?sid={shop_id}&token={token}"

# --- Helper: build add-friend link for consumer OA using basic_id ---
def _build_consumer_add_friend_link(settings: Optional[Dict[str, Any]]) -> Optional[str]:
    """Return public add-friend URL for the consumer OA using basic_id, if available."""
    def _normalize_basic_id(raw: Optional[str]) -> Optional[str]:
        val = (raw or "").strip()
        if not val:
            return None
        if val.startswith("@"):
            val = val[1:]
        return val or None

    try:
        cfg = (settings or {}).get("oa_consumer")
        if isinstance(cfg, dict):
            basic_id = _normalize_basic_id(cfg.get("basic_id"))
            if basic_id:
                return f"https://page.line.me/{basic_id}"
        basic_id = _normalize_basic_id((settings or {}).get("basic_id"))
        if basic_id:
            return f"https://page.line.me/{basic_id}"
    except Exception:
        pass
    return None

def _send_owner_invite_message(shop_id, settings, target_user_id, invite_url):
    if not target_user_id:
        return False, "missing_target_user"

    logger = logging.getLogger("admin-invite")

    # Force use admin OA (MIA)
    token = (ADMIN_LINE_TOKEN or os.getenv("ADMIN_LINE_CHANNEL_ACCESS_TOKEN", "").strip())
    if not token:
        logger.warning("owner invite push skipped: missing ADMIN_LINE_CHANNEL_ACCESS_TOKEN")
        return False, "missing_admin_token"

    api = LineBotApi(token) if LineBotApi else None
    if not api or not TextSendMessage:
        return False, "linebot_unavailable"

    # Diagnostic: verify friendship/profile with this token first
    try:
        prof = api.get_profile(target_user_id)
        logger.info(
            "owner invite push using ADMIN token prefix=%s user=%s display=%s",
            (token[:8] + "…"), target_user_id, getattr(prof, "display_name", None)
        )
    except Exception as e:
        logger.warning("admin token cannot get profile user=%s err=%s", target_user_id, e)
        # Continue anyway; some channels may restrict profile but still allow push

    try:
        add_friend_url = _build_consumer_add_friend_link(settings)
        messages: List[Any] = []
        has_link = bool(add_friend_url)
        has_qr = False
        if add_friend_url and TextSendMessage:
            messages.append(TextSendMessage(text=add_friend_url))
        if add_friend_url and ImageSendMessage:
            from urllib.parse import quote as _q
            qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=600x600&data={_q(add_friend_url, safe='')}"
            messages.append(ImageSendMessage(original_content_url=qr_url, preview_image_url=qr_url))
            has_qr = True
        thai_copy = (
            "เพิ่มเพื่อน LINE OA ของร้านคุณจากลิงก์/QR นี้ แล้วพิมพ์ “เริ่มต้นใช้งาน”\n"
            "เพื่อยืนยันสิทธิ์เจ้าของร้านในแชตของร้าน"
        )
        if TextSendMessage:
            if has_link or has_qr:
                messages.append(TextSendMessage(text=thai_copy))
            else:
                fallback = (
                    "กรุณาเพิ่มเพื่อน LINE OA ของร้านคุณ แล้วพิมพ์ “เริ่มต้นใช้งาน”\n"
                    "(ผู้ดูแลระบบยังไม่พบ basic_id ของ OA ร้านนี้)"
                )
                messages.append(TextSendMessage(text=fallback))
                logger.info("admin-invite: basic_id missing; sent fallback text only")
        if not messages:
            return False, "no_messages"
        logger.info(
            "admin-invite: first-push consumer add-friend link present=%s qr=%s",
            has_link, has_qr
        )
        api.push_message(target_user_id, messages)
        return True, None
    except Exception as e:
        logger.warning(
            "push owner invite failed via ADMIN shop=%s user=%s err=%s",
            shop_id, target_user_id, e,
        )
        return False, str(e)

def _verify_owner_invite_token(raw_token: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    log = logging.getLogger("admin-auth")
    secret = _get_magic_secret()
    try:
        data = jwt.decode(raw_token, secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None, "invite_expired"
    except Exception as e:
        log.warning("owner invite token decode failed: %s", e)
        return None, "invite_invalid"

    if data.get("scope") != "owner_invite":
        return None, "invite_bad_scope"
    shop_id = data.get("shop_id")
    jti = data.get("jti")
    if not shop_id or not jti:
        return None, "invite_missing_fields"
    db = get_db()
    ref = (
        db.collection("shops").document(shop_id)
          .collection("magic_links").document(jti)
    )
    snap = ref.get()
    if not snap.exists:
        return None, "invite_not_found"
    doc = snap.to_dict() or {}
    if doc.get("revoked"):
        return None, "invite_revoked"
    if doc.get("used_at"):
        return None, "invite_used"
    if doc.get("scope") and doc.get("scope") != "owner_invite":
        return None, "invite_scope_mismatch"
    return {
        "shop_id": shop_id,
        "jti": jti,
        "token": raw_token,
        "token_data": data,
        "link_ref": ref,
        "link_doc": doc,
    }, None

def _set_owner_session_cookie(resp, shop_id: str) -> None:
    """Attach owner session cookie to response."""
    max_age = OWNER_SESSION_DAYS * 24 * 60 * 60
    secure_flag = True
    try:
        host = (request.host or "").split(":")[0]
        if host in ("127.0.0.1", "localhost"):
            secure_flag = False
    except Exception:
        pass
    resp.set_cookie(
        OWNER_SESSION_COOKIE,
        shop_id,
        max_age=max_age,
        httponly=True,
        secure=secure_flag,
        samesite="Lax",
        path="/owner"
    )

def _get_owner_session_shop_id() -> Optional[str]:
    """Return shop_id from owner session cookie if present."""
    try:
        return (request.cookies.get(OWNER_SESSION_COOKIE) or "").strip() or None
    except Exception:
        return None

# --- Helper: resolve shop by owner LINE userId using collection group query
def _find_shop_by_owner_user_id(owner_user_id: str) -> Optional[str]:
    log = logging.getLogger("admin-auth")
    db = get_db()

    # --- Prefer explicit owner_shops/{sub}/shops/{shop_id} mapping ---
    try:
        root = db.collection("owner_shops").document(owner_user_id)
        sub_docs = list(root.collection("shops").where("active", "==", True).limit(5).stream())
        for doc in sub_docs:
            parent_id = doc.id
            if parent_id:
                log.debug("OWNER MAP direct hit sub=%s shop=%s", owner_user_id, parent_id)
                return parent_id
    except Exception as e:
        log.debug("OWNER MAP direct lookup failed sub=%s err=%s", owner_user_id, e)

    # --- Primary: collection_group + document_id() (no index needed) ---
    if _FieldPath is not None:
        try:
            # Query by document ID only, then check `active` in Python to avoid requiring an index
            q = (
                db.collection_group("owners")
                  .where(_FieldPath.document_id(), "==", owner_user_id)
                  .limit(5)
            )
            matches = list(q.stream())
            log.error("OWNER MAP DEBUG primary_q hits=%s for sub=%s", len(matches), owner_user_id)
            for doc in matches:
                data = doc.to_dict() or {}
                if bool(data.get("active", True)):
                    shop_ref = doc.reference.parent.parent
                    return shop_ref.id if shop_ref else None
        except Exception as e:
            log.error("OWNER MAP primary failed: %s", e)

    # --- Fallback: stream a small set and match id in code (no index required) ---
    try:
        fq = db.collection_group("owners").limit(500)
        for doc in fq.stream():
            if (doc.id or "").strip() == owner_user_id:
                data = doc.to_dict() or {}
                if bool(data.get("active", True)):
                    shop_ref = doc.reference.parent.parent
                    log.error(
                        "OWNER MAP DEBUG hit via fallback for sub=%s shop=%s",
                        owner_user_id, shop_ref.id if shop_ref else None,
                    )
                    return shop_ref.id if shop_ref else None
    except Exception as e:
        log.error("OWNER MAP fallback error: %s", e)

    return None


def _ensure_owner_record(shop_id: str, owner_sub: str, claims: Optional[dict] = None) -> None:
    """Ensure shops/{shop}/owners/{owner_sub} exists and mark active."""
    if not (shop_id and owner_sub):
        return
    log = logging.getLogger("admin-auth")
    global_channel_id = os.getenv("GLOBAL_LINE_LOGIN_CHANNEL_ID", "").strip()
    try:
        db = get_db()
        doc_ref = db.collection("shops").document(shop_id).collection("owners").document(owner_sub)
        snap = doc_ref.get()
        now = datetime.now(timezone.utc)
        payload: Dict[str, Any] = {
            "active": True,
            "roles": ["owner"],
            "updated_at": now,
            "last_login_at": now,
        }
        if claims:
            verified_aud = claims.get("_verified_audience") or claims.get("aud") or claims.get("azp")
            if verified_aud:
                payload["last_login_channel_id"] = verified_aud
        if global_channel_id and "last_login_channel_id" not in payload:
            payload["last_login_channel_id"] = global_channel_id
        if not snap.exists:
            payload["created_at"] = now
        doc_ref.set(payload, merge=True)

        # Mirror mapping for owner_shops index
        try:
            idx_ref = (
                db.collection("owner_shops")
                  .document(owner_sub)
                  .collection("shops")
                  .document(shop_id)
            )
            idx_payload: Dict[str, Any] = {
                "active": True,
                "linked_at": now,
                "last_login_at": now,
            }
            if global_channel_id:
                idx_payload["last_login_channel_id"] = global_channel_id
            idx_ref.set(idx_payload, merge=True)
        except Exception as idx_err:
            log.debug("ensure_owner_record index update failed shop=%s owner=%s err=%s", shop_id, owner_sub, idx_err)
    except Exception as e:
        log.error("ensure_owner_record failed shop=%s owner=%s err=%s", shop_id, owner_sub, e)

_SHOP_ID_RE = re.compile(r"^shop_(\d+)$")

def _next_shop_id() -> str:
    db = get_db()
    log = logging.getLogger("admin-oa-new")
    latest_seq = 0
    try:
        candidates = list(
            db.collection("shops")
              .order_by("created_at", direction=firestore.Query.DESCENDING)
              .limit(10)
              .stream()
        )
        for doc in candidates:
            match = _SHOP_ID_RE.match(doc.id or "")
            if match:
                latest_seq = int(match.group(1))
                break
    except Exception as e:
        log.debug("next_shop_id primary lookup failed: %s", e)
    if latest_seq == 0:
        try:
            fallback_docs = list(db.collection("shops").limit(200).stream())
            for doc in fallback_docs:
                match = _SHOP_ID_RE.match(doc.id or "")
                if match:
                    latest_seq = max(latest_seq, int(match.group(1)))
        except Exception as e:
            log.debug("next_shop_id fallback lookup failed: %s", e)
    return f"shop_{latest_seq + 1:05d}"
# --- Pub/Sub helper ---
_PUBSUB_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
def _publish(topic: str, attrs: Dict[str, Any] | None = None, data: Dict[str, Any] | None = None) -> None:
    try:
        if not _PUBSUB_PROJECT:
            return
        client = pubsub_v1.PublisherClient()
        topic_path = client.topic_path(_PUBSUB_PROJECT, topic)
        payload = json.dumps(data or {}).encode("utf-8")
        attributes = {k: str(v) for k, v in (attrs or {}).items() if v is not None}
        fut = client.publish(topic_path, payload, **attributes)
        fut.result(timeout=5)
    except Exception:
        # best-effort; อย่าให้กระทบ flow หลัก
        pass

@admin_bp.get("/admin/oa/requests")
def admin_onboarding_requests():
    db = get_db()
    rows: List[Dict[str, Any]] = []
    error: Optional[str] = None
    try:
        base = (
            db.collection("onboarding")
              .document("requests")
              .collection("items")
              .where("status", "==", "pending")
        )
        try:
            docs = list(base.order_by("created_at", direction=firestore.Query.DESCENDING).limit(50).stream())
        except Exception:
            docs = list(base.limit(50).stream())
        for doc in docs:
            data = doc.to_dict() or {}
            created = data.get("created_at")
            if hasattr(created, "isoformat"):
                created = created.isoformat()
            rows.append({
                "id": doc.id,
                "name": data.get("name"),
                "phone": data.get("phone"),
                "shop": data.get("shop"),
                "messaging_user_id": data.get("messaging_user_id") or data.get("user_id"),
                "created_at": created,
            })
    except Exception as exc:
        error = str(exc)
    return render_template("admin_oa_requests.html", requests=rows, error=error)

@admin_bp.post("/admin/oa/migrate-settings")
def admin_migrate_settings():
    db = get_db()
    payload = request.get_json(silent=True) or {}
    shop_id = (payload.get("shop_id")
               or request.form.get("shop_id")
               or request.args.get("shop_id")
               or "").strip()
    if not shop_id:
        return jsonify({"migrated": False, "error": "missing_shop_id"}), 400
    root_ref = db.collection("shops").document(shop_id)
    snap = root_ref.get()
    if not snap.exists:
        return jsonify({"migrated": False, "error": "shop_not_found"}), 404
    data = snap.to_dict() or {}
    settings_map = data.get("settings")
    if not isinstance(settings_map, dict) or not settings_map:
        return jsonify({"migrated": False, "reason": "no_root_settings"}), 200
    settings_ref = root_ref.collection("settings").document("default")
    settings_ref.set(settings_map, merge=True)
    try:
        root_ref.update({"settings": firestore.DELETE_FIELD})
    except Exception as e:
        logging.getLogger("admin-migrate").warning("remove root settings failed shop=%s err=%s", shop_id, e)
    return jsonify({"migrated": True})

@admin_bp.route("/admin/oa/new", methods=["GET", "POST"])
def admin_create_oa():
    log = logging.getLogger("admin-oa-new")
    env_info = {
        "GLOBAL_LIFF_ID": os.getenv("GLOBAL_LIFF_ID", "").strip(),
        "LIFF_ID_REPORT": os.getenv("LIFF_ID_REPORT", "").strip(),
        "LIFF_ID_PROMOTION": os.getenv("LIFF_ID_PROMOTION", "").strip(),
        "MEDIA_BUCKET": os.getenv("MEDIA_BUCKET", "").strip(),
        "REPORT_BUCKET": os.getenv("REPORT_BUCKET", "").strip(),
        "GLOBAL_LINE_LOGIN_CHANNEL_ID": os.getenv("GLOBAL_LINE_LOGIN_CHANNEL_ID", "").strip(),
    }
    result: Optional[Dict[str, Any]] = None
    errors: List[str] = []
    db = get_db()
    now = datetime.now(timezone.utc)
    prefill_id = (request.form.get("prefill_id") or request.args.get("prefill") or "").strip()
    prefill_data: Optional[Dict[str, Any]] = None
    if prefill_id:
        try:
            req_snap = (
                db.collection("onboarding")
                  .document("requests")
                  .collection("items")
                  .document(prefill_id)
                  .get()
            )
            if req_snap.exists:
                prefill_data = req_snap.to_dict() or {}
            else:
                errors.append("ไม่พบคำขอ onboarding ที่เลือกไว้")
        except Exception as exc:
            errors.append(f"ไม่สามารถอ่านข้อมูลสำหรับ prefill ได้: {exc}")
    default_form = {
        "channel_id": "",
        "oa_display_name": "",
        "line_oa_id": "",
    }
    form = default_form.copy()

    if request.method == "GET" and prefill_data:
        form["oa_display_name"] = prefill_data.get("shop") or ""

    if request.method == "POST":
        channel_id = (request.form.get("channel_id") or "").strip()
        oa_display_name = (request.form.get("oa_display_name") or "").strip()
        line_access_token = (request.form.get("line_channel_access_token") or "").strip()
        line_channel_secret = (request.form.get("line_channel_secret") or "").strip()
        line_oa_id = (request.form.get("line_oa_id") or "").strip()

        form.update({
            "channel_id": channel_id,
            "oa_display_name": oa_display_name,
            "line_oa_id": line_oa_id,
        })

        if not channel_id:
            errors.append("Channel ID is required.")
        if not line_access_token:
            errors.append("LINE channel access token is required.")
        if not line_channel_secret:
            errors.append("LINE channel secret is required.")

        try:
            int(channel_id)
        except Exception:
            errors.append("Channel ID must be numeric.")

        media_bucket = env_info.get("MEDIA_BUCKET")
        report_bucket = env_info.get("REPORT_BUCKET")

        if not errors:
            try:
                now = datetime.now(timezone.utc)
                shop_id = _next_shop_id()
                channel_id_str = str(channel_id)

                oa_consumer: Dict[str, Any] = {
                    "line_channel_access_token": line_access_token,
                    "line_channel_secret": line_channel_secret,
                }
                settings_payload: Dict[str, Any] = {
                    "oa_consumer": oa_consumer,
                    "media_bucket": media_bucket or "lineoa-media-dev",
                    "report_bucket": report_bucket or "lineoa-report-for-owner",
                }

                # --- Auto-populate bot identity from /v2/bot/info ---
                bot_info = _fetch_bot_info_v2(line_access_token)
                bot_user_id = ""
                try:
                    if bot_info:
                        bot_user_id = (bot_info.get("userId") or "").strip()
                        basic_id = (bot_info.get("basicId") or "").strip()
                        display_name = (bot_info.get("displayName") or "").strip()
                        picture_url = (bot_info.get("pictureUrl") or "").strip()
                        if basic_id:
                            oa_consumer["basic_id"] = basic_id
                        if picture_url:
                            oa_consumer["picture_url"] = picture_url
                        if display_name:
                            oa_consumer["display_name"] = display_name
                        if not form.get("oa_display_name"):
                            form["oa_display_name"] = display_name
                except Exception as _e:
                    logging.getLogger("admin-oa-new").warning("populate bot info failed: %s", _e)

                final_display_name = (form.get("oa_display_name") or oa_display_name or "").strip()
                if final_display_name:
                    settings_payload["oa_display_name"] = final_display_name

                root_ref = db.collection("shops").document(shop_id)
                root_exists = root_ref.get().exists
                meta_payload: Dict[str, Any] = {
                    "channel_id": channel_id_str,
                    "line_oa_id": channel_id_str,
                    "updated_at": firestore.SERVER_TIMESTAMP,
                }
                if bot_user_id:
                    meta_payload["bot_user_id"] = bot_user_id
                meta_payload["settings"] = firestore.DELETE_FIELD
                meta_payload["oa_display_name"] = firestore.DELETE_FIELD
                if not root_exists:
                    meta_payload["created_at"] = firestore.SERVER_TIMESTAMP
                root_ref.set(meta_payload, merge=True)

                settings_ref = root_ref.collection("settings").document("default")
                settings_ref.set(settings_payload, merge=True)
                settings_saved = settings_ref.get().to_dict() or {}

                logging.getLogger("admin-oa-new").info(
                    "create OA shop=%s line_oa_id=%s bot_user_id=%s",
                    shop_id, channel_id_str, bot_user_id,
                )

                webhook_url = CONSUMER_WEBHOOK_URL
                owner_signin_url = f"{ADMIN_BASE_URL}/owner/auth/liff/boot?next=/owner/promotions/form&sid={shop_id}"

                result = {
                    "shop_id": shop_id,
                    "owner_signin_url": owner_signin_url,
                    "webhook_url": webhook_url,
                    "secrets": {
                        "access_token": line_access_token,
                        "channel_secret": line_channel_secret,
                    },
                    "settings": settings_saved,
                    "prefill_id": prefill_id or None,
                }

                if prefill_id and prefill_data:
                    try:
                        req_ref = (
                            db.collection("onboarding")
                              .document("requests")
                              .collection("items")
                              .document(prefill_id)
                        )
                        req_ref.set({
                            "status": "approved",
                            "shop_id": shop_id,
                            "approved_at": now,
                            "updated_at": now,
                        }, merge=True)
                        profile_payload = {
                            "name": prefill_data.get("name"),
                            "phone": prefill_data.get("phone"),
                            "shop": prefill_data.get("shop"),
                            "location": prefill_data.get("location"),
                            "logo_url": prefill_data.get("logo_url"),
                            "source_request_id": prefill_id,
                            "synced_at": now,
                            "messaging_user_id": prefill_data.get("messaging_user_id"),
                        }
                        owner_profile_col = db.collection("shops").document(shop_id).collection("owner_profile")
                        owner_profile_col.document("information").set(profile_payload, merge=True)
                        owner_profile_col.document("default").set(profile_payload, merge=True)
                    except Exception as sync_err:
                        log.warning("failed to sync owner profile from request %s: %s", prefill_id, sync_err)

                    invite_info: Optional[Dict[str, Any]] = None
                    messaging_user_id = (prefill_data.get("messaging_user_id") or prefill_data.get("user_id") or "").strip()
                    try:
                        token, jti, exp = _sign_owner_invite(shop_id)
                        exp_iso = datetime.fromtimestamp(exp, timezone.utc).isoformat()
                        link_payload = {
                            "scope": "owner_invite",
                            "created_at": now,
                            "exp": exp,
                            "revoked": False,
                            "target_user_id": messaging_user_id or None,
                            "request_id": prefill_id,
                            "created_via": "auto",
                        }
                        (
                            db.collection("shops").document(shop_id)
                              .collection("magic_links").document(jti)
                        ).set(link_payload, merge=False)
                        try:
                            req_ref.set({
                                "owner_invite_jti": jti,
                                "owner_invite_sent_at": now,
                            }, merge=True)
                        except Exception:
                            pass
                        invite_url = _build_owner_invite_url(shop_id, token)
                        pushed = False
                        push_error = None
                        if messaging_user_id:
                            pushed, push_error = _send_owner_invite_message(shop_id, settings_saved, messaging_user_id, invite_url)
                        invite_info = {
                            "url": invite_url,
                            "token": token,
                            "jti": jti,
                            "exp": exp_iso,
                            "pushed": pushed,
                            "messaging_user_id": messaging_user_id or None,
                        }
                        if push_error:
                            invite_info["push_error"] = push_error
                        log.info("owner invite created shop=%s req=%s jti=%s pushed=%s", shop_id, prefill_id, jti, pushed)
                    except Exception as invite_err:
                        log.warning("auto invite generation failed shop=%s err=%s", shop_id, invite_err)
                        invite_info = {"error": str(invite_err)}
                    if invite_info:
                        result["invite_info"] = invite_info

                # Clear sensitive fields from form after success
                form = default_form.copy()
            except Exception as exc:
                log.error("admin oa new failed: %s", exc, exc_info=True)
                errors.append("Failed to create the shop. Please try again or contact the platform team.")

    status_code = 200 if not errors else 400 if request.method == "POST" and not result else 200
    return render_template(
        "admin_oa_new.html",
        env_info=env_info,
        form=form,
        errors=errors,
        result=result,
        prefill_id=prefill_id,
        prefill_data=prefill_data,
    ), status_code

@admin_bp.route("/admin/oa/owners", methods=["GET", "POST"])
def admin_manage_owners():
    db = get_db()
    errors: List[str] = []
    message: Optional[str] = None
    invite_result: Optional[Dict[str, Any]] = None
    shop_id = (request.values.get("shop_id") or "").strip()
    settings: Optional[Dict[str, Any]] = None

    if request.method == "POST":
        action = request.form.get("action") or ""
        shop_id = (request.form.get("shop_id") or "").strip()
        if not shop_id:
            errors.append("กรุณาระบุ shop_id")
        else:
            try:
                snap = db.collection("shops").document(shop_id).get()
                if not snap.exists:
                    errors.append("ไม่พบร้านนี้ในระบบ")
                else:
                    settings_snap = snap.reference.collection("settings").document("default").get()
                    settings = settings_snap.to_dict() if settings_snap.exists else {}
            except Exception as e:
                errors.append(f"ไม่สามารถอ่านข้อมูลร้านได้: {e}")

        if not errors and action == "create":
            try:
                now = datetime.now(timezone.utc)
                token, jti, exp = _sign_owner_invite(shop_id)
                exp_iso = datetime.fromtimestamp(exp, timezone.utc).isoformat()
                messaging_user = (request.form.get("messaging_user_id") or "").strip()
                link_payload = {
                    "scope": "owner_invite",
                    "created_at": now,
                    "exp": exp,
                    "revoked": False,
                    "created_via": "manual",
                    "target_user_id": messaging_user or None,
                }
                (
                    db.collection("shops").document(shop_id)
                      .collection("magic_links").document(jti)
                ).set(link_payload, merge=False)
                invite_url = _build_owner_invite_url(shop_id, token)
                pushed = False
                push_error = None
                if messaging_user:
                    pushed, push_error = _send_owner_invite_message(shop_id, settings, messaging_user, invite_url)
                invite_result = {
                    "url": invite_url,
                    "token": token,
                    "jti": jti,
                    "exp": exp_iso,
                    "pushed": pushed,
                    "messaging_user_id": messaging_user or None,
                }
                if push_error:
                    invite_result["push_error"] = push_error
                message = "สร้างลิงก์เชิญเรียบร้อย"
                logging.getLogger("admin-invite").info("manual owner invite created shop=%s jti=%s pushed=%s", shop_id, jti, pushed)
            except Exception as e:
                errors.append(f"ไม่สามารถสร้างลิงก์เชิญได้: {e}")
        elif not errors and action == "revoke":
            jti = (request.form.get("jti") or "").strip()
            if not jti:
                errors.append("กรุณาระบุ jti ที่ต้องการยกเลิก")
            else:
                try:
                    now = datetime.now(timezone.utc)
                    (
                        db.collection("shops").document(shop_id)
                          .collection("magic_links").document(jti)
                    ).set({
                        "revoked": True,
                        "revoked_at": now,
                    }, merge=True)
                    message = "ยกเลิกลิงก์เรียบร้อย"
                except Exception as e:
                    errors.append(f"ไม่สามารถยกเลิกได้: {e}")

    if shop_id and settings is None and not errors:
        try:
            snap = db.collection("shops").document(shop_id).get()
            if snap.exists:
                settings_snap = snap.reference.collection("settings").document("default").get()
                settings = settings_snap.to_dict() if settings_snap.exists else {}
            else:
                errors.append("ไม่พบร้านนี้ในระบบ")
        except Exception as e:
            errors.append(f"ไม่สามารถอ่านข้อมูลร้านได้: {e}")

    owners: List[Dict[str, Any]] = []
    magic_links: List[Dict[str, Any]] = []
    if shop_id and not errors:
        try:
            owner_docs = db.collection("shops").document(shop_id).collection("owners").stream()
            for doc in owner_docs:
                data = doc.to_dict() or {}
                linked = data.get("created_at") or data.get("linked_at")
                if hasattr(linked, "isoformat"):
                    linked = linked.isoformat()
                owners.append({
                    "id": doc.id,
                    "active": bool(data.get("active", True)),
                    "roles": data.get("roles") or [],
                    "linked_at": linked,
                })
        except Exception as e:
            errors.append(f"ไม่สามารถอ่านข้อมูล owner ได้: {e}")

        try:
            link_query = (
                db.collection("shops").document(shop_id)
                  .collection("magic_links")
                  .order_by("created_at", direction=firestore.Query.DESCENDING)
                  .limit(20)
            )
            for doc in link_query.stream():
                data = doc.to_dict() or {}
                if data.get("scope") and data.get("scope") != "owner_invite":
                    continue
                created = data.get("created_at")
                exp = data.get("exp")
                used = data.get("used_at")
                if hasattr(created, "isoformat"):
                    created = created.isoformat()
                if isinstance(exp, (int, float)):
                    exp_ts = datetime.fromtimestamp(exp, timezone.utc)
                elif hasattr(exp, "isoformat"):
                    exp_ts = exp
                else:
                    exp_ts = None
                if hasattr(exp_ts, "isoformat"):
                    exp = exp_ts.isoformat()
                elif exp_ts is None:
                    exp = ""
                else:
                    exp = str(exp_ts)
                if hasattr(used, "isoformat"):
                    used = used.isoformat()
                magic_links.append({
                    "jti": doc.id,
                    "target_user_id": data.get("target_user_id"),
                    "created_at": created,
                    "exp": exp,
                    "revoked": bool(data.get("revoked")),
                    "used_at": used,
                })
        except Exception as e:
            errors.append(f"ไม่สามารถอ่าน magic link ได้: {e}")

    status_code = 400 if (request.method == "POST" and errors) else 200
    return render_template(
        "admin_oa_owners.html",
        shop_id=shop_id,
        owners=owners,
        magic_links=magic_links,
        invite_result=invite_result,
        message=message,
        errors=errors,
    ), status_code

# --- Image upload helper for promotions/products ---
def _upload_images_from_request(prefix: str) -> list[str]:
    files = []
    try:
        up_files = []
        if 'pictures' in request.files:
            f = request.files.get('pictures')
            if f: up_files.append(f)
        if 'pictures[]' in request.files:
            up_files.extend(request.files.getlist('pictures[]'))
        bucket_name = os.getenv("MEDIA_BUCKET") or os.getenv("REPORT_BUCKET")
        if not up_files or not bucket_name:
            return []
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        urls = []
        for f in up_files:
            filename = secure_filename(f.filename or "")
            ext = ""
            if "." in filename:
                ext = "." + filename.rsplit(".", 1)[-1].lower()
            blob_name = f"{prefix}/{uuid.uuid4().hex}{ext}"
            blob = bucket.blob(blob_name)
            data = f.read()
            blob.upload_from_string(data, content_type=f.mimetype or "image/jpeg")
            try:
                blob.cache_control = "public, max-age=86400"
                blob.patch()
            except Exception:
                pass
            public_base = os.getenv("MEDIA_PUBLIC_BASE", f"https://storage.googleapis.com/{bucket_name}")
            urls.append(f"{public_base}/{blob_name}")
        return urls
    except Exception:
        return []

def _now_utc():
    return datetime.now(timezone.utc)

def _require_owner_auth():
    # MVP: ใช้ bearer token ง่ายๆ ก่อน (สามารถต่อยอดเป็น login/jwt ภายหลัง)
    from flask import request
    want = (request.headers.get("Authorization") or "").replace("Bearer ","").strip()
    need = (request.environ.get("API_BEARER_TOKEN") or "")  # can be injected later, or use app.config
    # สำหรับ MVP ไม่บังคับ — ถ้าจะเปิด auth ใส่ logic ตรงนี้
    return True

@admin_bp.get('/owner/<shop_id>/promotions/form')
def owner_promo_form(shop_id):
    display_name = _resolve_shop_display_name(shop_id)
    try:
        return render_template(
            'owner_promotions_form.html',
            shop_id=shop_id,
            shop_display_name=display_name,
        )
    except TemplateNotFound:
        return render_template_string('''
        <!doctype html>
        <html lang="th"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>โปรโมชัน — {{ shop_label }}</title>
        <style>
        :root{--brand:#008080;--accent:#F97316;--bg:#F8F9FA;--ink:#000}
        body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif}
        header{padding:12px 16px;background:linear-gradient(90deg,var(--brand),#38b2b2);color:#fff;font-weight:700}
        .wrap{max-width:880px;margin:18px auto;padding:0 14px}
        .card{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:14px}
        label{display:block;margin:8px 0 4px;font-weight:600}
        input[type=text],textarea,select{width:100%;padding:10px;border:1px solid #e5e7eb;border-radius:8px}
        .btn{display:inline-block;border:none;border-radius:10px;padding:10px 14px;cursor:pointer;font-weight:700}
        .btn-primary{background:var(--accent);color:#111}
        .muted{color:#6b7280;font-size:12px}
        </style></head>
        <body>
        <header>โปรโมชัน — {{ shop_label }}</header>
        <div class="wrap">
          <div class="card">
            <form id="promoForm">
              <label>ชื่อโปรโมชัน</label>
              <input type="text" name="title" placeholder="เช่น ซื้อ 1 แถม 1">
              <label>รายละเอียด</label>
              <textarea name="description" rows="4" placeholder="ระบุเงื่อนไข/ช่วงเวลา ฯลฯ"></textarea>
              <label>สถานะ</label>
              <select name="status"><option value="draft">Draft</option><option value="active">Active</option><option value="archived">Archived</option></select>
              <div style="margin-top:12px"><button class="btn btn-primary" type="submit">บันทึก</button></div>
              <p class="muted">* โหมด fallback (ไฟล์เทมเพลตยังไม่พบ)</p>
            </form>
          </div>
        </div>
        <script>
        const shopId = {{ shop_id|tojson }};
        const formEl = document.getElementById('promoForm');
        formEl.addEventListener('submit', async (e)=>{
          e.preventDefault();
          const fd = new FormData(formEl); const body={}; for(const [k,v] of fd.entries()) body[k]=v;
          const res = await fetch(`/owner/${shopId}/promotions`, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
          if(res.ok){ alert('บันทึกแล้ว'); formEl.reset(); }
          else{ alert('บันทึกล้มเหลว'); }
        });
        </script>
        </body></html>
        ''', shop_label=display_name or shop_id, shop_id=shop_id)

@admin_bp.get('/owner/<shop_id>/reports/request')
def owner_report_form(shop_id):
    display_name = _resolve_shop_display_name(shop_id)
    try:
        return render_template(
            'owner_report_request.html',
            shop_id=shop_id,
            shop_display_name=display_name,
        )
    except TemplateNotFound:
        return render_template_string('''
        <!doctype html>
        <html lang="th"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>ขอรายงาน — {{ shop_label }}</title>
        <style>
        :root{--brand:#008080;--accent:#F97316;--bg:#F8F9FA;--ink:#000}
        body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif}
        header{padding:12px 16px;background:linear-gradient(90deg,var(--brand),#38b2b2);color:#fff;font-weight:700}
        .wrap{max-width:880px;margin:18px auto;padding:0 14px}
        .card{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:14px}
        label{display:block;margin:8px 0 4px;font-weight:600}
        input[type=text]{width:100%;padding:10px;border:1px solid #e5e7eb;border-radius:8px}
        .btn{display:inline-block;border:none;border-radius:10px;padding:10px 14px;cursor:pointer;font-weight:700}
        .btn-primary{background:var(--accent);color:#111}
        </style></head>
        <body>
        <header>ขอรายงาน — {{ shop_label }}</header>
        <div class="wrap">
          <div class="card">
            <form id="reqForm">
              <label>หมายเหตุ</label>
              <input type="text" name="note" placeholder="บันทึกเพิ่มเติม (ถ้ามี)">
              <div style="margin-top:12px"><button class="btn btn-primary" type="submit">ขอรายงาน</button></div>
              <p class="muted">* โหมด fallback (ไฟล์เทมเพลตยังไม่พบ)</p>
            </form>
          </div>
        </div>
        <script>
        const shopId = {{ shop_id|tojson }};
        const formEl = document.getElementById('reqForm');
        formEl.addEventListener('submit', async (e)=>{
          e.preventDefault();
          const fd = new FormData(formEl); const body={}; for(const [k,v] of fd.entries()) body[k]=v;
          const res = await fetch(`/owner/${shopId}/reports/requests`, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
          if(res.ok){ alert('ส่งคำขอแล้ว'); formEl.reset(); }
          else{ alert('ส่งคำขอล้มเหลว'); }
        });
        </script>
        </body></html>
        ''', shop_label=display_name or shop_id, shop_id=shop_id)
def _owner_session_or_403(shop_id: str):
    sess = _get_owner_session_shop_id()
    if not sess or sess != shop_id:
        abort(403, "owner_session_missing_or_mismatch")

@admin_bp.post("/owner/<shop_id>/reports/requests")
def create_report_request(shop_id):
    _owner_session_or_403(shop_id)
    db = get_db()
    data = request.get_json(silent=True) or request.form.to_dict()

    kind = (data.get("kind") or "mini").strip().lower()  # "mini" | "full" (ตามที่ UI ส่งมา)
    note = (data.get("note") or "").strip()

    # อ่านช่วงวันที่จาก UI ถ้ามี; ไม่มีก็ default 14 วันล่าสุด
    now = datetime.now(timezone.utc)
    start_iso = (data.get("start_date") or "").strip()
    end_iso = (data.get("end_date") or "").strip()
    try:
        start_dt = _dtparser.isoparse(start_iso) if start_iso else (now - timedelta(days=14))
    except Exception:
        start_dt = now - timedelta(days=14)
    try:
        end_dt = _dtparser.isoparse(end_iso) if end_iso else now
    except Exception:
        end_dt = now

    payload = {
        "kind": kind,
        "note": note,
        "start_date": start_dt,
        "end_date": end_dt,
        "status": "queued",               # queued -> processing -> done / failed
        "created_at": now,
        "created_by": "owner",
    }

    ref = db.collection("shops").document(shop_id)\
            .collection("report_requests").document()
    ref.set(payload, merge=False)

    # (ถ้าจะมี worker มารับคิวภายหลัง ค่อย publish Pub/Sub ที่นี่)
    # _publish("report.requested", attrs={"shop_id": shop_id, "request_id": ref.id}, data={"kind": kind})

    return jsonify({"ok": True, "request_id": ref.id})

@admin_bp.get("/owner/<shop_id>/reports/requests")
def list_report_requests(shop_id):
    _owner_session_or_403(shop_id)
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("report_requests")
    items = []
    for d in col.order_by("created_at", direction=firestore.Query.DESCENDING).limit(50).stream():
        obj = d.to_dict() or {}
        obj["_id"] = d.id
        for k in ("created_at","start_date","end_date"):
            v = obj.get(k)
            if hasattr(v, "isoformat"):
                obj[k] = v.isoformat()
        items.append(obj)
    return jsonify({"ok": True, "items": items})


# --- Helpers: parse incoming Start/End robustly (accept Thai BE year and strings) ---
_DEF_TZ = timezone.utc

def _to_dt_utc(v):
    """Coerce Firestore Timestamp, datetime, or string to aware UTC datetime.
    Accepts Thai Buddhist years (BE), e.g., '27 Oct BE 2568' or year>=2400.
    """
    try:
        # Firestore Timestamp
        if hasattr(v, "to_datetime"):
            return v.to_datetime().astimezone(timezone.utc)
        # Already datetime
        if hasattr(v, "astimezone"):
            return v.astimezone(timezone.utc)
        s = (str(v) or "").strip()
        if not s:
            return None
        # Normalize Thai BE -> AD
        # If the string contains 'BE' or year looks like 24xx-26xx, subtract 543
        try:
            if "BE" in s or "บ.ศ." in s or "พ.ศ." in s:
                s_norm = s.replace("BE", "").replace("พ.ศ.", "").replace("บ.ศ.", "").strip()
                dt = _dtparser.parse(s_norm, dayfirst=False, yearfirst=False, fuzzy=True)
                if dt.year >= 2400:
                    dt = dt.replace(year=dt.year - 543)
                return dt.astimezone(timezone.utc)
        except Exception:
            pass
        # Generic parse; if the year is >=2400, shift -543
        dt = _dtparser.parse(s, fuzzy=True)
        if dt.year >= 2400:
            dt = dt.replace(year=dt.year - 543)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


@admin_bp.post("/owner/<shop_id>/reports/requests/<req_id>/run")
def run_report_request(shop_id: str, req_id: str):
    """Generate the PDF for a queued report request and update Firestore with links.
    Returns {ok, pdf_url, request_id} or 404/400 on errors.
    """
    _owner_session_or_403(shop_id)
    db = get_db()

    doc_ref = db.collection("shops").document(shop_id).collection("report_requests").document(req_id)
    snap = doc_ref.get()
    if not snap.exists:
        abort(404, "request_not_found")
    obj = snap.to_dict() or {}

    # Resolve time range
    start_dt = _to_dt_utc(obj.get("start_date")) or (datetime.now(timezone.utc) - timedelta(days=14))
    end_dt   = _to_dt_utc(obj.get("end_date"))   or datetime.now(timezone.utc)

    # Choose renderer with graceful fallback (never 500 because of missing WeasyPrint/Jinja2)
    kind = (obj.get("kind") or "mini").lower()
    pdf_bytes = None
    if kind == "mini":
        # mini
        if render_mini_report_pdf:
            try:
                pdf_bytes = render_mini_report_pdf(shop_id, start_dt, end_dt)
            except Exception as e:
                logging.getLogger("admin-auth").error("mini report render failed, try v3: %s", e)
                try:
                    from report_renderer import build_report_pdf_v3 as _fallback_pdf
                    pdf_bytes = _fallback_pdf(shop_id, start_dt, end_dt)
                except Exception as e2:
                    logging.getLogger("admin-auth").error("v3 fallback failed, using stub: %s", e2)
                    pdf_bytes = _fallback_pdf_stub(shop_id, start_dt, end_dt)
        else:
            try:
                from report_renderer import build_report_pdf_v3 as _fallback_pdf
                pdf_bytes = _fallback_pdf(shop_id, start_dt, end_dt)
            except Exception as e:
                logging.getLogger("admin-auth").error("fallback import failed, using stub: %s", e)
                pdf_bytes = _fallback_pdf_stub(shop_id, start_dt, end_dt)
    else:
        # full
        if render_full_report_pdf:
            try:
                pdf_bytes = render_full_report_pdf(shop_id, start_dt, end_dt)
            except Exception as e:
                logging.getLogger("admin-auth").error("full report render failed, try v3: %s", e)
                try:
                    from report_renderer import build_mini_report_pdf as _fallback_pdf
                    pdf_bytes = _fallback_pdf(shop_id, start_dt, end_dt)
                except Exception as e2:
                    logging.getLogger("admin-auth").error("mini fallback failed, using stub: %s", e2)
                    pdf_bytes = _fallback_pdf_stub(shop_id, start_dt, end_dt)
        else:
            try:
                from report_renderer import build_mini_report_pdf as _fallback_pdf
                pdf_bytes = _fallback_pdf(shop_id, start_dt, end_dt)
            except Exception as e:
                logging.getLogger("admin-auth").error("fallback import failed, using stub: %s", e)
                pdf_bytes = _fallback_pdf_stub(shop_id, start_dt, end_dt)

    # Upload to GCS
    bucket_name = os.getenv("REPORT_BUCKET") or os.getenv("MEDIA_BUCKET")
    if not bucket_name:
        return jsonify({"ok": False, "error": "report_bucket_not_set"}), 500
    try:
        storage_client = storage.Client()
        blob_path = f"reports/{shop_id}/{req_id}.pdf"
        blob = storage_client.bucket(bucket_name).blob(blob_path)
        blob.upload_from_string(pdf_bytes, content_type="application/pdf")
        try:
            blob.cache_control = "public, max-age=86400"; blob.patch()
        except Exception:
            pass
        public_base = os.getenv("MEDIA_PUBLIC_BASE", f"https://storage.googleapis.com/{bucket_name}")
        pdf_url = f"{public_base}/{blob_path}"
        gcs_uri = f"gs://{bucket_name}/{blob_path}"
    except Exception as e:
        logging.getLogger("admin-auth").error("report upload failed: %s", e)
        return jsonify({"ok": False, "error": "upload_failed"}), 500

    # Update request document
    try:
        doc_ref.set({
            "status": "done",
            "updated_at": datetime.now(timezone.utc),
            "pdf_url": pdf_url,
            "pdf_gcs_uri": gcs_uri,
            "start_date": start_dt,
            "end_date": end_dt,
        }, merge=True)
    except Exception:
        pass

    return jsonify({"ok": True, "request_id": req_id, "pdf_url": pdf_url})


@admin_bp.get("/owner/reports/request")
def owner_report_form_session():
    shop_id = _get_owner_session_shop_id()
    if not shop_id:
        target = quote("/owner/reports/request", safe="/")
        return redirect(f"/owner/auth/liff/boot?next={target}")
    return owner_report_form(shop_id)

# --- Context endpoint for current owner session ---
@admin_bp.get("/owner/context")
def owner_context():
    """
    Lightweight endpoint for templates/JS to discover the current owner's shop_id from the session cookie.
    Returns {ok, shop_id} or 401 if no session is present.
    """
    sid = _get_owner_session_shop_id()
    if not sid:
        return jsonify({"ok": False, "error": "owner_session_missing"}), 401
    return jsonify({"ok": True, "shop_id": sid})

# ---- API: Promotions ----
@admin_bp.get("/owner/<shop_id>/promotions")
def list_promotions_api(shop_id):
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("promotions")
    docs = []
    for d in col.order_by('created_at', direction=firestore.Query.DESCENDING).limit(100).stream():
        item = d.to_dict() or {}
        item["_id"] = d.id
        for k in ("created_at","updated_at","start_date","end_date"):
            v = item.get(k)
            if hasattr(v, "isoformat"):
                item[k] = v.isoformat()
        docs.append(item)
    return jsonify({"items": docs})

@admin_bp.post("/owner/<shop_id>/promotions")
def create_or_update_promotion(shop_id):
    db = get_db()
    data: Dict[str, Any] = request.get_json(silent=True) or request.form.to_dict()
    if not data and not request.files:
        abort(400, "no_payload")
    pid: Optional[str] = data.get("_id") or data.get("id")
    now = _now_utc()
    pictures = _upload_images_from_request(f"shops/{shop_id}/promotions") or []
    # start_date: use provided value if parseable, otherwise default to now
    start_dt = now
    try:
        if data.get("start_date"):
            start_dt = _dtparser.isoparse(str(data.get("start_date")))
    except Exception:
        start_dt = now
    payload: Dict[str, Any] = {
        "title": data.get("title"),
        "description": data.get("description"),
        "status": data.get("status") or "draft",
        "start_date": start_dt,
        "end_date": None,
        "updated_at": now,
        "pictures": pictures,
    }
    if not pid:
        ref = db.collection("shops").document(shop_id).collection("promotions").document()
        payload["created_at"] = now
        ref.set(payload, merge=False)
        try:
            _publish(
                "promotion.updated",
                attrs={"shop_id": shop_id, "promotion_id": ref.id, "op": "upsert"},
                data={"pictures": pictures, "title": payload.get("title"), "status": payload.get("status")}
            )
        except Exception:
            pass
        pid = ref.id
    else:
        ref = db.collection("shops").document(shop_id).collection("promotions").document(pid)
        if not ref.get().exists:
            payload["created_at"] = now
        ref.set(payload, merge=True)
        try:
            _publish(
                "promotion.updated",
                attrs={"shop_id": shop_id, "promotion_id": pid, "op": "upsert"},
                data={"pictures": pictures, "title": payload.get("title"), "status": payload.get("status")}
            )
        except Exception:
            pass
    return jsonify({"ok": True, "promotion_id": pid})

# ---- API: Products ----
@admin_bp.get("/owner/<shop_id>/products")
def list_products_api(shop_id):
    db = get_db()
    col = db.collection("shops").document(shop_id).collection("products")
    docs = []
    for d in col.order_by('created_at', direction=firestore.Query.DESCENDING).limit(100).stream():
        item = d.to_dict() or {}
        item["_id"] = d.id
        for k in ("created_at","updated_at"):
            v = item.get(k)
            if hasattr(v, "isoformat"):
                item[k] = v.isoformat()
        docs.append(item)
    return jsonify({"items": docs})

@admin_bp.post("/owner/<shop_id>/products")
def create_or_update_product(shop_id):
    db = get_db()
    data = request.get_json(silent=True) or request.form.to_dict()
    if not data and not request.files:
        abort(400, "no_payload")
    pid: Optional[str] = data.get("_id") or data.get("id")
    now = _now_utc()
    pictures = _upload_images_from_request(f"shops/{shop_id}/products") or []
    title = data.get("title") or data.get("topic")
    try:
        unit_price = float(data.get("unit_price")) if data.get("unit_price") not in (None, "",) else None
    except Exception:
        unit_price = None
    payload: Dict[str, Any] = {
        "topic": title,
        "title": title,
        "description": data.get("description") or data.get("description_prod"),
        "unit_price": unit_price,
        "pictures": pictures,
        "status": data.get("status") or "active",
        "updated_at": now,
    }
    if not pid:
        ref = db.collection("shops").document(shop_id).collection("products").document()
        payload["created_at"] = now
        ref.set(payload, merge=False)
        try:
            _publish(
                "product.updated",
                attrs={"shop_id": shop_id, "product_id": ref.id, "op": "upsert"},
                data={
                    "title": payload.get("title"),
                    "unit_price": payload.get("unit_price"),
                    "status": payload.get("status"),
                    "pictures": pictures
                }
            )
        except Exception:
            pass
        pid = ref.id
    else:
        ref = db.collection("shops").document(shop_id).collection("products").document(pid)
        if not ref.get().exists:
            payload["created_at"] = now
        ref.set(payload, merge=True)
        try:
            _publish(
                "product.updated",
                attrs={"shop_id": shop_id, "product_id": pid, "op": "upsert"},
                data={
                    "title": payload.get("title"),
                    "unit_price": payload.get("unit_price"),
                    "status": payload.get("status"),
                    "pictures": pictures
                }
            )
        except Exception:
            pass
    return jsonify({"ok": True, "product_id": pid})

# --- Convenience route for form (no shop_id in path, use ?sid=) ---

# --- Convenience route for form (no shop_id in path, use ?sid= or ?token=) ---
# --- Convenience route for form (no shop_id in path, use ?sid= or ?token=) ---
@admin_bp.get("/owner/auth/liff/boot")
def owner_auth_liff_boot():
    next_param = (request.args.get("next") or "").strip()
    kind = (request.args.get("kind") or "").strip().lower()

    liff_id_global = os.getenv("GLOBAL_LIFF_ID", "").strip()
    liff_id_default = os.getenv("LIFF_ID", "").strip()
    liff_id_report  = os.getenv("LIFF_ID_REPORT", "").strip()
    liff_id_promotion = os.getenv("LIFF_ID_PROMOTION", "").strip()

    def _append(candidate: Optional[str], acc: List[str]) -> None:
        if candidate and candidate not in acc:
            acc.append(candidate)

    next_path = next_param.split("?")[0] if next_param else ""
    is_report_context = (kind == "report") or next_path.startswith("/owner/reports")
    # treat product management as the same LIFF context as promotion
    is_promo_context = (kind in ("promotion", "product")) or next_path.startswith("/owner/promotions")

    liff_candidates: List[str] = []
    _append(liff_id_global, liff_candidates)
    if is_report_context:
        _append(liff_id_report, liff_candidates)
        _append(liff_id_default, liff_candidates)
        _append(liff_id_promotion, liff_candidates)
    elif is_promo_context:
        _append(liff_id_promotion, liff_candidates)
        _append(liff_id_default, liff_candidates)
        _append(liff_id_report, liff_candidates)
    else:
        _append(liff_id_default, liff_candidates)
        _append(liff_id_report, liff_candidates)
        _append(liff_id_promotion, liff_candidates)

    if not liff_candidates:
        logging.getLogger("admin-auth").error("LIFF IDs missing for context kind=%s next=%s", kind, next_param)
        return render_template_string("<p>Missing LIFF_ID env</p>"), 500

    logging.getLogger("admin-auth").info("owner_auth_liff_boot context=%s next=%s candidates=%s", kind, next_param, liff_candidates)
    return render_template("owner_liff_boot.html", liff_ids=liff_candidates)

@admin_bp.get('/owner/promotions/form')
def owner_promo_form_shortcut():
    token = (request.args.get("token") or "").strip()
    sid = (request.args.get("sid") or "").strip()

    # 1) Bootstrap via magic token
    if token:
        try:
            data = jwt.decode(token, _get_magic_secret(), algorithms=["HS256"])
            if data.get("scope") != "owner_form":
                return render_template_string("<p>invalid scope</p>"), 403
            shop_id = data.get("shop_id")
            jti = data.get("jti")
            # Optional: check revoke
            try:
                db = get_db()
                snap = db.collection("shops").document(shop_id).collection("magic_links").document(jti).get()
                if snap.exists and (snap.to_dict() or {}).get("revoked"):
                    return render_template_string("<p>link revoked</p>"), 403
            except Exception:
                pass
            # Set cookie and redirect to clean URL (hide token)
            resp = make_response(redirect("/owner/promotions/form"))
            _set_owner_session_cookie(resp, shop_id)
            return resp
        except jwt.ExpiredSignatureError:
            return render_template_string("<p>link expired</p>"), 403
        except Exception:
            return render_template_string("<p>invalid token</p>"), 400

    # 2) Existing cookie or fallback sid
    sess_sid = request.cookies.get(OWNER_SESSION_COOKIE)
    final_sid = (sid or sess_sid) or None
    try:
        return render_template(
            'owner_promotions_form.html',
            shop_id=final_sid,
            shop_display_name=_resolve_shop_display_name(final_sid) if final_sid else None,
        )
    except TemplateNotFound:
        return render_template_string("&lt;p&gt;กรุณาใส่ ?sid=&lt;shop_id&gt;&lt;/p&gt;")

# --- Owner LIFF ID token callback ---
@admin_bp.post("/owner/auth/liff/callback")
def owner_auth_liff_callback():
    """
    Accepts { id_token, nonce? } from LIFF JS, verifies against LINE JWKS (RS256),
    binds the owner session to a shop_id, then returns {ok, shop_id, redirect}.
    """
    # 1) Read payload
    try:
        payload = request.get_json(silent=True) or {}
    except Exception:
        payload = {}
    raw_id_token = (payload.get("id_token")
                    or request.form.get("id_token")
                    or request.args.get("id_token")
                    or "").strip()
    next_param = (request.args.get("next") or "").strip()
    sid_param = (request.args.get("sid") or "").strip() or None
    if not sid_param:
        sid_body = (payload.get("sid") or "").strip() or None
        if sid_body:
            sid_param = sid_body
    invite_token = (payload.get("invite_token")
                    or request.args.get("token")
                    or "").strip() or None
    if not sid_param and next_param:
        try:
            parsed = urlparse(next_param)
            qs = parse_qs(parsed.query or "")
            sid_candidates = qs.get("sid") or []
            if sid_candidates:
                sid_param = (sid_candidates[0] or "").strip() or None
        except Exception:
            sid_param = sid_param  # keep existing (noop)
    if not raw_id_token:
        return jsonify({"ok": False, "error": "missing_id_token"}), 400

    # 2) DEBUG decode (no signature) for easier troubleshooting
    try:
        unverified = jwt.decode(raw_id_token, options={"verify_signature": False})
        logging.getLogger("admin-auth").error("LIFF DEBUG claims=%s", json.dumps({
            "aud": unverified.get("aud"),
            "azp": unverified.get("azp"),
            "iss": unverified.get("iss"),
            "sub": unverified.get("sub"),
            "exp": unverified.get("exp"),
            "iat": unverified.get("iat"),
        }))
    except Exception as e:
        logging.getLogger("admin-auth").error("LIFF DEBUG cannot decode: %s", e)

    # 3) Verify with LINE JWKS (RS256)
    claims = _verify_line_id_token(raw_id_token)
    if not claims:
        return jsonify({"ok": False, "error": "invalid_id_token"}), 401

    owner_user_id = claims.get("sub")  # LINE userId of the owner (global preferred)
    if not owner_user_id:
        return jsonify({"ok": False, "error": "missing_sub"}), 400

    # 4) Resolve shop_id for this owner (cookie → mapping → sid)
    invite_ctx = None
    if invite_token:
        invite_ctx, invite_err = _verify_owner_invite_token(invite_token)
        if not invite_ctx:
            return jsonify({"ok": False, "error": invite_err or "invite_invalid"}), 403
        shop_id = invite_ctx["shop_id"]
        if sid_param and sid_param != shop_id:
            return jsonify({"ok": False, "error": "sid_mismatch"}), 403
        sid_param = shop_id
    else:
        shop_id = _get_owner_session_shop_id()
    if not shop_id:
        shop_id = _find_shop_by_owner_user_id(owner_user_id)
    if not shop_id and sid_param:
        shop_id = sid_param
    if not shop_id:
        logging.getLogger("admin-auth").error("owner user has no shop mapping sub=%s", owner_user_id)
        return jsonify({"ok": False, "error": "owner_not_mapped"}), 403

    # 4.1) Ensure Firestore mapping exists/updates for this owner
    try:
        _ensure_owner_record(shop_id, owner_user_id, claims)
    except Exception:
        pass

    # 4.2) If invite token used, mark as consumed and sync owner profile
    if invite_ctx:
        try:
            now = datetime.now(timezone.utc)
            invite_ctx["link_ref"].set({
                "used_at": now,
                "used_by": owner_user_id,
            }, merge=True)
            target_user = invite_ctx["link_doc"].get("target_user_id")
            if target_user:
                try:
                    prof_ref = get_db().collection("owner_profiles").document(owner_user_id)
                    prof_ref.set({
                        "messaging_user_id": target_user,
                        "verified_at": now,
                        "updated_at": now,
                        "last_shop_id": shop_id,
                    }, merge=True)
                except Exception as prof_err:
                    logging.getLogger("admin-auth").warning("sync owner_profiles failed: %s", prof_err)
            logging.getLogger("admin-auth").info(
                "owner invite consumed shop=%s jti=%s owner=%s",
                shop_id, invite_ctx["jti"], owner_user_id,
            )
        except Exception as e:
            logging.getLogger("admin-auth").warning("mark invite used failed: %s", e)

    # 5) Set cookie session and return redirect target
    resp = jsonify({
        "ok": True,
        "shop_id": shop_id,
        "redirect": (next_param or f"/owner/{shop_id}/promotions/form")
    })
    _set_owner_session_cookie(resp, shop_id)
    return resp

    # Verify with LINE JWKS (RS256)
def _verify_line_id_token(raw_id_token: str) -> Optional[dict]:
    """
    Verify a LINE LIFF ID token using LINE's JWKs.
    Accept both RS256 and ES256 (LINE may rotate keys between RSA/EC).
    Rules:
      - issuer must be https://access.line.me
      - audience must match GLOBAL_LINE_LOGIN_CHANNEL_ID
    Returns a claims dict on success; None on failure.
    """
    log = logging.getLogger("admin-auth")
    try:
        import jwt
        from jwt import PyJWKClient
        from jwt import InvalidAudienceError
    except Exception as e:
        log.error("PyJWT import failed: %s", e)
        return None

    global_cid = os.environ.get("GLOBAL_LINE_LOGIN_CHANNEL_ID", "").strip()
    if not global_cid:
        log.error("GLOBAL_LINE_LOGIN_CHANNEL_ID is not configured")
        return None
    aud_candidates: List[str] = [global_cid]

    # Parse header & unverified claims for diagnostics
    try:
        hdr = jwt.get_unverified_header(raw_id_token)
    except Exception as e:
        hdr = {}
        log.error("LIFF DEBUG cannot parse header: %s", e)
    try:
        unverified = jwt.decode(raw_id_token, options={"verify_signature": False})
    except Exception:
        unverified = {}
    try:
        log.error("LIFF DEBUG header=%s claims=%s", json.dumps(hdr), json.dumps({
            "aud": unverified.get("aud"),
            "azp": unverified.get("azp"),
            "iss": unverified.get("iss"),
            "sub": unverified.get("sub"),
            "exp": unverified.get("exp"),
            "iat": unverified.get("iat"),
        }))
    except Exception:
        pass

    alg = (hdr or {}).get("alg")
    if alg not in ("RS256", "ES256"):
        log.error("unsupported alg in id_token: %s", alg)
        return None

    try:
        jwks_client = PyJWKClient("https://api.line.me/oauth2/v2.1/certs")
        signing_key = jwks_client.get_signing_key_from_jwt(raw_id_token).key

        for candidate in aud_candidates:
            try:
                claims = jwt.decode(
                    raw_id_token,
                    key=signing_key,
                    algorithms=[alg],
                    audience=candidate,
                    issuer="https://access.line.me",
                )
                claims["_verified_audience"] = candidate
                return claims
            except InvalidAudienceError:
                try:
                    claims = jwt.decode(
                        raw_id_token,
                        key=signing_key,
                        algorithms=[alg],
                        options={"verify_aud": False},
                        issuer="https://access.line.me",
                    )
                except Exception as inner_err:
                    log.error("LIFF verify fallback decode failed for channel %s: %s", candidate, inner_err)
                    continue
                aud_value = claims.get("aud")
                azp_value = claims.get("azp")
                ok_aud = False
                if isinstance(aud_value, list):
                    ok_aud = candidate in aud_value
                elif isinstance(aud_value, str):
                    ok_aud = (aud_value == candidate)
                if not (ok_aud or (azp_value == candidate)):
                    log.error("LIFF verify mismatch candidate=%s aud=%s azp=%s", candidate, aud_value, azp_value)
                    continue
                claims["_verified_audience"] = candidate
                return claims
    except Exception as e:
        log.error("verify_line_id_token failed: %s", e)
        return None

# --- Alias endpoint for legacy/compatibility with JS ---
@admin_bp.post("/owner/auth/liff")
def owner_auth_liff_alias():
    return owner_auth_liff_callback()
