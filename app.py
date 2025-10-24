import os, logging
from flask import Flask, request, abort, jsonify, g
from flask_cors import CORS
from datetime import datetime, timezone

import hashlib
import math
from google.cloud import storage
from linebot.models import ImageMessage
import geohash2
from dao import (
    get_shop, get_shop_id_by_line_oa_id,
    list_customers, list_messages, list_products,
    create_product, create_promotion, list_promotions,
    upsert_customer, save_message,
    set_session_state, get_session_state,
    create_payment, update_customer_spending_and_tier,
    list_locations_by_geohash_prefix
)

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage

from firestore_client import get_db
from google.cloud import secretmanager
from functools import lru_cache
import json
import time
import re
import requests

# ---------- App & Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")
app = Flask(__name__)

@app.before_request
def _log_request():
    logger.info(f"REQ {request.method} {request.path}")

# ---------- Config ----------
API_BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN")  # ตั้งตอน deploy
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "http://localhost:5173")
PROOF_BUCKET = os.environ.get("PROOF_BUCKET", "lineoa-g49-proof-uploads")
MAPS_API_KEY = os.environ.get("MAPS_API_KEY")  # สำหรับ Geocoding address -> lat/lng (optional)

# CORS เฉพาะ frontend ของเรา
CORS(app, resources={
    r"/api/*": {"origins": [FRONTEND_ORIGIN]},
})

# ---------- LINE config (multi-tenant via Secret Manager per shop) ----------
# Note: We still allow env LINE_* for quick local testing, but webhook will prefer per-shop secrets.
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")  # optional (local)
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET")  # optional (local)

db = get_db()  # Firestore client
sm_client = secretmanager.SecretManagerServiceClient()

# tiny TTL cache to reduce Secret Manager calls (per-process)
_secret_cache = {}  # key: secret_resource_name, value: (expire_epoch, secret_value)
_SECRET_TTL_SEC = int(os.environ.get("SECRET_TTL_SEC", "300"))

# ---------- Helpers ----------
def require_auth():
    """Simple Bearer auth for internal API. Accepts either Authorization: Bearer <token> or X-Api-Token: <token>."""
    if not API_BEARER_TOKEN:
        abort(500, "API_BEARER_TOKEN not configured")

    # Prefer standard Authorization header
    auth = request.headers.get("Authorization", "")
    token = None
    if auth.startswith("Bearer "):
        token = auth.split(" ", 1)[1].strip()
    else:
        # fallback to custom header for cases where Authorization is used by upstream (e.g., Cloud Run IAM)
        token = request.headers.get("X-Api-Token")

    if not token:
        abort(401, "Missing API token")
    if token != API_BEARER_TOKEN:
        abort(403, "Invalid token")

def _get_secret(secret_resource_name: str) -> str:
    """Fetch secret text with small in-memory TTL cache. secret_resource_name like:
    projects/<PROJECT_ID>/secrets/line-oa/<shop_id>/channel_secret
    """
    now = time.time()
    cached = _secret_cache.get(secret_resource_name)
    if cached and cached[0] > now:
        return cached[1]
    # access latest version
    name = f"{secret_resource_name}/versions/latest" if "/versions/" not in secret_resource_name else secret_resource_name
    resp = sm_client.access_secret_version(request={"name": name})
    val = resp.payload.data.decode("utf-8")
    _secret_cache[secret_resource_name] = (now + _SECRET_TTL_SEC, val)
    return val

def load_line_config_for_shop(shop_id: str) -> dict:
    """Read shops/{shop_id}/settings for secret paths, then resolve to actual tokens."""
    doc = db.collection("shops").document(shop_id).collection("settings").document("_default").get()
    if not doc.exists:
        # fallback to legacy shops/{shop_id}/settings document (flat)
        flat = db.collection("shops").document(shop_id).collection("settings").document("settings").get()
        data = flat.to_dict() if flat.exists else {}
    else:
        data = doc.to_dict() or {}

    # Support both direct values or secret names
    secret_name = data.get("secret_name")
    access_token_name = data.get("access_token_name")
    channel_id = data.get("line_channel_id")

    secret = data.get("line_channel_secret")
    access_token = data.get("line_channel_access_token")

    if not secret and secret_name:
        secret = _get_secret(secret_name)
    if not access_token and access_token_name:
        access_token = _get_secret(access_token_name)

    return {
        "channel_id": channel_id,
        "channel_secret": secret or LINE_CHANNEL_SECRET,
        "access_token": access_token or LINE_CHANNEL_ACCESS_TOKEN,
    }

def haversine_km(lat1, lon1, lat2, lon2):
    R=6371.0
    dlat=math.radians(lat2-lat1)
    dlon=math.radians(lon2-lon1)
    a=math.sin(dlat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return 2*R*math.asin(math.sqrt(a))

def _storage_client():
    return storage.Client()

def _upload_proof_and_hash(file_bytes: bytes, content_type: str, blob_path: str) -> dict:
    sha = hashlib.sha256(file_bytes).hexdigest()
    bucket = _storage_client().bucket(PROOF_BUCKET)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(file_bytes, content_type=content_type or "application/octet-stream")
    url = f"https://storage.googleapis.com/{bucket.name}/{blob_path}"
    return {"url": url, "sha256": sha}

# ---------- Address helpers (owner-friendly mode) ----------
_slug_re_nonword = re.compile(r"[^a-z0-9\-]+")
_slug_re_ws = re.compile(r"[\s_]+")

def _slugify(text: str) -> str:
    if not text:
        return ""
    s = text.strip().lower()
    s = _slug_re_ws.sub("-", s)
    s = _slug_re_nonword.sub("-", s)
    s = s.strip("-")
    return s or f"loc-{int(time.time())}"

def _address_to_string(address_any) -> str:
    """Accept either string or dict {province,district,zipcode,...} and return a single-line string.
    Stores the original as address.raw later.
    """
    if not address_any:
        return ""
    if isinstance(address_any, str):
        return address_any
    if isinstance(address_any, dict):
        parts = [
            address_any.get("line1"),
            address_any.get("line2"),
            address_any.get("district"),
            address_any.get("province"),
            address_any.get("zipcode"),
            address_any.get("country"),
        ]
        return ", ".join([p for p in parts if p])
    return str(address_any)

def _geocode_address(address_str: str):
    """Call Google Geocoding API to resolve textual address -> (lat, lng, components_dict)."""
    if not MAPS_API_KEY:
        raise RuntimeError("MAPS_API_KEY not configured")
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address_str, "key": MAPS_API_KEY, "language": "th"}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK" or not data.get("results"):
        raise RuntimeError(f"Geocoding failed: {data.get('status')}")
    r0 = data["results"][0]
    loc = r0["geometry"]["location"]
    lat = float(loc["lat"])
    lng = float(loc["lng"])
    comp_map = {c["types"][0]: c.get("long_name") for c in r0.get("address_components", []) if c.get("types")}
    components = {
        "province": comp_map.get("administrative_area_level_1"),
        "district": comp_map.get("sublocality") or comp_map.get("locality") or comp_map.get("administrative_area_level_2"),
        "zipcode": comp_map.get("postal_code"),
        "country": comp_map.get("country"),
    }
    return lat, lng, components

# ---------- Health ----------
@app.get("/")
def root():
    return jsonify({"ok": True, "service": "line-oa-api", "time": datetime.now(timezone.utc).isoformat()}), 200

@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok"}), 200

# ---------- Diagnostic (หลัง deploy ใช้เช็ค Firestore/Ready) ----------
@app.get("/diag/ready")
def diag_ready():
    return jsonify({"status": "ready"}), 200

# ---------- Webhook (multi-tenant) ----------
@app.post("/webhook/<shop_id>")
def webhook(shop_id):
    """Dynamic, per-shop webhook:
    - read LINE secrets for this shop from Firestore/Secret Manager
    - validate signature with that secret
    - initialize LineBotApi with that access token
    - upsert customer, save message, and reply ack
    """
    g.shop_id = shop_id
    cfg = load_line_config_for_shop(shop_id)
    channel_secret = cfg.get("channel_secret")
    access_token = cfg.get("access_token")

    if not channel_secret or not access_token:
        abort(500, f"LINE config missing for shop {shop_id}")

    signature = request.headers.get("X-Line-Signature")
    body = request.get_data(as_text=True)
    logger.info(f"WEBHOOK shop_id={shop_id} body_len={len(body) if body else 0}")
    if not signature:
        abort(400, "X-Line-Signature missing")

    # create handler and api per request using shop config
    _handler = WebhookHandler(channel_secret)
    _api = LineBotApi(access_token)

    # register text handler dynamically
    from linebot.models import TextSendMessage  # v2
    def _on_text_message(event):
        try:
            user_id = event.source.user_id
            text = (event.message.text or "").strip()
            now = datetime.now(timezone.utc)

            # fetch profile (best-effort)
            profile = None
            try:
                p = _api.get_profile(user_id)
                profile = {
                    "display_name": getattr(p, "display_name", None),
                    "picture_url": getattr(p, "picture_url", None),
                }
            except Exception as e:
                logger.warning(f"get_profile failed: {e}")

            # upsert customer + interaction time
            upsert_customer(shop_id, user_id, (profile or {}).get("display_name"))

            # read session state
            sess = get_session_state(shop_id, user_id) or {"state": "idle"}
            state = sess.get("state", "idle")

            # intent entry keywords (Thai/EN minimal)
            intent_pay_keywords = {"แจ้งชำระเงิน", "ชำระเงิน", "โอนเงิน", "payment", "pay"}
            if state == "idle" and any(k in text for k in intent_pay_keywords):
                set_session_state(shop_id, user_id, "awaiting_amount")
                # save message
                save_message(shop_id, user_id, text, ts=now, direction="inbound")
                _api.reply_message(event.reply_token, TextSendMessage(text="โอเคครับ ระบุจำนวนเงินที่ต้องการแจ้งชำระ (เช่น 3200)"))
                return

            if state == "awaiting_amount":
                # extract amount (simple)
                amt = None
                try:
                    amt = float(text.replace(",", ""))
                except Exception:
                    pass
                if amt is None or amt <= 0:
                    _api.reply_message(event.reply_token, TextSendMessage(text="ขอจำนวนเงินเป็นตัวเลขนะครับ เช่น 3200"))
                    return
                set_session_state(shop_id, user_id, "awaiting_payment_proof", {"expected_amount": amt})
                save_message(shop_id, user_id, text, ts=now, direction="inbound")
                _api.reply_message(event.reply_token, TextSendMessage(text=f"รับยอด {amt:.2f} บาท แล้วครับ กรุณาแนบรูปสลิปโอนเงิน"))
                return

            # default: normal chat
            save_message(shop_id, user_id, text, ts=now, direction="inbound")
            _api.reply_message(event.reply_token, TextSendMessage(text="รับข้อความแล้วครับ/ค่ะ ✅"))
            logger.info(f"LINE text handled shop_id={shop_id} user_id={user_id}")
        except Exception as e:
            logger.exception(f"_on_text_message failed: {e}")

    # attach handler mapping dynamically
    _handler.add(MessageEvent, message=TextMessage)(_on_text_message)

    from linebot.models import TextSendMessage

    def _on_image_message(event):
        try:
            user_id = event.source.user_id
            now = datetime.now(timezone.utc)
            upsert_customer(shop_id, user_id)

            sess = get_session_state(shop_id, user_id) or {"state": "idle"}
            state = sess.get("state", "idle")
            if state != "awaiting_payment_proof":
                # Not in payment flow → treat as normal image; prompt to enter flow
                _api.reply_message(event.reply_token, TextSendMessage(
                    text="หากรูปนี้เป็นสลิปโอนเงิน ให้กด ‘แจ้งชำระเงิน’ ก่อน แล้วส่งรูปอีกครั้งนะครับ"))
                return

            expected_amount = sess.get("expected_amount")

            # download image content from LINE
            content = _api.get_message_content(event.message.id)
            file_bytes = b"".join(chunk for chunk in content.iter_content())
            mime = content.content_type or "image/jpeg"

            payment_id = f"pay_{int(now.timestamp())}"
            blob_path = f"{shop_id}/{user_id}/{payment_id}.jpg"
            uploaded = _upload_proof_and_hash(file_bytes, mime, blob_path)

            # create payment & update spending
            pay_doc = {
                "amount": float(expected_amount) if expected_amount is not None else 0.0,
                "currency": "THB",
                "method": "transfer",
                "status": "pending",  # start as pending; can be verified by backoffice
                "ts": now,
                "order_id": None,
                "proof_url": uploaded["url"],
                "message_id": event.message.id,
                "file_hash": f"sha256:{uploaded['sha256']}",
                "raw": {"source": "line", "content_type": mime},
            }
            create_payment(shop_id, user_id, payment_id, pay_doc)
            spending = update_customer_spending_and_tier(shop_id, user_id, pay_doc["amount"], ts=now)

            # reset session
            set_session_state(shop_id, user_id, "idle", {"expected_amount": None})

            _api.reply_message(event.reply_token, TextSendMessage(
                text=f"บันทึกการแจ้งชำระแล้วครับ payment_id={payment_id} ยอด {pay_doc['amount']:.2f} บาท ✅"))
        except Exception as e:
            logger.exception(f"_on_image_message failed: {e}")
            _api.reply_message(event.reply_token, TextSendMessage(text="เกิดข้อผิดพลาดขณะบันทึกสลิป กรุณาลองใหม่ครับ"))

    _handler.add(MessageEvent, message=ImageMessage)(_on_image_message)

    try:
        _handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400, "Invalid signature")

    return "OK", 200

# ---------- API: customers ----------
@app.get("/api/v1/shops/<shop_id>/customers")
def api_list_customers(shop_id):
    require_auth()
    limit = int(request.args.get("limit", "20"))
    last_doc_id = request.args.get("lastDocId")
    items = list_customers(shop_id, limit=limit, last_doc_id=last_doc_id)
    return jsonify({"ok": True, "items": items}), 200

# ---------- API: messages ----------
@app.get("/api/v1/shops/<shop_id>/messages")
def api_list_messages(shop_id):
    require_auth()
    customer_id = request.args.get("customerId")
    if not customer_id:
        abort(400, "customerId is required")
    limit = int(request.args.get("limit", "50"))
    items = list_messages(shop_id, customer_id, limit=limit)
    return jsonify({"ok": True, "items": items}), 200

# ---------- API: products (บริการ/แพ็กเกจ) ----------
@app.get("/api/v1/shops/<shop_id>/products")
def api_list_products(shop_id):
    require_auth()
    items = list_products(shop_id)
    return jsonify({"ok": True, "items": items}), 200

@app.post("/api/v1/shops/<shop_id>/products")
def api_create_product(shop_id):
    require_auth()
    try:
        data = request.get_json(silent=True) or {}
        product_id = create_product(shop_id, data)
        return jsonify({"ok": True, "product_id": product_id}), 201
    except Exception as e:
        logger.exception(f"POST /products failed: shop_id={shop_id}")
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------- API: promotions ----------
@app.post("/api/v1/shops/<shop_id>/promotions")
def api_create_promotion(shop_id):
    require_auth()
    try:
        data = request.get_json(silent=True) or {}
        promo_id = create_promotion(shop_id, data)
        return jsonify({"ok": True, "promotion_id": promo_id}), 201
    except Exception as e:
        logger.exception(f"POST /promotions failed: shop_id={shop_id}")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/api/v1/shops/<shop_id>/promotions")
def api_list_promos(shop_id):
    require_auth()
    status = request.args.get("status")
    try:
        items = list_promotions(shop_id, status=status)
        return jsonify({"ok": True, "items": items}), 200
    except Exception as e:
        logger.exception(f"GET /promotions failed: shop_id={shop_id}, status={status}")
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------- API: locations nearby ----------
@app.get("/api/v1/locations/nearby")
def api_locations_nearby():
    require_auth()
    shop_id = request.args.get("shop_id")
    lat = request.args.get("lat")
    lng = request.args.get("lng")
    product_id = request.args.get("product_id")
    radius_km = float(request.args.get("radius_km", "30"))
    precision = int(request.args.get("precision", "5"))
    if not shop_id or not lat or not lng:
        abort(400, "shop_id, lat, lng are required")
    try:
        lat = float(lat); lng = float(lng)
    except Exception:
        abort(400, "lat/lng must be numbers")

    prefix = geohash2.encode(lat, lng, precision=precision)
    candidates = list(list_locations_by_geohash_prefix(shop_id, prefix, limit=200))
    # optional filter by product
    if product_id:
        candidates = [c for c in candidates if product_id in (c.get("in_stock_products") or [])]

    for c in candidates:
        try:
            c["distance_km"] = round(haversine_km(lat, lng, c["lat"], c["lng"]), 2)
        except Exception:
            c["distance_km"] = None
    results = [c for c in candidates if c["distance_km"] is not None and c["distance_km"] <= radius_km]
    results.sort(key=lambda x: x["distance_km"])
    return jsonify({"ok": True, "count": len(results), "items": results}), 200

# ---------- API: locations upsert (single or bulk) ----------
@app.post("/api/v1/shops/<shop_id>/locations")
def api_upsert_locations(shop_id):
    """
    Owner-friendly upsert:
    - Accepts single object or array.
    - Supports two modes per item:
      (A) Full: {id, lat, lng, name, address, in_stock_products, is_active}
      (B) Easy: {name, address, in_stock_products, is_active} → auto id + geocode address
    - address may be a string or an object; stored as {raw, province, district, zipcode, ...}
    """
    require_auth()
    try:
        payload = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid or missing JSON body"}), 400

    items = payload if isinstance(payload, list) else [payload]
    if not items:
        return jsonify({"ok": False, "error": "Empty payload"}), 400

    dbi = get_db()
    batch = dbi.batch()
    now = datetime.now(timezone.utc)
    upserted = []
    errors = []

    for idx, it in enumerate(items):
        try:
            it = it or {}
            loc_id = it.get("id") or it.get("_id")
            name = (it.get("name") or "").strip() or None
            in_stock_products = it.get("in_stock_products") or []
            is_active = bool(it.get("is_active", True))
            address_in = it.get("address")
            lat = it.get("lat")
            lng = it.get("lng")

            # If lat/lng missing but address present -> geocode
            resolved_address = {}
            address_raw = _address_to_string(address_in)
            if (lat is None or lng is None) and address_raw:
                try:
                    g_lat, g_lng, comps = _geocode_address(address_raw)
                    lat, lng = g_lat, g_lng
                    resolved_address.update({k: v for k, v in comps.items() if v})
                except Exception as ge:
                    raise ValueError(f"geocode_failed: {ge}")

            # Validate lat/lng finally
            try:
                lat = float(lat); lng = float(lng)
            except Exception:
                raise ValueError("lat_lng_missing_or_invalid")

            # Auto-generate id if missing
            if not loc_id:
                base = _slugify(name or address_raw or f"loc-{int(time.time())}")
                loc_id = base or f"loc-{int(time.time())}"

            # Compute geohash (precision=7 convention)
            gh = geohash2.encode(lat, lng, precision=7)

            # Normalize address object
            addr_obj = {}
            if address_raw:
                addr_obj["raw"] = address_raw
            if isinstance(address_in, dict):
                addr_obj.update({k: v for k, v in address_in.items() if v})
            addr_obj.update({k: v for k, v in resolved_address.items() if v})

            doc = {
                "name": name,
                "address": addr_obj,
                "lat": lat,
                "lng": lng,
                "geohash": gh,
                "in_stock_products": in_stock_products,
                "is_active": is_active,
                "updated_at": now,
            }
            if it.get("created_at") is None:
                doc["created_at"] = now

            ref = (dbi.collection("shops").document(shop_id)
                      .collection("locations").document(loc_id))
            batch.set(ref, doc, merge=True)
            upserted.append(loc_id)
        except Exception as e:
            errors.append({"index": idx, "id": it.get("id") or it.get("_id"), "error": str(e)})

    if not upserted:
        return jsonify({"ok": False, "error": "No valid items to upsert", "details": errors}), 400

    batch.commit()
    return jsonify({"ok": True, "upserted": upserted, "errors": errors}), 201

# ---------- API: payments ----------
@app.post("/api/v1/shops/<shop_id>/customers/<customer_id>/payments")
def api_create_payment(shop_id, customer_id):
    require_auth()
    now = datetime.now(timezone.utc)
    payment_id = request.args.get("payment_id") or f"pay_{int(now.timestamp())}"
    data_json = request.get_json(silent=True) or {}
    form = request.form or {}

    amount = form.get("amount") or data_json.get("amount")
    currency = form.get("currency") or data_json.get("currency") or "THB"
    method = form.get("method") or data_json.get("method") or "transfer"
    status = form.get("status") or data_json.get("status") or "pending"
    order_id = form.get("order_id") or data_json.get("order_id")

    try:
        amount = float(amount)
    except Exception:
        abort(400, "amount is required and must be number")

    proof_url = None
    file_hash = None
    if request.files.get("file"):
        f = request.files["file"]
        content = f.read()
        path = f"{shop_id}/{customer_id}/{payment_id}"
        # try to infer extension
        ext = ".jpg"
        if f.mimetype and "png" in f.mimetype:
            ext = ".png"
        uploaded = _upload_proof_and_hash(content, f.mimetype or "application/octet-stream", path + ext)
        proof_url = uploaded["url"]; file_hash = f"sha256:{uploaded['sha256']}"
    else:
        proof_url = data_json.get("proof_url")

    pay_doc = {
        "amount": amount,
        "currency": currency,
        "method": method,
        "status": status,
        "ts": now,
        "order_id": order_id,
        "proof_url": proof_url,
        "message_id": None,
        "file_hash": file_hash,
        "raw": {},
    }
    create_payment(shop_id, customer_id, payment_id, pay_doc)
    spending = update_customer_spending_and_tier(shop_id, customer_id, amount, ts=now)
    return jsonify({"ok": True, "payment_id": payment_id, "spending": spending}), 201

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)