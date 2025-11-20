import uuid, logging, os
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from google.cloud import storage, firestore
from firestore_client import get_db
from urllib.parse import quote

MEDIA_BUCKET = os.getenv("MEDIA_BUCKET")
MEDIA_PUBLIC_BASE = os.getenv("MEDIA_PUBLIC_BASE")

def _now():
    return datetime.now(timezone.utc)

def _sessions():
    return get_db().collection("onboarding").document("sessions").collection("users")

def _requests():
    return get_db().collection("onboarding").document("requests").collection("items")

def _payload_fingerprint(data: Dict[str, Any]) -> str:
    name = (data.get("name") or "").strip()
    phone = (data.get("phone") or "").strip()
    shop = (data.get("shop") or "").strip()
    loc = data.get("location") or {}
    loc_key = f"{loc.get('lat')}|{loc.get('lng')}|{loc.get('address')}"
    logo = (data.get("logo_url") or "").strip()
    payment_map = data.get("payment") or {}
    pay_promptpay = (payment_map.get("payment_promptpay")
                     or data.get("payment_promptpay") or "").strip()
    pay_note = (payment_map.get("payment_note")
                or data.get("payment_note") or "").strip()
    pay_qr = (payment_map.get("payment_qr_url")
              or data.get("payment_qr_url") or "").strip()
    return f"{name}|{phone}|{shop}|{loc_key}|{logo}|{pay_promptpay}|{pay_note}|{pay_qr}"

def get_session(user_id: str) -> Dict[str, Any]:
    snap = _sessions().document(user_id).get()
    return snap.to_dict() if snap.exists else {}

def save_session(user_id: str, data: Dict[str, Any]):
    data["updated_at"] = _now()
    _sessions().document(user_id).set(data, merge=True)

def clear_session(user_id: str):
    _sessions().document(user_id).delete()

def upload_logo_bytes(user_id: str, content: bytes, content_type: Optional[str]) -> Optional[str]:
    if not MEDIA_BUCKET or not content:
        return None
    try:
        client = storage.Client()
        bucket = client.bucket(MEDIA_BUCKET)
        blob_name = f"shops/_onboarding/{user_id}/{uuid.uuid4().hex}.jpg"
        blob = bucket.blob(blob_name)
        try:
            blob.cache_control = "public, max-age=86400"
        except Exception:
            pass
        blob.upload_from_string(content, content_type=content_type or "image/jpeg")
        base = MEDIA_PUBLIC_BASE or f"https://storage.googleapis.com/{MEDIA_BUCKET}"
        return f"{base}/{blob_name}"
    except Exception as e:
        logging.getLogger("onboarding").error("upload_logo_bytes failed: %s", e)
        return None


def upload_payment_qr_bytes(user_id: str, content: bytes, content_type: Optional[str]) -> Optional[str]:
    if not MEDIA_BUCKET or not content:
        return None
    try:
        client = storage.Client()
        bucket = client.bucket(MEDIA_BUCKET)
        suffix = ".png"
        ct = (content_type or "").lower()
        if ct and "jpeg" in ct:
            suffix = ".jpg"
        blob_name = f"shops/_onboarding/{user_id}/payment_qr/{uuid.uuid4().hex}{suffix}"
        blob = bucket.blob(blob_name)
        try:
            blob.cache_control = "public, max-age=86400"
        except Exception:
            pass
        blob.upload_from_string(content, content_type=content_type or ("image/jpeg" if suffix == ".jpg" else "image/png"))
        base = MEDIA_PUBLIC_BASE or f"https://storage.googleapis.com/{MEDIA_BUCKET}"
        return f"{base}/{blob_name}"
    except Exception as e:
        logging.getLogger("onboarding").error("upload_payment_qr_bytes failed: %s", e)
        return None

def finalize_request_from_session(user_id: str) -> Optional[str]:
    s = get_session(user_id)
    if not s or not s.get("name") or not s.get("phone") or not s.get("shop"):
        return None
    fp = _payload_fingerprint(s)
    now = _now()
    logger = logging.getLogger("onboarding")
    try:
        q = (
            get_db()
            .collection("onboarding")
            .document("requests")
            .collection("items")
            .where("user_id", "==", user_id)
            .limit(25)
        )
        for doc in q.stream():
            data = doc.to_dict() or {}
            if data.get("status") == "pending" and data.get("fingerprint") == fp:
                _requests().document(doc.id).set({
                    "last_submitted_at": now,
                    "updated_at": now,
                }, merge=True)
                return doc.id
    except Exception as e:
        logger.warning("finalize_request dedupe failed: %s", e)
    req_id = uuid.uuid4().hex
    payload = {
        "user_id": user_id,
        "messaging_user_id": s.get("messaging_user_id") or user_id,
        "name": s.get("name"),
        "phone": s.get("phone"),
        "shop": s.get("shop"),
        "location": s.get("location"),
        "logo_url": s.get("logo_url"),
        "status": "pending",
        "created_at": now,
        "updated_at": now,
        "fingerprint": fp,
        "source": "OA-Admin(A>B)",
    }
    payment = {
        "payment_promptpay": (s.get("payment_promptpay") or "").strip() or None,
        "payment_note": (s.get("payment_note") or "").strip() or None,
        "payment_qr_url": (s.get("payment_qr_url") or "").strip() or None,
    }
    payload["payment"] = payment
    _requests().document(req_id).set(payload, merge=False)
    return req_id
def to_flex_summary(session: Dict[str, Any]) -> Dict[str, Any]:
    shop = session.get("shop") or "(ไม่ระบุชื่อร้าน)"
    name = session.get("name") or "-"
    phone = session.get("phone") or "-"
    address = (session.get("location") or {}).get("address") or "ธุรกิจออนไลน์ ไม่มีหน้าร้าน"
    safe = quote(shop, safe="")
    logo_url = session.get("logo_url") or f"https://dummyimage.com/600x400/cccccc/000000&text={safe}"

    # --- payment summary from session ---
    pay_promptpay = (session.get("payment_promptpay") or "").strip()
    pay_note = (session.get("payment_note") or "").strip()
    pay_qr_url = (session.get("payment_qr_url") or "").strip()

    pay_lines = []
    if pay_promptpay:
        pay_lines.append(f"PromptPay/บัญชี: {pay_promptpay}")
    if pay_note:
        pay_lines.append(f"หมายเหตุ: {pay_note}")
    if pay_qr_url:
        pay_lines.append("มี QR code สำหรับรับเงินแนบแล้ว")

    if pay_lines:
        pay_text = "\n".join(pay_lines)
    else:
        pay_text = "ยังไม่ระบุ (สามารถแจ้งในแชทเพิ่มเติมได้)"

    return {
        "type": "flex",
        "altText": "สรุปข้อมูลร้านค้า",
        "contents": {
            "type": "bubble",
            "hero": {
                "type": "image",
                "url": logo_url,
                "size": "full",
                "aspectRatio": "20:13",
                "aspectMode": "cover",
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": shop,
                        "weight": "bold",
                        "size": "xl",
                        "color": "#1DB446",
                    },
                    {
                        "type": "text",
                        "text": "บัญชีร้านค้าอย่างเป็นทางการ",
                        "size": "sm",
                        "margin": "sm",
                    },
                    {"type": "separator", "margin": "md"},
                    {
                        "type": "box",
                        "layout": "vertical",
                        "margin": "md",
                        "spacing": "sm",
                        "contents": [
                            {
                                "type": "text",
                                "text": "ข้อมูลร้านค้า",
                                "weight": "bold",
                                "size": "md",
                                "margin": "sm",
                            },
                            {
                                "type": "box",
                                "layout": "vertical",
                                "spacing": "xs",
                                "contents": [
                                    {
                                        "type": "box",
                                        "layout": "baseline",
                                        "spacing": "sm",
                                        "contents": [
                                            {
                                                "type": "text",
                                                "text": "ชื่อผู้ติดต่อ",
                                                "color": "#AAAAAA",
                                                "size": "sm",
                                                "flex": 2,
                                            },
                                            {
                                                "type": "text",
                                                "text": name,
                                                "wrap": True,
                                                "size": "sm",
                                                "flex": 4,
                                            },
                                        ],
                                    },
                                    {
                                        "type": "box",
                                        "layout": "baseline",
                                        "spacing": "sm",
                                        "contents": [
                                            {
                                                "type": "text",
                                                "text": "เบอร์โทร",
                                                "color": "#AAAAAA",
                                                "size": "sm",
                                                "flex": 2,
                                            },
                                            {
                                                "type": "text",
                                                "text": phone,
                                                "wrap": True,
                                                "size": "sm",
                                                "flex": 4,
                                            },
                                        ],
                                    },
                                    {
                                        "type": "box",
                                        "layout": "baseline",
                                        "spacing": "sm",
                                        "contents": [
                                            {
                                                "type": "text",
                                                "text": "ที่อยู่ร้าน",
                                                "color": "#AAAAAA",
                                                "size": "sm",
                                                "flex": 2,
                                            },
                                            {
                                                "type": "text",
                                                "text": address,
                                                "wrap": True,
                                                "size": "sm",
                                                "flex": 4,
                                                "color": "#444444",
                                            },
                                        ],
                                    },
                                    {
                                        "type": "box",
                                        "layout": "baseline",
                                        "spacing": "sm",
                                        "contents": [
                                            {
                                                "type": "text",
                                                "text": "ช่องทางรับเงิน",
                                                "color": "#AAAAAA",
                                                "size": "sm",
                                                "flex": 2,
                                            },
                                            {
                                                "type": "text",
                                                "text": pay_text,
                                                "wrap": True,
                                                "size": "sm",
                                                "flex": 4,
                                                "color": "#444444",
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                    {"type": "separator", "margin": "xl"},
                    {
                        "type": "text",
                        "text": "💬 หากต้องการเปลี่ยนแปลง พิมพ์รายละเอียดเพิ่มในแชทได้เลย หรือกดปุ่มด้านล่าง",
                        "wrap": True,
                        "size": "xs",
                        "color": "#888888",
                        "margin": "lg",
                        "align": "center",
                    },
                ],
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "flex": 0,
                "contents": [
                    {
                        "type": "button",
                        "style": "primary",
                        "color": "#1DB446",
                        "height": "sm",
                        "action": {
                            "type": "message",
                            "label": "ยืนยันข้อมูล",
                            "text": "ยืนยันข้อมูล",
                        },
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "height": "sm",
                        "margin": "md",
                        "action": {
                            "type": "message",
                            "label": "แก้ไขข้อมูล",
                            "text": "แก้ไขข้อมูล",
                        },
                    },
                ],
            },
        },
    }
