

# core/secrets.py
from __future__ import annotations
from typing import Dict, Any, Optional
import os, json

try:
    from services.firestore_client import get_db
except Exception:
    from firestore_client import get_db

try:
    from google.cloud import secretmanager
    _SM_AVAILABLE = True
except Exception:
    _SM_AVAILABLE = False

# Optional DAO helpers for mapping destination -> shop_id
try:
    from dao import get_shop_id_by_bot_user_id, get_shop_id_by_line_oa_id
except Exception:
    def get_shop_id_by_bot_user_id(dest: str) -> Optional[str]:
        return None
    def get_shop_id_by_line_oa_id(cid: str) -> Optional[str]:
        return None

def _get_settings_by_shop_id(shop_id: str) -> Dict[str, Any]:
    db = get_db()
    snap = db.collection("shops").document(shop_id).collection("settings").document("default").get()
    return snap.to_dict() if snap.exists else {}

def _lookup(settings: Dict[str, Any], path: str) -> Optional[Any]:
    if not settings or not path:
        return None
    if "." not in path:
        return settings.get(path)
    cur: Any = settings
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur

def resolve_secret(settings: Dict[str, Any], direct_key: str, sm_key: str) -> Optional[str]:
    """Resolve a secret either from plain settings[direct_key] or Secret Manager reference in settings[sm_key]."""
    val = _lookup(settings, direct_key)
    if val:
        return val
    sm_res = _lookup(settings, sm_key)
    if not sm_res:
        return None
    if not _SM_AVAILABLE:
        return None
    try:
        sm = secretmanager.SecretManagerServiceClient()
        resp = sm.access_secret_version(name=sm_res)
        return resp.payload.data.decode("utf-8")
    except Exception:
        return None

def load_shop_context_by_destination(destination: str) -> Optional[Dict[str, Any]]:
    """
    Map LINE webhook 'destination' (bot_user_id; often starts with 'U') or channelId -> {shop_id, settings}.
    Falls back to DEFAULT_SHOP_ID for dev if mapping is missing.
    """
    if not destination:
        return None
    # 1) Preferred: bot_user_id mapping
    shop_id = get_shop_id_by_bot_user_id(destination)
    # 2) Legacy fallback: numeric channelId
    if (not shop_id) and destination.isdigit():
        shop_id = get_shop_id_by_line_oa_id(destination)
    # 3) Dev fallback
    if not shop_id:
        default_sid = os.getenv("DEFAULT_SHOP_ID", "").strip()
        if default_sid:
            settings = _get_settings_by_shop_id(default_sid) or {}
            return {"shop_id": default_sid, "settings": settings}
        return None
    settings = _get_settings_by_shop_id(shop_id) or {}
    return {"shop_id": shop_id, "settings": settings}
