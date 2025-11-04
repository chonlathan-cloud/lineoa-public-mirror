

# core/pubsub.py
from __future__ import annotations
from typing import Tuple, Dict, Any
import os, json, base64
from flask import Request

def verify_pubsub_token(req: "Request") -> bool:
    """
    Verify a simple shared token for Pub/Sub push (query ?token=... or header X-PubSub-Token).
    If PUBSUB_TOKEN is not set, return True (no protection).
    """
    want = (os.environ.get("PUBSUB_TOKEN") or "").strip()
    if not want:
        return True
    got = (req.args.get("token") if hasattr(req, "args") else None) or req.headers.get("X-PubSub-Token", "")
    got = (got or "").strip()
    return bool(got) and (got == want)

def parse_pubsub_envelope(req: "Request") -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Parse Pub/Sub push JSON envelope -> (attributes:dict, data:dict). Safe-fail.
    """
    try:
        env = req.get_json(force=True, silent=True) or {}
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
    except Exception:
        return {}, {}