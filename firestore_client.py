"""
firestore_client.py — Minimal Firestore initializer (no circular imports)
- Provides: get_db()
- Credential priority: FIREBASE_SERVICE_ACCOUNT_JSON (path) > FIREBASE_CONFIG_JSON (inline JSON) > ADC
- Logs project used on init
"""
from __future__ import annotations

import os
import json
import logging
from typing import Optional

import firebase_admin
from firebase_admin import credentials, firestore

__all__ = ["get_db"]

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logger = logging.getLogger("firestore-client")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------
_db: Optional[firestore.Client] = None
_inited: bool = False


def _project_id() -> Optional[str]:
    return (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GCLOUD_PROJECT")
    )


def _init_firebase_app() -> None:
    """Initialize firebase_admin app exactly once using the best available creds."""
    if firebase_admin._apps:  # already initialized
        return

    proj = _project_id()
    sa_path = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    sa_json = os.environ.get("FIREBASE_CONFIG_JSON")

    if sa_path and os.path.isfile(sa_path):
        logger.info("Initializing Firebase with SA file: %s", sa_path)
        cred = credentials.Certificate(sa_path)
        firebase_admin.initialize_app(cred, {"projectId": proj} if proj else None)
        return

    if sa_json:
        try:
            logger.info("Initializing Firebase with SA JSON from env")
            cred = credentials.Certificate(json.loads(sa_json))
            firebase_admin.initialize_app(cred, {"projectId": proj} if proj else None)
            return
        except Exception:
            logger.exception("Invalid FIREBASE_CONFIG_JSON; falling back to ADC")

    logger.info("Initializing Firebase with ADC (Application Default Credentials)")
    firebase_admin.initialize_app()


def get_db() -> firestore.Client:
    """Return a cached Firestore client. Initializes on first call.

    Environment variables respected:
      - GOOGLE_CLOUD_PROJECT / GCP_PROJECT_ID / GCLOUD_PROJECT
      - FIREBASE_SERVICE_ACCOUNT_JSON (path)
      - FIREBASE_CONFIG_JSON (inline JSON)
    """
    global _db, _inited
    if _db is not None and _inited:
        return _db

    try:
        _init_firebase_app()
        _db = firestore.client()
        _inited = True
        try:
            proj = _project_id() or getattr(_db, "project", None)
        except Exception:
            proj = _project_id()
        logger.info("Firestore initialized (project=%s)", proj)
        return _db
    except Exception as e:
        logger.exception("Failed to initialize Firestore: %s", e)
        raise


# ---------------------------------------------------------------------
# (Optional) testing helper
# ---------------------------------------------------------------------
def _reset_db_for_tests() -> None:
    """Reset cached client (useful in unit tests)."""
    global _db, _inited
    _db = None
    _inited = False