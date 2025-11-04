# app_admin.py — Cloud Run entrypoint (admin/B side)
from flask import Flask
from flask_cors import CORS
import traceback
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_admin")

def create_app():
    app = Flask(__name__)
    import os
    app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret')  # override via ENV
    CORS(app)

    # Import blueprints with diagnostics
    admin_bp = None
    owner_bp = None

    try:
        from admin.blueprint import admin_bp as _abp
        admin_bp = _abp
        logger.info("✅ Loaded admin.blueprint.admin_bp")
    except Exception as e:
        logger.error("❌ Failed to load admin.blueprint: %s\n%s", e, traceback.format_exc())

    try:
        from owner.blueprint import owner_bp as _obp
        owner_bp = _obp
        logger.info("✅ Loaded owner.blueprint.owner_bp")
    except Exception as e:
        logger.error("❌ Failed to load owner.blueprint: %s\n%s", e, traceback.format_exc())

    if admin_bp:
        app.register_blueprint(admin_bp)
        logger.info("🔗 Registered admin blueprint")
    if owner_bp:
        app.register_blueprint(owner_bp)
        logger.info("🔗 Registered owner blueprint")

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "admin": bool(admin_bp), "owner": bool(owner_bp)}

    # Debug: list all routes
    @app.get("/debug/routes")
    def debug_routes():
        routes = []
        for r in app.url_map.iter_rules():
            routes.append({"rule": str(r), "endpoint": r.endpoint, "methods": sorted([m for m in r.methods if m not in ("HEAD","OPTIONS")])})
        return {"routes": routes}

    # Friendly index (helps confirm blueprint registration quickly)
    @app.get("/")
    def index():
        return {
            "message": "LINE OA Admin service",
            "health": "/healthz",
            "routes": "/debug/routes",
            "hint": "Expect to see /owner/<shop_id>/reports/request, /owner/<shop_id>/reports/requests, etc."
        }

    return app

app = create_app()