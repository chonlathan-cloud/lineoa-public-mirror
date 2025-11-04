from flask import Blueprint

owner_bp = Blueprint("owner", __name__)

@owner_bp.get("/owner/ping")
def owner_ping():
    return {"ok": True, "role": "owner"}