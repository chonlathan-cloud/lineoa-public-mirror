

from __future__ import annotations
from flask import Blueprint, request, redirect, url_for, render_template_string
import os
from datetime import datetime, timezone as dtz
from typing import Any, Dict, List, Optional
from firestore_client import get_db
from google.cloud import storage

owner_bp = Blueprint("owner_bp", __name__, url_prefix="/owner")

HTML_BASE = """
<!doctype html>
<html lang="th">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Owner · {{ title }}</title>
  <style>
    :root{--brand:#2B5EA4;--accent:#7FADEB;--ink:#0b1324}
    *{box-sizing:border-box}
    body{font-family: -apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif; color:var(--ink); margin:16px}
    header{display:flex;gap:10px;align-items:center;background:linear-gradient(90deg,var(--accent),#d8e8ff);padding:10px 12px;border-radius:10px;margin-bottom:10px}
    h1{margin:0;font-size:18px}
    a{color:var(--brand);text-decoration:none}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    form{display:grid;gap:8px;background:#f8fbff;border:1px solid #e6eefc;padding:10px;border-radius:10px}
    input{padding:8px;border:1px solid #cad7ef;border-radius:8px}
    table{width:100%;border-collapse:collapse;background:#fff}
    th,td{border:1px solid #e6eefc;padding:8px;font-size:13px}
    th{background:#f8fbff;text-align:left}
    .muted{color:#6b7280;font-size:12px}
    .btn{display:inline-block;padding:6px 10px;border-radius:8px;border:1px solid #2B5EA4;background:#2B5EA4;color:#fff}
    .btn.subtle{background:#eef5ff;color:#1a3e72;border-color:#cad7ef}
  </style>
</head>
<body>
  <header>
    <div><h1>{{ heading }}</h1><div class="muted">{{ subtitle }}</div></div>
    <div style="margin-left:auto" class="muted">shop_id: <b>{{ shop_id }}</b></div>
  </header>
  {% block content %}{% endblock %}
</body>
</html>
"""

def _utcnow() -> datetime:
  return datetime.now(dtz.utc)

@owner_bp.route("/<shop_id>/profile", methods=["GET","POST"])
def profile(shop_id: str):
  db = get_db()
  ref = db.collection("shops").document(shop_id).collection("owner_profile").document("default")
  if request.method == "POST":
    full_name = request.form.get("full_name","").strip()
    phone = request.form.get("phone","").strip()
    business_name = request.form.get("business_name","").strip()
    ref.set({"full_name":full_name,"phone":phone,"business_name":business_name,"updated_at":_utcnow()}, merge=True)
    return redirect(url_for("owner_bp.profile", shop_id=shop_id))
  snap = ref.get()
  data = snap.to_dict() if snap.exists else {}
  content_tpl = """
  <div class="grid">
    <div>
      <h3>Owner Profile</h3>
      <form method="post">
        <label>Full name <input name="full_name" value="{{ data.get('full_name','') }}"></label>
        <label>Phone <input name="phone" value="{{ data.get('phone','') }}"></label>
        <label>Business name <input name="business_name" value="{{ data.get('business_name','') }}"></label>
        <div><button class="btn" type="submit">Save</button></div>
      </form>
    </div>
    <div>
      <h3>Current</h3>
      <table>
        <tr><th>Full name</th><td>{{ data.get('full_name','-') }}</td></tr>
        <tr><th>Phone</th><td>{{ data.get('phone','-') }}</td></tr>
        <tr><th>Business</th><td>{{ data.get('business_name','-') }}</td></tr>
      </table>
    </div>
  </div>
  """
  tpl = HTML_BASE.replace("{% block content %}{% endblock %}", content_tpl)
  return render_template_string(tpl, title="Profile", heading="Owner · Profile", subtitle="ข้อมูลเจ้าของร้าน", shop_id=shop_id, data=data)

@owner_bp.route("/<shop_id>/owners", methods=["GET","POST"])
def owners(shop_id: str):
  db = get_db()
  col = db.collection("shops").document(shop_id).collection("owners")
  if request.method == "POST":
    uid = request.form.get("user_id","").strip()
    if uid:
      col.document(uid).set({"active": True, "updated_at": _utcnow()}, merge=True)
    return redirect(url_for("owner_bp.owners", shop_id=shop_id))
  docs = list(col.stream())
  items = [dict(id=d.id, **d.to_dict()) for d in docs]
  content_tpl = """
  <div class="grid">
    <div>
      <h3>Add Owner (LINE userId)</h3>
      <form method="post">
        <label>userId <input name="user_id" required placeholder="Uxxxxxxxx..."></label>
        <div><button class="btn" type="submit">Add</button></div>
        <div class="muted">* เพิ่ม userId ที่จะรับรายงานผ่าน LINE</div>
      </form>
    </div>
    <div>
      <h3>Owners</h3>
      <table>
        <thead><tr><th>userId</th><th>active</th><th>Actions</th></tr></thead>
        <tbody>
          {% for o in items %}
          <tr>
            <td>{{ o.id }}</td>
            <td>{{ 'true' if o.get('active') else 'false' }}</td>
            <td>
              <form method="post" action="{{ url_for('owner_bp.owner_toggle', shop_id=shop_id, user_id=o.id) }}">
                <button class="btn subtle" type="submit">{{ 'Deactivate' if o.get('active') else 'Activate' }}</button>
              </form>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>
  """
  tpl = HTML_BASE.replace("{% block content %}{% endblock %}", content_tpl)
  return render_template_string(tpl, title="Owners", heading="Owner · Receivers", subtitle="ผู้รับรายงานผ่าน LINE", shop_id=shop_id, items=items)

@owner_bp.route("/<shop_id>/owners/<user_id>/toggle", methods=["POST"])
def owner_toggle(shop_id: str, user_id: str):
  db = get_db()
  ref = db.collection("shops").document(shop_id).collection("owners").document(user_id)
  snap = ref.get()
  cur = True
  if snap.exists:
    cur = bool(snap.to_dict().get("active", True))
  ref.set({"active": not cur, "updated_at": _utcnow()}, merge=True)
  return redirect(url_for("owner_bp.owners", shop_id=shop_id))

@owner_bp.route("/<shop_id>/reports")
def reports(shop_id: str):
  # List latest reports from GCS public bucket
  bucket = os.environ.get("REPORT_BUCKET", "lineoa-report-for-owner")
  prefix = f"reports/{shop_id}/"
  client = storage.Client()
  blobs = list(client.list_blobs(bucket, prefix=prefix))
  # Sort newest first by name (report_id timestamp yyyymmddHHMMSS.pdf)
  blobs.sort(key=lambda b: b.name, reverse=True)
  items = [{"name": b.name.split("/")[-1],
            "public_url": f"https://storage.googleapis.com/{bucket}/{b.name}",
            "size": b.size} for b in blobs[:30]]
  content_tpl = """
  <h3>Latest Reports</h3>
  <table>
    <thead><tr><th>File</th><th>Size</th><th>Open</th></tr></thead>
    <tbody>
      {% for it in items %}
      <tr>
        <td>{{ it.name }}</td>
        <td>{{ it.size }} bytes</td>
        <td><a class="btn subtle" target="_blank" href="{{ it.public_url }}">Open</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  <div class="muted">* ใช้ public URL จาก GCS (ต้องเปิดสิทธิ์ allUsers: Reader ที่ bucket)</div>
  """
  tpl = HTML_BASE.replace("{% block content %}{% endblock %}", content_tpl)
  return render_template_string(tpl, title="Reports", heading="Owner · Reports", subtitle="รายงานล่าสุด", shop_id=shop_id, items=items)