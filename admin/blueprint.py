

from __future__ import annotations
from flask import Blueprint, request, redirect, url_for, render_template_string, flash
import os
from datetime import datetime, timezone as dtz
from typing import Any, Dict, List, Optional
from firestore_client import get_db

admin_bp = Blueprint("admin_bp", __name__, url_prefix="/admin")

HTML_BASE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Admin · {{ title }}</title>
  <style>
    :root{--brand:#2B5EA4;--accent:#7FADEB;--ink:#0b1324}
    *{box-sizing:border-box}
    body{font-family: -apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif; color:var(--ink); margin:16px}
    header{display:flex;gap:10px;align-items:center;background:linear-gradient(90deg,var(--accent),#d8e8ff);padding:10px 12px;border-radius:10px;margin-bottom:10px}
    h1{margin:0;font-size:18px}
    a{color:var(--brand);text-decoration:none}
    .tabs{display:flex;gap:12px;margin:8px 0 12px}
    .tab{padding:6px 10px;border:1px solid #d8e8ff;border-radius:999px;background:#f5f9ff}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    table{width:100%;border-collapse:collapse;background:#fff}
    th,td{border:1px solid #e6eefc;padding:8px;font-size:13px}
    th{background:#f8fbff;text-align:left}
    form{display:grid;gap:8px;background:#f8fbff;border:1px solid #e6eefc;padding:10px;border-radius:10px}
    input,select{padding:8px;border:1px solid #cad7ef;border-radius:8px}
    .actions{display:flex;gap:8px}
    .btn{display:inline-block;padding:6px 10px;border-radius:8px;border:1px solid #2B5EA4;background:#2B5EA4;color:#fff}
    .btn.subtle{background:#eef5ff;color:#1a3e72;border-color:#cad7ef}
    .muted{color:#6b7280;font-size:12px}
    .ok{color:#0a6d34}
    .ng{color:#a8322f}
  </style>
</head>
<body>
  <header>
    <div><h1>{{ heading }}</h1><div class="muted">{{ subtitle }}</div></div>
    <div style="margin-left:auto" class="muted">shop_id: <b>{{ shop_id }}</b></div>
  </header>
  <div class="tabs">
    <a class="tab" href="{{ url_for('admin_bp.products', shop_id=shop_id) }}">Products</a>
    <a class="tab" href="{{ url_for('admin_bp.promotions', shop_id=shop_id) }}">Promotions</a>
  </div>
  {% block content %}{% endblock %}
</body>
</html>
"""

def _utcnow() -> datetime:
  return datetime.now(dtz.utc)

def _products_ref(shop_id: str):
  db = get_db()
  return db.collection("shops").document(shop_id).collection("products")

def _promos_ref(shop_id: str):
  db = get_db()
  return db.collection("shops").document(shop_id).collection("promotions")

@admin_bp.route("/<shop_id>/products", methods=["GET","POST"])
def products(shop_id: str):
  db = get_db()
  ref = _products_ref(shop_id)
  if request.method == "POST":
    name = request.form.get("name","").strip()
    price = request.form.get("price","0").strip()
    stock = request.form.get("stock","0").strip()
    is_active = request.form.get("is_active","on") == "on"
    if not name:
      flash("Name is required","error")
    else:
      doc = ref.document()
      doc.set({
        "name": name,
        "price": float(price or 0),
        "stock": int(stock or 0),
        "is_active": is_active,
        "created_at": _utcnow(),
        "updated_at": _utcnow(),
      })
    return redirect(url_for("admin_bp.products", shop_id=shop_id))

  docs = list(ref.order_by("created_at", direction="DESCENDING").limit(100).stream())
  items = [dict(id=d.id, **d.to_dict()) for d in docs]
  content_tpl = """
  <div class="grid">
    <div>
      <h3>Add / Edit Product</h3>
      <form method="post">
        <label>Name <input name="name" required></label>
        <label>Price (THB) <input name="price" type="number" step="0.01" value="0"></label>
        <label>Stock <input name="stock" type="number" value="0"></label>
        <label><input name="is_active" type="checkbox" checked> Active</label>
        <div class="actions">
          <button class="btn" type="submit">Save</button>
          <a class="btn subtle" href="{{ url_for('admin_bp.products', shop_id=shop_id) }}">Reset</a>
        </div>
        <div class="muted">* กรอกชื่อ/ราคา/สต็อก แล้วกด Save</div>
      </form>
    </div>
    <div>
      <h3>Products List</h3>
      <table>
        <thead><tr><th>Name</th><th>Price</th><th>Stock</th><th>Status</th><th>Actions</th></tr></thead>
        <tbody>
          {% for p in items %}
          <tr>
            <td>{{ p.name }}</td>
            <td>{{ "%.2f"|format(p.price or 0) }}</td>
            <td>{{ p.stock or 0 }}</td>
            <td>{% if p.is_active %}<span class="ok">active</span>{% else %}<span class="ng">inactive</span>{% endif %}</td>
            <td class="actions">
              <form method="post" action="{{ url_for('admin_bp.product_toggle', shop_id=shop_id, product_id=p.id) }}">
                <button class="btn subtle" type="submit">{{ "Deactivate" if p.is_active else "Activate" }}</button>
              </form>
              <form method="post" action="{{ url_for('admin_bp.product_delete', shop_id=shop_id, product_id=p.id) }}">
                <button class="btn subtle" type="submit">Archive</button>
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
  return render_template_string(tpl, title="Products", heading="Admin · Products", subtitle="Manage products", shop_id=shop_id, items=items)

@admin_bp.route("/<shop_id>/products/<product_id>/toggle", methods=["POST"])
def product_toggle(shop_id: str, product_id: str):
  ref = _products_ref(shop_id).document(product_id)
  snap = ref.get()
  if snap.exists:
    cur = snap.to_dict().get("is_active", True)
    ref.set({"is_active": not cur, "updated_at": _utcnow()}, merge=True)
  return redirect(url_for("admin_bp.products", shop_id=shop_id))

@admin_bp.route("/<shop_id>/products/<product_id>/archive", methods=["POST"])
def product_delete(shop_id: str, product_id: str):
  ref = _products_ref(shop_id).document(product_id)
  ref.set({"archived": True, "is_active": False, "updated_at": _utcnow()}, merge=True)
  return redirect(url_for("admin_bp.products", shop_id=shop_id))

@admin_bp.route("/<shop_id>/promotions", methods=["GET","POST"])
def promotions(shop_id: str):
  ref = _promos_ref(shop_id)
  if request.method == "POST":
    name = request.form.get("name","").strip()
    status = request.form.get("status","draft").strip()
    start_date = request.form.get("start_date","").strip()
    end_date = request.form.get("end_date","").strip()
    if not name:
      pass
    else:
      doc = ref.document()
      doc.set({
        "name": name,
        "status": status,
        "start_date": start_date or None,
        "end_date": end_date or None,
        "created_at": _utcnow(),
        "updated_at": _utcnow(),
      })
    return redirect(url_for("admin_bp.promotions", shop_id=shop_id))

  docs = list(ref.order_by("created_at", direction="DESCENDING").limit(100).stream())
  items = [dict(id=d.id, **d.to_dict()) for d in docs]
  content_tpl = """
  <div class="grid">
    <div>
      <h3>Create Promotion</h3>
      <form method="post">
        <label>Name <input name="name" required></label>
        <label>Status
          <select name="status">
            <option value="draft">draft</option>
            <option value="active">active</option>
            <option value="ended">ended</option>
          </select>
        </label>
        <label>Start date <input name="start_date" type="date"></label>
        <label>End date <input name="end_date" type="date"></label>
        <div class="actions">
          <button class="btn" type="submit">Save</button>
          <a class="btn subtle" href="{{ url_for('admin_bp.promotions', shop_id=shop_id) }}">Reset</a>
        </div>
        <div class="muted">* เลือกช่วงวันที่ถ้ามี</div>
      </form>
    </div>
    <div>
      <h3>Promotions List</h3>
      <table>
        <thead><tr><th>Name</th><th>Status</th><th>Start</th><th>End</th><th>Actions</th></tr></thead>
        <tbody>
          {% for p in items %}
          <tr>
            <td>{{ p.name }}</td>
            <td>{{ p.status }}</td>
            <td>{{ p.start_date or "-" }}</td>
            <td>{{ p.end_date or "-" }}</td>
            <td class="actions">
              <form method="post" action="{{ url_for('admin_bp.promo_set_status', shop_id=shop_id, promo_id=p.id, status='active') }}"><button class="btn subtle" type="submit">Activate</button></form>
              <form method="post" action="{{ url_for('admin_bp.promo_set_status', shop_id=shop_id, promo_id=p.id, status='ended') }}"><button class="btn subtle" type="submit">End</button></form>
              <form method="post" action="{{ url_for('admin_bp.promo_archive', shop_id=shop_id, promo_id=p.id) }}"><button class="btn subtle" type="submit">Archive</button></form>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>
  """
  tpl = HTML_BASE.replace("{% block content %}{% endblock %}", content_tpl)
  return render_template_string(tpl, title="Promotions", heading="Admin · Promotions", subtitle="Manage promotions", shop_id=shop_id, items=items)

@admin_bp.route("/<shop_id>/promotions/<promo_id>/status/<status>", methods=["POST"])
def promo_set_status(shop_id: str, promo_id: str, status: str):
  status = status.lower().strip()
  if status not in ("draft","active","ended"):
    status = "draft"
  ref = _promos_ref(shop_id).document(promo_id)
  ref.set({"status": status, "updated_at": _utcnow()}, merge=True)
  return redirect(url_for("admin_bp.promotions", shop_id=shop_id))

@admin_bp.route("/<shop_id>/promotions/<promo_id>/archive", methods=["POST"])
def promo_archive(shop_id: str, promo_id: str):
  _promos_ref(shop_id).document(promo_id).set({"archived": True, "updated_at": _utcnow()}, merge=True)
  return redirect(url_for("admin_bp.promotions", shop_id=shop_id))