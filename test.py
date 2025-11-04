
import os
import io
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import base64
import random

# --- การจำลอง Dependencies เพื่อให้โค้ดรันได้ (Dummy Functions) ---
# ในโค้ดจริง ส่วนนี้จะเชื่อมต่อกับฐานข้อมูลของคุณ
class MockDB:
    def collection(self, name):
        return MockCollection()

class MockCollection:
    def document(self, name):
        return MockDocument()
    def stream(self):
        return [MockDocumentSnapshot(i) for i in range(random.randint(50, 150))]
    def where(self, *args, **kwargs):
        return self
    def limit(self, *args, **kwargs):
        return self

class MockDocument:
    def collection(self, name):
        return MockCollection()

class MockDocumentSnapshot:
    def __init__(self, i):
        self.id = f"user_{i}"
    def to_dict(self):
        ts = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30))
        return {
            "timestamp": ts,
            "direction": random.choice(["inbound", "outbound"]),
            "first_interaction_at": ts,
        }

def get_db():
    """Returns a mock Firestore client."""
    return MockDB()

def list_customers(shop_id: str, start: datetime, end: datetime, limit: int = 1000):
    """Returns a list of mock customers."""
    return [{"id": f"cust_{i}", "first_interaction_at": datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30))} for i in range(random.randint(80, 120))]

def list_messages(shop_id: str, customer_id: str, start: datetime, end: datetime, limit: int = 1000):
    """Returns a list of mock messages."""
    return [{"timestamp": datetime.now(timezone.utc) - timedelta(days=random.randint(0, 14)), "direction": random.choice(["inbound", "outbound"])} for i in range(random.randint(5, 50))]

def list_payments(shop_id: str, start: datetime, end: datetime, status: Optional[str] = None, limit: int = 2000):
    """Returns a list of mock payments."""
    statuses = ["confirmed", "succeeded", "paid", "completed", "failed"]
    return [{"status": random.choice(statuses), "amount": random.uniform(100.0, 5000.0)} for _ in range(random.randint(10, 50))]
# --- สิ้นสุดส่วนการจำลอง Dependencies ---


# HTML → PDF (marketing-grade)
try:
    from weasyprint import HTML
    _WEASYPRINT_AVAILABLE = True
except Exception:
    print("Warning: weasyprint is not installed. Full report will fall back to the mini version.")
    _WEASYPRINT_AVAILABLE = False

try:
    from jinja2 import Template
    _JINJA_AVAILABLE = True
except Exception:
    _JINJA_AVAILABLE = False

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.utils import ImageReader
import numpy as _np


# ---- Data aggregation helpers (approved schema) ----
def _daterange_days(start: datetime, end: datetime) -> List[str]:
    """Return list of YYYY-MM-DD for each day from start..end (inclusive) in Asia/Bangkok."""
    th_tz = timezone(timedelta(hours=7))
    days: List[str] = []
    d = datetime(start.year, start.month, start.day, tzinfo=start.tzinfo or timezone.utc)
    e = datetime(end.year, end.month, end.day, tzinfo=end.tzinfo or timezone.utc)
    while d <= e:
        days.append(d.astimezone(th_tz).strftime("%Y-%m-%d"))
        d = d + timedelta(days=1)
    return days

def _aggregate_period_metrics(shop_id: str, period_start: datetime, period_end: datetime) -> Dict[str, Any]:
    """
    Compute KPIs from Firestore using the schema discussed in the meeting.
    """
    db = get_db()
    logger.info(f"Aggregating metrics for shop_id={shop_id} from {period_start.isoformat()} to {period_end.isoformat()}")

    # 1) total customers
    cust_col = db.collection("shops").document(shop_id).collection("customers")
    total_customers = 0
    customers: List[str] = []
    for cdoc in cust_col.stream():
        total_customers += 1
        customers.append(cdoc.id)
    logger.info(f"Found {total_customers} total customers for shop_id={shop_id}")

    # 2) new customers
    th_start = period_start
    th_end = period_end
    new_customers = 0
    try:
        q_new = (
            cust_col.where("first_interaction_at", ">=", th_start)
                    .where("first_interaction_at", "<=", th_end)
                    .limit(5000)
        )
        for _ in q_new.stream():
            new_customers += 1
    except Exception as e:
        logger.warning(f"Could not query new customers for shop_id={shop_id}. Error: {e}")
        new_customers = 0

    # 3) messages + active users + per-day trend
    day_keys = _daterange_days(period_start, period_end)
    trend: Dict[str, Dict[str, int]] = {d: {"inbound": 0, "outbound": 0} for d in day_keys}
    active_users: set[str] = set()

    for uid in customers:
        msg_col = cust_col.document(uid).collection("messages")
        try:
            q_msgs = (
                msg_col.where("timestamp", ">=", th_start)
                       .where("timestamp", "<=", th_end)
                       .limit(20000)
            )
            for m in q_msgs.stream():
                mdata = m.to_dict() or {}
                ts = mdata.get("timestamp") or mdata.get("ts") or mdata.get("created_at")
                if ts is None: continue
                if not getattr(ts, "tzinfo", None):
                    ts = ts.replace(tzinfo=timezone.utc)
                dkey = ts.astimezone(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")
                direction = (mdata.get("direction") or "inbound").lower()
                if direction not in ("inbound", "outbound"):
                    direction = "inbound"
                trend.setdefault(dkey, {"inbound": 0, "outbound": 0})
                trend[dkey][direction] = trend[dkey].get(direction, 0) + 1
                active_users.add(uid)
        except Exception:
            continue
    active_chat_users = len(active_users)
    inbound_msgs = sum(trend[d]["inbound"] for d in trend)
    outbound_msgs = sum(trend[d]["outbound"] for d in trend)

    # 4) revenue
    revenue = 0.0
    payments_success = 0
    try:
        positive_statuses = ("confirmed", "succeeded", "paid", "completed")
        pays = list_payments(shop_id, start=period_start, end=period_end, status=None, limit=2000)
        for p in pays:
            status = (p.get("status") or "").lower()
            if status in positive_statuses:
                try:
                    revenue += float(p.get("amount") or 0.0)
                    payments_success += 1
                except (ValueError, TypeError):
                    continue
    except Exception as e_pay:
        logger.error(f"Failed to aggregate revenue for shop {shop_id}: {e_pay}")

    return {
        "total_customers": total_customers,
        "new_customers": new_customers,
        "active_chat_users": active_chat_users,
        "inbound_msgs": inbound_msgs,
        "outbound_msgs": outbound_msgs,
        "revenue": revenue,
        "payments_success": payments_success,
        "trend": trend,
    }


# --- Compose rule-based insights for PDF reports ---
def _compose_rule_based_insights(curr: dict, prev: Optional[dict], trend: Optional[Dict[str, Dict[str, int]]]) -> List[str]:
    """
    Create a few human-readable insights based on KPI deltas and simple heuristics.
    """
    insights: List[str] = []
    prev = prev or {}
    def _delta(a, b):
        try:
            a = float(a or 0); b = float(b or 0)
            if b == 0: return 0.0
            return (a - b) * 100.0 / b
        except Exception:
            return 0.0

    d_active = _delta(curr.get("active_chat_users"), prev.get("active_chat_users"))
    d_rev = _delta(curr.get("revenue"), prev.get("revenue"))

    if d_active > 10:
        insights.append(f"มีลูกค้ากลับมามีส่วนร่วมเพิ่มขึ้นถึง {abs(d_active):.0f}% สะท้อนว่าแคมเปญล่าสุดได้ผลดี")
    elif d_active < -10:
        insights.append(f"การมีส่วนร่วมของลูกค้าลดลง {abs(d_active):.0f}% ควรพิจารณาส่ง Broadcast หรือ Quick Reply เพื่อกระตุ้นการสนทนา")

    if d_rev > 10:
        insights.append(f"รายได้เติบโตขึ้น {abs(d_rev):.0f}% เป็นสัญญาณที่ดีจากการใช้จ่ายของลูกค้า")
    elif d_rev < -10:
        insights.append(f"รายได้ลดลง {abs(d_rev):.0f}% อาจต้องพิจารณาจัดโปรโมชันเพื่อกระตุ้นยอดขายระยะสั้น")

    try:
        if trend:
            top_day = max(trend.items(), key=lambda kv: kv[1].get("inbound", 0))[0]
            top_val = trend[top_day].get("inbound", 0)
            if top_val > (sum(t.get("inbound", 0) for t in trend.values()) / len(trend) * 1.5):
                insights.append(f"วันที่ {top_day} มีลูกค้าทักเข้ามามากเป็นพิเศษ ({top_val} ข้อความ) ควรตรวจสอบเพิ่มเติมว่าเกิดจากสาเหตุใด")
    except Exception:
        pass

    if not insights:
        insights.append("ภาพรวมทราฟฟิกและการมีส่วนร่วมของลูกค้าอยู่ในระดับคงที่เมื่อเทียบกับช่วงก่อนหน้า")
    return insights


# --- Report constants ---
REPORT_LOGO_PATH = os.environ.get("REPORT_LOGO_PATH", "").strip()
BRAND_PRIMARY_HEX = os.environ.get("BRAND_PRIMARY_HEX", "#008080").strip()  # Expert Teal
BRAND_ACCENT_HEX = os.environ.get("BRAND_ACCENT_HEX", "#F97316").strip()    # Action Orange
REPORT_TITLE_TH = os.environ.get("REPORT_TITLE_TH", "รายงานสรุปข้อมูลลูกค้า").strip()
REPORT_TITLE_EN = os.environ.get("REPORT_TITLE_EN", "Customer Insight Report").strip()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ---------- Formatting helpers ----------
def _fmt_int(v: Any) -> str:
    try:
        return f"{int(float(v)):,}"
    except Exception:
        return str(v) if v is not None else "-"

def _fmt_money(v: Any) -> str:
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "0.00"

def _period_text_th(start: datetime, end: datetime) -> str:
    th_tz = timezone(timedelta(hours=7))
    s = start.astimezone(th_tz).strftime("%d %b %Y")
    e = end.astimezone(th_tz).strftime("%d %b %Y")
    return f"{s} – {e}"

def _compute_prev_window(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    length = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=length-1)
    return (prev_start, prev_end)

def _hex_to_rgb(hex_color: str):
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i+2], 16) / 255.0 for i in (0, 2, 4))

def _register_thai_font_reportlab() -> Optional[str]:
    # Dummy function for font registration
    return "Helvetica"

def _chart_to_base64(fig) -> str:
    """Converts a matplotlib figure to a base64 encoded PNG."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def _generate_traffic_chart_b64(trend: Dict[str, Dict[str, int]]) -> str:
    """Generates the 'Daily Visits' chart and returns it as a base64 string."""
    try:
        if not trend: return ""
        days = sorted(trend.keys())
        visits = [trend[d].get("inbound", 0) for d in days]

        fig, ax = plt.subplots(figsize=(6.0, 2.8))
        x = _np.arange(len(days))

        # Spline or moving average for smooth curve
        try:
            from scipy.interpolate import make_interp_spline
            xs = _np.linspace(x.min(), x.max(), 200) if len(x) >= 3 else x
            ys_visits = make_interp_spline(x, _np.array(visits), k=2)(xs) if len(x) >=3 else _np.array(visits)
        except ImportError:
            # Fallback if scipy is not installed
            kernel = _np.ones(3)/3.0
            ys_visits = _np.convolve(_np.array(visits), kernel, mode="same")
            ys_visits[0] = visits[0]; ys_visits[-1] = visits[-1]
            xs = x

        ax.plot(xs, ys_visits, linewidth=2.5, color=BRAND_PRIMARY_HEX)
        ax.fill_between(xs, ys_visits, color=BRAND_PRIMARY_HEX, alpha=0.1)

        ax.set_title("Daily Messages (Inbound)", fontsize=10, pad=10)
        ax.grid(axis='y', linestyle='--', alpha=0.6)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.tick_params(axis='x', rotation=30, labelsize=8)
        ax.tick_params(axis='y', labelsize=8)
        
        # Set ticks to match data points
        ax.set_xticks(x)
        ax.set_xticklabels([d.split('-')[-1] for d in days]) # Show only day
        
        return _chart_to_base64(fig)
    except Exception as e:
        logger.error(f"Failed to generate traffic chart: {e}")
        plt.close("all")
        return ""

def _generate_conversion_chart_b64(trend: Dict[str, Dict[str, int]]) -> str:
    """Generates the 'Conversion' (Outbound messages) chart and returns it as a base64 string."""
    try:
        if not trend: return ""
        days = sorted(trend.keys())
        conversions = [trend[d].get("outbound", 0) for d in days]

        fig, ax = plt.subplots(figsize=(6.0, 2.8))
        x = _np.arange(len(days))

        try:
            from scipy.interpolate import make_interp_spline
            xs = _np.linspace(x.min(), x.max(), 200) if len(x) >= 3 else x
            ys_conv = make_interp_spline(x, _np.array(conversions), k=2)(xs) if len(x) >= 3 else _np.array(conversions)
        except ImportError:
            kernel = _np.ones(3)/3.0
            ys_conv = _np.convolve(_np.array(conversions), kernel, mode="same")
            ys_conv[0] = conversions[0]; ys_conv[-1] = conversions[-1]
            xs = x

        ax.plot(xs, ys_conv, linewidth=2.5, color=BRAND_ACCENT_HEX)
        ax.fill_between(xs, ys_conv, color=BRAND_ACCENT_HEX, alpha=0.1)

        ax.set_title("Daily Proactive Messages (Outbound)", fontsize=10, pad=10)
        ax.grid(axis='y', linestyle='--', alpha=0.6)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.tick_params(axis='x', rotation=30, labelsize=8)
        ax.tick_params(axis='y', labelsize=8)

        ax.set_xticks(x)
        ax.set_xticklabels([d.split('-')[-1] for d in days])
        
        return _chart_to_base64(fig)
    except Exception as e:
        logger.error(f"Failed to generate conversion chart: {e}")
        plt.close("all")
        return ""

def build_mini_report_pdf(shop_id: str, start_dt: datetime, end_dt: datetime) -> bytes:
    """
    Mini report 1 หน้า: ใช้ตัวคำนวณเดียวกัน แล้วเรนเดอร์แบบ ReportLab (เสถียรและเบา)
    """
    summary = _aggregate_period_metrics(shop_id, start_dt, end_dt)
    prev_start, prev_end = _compute_prev_window(start_dt, end_dt)
    prev_summary = _aggregate_period_metrics(shop_id, prev_start, prev_end)
    trend = summary.get("trend") or {}
    insights = _compose_rule_based_insights(summary, prev_summary, trend)

    return _build_report_pdf(
        shop_id=shop_id,
        period_start=start_dt,
        period_end=end_dt,
        summary=summary,
        insights=insights,
        trend=trend,
    )

def build_report_pdf_v3(shop_id: str, start_dt: datetime, end_dt: datetime) -> bytes:
    """
    Full report สไตล์ marketing (HTML/CSS)
    """
    if not _WEASYPRINT_AVAILABLE:
        logger.warning("WeasyPrint not found. Falling back to mini report.")
        return build_mini_report_pdf(shop_id, start_dt, end_dt)

    # 1) Aggregate data
    summary = _aggregate_period_metrics(shop_id, start_dt, end_dt)
    prev_start, prev_end = _compute_prev_window(start_dt, end_dt)
    prev_summary = _aggregate_period_metrics(shop_id, prev_start, prev_end)
    trend = summary.get("trend") or {}
    insights = _compose_rule_based_insights(summary, prev_summary, trend)

    # 2) Generate charts as base64
    chart_visits_b64 = _generate_traffic_chart_b64(trend)
    chart_conversion_b64 = _generate_conversion_chart_b64(trend)

    # 3) Helper for %Δ
    def _pct(curr, prev):
        try:
            prev = float(prev or 0); curr = float(curr or 0)
            if prev == 0: return "• 0%"
            p = (curr - prev) * 100.0 / prev
            return ("↑" if p > 0 else ("↓" if p < 0 else "•")) + f" {abs(p):.0f}%"
        except Exception:
            return "• 0%"
    
    p = prev_summary or {}
    
    # 4) Prepare data for template
    period_txt = _period_text_th(start_dt, end_dt)
    report_date = datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y")
    
    kpis = {
        "inbound_msgs": summary.get("inbound_msgs", 0),
        "inbound_delta": _pct(summary.get("inbound_msgs"), p.get("inbound_msgs")),
        "active_users": summary.get("active_chat_users", 0),
        "active_delta": _pct(summary.get("active_chat_users"), p.get("active_chat_users")),
        "new_cust": summary.get("new_customers", 0),
        "new_delta": _pct(summary.get("new_customers"), p.get("new_customers")),
        "revenue": summary.get("revenue", 0.0),
        "revenue_delta": _pct(summary.get("revenue"), p.get("revenue")),
    }
    
    # 5) Render HTML template
    html = f"""
<!doctype html>
<html lang="th">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Customer Engagement Report</title>
<style>
  :root {{
    --teal: {BRAND_PRIMARY_HEX};
    --orange: {BRAND_ACCENT_HEX};
    --offwhite: #F8F9FA;
    --text: #111827;
  }}
  \* {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 24px; background: var(--offwhite); color: var(--text);
    font-family: 'Helvetica', 'Arial', sans-serif; /* Fallback font */
  }}
  .container {{ max-width: 1040px; margin: 0 auto; }}
  .title {{ background: linear-gradient(135deg, var(--teal), #006a6a); color: white; padding: 28px 32px; border-radius: 16px; }}
  h1 {{ margin: 0; font-size: 28px; letter-spacing: .3px; }}
  .grid {{ display: grid; gap: 16px; }}
  /* Changed to 2x2 grid for KPI cards */
  .cards {{ grid-template-columns: repeat(2, 1fr); margin-top: 16px; }}
  .card {{
    padding: 18px; border-radius: 14px; background: white; box-shadow: 0 2px 8px rgba(0,0,0,.06);
    display: flex; flex-direction: column; gap: 8px; min-height: 110px;
  }}
  .card h3 {{ margin: 0; font-weight: 600; font-size: 14px; color: #374151; }}
  .card .value {{ font-size: 28px; font-weight: 700; }}
  .card--1 {{ background: #E6FFFB; }}
  .card--2 {{ background: #FFF7ED; }}
  .card--3 {{ background: #EEF2FF; }}
  .card--4 {{ background: #FEF2F2; }}
  .section {{ background: white; padding: 18px; border-radius: 14px; box-shadow: 0 2px 8px rgba(0,0,0,.06); }}
  .two-cols {{ grid-template-columns: 1fr 1fr; }}
  .section h2 {{ margin: 0 0 8px 0; font-size: 16px; }}
  .insights {{ display:grid; gap:16px; margin-top:16px; }}
  .insight-card {{
    background: linear-gradient(180deg, #ffffff, #fafafa);
    border: 1px solid #E5E7EB; border-radius: 14px; padding: 16px 18px;
    box-shadow: 0 4px 14px rgba(249,115,22,.15);
  }}
  .insight-title {{ font-weight:700; font-size:16px; margin:0 0 6px 0; }}
  .insight-body {{ margin:0; color:#374151; line-height:1.6; }}
  .badge {{ display:inline-block; background: var(--orange); color:white; padding:4px 8px; border-radius: 999px; font-size: 12px; margin-left:8px; }}
  .footer {{ text-align:right; color:#6B7280; font-size:12px; margin-top:12px; }}
  .title .footer {{ color: white; opacity: .8; text-align: left; }}
  .chart-container img {{ max-width: 100%; height: auto; }}

  @media (max-width: 900px) {{
    .cards, .two-cols {{ grid-template-columns: 1fr; }}
  }}
  @media print {{
    body {{ background: white; padding: 0; }}
    .title {{ border-radius: 0; }}
    .card, .section, .insight-card {{ box-shadow: none; }}
    .container {{ max-width: unset; padding: 16mm; }}
  }}
</style>
</head>
<body>
<div class="container">
  <div class="title">
    <h1>{REPORT_TITLE_TH}</h1>
    <div class="footer">ออกรายงานวันที่ {report_date} • ช่วงข้อมูล: {period_txt}</div>
  </div>

  <div class="grid cards">
    <div class="card card--1">
      <h3>Inbound Messages</h3>
      <div class="value">{_fmt_int(kpis['inbound_msgs'])}</div>
      <small>{kpis['inbound_delta']} vs prev. period</small>
    </div>
    <div class="card card--2">
      <h3>Active Chat Users</h3>
      <div class="value">{_fmt_int(kpis['active_users'])}</div>
      <small>{kpis['active_delta']} vs prev. period</small>
    </div>
    <div class="card card--3">
      <h3>New Customers</h3>
      <div class="value">{_fmt_int(kpis['new_cust'])}</div>
      <small>{kpis['new_delta']} vs prev. period</small>
    </div>
    <div class="card card--4">
      <h3>Revenue (THB)</h3>
      <div class="value">{_fmt_money(kpis['revenue'])}</div>
      <small>{kpis['revenue_delta']} vs prev. period</small>
    </div>
  </div>

  <div class="grid two-cols" style="margin-top:16px;">
    <div class="section chart-container">
      <img src="data:image/png;base64,{chart_visits_b64}" alt="Daily Visits Chart"/>
    </div>
    <div class="section chart-container">
      <img src="data:image/png;base64,{chart_conversion_b64}" alt="Conversion Rate Chart"/>
    </div>
  </div>

  <div class="insights">
    {''.join(f'<div class="insight-card"><div class="insight-title">🔎 Key Insight</div><p class="insight-body">{i}</p></div>' for i in insights)}
  </div>

  <div class="footer">Prepared by MANEE AND SON for shop {shop_id}</div>
</div>
</body>
</html>
"""
    return HTML(string=html).write_pdf()


def _build_report_pdf(shop_id: str, period_start: datetime, period_end: datetime, summary: dict, insights: list[str], trend: Optional[Dict[str, Dict[str, int]]]) -> bytes:
    '''
    Renders a 1-page MINI PDF using ReportLab.
    This version is simplified to only show insights and a trend chart.
    '''
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    _fontname = _register_thai_font_reportlab()
    
    styles["Normal"].fontName = _fontname or 'Helvetica'
    styles["Heading1"].fontName = _fontname or 'Helvetica-Bold'
    styles["Heading2"].fontName = _fontname or 'Helvetica-Bold'

    story = []

    # Title
    title_th = Paragraph(f"<b>{REPORT_TITLE_TH} (Mini)</b>", styles["Heading1"])
    period_txt = _period_text_th(period_start, period_end)
    story.extend([title_th, Paragraph(f"Period: {period_txt}", styles["Normal"]), Spacer(1, 0.5*cm)])
    
    # Insights bullets
    if insights:
        story.append(Paragraph("<b>Key Insights</b>", styles["Heading2"]))
        for t in insights:
            story.append(Paragraph(f"• {t}", styles["Normal"]))
        story.append(Spacer(1, 0.5*cm))

    # Trend chart
    if trend:
        # Generate chart using matplotlib
        fig, ax = plt.subplots(figsize=(8, 3.5))
        days = sorted(trend.keys())
        inbound = [trend[d].get("inbound", 0) for d in days]
        outbound = [trend[d].get("outbound", 0) for d in days]
        ax.plot(days, inbound, marker='o', linestyle='-', label='Inbound')
        ax.plot(days, outbound, marker='.', linestyle='--', label='Outbound')
        ax.set_title("Daily Message Trend")
        ax.set_ylabel("Message Count")
        ax.tick_params(axis='x', rotation=45)
        ax.legend()
        plt.tight_layout()
        
        img_buf = io.BytesIO()
        fig.savefig(img_buf, format='png', dpi=150)
        plt.close(fig)
        img_buf.seek(0)
        
        story.append(RLImage(img_buf, width=16*cm, height=7.8*cm))

    doc.build(story)
    return buf.getvalue()


# --- Main execution block for testing ---
if __name__ == '__main__':
    print("Starting report generation test...")
    shop_id_test = "shop_12345"
    end_date_test = datetime.now(timezone.utc)
    start_date_test = end_date_test - timedelta(days=13)

    # 1. Generate the full V3 report
    try:
        print(f"Generating full report (v3) for shop {shop_id_test}...")
        pdf_bytes_v3 = build_report_pdf_v3(shop_id_test, start_date_test, end_date_test)
        with open("full_report.pdf", "wb") as f:
            f.write(pdf_bytes_v3)
        print(" -> Successfully created 'full_report.pdf'")
    except Exception as e:
        print(f" [ERROR] Failed to generate full report: {e}")
        if not _WEASYPRINT_AVAILABLE:
            print("   Please install weasyprint: pip install weasyprint")

    # 2. Generate the mini report
    try:
        print(f"\nGenerating mini report for shop {shop_id_test}...")
        pdf_bytes_mini = build_mini_report_pdf(shop_id_test, start_date_test, end_date_test)
        with open("mini_report.pdf", "wb") as f:
            f.write(pdf_bytes_mini)
        print(" -> Successfully created 'mini_report.pdf'")
    except Exception as e:
        print(f" [ERROR] Failed to generate mini report: {e}")

    print("\nReport generation test finished.")

        <div class="insight-card">
          <div class="insight-title">💡 Recommendation</div>
          <p class="insight-body">{recommendation}</p>
        </div>