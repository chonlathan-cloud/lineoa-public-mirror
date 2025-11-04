import os
import io
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
# HTML → PDF (marketing-grade)
try:
    from weasyprint import HTML
    _WEASYPRINT_AVAILABLE = True
except Exception:
    _WEASYPRINT_AVAILABLE = False
try:
    from jinja2 import Template
    _JINJA_AVAILABLE = True
except Exception:
    _JINJA_AVAILABLE = False
import base64
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

# --- Data access ---
from firestore_client import get_db
from dao import list_customers, list_messages, list_payments
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
    Compute KPIs from Firestore using the schema discussed in the meeting:
      - total_customers: count of docs in shops/{shop}/customers
      - new_customers: customers whose first_interaction_at ∈ [start,end]
      - active_chat_users: distinct customers who have any message in [start,end]
      - inbound/outbound counts and a per-day trend across the period
      - revenue: sum of payments with positive statuses and paid_at ∈ [start,end]
    NOTE: messages live under customers/*/messages. We therefore iterate customers
          in this shop and query their subcollection bounded by the period.
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

    # 2) new customers strictly by first_interaction_at in range
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
        logger.warning(f"Could not query new customers for shop_id={shop_id}. This might be due to a missing index. Error: {e}")
        # ถ้า field ไม่มีเลย ให้ถือเป็น 0 ตามสเปคล่าสุด (ไม่ fallback created_at)
        new_customers = 0

    # 3) messages + active users + per-day trend
    day_keys = _daterange_days(period_start, period_end)
    trend: Dict[str, Dict[str, int]] = {d: {"inbound": 0, "outbound": 0} for d in day_keys}
    active_users: set[str] = set()

    # Iterate each customer to scope correctly to this shop
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
                # normalize ts to timezone-aware datetime (UTC)
                if hasattr(ts, "to_datetime"):
                    ts = ts.to_datetime()
                elif isinstance(ts, str):
                    from dateutil import parser as _p
                    try:
                        ts = _p.isoparse(ts)
                    except Exception:
                        try:
                            ts = _p.parse(ts)
                        except Exception:
                            ts = None
                elif isinstance(ts, (int, float)):
                    try:
                        ts = datetime.fromtimestamp(float(ts), tz=timezone.utc)
                    except Exception:
                        ts = None
                if ts is None:
                    continue
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

    # 4) revenue + payments count via DAO — count only positive statuses
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
    Returns a list of short Thai sentences (safe for PDF).
    """
    insights: List[str] = []
    prev = prev or {}
    def _delta(a, b):
        try:
            a = float(a or 0); b = float(b or 0)
            if b == 0:
                return 0.0
            return (a - b) * 100.0 / b
        except Exception:
            return 0.0

    d_active = _delta(curr.get("active_chat_users"), prev.get("active_chat_users"))
    d_new    = _delta(curr.get("new_customers"), prev.get("new_customers"))
    d_rev    = _delta(curr.get("revenue"), prev.get("revenue"))

    # Active chats insight
    if d_active > 0:
        insights.append(f"ผู้ใช้สนทนา (Active chat) เพิ่มขึ้นประมาณ {abs(d_active):.0f}% เมื่อเทียบช่วงก่อนหน้า")
    elif d_active < 0:
        insights.append(f"ผู้ใช้สนทนา (Active chat) ลดลงประมาณ {abs(d_active):.0f}% ควรกระตุ้นด้วย Quick Reply/Broadcast")

    # New customers insight
    if d_new > 0:
        insights.append(f"ลูกค้าใหม่เพิ่มขึ้น {abs(d_new):.0f}% — ควรต่อยอดด้วยคูปองต้อนรับ")
    elif d_new < 0:
        insights.append(f"ลูกค้าใหม่ลดลง {abs(d_new):.0f}% — ลองรีมาร์เก็ตติ้งจากฐานลูกค้าเก่า")

    # Revenue insight
    if d_rev > 0:
        insights.append(f"รายได้เติบโต {abs(d_rev):.0f}% ในช่วงที่วิเคราะห์")
    elif d_rev < 0:
        insights.append(f"รายได้ลดลง {abs(d_rev):.0f}% — พิจารณาโปรโมชันระยะสั้นเพื่อกระตุ้นยอด")

    # Trend spike insight (optional)
    try:
        if trend:
            # หา Top-1 วันที่ยอด inbound สูงสุด
            top_day = max(trend.items(), key=lambda kv: kv[1].get("inbound", 0))[0]
            top_val = trend[top_day].get("inbound", 0)
            if top_val > 0:
                insights.append(f"มีจุดพีกการสนทนาวันที่ {top_day} (Inbound {top_val} ข้อความ)")
    except Exception:
        pass

    if not insights:
        insights.append("ทราฟฟิกและการสนทนาอยู่ในระดับคงที่เมื่อเทียบช่วงก่อนหน้า")
    return insights




# --- Report constants ---
REPORT_LOGO_PATH = os.environ.get("REPORT_LOGO_PATH", "/Users/chonlathansongsri/Documents/company/line OA/data/Logo.png").strip()
BRAND_PRIMARY_HEX = os.environ.get("BRAND_PRIMARY_HEX", "#008080").strip()  # Expert Teal
BRAND_ACCENT_HEX = os.environ.get("BRAND_ACCENT_HEX", "#F97316").strip()    # Action Orange
REPORT_TITLE_TH = os.environ.get("REPORT_TITLE_TH", "รายงานสรุปข้อมูลลูกค้า").strip()
REPORT_TITLE_EN = os.environ.get("REPORT_TITLE_EN", "Customer Insight Report").strip()

logger = logging.getLogger(__name__)

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
    """Return previous window with same length as [start,end]."""
    length = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=length-1)
    return (prev_start, prev_end)


def _hex_to_rgb(hex_color: str):
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i+2], 16) / 255.0 for i in (0, 2, 4))
def _detect_thai_font_css() -> str:
    candidates = [
        os.path.expanduser("~/Library/Fonts/NotoSansThai-Regular.ttf"),
        os.path.expanduser("~/Library/Fonts/NotoSansThai-Bold.ttf"),
        "/Library/Fonts/NotoSansThai-Regular.ttf",
        "/Library/Fonts/NotoSansThai-Bold.ttf",
        os.path.expanduser("~/Library/Fonts/Prompt-Regular.ttf"),
        os.path.expanduser("~/Library/Fonts/Prompt-Bold.ttf"),
        "/Library/Fonts/Prompt-Regular.ttf",
        "/Library/Fonts/Prompt-Bold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansThai-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansThai-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ]
    reg, bold = None, None
    for p in candidates:
        if os.path.isfile(p):
            n = os.path.basename(p).lower()
            if "bold" in n and bold is None:
                bold = p
            if "regular" in n and reg is None:
                reg = p
    if not reg and not bold:
        return ""
    css = []
    if reg:
        css.append(f"@font-face{{font-family:'ThaiBrand';src:url('file://{reg}') format('truetype');font-weight:400;}}")
    if bold:
        css.append(f"@font-face{{font-family:'ThaiBrand';src:url('file://{bold}') format('truetype');font-weight:700;}}")
    return "\n".join(css)

def _register_thai_font_reportlab() -> Optional[str]:
    """
    Register a Thai-capable font family for ReportLab and return the regular font name.
    Uses the same search list as _detect_thai_font_css(). Falls back gracefully.
    """
    try:
        candidates = [
            os.path.expanduser("~/Library/Fonts/NotoSansThai-Regular.ttf"),
            "/Library/Fonts/NotoSansThai-Regular.ttf",
            os.path.expanduser("~/Library/Fonts/Prompt-Regular.ttf"),
            "/Library/Fonts/Prompt-Regular.ttf",
        ]
        bold_candidates = [
            os.path.expanduser("~/Library/Fonts/NotoSansThai-Bold.ttf"),
            "/Library/Fonts/NotoSansThai-Bold.ttf",
            os.path.expanduser("~/Library/Fonts/Prompt-Bold.ttf"),
            "/Library/Fonts/Prompt-Bold.ttf",
        ]
        reg = next((p for p in candidates if os.path.isfile(p)), None)
        bold = next((p for p in bold_candidates if os.path.isfile(p)), None)
        if not reg:
            return None
        try:
            pdfmetrics.getFont("ThaiBrand")
        except Exception:
            pdfmetrics.registerFont(TTFont("ThaiBrand", reg))
            if bold:
                try:
                    pdfmetrics.registerFont(TTFont("ThaiBrand-Bold", bold))
                except Exception:
                    pass
        return "ThaiBrand"
    except Exception:
        return None

def _chart_messages_trend_image(trend: Dict[str, Dict[str, int]]) -> Optional[io.BytesIO]:
    """Build a simple line chart image (PNG) for inbound/outbound per day and return a BytesIO buffer."""
    try:
        if not trend:
            return None
        days = sorted(trend.keys())
        inbound = [trend[d].get("inbound", 0) for d in days]
        outbound = [trend[d].get("outbound", 0) for d in days]

        fig, ax = plt.subplots()
        ax.plot(days, inbound, marker="o", label="Inbound")
        ax.plot(days, outbound, marker="o", label="Outbound")
        ax.set_title("Daily Messages (14 days)")
        ax.set_xlabel("Date")
        ax.set_ylabel("Count")
        ax.legend()
        fig.autofmt_xdate(rotation=45)

        buf = io.BytesIO()
        plt.tight_layout()
        fig.savefig(buf, format="png", dpi=150)
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception:
        try:
            plt.close("all")
        except Exception:
            pass
        return None

def build_mini_report_pdf(shop_id: str, start_dt: datetime, end_dt: datetime) -> bytes:
    """
    Mini report 1 หน้า: ใช้ตัวคำนวณเดียวกัน แล้วเรนเดอร์แบบ ReportLab (เสถียรและเบา)
    """
    # 1) Summary ของช่วงที่ขอ
    summary = _aggregate_period_metrics(shop_id, start_dt, end_dt)

    # 2) Summary ของช่วงก่อนหน้า (ความยาวเท่ากัน) เพื่อคำนวณ %Δ
    prev_start, prev_end = _compute_prev_window(start_dt, end_dt)
    prev_summary = _aggregate_period_metrics(shop_id, prev_start, prev_end)

    # 3) Insights และ trend
    trend = summary.get("trend") or {}
    insights = _compose_rule_based_insights(summary, prev_summary, trend)

    # 4) Render PDF (สไตล์ minimalist เดิม)
    return _build_report_pdf(
        shop_id=shop_id,
        period_start=start_dt,
        period_end=end_dt,
        summary=summary,
        insights=insights,
        prev_summary=prev_summary,
        trend=trend,
    )


def build_report_pdf_v3(shop_id: str, start_dt: datetime, end_dt: datetime) -> bytes:
    """
    Full report สไตล์ marketing (header ไล่เฉด, การ์ดโค้งมน, bilingual) — ใช้ WeasyPrint (HTML/CSS)
    ถ้า WeasyPrint ไม่พร้อม ให้ fallback ไปใช้ _build_report_pdf แบบเดิมโดยอัตโนมัติ
    """
    # 1) รวมข้อมูล
    summary = _aggregate_period_metrics(shop_id, start_dt, end_dt)
    prev_start, prev_end = _compute_prev_window(start_dt, end_dt)
    prev_summary = _aggregate_period_metrics(shop_id, prev_start, prev_end)
    trend = summary.get("trend") or {}
    insights = _compose_rule_based_insights(summary, prev_summary, trend)

    # 2) helper สำหรับ %Δ
    def _pct(curr, prev):
        try:
            prev = float(prev); curr = float(curr)
            if prev == 0:
                return "0%"
            p = (curr - prev) * 100.0 / prev
            return ("↑" if p > 0 else ("↓" if p < 0 else "•")) + f" {abs(p):.0f}%"
        except Exception:
            return "0%"

    p = prev_summary or {}
    k_total = summary.get("total_customers", 0)
    k_new   = summary.get("new_customers", 0)
    k_act   = summary.get("active_chat_users", 0)
    k_rev   = summary.get("revenue", 0.0)

    # 3) ฟอนต์ไทยสำหรับ HTML/CSS
    font_css = _detect_thai_font_css()
    period_txt = _period_text_th(start_dt, end_dt)

    # 4) HTML สไตล์เดียวกับตัวอย่างของผู้ใช้ (ปรับให้เป็นรายงานลูกค้า)
    html = f"""<!doctype html>
<html lang="th">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{REPORT_TITLE_EN}</title>
<style>
  {font_css}
  :root {{
    --teal: {BRAND_PRIMARY_HEX};
    --orange: {BRAND_ACCENT_HEX};
    --offwhite: #F8F9FA;
    --lightgray: #F5F5F4;
    --text: #111827;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 24px; background: var(--offwhite); color: var(--text);
    font-family: 'ThaiBrand', ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial;
  }}
  .container {{ max-width: 1040px; margin: 0 auto; }}
  .title {{ background: linear-gradient(135deg, var(--teal), #006a6a); color: white; padding: 28px 32px; border-radius: 16px; }}
  h1 {{ margin: 0; font-size: 24px; letter-spacing: .3px; }}
  .subtitle {{ opacity:.9; margin-top:6px; font-size:13px; }}
  .grid {{ display: grid; gap: 16px; }}
  .cards {{ grid-template-columns: repeat(4, 1fr); margin-top: 16px; }}
  .card {{
    padding: 18px; border-radius: 14px; background: white; box-shadow: 0 2px 8px rgba(0,0,0,.06);
    display: flex; flex-direction: column; gap: 8px; min-height: 110px;
  }}
  .card h3 {{ margin: 0; font-weight: 600; font-size: 14px; color: #374151; }}
  .card .value {{ font-size: 28px; font-weight: 700; }}
  .card--1 {{ background: #E6FFFB; }}
  .card--2 {{ background: #FFF7ED; }}
  .card--3 {{ background: #EEF2FF; }}
  .card--4 {{ background: #ECFDF5; }}
  .section {{ background: white; padding: 18px; border-radius: 14px; box-shadow: 0 2px 8px rgba(0,0,0,.06); }}
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
  @media (max-width: 900px) {{ .cards {{ grid-template-columns: 1fr 1fr; }} }}
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
      <div class="subtitle">{REPORT_TITLE_EN} • Period: {period_txt}</div>
    </div>

    <!-- Top 4 colored cards -->
    <div class="grid cards">
      <div class="card card--1">
        <h3>Total Customers</h3>
        <div class="value">{_fmt_int(k_total)}</div>
        <small>{_pct(k_total, p.get('total_customers'))}</small>
      </div>
      <div class="card card--2">
        <h3>New Customers</h3>
        <div class="value">{_fmt_int(k_new)}</div>
        <small>{_pct(k_new, p.get('new_customers'))}</small>
      </div>
      <div class="card card--3">
        <h3>Active Chat Users</h3>
        <div class="value">{_fmt_int(k_act)}</div>
        <small>{_pct(k_act, p.get('active_chat_users'))}</small>
      </div>
      <div class="card card--4">
        <h3>Revenue (THB)</h3>
        <div class="value">{_fmt_money(k_rev)}</div>
        <small>{_pct(k_rev, p.get('revenue'))}</small>
      </div>
    </div>

    <!-- Insights -->
    <div class="insights">
      {"".join(f'<div class="insight-card"><div class="insight-title">🔎 Insight</div><p class="insight-body">{i}</p></div>' for i in insights)}
    </div>

    <div class="footer">Prepared by AI for Org • Shop: {shop_id}</div>
  </div>
</body>
</html>"""

    # 5) เรนเดอร์ PDF
    try:
        if _WEASYPRINT_AVAILABLE:
            return HTML(string=html).write_pdf()
    except Exception as e:
        logging.getLogger(__name__).error("WeasyPrint failed, fallback to ReportLab: %s", e)

    # Fallback (ใช้หน้าตา minimalist แต่ข้อมูลถูกต้อง)
    return _build_report_pdf(
        shop_id=shop_id,
        period_start=start_dt,
        period_end=end_dt,
        summary=summary,
        insights=insights,
        prev_summary=prev_summary,
        trend=trend,
    )

def _chart_messages_trend_image_v3(trend: Dict[str, Dict[str, int]]) -> Optional[io.BytesIO]:
    '''Styled chart: curved lines for inbound/outbound + dashed avg (brand colors).'''
    try:
        if not trend:
            return None
        days = sorted(trend.keys())
        inbound  = [trend[d].get("inbound", 0) for d in days]
        outbound = [trend[d].get("outbound", 0) for d in days]
        avg_in = (sum(inbound) / float(len(inbound))) if len(inbound) else 0.0

        import numpy as _np
        x = _np.arange(len(days), dtype=float)

        # Smooth curves if scipy is available; otherwise do a light moving-average to look rounded.
        y_in, y_out = _np.array(inbound, dtype=float), _np.array(outbound, dtype=float)
        xs = x
        ys_in = y_in
        ys_out = y_out
        try:
            from scipy.interpolate import make_interp_spline  # type: ignore
            # create 200 sample points for smooth curves
            xs = _np.linspace(x.min(), x.max(), 200) if len(x) >= 3 else x
            if len(x) >= 3:
                ys_in = make_interp_spline(x, y_in, k=3)(xs)
                ys_out = make_interp_spline(x, y_out, k=3)(xs)
        except Exception:
            # 3-point moving average smoothing fallback
            def _ma(a):
                if a.size <= 2:
                    return a
                kernel = _np.ones(3)/3.0
                b = _np.convolve(a, kernel, mode="same")
                b[0] = a[0]; b[-1] = a[-1]
                return b
            ys_in = _ma(y_in)
            ys_out = _ma(y_out)

        fig, ax = plt.subplots(figsize=(6.0, 3.2))
        ax.plot(xs, ys_in, linewidth=2.2, label="Inbound", color=BRAND_PRIMARY_HEX)
        ax.plot(xs, ys_out, linewidth=2.2, label="Outbound", color=BRAND_ACCENT_HEX)
        if len(x) >= 2:
            ax.hlines(avg_in, xmin=x.min(), xmax=x.max(), linestyles="dashed", label="Inbound avg", color="#94a3b8")

        ax.set_title("Messages Overview (14 days)", fontsize=10, pad=6)
        ax.set_xlabel("Date", fontsize=8)
        ax.set_ylabel("Count", fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(days, rotation=45, ha="right", fontsize=7)
        ax.tick_params(axis="y", labelsize=7)
        ax.legend(fontsize=8)

        buf = io.BytesIO()
        plt.tight_layout()
        fig.savefig(buf, format="png", dpi=150)
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception:
        try:
            plt.close("all")
        except Exception:
            pass
        return None

def _build_report_pdf(shop_id: str, period_start: datetime, period_end: datetime, summary: dict, insights: list[str], prev_summary: Optional[dict] = None, trend: Optional[Dict[str, Dict[str, int]]] = None) -> bytes:
    '''
    Renders a 1-page PDF using ReportLab with logo, bilingual title and KPI table.
    Returns PDF bytes.
    '''
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    _fontname = _register_thai_font_reportlab()
    if _fontname:
        styles["Normal"].fontName = _fontname
        if "Heading1" in styles: styles["Heading1"].fontName = _fontname
        if "Heading2" in styles: styles["Heading2"].fontName = _fontname
    styleH = styles["Heading1"]
    styleN = styles["Normal"]

    brand_primary = colors.Color(*_hex_to_rgb(BRAND_PRIMARY_HEX))
    brand_accent = colors.Color(*_hex_to_rgb(BRAND_ACCENT_HEX))

    story = []

    # Logo + Title (skip gracefully if logo path invalid)
    try:
        if REPORT_LOGO_PATH and os.path.exists(REPORT_LOGO_PATH):
            story.append(RLImage(REPORT_LOGO_PATH, width=3.2*cm, height=3.2*cm))
            story.append(Spacer(1, 0.3*cm))
    except Exception:
        logger.warning("logo not found or unreadable at %s", REPORT_LOGO_PATH)
        # continue without logo
        pass

    title_th = Paragraph(f"<b>{REPORT_TITLE_TH}</b>", styleH)
    title_en = Paragraph(f"<i>{REPORT_TITLE_EN}</i>", styleN)
    story.extend([title_th, title_en, Spacer(1, 0.4*cm)])

    # Period
    period_txt = f"{period_start.astimezone(timezone(timedelta(hours=7))).strftime('%d %b %Y')} – {period_end.astimezone(timezone(timedelta(hours=7))).strftime('%d %b %Y')}"
    story.append(Paragraph(f"Period: {period_txt}", styleN))
    story.append(Spacer(1, 0.4*cm))

# KPI Table (+%Δ if prev_summary provided)
    def _pct(curr, prev) -> str:
        try:
            if prev in (None, "-", ""):
                return "-"
            prev = float(prev); curr = float(curr)
            if prev == 0:
                return "—"
            pc = (curr - prev) * 100.0 / prev
            sign = "▲" if pc > 0 else ("▼" if pc < 0 else "•")
            return f"{sign} {pc:.0f}%"
        except Exception:
            return "-"

    pay_count = summary.get("payments_success")
    revenue_val = summary.get("revenue")
    revenue_txt = f"{revenue_val:,.2f}" if isinstance(revenue_val, (int, float)) else (revenue_val or "-")

    p = prev_summary or {}
    rows = [
        ["Metric", "Value", "%Δ"],
        ["Total Customers", summary.get("total_customers"), _pct(summary.get("total_customers"), p.get("total_customers"))],
        ["New Customers", summary.get("new_customers"), _pct(summary.get("new_customers"), p.get("new_customers"))],
        ["Active Chat Users", summary.get("active_chat_users"), _pct(summary.get("active_chat_users"), p.get("active_chat_users"))],
        ["Inbound Messages", summary.get("inbound_msgs"), _pct(summary.get("inbound_msgs"), p.get("inbound_msgs"))],
        ["Outbound Messages", summary.get("outbound_msgs"), _pct(summary.get("outbound_msgs"), p.get("outbound_msgs"))],
        ["Payments (confirmed)", pay_count, _pct(pay_count, p.get("payments_success"))],
        ["Revenue (THB)", revenue_txt, _pct(summary.get("revenue"), p.get("revenue"))],
    ]
    tbl = Table(rows, hAlign="LEFT", colWidths=[7*cm, 4*cm, 3*cm])

    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F5F5F4")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.HexColor("#111827")),
        ("FONTNAME", (0,0), (-1,0), _fontname or "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 11),
        ("ALIGN", (1,1), (-2,-1), "RIGHT"),
        ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#E5E7EB")),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 0.5*cm))

    # Insights bullets (if any)
    if insights:
        story.append(Paragraph("<b>Insights</b>", styleN))
        for t in insights:
            story.append(Paragraph(f"• {t}", styleN))
        story.append(Spacer(1, 0.4*cm))
    # Trend chart (messages per day)
    if trend:
        img = _chart_messages_trend_image(trend)
        if img:
            story.append(RLImage(img, width=16*cm, height=7*cm))
            story.append(Spacer(1, 0.5*cm))

    # CTA TH + EN
    cta_th = "อย่าลืมต่ออายุ Subscription เพื่อรับรายงานนี้ต่อเนื่องทุก 2 สัปดาห์"
    cta_en = "Don’t forget to renew your subscription to continue receiving this report every 2 weeks."
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph(cta_th, styleN))
    story.append(Paragraph(f"<i>{cta_en}</i>", styleN))

    doc.build(story)
    return buf.getvalue()

def _build_report_pdf_v3(shop_id: str, period_start: datetime, period_end: datetime,
                         summary: dict, prev_summary: Optional[dict],
                         trend: Optional[Dict[str, Dict[str, int]]]) -> bytes:
    '''Modern marketing style: header band, KPI cards with %Δ, styled chart, insights, and CTA.'''
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.2*cm, bottomMargin=1.2*cm)
    styles = getSampleStyleSheet()
    _fontname = _register_thai_font_reportlab()
    if _fontname:
        styles["Normal"].fontName = _fontname
        if "Heading1" in styles: styles["Heading1"].fontName = _fontname
        if "Heading2" in styles: styles["Heading2"].fontName = _fontname
    styleN = styles["Normal"]
    styleH = styles["Heading2"]

    brand_primary = colors.Color(*_hex_to_rgb(BRAND_PRIMARY_HEX))
    brand_accent  = colors.Color(*_hex_to_rgb(BRAND_ACCENT_HEX))

    story: List[Any] = []

    # Header band
    header_tbl = Table(
        [[
            RLImage(REPORT_LOGO_PATH, width=2.2*cm, height=2.2*cm) if (REPORT_LOGO_PATH and os.path.exists(REPORT_LOGO_PATH)) else "",
            Paragraph(f"<b>{REPORT_TITLE_TH}</b><br/><i>{REPORT_TITLE_EN}</i>", styleH)
        ]],
        colWidths=[2.6*cm, 14*cm]
    )
    header_tbl.setStyle(TableStyle([
    ("BACKGROUND", (0,0), (-1,-1), brand_primary),   # ใช้ Teal เป็นสีหลัก
    ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
    ("LEFTPADDING", (0,0), (-1,-1), 10),
    ("RIGHTPADDING", (0,0), (-1,-1), 10),
    ("TOPPADDING", (0,0), (-1,-1), 8),
    ("BOTTOMPADDING", (0,0), (-1,-1), 8),
    ("LINEBELOW", (0,0), (-1,-1), 2, brand_accent),  # คาดเส้นบาง ๆ สีส้มเป็น accent
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 0.25*cm))

    # Period
    period_txt = f"{period_start.astimezone(timezone(timedelta(hours=7))).strftime('%d %b %Y')} – {period_end.astimezone(timezone(timedelta(hours=7))).strftime('%d %b %Y')}"
    story.append(Paragraph(f"<b>Period:</b> {period_txt}", styleN))
    story.append(Spacer(1, 0.2*cm))

    # KPI cards (2x2)
    def pct(curr, prev):
        try:
            prev = float(prev); curr = float(curr)
            if prev == 0: return "—"
            p = (curr - prev) * 100.0 / prev
            sign = "▲" if p>0 else ("▼" if p<0 else "•")
            return f"{sign} {p:.0f}%"
        except Exception:
            return "-"

    p = prev_summary or {}
    revenue_val = summary.get("revenue")
    revenue_txt = f"{revenue_val:,.2f}" if isinstance(revenue_val, (int, float)) else (revenue_val or "-")

    kpis = [
        ("👥 Total Customers",  summary.get("total_customers"),  pct(summary.get("total_customers"),  p.get("total_customers"))),
        ("🆕 New Customers",    summary.get("new_customers"),    pct(summary.get("new_customers"),    p.get("new_customers"))),
        ("💬 Active Chat Users",summary.get("active_chat_users"),pct(summary.get("active_chat_users"),p.get("active_chat_users"))),
                ("💸 Revenue (THB)", summary.get("revenue"), pct(summary.get("revenue"), p.get("revenue"))),
    ]

    # Render KPI table (2x2 layout)
    data = []
    for i in range(0, len(kpis), 2):
        row = []
        for j in range(2):
            if i + j < len(kpis):
                k, v, delta = kpis[i + j]
                cell = Paragraph(
                    f"<b>{k}</b><br/><font size=14>{_fmt_int(v)}</font> <font color='{BRAND_ACCENT_HEX}'>{delta}</font>",
                    styleN,
                )
                row.append(cell)
        data.append(row)

    kpi_tbl = Table(data, colWidths=[8*cm, 8*cm])
    kpi_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), colors.whitesmoke),
        ("BOX", (0,0), (-1,-1), 0.5, brand_primary),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.grey),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
    ]))
    story.append(kpi_tbl)
    story.append(Spacer(1, 0.4*cm))

    # Trend chart (curved)
    img = _chart_messages_trend_image_v3(trend)
    if img:
        story.append(RLImage(img, width=16*cm, height=6*cm))
        story.append(Spacer(1, 0.4*cm))

    # Insights
    if insights:
        story.append(Paragraph("<b>🔎 Key Insights</b>", styleH))
        for i in insights:
            story.append(Paragraph(f"• {i}", styleN))
        story.append(Spacer(1, 0.4*cm))

    # CTA
    story.append(Paragraph(
        f"<font color='{BRAND_ACCENT_HEX}'>อย่าลืมต่ออายุบริการ เพื่อรับรายงานนี้ต่อเนื่องทุก 2 สัปดาห์</font>",
        styleN))
    story.append(Paragraph(
        f"<i>Don’t forget to renew your subscription to continue receiving this report every 2 weeks.</i>",
        styleN))

    doc.build(story)
    return buf.getvalue()
