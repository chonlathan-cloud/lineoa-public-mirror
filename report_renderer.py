import os
import io
import logging
import numpy as _np
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

# --- Assets base (for fonts/images bundled in the container) ---
_HERE = os.path.dirname(__file__)
_WEASY_BASE_FONTS = os.path.join(_HERE, "assets", "fonts")

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

# --- Helpers to normalize day bounds (inclusive end-of-day) ---
def _start_of_day_utc(dt: datetime) -> datetime:
    if not isinstance(dt, datetime):
        return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    if not getattr(dt, "tzinfo", None):
        dt = dt.replace(tzinfo=timezone.utc)
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)

def _end_of_day_utc(dt: datetime) -> datetime:
    s = _start_of_day_utc(dt)
    # inclusive end-of-day (23:59:59.999999)
    return s + timedelta(days=1, microseconds=-1)

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

    # Normalize to whole-day window in UTC: [00:00:00, 23:59:59.999999]
    start_bound = _start_of_day_utc(period_start)
    end_bound = _end_of_day_utc(period_end)

    def _coerce_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        dt_val: Optional[datetime] = None
        if hasattr(value, "to_datetime"):
            dt_val = value.to_datetime()
        elif isinstance(value, datetime):
            dt_val = value
        elif isinstance(value, str):
            from dateutil import parser as _p
            try:
                dt_val = _p.isoparse(value)
            except Exception:
                try:
                    dt_val = _p.parse(value)
                except Exception:
                    dt_val = None
        elif isinstance(value, (int, float)):
            try:
                dt_val = datetime.fromtimestamp(float(value), tz=timezone.utc)
            except Exception:
                dt_val = None
        if dt_val and not getattr(dt_val, "tzinfo", None):
            dt_val = dt_val.replace(tzinfo=timezone.utc)
        return dt_val

    # 1) total customers + new customers (inline evaluation)
    cust_col = db.collection("shops").document(shop_id).collection("customers")
    total_customers = 0
    customers: List[str] = []
    new_customers = 0
    for cdoc in cust_col.stream():
        total_customers += 1
        customers.append(cdoc.id)
        try:
            cdata = cdoc.to_dict() or {}
        except Exception:
            cdata = {}
        first_dt = _coerce_datetime(
            cdata.get("first_interaction_at")
            or cdata.get("created_at")
            or cdata.get("first_seen_at")
        )
        if first_dt and start_bound <= first_dt <= end_bound:
            new_customers += 1
    logger.info(f"Found {total_customers} total customers for shop_id={shop_id}")

    # 3) messages + active users + per-day trend
    day_keys = _daterange_days(start_bound, end_bound)
    trend: Dict[str, Dict[str, int]] = {d: {"inbound": 0, "outbound": 0} for d in day_keys}
    active_users: set[str] = set()

    # Iterate each customer to scope correctly to this shop
    for uid in customers:
        msg_col = cust_col.document(uid).collection("messages")
        try:
            q_msgs = (
                msg_col.where("timestamp", ">=", start_bound)
                       .where("timestamp", "<=", end_bound)
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

    # 4) revenue + payments count — in our system, payments are persisted only AFTER approval,
    # so summing all docs in the window is correct (no status filter needed).
    revenue = 0.0
    payments_success = 0
    try:
        pays = list_payments(shop_id, start=start_bound, end=end_bound, status=None, limit=2000)
        for p in pays:
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
    """
    Return @font-face CSS that embeds Thai-capable fonts.
    Priority:
      1) Bundled fonts under assets/fonts (works on Cloud Run)
      2) System fonts if found (local dev convenience)
    NOTE: When using WeasyPrint, pass base_url=_WEASY_BASE_FONTS so relative font URLs resolve.
    """
    css_parts = []

    # 1) Bundled fonts (ship with the app image)
    try:
        reg_bundled = os.path.join(_WEASY_BASE_FONTS, "NotoSansThai-Regular.ttf")
        bold_bundled = os.path.join(_WEASY_BASE_FONTS, "NotoSansThai-Bold.ttf")
        used_any = False
        if os.path.isfile(reg_bundled):
            css_parts.append(
                "@font-face{font-family:'ThaiBrand';src:url('NotoSansThai-Regular.ttf') format('truetype');font-weight:400;font-style:normal;}"
            )
            used_any = True
        if os.path.isfile(bold_bundled):
            css_parts.append(
                "@font-face{font-family:'ThaiBrand';src:url('NotoSansThai-Bold.ttf') format('truetype');font-weight:700;font-style:bold;}"
            )
            used_any = True
        if used_any:
            return "\n".join(css_parts)
    except Exception:
        pass

    # 2) System fonts (best-effort for local dev)
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
            if ("regular" in n or "dejavu" in n) and reg is None:
                reg = p
    if not reg and not bold:
        return ""
    if reg:
        css_parts.append(f"@font-face{{font-family:'ThaiBrand';src:url('file://{reg}') format('truetype');font-weight:400;}}")
    if bold:
        css_parts.append(f"@font-face{{font-family:'ThaiBrand';src:url('file://{bold}') format('truetype');font-weight:700;}}")
    return "\n".join(css_parts)

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

        import numpy as _np
        x = _np.arange(len(days), dtype=float)

        # Smooth curves using spline interpolation or moving average
        y_in, y_out = _np.array(inbound, dtype=float), _np.array(outbound, dtype=float)
        xs = x
        ys_in = y_in
        ys_out = y_out
        try:
            from scipy.interpolate import make_interp_spline  # type: ignore
            # Create more sample points for smoother curves if enough data points
            if len(x) >= 3:
                xs = _np.linspace(x.min(), x.max(), 200)
                ys_in = make_interp_spline(x, y_in, k=3)(xs)
                ys_out = make_interp_spline(x, y_out, k=3)(xs)
            # If less than 3 points, just use original points
        except Exception:
            # Fallback to 3-point moving average smoothing if scipy is not available
            def _ma(a):
                if a.size <= 2:
                    return a
                kernel = _np.ones(3)/3.0
                b = _np.convolve(a, kernel, mode="same")
                b[0] = a[0]; b[-1] = a[-1] # Preserve start/end points
                return b
            ys_in = _ma(y_in)
            ys_out = _ma(y_out)

        fig, ax = plt.subplots(figsize=(8, 4)) # ใช้ขนาดที่ปรับไปก่อนหน้านี้
        ax.plot(xs, ys_in, label="Inbound", color=BRAND_PRIMARY_HEX) # ใช้ xs, ys_in
        ax.plot(xs, ys_out, label="Outbound", color=BRAND_ACCENT_HEX) # ใช้ xs, ys_out
        ax.set_title("Daily Messages (14 days)")
        ax.set_xlabel("Date")
        ax.set_ylabel("Count")

        # --- เพิ่มส่วนนี้เพื่อกำหนดให้แกน X แสดงเป็นวันที่ ---
        ax.set_xticks(x) # กำหนดตำแหน่งของ tick บนแกน X ให้ตรงกับจำนวนวัน
        ax.set_xticklabels([d.split('-')[-1] for d in days], rotation=0, ha="right") # แสดงเฉพาะ "วัน" และหมุน 45 องศา

        buf = io.BytesIO()
        plt.tight_layout(pad=0.1)  # ลดระยะขอบของกราฟ
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

def _chart_messages_trend_svg(trend: Dict[str, Dict[str, int]]) -> Optional[str]:
    """
    Return an SVG string (not bytes) for the inbound/outbound per-day chart.
    Vector output keeps lines crisp in all PDF viewers (LINE in-app, iOS, Android).
    Uses Catmull–Rom spline converted to Bézier curves for smoothness.
    """
    try:
        if not trend:
            return None
        days = sorted(trend.keys())

        def _series(direction: str) -> List[float]:
            values: List[float] = []
            for day in days:
                raw = (trend.get(day) or {}).get(direction, 0)
                try:
                    if raw is None:
                        values.append(0.0)
                    else:
                        values.append(float(raw))
                except (ValueError, TypeError):
                    values.append(0.0)
            # guard against empty list (should not happen but keeps math safe)
            return values or [0.0]

        inbound = _series("inbound")
        outbound = _series("outbound")

        n = len(days)
        if n == 0:
            return None

        # --- Layout constants ---
        W, H = 600, 220         # overall viewport
        PAD_L, PAD_R = 40, 18
        PAD_T, PAD_B = 22, 28
        CH = H - PAD_T - PAD_B  # chart height
        CW = W - PAD_L - PAD_R  # chart width

        # --- Scale helpers ---
        def _minmax(series):
            lo = min(series) if series else 0
            hi = max(series) if series else 0
            if hi == lo:
                hi = lo + 1  # avoid div/0
            return lo, hi

        lo_all = min(min(inbound or [0]), min(outbound or [0]))
        hi_all = max(max(inbound or [0]), max(outbound or [1]))
        if hi_all == lo_all:
            hi_all = lo_all + 1

        def sx(i: int) -> float:
            if n == 1:
                return PAD_L + CW/2
            return PAD_L + (CW * i / (n - 1))

        def sy(v: float) -> float:
            # y grows downward; map [lo_all..hi_all] to [PAD_T..PAD_T+CH]
            t = (v - lo_all) / (hi_all - lo_all)
            return PAD_T + (1.0 - t) * CH

        # --- Catmull–Rom → Bézier conversion ---
        # see: https://www.cairographics.org/samples/cairo-paths/
        def cr_to_bezier(points):
            """points: list of (x,y). returns SVG path string using M + C commands."""
            if len(points) == 1:
                x, y = points[0]
                return f"M {x:.2f},{y:.2f}"
            if len(points) == 2:
                (x0,y0),(x1,y1) = points
                return f"M {x0:.2f},{y0:.2f} L {x1:.2f},{y1:.2f}"
            path = []
            p = points
            path.append(f"M {p[0][0]:.2f},{p[0][1]:.2f}")
            for i in range(len(p)-1):
                p0 = p[i-1] if i-1 >= 0 else p[i]
                p1 = p[i]
                p2 = p[i+1]
                p3 = p[i+2] if i+2 < len(p) else p[i+1]
                # Catmull-Rom to Bezier control points
                c1x = p1[0] + (p2[0] - p0[0]) / 6.0
                c1y = p1[1] + (p2[1] - p0[1]) / 6.0
                c2x = p2[0] - (p3[0] - p1[0]) / 6.0
                c2y = p2[1] - (p3[1] - p1[1]) / 6.0
                path.append(f"C {c1x:.2f},{c1y:.2f} {c2x:.2f},{c2y:.2f} {p2[0]:.2f},{p2[1]:.2f}")
            return " ".join(path)

        pts_in = [(sx(i), sy(v)) for i, v in enumerate(inbound)]
        pts_out = [(sx(i), sy(v)) for i, v in enumerate(outbound)]
        d_in = cr_to_bezier(pts_in)
        d_out = cr_to_bezier(pts_out)

        # Axis ticks (x as day-of-month)
        x_ticks = []
        for i, d in enumerate(days):
            dd = d.split("-")[-1]
            x = sx(i)
            y = PAD_T + CH + 16
            x_ticks.append(f"<text x='{x:.2f}' y='{y:.2f}' font-size='10' text-anchor='middle' fill='#6B7280' font-family='ThaiBrand, Arial, sans-serif'>{dd}</text>")

        # Horizontal gridlines (4 levels)
        grid = []
        for j in range(0, 4):
            yy = PAD_T + (CH * j / 3.0)
            grid.append(f"<line x1='{PAD_L}' y1='{yy:.2f}' x2='{W-PAD_R}' y2='{yy:.2f}' stroke='#E5E7EB' stroke-width='1'/>")

        # Build SVG
        svg = f"""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-label="Message trend" shape-rendering="geometricPrecision" preserveAspectRatio="xMidYMid meet" style="display:block">
  <g>
    {''.join(grid)}
    <path d="{d_in}" fill="none" stroke="{BRAND_PRIMARY_HEX}" stroke-width="2.8"
          stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>
    <path d="{d_out}" fill="none" stroke="{BRAND_ACCENT_HEX}" stroke-width="2.8"
          stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>
    {''.join(x_ticks)}
  </g>
</svg>
""".strip()
        return svg
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

    def _pct(curr, prev):
        try:
            prev = float(prev)
            curr = float(curr)
            if prev == 0:
                return "0%"
            delta = (curr - prev) * 100.0 / prev
            arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "•")
            return f"{arrow} {abs(delta):.0f}%"
        except Exception:
            return "0%"

    font_css = _detect_thai_font_css()
    period_txt = _period_text_th(start_dt, end_dt)
    report_date_txt = end_dt.astimezone(timezone(timedelta(hours=7))).strftime("%d/%m/%Y")

    inbound = summary.get("inbound_msgs", 0) or 0
    outbound = summary.get("outbound_msgs", 0) or 0
    total_msgs = inbound + outbound
    prev_inbound = prev_summary.get("inbound_msgs") if prev_summary else 0
    prev_outbound = prev_summary.get("outbound_msgs") if prev_summary else 0
    prev_total = (prev_inbound or 0) + (prev_outbound or 0)
    k_new = summary.get("new_customers", 0)
    k_total = summary.get("total_customers", 0)
    prev_new = prev_summary.get("new_customers") if prev_summary else 0
    prev_total_customers = prev_summary.get("total_customers") if prev_summary else 0
    revenue_value = 300
    prev_revenue = prev_summary.get("revenue") if prev_summary else 0

    chart_data_uri = ""
    # --- Build chart markup (prefer inline SVG to keep curves crisp in LINE viewer) ---
    chart_html = ""
    svg_text = _chart_messages_trend_svg(trend)
    if svg_text:
        # Inline SVG → WeasyPrint จะฝังแบบเวกเตอร์ (ไม่รันเป็นภาพ)
        chart_html = svg_text
    else:
        # Fallback PNG เฉพาะกรณี SVG สร้างไม่สำเร็จ
        chart_buf = _chart_messages_trend_image(trend)
        if chart_buf:
            chart_data_uri = "data:image/png;base64," + base64.b64encode(chart_buf.getvalue()).decode("ascii")
            chart_html = f"<img src='{chart_data_uri}' alt='Message trend chart' style='max-width:100%;height:auto;display:block' />"

    key_insight = insights[0] if insights else "ยังไม่มีข้อมูล insight สำหรับช่วงนี้"
    recommendation = insights[1] if len(insights) > 1 else "ลองโปรโมตข้อความหรือคูปองเพื่อกระตุ้น Conversion เพิ่มเติม"

    html = f"""<!doctype html>
<html lang="th">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{REPORT_TITLE_EN} • Mini Snapshot</title>
<style>
  {font_css}
  :root {{
    --teal: {BRAND_PRIMARY_HEX};
    --orange: {BRAND_ACCENT_HEX};
    --offwhite: #F8F9FA;
    --lightgray: #F5F5F4;
    --text: #111827;
  }}
  @page {{ size: A4; margin: 8mm; }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 1px; background: var(--offwhite); color: var(--text); /* ลด padding ของ body */
    font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial;
  }}
  .container {{ max-width: 1200px; margin: 0 auto; border-radius: 12px; display: grid; gap: 3px; }} /* ขยายความกว้างและจัดกลาง */
  .title {{
    background: var(--teal);
    color: white; padding: 16px 20px; border-radius: 12px 12px 0 0; /* เพิ่มความโค้งมนให้ขอบบน */
    box-shadow: 0 8px 20px rgba(0,0,0,.12);
  }}
  .title h1 {{ margin: 0; font-size: 24px; letter-spacing: .3px; }}
  .subtitle {{ margin: 6px 0 0; font-size: 13px; opacity: .9; }}
  .grid {{ display: grid; gap: 5px; }} /* ลด gap ของ grid */
  .cards {{ grid-template-columns: repeat(2, 1fr); margin-top: 5px; }} /* ลด margin-top ของ cards */
  .card {{
    text-align: center;
    padding: 22px 16px;
    border-radius: 12px;
    background: white;
    box-shadow: 0 2px 8px rgba(0,0,0,.06);
    border: 1px solid rgba(0,0,0,0.04);
    min-height: 130px;
  }}
  .card h3 {{
    margin: 0 0 12px 0;
    font-weight: 600;
    font-size: 13px;
    line-height: 1.35;
    color: #374151;
    max-width: 100%;
    word-break: break-word;
  }}
  .card .value {{
    font-size: 26px;
    font-weight: 700;
    line-height: 1.2;
    margin: 0 0 12px 0;
    display: block;
  }}
  .card .delta {{
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
    color: #1f2937;
    background: rgba(255,255,255,0.78);
    padding: 5px 14px;
    border-radius: 999px;
    line-height: 1.4;
  }}
  .card--1 {{ background: #E6FFFB; }}
  .card--2 {{ background: #FFF7ED; }}
  .card--3 {{ background: #EEF2FF; }}
  .card--4 {{ background: #ECFDF5; }}
  .section {{ background: white; padding: 12px; border-radius: 14px; box-shadow: 0 2px 8px rgba(0,0,0,.06); }} /* ลด padding ของ section */
  .section h2 {{ margin: 0 0 8px 0; font-size: 16px; }}
  .legend {{ display:flex; gap:12px; align-items:center; margin-bottom: 10px; font-size:12px; color:#4B5563; }}
  .legend span {{ display:inline-flex; align-items:center; gap:6px; }}
  .dot {{ width:10px; height:10px; border-radius:999px; display:inline-block; }}
  .chart-wrapper img {{ width: 100%; height: auto; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,.04); }}
  .chart-wrapper svg {{ width: 100%; height: auto; display: block; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,.04); }}
  .chart-empty {{ font-size: 13px; color: #6B7280; padding: 24px 0; text-align: center; }}
  .insights {{ display:grid; gap:10px; margin-top:10px; }} /* ลด gap และ margin-top ของ insights */
  .insight-card {{
    background: linear-gradient(180deg, #ffffff, #fafafa);
    border: 1px solid #E5E7EB; border-radius: 14px; padding: 5px 5px;
    box-shadow: 0 4px 18px rgba(249,115,22,.15);
  }}
  .insight-title {{ font-weight:700; font-size:16px; margin:10px 0 4px 0; }}
  .insight-body {{ margin:0; color:#374151; line-height:1.6; }}
  .badge {{ display:inline-block; background: var(--orange); color:white; padding:4px 8px; border-radius: 999px; font-size: 12px; margin-left:8px; }}
  .footer {{ text-align:right; color:#6B7280; font-size:12px; margin-top:12px; }}
  @media (max-width: 900px) {{
    .cards {{ grid-template-columns: 1fr; }}
    .two-cols {{ grid-template-columns: 1fr; }}
  }}
  @media print {{
    body {{ background: white; padding: -10; }}
    .card, .section, .insight-card {{ box-shadow: none; border: 1px solid #eee; }}
    .container {{ max-width: unset; padding: 1mm 1mm; }} /* ปรับขอบบน/ล่าง/ข้างให้แคบลง */
  }}
</style>
</head>
<body>
  <div class="container">
    <div class="title">
      <h1>Message Insights Snapshot</h1>
      <div class="subtitle">{REPORT_TITLE_EN} • Period: {period_txt}</div>
      <div class="subtitle">ออกรายงานวันที่ {report_date_txt} • Shop: {shop_id}</div>
    </div>

    <div class="grid cards">
      <div class="card card--1">
        <h3>Total Messages</h3>
        <div class="value">{_fmt_int(total_msgs)}</div>
        <div class="delta">{_pct(total_msgs, prev_total)}</div>
      </div>
      <div class="card card--2">
        <h3>New Customers</h3>
        <div class="value">{_fmt_int(k_new)}</div>
        <div class="delta">{_pct(k_new, prev_new)}</div>
      </div>
      <div class="card card--3">
        <h3>Total Customers</h3>
        <div class="value">{_fmt_int(k_total)}</div>
        <div class="delta">{_pct(k_total, prev_total_customers)}</div>
      </div>
      <div class="card card--4">
        <h3>Revenue</h3>
        <div class="value">{_fmt_int(revenue_value)}</div>
        <div class="delta">{_pct(revenue_value, prev_revenue)}</div>
      </div>
    </div>

    <div class="section" style="margin-top:16px;">
      <h2>Message Trend (14 days)</h2>
      <div class="legend">
        <span><span class="dot" style="background: var(--teal)"></span>Inbound</span>
        <span><span class="dot" style="background: var(--orange)"></span>Outbound</span>
      </div>
        <div class="chart-wrapper" style="min-height: 220px;">
        {chart_html if chart_html else "<div class='chart-empty'>ไม่มีข้อมูลเพียงพอในการสร้างกราฟสำหรับช่วงนี้</div>"}
        </div>
      <div style="margin-top:10px;">
        <h2 style="font-size:15px; margin:0 0 6px 0;">Messaging Overview</h2>
        <p style="font-size:13px; color:#374151; line-height:1.6;">
          มีการส่งข้อความทั้งหมด {_fmt_int(total_msgs)} ข้อความในช่วงรายงาน โดย inbound {_fmt_int(inbound)} และ outbound {_fmt_int(outbound)}.
          แนวโน้มเทียบกับช่วงก่อนหน้า: {_pct(total_msgs, prev_total)} สำหรับข้อความรวม,
          {_pct(inbound, prev_inbound)} สำหรับ inbound และ {_pct(outbound, prev_outbound)} สำหรับ outbound.
        </p>
      </div>
      <div class="insights" style="margin-top:2px;">
        <div class="insight-card">
          <div class="insight-title"> Key Insight<span class="badge">Storytelling</span></div>
          <p class="insight-body">"{key_insight}"</p>
        </div>
      </div>
    </div>
    <div class="footer">อย่าลืมกดติดตามเรา มีอะไรมาให้อัปเดตตลอดเวลา รอดูได้เลย</div>
  </div>
</body>
</html>"""
    try:
        if _WEASYPRINT_AVAILABLE:
            return HTML(string=html, base_url=_WEASY_BASE_FONTS).write_pdf()
    except Exception as e:
        logger.error("Mini report WeasyPrint failed: %s", e)

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
    html = f"""
<!doctype html>
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
        * {{
            box-sizing: border-box;
        }}
        body {{
            margin: 0;
            padding: 28px;
            background: linear-gradient(180deg, #f5f7fa 0%, #ffffff 70%);
            color: var(--text);
            font-family: 'ThaiBrand', ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial;
            -webkit-text-size-adjust: 100%;
        }}
        .container {{
            max-width: 1040px;
            margin: 0 auto;
            display: flex;
            flex-direction: column;
            gap: 24px;
        }}
        .title {{
            position: relative;
            background: linear-gradient(135deg, var(--teal), #006a6a);
            color: white;
            padding: 32px 36px;
            border-radius: 22px;
            box-shadow: 0 20px 48px rgba(0, 0, 0, 0.18);
            overflow: hidden;
            display: flex;
            flex-direction: column;
            gap: 18px;
        }}
        .title::after {{
            content: "";
            position: absolute;
            inset: 0;
            background: radial-gradient(circle at top right, rgba(255, 255, 255, 0.35), transparent 55%);
            mix-blend-mode: screen;
        }}
        .title-head {{
            position: relative;
            z-index: 1;
        }}
        h1 {{
            margin: 0;
            font-size: 30px;
            letter-spacing: .3px;
        }}
        .title-meta {{
            position: relative;
            z-index: 1;
            display: flex;
            flex-wrap: wrap;
            gap: 12px 20px;
            align-items: center;
            font-size: 14px;
            opacity: .92;
        }}
        .subtitle {{
            margin: 0;
        }}
        .grid {{
            display: grid;
            gap: 15px;
        }}
        .cards {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }}

        /* --- START: FINAL SOLUTION --- */
        .card {{
            position: relative;
            padding: 26px 24px;
            border-radius: 20px;
            background: white;
            box-shadow: 0 4px 12px rgba(0, 128, 128, 0.08), 0 10px 30px rgba(0, 128, 128, 0.1);
            border: 1px solid rgba(0, 128, 128, 0.08);
            text-align: center;
            min-height: 160px;
        }}
        /* --- END: FINAL SOLUTION --- */

        .card::after {{
            content: "";
            position: absolute;
            inset: 0;
            border-radius: inherit;
            background: linear-gradient(140deg, rgba(255, 255, 255, 0.25), transparent 45%);
            opacity: 0;
            transition: opacity .25s ease;
        }}
        .card:hover::after {{
            opacity: 1;
        }}
        .card h3 {{
            margin: 0 0 14px 0;
            font-weight: 600;
            font-size: 14px;
            line-height: 1.4;
            color: #374151;
            white-space: normal;
            overflow: visible;
            max-width: 100%;
            word-break: break-word;
        }}
        .card .value {{
            font-size: 38px;
            font-weight: 700;
            color: #0f172a;
            line-height: 1.05;
            margin: 0 0 14px 0;
            display: block;
        }}
        .card .delta {{
            font-size: 13px;
            font-weight: 600;
            color: #115e59;
            background: rgba(255, 255, 255, 0.72);
            padding: 6px 16px;
            border-radius: 999px;
            display: inline-block;
            line-height: 1.4;
        }}
        .card--1 {{ background: linear-gradient(135deg, #E6FFFB, #F6FFFD); }}
        .card--2 {{ background: linear-gradient(135deg, #FFF7ED, #FFF2E6); }}
        .card--3 {{ background: linear-gradient(135deg, #EEF2FF, #F4F6FF); }}
        .card--4 {{ background: linear-gradient(135deg, #ECFDF5, #F4FFF9); }}
        .insights {{
            display: grid;
            gap: 20px;
            margin-top: 8px;
        }}
        .insight-card {{
            position: relative;
            background: linear-gradient(180deg, #ffffff, #fafafa);
            border: 1px solid rgba(249, 115, 22, 0.25);
            border-radius: 20px;
            padding: 22px 24px;
            box-shadow: 0 22px 36px rgba(249, 115, 22, .18);
            overflow: hidden;
        }}
        .insight-card::before {{
            content: "";
            position: absolute;
            inset: 0;
            background: radial-gradient(circle at top left, rgba(249, 115, 22, 0.18), transparent 55%);
        }}
        .insight-card > * {{
            position: relative;
            z-index: 1;
        }}
        .insight-title {{
            font-weight: 700;
            font-size: 16px;
            margin: 0 0 8px 0;
        }}
        .insight-body {{
            margin: 0;
            color: #374151;
            line-height: 1.6;
        }}
        .badge {{
            display: inline-block;
            background: var(--orange);
            color: white;
            padding: 4px 8px;
            border-radius: 999px;
            font-size: 12px;
            margin-left: 8px;
        }}
        .footer {{
            text-align: right;
            color: #6B7280;
            font-size: 12px;
            margin-top: 0;
            position: relative;
            z-index: 1;
        }}
        @media (max-width: 900px) {{
            body {{
                padding: 18px;
            }}
            .cards {{
                grid-template-columns: 1fr;
            }}
        }}
        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            .card,
            .section,
            .insight-card {{
                box-shadow: none;
            }}
            .container {{
                max-width: unset;
                padding: 16mm;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="title">
            <div class="title-head">
                <h1>{REPORT_TITLE_TH}</h1>
            </div>
            <div class="title-meta">
                <div class="subtitle">{REPORT_TITLE_EN} • Period: {period_txt}</div>
            </div>
        </div>

        <!-- KPI Cards -->
        <div class="grid cards">
            <div class="card card--1">
                <h3>Total Customers</h3>
                <div class="value">{_fmt_int(k_total)}</div>
                <div class="delta">{_pct(k_total, p.get('total_customers'))}</div>
            </div>
            <div class="card card--2">
                <h3>New Customers</h3>
                <div class="value">{_fmt_int(k_new)}</div>
                <div class="delta">{_pct(k_new, p.get('new_customers'))}</div>
            </div>
            <div class="card card--3">
                <h3>Active Chat Users</h3>
                <div class="value">{_fmt_int(k_act)}</div>
                <div class="delta">{_pct(k_act, p.get('active_chat_users'))}</div>
            </div>
            <div class="card card--4">
                <h3>Revenue (THB)</h3>
                <div class="value">{_fmt_money(k_rev)}</div>
                <div class="delta">{_pct(k_rev, p.get('revenue'))}</div>
            </div>
        </div>

        <!-- Insights -->
        <div class="insights">
            {"".join(f'<div class="insight-card"><div class="insight-title"> Insight</div><p class="insight-body">{i}</p></div>' for i in insights)}
        </div>

        <div class="footer">
            Prepared by AI for Org • Shop: {shop_id}
        </div>
    </div>
</body>
</html>
"""

    # 5) เรนเดอร์ PDF
    try:
        if _WEASYPRINT_AVAILABLE:
            return HTML(string=html, base_url=_WEASY_BASE_FONTS).write_pdf()
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

        fig, ax = plt.subplots(figsize=(5.0, 2.2))
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
        plt.tight_layout(pad=0.1)  # ลดระยะขอบของกราฟ
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
