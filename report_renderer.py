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

# --- Report constants ---
REPORT_LOGO_PATH = os.environ.get("REPORT_LOGO_PATH", "/mnt/data/Logo.png").strip()
BRAND_PRIMARY_HEX = os.environ.get("BRAND_PRIMARY_HEX", "#2B5EA4").strip()  # light navy
BRAND_ACCENT_HEX = os.environ.get("BRAND_ACCENT_HEX", "#7FADEB").strip()    # soft light blue
REPORT_TITLE_TH = os.environ.get("REPORT_TITLE_TH", "รายงานสรุปข้อมูลลูกค้า").strip()
REPORT_TITLE_EN = os.environ.get("REPORT_TITLE_EN", "Customer Insight Report").strip()

logger = logging.getLogger(__name__)


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

def _compose_rule_based_insights(summary: Dict[str, Any], prev: Optional[Dict[str, Any]], trend: Optional[Dict[str, Dict[str, int]]]) -> List[str]:
    '''Return short bilingual insights (TH+EN) based on deltas and trend.'''
    tips: List[str] = []

    def _pct(curr, prv):
        try:
            prv = float(prv)
            curr = float(curr)
            if prv == 0:
                return None
            return (curr - prv) * 100.0 / prv
        except Exception:
            return None

    if prev:
        d_in  = _pct(summary.get("inbound_msgs"), prev.get("inbound_msgs"))
        d_new = _pct(summary.get("new_customers"), prev.get("new_customers"))
        d_act = _pct(summary.get("active_chat_users"), prev.get("active_chat_users"))
        d_rev = _pct(summary.get("revenue"), prev.get("revenue"))
        for v, th_up, th_dn, en_up, en_dn in [
            (d_in,  "การมีส่วนร่วม (Inbound) เพิ่มขึ้น {x:.0f}%", "การมีส่วนร่วมลดลง {x:.0f}%", "Engagement (Inbound) up {x:.0f}%", "Engagement down {x:.0f}%"),
            (d_new, "ลูกค้าใหม่เพิ่มขึ้น {x:.0f}%",           "ลูกค้าใหม่ลดลง {x:.0f}%",           "New customers up {x:.0f}%",        "New customers down {x:.0f}%"),
            (d_act, "ผู้ใช้งานแชทเพิ่มขึ้น {x:.0f}%",          "ผู้ใช้งานแชทลดลง {x:.0f}%",          "Active chat users up {x:.0f}%",     "Active chat users down {x:.0f}%"),
            (d_rev, "รายได้เพิ่มขึ้น {x:.0f}%",                "รายได้ลดลง {x:.0f}%",                "Revenue up {x:.0f}%",               "Revenue down {x:.0f}%"),
        ]:
            if v is None:
                continue
            tips.append((th_up if v > 0 else th_dn).format(x=abs(v)) + " | " + (en_up if v > 0 else en_dn).format(x=abs(v)))

    # Peak day from trend
    try:
        if trend:
            days = sorted(trend.keys())
            peak_day = max(days, key=lambda d: trend[d].get("inbound", 0))
            tips.append(f"วันพีคคือ {peak_day} | Peak day: {peak_day}.")
    except Exception:
        pass

    if not tips:
        tips.append("ระบบพร้อมใช้งานและข้อมูลคงที่ | System stable and metrics unchanged.")
    return tips[:3]

def _chart_messages_trend_image_v3(trend: Dict[str, Dict[str, int]]) -> Optional[io.BytesIO]:
    '''Styled chart: area for inbound, line for outbound, and an average guide.'''
    try:
        if not trend:
            return None
        days = sorted(trend.keys())
        inbound  = [trend[d].get("inbound", 0) for d in days]
        outbound = [trend[d].get("outbound", 0) for d in days]
        avg_in = (sum(inbound) / float(len(inbound))) if len(inbound) else 0.0

        fig, ax = plt.subplots(figsize=(6.0, 3.2)) #figsize=(6.0, 2.2)
        x = list(range(len(days)))
        width = 0.35

        # compact bar chart
        ax.bar([i - width/2 for i in x], inbound, width=width, label="Inbound")
        ax.bar([i + width/2 for i in x], outbound, width=width, label="Outbound")

        if len(x) >= 2:
            ax.hlines(avg_in, xmin=0, xmax=len(x)-1, linestyles="dashed", label="Inbound avg")

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
        ["Active Promotions", summary.get("promo_active"), _pct(summary.get("promo_active"), p.get("promo_active"))],
        ["Payments (confirmed)", pay_count, _pct(pay_count, p.get("payments_success"))],
        ["Revenue (THB)", revenue_txt, _pct(summary.get("revenue"), p.get("revenue"))],
    ]
    tbl = Table(rows, hAlign="LEFT", colWidths=[7*cm, 4*cm, 3*cm])

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
        ("BACKGROUND", (0,0), (-1,-1), brand_accent),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING", (0,0), (-1,-1), 10),
        ("RIGHTPADDING", (0,0), (-1,-1), 10),
        ("TOPPADDING", (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
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
        ("💸 Revenue (THB)",    revenue_txt,                     pct(summary.get("revenue"),          p.get("revenue"))),
    ]

    cells, row = [], []
    for i, (title, value, delta) in enumerate(kpis, start=1):
        card = Table(
            [[Paragraph(f"<b>{title}</b>", styleN)],
             [Paragraph(f"<para align=center><font size=16><b>{value}</b></font></para>", styleN)],
             [Paragraph(f"<para align=right><i>{delta}</i></para>", styleN)]],
            colWidths=[8*cm]
        )
        card.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,-1), colors.whitesmoke),
            ("BOX", (0,0), (-1,-1), 0.5, brand_primary),
            ("TOPPADDING", (0,0), (-1,-1), 6),
            ("BOTTOMPADDING", (0,0), (-1,-1), 6),
            ("LEFTPADDING", (0,0), (-1,-1), 8),
            ("RIGHTPADDING", (0,0), (-1,-1), 8),
        ]))
        row.append(card)
        if i % 2 == 0:
            cells.append(row); row = []
    if row:
        row.append("")
        cells.append(row)
    story.append(Table(cells, colWidths=[8*cm, 8*cm], hAlign="LEFT"))
    story.append(Spacer(1, 0.2*cm))

    # Styled chart
    if trend:
        img = _chart_messages_trend_image_v3(trend)
        if img:
            story.append(RLImage(img, width=16*cm, height=7*cm))
            story.append(Spacer(1, 0.2*cm))

    # Insight box
    insights_v3 = _compose_rule_based_insights(summary, p, trend)
    if insights_v3:
        rows = [[Paragraph("<b>💡 Insights</b>", styleN)]] + [[Paragraph(t, styleN)] for t in insights_v3]
        ibox = Table(rows, colWidths=[16*cm])
        ibox.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,-1), colors.HexColor("#F2F7FF")),
            ("BOX", (0,0), (-1,-1), 0.5, brand_primary),
            ("LEFTPADDING", (0,0), (-1,-1), 10),
            ("RIGHTPADDING", (0,0), (-1,-1), 10),
            ("TOPPADDING", (0,0), (-1,-1), 8),
            ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ]))
        story.append(ibox)
        story.append(Spacer(1, 0.2*cm))

    # CTA footer
    story.append(Table([[""]], colWidths=[16*cm], style=TableStyle([("LINEBELOW", (0,0), (-1,-1), 1, brand_primary)])))
    cta_th = "อย่าลืมต่ออายุ Subscription เพื่อรับรายงานนี้ต่อเนื่องทุก 2 สัปดาห์"
    cta_en = "Don’t forget to renew your subscription to continue receiving this report every 2 weeks."
    story.append(Paragraph(cta_th, styleN))
    story.append(Paragraph(f"<i>{cta_en}</i>", styleN))

    doc.build(story)
    return buf.getvalue()

def _build_report_pdf_weasy(shop_id: str, period_start: datetime, period_end: datetime,
                            summary: dict, prev_summary: Optional[dict],
                            trend: Optional[Dict[str, Dict[str, int]]]) -> bytes:
    if not _WEASYPRINT_AVAILABLE or not _JINJA_AVAILABLE:
        raise RuntimeError("WeasyPrint/Jinja2 not available. pip install WeasyPrint Jinja2")

    th_tz = timezone(timedelta(hours=7))
    period_text = f"{period_start.astimezone(th_tz).strftime('%d %b %Y')} – {period_end.astimezone(th_tz).strftime('%d %b %Y')}"
    prev = prev_summary or {}

    def _delta(curr, prv):
        try:
            prv = float(prv); curr = float(curr)
            if prv == 0: return ("• 0%", "flat")
            pct = (curr - prv) * 100.0 / prv
            return ((f"▲ {pct:.0f}%", "up") if pct>0 else (f"▼ {abs(pct):.0f}%", "down"))
        except Exception:
            return ("-", "flat")

    kpi_defs = [
        ("👥 Total Customers",  "total_customers"),
        ("🆕 New Customers",    "new_customers"),
        ("💬 Active Chat Users","active_chat_users"),
        ("💸 Revenue (THB)",    "revenue"),
    ]
    kpis=[]
    for title,key in kpi_defs:
        val = summary.get(key)
        if key == "revenue" and isinstance(val,(int,float)):
            val = f"{val:,.2f}"
        text, ddir = _delta(summary.get(key), prev.get(key))
        kpis.append({"title":title,"value":val,"delta_text":text,"delta_dir":ddir})

    chart_uri = None
    try:
        img = _chart_messages_trend_image_v3(trend) if trend else None
        if img:
            chart_uri = "data:image/png;base64," + base64.b64encode(img.getvalue()).decode("ascii")
    except Exception:
        pass

    insights = _compose_rule_based_insights(summary, prev_summary, trend)
    logo_uri = f"file://{REPORT_LOGO_PATH}" if (REPORT_LOGO_PATH and os.path.exists(REPORT_LOGO_PATH)) else None

    thai_css = _detect_thai_font_css()
    css = f"""
    :root{{--brand:{BRAND_PRIMARY_HEX};--accent:{BRAND_ACCENT_HEX};--ink:#0b1324;--muted:#6b7280;--card:#f8fbff}}
    {thai_css}
    *{{box-sizing:border-box}}
    body{{font-family:{'ThaiBrand, ' if thai_css else ''}-apple-system,BlinkMacSystemFont,'Helvetica',Arial,sans-serif;color:var(--ink);margin:12px}}
    .hero{{
      display:flex;gap:10px;align-items:center;
      background: linear-gradient(90deg, var(--accent), #d8e8ff);
      padding:10px 12px;border-radius:10px;margin-bottom:6px
    }}
    .logo{{width:42px;height:42px;object-fit:contain}}
    h1{{margin:0;font-weight:700;font-size:20px}}
    h2{{margin:0;font-weight:400;font-size:13px;opacity:.85}}
    .period{{margin:6px 2px 4px;font-size:12px}}
    .kpi-grid{{display:grid;grid-template-columns:1fr 1fr;gap:6px}}
    .kpi-card{{
      border:1px solid rgba(43,94,164,.25);
      border-radius:8px;padding:8px;background:var(--card)
    }}
    .kpi-title{{font-weight:700;margin-bottom:4px}}
    .kpi-value{{font-size:22px;font-weight:700;text-align:center}}
    .kpi-delta{{display:flex;justify-content:flex-end;margin-top:3px}}
    .kpi-delta span{{padding:2px 8px;border-radius:999px;font-size:12px;color:#111}}
    .kpi-delta.up span{{background:#e7f6ec;color:#0a6d34}}
    .kpi-delta.down span{{background:#ffecec;color:#a8322f}}
    .kpi-delta.flat span{{background:#eef2f7;color:#384454}}
    .chart{{background:#fff;border:1px solid #e6eefc;border-radius:10px;padding:6px;margin-top:4px}}
    .chart img{{width:100%;border-radius:6px}}
    .insights{{margin-top:4px;padding:6px;border:1px solid rgba(43,94,164,.25);background:#F2F7FF;border-radius:10px}}
    .insights h3{{margin:0 0 4px;font-size:13px}}
    .insights li{{font-size:12px;margin-bottom:2px}}
    .cta hr{{border:none;border-top:1px solid rgba(43,94,164,.45);margin:8px 0 4px}}
    footer p{{margin:2px 0;font-size:11px}}
    """
    tpl = Template("""
    <!doctype html><html lang="th"><head><meta charset="utf-8"><title>Owner Report</title><style>{{ css }}</style></head><body>
      <header class="hero">
        {% if logo_uri %}<img class="logo" src="{{ logo_uri }}" alt="logo">{% endif %}
        <div><h1>{{ title_th }}</h1><h2>{{ title_en }}</h2></div>
      </header>
      <p class="period"><b>Period:</b> {{ period_text }}</p>
      <section class="kpi-grid">
        {% for k in kpis %}
        <div class="kpi-card">
          <div class="kpi-title">{{ k.title }}</div>
          <div class="kpi-value">{{ k.value }}</div>
          <div class="kpi-delta {{ k.delta_dir }}"><span>{{ k.delta_text }}</span></div>
        </div>
        {% endfor %}
      </section>
      {% if chart_uri %}<section class="chart"><img src="{{ chart_uri }}" alt="trend chart"></section>{% endif %}
      {% if insights %}<section class="insights"><h3>💡 Insights</h3><ul>{% for line in insights %}<li>{{ line }}</li>{% endfor %}</ul></section>{% endif %}
      <footer class="cta"><hr><p>อย่าลืมต่ออายุ Subscription เพื่อรับรายงานนี้ต่อเนื่องทุก 2 สัปดาห์</p>
      <p><i>Don’t forget to renew your subscription to continue receiving this report every 2 weeks.</i></p></footer>
    </body></html>
    """)
    html = tpl.render(
        css=css,
        logo_uri=logo_uri,
        title_th=REPORT_TITLE_TH,
        title_en=REPORT_TITLE_EN,
        period_text=period_text,
        kpis=kpis,
        chart_uri=chart_uri,
        insights=insights,
    )
    try:
        pdf_bytes = HTML(string=html, base_url=".").write_pdf()
        if not pdf_bytes or (isinstance(pdf_bytes, (bytes, bytearray)) and len(pdf_bytes) == 0):
            raise RuntimeError("weasyprint returned empty bytes")
        return pdf_bytes
    except Exception as e:
        logging.exception("WeasyPrint render failed: %s", e)
        return None