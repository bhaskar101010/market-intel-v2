"""
Pre-compute locality intelligence reports using Gemini.
Stores rendered HTML in Atlas → locality_reports collection.

Usage:
  python generate_reports.py                    # all localities
  python generate_reports.py Gachibowli         # single locality test
"""

import sys, os, json, re, time, traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import google.generativeai as genai
from pymongo import MongoClient
from mongo_supply import fetch_supply, get_locality_intelligence

# ── Config ──────────────────────────────────────────────────────────────────
GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "AIzaSyD6kZTep1c_VRSZrVhycln-Ip53jnX5NLs")
ATLAS_URI  = os.environ.get("ATLAS_URI",
    "mongodb+srv://bhdev:mQNJcKiRikdmVhWY@cluster0.olxmzw1.mongodb.net/market_intel?retryWrites=true&w=majority")
MODEL      = "gemini-3.8-flash"

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel(MODEL)
client = MongoClient(ATLAS_URI)
db = client["market_intel"]


# ── Data Gathering ──────────────────────────────────────────────────────────
def gather_locality_data(locality: str, city: str = "Hyderabad") -> dict:
    # Use the existing normalized fetch_supply for project data
    supply_data = fetch_supply(locality, city, fetch_infra=False)
    projects = supply_data.get("supply_projects") or []
    ss = supply_data.get("supply_summary") or {}

    # Build status distribution & developer counts from normalized projects
    status_dist = {}
    configs_dist = {}
    developers = {}
    gated = 0
    for p in projects:
        s = p.get("status") or "Unknown"
        status_dist[s] = status_dist.get(s, 0) + 1
        for c in (p.get("configurations") or []):
            configs_dist[c] = configs_dist.get(c, 0) + 1
        dev = p.get("developer") or ""
        if dev and isinstance(dev, str):
            developers[dev] = developers.get(dev, 0) + 1
        if p.get("is_gated"):
            gated += 1

    supply_summary = {
        "total_projects": ss.get("total_projects") or len(projects),
        "avg_psf": ss.get("avg_price_per_sqft"),
        "min_price_range": ss.get("min_price"),
        "max_price_range": ss.get("max_price"),
        "status_distribution": status_dist,
        "config_distribution": dict(sorted(configs_dist.items(), key=lambda x: -x[1])[:10]),
        "top_developers": dict(sorted(developers.items(), key=lambda x: -x[1])[:10]),
        "gated_communities": gated,
    }

    # Use existing get_locality_intelligence for 99a + Google data
    li = get_locality_intelligence(locality, city)

    locality_intel = {}
    if li.get("has_99a"):
        locality_intel = {
            "average_rate": f"₹{li['avg_rate_psf']:,}" if li.get("avg_rate_psf") else None,
            "overall_rating": li.get("overall_rating_99a"),
            "total_reviews": li.get("total_reviews_99a"),
            "star_counts": li.get("star_counts") or {},
            "positive_pct": li.get("positive_pct"),
            "likes": li.get("likes") or [],
            "dislikes": li.get("dislikes") or [],
            "features_ratings": li.get("features_ratings") or [],
            "whats_great": li.get("whats_great") or [],
            "whats_needs_attention": li.get("whats_needs_attention") or [],
            "price_trends": li.get("price_trends_99a") or [],
            "sidebar_prices": li.get("sidebar_prices") or {},
        }

    google_data = {}
    if li.get("has_google"):
        google_data = {
            "avg_rating": li.get("avg_google_rating"),
            "total_reviews": li.get("total_google_reviews"),
            "projects_reviewed": li.get("reviewed_projects_count"),
            "top_projects": [
                {"name": t.get("title", ""), "rating": t.get("rating"),
                 "reviews": t.get("review_count")}
                for t in (li.get("top_reviewed_projects") or [])[:8]
            ],
        }

    # Top projects sorted by PSF for the table
    top_projects = sorted(
        [p for p in projects if p.get("project_name")],
        key=lambda x: -(x.get("price_per_sqft") or 0)
    )[:15]

    return {
        "locality": locality,
        "city": city,
        "supply": supply_summary,
        "top_projects": top_projects,
        "locality_intel": locality_intel,
        "google_reviews": google_data,
        "has_99a": li.get("has_99a", False),
        "has_google": li.get("has_google", False),
    }


# ── Gemini Analysis ────────────────────────────────────────────────────────
PROMPT_TEMPLATE = """You are a real estate market analyst. Given the following data about {locality}, {city}, generate a concise one-page intelligence report.

DATA:
```json
{data_json}
```

Return a JSON object with these exact keys (no markdown, just raw JSON):
{{
  "executive_summary": "2-3 sentence overview of the locality's real estate market",
  "market_position": "1-2 sentences on where this locality stands relative to the city",
  "price_analysis": "2-3 sentences analyzing pricing trends, avg rates, and value proposition",
  "supply_insight": "1-2 sentences on project supply, developer activity, construction status mix",
  "sentiment_summary": "1-2 sentences interpreting resident sentiment from likes/dislikes/ratings",
  "investment_outlook": "2-3 sentences on investment potential — strengths, risks, who should buy here",
  "key_highlights": ["highlight 1", "highlight 2", "highlight 3", "highlight 4", "highlight 5"],
  "risk_factors": ["risk 1", "risk 2", "risk 3"]
}}

Rules:
- Be specific with numbers from the data. Use actual figures, not vague language.
- If some data sections are empty, skip analysis for those parts gracefully.
- Keep each section concise — this is a one-page report.
- Use ₹ for currency. Format large numbers as Cr/L (1 Cr = 10M, 1 L = 100K).
- Write in professional but accessible language. No jargon.
- Return ONLY the JSON object. No explanation, no markdown fences.
"""

def get_gemini_analysis(locality: str, city: str, data: dict) -> dict:
    data_for_prompt = {k: v for k, v in data.items() if v}
    prompt = PROMPT_TEMPLATE.format(
        locality=locality, city=city,
        data_json=json.dumps(data_for_prompt, default=str, indent=2)
    )
    resp = model.generate_content(prompt)
    text = resp.text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


# ── HTML Report Renderer ───────────────────────────────────────────────────
def render_report_html(locality: str, city: str, data: dict, analysis: dict) -> str:
    now = datetime.now().strftime("%d %b %Y")
    supply = data.get("supply") or {}
    intel = data.get("locality_intel") or {}
    google = data.get("google_reviews") or {}
    top_projects = data.get("top_projects") or []

    # Format helpers
    def fmt_price(v):
        if not v: return "—"
        try:
            x = float(str(v).replace(",", "").replace("₹", ""))
        except: return str(v)
        if x >= 1e7: return f"₹{x/1e7:.1f} Cr"
        if x >= 1e5: return f"₹{x/1e5:.1f} L"
        return f"₹{x:,.0f}"

    def fmt_num(v):
        if v is None: return "—"
        try: return f"{int(v):,}"
        except: return str(v)

    # KPI cards
    avg_rate = intel.get("average_rate") or "—"
    overall_rating = intel.get("overall_rating") or "—"
    total_reviews = intel.get("total_reviews") or "—"
    google_rating = google.get("avg_rating") or "—"

    # Price trend SVG
    price_trends = intel.get("price_trends") or []
    trend_svg = ""
    if price_trends and isinstance(price_trends, list) and len(price_trends) >= 2:
        pts_data = []
        for pt in price_trends:
            if isinstance(pt, dict) and pt.get("price_per_sqft"):
                try:
                    pts_data.append({
                        "q": pt.get("quarter", ""),
                        "p": float(str(pt["price_per_sqft"]).replace(",", ""))
                    })
                except: pass

        if len(pts_data) >= 2:
            min_p = min(d["p"] for d in pts_data)
            max_p = max(d["p"] for d in pts_data)
            rng = max_p - min_p or 1
            w, h = 440, 140
            pad_l, pad_r, pad_t, pad_b = 55, 15, 15, 30
            cw = w - pad_l - pad_r
            ch = h - pad_t - pad_b

            points = []
            labels = []
            for i, d in enumerate(pts_data):
                x = pad_l + (i / (len(pts_data) - 1)) * cw
                y = pad_t + ch - ((d["p"] - min_p) / rng) * ch
                points.append(f"{x:.1f},{y:.1f}")
                if i % max(1, len(pts_data) // 5) == 0 or i == len(pts_data) - 1:
                    labels.append(f'<text x="{x:.0f}" y="{h - 5}" font-size="8" fill="#64748B" text-anchor="middle">{d["q"]}</text>')

            # Y-axis labels
            y_labels = ""
            for frac in [0, 0.5, 1]:
                val = min_p + frac * rng
                y = pad_t + ch - frac * ch
                y_labels += f'<text x="{pad_l - 5}" y="{y + 3:.0f}" font-size="8" fill="#64748B" text-anchor="end">₹{val:,.0f}</text>'
                y_labels += f'<line x1="{pad_l}" y1="{y:.0f}" x2="{w - pad_r}" y2="{y:.0f}" stroke="#E2E8F0" stroke-width="0.5"/>'

            polyline = " ".join(points)
            # Area fill
            area_points = f"{pad_l},{pad_t + ch} " + polyline + f" {w - pad_r},{pad_t + ch}"

            trend_svg = f'''
            <svg viewBox="0 0 {w} {h}" style="width:100%;max-width:460px">
              {y_labels}
              <polygon points="{area_points}" fill="url(#trendGrad)" opacity="0.3"/>
              <polyline points="{polyline}" fill="none" stroke="#2563EB" stroke-width="2"/>
              {"".join(labels)}
              <defs><linearGradient id="trendGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#2563EB"/><stop offset="100%" stop-color="#fff"/>
              </linearGradient></defs>
            </svg>'''

    # Status distribution chart (horizontal bars)
    status_dist = supply.get("status_distribution") or {}
    total_proj = supply.get("total_projects") or 1
    status_colors = {
        "New Launch": "#2563EB", "Ready to Move": "#059669",
        "Under Construction": "#D97706", "Unknown": "#94A3B8"
    }
    status_bars = ""
    for st, cnt in sorted(status_dist.items(), key=lambda x: -x[1]):
        pct = (cnt / total_proj) * 100
        color = status_colors.get(st, "#64748B")
        status_bars += f'''
        <div style="margin-bottom:6px">
          <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px">
            <span>{st}</span><span style="color:#64748B">{cnt} ({pct:.0f}%)</span>
          </div>
          <div style="background:#F1F5F9;border-radius:3px;height:8px;overflow:hidden">
            <div style="width:{pct:.0f}%;background:{color};height:100%;border-radius:3px"></div>
          </div>
        </div>'''

    # Sentiment: likes/dislikes
    likes = intel.get("likes") or []
    dislikes = intel.get("dislikes") or []
    likes_html = ""
    for l in likes[:6]:
        tag = l.get("tag", str(l)) if isinstance(l, dict) else str(l)
        count = l.get("count", "") if isinstance(l, dict) else ""
        likes_html += f'<span class="tag tag-pos">{tag}{f" ({count})" if count else ""}</span>'
    dislikes_html = ""
    for d in dislikes[:4]:
        tag = d.get("tag", str(d)) if isinstance(d, dict) else str(d)
        count = d.get("count", "") if isinstance(d, dict) else ""
        dislikes_html += f'<span class="tag tag-neg">{tag}{f" ({count})" if count else ""}</span>'

    # Feature ratings bars
    features = intel.get("features_ratings") or []
    feat_bars = ""
    for f in features[:6]:
        if isinstance(f, dict):
            name = f.get("feature", "")
            rating = f.get("rating", 0)
            try: rating = float(rating)
            except: rating = 0
            pct = (rating / 5) * 100
            feat_bars += f'''
            <div style="margin-bottom:5px">
              <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:1px">
                <span>{name}</span><span style="color:#64748B">{rating}/5</span>
              </div>
              <div style="background:#F1F5F9;border-radius:3px;height:6px;overflow:hidden">
                <div style="width:{pct:.0f}%;background:#2563EB;height:100%;border-radius:3px"></div>
              </div>
            </div>'''

    # Top projects table
    proj_rows = ""
    for p in top_projects[:10]:
        name = p.get("project_name", "")
        dev = p.get("developer", "") or "—"
        st = p.get("status", "")
        psf = fmt_num(p.get("price_per_sqft"))
        price_range = f"{fmt_price(p.get('min_price'))} – {fmt_price(p.get('max_price'))}"
        bhk = ", ".join(p.get("configurations") or []) or "—"
        sc = status_colors.get(st, "#64748B")
        proj_rows += f'''<tr>
          <td style="font-weight:600;border-left:3px solid {sc};padding-left:8px">{name}</td>
          <td>{dev}</td>
          <td><span class="status-badge" style="background:{sc}15;color:{sc}">{st}</span></td>
          <td>{bhk}</td>
          <td>{price_range}</td>
          <td>{psf}</td>
        </tr>'''

    # Key highlights
    highlights = analysis.get("key_highlights") or []
    hl_html = "".join(f'<li>{h}</li>' for h in highlights)

    # Risk factors
    risks = analysis.get("risk_factors") or []
    risk_html = "".join(f'<li>{r}</li>' for r in risks)

    # Top developers
    top_devs = supply.get("top_developers") or {}
    dev_html = ""
    for dev, cnt in list(top_devs.items())[:6]:
        dev_html += f'<span class="tag tag-dev">{dev} ({cnt})</span>'

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Market Intelligence Report — {locality}, {city}</title>
<style>
@media print {{ @page {{ margin:12mm; size:A4 }} .no-print {{ display:none !important }} }}
* {{ box-sizing:border-box; margin:0; padding:0 }}
body {{ font-family:'Segoe UI',system-ui,-apple-system,sans-serif; font-size:12px; color:#0F172A; background:#fff; line-height:1.55 }}
.page {{ max-width:980px; margin:0 auto; padding:20px 24px }}
.header {{ display:flex; justify-content:space-between; align-items:flex-end; border-bottom:3px solid #1D4ED8; padding-bottom:10px; margin-bottom:16px }}
.header h1 {{ font-size:20px; color:#1D4ED8; letter-spacing:-0.3px }}
.header .sub {{ color:#64748B; font-size:11px }}
.section-title {{ font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.6px; color:#1D4ED8; margin:16px 0 8px; padding-bottom:4px; border-bottom:1px solid #E2E8F0 }}
.kpis {{ display:grid; grid-template-columns:repeat(5,1fr); gap:8px; margin-bottom:14px }}
.kpi {{ background:#F8FAFC; border:1px solid #E2E8F0; border-radius:6px; padding:10px; text-align:center }}
.kpi .v {{ font-size:20px; font-weight:800; color:#1D4ED8 }}
.kpi .l {{ font-size:9px; color:#64748B; text-transform:uppercase; letter-spacing:.4px; margin-top:2px }}
.grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:14px }}
.grid3 {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:14px }}
.card {{ background:#FAFBFC; border:1px solid #E2E8F0; border-radius:6px; padding:12px }}
.card-title {{ font-size:11px; font-weight:700; text-transform:uppercase; color:#334155; margin-bottom:8px; letter-spacing:.3px }}
.text-section {{ font-size:11.5px; color:#334155; line-height:1.6 }}
.text-section p {{ margin-bottom:6px }}
table {{ width:100%; border-collapse:collapse; font-size:11px }}
th {{ background:#F1F5F9; text-align:left; padding:6px 7px; font-size:9px; text-transform:uppercase; color:#64748B; letter-spacing:.3px }}
td {{ padding:6px 7px; border-bottom:1px solid #F1F5F9 }}
.status-badge {{ padding:2px 6px; border-radius:3px; font-size:9px; font-weight:600 }}
.tag {{ display:inline-block; padding:3px 8px; border-radius:4px; font-size:10px; margin:2px 3px 2px 0 }}
.tag-pos {{ background:#ECFDF5; color:#059669 }}
.tag-neg {{ background:#FEF2F2; color:#DC2626 }}
.tag-dev {{ background:#EFF6FF; color:#2563EB }}
.highlights {{ padding-left:16px }}
.highlights li {{ margin-bottom:3px; font-size:11px; color:#334155 }}
.risks li {{ color:#DC2626 }}
.footer {{ margin-top:16px; padding-top:10px; border-top:1px solid #E2E8F0; color:#94A3B8; font-size:9px; text-align:center }}
button {{ background:#1D4ED8; color:#fff; border:none; padding:8px 16px; border-radius:6px; cursor:pointer; font-weight:600; font-size:12px }}
</style></head>
<body><div class="page">

<div class="no-print" style="margin-bottom:12px">
  <button onclick="window.print()">🖨️ Print / Save as PDF</button>
</div>

<div class="header">
  <div>
    <h1>{locality}, {city}</h1>
    <div style="font-size:13px;color:#334155;margin-top:2px">Market Intelligence Report</div>
  </div>
  <div class="sub">Generated {now} · RE·ANALYZE</div>
</div>

<!-- Executive Summary -->
<div class="text-section" style="background:#F0F9FF;border-left:3px solid #2563EB;padding:10px 14px;border-radius:0 6px 6px 0;margin-bottom:14px">
  <p style="font-weight:600;color:#1D4ED8;margin-bottom:4px">Executive Summary</p>
  <p>{analysis.get("executive_summary", "")}</p>
</div>

<!-- KPIs -->
<div class="kpis">
  <div class="kpi"><div class="v">{fmt_num(supply.get("total_projects"))}</div><div class="l">Total Projects</div></div>
  <div class="kpi"><div class="v">{avg_rate}</div><div class="l">Avg Rate /sqft</div></div>
  <div class="kpi"><div class="v">{overall_rating}</div><div class="l">Rating (99acres)</div></div>
  <div class="kpi"><div class="v">{google_rating}</div><div class="l">Google Rating</div></div>
  <div class="kpi"><div class="v">{fmt_num(total_reviews)}</div><div class="l">Total Reviews</div></div>
</div>

<div class="grid2">
  <!-- Left column -->
  <div>
    <div class="section-title">Market Analysis</div>
    <div class="text-section">
      <p>{analysis.get("market_position", "")}</p>
      <p>{analysis.get("price_analysis", "")}</p>
      <p>{analysis.get("supply_insight", "")}</p>
    </div>

    <div class="section-title">Project Status Mix</div>
    <div class="card" style="padding:10px">
      {status_bars}
    </div>

    <div class="section-title">Key Highlights</div>
    <ul class="highlights">{hl_html}</ul>

    <div class="section-title" style="color:#DC2626">Risk Factors</div>
    <ul class="highlights risks">{risk_html}</ul>
  </div>

  <!-- Right column -->
  <div>
    <div class="section-title">Price Trend (₹/sqft)</div>
    <div class="card" style="text-align:center;padding:8px">
      {trend_svg if trend_svg else '<p style="color:#94A3B8;font-size:11px;padding:20px">Price trend data not available</p>'}
    </div>

    <div class="section-title">Resident Sentiment</div>
    <div class="text-section" style="margin-bottom:8px">
      <p>{analysis.get("sentiment_summary", "")}</p>
    </div>
    <div style="margin-bottom:6px">
      <span style="font-size:10px;font-weight:700;color:#059669">👍 LIKES</span><br>
      {likes_html if likes_html else '<span style="color:#94A3B8;font-size:10px">No data</span>'}
    </div>
    <div style="margin-bottom:8px">
      <span style="font-size:10px;font-weight:700;color:#DC2626">👎 DISLIKES</span><br>
      {dislikes_html if dislikes_html else '<span style="color:#94A3B8;font-size:10px">No data</span>'}
    </div>

    {"<div class='section-title'>Feature Ratings</div><div class='card' style='padding:10px'>" + feat_bars + "</div>" if feat_bars else ""}

    <div class="section-title">Top Developers</div>
    <div>{dev_html if dev_html else '<span style="color:#94A3B8;font-size:10px">No data</span>'}</div>
  </div>
</div>

<!-- Investment Outlook -->
<div class="section-title">Investment Outlook</div>
<div class="text-section" style="background:#FEFCE8;border-left:3px solid #CA8A04;padding:10px 14px;border-radius:0 6px 6px 0;margin-bottom:12px">
  <p>{analysis.get("investment_outlook", "")}</p>
</div>

<!-- Top Projects Table -->
<div class="section-title">Notable Projects</div>
<table>
  <thead><tr><th>Project</th><th>Developer</th><th>Status</th><th>Config</th><th>Price Range</th><th>PSF</th></tr></thead>
  <tbody>{proj_rows if proj_rows else '<tr><td colspan="6" style="text-align:center;color:#94A3B8">No project data available</td></tr>'}</tbody>
</table>

<div class="footer">
  Market Intelligence Report · {locality}, {city} · {now} · RE·ANALYZE Platform<br>
  Data sources: 99acres, Google Reviews, RERA, Developer submissions · This report is auto-generated for informational purposes only.
</div>

</div></body></html>"""


# ── Main ────────────────────────────────────────────────────────────────────
def generate_report(locality: str, city: str = "Hyderabad") -> dict:
    print(f"  📊 Gathering data for {locality}...")
    data = gather_locality_data(locality, city)

    print(f"  🤖 Generating analysis with Gemini...")
    analysis = get_gemini_analysis(locality, city, data)

    print(f"  🎨 Rendering HTML report...")
    html = render_report_html(locality, city, data, analysis)

    doc = {
        "locality": locality,
        "city": city,
        "html": html,
        "analysis": analysis,
        "generated_at": datetime.utcnow(),
        "model": MODEL,
        "has_99a": data.get("has_99a", False),
        "has_google": data.get("has_google", False),
        "total_projects": (data.get("supply") or {}).get("total_projects", 0),
    }

    db["locality_reports"].update_one(
        {"locality": locality, "city": city},
        {"$set": doc},
        upsert=True,
    )
    print(f"  ✅ Saved to Atlas: locality_reports/{locality}")
    return doc


def run_all(city: str = "Hyderabad"):
    locs = db["projects_master"].distinct("location.locality")
    locs = sorted(set(l.strip() for l in locs if l and l.strip()))
    print(f"\n🏙️  Generating reports for {len(locs)} localities in {city}...\n")

    success, fail = 0, 0
    for i, loc in enumerate(locs, 1):
        print(f"[{i}/{len(locs)}] {loc}")
        try:
            generate_report(loc, city)
            success += 1
            time.sleep(1)  # rate limit courtesy
        except Exception as e:
            print(f"  ❌ FAILED: {e}")
            traceback.print_exc()
            fail += 1
            time.sleep(2)

    print(f"\n🏁 Done! {success} succeeded, {fail} failed.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        loc_name = " ".join(sys.argv[1:])
        generate_report(loc_name)
    else:
        run_all()
