"""
app_v2.py  — Market Intelligence API server (Draft 1)
Stripped version: no buyer persona, no regulatory, no ASBL-specific logic.
Runs on port 5002 by default.
"""
import os, json, uuid, threading, time, secrets
from pathlib  import Path
from datetime import datetime, timedelta
from mongo_supply import (
    fetch_supply, list_localities_from_mongo, get_localities_by_city,
    get_height_restrictions,
    get_infra_summary, get_nearby_pois,
    get_pricing_intel, fetch_supply_by_radius,
    get_project_intelligence, canonicalize_locality,
    get_locality_centroid,
    _re,
)
from flask import (
    Flask, jsonify, request, send_file, send_from_directory,
    Response, session, redirect, url_for, render_template,
)
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__, template_folder=".")
app.secret_key = os.environ.get("MI_SECRET_KEY", secrets.token_hex(32))
CORS(app, supports_credentials=True)

# ── Auth helpers ─────────────────────────────────────────────────────────────
def _users_col():
    return _re()["user_authentication"]

def _current_user():
    return session.get("user")

def _login_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _current_user():
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required", "redirect": "/login"}), 401
            return redirect(url_for("login_page"))
        return fn(*args, **kwargs)
    return wrapper

DATA_DIR  = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — Pages
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def homepage():
    if _current_user():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login_page"))

@app.route("/login")
def login_page():
    if _current_user():
        return redirect(url_for("dashboard"))
    return render_template("login.html")

@app.route("/signup")
def signup_page():
    if _current_user():
        return redirect(url_for("dashboard"))
    return render_template("signup.html")

@app.route("/forgot-password")
def forgot_password_page():
    return render_template("forgot_password.html")

@app.route("/dashboard")
@_login_required
def dashboard():
    return send_from_directory(str(Path(__file__).parent), "dashboard_v2.html")


# ── Auth API ─────────────────────────────────────────────────────────────────

@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    body     = request.get_json(force=True, silent=True) or {}
    email    = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400
    try:
        col  = _users_col()
        user = col.find_one({"email": email})
    except Exception as e:
        return jsonify({"error": "Database error — please try again"}), 500
    if not user:
        return jsonify({"error": "Invalid email or password"}), 401
    pw_hash = user.get("password_hash") or ""
    try:
        pw_ok = check_password_hash(pw_hash, password)
    except Exception:
        pw_ok = False
    if not pw_ok:
        return jsonify({"error": "Invalid email or password"}), 401
    session.permanent = True
    session["user"] = {"email": email, "name": user.get("name") or email.split("@")[0]}
    try:
        col.update_one({"email": email}, {"$set": {"last_login": datetime.utcnow()}})
    except Exception:
        pass
    return jsonify({"ok": True, "name": session["user"]["name"]})

@app.route("/api/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/auth/register", methods=["POST"])
def auth_register():
    body     = request.get_json(force=True, silent=True) or {}
    email    = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""
    name     = (body.get("name") or "").strip() or email.split("@")[0]
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    if "@" not in email or "." not in email.split("@")[-1]:
        return jsonify({"error": "Please enter a valid email address"}), 400
    try:
        col = _users_col()
        if col.find_one({"email": email}):
            return jsonify({"error": "An account with this email already exists"}), 409
        col.insert_one({
            "email": email, "name": name,
            "password_hash": generate_password_hash(password),
            "created_at": datetime.utcnow(), "last_login": None,
            "reset_token": None, "reset_expires": None,
        })
    except Exception as e:
        return jsonify({"error": "Could not create account — database error"}), 500
    return jsonify({"ok": True, "message": f"Account created for {email}"}), 201

@app.route("/api/auth/forgot-password", methods=["POST"])
def auth_forgot_password():
    body  = request.get_json(force=True, silent=True) or {}
    email = (body.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "Email is required"}), 400
    try:
        col  = _users_col()
        user = col.find_one({"email": email})
    except Exception:
        return jsonify({"error": "Database error"}), 500
    if not user:
        return jsonify({"ok": True, "message": "If that email exists, a reset link has been generated."})
    token   = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(hours=2)
    try:
        col.update_one({"email": email}, {"$set": {"reset_token": token, "reset_expires": expires}})
    except Exception:
        return jsonify({"error": "Could not generate reset token"}), 500
    reset_url = f"{request.host_url.rstrip('/')}/forgot-password?token={token}"
    return jsonify({"ok": True, "message": "Reset link generated.", "reset_url": reset_url})

@app.route("/api/auth/reset-password", methods=["POST"])
def auth_reset_password():
    body     = request.get_json(force=True, silent=True) or {}
    token    = (body.get("token") or "").strip()
    password = body.get("password") or ""
    if not token or not password:
        return jsonify({"error": "Token and new password are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    try:
        col  = _users_col()
        user = col.find_one({"reset_token": token})
    except Exception:
        return jsonify({"error": "Database error"}), 500
    if not user:
        return jsonify({"error": "Invalid or expired reset link"}), 400
    expires = user.get("reset_expires")
    if expires and datetime.utcnow() > expires:
        return jsonify({"error": "This reset link has expired"}), 400
    try:
        col.update_one({"reset_token": token}, {"$set": {
            "password_hash": generate_password_hash(password),
            "reset_token": None, "reset_expires": None,
        }})
    except Exception:
        return jsonify({"error": "Could not update password"}), 500
    return jsonify({"ok": True, "message": "Password updated successfully"})

@app.route("/api/auth/me")
def auth_me():
    user = _current_user()
    if not user:
        return jsonify({"authenticated": False}), 401
    return jsonify({"authenticated": True, **user})

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})


# ─────────────────────────────────────────────────────────────────────────────
# DATA APIs
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/city-overview", methods=["POST"])
@_login_required
def city_overview():
    """Lightweight city-wide summary using aggregation pipeline — fast even on Atlas free tier."""
    import re as re_mod
    body = request.get_json() or {}
    city = body.get("city", "Hyderabad").strip()
    try:
        col = _re()["projects_master"]
        city_re = re_mod.compile(re_mod.escape(city), re_mod.IGNORECASE)
        # Also match district — e.g. "Rangareddy" in city dropdown matches "Ranga Reddy" in district
        dist_pattern = re_mod.sub(r'(?i)(reddy|giri|palli|abad)', lambda m: r'\s*' + m.group(), city)
        dist_re = re_mod.compile(dist_pattern.replace(' ', r'\s*'), re_mod.IGNORECASE)
        if city.lower() == "hyderabad":
            match_q = {"$or": [{"location.city": city_re}, {"location.city": {"$in": [None, ""]}}]}
        else:
            match_q = {"$or": [{"location.city": city_re}, {"location.district": dist_re}]}

        pipeline = [
            {"$match": match_q},
            {"$group": {
                "_id": None,
                "total_projects": {"$sum": 1},
                "total_units": {"$sum": {"$ifNull": [{"$toInt": {"$ifNull": ["$building.total_units", 0]}}, 0]}},
                "avg_psf": {"$avg": {"$cond": [
                    {"$gt": [{"$toDouble": {"$ifNull": ["$pricing.price_per_sqft", 0]}}, 0]},
                    {"$toDouble": "$pricing.price_per_sqft"}, None
                ]}},
                "gated": {"$sum": {"$cond": [{"$eq": ["$identity.project_segment", "Gated Community"]}, 1, 0]}},
                "rera_count": {"$sum": {"$cond": [{"$ne": [{"$ifNull": ["$rera.rera_number", ""]}, ""]}, 1, 0]}},
                "under_construction": {"$sum": {"$cond": [{"$eq": ["$identity.construction_status", "Under Construction"]}, 1, 0]}},
                "ready_to_move": {"$sum": {"$cond": [{"$eq": ["$identity.construction_status", "Ready to Move"]}, 1, 0]}},
                "new_launch": {"$sum": {"$cond": [{"$eq": ["$identity.construction_status", "New Launch"]}, 1, 0]}},
                "developers": {"$addToSet": "$identity.builder_name"},
            }}
        ]
        result = list(col.aggregate(pipeline, maxTimeMS=15000))
        if not result:
            return jsonify({"total_projects": 0, "city": city})

        r = result[0]
        devs = [d for d in (r.get("developers") or []) if d]
        segments = {}
        for seg_doc in col.aggregate([
            {"$match": match_q},
            {"$group": {"_id": "$identity.project_segment", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ], maxTimeMS=10000):
            seg_name = seg_doc["_id"] or "Unknown"
            segments[seg_name] = seg_doc["count"]

        status_dist = {}
        for st_doc in col.aggregate([
            {"$match": match_q},
            {"$group": {"_id": "$identity.construction_status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ], maxTimeMS=10000):
            st_name = st_doc["_id"] or "Unknown"
            status_dist[st_name] = st_doc["count"]

        summary = {
            "city": city,
            "total_projects": r.get("total_projects", 0),
            "total_units": r.get("total_units", 0),
            "avg_psf": round(r["avg_psf"]) if r.get("avg_psf") else 0,
            "gated_communities": r.get("gated", 0),
            "rera_count": r.get("rera_count", 0),
            "active_developers": len(devs),
            "segment_distribution": segments,
            "status_distribution": status_dist,
        }
        return jsonify(summary)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/supply", methods=["POST"])
@_login_required
def supply_from_mongo():
    body       = request.get_json() or {}
    locality   = body.get("locality","").strip()
    city       = body.get("city","").strip()
    with_infra = body.get("with_infra", True)

    if not city:
        return jsonify({"error": "city required"}), 400

    if locality:
        canonical = canonicalize_locality(locality)
        if canonical:
            locality = canonical

    page      = int(body.get("page", 1))
    page_size = int(body.get("page_size", 200))
    try:
        supply = fetch_supply(locality, city, page=page, page_size=page_size)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    infra_data, pois_data = {}, {}
    if with_infra and locality:
        projects = supply["supply_projects"]
        lats = [p["latitude"]  for p in projects if p.get("latitude")]
        lngs = [p["longitude"] for p in projects if p.get("longitude")]
        clat = clng = None
        if lats and lngs:
            clat = sum(lats) / len(lats)
            clng = sum(lngs) / len(lngs)
            if not (17.2 <= clat <= 17.7 and 78.2 <= clng <= 78.7):
                clat, clng = get_locality_centroid(locality)
        else:
            clat, clng = get_locality_centroid(locality)
        if clat and clng:
            try:
                infra_data = get_infra_summary(clat, clng, radius_km=3.0, city=city)
                pois_data  = get_nearby_pois(clat, clng, radius_km=3.0, city=city)
                supply["meta"]["centroid_lat"] = round(clat, 6)
                supply["meta"]["centroid_lng"] = round(clng, 6)
            except Exception as e:
                print(f"  [infra load] {e}")

    results = {
        "locality":        locality,
        "city":            city,
        "generated_at":    datetime.now().isoformat(),
        "supply_summary":  supply["supply_summary"],
        "supply_projects": supply["supply_projects"],
        "infra":           infra_data,
        "pois":            pois_data,
        "meta":            supply["meta"],
    }
    return app.response_class(json.dumps(results, default=str), mimetype="application/json")


@app.route("/api/supply-radius", methods=["POST"])
@_login_required
def supply_by_radius():
    body      = request.get_json() or {}
    lat       = body.get("lat")
    lng       = body.get("lng")
    radius_km = float(body.get("radius_km", 3.0))
    city      = (body.get("city") or "Hyderabad").strip()
    if not lat or not lng:
        return jsonify({"error": "lat and lng required"}), 400
    try:
        result = fetch_supply_by_radius(float(lat), float(lng), radius_km)
        result["generated_at"] = datetime.now().isoformat()
        infra_data = get_infra_summary(float(lat), float(lng), radius_km, city=city)
        pois_data  = get_nearby_pois(float(lat), float(lng), min(radius_km, 3.0), city=city)
        result["infra"] = infra_data
        result["pois"]  = pois_data
        return app.response_class(json.dumps(result, default=str), mimetype="application/json")
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pricing-intel", methods=["POST"])
@_login_required
def api_pricing_intel():
    body     = request.get_json() or {}
    locality = (body.get("locality") or "").strip()
    city     = (body.get("city") or "Hyderabad").strip()
    if not locality:
        return jsonify({"error": "locality required"}), 400
    try:
        return jsonify(get_pricing_intel(locality, city))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/project-intel", methods=["GET", "POST"])
@_login_required
def api_project_intel():
    if request.method == "POST":
        body = request.get_json() or {}
        locality = (body.get("locality") or "").strip()
        city     = (body.get("city") or "Hyderabad").strip()
    else:
        locality = (request.args.get("locality") or "").strip()
        city     = (request.args.get("city") or "Hyderabad").strip()
    if not locality:
        return jsonify({"error": "locality required"}), 400
    try:
        return jsonify(get_project_intelligence(locality, city))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/infra", methods=["POST"])
@_login_required
def api_infra():
    body      = request.json or {}
    lat       = float(body.get("lat") or 0)
    lng       = float(body.get("lng") or 0)
    radius_km = float(body.get("radius_km") or 3.0)
    city      = (body.get("city") or "Hyderabad").strip()
    if not lat or not lng:
        return jsonify({"error": "lat and lng required"}), 400
    try:
        return jsonify({
            "summary": get_infra_summary(lat, lng, radius_km, city=city),
            "pois":    get_nearby_pois(lat, lng, min(radius_km, 3.0), city=city),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/height-restrictions", methods=["GET"])
@_login_required
def api_height_restrictions():
    try:
        lat = float(request.args.get("lat") or 17.385)
        lng = float(request.args.get("lng") or 78.4867)
        return jsonify({"zones": get_height_restrictions(lat, lng)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/news", methods=["GET"])
@_login_required
def api_news():
    import urllib.request, xml.etree.ElementTree as ET, html as html_module, re as _re_mod
    locality = (request.args.get("locality") or "").strip()
    city     = (request.args.get("city") or "Hyderabad").strip()
    query = urllib.request.quote(f"{locality} real estate Hyderabad" if locality else "Hyderabad real estate market")
    rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
    items = []
    try:
        req = urllib.request.Request(rss_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as r:
            xml_data = r.read()
        root = ET.fromstring(xml_data)
        for item in root.findall(".//item")[:15]:
            title = html_module.unescape(item.findtext("title") or "")
            desc  = _re_mod.sub(r"<[^>]+>", "", html_module.unescape(item.findtext("description") or ""))[:250]
            link  = item.findtext("link") or ""
            pub   = item.findtext("pubDate") or ""
            items.append({"title": title, "desc": desc, "link": link, "pub": pub})
    except Exception:
        pass
    if not items:
        q_loc = urllib.request.quote(f"{locality} real estate" if locality else "Hyderabad real estate")
        items = [
            {"title": f"{locality or 'Hyderabad'} Real Estate — Latest News",
             "desc": f"Search Google News for latest updates in {locality or 'Hyderabad'}.",
             "link": f"https://news.google.com/search?q={q_loc}&hl=en-IN&gl=IN&ceid=IN:en", "pub": ""},
        ]
    return jsonify({"locality": locality or city, "items": items[:20]})


@app.route("/api/localities-by-city")
@_login_required
def localities_by_city():
    try:
        return jsonify(get_localities_by_city())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/localities")
@_login_required
def list_localities():
    try:
        items = list_localities_from_mongo()
        if items:
            return jsonify(items)
    except Exception as e:
        print(f"  [localities] {e}")
    return jsonify([])


@app.route("/api/report-html", methods=["POST"])
@_login_required
def api_report_html():
    body     = request.json or {}
    locality = (body.get("locality") or "").strip()
    city     = (body.get("city")     or "Hyderabad").strip()
    if not locality:
        return jsonify({"error": "locality required"}), 400
    try:
        data = fetch_supply(locality, city, fetch_infra=True)
        html = _render_report_html(data)
        return Response(html, mimetype="text/html")
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _render_report_html(data: dict) -> str:
    """Minimal print-ready HTML report."""
    locality = data.get("locality", "")
    city     = data.get("city", "")
    ss       = data.get("supply_summary") or {}
    projects = data.get("supply_projects") or []
    now      = datetime.now().strftime("%d %b %Y, %I:%M %p")

    def fmt(v):
        if not v: return "—"
        x = float(str(v).replace(",", ""))
        if x >= 1e7: return f"₹{x/1e7:.1f} Cr"
        if x >= 1e5: return f"₹{x/1e5:.1f} L"
        return f"₹{x:,.0f}"

    proj_rows = ""
    for p in sorted(projects, key=lambda x: -(x.get("platform_count") or 1))[:25]:
        sc = {"New Launch":"#2563EB","Ready to Move":"#059669","Under Construction":"#D97706"}.get(p.get("status",""),"#64748B")
        bhk = ", ".join(p.get("configurations") or []) or "—"
        proj_rows += (
            f'<tr><td style="border-left:3px solid {sc};padding-left:8px;font-weight:600">{p.get("project_name","")}</td>'
            f'<td>{p.get("developer","") or "—"}</td>'
            f'<td><span style="background:{sc}22;color:{sc};padding:2px 8px;border-radius:4px;font-size:11px">{p.get("status","")}</span></td>'
            f'<td>{bhk}</td>'
            f'<td>{fmt(p.get("min_price"))} – {fmt(p.get("max_price"))}</td>'
            f'<td>{"₹{:,}".format(p.get("price_per_sqft")) if p.get("price_per_sqft") else "—"}/sqft</td>'
            f'<td>{p.get("rera_id") or "—"}</td></tr>'
        )

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Market Intelligence — {locality}, {city}</title>
<style>
@media print {{ @page {{ margin:15mm }} .no-print {{ display:none }} }}
* {{ box-sizing:border-box;margin:0;padding:0 }}
body {{ font-family:system-ui,sans-serif;font-size:13px;color:#0F172A;background:#fff;line-height:1.5 }}
.page {{ max-width:960px;margin:0 auto;padding:24px }}
h2 {{ font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;margin:20px 0 10px;border-bottom:1px solid #E2E8F0;padding-bottom:5px }}
.kpis {{ display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px }}
.kpi {{ background:#F8FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:12px }}
.kpi .v {{ font-size:22px;font-weight:800;color:#1D4ED8 }}
.kpi .l {{ font-size:10px;color:#64748B;text-transform:uppercase }}
table {{ width:100%;border-collapse:collapse;font-size:12px }}
th {{ background:#F1F5F9;text-align:left;padding:7px 8px;font-size:10px;text-transform:uppercase;color:#64748B }}
td {{ padding:7px 8px;border-bottom:1px solid #F1F5F9 }}
.footer {{ margin-top:28px;padding-top:14px;border-top:1px solid #E2E8F0;color:#94A3B8;font-size:10px;text-align:center }}
button {{ background:#1D4ED8;color:#fff;border:none;padding:9px 18px;border-radius:6px;cursor:pointer;font-weight:600 }}
</style></head>
<body><div class="page">
<div class="no-print" style="margin-bottom:14px"><button onclick="window.print()">🖨️ Print / Save as PDF</button></div>
<h1 style="font-size:18px;margin-bottom:4px">{locality}, {city}</h1>
<div style="color:#64748B;font-size:12px;margin-bottom:16px">Market Intelligence Report · {now}</div>
<h2>Key Metrics</h2>
<div class="kpis">
  <div class="kpi"><div class="v">{ss.get("total_projects","—")}</div><div class="l">Total Projects</div></div>
  <div class="kpi"><div class="v">{"₹{:,}".format(ss.get("avg_price_per_sqft",0)) if ss.get("avg_price_per_sqft") else "—"}</div><div class="l">Avg PSF</div></div>
  <div class="kpi"><div class="v">{ss.get("gated_projects","—")}</div><div class="l">Gated Communities</div></div>
  <div class="kpi"><div class="v">{ss.get("total_units","—") or "—"}</div><div class="l">Total Units</div></div>
</div>
<h2>Projects (Top 25)</h2>
<table><thead><tr><th>Project</th><th>Developer</th><th>Status</th><th>BHK</th><th>Price Range</th><th>PSF</th><th>RERA</th></tr></thead>
<tbody>{proj_rows}</tbody></table>
<div class="footer">Market Intelligence Report · {now}</div>
</div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    print(f"""
╔════════════════════════════════════════════╗
║  Market Intelligence — API v2 (Draft 1)   ║
╚════════════════════════════════════════════╝
  Dashboard: http://localhost:{port}/dashboard
  API:       http://localhost:{port}/api/health
    """)
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=port, threaded=True)
