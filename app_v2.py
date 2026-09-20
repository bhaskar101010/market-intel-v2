"""
app.py  — RE·ANALYZE API server v3
Uses the unified Playwright supply pipeline + enhanced processors.
"""
import os, json, uuid, threading, time, secrets
from pathlib  import Path
from datetime import datetime, timedelta
from mongo_supply import (
    fetch_supply, list_localities_from_mongo, get_localities_by_city,
    get_height_restrictions,
    get_infra_summary, get_nearby_pois, get_regulatory_summary,
    get_pricing_intel, get_buyer_persona_data, fetch_supply_by_radius,
    get_approval_stats, get_project_intelligence, canonicalize_locality,
    list_bp_localities, get_locality_intelligence, get_locality_centroid,
    _re,
)
from flask import (
    Flask, jsonify, request, send_file, send_from_directory,
    Response, session, redirect, url_for, render_template,
)
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__, template_folder=".")
app.secret_key = os.environ.get("RE_SECRET_KEY", "re-analyze-secret-key-2024-change-in-prod")
CORS(app, supports_credentials=True)

# ── Auth helpers ─────────────────────────────────────────────────────────────
def _users_col():
    return _re()["user_authentication"]

def _current_user():
    return session.get("user")

def _login_required(fn):
    """Decorator: redirect to /login if not authenticated."""
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
JOBS_DIR  = Path(__file__).parent / "data" / "_jobs"
DATA_DIR.mkdir(exist_ok=True)
JOBS_DIR.mkdir(exist_ok=True)

# In-memory job registry — also persisted to JOBS_DIR so restarts don't lose state
JOBS: dict = {}


def _jobs_path(job_id: str) -> Path:
    return JOBS_DIR / f"job_{job_id}.json"


def _persist_job(job: dict) -> None:
    """Write job dict to disk (status + results). Called on every status change."""
    try:
        with open(_jobs_path(job["id"]), "w", encoding="utf-8") as f:
            json.dump(job, f, ensure_ascii=False, default=str)
    except Exception as e:
        print(f"  [persist] Failed to save job {job['id']}: {e}")


def _restore_jobs() -> None:
    """On startup, reload any jobs from disk that are still in progress or done."""
    for fp in JOBS_DIR.glob("job_*.json"):
        try:
            with open(fp) as f:
                job = json.load(f)
            # Resurrect done/error jobs so status + results endpoints work after restart
            if job.get("status") in ("done", "error"):
                JOBS[job["id"]] = job
        except Exception:
            pass


_restore_jobs()


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def homepage():
    return render_template("home.html")


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
    return send_from_directory(str(Path(__file__).parent), "index.html")


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
        print(f"[auth_login] DB error: {e}")
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
    session["user"] = {
        "email": email,
        "name":  user.get("name") or email.split("@")[0],
    }
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
    """Create a new user account — open for signup."""
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
            "email":         email,
            "name":          name,
            "password_hash": generate_password_hash(password),
            "created_at":    datetime.utcnow(),
            "last_login":    None,
            "reset_token":   None,
            "reset_expires": None,
        })
    except Exception as e:
        print(f"[auth_register] DB error: {e}")
        return jsonify({"error": "Could not create account — database error"}), 500

    return jsonify({"ok": True, "message": f"Account created for {email}"}), 201


@app.route("/api/auth/forgot-password", methods=["POST"])
def auth_forgot_password():
    """Generate a password reset token and return the reset URL."""
    body  = request.get_json(force=True, silent=True) or {}
    email = (body.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "Email is required"}), 400

    try:
        col  = _users_col()
        user = col.find_one({"email": email})
    except Exception as e:
        return jsonify({"error": "Database error"}), 500

    # Always return success to prevent email enumeration
    if not user:
        return jsonify({"ok": True, "message": "If that email exists, a reset link has been generated."})

    token   = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(hours=2)
    try:
        col.update_one(
            {"email": email},
            {"$set": {"reset_token": token, "reset_expires": expires}},
        )
    except Exception:
        return jsonify({"error": "Could not generate reset token"}), 500

    # Build the reset URL — uses request host so it works in any environment
    reset_url = f"{request.host_url.rstrip('/')}  /forgot-password?token={token}"
    reset_url = reset_url.replace("  /", "/")  # clean up any accidental spaces

    return jsonify({
        "ok":        True,
        "message":   "Reset link generated. Copy the link below and open it in your browser.",
        "reset_url": reset_url,
    })


@app.route("/api/auth/reset-password", methods=["POST"])
def auth_reset_password():
    """Verify reset token and update password."""
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
        return jsonify({"error": "This reset link has expired (valid for 2 hours)"}), 400

    try:
        col.update_one(
            {"reset_token": token},
            {"$set": {
                "password_hash": generate_password_hash(password),
                "reset_token":   None,
                "reset_expires": None,
            }},
        )
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



@app.route("/api/analyze", methods=["POST"])
@_login_required
def start_analysis():
    body     = request.get_json() or {}
    locality = body.get("locality", "").strip()
    city     = body.get("city",     "").strip()
    if not locality or not city:
        return jsonify({"error": "locality and city are required"}), 400

    job = {
        "id":           str(uuid.uuid4())[:8],
        "locality":     locality,
        "city":         city,
        "options":      body.get("options", {}),
        "lat":          body.get("lat"),
        "lng":          body.get("lng"),
        "status":       "queued",
        "progress":     0,
        "step":         "Queued",
        "started_at":   datetime.now().isoformat(),
        "finished_at":  None,
        "results":      None,
        "report_path":  None,
        "error":        None,
    }
    JOBS[job["id"]] = job
    threading.Thread(target=_run, args=(job["id"],), daemon=True).start()
    return jsonify({"job_id": job["id"], "message": "Analysis started"})


def _get_job(job_id: str) -> dict | None:
    """Look up a job in memory first, then fall back to disk (survives restarts)."""
    if job_id in JOBS:
        return JOBS[job_id]
    fp = _jobs_path(job_id)
    if fp.exists():
        try:
            with open(fp) as f:
                job = json.load(f)
            JOBS[job_id] = job   # re-cache
            return job
        except Exception:
            pass
    return None


@app.route("/api/supply", methods=["POST"])
@_login_required
def supply_from_mongo():
    """
    Query MongoDB directly — returns supply + infra + regulatory in one response.
    No scraping. No job queue.
    """
    body       = request.get_json() or {}
    locality   = body.get("locality","").strip()
    city       = body.get("city","").strip()
    with_infra = body.get("with_infra", True)   # default True now

    if not city:
        return jsonify({"error": "city required"}), 400

    # Canonicalize locality name (handles case variants, KPHB phases, etc.)
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

    # Always try to load infra + regulatory using project centroid
    infra_data, pois_data, regulatory_data = {}, {}, {}
    if with_infra and locality:
        projects = supply["supply_projects"]
        lats = [p["latitude"]  for p in projects if p.get("latitude")]
        lngs = [p["longitude"] for p in projects if p.get("longitude")]
        clat = clng = None
        print(f"[DEBUG] Processing infra for {locality}: {len(lats)} projects with coords")
        if lats and lngs:
            clat = sum(lats) / len(lats)
            clng = sum(lngs) / len(lngs)
            print(f"[DEBUG] Calculated centroid: ({clat:.4f}, {clng:.4f})")
            # Validate coordinates are within reasonable Hyderabad bounds (17.2-17.7, 78.2-78.7)
            # If outside bounds, fall back to buyer_persona centroid
            if not (17.2 <= clat <= 17.7 and 78.2 <= clng <= 78.7):
                print(f"[WARNING] Invalid centroid ({clat:.4f}, {clng:.4f}) for {locality}, using fallback")
                clat, clng = get_locality_centroid(locality)
                print(f"[DEBUG] Fallback centroid: ({clat}, {clng})")
        else:
            print(f"[DEBUG] No coords in projects, using fallback")
            clat, clng = get_locality_centroid(locality)
        if clat and clng:
            try:
                # ponytail: locality mode uses 3km to stay local; radius view stays at 5km
                infra_data    = get_infra_summary(clat, clng, radius_km=3.0, city=city)
                pois_data     = get_nearby_pois(clat, clng, radius_km=3.0, city=city)
                regulatory_data = get_regulatory_summary(locality, city)
                # Attach centroid to meta for frontend map zoom
                supply["meta"]["centroid_lat"] = round(clat, 6)
                supply["meta"]["centroid_lng"] = round(clng, 6)
            except Exception as e:
                print(f"  [infra/regulatory load] {e}")

    results = {
        "locality":        locality,
        "city":            city,
        "generated_at":    datetime.now().isoformat(),
        "supply_summary":  supply["supply_summary"],
        "supply_projects": supply["supply_projects"],
        "demand":          {},
        "infra":           infra_data,
        "pois":            pois_data,
        "regulatory":      regulatory_data,
        "personas":        {},
        "persona_meta":    {},
        "meta":            supply["meta"],
    }
    return app.response_class(
        json.dumps(results, default=str),
        mimetype="application/json"
    )


@app.route("/api/supply-radius", methods=["POST"])
@_login_required
def supply_by_radius():
    """
    Geospatial supply query by lat/lng radius.
    Body: {lat, lng, radius_km}
    """
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
        # Include infra for radius queries too
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
    """
    Returns ASBL vs market PSF benchmarks for a locality.
    Body: {locality, city}
    """
    body     = request.get_json() or {}
    locality = (body.get("locality") or "").strip()
    city     = (body.get("city") or "Hyderabad").strip()
    if not locality:
        return jsonify({"error": "locality required"}), 400
    try:
        return jsonify(get_pricing_intel(locality, city))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/buyer-persona", methods=["GET"])
@_login_required
def api_buyer_persona():
    """
    Returns full buyer persona data sourced entirely from buyer_persona DB.
    Collections: localities (market tier) + micromarkets (claims) + reports (buyer_profile).
    Query params:
      locality  (optional) — locality name
      radius_km (optional) — integer 1–10; when supplied, serves the radius_report
                             for that exact radius instead of the locality_report
    """
    locality  = (request.args.get("locality") or "").strip()
    try:
        radius_km = int(request.args.get("radius_km") or 0)
    except (ValueError, TypeError):
        radius_km = 0
    try:
        return jsonify(get_buyer_persona_data(locality, radius_km=radius_km))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/approval-stats", methods=["GET"])
@_login_required
def api_approval_stats():
    """
    Returns GHMC/HMDA/RERA/Fire NOC approval stats for a locality.
    Source: insightforge.approval_project_matches joined via projects_master.
    Query param: locality (required)
    """
    locality = (request.args.get("locality") or "").strip()
    if not locality:
        return jsonify({"error": "locality required"}), 400
    try:
        return jsonify(get_approval_stats(locality))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/project-intel", methods=["GET", "POST"])
@_login_required
def api_project_intel():
    """
    Returns enhanced project intelligence: developer rankings, market share, lifecycle.
    Source: real_estate.projects_master + insightforge.approval_project_matches
    """
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




@app.route("/api/pincode-boundaries", methods=["GET"])
@_login_required
def api_pincode_boundaries():
    """
    Returns pincode boundary polygons for coverage map.
    Source: insightforge.pincode_boundaries
    Query param: city (optional, default Hyderabad), limit (optional, default 200)
    """
    from mongo_supply import _ig
    city  = (request.args.get("city") or "Hyderabad").strip()
    limit = min(int(request.args.get("limit", 200)), 500)
    try:
        import re as _re_mod
        docs = list(_ig()["pincode_boundaries"].find(
            {"city": _re_mod.compile(city, _re_mod.I)},
            {"pincode":1, "locality":1, "geometry":1, "_id":0},
            limit=limit
        ))
        return app.response_class(
            json.dumps({"boundaries": docs, "count": len(docs)}, default=str),
            mimetype="application/json"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/news", methods=["GET"])
@_login_required
def api_news():
    """
    Returns live real estate news via Google News RSS for a locality.
    Falls back to demand_raw JSON files if RSS fails.
    """
    import urllib.request, xml.etree.ElementTree as ET, html as html_module, re as _re_mod
    locality = (request.args.get("locality") or "").strip()
    city     = (request.args.get("city") or "Hyderabad").strip()

    # Try Google News RSS first
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

    # Fallback 1: demand_raw JSON files
    if not items:
        try:
            for fp in sorted((DATA_DIR.parent).glob("demand_raw_*.json"), reverse=True)[:5]:
                try:
                    with open(fp) as f:
                        raw = json.load(f)
                    if isinstance(raw, list):
                        for post in raw[:3]:
                            if isinstance(post, dict) and post.get("title"):
                                items.append({
                                    "title": post.get("title",""),
                                    "desc":  post.get("snippet","") or post.get("body","")[:200],
                                    "pub":   post.get("date","") or post.get("published",""),
                                    "link":  post.get("url",""),
                                })
                except Exception:
                    pass
        except Exception:
            pass

    # Fallback 2: curated static items with real Google News search links
    if not items:
        q_loc  = urllib.request.quote(f"{locality} real estate" if locality else "Hyderabad real estate")
        q_gen  = urllib.request.quote("Hyderabad real estate market 2026")
        q_rera = urllib.request.quote("Telangana RERA 2026")
        q_infra= urllib.request.quote("Hyderabad metro infrastructure 2026")
        items = [
            {"title": f"{locality or 'Hyderabad'} Real Estate — Latest News",
             "desc":  f"Search Google News for the latest real estate updates in {locality or 'Hyderabad'}.",
             "link":  f"https://news.google.com/search?q={q_loc}&hl=en-IN&gl=IN&ceid=IN:en", "pub": ""},
            {"title": "Hyderabad Real Estate Market — Market Updates",
             "desc":  "Latest market intelligence, price trends and new launches in the Hyderabad residential market.",
             "link":  f"https://news.google.com/search?q={q_gen}&hl=en-IN&gl=IN&ceid=IN:en", "pub": ""},
            {"title": "Telangana RERA — Registrations & Compliance",
             "desc":  "Stay updated on RERA registrations, show-cause notices and compliance deadlines in Telangana.",
             "link":  f"https://news.google.com/search?q={q_rera}&hl=en-IN&gl=IN&ceid=IN:en", "pub": ""},
            {"title": "Hyderabad Metro & Infrastructure — 2026 Updates",
             "desc":  "Track metro expansion, ORR, highway and infrastructure projects affecting Hyderabad property values.",
             "link":  f"https://news.google.com/search?q={q_infra}&hl=en-IN&gl=IN&ceid=IN:en", "pub": ""},
        ]

    return jsonify({"locality": locality or city, "items": items[:20]})

@app.route("/api/localities-by-city")
@_login_required
def localities_by_city():
    """Returns {city: [locality, ...]} built from actual MongoDB data."""
    try:
        return jsonify(get_localities_by_city())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/locality-intel")
@_login_required
def locality_intel():
    locality = request.args.get("locality","").strip()
    city     = request.args.get("city","Hyderabad").strip()
    if not locality:
        return jsonify({"error": "locality required"}), 400
    try:
        return jsonify(get_locality_intelligence(locality, city))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/bp-localities")
@_login_required
def bp_localities():
    """Localities that have direct buyer_persona report data, plus those reachable
    via a radius_report (returned as {direct:[...], radius:[...]})."""
    try:
        from mongo_supply import _bp
        bp = _bp()
        direct = list_bp_localities()
        # Collect all included_localities names from every radius_report
        radius_covered = set()
        for doc in bp["reports"].find(
            {"radius_report.included_localities": {"$exists": True}},
            {"radius_report.included_localities.name": 1}
        ):
            for loc in (doc.get("radius_report") or {}).get("included_localities") or []:
                n = loc.get("name")
                if n:
                    radius_covered.add(n)
        return jsonify({
            "direct": direct,
            "radius": sorted(radius_covered - set(direct)),
        })
    except Exception as e:
        return jsonify({"direct": [], "radius": []}), 500

@app.route("/api/status/<job_id>")
@_login_required
def job_status(job_id):
    job = _get_job(job_id)
    if not job: return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "job_id":      job_id,
        "status":      job["status"],
        "progress":    job["progress"],
        "step":        job["step"],
        "started_at":  job["started_at"],
        "finished_at": job["finished_at"],
    })


@app.route("/api/results/<job_id>")
@_login_required
def job_results(job_id):
    job = _get_job(job_id)
    if not job: return jsonify({"error": "Job not found"}), 404
    if job["status"] != "done":
        return jsonify({"error": "Not finished", "status": job["status"]}), 202
    return app.response_class(
        json.dumps(job["results"], default=str),
        mimetype="application/json"
    )


@app.route("/api/report/<job_id>")
@_login_required
def download_report(job_id):
    job = _get_job(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "Not ready"}), 404
    rp = job.get("report_path")
    if not rp or not Path(rp).exists():
        return jsonify({"error": "Report file not found"}), 404
    return send_file(
        rp, as_attachment=True,
        download_name=Path(rp).name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/api/localities")
@_login_required
def list_localities():
    # Try MongoDB first
    try:
        items = list_localities_from_mongo()
        if items:
            return jsonify(items)
    except Exception as e:
        print(f"  [localities] Mongo fallback to file scan: {e}")
 
    # File-based fallback (original behaviour)
    items = []
    for f in sorted(DATA_DIR.glob("*_analysis_*.json"), reverse=True)[:20]:
        try:
            with open(f) as fp:
                d = json.load(fp)
                items.append({
                    "locality":     d.get("locality"),
                    "city":         d.get("city"),
                    "generated_at": d.get("generated_at"),
                    "file":         f.name,
                })
        except:
            pass
    return jsonify(items)


@app.route("/api/load-cached", methods=["POST"])
@_login_required
def load_cached():
    body     = request.get_json() or {}
    locality = body.get("locality","").strip()
    city     = body.get("city","").strip()
    slug     = f"{city}_{locality}".lower().replace(" ","_")
    files    = sorted(DATA_DIR.glob(f"{slug}_analysis_*.json"), reverse=True)
    if not files:
        return jsonify({"error": f"No cached analysis for {locality}, {city}"}), 404
    with open(files[0]) as f:
        results = json.load(f)
    job = {
        "id": str(uuid.uuid4())[:8], "locality": locality, "city": city,
        "status": "done", "progress": 100, "step": "Loaded from cache",
        "started_at": datetime.now().isoformat(), "finished_at": datetime.now().isoformat(),
        "results": results, "report_path": None, "options": {}, "error": None,
    }
    JOBS[job["id"]] = job
    return app.response_class(
        json.dumps({"job_id": job["id"], "results": results}, default=str),
        mimetype="application/json"
    )


# ─────────────────────────────────────────────────────────────────────────────
# NEW ROUTES — InsightForge integration
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/infra", methods=["POST"])
@_login_required
def api_infra():
    """
    Source: insightforge.points_of_interest, metro_stations, hospitals,
            schools, malls, it_companies, lakes
    Body: {lat, lng, radius_km (default 5.0)}
    """
    body      = request.json or {}
    lat       = float(body.get("lat") or 0)
    lng       = float(body.get("lng") or 0)
    locality  = (body.get("locality") or "").strip()
    # mode: 'locality' uses locality name match; 'radius' uses geospatial radius
    mode      = (body.get("mode") or "radius").strip()
    radius_km = float(body.get("radius_km") or (5.0 if mode == "radius" else 3.0))
    city      = (body.get("city") or "Hyderabad").strip()
    if not lat or not lng:
        return jsonify({"error": "lat and lng required"}), 400
    try:
        return jsonify({
            "summary":  get_infra_summary(lat, lng, radius_km, city=city),
            "pois":     get_nearby_pois(lat, lng, min(radius_km, 3.0), city=city),
            "locality": locality,
            "mode":     mode,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/height-restrictions", methods=["GET"])
@_login_required
def api_height_restrictions():
    """
    Source: insightforge.airport_height_restriction_zones (58 docs, all Hyderabad)
    Returns all zones for frontend map overlay.
    """
    try:
        lat = float(request.args.get("lat") or 17.385)
        lng = float(request.args.get("lng") or 78.4867)
        return jsonify({"zones": get_height_restrictions(lat, lng)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/regulatory", methods=["POST"])
@_login_required
def api_regulatory():
    """
    Source: insightforge.hmda_all_records, insightforge.fire_noc_r4
    Body: {locality, city}
    """
    body     = request.json or {}
    locality = (body.get("locality") or "").strip()
    city     = (body.get("city")     or "Hyderabad").strip()
    try:
        return jsonify(get_regulatory_summary(locality, city))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/report-html", methods=["POST"])
@_login_required
def api_report_html():
    """
    Returns print-ready HTML. Open in browser → Ctrl+P → Save as PDF.
    Body: {locality, city}
    """
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

@app.route("/api/report-issue", methods=["POST"])
@_login_required
def api_report_issue():
    try:
        body = request.get_json(force=True) or {}
        doc = {
            "page":         body.get("page", ""),
            "entity_type":  body.get("entity_type", ""),
            "entity_id":    body.get("entity_id", ""),
            "entity_name":  body.get("entity_name", ""),
            "issue_type":          body.get("issue_type", "other"),
            "description":         (body.get("description") or "").strip(),
            "correct_data_source": (body.get("correct_data_source") or "").strip(),
            "user_info":    {
                "name":  (body.get("user_name") or "").strip(),
                "email": (body.get("user_email") or "").strip(),
                "phone": (body.get("user_phone") or "").strip(),
            },
            "source_url":  body.get("source_url", ""),
            "status":      "open",
            "created_at":  __import__("datetime").datetime.utcnow(),
        }
        if not doc["description"]:
            return jsonify({"error": "description required"}), 400
        result = _re()["report_issue"].insert_one(doc)
        return jsonify({"success": True, "id": str(result.inserted_id)})
    except Exception as e:
        print(f"[ERROR] report-issue: {e}")
        return jsonify({"error": str(e)}), 500

def _render_report_html(data: dict) -> str:
    """Print-ready HTML report for PDF export via browser print dialog."""
    locality = data.get("locality", "")
    city     = data.get("city", "")
    ss       = data.get("supply_summary") or {}
    projects = data.get("supply_projects") or []
    infra    = data.get("infra") or {}
    now      = datetime.now().strftime("%d %b %Y, %I:%M %p")

    def fmt(v):
        if not v: return "—"
        n = float(str(v).replace(",", ""))
        if n >= 1e7: return f"₹{n/1e7:.1f} Cr"
        if n >= 1e5: return f"₹{n/1e5:.1f} L"
        return f"₹{n:,.0f}"

    seg_dist  = ss.get("segment_distribution") or {}
    stat_dist = ss.get("status_distribution")  or {}
    cfg_dist  = ss.get("config_distribution")  or {}

    proj_rows = ""
    for p in sorted(projects, key=lambda x: -(x.get("platform_count") or 1))[:20]:
        sc = {"New Launch":"#2563EB","Ready to Move":"#059669","Under Construction":"#D97706"}.get(p.get("status",""),"#64748B")
        bhk = ", ".join(p.get("configurations") or []) or "—"
        proj_rows += (
            f'<tr><td style="border-left:3px solid {sc};padding-left:8px;font-weight:600">{p.get("project_name","")}</td>'
            f'<td>{p.get("developer","") or "—"}</td>'
            f'<td><span style="background:{sc}22;color:{sc};padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600">{p.get("status","")}</span></td>'
            f'<td>{bhk}</td>'
            f'<td>{fmt(p.get("min_price"))} – {fmt(p.get("max_price"))}</td>'
            f'<td>{"₹{:,}".format(p.get("price_per_sqft")) if p.get("price_per_sqft") else "—"}/sqft</td>'
            f'<td>{p.get("absorption_pct","—")}{"%" if p.get("absorption_pct") is not None else ""}</td>'
            f'<td>{p.get("rera_id") or "—"}</td></tr>'
        )

    infra_rows = ""
    icons = {"metro":"🚇","hospital":"🏥","school":"🏫","mall":"🛍","it_company":"💻"}
    for key, label in [("metro","Metro"),("hospital","Hospital"),("school","School"),("mall","Mall"),("it_company","IT Park")]:
        item = (infra.get(key) or {}).get("nearest")
        if item:
            infra_rows += (
                f'<tr><td>{icons.get(key,"")} {label}</td>'
                f'<td style="font-weight:600">{item["name"]}</td>'
                f'<td>{item["dist"]}</td>'
                f'<td>{(infra.get(key) or {}).get("count_within_radius",0)} within 5km</td></tr>'
            )

    dist_rows = lambda d: "".join(
        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;font-size:12px">'
        f'<span style="min-width:130px">{k}</span>'
        f'<div style="flex:1;height:6px;background:#E2E8F0;border-radius:3px">'
        f'<div style="width:{round(v/max(d.values(),default=1)*100)}%;height:100%;background:#1D4ED8;border-radius:3px"></div></div>'
        f'<span style="font-weight:600">{v}</span></div>'
        for k, v in sorted(d.items(), key=lambda x: -x[1])
    )

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>RE·ANALYZE — {locality}, {city}</title>
<style>
@media print {{ @page {{ margin:15mm }} .no-print {{ display:none }} }}
* {{ box-sizing:border-box;margin:0;padding:0 }}
body {{ font-family:'Inter',system-ui,sans-serif;font-size:13px;color:#0F172A;background:#fff;line-height:1.5 }}
.page {{ max-width:960px;margin:0 auto;padding:24px }}
.hdr {{ display:flex;justify-content:space-between;border-bottom:2px solid #1D4ED8;padding-bottom:16px;margin-bottom:24px }}
.logo {{ font-size:20px;font-weight:800;color:#1D4ED8 }}
h2 {{ font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;margin:20px 0 10px;border-bottom:1px solid #E2E8F0;padding-bottom:5px }}
.kpis {{ display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:20px }}
.kpi {{ background:#F8FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:12px }}
.kpi .v {{ font-size:22px;font-weight:800;color:#1D4ED8 }}
.kpi .l {{ font-size:10px;color:#64748B;text-transform:uppercase;letter-spacing:.5px;margin-top:2px }}
.dists {{ display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:20px }}
.db {{ background:#F8FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:12px }}
.db h3 {{ font-size:10px;color:#64748B;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px }}
table {{ width:100%;border-collapse:collapse;font-size:12px }}
th {{ background:#F1F5F9;text-align:left;padding:7px 8px;font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:#64748B }}
td {{ padding:7px 8px;border-bottom:1px solid #F1F5F9 }}
.footer {{ margin-top:28px;padding-top:14px;border-top:1px solid #E2E8F0;color:#94A3B8;font-size:10px;text-align:center }}
button {{ background:#1D4ED8;color:#fff;border:none;padding:9px 18px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600 }}
</style></head>
<body><div class="page">
<div class="no-print" style="margin-bottom:14px">
  <button onclick="window.print()">🖨️ Print / Save as PDF</button>
  <span style="margin-left:12px;color:#64748B;font-size:12px">Print dialog → Save as PDF</span>
</div>
<div class="hdr">
  <div>
    <div class="logo">RE·ANALYZE</div>
    <div style="font-size:18px;font-weight:700;margin-top:4px">{locality}, {city}</div>
    <div style="color:#64748B;font-size:12px">Locality Intelligence Report · ASBL Internal</div>
  </div>
  <div style="text-align:right;color:#64748B;font-size:11px">
    <div>Generated: {now}</div>
    <div>Source: projects_master + InsightForge</div>
    <div>Projects analysed: {ss.get("total_projects",0)}</div>
  </div>
</div>
<h2>Key Metrics</h2>
<div class="kpis">
  <div class="kpi"><div class="v">{ss.get("total_projects","—")}</div><div class="l">Total Projects</div></div>
  <div class="kpi"><div class="v">{"₹{:,}".format(ss.get("avg_price_per_sqft",0)) if ss.get("avg_price_per_sqft") else "—"}</div><div class="l">Avg PSF</div></div>
  <div class="kpi"><div class="v">{ss.get("gated_projects","—")}</div><div class="l">Gated Communities</div></div>
  <div class="kpi"><div class="v">{ss.get("total_units","—") or "—"}</div><div class="l">Total Units</div></div>
  <div class="kpi"><div class="v">{str(ss.get("absorption_rate_pct","—"))+"%"  if ss.get("absorption_rate_pct") is not None else "—"}</div><div class="l">Avg Absorption{f' ({ss.get("absorption_project_count",0)} projects)' if ss.get("absorption_project_count") else ''}</div></div>
</div>
<h2>Distribution</h2>
<div class="dists">
  <div class="db"><h3>By Segment</h3>{dist_rows(seg_dist)}</div>
  <div class="db"><h3>By Status</h3>{dist_rows(stat_dist)}</div>
  <div class="db"><h3>By BHK</h3>{dist_rows(dict(list(sorted(cfg_dist.items(),key=lambda x:-x[1]))[:6]))}</div>
</div>
{"<h2>Infrastructure Proximity</h2><table><thead><tr><th>Type</th><th>Nearest</th><th>Distance</th><th>Coverage</th></tr></thead><tbody>"+infra_rows+"</tbody></table>" if infra_rows else ""}
<h2>Projects (Top 20 by data quality)</h2>
<table><thead><tr><th>Project</th><th>Developer</th><th>Status</th><th>BHK</th><th>Price Range</th><th>PSF</th><th>Absorption</th><th>RERA</th></tr></thead>
<tbody>{proj_rows}</tbody></table>
<div class="footer">RE·ANALYZE by ASBL · {now}</div>
</div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# ANALYSIS PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def _upd(job_id: str, pct: int, step: str):
    if job_id in JOBS:
        JOBS[job_id]["progress"] = pct
        JOBS[job_id]["step"]     = step
        print(f"  [{pct:3d}%] {step}")
        _persist_job(JOBS[job_id])


def _run(job_id: str):
    job      = JOBS[job_id]
    locality = job["locality"]
    city     = job["city"]
    opts     = job["options"]

    try:
        job["status"] = "running"

        # ── 1. SUPPLY SCRAPING ─────────────────────────────────────────────
        _upd(job_id, 5, "Scraping property portals...")
        raw_listings = []
        try:
            from scraper_pipeline import run_supply_pipeline
            pipe         = run_supply_pipeline(locality, city, max_pages=4)
            raw_listings = pipe.get("listings", [])
        except Exception as e:
            print(f"  Pipeline error: {e}")
            import traceback; traceback.print_exc()
            # Hard fallback: old HTTP scrapers
            _upd(job_id, 8, "Fallback: HTTP scrapers...")
            try:
                from scrapermagicbricks import ScraperMagicBricks
                raw_listings = ScraperMagicBricks().scrape(locality, city, max_pages=3)
            except Exception as e2:
                print(f"  Fallback error: {e2}")

        _upd(job_id, 20, f"Supply: {len(raw_listings)} raw listings found")

        # ── 2. RERA (best-effort only — often 404) ─────────────────────────
        _upd(job_id, 25, "Fetching RERA registered projects...")
        rera_count = 0
        try:
            rera = _fetch_rera(locality, city)
            raw_listings.extend(rera)
            rera_count = len(rera)
        except: pass
        _upd(job_id, 30, f"RERA: {rera_count} projects added")

        # ── 3. INFRASTRUCTURE ──────────────────────────────────────────────
        _upd(job_id, 35, "Finding infrastructure via OpenStreetMap...")
        infra_data = {}
        try:
            from infrafinder import InfrastructureFinder
            infra_data = InfrastructureFinder().get_infrastructure_data(
                locality, city, radius_km=opts.get("radius", 3)
            )
        except Exception as e:
            print(f"  Infra error: {e}")
        _upd(job_id, 45, "Infrastructure data collected")

        # ── 4. DEMAND SCRAPING ─────────────────────────────────────────────
        _upd(job_id, 50, "Collecting demand signals...")
        demand_posts = _load_cached_demand(locality, city)
        if not demand_posts:
            _upd(job_id, 55, "Scraping Google News + search...")
            try:
                from demand_scraper_noreddit import AlternativeDemandScraper
                demand_posts = AlternativeDemandScraper().scrape_all(
                    locality, city,
                    fetch_articles=not opts.get("no_fetch", True),
                )
            except Exception as e:
                print(f"  Demand error: {e}")
                import traceback; traceback.print_exc()
        _upd(job_id, 65, f"Demand: {len(demand_posts)} posts collected")

        # ── 5. SUPPLY PROCESSING ───────────────────────────────────────────
        _upd(job_id, 70, "Processing and cleaning supply data...")
        supply_data, supply_summary, csv_path = [], {}, None
        try:
            from supplyprocessor import SupplyProcessor
            proc            = SupplyProcessor()
            supply_data     = proc.process(raw_listings, infra_data)
            supply_summary  = proc.get_market_summary(supply_data)
            csv_path        = proc.export_csv(supply_data, locality, city)
        except Exception as e:
            print(f"  Supply processor error: {e}")
            import traceback; traceback.print_exc()
        _upd(job_id, 78, f"Supply: {len(supply_data)} clean projects")

        # ── 6. NLP / DEMAND ANALYSIS ───────────────────────────────────────
        _upd(job_id, 82, "Running NLP analysis...")
        demand_data = {
            "locality": locality, "city": city, "post_count": len(demand_posts)
        }
        try:
            from demandanalyzer import DemandAnalyzer
            if len(demand_posts) >= 3:
                raw_d  = DemandAnalyzer().analyze(demand_posts, locality, city)
                sent   = raw_d.get("sentiment", {})
                # Normalise sentiment to 0-1 fractions for frontend
                raw_d["sentiment_distribution"] = {
                    "positive": round(sent.get("positive", 0) / 100, 3),
                    "neutral":  round(sent.get("neutral",  0) / 100, 3),
                    "negative": round(sent.get("negative", 0) / 100, 3),
                }
                raw_d["overall_sentiment"]  = sent.get("interpretation", "")
                raw_d["avg_score"]          = sent.get("average_score", 0)
                # Alias keys the frontend expects
                raw_d["buyer_requirements"] = raw_d.get("top_requirements", [])
                raw_d["builders"]           = raw_d.get("most_discussed", [])
                raw_d["supply_summary"]     = supply_summary
                demand_data = raw_d
        except Exception as e:
            print(f"  NLP error: {e}")
            import traceback; traceback.print_exc()
        _upd(job_id, 88, "Sentiment and topic analysis done")

        # ── 7. BUYER PERSONAS ──────────────────────────────────────────────
        _upd(job_id, 90, "Building buyer personas...")
        personas = {}
        persona_result = {}
        try:
            from persona_builder import PersonaBuilder
            persona_result = PersonaBuilder().build_personas(
                demand_posts, demand_data, supply_summary, locality, city
            )
            for seg in (persona_result.get("segments") or []):
                key = f"segment_{seg['segment_id']}"
                personas[key] = {
                    "name":          seg.get("label", ""),
                    "description":   seg.get("description", ""),
                    "budget":        seg.get("budget", ""),
                    "config":        seg.get("dominant_config", ""),
                    "share_pct":     seg.get("share_pct", 0),
                    "buyer_type":    seg.get("buyer_type", ""),
                    "top_needs":     seg.get("top_needs", []),
                    "top_pain_points": seg.get("top_pain_points", []),
                    "product_fit":   seg.get("product_fit", []),
                    "confidence":    seg.get("confidence", ""),
                    "is_nri":        bool(seg.get("is_nri", False)),
                    "is_investor":   bool(seg.get("is_investor", False)),
                    "wants_gated":   bool(seg.get("wants_gated", False)),
                    "price_elasticity": seg.get("price_elasticity", ""),
                    "sentiment_label":  seg.get("sentiment_label", ""),
                }
        except Exception as e:
            print(f"  Persona error: {e}")
            import traceback; traceback.print_exc()
        _upd(job_id, 94, f"Personas built: {len(personas)} segments")

        # ── 8. EXCEL REPORT ────────────────────────────────────────────────
        _upd(job_id, 96, "Generating Excel report...")
        report_path = None
        try:
            from reportgenerator import ReportGenerator
            report_path = str(ReportGenerator().generate(
                supply_data, demand_data, supply_summary,
                infra_data, locality, city
            ))
        except Exception as e:
            print(f"  Report error: {e}")

        # ── ASSEMBLE RESULTS ───────────────────────────────────────────────
        results = {
            "locality":        locality,
            "city":            city,
            "generated_at":    datetime.now().isoformat(),
            "lat":             job.get("lat"),
            "lng":             job.get("lng"),
            "supply_summary":  supply_summary,
            "supply_projects": supply_data[:60],   # cap at 60 for JSON size
            "demand":          demand_data,
            "infra":           infra_data.get("distances", {}),
            "infra_nearby":    {k: v[:15] for k, v in infra_data.get("nearby", {}).items()},
            "personas":        personas,
            "persona_meta": {
                "n_segments":  persona_result.get("n_segments", 0),
                "silhouette":  persona_result.get("clustering", {}).get("silhouette"),
                "quality":     persona_result.get("clustering", {}).get("quality", ""),
                "supply_gaps": persona_result.get("supply_gaps", []),
                "recommendations": persona_result.get("recommendations", []),
            },
            "meta": {
                "raw_listings":   len(raw_listings),
                "demand_posts":   len(demand_posts),
                "clean_projects": len(supply_data),
                "csv_path":       str(csv_path) if csv_path else None,
            },
        }

        # Persist to disk
        slug = f"{city}_{locality}".lower().replace(" ","_")
        fp   = DATA_DIR / f"{slug}_analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)

        job.update({
            "status":      "done",
            "progress":    100,
            "step":        "Analysis complete",
            "results":     results,
            "report_path": report_path,
            "finished_at": datetime.now().isoformat(),
        })
        _persist_job(job)
        s = datetime.fromisoformat(job["finished_at"])
        e = datetime.fromisoformat(job["started_at"])
        print(f"\n  Job {job_id} completed in {(s-e).seconds}s")

    except Exception as e:
        job.update({"status": "error", "step": f"Error: {e}", "error": str(e)})
        _persist_job(job)
        import traceback; traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_cached_demand(locality: str, city: str) -> list:
    slug = f"{locality}_{city}".lower().replace(" ","_")
    for fp in sorted(DATA_DIR.glob(f"demand_raw_{slug}*.json"), reverse=True)[:1]:
        try:
            with open(fp) as f:
                data = json.load(f)
                if isinstance(data, list) and data:
                    return data
        except: pass
    return []


def _fetch_rera(locality: str, city: str) -> list:
    """Best-effort RERA fetch.  Returns [] on any failure."""
    try:
        import requests
        from bs4 import BeautifulSoup
        r = requests.get(
            "https://rera.telangana.gov.in/public/viewRegisteredProjects",
            params={"district": city, "mandal": locality},
            headers={"User-Agent": "RE-Analyze/1.0"},
            timeout=10,
        )
        if r.status_code != 200: return []
        soup  = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table: return []
        listings = []
        for row in table.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) >= 3:
                listings.append({
                    "source":       "rera_official",
                    "locality":     locality,
                    "city":         city,
                    "project_name": cols[0],
                    "developer":    cols[1] if len(cols) > 1 else "",
                    "rera_id":      cols[2] if len(cols) > 2 else "",
                    "status":       cols[3] if len(cols) > 3 else "",
                })
        return listings
    except:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"""
╔════════════════════════════════════════════╗
║  RE·ANALYZE — API Server v3                ║
╚════════════════════════════════════════════╝
  Dashboard: http://localhost:{port}
  API:       http://localhost:{port}/api/health
    """)
    # use_reloader=False prevents Flask from restarting when files in the project
    # directory change (e.g. scrapers/ subdir), which would clear the in-memory
    # JOBS dict and make all in-flight job status polls return 404.
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=port, threaded=True)
