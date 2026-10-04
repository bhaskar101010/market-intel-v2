"""
compute_vectors.py — Build locality feature vectors for EstateIQ recommendations.
75% numerical features + 25% TF-IDF text features, stored in locality_vectors collection.
"""
import os, re, math, sys
from collections import defaultdict
from datetime import datetime
from pymongo import MongoClient

ATLAS_URI = os.environ.get("MONGO_URI", "")
if not ATLAS_URI:
    print("ERROR: set MONGO_URI"); sys.exit(1)

client = MongoClient(ATLAS_URI)
db = client["market_intel"]
col = db["projects_master"]

INFRA_COLLECTIONS = [
    "schools", "hospitals", "metro_stations", "malls", "parks",
    "it_companies", "banks", "bus_stops", "universities", "lakes",
]
RADIUS_KM = 3.0
EARTH_RADIUS_KM = 6371.0

NUM_WEIGHT = 0.75
TEXT_WEIGHT = 0.25


def get_all_localities():
    locs = col.distinct("location.locality")
    return sorted([l.strip() for l in locs if l and l.strip() and l.strip().upper() not in ("NA", "NONE", "NULL", "")])


def get_locality_centroid(locality):
    pipeline = [
        {"$match": {"location.locality": re.compile("^" + re.escape(locality) + "$", re.I)}},
        {"$match": {"location.lat": {"$gt": 10}, "location.lng": {"$gt": 70}}},
        {"$group": {"_id": None, "lat": {"$avg": "$location.lat"}, "lng": {"$avg": "$location.lng"}}},
    ]
    r = list(col.aggregate(pipeline))
    if r and r[0].get("lat") and r[0].get("lng"):
        return r[0]["lat"], r[0]["lng"]
    return None, None


def compute_numerical_features(locality):
    match = {"location.locality": re.compile("^" + re.escape(locality) + "$", re.I)}
    projects = list(col.find(match))
    n = len(projects)
    if n == 0:
        return None

    def safe_float(v):
        try: return float(v) if v else 0.0
        except: return 0.0

    psf_vals = [safe_float(p.get("pricing", {}).get("price_per_sqft")) for p in projects]
    psf_vals = [v for v in psf_vals if v > 0]

    min_prices = [safe_float(p.get("pricing", {}).get("min_price")) for p in projects]
    min_prices = [v for v in min_prices if v > 0]

    max_prices = [safe_float(p.get("pricing", {}).get("max_price")) for p in projects]
    max_prices = [v for v in max_prices if v > 0]

    units = [safe_float(p.get("building", {}).get("total_apartments")) for p in projects]
    units = [v for v in units if v > 0]

    land = [safe_float(p.get("building", {}).get("land_area_acres")) for p in projects]
    land = [v for v in land if v > 0]

    floors = [safe_float(p.get("building", {}).get("total_floors")) for p in projects]
    floors = [v for v in floors if v > 0]

    amenity_counts = [p.get("amenities", {}).get("count", 0) or 0 for p in projects]

    ratings = [safe_float(p.get("reviews", {}).get("overall_rating")) for p in projects]
    ratings = [v for v in ratings if v > 0]

    all_bhks = set()
    for p in projects:
        bl = p.get("configurations", {}).get("bhk_list") or []
        all_bhks.update(bl)

    developers = set()
    for p in projects:
        b = (p.get("identity", {}).get("builder_name") or "").strip()
        if b:
            developers.add(b.lower())

    def pct(cond):
        return sum(1 for p in projects if cond(p)) / n if n else 0

    def avg(vals):
        return sum(vals) / len(vals) if vals else 0

    def median(vals):
        if not vals: return 0
        s = sorted(vals)
        mid = len(s) // 2
        return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2

    features = {
        "total_projects": n,
        "avg_psf": avg(psf_vals),
        "median_psf": median(psf_vals),
        "avg_total_units": avg(units),
        "avg_land_area_acres": avg(land),
        "avg_floors": avg(floors),
        "pct_gated": pct(lambda p: p.get("identity", {}).get("project_segment") == "Gated Community"),
        "pct_under_construction": pct(lambda p: p.get("identity", {}).get("construction_status") == "Under Construction"),
        "pct_ready_to_move": pct(lambda p: p.get("identity", {}).get("construction_status") == "Ready to Move"),
        "pct_new_launch": pct(lambda p: p.get("identity", {}).get("construction_status") == "New Launch"),
        "pct_rera_registered": pct(lambda p: bool(p.get("rera", {}).get("number"))),
        "avg_amenities_count": avg(amenity_counts),
        "pct_has_swimming_pool": pct(lambda p: p.get("amenities", {}).get("flags", {}).get("swimming_pool", False)),
        "pct_has_clubhouse": pct(lambda p: p.get("amenities", {}).get("flags", {}).get("clubhouse", False)),
        "pct_has_gymnasium": pct(lambda p: p.get("amenities", {}).get("flags", {}).get("gymnasium", False)),
        "avg_rating": avg(ratings),
        "bhk_diversity": len(all_bhks),
        "avg_min_price": avg(min_prices),
        "avg_max_price": avg(max_prices),
        "developer_diversity": len(developers),
    }
    return features


def count_nearby(lat, lng, collection_name, radius_km=RADIUS_KM):
    rad = radius_km / EARTH_RADIUS_KM
    try:
        count = db[collection_name].count_documents({
            "geometry": {
                "$geoWithin": {
                    "$centerSphere": [[lng, lat], rad]
                }
            }
        })
        return count
    except Exception:
        return 0


def compute_infra_features(lat, lng):
    features = {}
    for cname in INFRA_COLLECTIONS:
        features[f"nearby_{cname}"] = count_nearby(lat, lng, cname)
    return features


def extract_report_text(locality):
    doc = db["locality_reports"].find_one(
        {"locality": re.compile("^" + re.escape(locality) + "$", re.I)},
        {"html": 1}
    )
    if not doc or not doc.get("html"):
        return ""
    text = re.sub(r"<style[^>]*>.*?</style>", " ", doc["html"], flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_tfidf(texts):
    from collections import Counter
    stop = {"the","a","an","and","or","but","in","on","at","to","for","of","is","it","by",
            "this","that","with","as","are","was","were","be","been","has","had","have",
            "from","not","no","all","can","will","do","does","did","its","their","they",
            "our","we","you","your","he","she","his","her","than","so","if","about",
            "which","when","what","how","who","each","more","also","very","just","only",
            "may","would","could","should","up","out","over","into","such","these","those",
            "through","between","after","before","during","under","above","below","both",
            "few","some","any","other","most","many","per","via","etc","vs","ie","eg",
            "sqft","rsqft","cr","lakh","hyderabad","telangana","india","report","market",
            "intelligence","estateiq","print","page","style","font","color","background",
            "border","padding","margin","display","none","width","height","size","weight",
            "segoe","system","sans","serif","box","sizing","line"}

    def tokenize(t):
        words = re.findall(r"[a-z]{3,}", t.lower())
        return [w for w in words if w not in stop]

    tokenized = [tokenize(t) for t in texts]
    doc_count = len(texts)

    df = Counter()
    for tokens in tokenized:
        for w in set(tokens):
            df[w] += 1

    vocab = [w for w, cnt in df.items() if 2 <= cnt <= doc_count * 0.85]
    vocab.sort()
    if len(vocab) > 200:
        vocab = sorted(vocab, key=lambda w: df[w], reverse=True)[:200]
        vocab.sort()

    word_idx = {w: i for i, w in enumerate(vocab)}
    dim = len(vocab)

    vectors = []
    for tokens in tokenized:
        tf = Counter(tokens)
        vec = [0.0] * dim
        for w, count in tf.items():
            if w in word_idx:
                idf = math.log(doc_count / (1 + df[w]))
                vec[word_idx[w]] = count * idf
        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]
        vectors.append(vec)

    return vectors, vocab


def normalize_numerical(all_features, feature_names):
    mins = {f: float("inf") for f in feature_names}
    maxs = {f: float("-inf") for f in feature_names}

    for feat in all_features:
        for f in feature_names:
            v = feat.get(f, 0)
            if v < mins[f]: mins[f] = v
            if v > maxs[f]: maxs[f] = v

    normalized = []
    for feat in all_features:
        vec = []
        for f in feature_names:
            v = feat.get(f, 0)
            rng = maxs[f] - mins[f]
            vec.append((v - mins[f]) / rng if rng > 0 else 0.0)
        normalized.append(vec)
    return normalized


def run_one(locality):
    print(f"\n=== Testing: {locality} ===")

    print("  Computing numerical features...")
    num_feat = compute_numerical_features(locality)
    if not num_feat:
        print(f"  ERROR: No projects found for {locality}")
        return None
    for k, v in num_feat.items():
        print(f"    {k}: {v:.2f}" if isinstance(v, float) else f"    {k}: {v}")

    lat, lng = get_locality_centroid(locality)
    if lat and lng:
        print(f"  Centroid: {lat:.4f}, {lng:.4f}")
        print("  Computing infra features...")
        infra = compute_infra_features(lat, lng)
        for k, v in infra.items():
            print(f"    {k}: {v}")
        num_feat.update(infra)
    else:
        print("  WARNING: No centroid found, skipping infra features")
        for cname in INFRA_COLLECTIONS:
            num_feat[f"nearby_{cname}"] = 0

    print("  Extracting report text...")
    text = extract_report_text(locality)
    print(f"    Text length: {len(text)} chars")

    print(f"\n  Total feature dimensions: {len(num_feat)} numerical + TF-IDF text")
    return num_feat, text


def run_all():
    localities = get_all_localities()
    print(f"Found {len(localities)} localities")

    print("\n── Phase 1: Numerical + Infra features ──")
    all_num = []
    all_texts = []
    valid_locs = []

    for loc in localities:
        print(f"  [{len(valid_locs)+1}/{len(localities)}] {loc}...", end=" ", flush=True)
        num_feat = compute_numerical_features(loc)
        if not num_feat:
            print("SKIP (no projects)")
            continue

        lat, lng = get_locality_centroid(loc)
        if lat and lng:
            infra = compute_infra_features(lat, lng)
            num_feat.update(infra)
        else:
            for cname in INFRA_COLLECTIONS:
                num_feat[f"nearby_{cname}"] = 0

        text = extract_report_text(loc)
        all_num.append(num_feat)
        all_texts.append(text)
        valid_locs.append(loc)
        print(f"OK ({num_feat['total_projects']}p, {len(text)}ch)")

    print(f"\n── Phase 2: Normalize numerical ({len(valid_locs)} localities) ──")
    feature_names = sorted(all_num[0].keys())
    num_vectors = normalize_numerical(all_num, feature_names)

    print(f"\n── Phase 3: TF-IDF ({len(all_texts)} reports) ──")
    tfidf_vectors, vocab = build_tfidf(all_texts)
    print(f"  Vocabulary size: {len(vocab)} terms")

    print(f"\n── Phase 4: Combine & store ──")
    out_col = db["locality_vectors"]

    for i, loc in enumerate(valid_locs):
        num_vec = num_vectors[i]
        txt_vec = tfidf_vectors[i]

        num_norm = math.sqrt(sum(v * v for v in num_vec))
        txt_norm = math.sqrt(sum(v * v for v in txt_vec))
        if num_norm > 0:
            num_unit = [v / num_norm for v in num_vec]
        else:
            num_unit = num_vec
        if txt_norm > 0:
            txt_unit = [v / txt_norm for v in txt_vec]
        else:
            txt_unit = txt_vec

        combined = [NUM_WEIGHT * v for v in num_unit] + [TEXT_WEIGHT * v for v in txt_unit]
        c_norm = math.sqrt(sum(v * v for v in combined))
        if c_norm > 0:
            combined = [v / c_norm for v in combined]

        doc = {
            "locality": loc,
            "city": "Hyderabad",
            "numerical_features": all_num[i],
            "numerical_vector": num_vec,
            "tfidf_vector": txt_vec,
            "combined_vector": combined,
            "feature_names": feature_names,
            "tfidf_vocab": vocab,
            "num_weight": NUM_WEIGHT,
            "text_weight": TEXT_WEIGHT,
            "vector_dim": len(combined),
            "updated_at": datetime.utcnow(),
        }
        out_col.update_one({"locality": loc}, {"$set": doc}, upsert=True)
        print(f"  [{i+1}/{len(valid_locs)}] {loc}: {len(combined)}d vector stored")

    print(f"\nDone! {len(valid_locs)} locality vectors in 'locality_vectors' collection.")
    print(f"  Numerical dims: {len(feature_names)}")
    print(f"  TF-IDF dims: {len(vocab)}")
    print(f"  Combined dims: {len(feature_names) + len(vocab)}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        loc = sys.argv[2] if len(sys.argv) > 2 else "Gachibowli"
        run_one(loc)
    else:
        run_all()
