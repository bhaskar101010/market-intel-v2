"""
rag_retrieval.py — Retrieval module for EstateIQ chatbot.
Extracts localities from queries (with fuzzy matching for typos),
fetches relevant data, builds LLM context.
"""
import re, math
from pymongo import MongoClient

_client = None
_db = None
_localities_cache = None
_tfidf_cache = None


def _get_db(uri):
    global _client, _db
    if _db is None:
        _client = MongoClient(uri)
        _db = _client["market_intel"]
    return _db


def _get_localities(db):
    global _localities_cache
    if _localities_cache is None:
        locs = db["locality_vectors"].distinct("locality")
        _localities_cache = sorted([l.strip() for l in locs if l and l.strip()])
    return _localities_cache


def _edit_distance(a, b):
    """Levenshtein distance between two strings."""
    if len(a) < len(b):
        return _edit_distance(b, a)
    if len(b) == 0:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            cost = 0 if ca == cb else 1
            curr.append(min(curr[j] + 1, prev[j + 1] + 1, prev[j] + cost))
        prev = curr
    return prev[-1]


def _fuzzy_match_locality(word, localities, max_dist=2):
    """Find the best matching locality for a word/phrase using edit distance."""
    word_lower = word.lower()
    best = None
    best_dist = max_dist + 1
    for loc in localities:
        loc_lower = loc.lower()
        dist = _edit_distance(word_lower, loc_lower)
        if dist < best_dist:
            best_dist = dist
            best = loc
    if best_dist <= max_dist:
        return best
    return None


def extract_localities(query, db):
    """Extract locality names from query using exact + fuzzy matching."""
    localities = _get_localities(db)
    query_lower = query.lower()
    found = []

    sorted_by_len = sorted(localities, key=len, reverse=True)

    # Pass 1: exact matches (longest first to catch "Financial District" before "Film Nagar")
    remaining = query_lower
    for loc in sorted_by_len:
        pattern = r'\b' + re.escape(loc.lower()) + r'\b'
        if re.search(pattern, remaining):
            found.append(loc)
            remaining = re.sub(pattern, ' ', remaining)

    if found:
        return found

    # Pass 2: fuzzy match — split query into n-grams and check each
    words = re.findall(r'[a-z]+', query_lower)
    skip = set()

    for n in (3, 2, 1):
        for i in range(len(words) - n + 1):
            if any(j in skip for j in range(i, i + n)):
                continue
            phrase = " ".join(words[i:i + n])
            if len(phrase) < 3:
                continue
            # Scale max_dist: short words get 1, longer get 2
            max_d = 1 if len(phrase) <= 5 else 2
            match = _fuzzy_match_locality(phrase, sorted_by_len, max_dist=max_d)
            if match and match not in found:
                found.append(match)
                for j in range(i, i + n):
                    skip.add(j)

    return found


def _get_tfidf_data(db):
    """Load TF-IDF vocab and vectors from locality_vectors for fallback retrieval."""
    global _tfidf_cache
    if _tfidf_cache is not None:
        return _tfidf_cache

    docs = list(db["locality_vectors"].find(
        {}, {"locality": 1, "tfidf_vector": 1, "tfidf_vocab": 1}
    ))
    if not docs:
        _tfidf_cache = ([], [], [])
        return _tfidf_cache

    vocab = docs[0].get("tfidf_vocab", [])
    word_idx = {w: i for i, w in enumerate(vocab)}
    locs = [d["locality"] for d in docs]
    vecs = [d.get("tfidf_vector", []) for d in docs]
    _tfidf_cache = (locs, vecs, word_idx)
    return _tfidf_cache


def tfidf_search(query, db, top_k=3):
    """Find most relevant localities by TF-IDF cosine similarity to query."""
    locs, vecs, word_idx = _get_tfidf_data(db)
    if not locs or not word_idx:
        return []

    stop = {"the","a","an","and","or","but","in","on","at","to","for","of","is",
            "it","by","this","that","with","as","are","was","were","be","been",
            "has","had","have","from","not","no","all","can","will","do","does",
            "which","when","what","how","who","good","bad","best","worst","should",
            "would","could","about","like","near","between","compare","vs","tell",
            "me","show","give","find","looking","want","need","any","some","more"}

    words = re.findall(r"[a-z]{3,}", query.lower())
    words = [w for w in words if w not in stop]

    dim = len(word_idx)
    q_vec = [0.0] * dim
    for w in words:
        if w in word_idx:
            q_vec[word_idx[w]] = 1.0

    q_norm = math.sqrt(sum(v * v for v in q_vec))
    if q_norm == 0:
        return []

    scores = []
    for i, vec in enumerate(vecs):
        dot = sum(a * b for a, b in zip(q_vec, vec))
        v_norm = math.sqrt(sum(v * v for v in vec))
        sim = dot / (q_norm * v_norm) if v_norm > 0 else 0
        if sim > 0:
            scores.append((locs[i], sim))

    scores.sort(key=lambda x: -x[1])
    return [s[0] for s in scores[:top_k]]


def fetch_locality_context(locality, db):
    """Fetch all relevant data for a locality to include in LLM context."""
    context = {"locality": locality}

    report = db["locality_reports"].find_one(
        {"locality": re.compile("^" + re.escape(locality) + "$", re.I)},
        {"analysis": 1, "total_projects": 1}
    )
    if report:
        analysis = report.get("analysis", {})
        context["executive_summary"] = analysis.get("executive_summary", "")
        context["market_position"] = analysis.get("market_position", "")
        context["price_analysis"] = analysis.get("price_analysis", "")
        context["supply_insight"] = analysis.get("supply_insight", "")
        context["sentiment_summary"] = analysis.get("sentiment_summary", "")
        context["investment_outlook"] = analysis.get("investment_outlook", "")
        context["key_highlights"] = analysis.get("key_highlights", [])
        context["risk_factors"] = analysis.get("risk_factors", [])
        context["total_projects"] = report.get("total_projects", 0)

    vec_doc = db["locality_vectors"].find_one(
        {"locality": re.compile("^" + re.escape(locality) + "$", re.I)},
        {"numerical_features": 1}
    )
    if vec_doc:
        context["features"] = vec_doc.get("numerical_features", {})

    return context


def format_context_for_llm(contexts):
    """Format retrieved locality data into a text block for the LLM prompt."""
    if not contexts:
        return ""

    parts = []
    for ctx in contexts:
        loc = ctx["locality"]
        lines = [f"## {loc}"]

        if ctx.get("executive_summary"):
            lines.append(f"**Overview:** {ctx['executive_summary']}")
        if ctx.get("market_position"):
            lines.append(f"**Market Position:** {ctx['market_position']}")
        if ctx.get("price_analysis"):
            lines.append(f"**Pricing:** {ctx['price_analysis']}")
        if ctx.get("supply_insight"):
            lines.append(f"**Supply:** {ctx['supply_insight']}")
        if ctx.get("sentiment_summary"):
            lines.append(f"**Sentiment:** {ctx['sentiment_summary']}")
        if ctx.get("investment_outlook"):
            lines.append(f"**Investment Outlook:** {ctx['investment_outlook']}")

        if ctx.get("key_highlights"):
            lines.append("**Key Highlights:**")
            for h in ctx["key_highlights"]:
                lines.append(f"  - {h}")

        if ctx.get("risk_factors"):
            lines.append("**Risk Factors:**")
            for r in ctx["risk_factors"]:
                lines.append(f"  - {r}")

        feat = ctx.get("features", {})
        if feat:
            lines.append("**Key Numbers:**")
            lines.append(f"  - Total Projects: {feat.get('total_projects', 'N/A')}")
            avg_psf = feat.get('avg_psf', 0)
            if avg_psf:
                lines.append(f"  - Avg Price/sqft: Rs {avg_psf:,.0f}")
            avg_min = feat.get('avg_min_price', 0)
            avg_max = feat.get('avg_max_price', 0)
            if avg_min and avg_max:
                lines.append(f"  - Price Range: Rs {avg_min/10000000:,.2f} Cr - Rs {avg_max/10000000:,.2f} Cr")
            lines.append(f"  - RERA Registered: {feat.get('pct_rera_registered', 0)*100:.0f}%")
            lines.append(f"  - Ready to Move: {feat.get('pct_ready_to_move', 0)*100:.0f}%")
            lines.append(f"  - Under Construction: {feat.get('pct_under_construction', 0)*100:.0f}%")
            schools = feat.get('nearby_schools', 0)
            hospitals = feat.get('nearby_hospitals', 0)
            metro = feat.get('nearby_metro_stations', 0)
            parks = feat.get('nearby_parks', 0)
            if any([schools, hospitals, metro, parks]):
                lines.append(f"  - Nearby: {schools} schools, {hospitals} hospitals, {metro} metro stations, {parks} parks (within 3km)")

        parts.append("\n".join(lines))

    return "\n\n---\n\n".join(parts)


def retrieve(query, mongo_uri, top_k=3):
    """
    Main retrieval function.
    1. Extract localities from query via exact + fuzzy regex
    2. If none found, fall back to TF-IDF similarity search
    3. If still none, return no_context=True so LLM asks clarifying questions
    4. Fetch context for matched localities
    """
    db = _get_db(mongo_uri)

    matched = extract_localities(query, db)
    method = "regex"

    if not matched:
        matched = tfidf_search(query, db, top_k=top_k)
        method = "tfidf"

    if not matched:
        return {
            "localities": [],
            "context": "",
            "method": "none",
            "no_context": True,
        }

    matched = matched[:top_k]
    contexts = [fetch_locality_context(loc, db) for loc in matched]
    formatted = format_context_for_llm(contexts)

    return {
        "localities": matched,
        "context": formatted,
        "method": method,
        "no_context": False,
    }
