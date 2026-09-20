"""
mongo_supply.py — RE·ANALYZE unified data layer
================================================
Sources (verified against actual DB schemas):
  real_estate.projects_master              → project data (31,412 docs)
  insightforge.rera_scraped_data           → absorption: total_apartments, total_booked_apartments
  insightforge.points_of_interest          → 91,595 POIs for proximity (location.lat/lng)
  insightforge.metro_stations              → metro proximity
  insightforge.it_companies                → IT park proximity
  insightforge.hospitals                   → healthcare proximity
  insightforge.schools                     → school proximity
  insightforge.malls                       → lifestyle proximity
  insightforge.lakes                       → FTL buffer zones
  insightforge.universities                → higher education proximity
  insightforge.parks                       → recreational proximity
  insightforge.bus_stops                   → transit proximity
  insightforge.industries                  → employment hub proximity
  insightforge.banks                       → banking proximity
  insightforge.airport_height_restriction_zones → AAI height limits (58 zones)
  insightforge.hmda_all_records            → permit status by mandal/village
  insightforge.fire_noc_r4                 → fire NOC by village/mandal
  insightforge.approval_project_matches    → project-level approval stages (3660 docs)
  insightforge.customer_lifestyle_survey   → buyer insights (262)
  insightforge.customer_property_survey    → buyer property preferences (250)
  buyer_persona.localities                 → market tier + RERA unit mix (100 localities)

Bug fixes vs previous version:
  - facs = doc.get("facilities") → was always None; now reads amenities.all
  - rera_raw = doc.get("rera_raw") → was always None; now reads from rera_scraped_data
  - is_gated now uses identity.project_segment (set by classify_segments.py)
  - segment now uses identity.project_segment directly
  - bhk_list now uses configurations.bhk_list (already normalized)
"""

import os
import re
import math
import logging
import certifi
from collections import defaultdict
from pymongo import MongoClient


# ─────────────────────────────────────────────────────────────────────────────
# LOCALITY CANONICALIZATION
# ─────────────────────────────────────────────────────────────────────────────
def _norm_key(s: str) -> str:
    """Normalize for alias lookup: lowercase, drop dots AND all whitespace."""
    return re.sub(r"[\s.]+", "", s.lower())

# Maps lowercase variant → canonical name. None = discard (junk entry).
LOCALITY_ALIASES: dict[str, str | None] = {
    # Himayat Nagar variants
    "himayath nagar": "Himayat Nagar",
    "himayatnagar": "Himayat Nagar",
    "himayathnagar": "Himayat Nagar",
    "bholakpur, himayath nagar": "Himayat Nagar",
    "gagan mall village, himayath nagar": "Himayat Nagar",
    # Tolichowki
    "toli chowki": "Tolichowki",
    # Gundlapochampally
    "gundlapochampalli": "Gundlapochampally",
    # Lakdikapul
    "lakdi ka pul": "Lakdikapul",
    # Medipalle
    "medipalli": "Medipalle",
    # Kowkur
    "kowkoor": "Kowkur",
    # Upparpalli
    "upperpally": "Upparpalli",
    "bachupalli": "Bachupally",
    "bachupalli, dundigal": "Bachupally",
    # Mushirabad
    "musheerabad": "Mushirabad",
    # A S Rao Nagar
    "as rao nagar": "A S Rao Nagar",
    # Shankarpalli
    "shankarpalli": "Shankarpally",
    # Bandlaguda Jagir
    "bandlaguda jagir": "Bandlaguda Jagir",
    # Pragathi Nagar
    "pragathi nagar kukatpally": "Pragathi Nagar",
    # Gajularamaram
    "gajulramaram kukatpally": "Gajularamaram",
    # Nallagandla
    "nallagandla gachibowli": "Nallagandla",
    # Hitech City
    "hitec city": "Hitech City",
    # Madeenaguda
    "madeenaguda": "Madinaguda",
    "madeenaguda": "Madinaguda",
    # KPHB consolidation
    "kphb colony": "KPHB",
    "kphb phase 1": "KPHB",
    "kphb phase 2": "KPHB",
    "kphb phase 3": "KPHB",
    "kphb phase 4": "KPHB",
    "kphb phase 5": "KPHB",
    "kphb phase 6": "KPHB",
    "1st phase kphb": "KPHB",
    "1st phase kphb": "KPHB",
    "2nd phase kphb": "KPHB",
    "5th phase kphb": "KPHB",
    "5th phase kphb": "KPHB",
    "6th phase kphb": "KPHB",
    "6th phase, kphb colony": "KPHB",
    "rtc x roads": "RTC X Roads",
    "rtc x road":  "RTC X Roads",
    "rtcxroads":   "RTC X Roads",
    # Junk / too-generic entries
    "road": None,
    "hyderabad": None,
    "na": None,
    # ── Puppalaguda variants ────────────────────────────────────────────────
    "puppalaguda - orrgc": "Puppalaguda",
    "puppalaguda-orrgc": "Puppalaguda",
    "puppalguda main": "Puppalaguda",
    "puppulaguda": "Puppalaguda",

    # ── Quthbullapur cluster (5+ spellings) ─────────────────────────────────
    "qutballapur": "Quthbullapur",
    "qutbullapur": "Quthbullapur",
    "quthubullapur": "Quthbullapur",
    "quthbulapoor": "Quthbullapur",

    # ── Raghavendra (Nagar) — separate from "Raghavendra Shelters" project ──
    "ragavendra": "Raghavendra",
    "raghvendra nagar": "Raghavendra Nagar",
    "raghavendra shelters": None,                # this is a society name
    "ragavendra and prasanti housing building society": None,

    # ── Raidurg cluster (administrative subdivisions all = Raidurg) ─────────
    "rai durg": "Raidurg",
    "raidurga": "Raidurg",
    "raidurgam": "Raidurg",
    "rai durga": "Raidurg",
    "raidurg nav khalsa": "Raidurg",
    "raidurg navkhalsa": "Raidurg",
    "raidurga nav khalsa": "Raidurg",
    "raidurga navkhalsa": "Raidurg",
    "raidurg nawkhalsa": "Raidurg",
    "raidurg nawkhalsa-cir11": "Raidurg",
    "raidurg nawkhalsa-cir 11": "Raidurg",
    "raidurga navkhalsa-cr 11": "Raidurg",
    "raidurga navkhalsa-cir11": "Raidurg",
    "raidurga navkhalsa-cir 11": "Raidurg",
    "raidarga nav calsa": "Raidurg",             # phonetic typo
    "raidarga nav khalsa": "Raidurg",

    # ── Raja Rajeshwari ─────────────────────────────────────────────────────
    "raja rajeshwari nagar": "Raja Rajeshwari Nagar",
    "raja rajeshwari": "Raja Rajeshwari Nagar",

    # ── Rajiv Gandhi normalization ──────────────────────────────────────────
    "rajiv gandhi nagar": "Rajiv Gandhi Nagar",

    # ── Junk: too-generic abbreviations / categories ────────────────────────
    "r t c": None,                                # distinct from "RTC X Roads"
    "rtc": None,
    "railway": None,
    "puranapool": "Puranapool",                  # keep — real area
    # Village/colony/layout suffix collapses
    "nallagandla village": "Nallagandla",
    "nallagandla village serilingampally mandal": "Nallagandla",
    "nallagandla huda residential complex": "Nallagandla",
    "nallagandla mig,lig residential layout": "Nallagandla",
    "nallagandla mig, lig residential layout": "Nallagandla",
    "nallagandla and serinallagandla": "Nallagandla",
    "kondapur village": "Kondapur",
    "kondapur, hyderabad": "Kondapur",
    "miyapur village": "Miyapur",
    "miyapur road": "Miyapur",
    "miyapur x road": "Miyapur",
    "kancha gachibowli village": "Gachibowli",
    "kancha gachibowli": "Gachibowli",
    "kanchi gachibowli": "Gachibowli",
    # Tolichowki extras
    "tolichowki - 19": "Tolichowki",
    # Himayat extras (already partial)
    "himayath sagar": "Himayat Sagar",  # NOTE: separate place from Himayat Nagar
     # LB Nagar consolidation (normalization handles L.B / L B / LB / Lb)
    "lb nagar": "LB Nagar",
    "l b nagar cercle": "LB Nagar",
    # Lakdikapul
    "lakidikapool": "Lakdikapul",
    # Lalaguda
    "lallaguda": "Lalaguda",
    "lalaguda, tarnaka": "Lalaguda",
    # Lanco Hills
    "lancohills": "Lanco Hills",
    # Langar Houz
    "langer house": "Langar Houz",
    # Laxmi Nagar
    "laxminagar": "Laxmi Nagar",
    # Lingogiguda
    "lingojiguda": "Lingogiguda",
    # Chandanagar (from earlier top-15)
    "chandanagar": "Chanda Nagar",
    # ── Spelling variants found in dropdown ─────────────────────────────────
    "abdullahpurmet": "Abdullapurmet", "abdullapurampet": "Abdullapurmet",
    "ameenapur": "Ameenpur",
    "ammu guda": "Ammuguda",
    "amberpeta": "Amberpet",
    "bachupalle": "Bachupally", "bachupallu": "Bachupally", "bachuplly": "Bachupally",
    "bachupallly": "Bachupally", "bachuguda": "Bachupally",
    "bahadurpalle": "Bahadurpally", "bahadupally": "Bahadurpally",
    "bahadoorpeta": "Bahadurpally", "bahadurpaly": "Bahadurpally",
    "bai ramal guda": "Bairamalguda", "biramalguda": "Bairamalguda",
    "biramulaguda": "Bairamalguda",
    "bala nagar": "Balanagar", "balanagr": "Balanagar",
    "bandla guda jagir": "Bandlaguda Jagir", "bandlaguda jagire": "Bandlaguda Jagir",
    "bandladuda": "Bandlaguda Jagir",
    "banjarahills": "Banjara Hills",
    "bn reddy nagar": "BN Reddy Nagar", "bn reddy": "BN Reddy Nagar",
    "bnreddynagar": "BN Reddy Nagar",
    "bolaram": "Bolarum",
    "bowrapet": "Bowrampet", "bowranpet": "Bowrampet",
    "chandanagar": "Chanda Nagar",
    "cherlapalli": "Cherlapally",
    "chikkadapally": "Chikkadpally",
    "chinthal": "Chintal", "chintapalli": "Chintal",  # only if you want merged
    "chowdhariguda": "Chowdariguda",
    "dilsuknagar": "Dilsukh Nagar", "dilsukhnagar": "Dilsukh Nagar",
    "domalaguda": "Domalguda",
    "dr as rao nagar": "A S Rao Nagar",
    "dundial": "Dundigal", "dundigalgandimaisamma": "Dundigal",
    "dundigalgandimaisamma": "Dundigal",
    "ecil": "ECIL",
    "gaddi annaram": "Gaddiannaram",
    "gajula ramaram": "Gajularamaram", "gajularamavaram": "Gajularamaram",
    "gandhinagar": "Gandhi Nagar",
    "gandi maisamma": "Gandimaisamma",
    "gopanapally": "Gopanpally", "gopanpalle": "Gopanpally",
    "gopanapalli": "Gopanpally", "gopanapalli thanda": "Gopanpally",
    "gopanapally cross": "Gopanpally",
    "gowdavelly": "Gowdavalli",
    "gundlapochampall": "Gundlapochampally", "gundlapochampalle": "Gundlapochampally",
    "gunfoundry": "Gun Foundry",
    "guttalabegumpet": "Guttala Begumpet", "guttalabegum pet": "Guttala Begumpet",
    "hasmatpet": "Hasmathpet",
    "hayath nagar": "Hayathnagar",
    "hi tech city": "Hitech City",
    "hyder nagar": "Hydernagar", "hydera nagar": "Hydernagar",
    "ibrahim bagh": "Ibrahimbagh", "ibrahimbagh 7b": "Ibrahimbagh",
    "jeedimrtla": "Jeedimetla", "jeedimetla 15": "Jeedimetla",
    "jilleliguda": "Jillelguda", "jillelaguda": "Jillelguda", "jilled": "Jillelguda",
    "jonnabanda": "Jonnabanda",
    "jubliehills": "Jubilee Hills", "jubilees hills": "Jubilee Hills",
    "kachiguda railway station": "Kachiguda",
    "kanchan bagh": "Kanchanbagh", "kanchi gachi bowli": "Gachibowli",
    "kancha gachi bowli": "Gachibowli",
    "kandlakoi": "Kandlakoya",
    "karmaghat": "Kharmanghat", "karmanghar": "Kharmanghat",
    "katedhan": "Katedan",
    "khaithalapur": "Khaitalapur",
    "khairtabad": "Khairatabad",
    "kismatpur": "Kismathpur",
    "kondapue": "Kondapur",
    "korremul": "Korremula",
    "kphb": "KPHB",
    "krishnareddypeta": "Krishna Reddy Pet", "krishtareddipet": "Krishna Reddy Pet",
    "kukatplly": "Kukatpally",
    "l b nagar": "LB Nagar", "lb nagar": "LB Nagar",
    "l b nagar cercle": "LB Nagar",
    "lancohills": "Lanco Hills",
    "lingojiguda": "Lingogiguda",
    "machabolaram": "Macha Bollaram", "macha bollarum": "Macha Bollaram",
    "machabollarum": "Macha Bollaram", "machabollaram": "Macha Bollaram",
    "machha bllaram": "Macha Bollaram", "machha bollaram": "Macha Bollaram",
    "madina guda": "Madinaguda",
    "maheswaram": "Maheshwaram",
    "mailardevpalli": "Mailardevpally", "mailarrdevpally": "Mailardevpally",
    "makthamahaboob peta": "Maktha Mahaboobpet", "makthamahaboobpet": "Maktha Mahaboobpet",
    "maktha mahaboobpeta": "Maktha Mahaboobpet",
    "malkajigiri": "Malkajgiri",
    "manikoda": "Manikonda", "manikonda jagir": "Manikonda",
    "manmole": "Manmole",
    "mansanpalle": "Mansanpally",
    "masabtank": "Masab Tank",
    "medchal-malkajgiri": "Medchal", "medchal malkagjgiri": "Medchal",
    "medipally": "Medipalle",
    "mehdiptnam": "Mehdipatnam",
    "moula-ali": "Moula Ali", "moulali": "Moula Ali",
    "munaganoor": "Munganoor",
    "muthawalliguda": "Muthangi",  # only if confirmed same place; remove if not
    "nampalli": "Nampally",
    "nanakaramguda": "Nanakramguda",
    "narsinghi": "Narsingi", "narsingi-orrgc": "Narsingi", "narsinghi muncipality": "Narsingi",
    "narsingi-orgc": "Narsingi", "narsingii": "Narsingi", "narsingi villaga": "Narsingi",
    "narsingin municipality": "Narsingi", "narsingi muncipality": "Narsingi",
    "narsingi muncipality - orrgc": "Narsingi", "narsingi muncipality-orrgc": "Narsingi",
    "neknapur": "Neknampur",
    "oldbownepally": "Old Bowenpally",
    "padma nagar": "Padmarao Nagar", "padmanagar": "Padmarao Nagar",
    "patancheruvu": "Patancheru", "patanchervu": "Patancheru",
    "patancheruvu-orrgc": "Patancheru",
    "peeramcheru": "Peeranchuruvu", "peeramcheruvu": "Peeranchuruvu",
    "peerancheruvu": "Peeranchuruvu",
    "pet basheerbad": "Pet Basheerabad", "pet basherabad": "Pet Basheerabad",
    "petbasheerbad": "Pet Basheerabad", "pet basherbad": "Pet Basheerabad",
    "pochampalle": "Pocharam",  # only if same place; verify
    "pragathinagar": "Pragathi Nagar",
    "puppalguda": "Puppalaguda", "puppaguda": "Puppalaguda", "puppala guda": "Puppalaguda",
    "qutubullapur": "Quthbullapur", "quthbulapoor": "Quthbullapur",
    "raidurga": "Raidurg", "rai durga": "Raidurg", "raidurgam": "Raidurg",
    "rajendranagar": "Rajendra Nagar",
    "ramchandrapuram": "Ramachandrapuram", "ramachandra puram": "Ramachandrapuram",
    "ramanthepur": "Ramanthapur",
    "ramakrishna nagar": "Ramakrishnapuram",
    "saheb nagar kalan": "Saheb Nagar", "sahebnagar kalan": "Saheb Nagar",
    "saheb nagar khurd": "Saheb Nagar",
    "saroor nagar": "Saroornagar", "saroornagazr": "Saroornagar",
    "serilingam pally": "Serilingampally", "serilimgampally": "Serilingampally",
    "shad nagar": "Shadnagar",
    "shankarpalle": "Shankarpally", "shankerpally": "Shankarpally",
    "srinagar": "Sri Nagar",
    "tara nagar": "Taranaka",  # only if same; remove otherwise
    "turkaimjal": "Turkayamjal", "turkayamajal": "Turkayamjal", "turkaymjal": "Turkayamjal",
    "vanastalipuram": "Vanasthalipuram",
    "vijaynagar": "Vijayanagar",
    "west maredpally": "West Marredpally",
    "yousuf guda": "Yousufguda",
    # ── Alkapoor cluster (Alkapoor / Alkapur / Alkapuri all same place) ─────
    "alkapoor township": "Alkapoor",
    "alkapur township": "Alkapoor", "alkapur town ship": "Alkapoor",
    "alkapuri": "Alkapoor", "alkapuri township": "Alkapoor",
    "alakaapoor township neknampur": "Alkapoor",
    "alakhapuri town ship": "Alkapoor",
    "alkapur,neknampur": "Alkapoor", "alkapur, neknampur": "Alkapoor",
    "alkapur main road,reliance fresh": None,    # landmark, reject
    "alakaapoor township": "Alkapoor",

    # ── Alwal suffix cluster ────────────────────────────────────────────────
    "alwal city": "Alwal",
    "alwal mandal and municipality": "Alwal",
    "alwal mandal,bollaram": "Alwal", "alwal mandal, bollaram": "Alwal",
    "alwal, anarayanapuram mes": "Alwal",
    "alwal, under ghmc": "Alwal",
    "alwal hills": None,                          # project, reject

    # ── Ameenpur suffix cluster ─────────────────────────────────────────────
    "ameenpur mandal and muncipality": "Ameenpur",
    "ameenpur mandal and municipality": "Ameenpur",
    "ameenpur municipality": "Ameenpur",
    "ameenpur village and": "Ameenpur",
    "ameenapur village mandal , sanga reddy dist": "Ameenpur",
    "ameenapur village mandal, sanga reddy dist": "Ameenpur",
    "amenpur": "Ameenpur",                        # likely typo

    # ── Ameerpet ────────────────────────────────────────────────────────────
    "ameerpet, balkampet": "Ameerpet",

    # ── Amberpet ────────────────────────────────────────────────────────────
    "amberpet extension - 20": "Amberpet",
    "amberpet extension": "Amberpet",
    "alkapur": "Alkapoor",

    # ── Generic single-word too-generic to be a locality ────────────────────
    "airport": None,
    # Generic categories (zoning labels, occupations) — never localities
    "advocates": None, "agriculture": None, "agricultural": None,
    "agricluture": None, "doctors": None, "doctor": None, "defence": None,
    "teachers": None, "lecturers": None,
    # Landmarks
    "adj. gns gas agency": None, "adj gns gas agency": None,
    "geological survey of india": None,
    "rajiv gandhi international airport": None,
    # Suffix cleanups
    "abids(south)": "Abids", "abids (south)": "Abids",
    "abids,nampally": "Abids",
    # Typos
    "abibatla": "Adibatla",
    "adharsh nagar": "Adarsh Nagar",
    # Project names slipping through allowlist
    "adithya villa grand": None,

    # ════════════════════════════════════════════════════════════════
    # CANONICAL SELF-MAPPINGS — explicit whitelist so these always
    # resolve regardless of future filter changes.
    # Also handles UPPERCASE and lowercase variants of the same name.
    # ════════════════════════════════════════════════════════════════

    # ── A ──────────────────────────────────────────────────────────
    "abids": "Abids",
    "adarsh nagar": "Adarsh Nagar",
    "adibatla": "Adibatla",
    "adikmet": "Adikmet",
    "almasguda": "Almasguda",
    "alwal": "Alwal",
    "amberpet": "Amberpet",
    "ameenpur": "Ameenpur",
    "ameerpet": "Ameerpet",
    "annojiguda": "Annojiguda",
    "attapur": "Attapur",
    "auto nagar": "Auto Nagar",

    # ── B ──────────────────────────────────────────────────────────
    "bachupally": "Bachupally",
    "badangpet": "Badangpet",
    "bahadurpally": "Bahadurpally",
    "balapur": "Balapur",
    "balkampet": "Balkampet",
    "balanagar": "Balanagar",
    "bandlaguda": "Bandlaguda",
    "bandlaguda jagir": "Bandlaguda Jagir",
    "banjara hills": "Banjara Hills",
    "begumpet": "Begumpet",
    "beeramguda": "Beeramguda",
    "bibinagar": "Bibinagar",
    "boduppal": "Boduppal",
    "bolarum": "Bolarum",
    "borabanda": "Borabanda",
    "bowenpally": "Bowenpally",
    "bowrampet": "Bowrampet",
    "budwel": "Budwel",
    "budvel": "Budwel",

    # ── C ──────────────────────────────────────────────────────────
    "champapet": "Champapet",
    "chengicherla": "Chengicherla",
    "cherlapally": "Cherlapally",
    "chevella": "Chevella",
    "chikkadpally": "Chikkadpally",
    "chilakalguda": "Chilakalguda",
    "chintal": "Chintal",
    "choutuppal": "Choutuppal",

    # ── D ──────────────────────────────────────────────────────────
    "dammaiguda": "Dammaiguda",
    "domalguda": "Domalguda",
    "dulapally": "Dulapally",
    "dullapally": "Dulapally",
    "dundigal": "Dundigal",

    # ── E ──────────────────────────────────────────────────────────
    "east marredpally": "East Marredpally",
    "ecil": "ECIL",
    "erragadda": "Erragadda",

    # ── F ──────────────────────────────────────────────────────────
    "film nagar": "Film Nagar",
    "financial district": "Financial District",

    # ── G ──────────────────────────────────────────────────────────
    "gachibowli": "Gachibowli",
    "gagillapur": "Gagillapur",
    "gandipet": "Gandipet",
    "ghatkesar": "Ghatkesar",
    "gopanpally": "Gopanpally",
    "gudimalkapur": "Gudimalkapur",
    "gundlapochampally": "Gundlapochampally",

    # ── H ──────────────────────────────────────────────────────────
    "habsiguda": "Habsiguda",
    "hafeezpet": "Hafeezpet",
    "hastinapuram": "Hastinapuram",
    "hasmathpet": "Hasmathpet",
    "hayathnagar": "Hayathnagar",
    "humayun nagar": "Humayun Nagar",
    "hyderguda": "Hyderguda",
    "hydershakote": "Hydershakote",

    # ── I ──────────────────────────────────────────────────────────
    "ibrahimpatnam": "Ibrahimpatnam",
    "indira nagar": "Indira Nagar",
    "isnapur": "Isnapur",

    # ── J ──────────────────────────────────────────────────────────
    "jeedimetla": "Jeedimetla",
    "jillalguda": "Jillelguda",
    "jillelguda": "Jillelguda",
    "jubilee hills": "Jubilee Hills",

    # ── K ──────────────────────────────────────────────────────────
    "kachiguda": "Kachiguda",
    "kandi": "Kandi",
    "kandukur": "Kandukur",
    "kapra": "Kapra",
    "karmanghat": "Karmanghat",
    "kavuri hills": "Kavuri Hills",
    "kavadiguda": "Kavadiguda",
    "keesara": "Keesara",
    "khairatabad": "Khairatabad",
    "khajaguda": "Khajaguda",
    "kismathpur": "Kismathpur",
    "kokapet": "Kokapet",
    "kollur": "Kollur",
    "kompally": "Kompally",
    "kondapur": "Kondapur",
    "kothaguda": "Kothaguda",
    "kothapet": "Kothapet",
    "kowkur": "Kowkur",
    "kphb": "KPHB",
    "kukatpally": "Kukatpally",
    "kushaiguda": "Kushaiguda",

    # ── L ──────────────────────────────────────────────────────────
    "langar houz": "Langar Houz",
    "lb nagar": "LB Nagar",
    "lingampally": "Lingampally",

    # ── M ──────────────────────────────────────────────────────────
    "madhapur": "Madhapur",
    "madhura nagar": "Madhura Nagar",
    "maheshwaram": "Maheshwaram",
    "mailardevpally": "Mailardevpally",
    "malkajgiri": "Malkajgiri",
    "malakpet": "Malakpet",
    "mallampet": "Mallampet",
    "mallapur": "Mallapur",
    "mallepally": "Mallepally",
    "manchirevula": "Manchirevula",
    "manikonda": "Manikonda",
    "manneguda": "Manneguda",
    "mansoorabad": "Mansoorabad",
    "medchal": "Medchal",
    "medipalle": "Medipalle",
    "meerpet": "Meerpet",
    "mehdipatnam": "Mehdipatnam",
    "mettuguda": "Mettuguda",
    "miyapur": "Miyapur",
    "mokila": "Mokila",
    "moti nagar": "Moti Nagar",
    "moosapet": "Moosapet",
    "moula ali": "Moula Ali",
    "mushirabad": "Mushirabad",
    "muthangi": "Muthangi",

    # ── N ──────────────────────────────────────────────────────────
    "nacharam": "Nacharam",
    "nagaram": "Nagaram",
    "nagole": "Nagole",
    "nallakunta": "Nallakunta",
    "nallagandla": "Nallagandla",
    "nanakramguda": "Nanakramguda",
    "narayanguda": "Narayanguda",
    "narsingi": "Narsingi",
    "neknampur": "Neknampur",
    "neredmet": "Neredmet",
    "new bowenpally": "New Bowenpally",
    "new nallakunta": "New Nallakunta",
    "nizampet": "Nizampet",
    "nizampet road": "Nizampet",          # road named after locality → merge

    # ── O ──────────────────────────────────────────────────────────
    "old bowenpally": "Old Bowenpally",
    "old malakpet": "Old Malakpet",
    "osman nagar": "Osman Nagar",

    # ── P ──────────────────────────────────────────────────────────
    "padmarao nagar": "Padmarao Nagar",
    "patancheru": "Patancheru",
    "peeranchuruvu": "Peeranchuruvu",
    "peerzadiguda": "Peerzadiguda",
    "pet basheerabad": "Pet Basheerabad",
    "pocharam": "Pocharam",
    "punjagutta": "Punjagutta",
    "puppalaguda": "Puppalaguda",

    # ── Q / R ──────────────────────────────────────────────────────
    "quthbullapur": "Quthbullapur",
    "ramanthapur": "Ramanthapur",
    "rampally": "Rampally",
    "red hills": "Red Hills",

    # ── S ──────────────────────────────────────────────────────────
    "saheb nagar": "Saheb Nagar",
    "sainikpuri": "Sainikpuri",
    "sanath nagar": "Sanath Nagar",
    "sanjeeva reddy nagar": "Sanjeeva Reddy Nagar",
    "sangareddy": "Sangareddy",
    "santosh nagar": "Santosh Nagar",
    "saroornagar": "Saroornagar",
    "saidabad": "Saidabad",
    "secunderabad": "Secunderabad",
    "serilingampally": "Serilingampally",
    "serlingampally": "Serilingampally",
    "shaikpet": "Shaikpet",
    "shankarpally": "Shankarpally",
    "shamirpet": "Shamirpet",
    "shamshabad": "Shamshabad",
    "somajiguda": "Somajiguda",
    "sri nagar colony": "Sri Nagar Colony",
    "srinagar colony": "Sri Nagar Colony",
    "suraram": "Suraram",

    # ── T ──────────────────────────────────────────────────────────
    "tarnaka": "Tarnaka",
    "tellapur": "Tellapur",
    "thumukunta": "Thumukunta",
    "tirumalagiri": "Tirumalagiri",
    "tolichowki": "Tolichowki",
    "tukkuguda": "Tukkuguda",
    "turkayamjal": "Turkayamjal",

    # ── U ──────────────────────────────────────────────────────────
    "uppal": "Uppal",
    "uppal kalan": "Uppal Kalan",
    "upparpalli": "Upparpalli",
    "uppalbhagath": "Uppal Bhagat",
    "uppalbhagayat": "Uppal Bhagat", 
    "uppalbhagayath": "Uppal Bhagat",
    "peerjadiguda": "Peerzadiguda",

    # ── V / W ──────────────────────────────────────────────────────
    "vanasthalipuram": "Vanasthalipuram",
    "vijayanagar colony": "Vijayanagar Colony",
    "vittal rao nagar": "Vittal Rao Nagar",
    "west marredpally": "West Marredpally",
    "whitefields": "Whitefields",

    # ── Y / Z ──────────────────────────────────────────────────────
    "yadagirigutta": "Yadagirigutta",
    "yapral": "Yapral",

    # ── Junk entries (clear landmark / address descriptions) ───────
    "near cyber towers": None,
    "near bheemas hotel": None,
    "near hdfc bank": None,
    "near k b r park": None,
    "opp k b r park": None,
    "near jubilee hills public school": None,
    "near krishna hospital": None,
    "near community hall": None,
    "near yadagiri theater": None,
    "near mahalakshmi temple": None,
    "near pace hospital": None,
    "near maxcure hospital": None,
    "near sree sree foot wear": None,
    "near hitex": None,
    "near lalaguda station": None,
    "near hockey ground": None,
    "near sai baba temple": None,
    "near ramanaidu studio": None,
    "near sampoorna super market": None,
    "above olivers school": None,
    "adj. gns gas agency": None,
    "sandhya theatre": None,
    "ghmc circle": None,
    "citi union bank": None,
    "ramky towers": None,
    "temple park besides": None,
    "vediri township": None,
    "florina apartment": None,
    "road no 10": None, "road no 12": None, "road no 13": None,
    "road no. 2": None, "road no.2": None,
    "road": None,
    "main road": None,
    "gandipet road": None,
    "srisailam highway": None,
    "vijayawada highway": None,
    "bhongiri warangal highway": None,
    "colony": None,
    "rc puram": "RC Puram",
    "RC PURAM": "RC Puram",

    # Fix: JNTU is an acronym
    "jntu": "JNTU",

    # Fix: keep Colony suffix for real named localities
    "venkateshwara colony": "Venkateshwara Colony",
    "janachaitanya colony": "Janachaitanya Colony",

    # Fix: project name leaking as locality → null it
    "sri hanuma s pagadala pride": None,
    "himayathanagar": "Himayat Nagar",
    "maredpally": "Marredpally", 
    "choutppal": "Choutuppal",
    "karimnagarurban": "Karimnagar",
    "mahabubnagarrural": "Mahabubnagar",
    "rtcxroads": "RTC X Roads",
    "rtcxroad":  "RTC X Roads",
    "bandladudjagir": "Bandlaguda Jagir",
    "bandlagudajagir": "Bandlaguda Jagir",
    "jagir": "Bandlaguda Jagir",

    # ════════════════════════════════════════════════════════════════
    # BULK VARIANT ADDITIONS — derived from DB scan of near-duplicate
    # canonical names (difflib ratio ≥ 0.94, verified by project count)
    # ════════════════════════════════════════════════════════════════

    # Atevelly (8 projects) — the canonical
    "atevelle": "Atevelly",
    "athevelly": "Atevelly",
    "athevelle": "Atevelly",

    # Bharat Nagar — pick standard spelling (Bharat, not Bharath)
    "bharath nagar": "Bharat Nagar",
    "bharathnagar": "Bharat Nagar",
    "bharatnagar": "Bharat Nagar",

    # Cantan City Petbasheerabad (2 each, pick one)
    "cantan city petbrasheerabad": "Cantan City Petbasheerabad",

    # Dundigal Municipality (2 > 1)
    "dundigal muncipality": "Dundigal Municipality",

    # Kompally Municipality
    "kompally muncipality": "Kompally Municipality",

    # Jeedimetla Village 1 — drop hyphen variant
    "jeedimetla village - 1": "Jeedimetla Village 1",
    "jeedimetla village-1": "Jeedimetla Village 1",

    # Maktha Mahaboobpet (9 > 1)
    "makthambahaboob pet": "Maktha Mahaboobpet",
    "maqtha mahaboobpet": "Maktha Mahaboobpet",

    # Mallikarjuna Nagar (5)
    "mallikharjuna nagar": "Mallikarjuna Nagar",
    "mallikarjun nagar": "Mallikarjuna Nagar",
    "mallikarjunanagar": "Mallikarjuna Nagar",

    # Shivarampally Jagir (4)
    "shivrampally jagir": "Shivarampally Jagir",
    "shivram pally jagir": "Shivarampally Jagir",

    # Andhra Kesari Nagar (both 1, pick spaced form)
    "andrakesari nagar": "Andhra Kesari Nagar",

    # Mytri Brundavanam (both 1, pick without h)
    "mytri brundhavanam": "Mytri Brundavanam",

    # Suraram Village 1 — drop hyphen/dash variant
    "suraram village - 1": "Suraram Village 1",
    "suraram village-1": "Suraram Village 1",

    # Prasanth Nagar (4 > 3) — without 'h'
    "prashanth nagar": "Prasanth Nagar",
    "prashant nagar": "Prasanth Nagar",

    # Chintalakunta (7 > 1)
    "chintalkunta": "Chintalakunta",

    # Gopanpally Serilingampally (both 1, standard spelling)
    "gopanpalle serilingampally": "Gopanpally Serilingampally",

    # Hanuman Temple (both 1)
    "hanuman teple": "Hanuman Temple",

    # Hastinapuram (51 > 1) — already in aliases but add variants
    "hasthinapuram": "Hastinapuram",
    "hastinapuri": "Hastinapuram",
    "hastinapur": "Hastinapuram",

    # Humayun Nagar (18 > 2)
    "humanyun nagar": "Humayun Nagar",
    "humayunagar": "Humayun Nagar",
    "humayunnagar": "Humayun Nagar",

    # Pashamylaram (7 > 1)
    "pashammylaram": "Pashamylaram",

    # Prabhat Nagar (both 1, standard without h)
    "prabhath nagar": "Prabhat Nagar",

    # Tatti Annaram (5 > 3)
    "thatti annaram": "Tatti Annaram",

    # Venkateshwara (12 > 2)
    "venkateswara": "Venkateshwara",

    # Chatanpally / Chattan Pally (2 > 1)
    "chatanpally": "Chattan Pally",

    # Chiluka Nagar / Chilkanagar (both 1)
    "chilkanagar": "Chiluka Nagar",

    # Devaryamjal (3 > 1+1)
    "devarayamjal": "Devaryamjal",
    "devra yamjal": "Devaryamjal",

    # Fathullaguda (4) — three variants
    "fathulaguda": "Fathullaguda",
    "fatullaguda": "Fathullaguda",

    # Hyathnagar → Hayathnagar (169)
    "hyathnagar": "Hayathnagar",

    # Kanajiguda / Khanajiguda (both 1)
    "kanajiguda": "Khanajiguda",

    # Karmanghat (39) ↔ Kharmanghat (43) — both large, merge into higher count
    "karmanghat": "Kharmanghat",

    # Machirevula → Manchirevula (37)
    "machirevula": "Manchirevula",

    # Mehadipatnam / Mehidipatnam → Mehdipatnam (248)
    "mehadipatnam": "Mehdipatnam",
    "mehidipatnam": "Mehdipatnam",
    "mehdiptnam": "Mehdipatnam",

    # Mithilnagar → Mithila Nagar (2)
    "mithilnagar": "Mithila Nagar",

    # Narayanaguda → Narayanguda (25)
    "narayanaguda": "Narayanguda",

    # Patighanpur (11 > 1)
    "phatighanpur": "Patighanpur",

    # Sriram Nagar (6 > 1)
    "sri rama nagar": "Sriram Nagar",
    "srirama nagar": "Sriram Nagar",

    # Subash Nagar (3 > 1)
    "subhash nagar": "Subash Nagar",
    "subhashnagar": "Subash Nagar",
    "subashnagar": "Subash Nagar",

    # Turkayamjal (71 > 1)
    "thurka yamjal": "Turkayamjal",
    "thurkayamjal": "Turkayamjal",

    # Gaghanpahad (2) → Gaganpahad (2) → Gagan Pahad (1) — merge all
    "gaghanpahad": "Gaganpahad",
    "gagan pahad": "Gaganpahad",

    # Buddha Nagar / Budda Nagar (both 1)
    "budda nagar": "Buddha Nagar",

    # Eshwaripuri / Eswaripuri (both 1)
    "eshwaripuri": "Eswaripuri",

    # Sadashivpet (14 > 1)
    "sadasivpet": "Sadashivpet",

    # Sanath Nagar (126 > 1) — 'Santhnagar' variant
    "santhnagar": "Sanath Nagar",

    # Tulasi Nagar (2 > 1)
    "tulsi nagar": "Tulasi Nagar",

    # Upparpally (both 2)
    "upparapally": "Upparpally",

    # Dundigal Gandimaisamma variants
    "dundigal gadimaisamma": "Dundigal Gandimaisamma",
    "dundigal gandimaisama": "Dundigal Gandimaisamma",

    # Baghameer (5 > 1)
    "bagh ameeri": "Baghameer",
    "bagh ameer": "Baghameer",

    # Bowrampet (143 > 1)
    "bowarampet": "Bowrampet",

    # Film Nagar (26 > 2)
    "filim nagar": "Film Nagar",

    # Harshaguda / Harshguda (both 1)
    "harshaguda": "Harshguda",

    # Lothkunta / Lothkuntha (both 1)
    "lothkunta": "Lothkuntha",

    # Mothi Nagar / Mothinagar → Moti Nagar (36)
    "mothi nagar": "Moti Nagar",
    "mothinagar": "Moti Nagar",

    # Mylargada / Mylargadda (both 1)
    "mylargada": "Mylargadda",

    # Sainikpuri (298 > 1)
    "sanikpuri": "Sainikpuri",

    # Saraswathi (2 > 1)
    "saraswati": "Saraswathi",

    # Thumukunta (27 > 2) — already handled but add new variant
    "thumkunta": "Thumukunta",
    "thumukunta": "Thumukunta",

    # Vijayapuri (5 > 2)
    "vijaypuri": "Vijayapuri",

    # Pragathi Nagar variants
    "pragathi nagar grama panchayathi": "Pragathi Nagar",
    "pragathinagar gram panchayat": "Pragathi Nagar",

    # Dommara Pochampally (2 > 1)
    "dommara pochampalle": "Dommara Pochampally",

    # Cheriyal (2) — Cheeriyal (5) → pick higher
    "cheriyal": "Cheeriyal",

    # Gandipet (175 > 1)
    "gandipet m": "Gandipet",

    # Hafeezpet (261 > 1)
    "hafee pet": "Hafeezpet",
    "hafezpet": "Hafeezpet",
    "hafeezpett": "Hafeezpet",

    # Khanamet (5 > 1)
    "khanameta": "Khanamet",

    # Sankeshwar Bazar (both 1, pick shankeshwar)
    "sankeshwar bazar": "Shankeshwar Bazar",

    # Gurram Guda (12 > 5) — same-norm-key groups also need aliases
    "gurramguda": "Gurram Guda",

    # Kamala Nagar (8 > 5)
    "kamalanagar": "Kamala Nagar",

    # Bandam Kommu (10 > 1)
    "bandamkommu": "Bandam Kommu",

    # Yellareddy Guda (2) + Yella Reddy Guda (1) → Yellareddyguda (8)
    "yellareddy guda": "Yellareddyguda",
    "yella reddy guda": "Yellareddyguda",

    # Jawahar Nagar (9 > 1)
    "jawahar nagar": "Jawahar Nagar",

    # Hmt Swarnapuri (8 > 2)
    "hmt swarna puri": "Hmt Swarnapuri",

    # Pakalakunta (8 > 1)
    "pakala kunta": "Pakalakunta",

    # GVK One Mall (2 > 1+1)
    "g v k one mall": "GVK One Mall",
    "gvk one mall": "GVK One Mall",

    # Izzathnagar (5 > 1)
    "izzath nagar": "Izzathnagar",

    # Mamidipally (11 > 4)
    "mamidpally": "Mamidipally",

    # Mansoorabad (121 > 1)
    "masoorabad": "Mansoorabad",

    # Nalagandla / Nallgandla → Nallagandla (262)
    "nalagandla": "Nallagandla",
    "nallgandla": "Nallagandla",

    # Madhina Guda → Madinaguda (159)
    "madhina guda": "Madinaguda",

    # SR Nagar (2 > 1+1) — already has 'sr nagar' variants, add more
    "s r nagar": "Sr Nagar",
    "s.r.nagar": "Sr Nagar",

    # Old Safil Guda (both 2, pick spaced)
    "old safilguda": "Old Safil Guda",

    # Sai Ram Nagar (3 > 2)
    "sairam nagar": "Sai Ram Nagar",

    # Cheeriyal > Cheriyal already handled above

    # Mithila Nagar / Mithilanagar (both 2 > Mithilnagar 1)
    "mithilanagar": "Mithila Nagar",

    # Krishna Nagar / Krishnanagar (both 3)
    "krishnanagar": "Krishna Nagar",
    "krishna nagar": "Krishna Nagar",

    # Keshava Nagar / Keshav Nagar (both 1)
    "keshav nagar": "Keshava Nagar",

    # Gafoor Nagar → Gafoornagar (2 > 1)
    "gafoor nagar": "Gafoornagar",

    # Premavathi Pet → Premavathipet (2 > 1)
    "premavathi pet": "Premavathipet",

    # Thumma Bowli → Thummabowli (2 > 1)
    "thumma bowli": "Thummabowli",

    # Gagan Mahal (1+1)
    "gaganmahal": "Gagan Mahal",

    # IDA Uppal (both 1)
    "idauppal": "IDA Uppal",

    # HMT Nagar (2 > 1)
    "hmt nagar": "Hmt Nagar",

    # Balaji Nagar (6 > 1)
    "balajinagar": "Balaji Nagar",

    # Green Hills (6 > 1)
    "greenhills": "Green Hills",

    # Sai Baba Temple (4 > 1)
    "saibaba temple": "Sai Baba Temple",

    # Ravindra Nagar (3 > 1)
    "ravindranagar": "Ravindra Nagar",

    # Krishi Nagar (2 > 1)
    "krishinagar": "Krishi Nagar",

    # Pedda Amberpet (21 > 1)
    "pedda amber pet": "Pedda Amberpet",

    # Mamatha Nagar (4 > 1)
    "mamatha nagar": "Mamatha Nagar",

    # Bagh Hayath Nagar (both 1)
    "bagh hayathnagar": "Bagh Hayath Nagar",

    # Bhagya Nagar (both 1)
    "bhagyanagar": "Bhagya Nagar",

    # Saroor Nagar Old (both 1)
    "saroor nagar old": "Saroor Nagar Old",

    # Vidya Nagar (both 1)
    "vidyanagar": "Vidya nagar",

    # Sun City (both 1)
    "suncity": "Sun City",

    # Rajappa Nagar (both 1, proper case)
    "rajappa nagar": "Rajappa Nagar",

    # High Tension (both 1, standard case)
    "high tension": "High Tension",

    # Geetha Nagar (both 1)
    "geethanagar": "Geetha Nagar",

    # Madhava Nagar (both 1)
    "madhavanagar": "Madhava Nagar",

    # Padmarao Nagar 18 (both 1)
    "padmaraonagar 18": "Padmarao Nagar 18",

    # Ganesh Nagar (both 1)
    "ganeshnagar": "Ganesh Nagar",

    # Bahadur Guda (both 1)
    "bahadurguda": "Bahadur Guda",

    # Saroor Nagar Old lowercase variant
    "saroor nagar old": "Saroor Nagar Old",

    # ════════════════════════════════════════════════════════════════
    # BATCH 2 — comprehensive DB scan (difflib ≥ 0.88, human-verified)
    # Only confirmed same-geographic-place merges; false positives skipped.
    # ════════════════════════════════════════════════════════════════

    # Municipality / administrative suffix variants → base locality
    "dundigal municipality": "Dundigal",
    "manikonda municipality": "Manikonda",
    "matrusri": "Matrusri Nagar",
    "medchal municipality": "Medchal",
    "nagaram municipality": "Nagaram",
    "kompally municipality": "Kompally",
    "turkaymjal muncipality": "Turkayamjal",
    "turkaymjalmuncipality": "Turkayamjal",
    "thukkuguda muncipality": "Tukkuguda",
    "thukkuguda muncipality-orrgc": "Tukkuguda",
    "thukkuguda municipality": "Tukkuguda",
    "dundigal/gandimaismma": "Dundigal Gandimaisamma",

    # Upparpally / Upparpalli — same place
    "upparpally": "Upparpalli",

    # Tarnaka (standard) / Taranaka variant
    "taranaka": "Tarnaka",

    # Tellapur (standard) / Telapur variant
    "telapur": "Tellapur",

    # Atevelly variants (Atevelly is canonical per count)
    "atvelly": "Atevelly",
    "athvelly": "Atevelly",

    # Sr Nagar → Sri Nagar
    "sr nagar": "Sri Nagar",

    # Banglaguda Jagir → Bandlaguda Jagir
    "banglaguda jagir": "Bandlaguda Jagir",
    "bangaliguda jagir": "Bandlaguda Jagir",
    "bangaliguda jagiir": "Bandlaguda Jagir",
    "banglaguda jagir municipal corporation": "Bandlaguda Jagir",
    "bangaliguda jagiir municipal corporation": "Bandlaguda Jagir",
    "bandlaguda jagir municipal corporation": "Bandlaguda Jagir",

    # Yousufguda (standard 102) / Yousafguda variant
    "yousafguda": "Yousufguda",

    # Punjagutta (standard 61) / Panjagutta variant
    "panjagutta": "Punjagutta",

    # Srinivasa / Srinivasa Nagar
    "srinivasa": "Srinivasa Nagar",

    # Pragathi → Pragathi Nagar
    "pragathi": "Pragathi Nagar",

    # Saket → Saket Nagar
    "saket": "Saket Nagar",

    # Narapalli / Narapally — same spelling variant
    "narapalli": "Narapally",

    # Pothireddypalli → Pothireddypally
    "pothireddypalli": "Pothireddypally",

    # Mangalpalli / Mangalpally / Mangalpalle — same place
    "mangalpalle": "Mangalpalli",
    "mangalpally": "Mangalpalli",

    # HMT without Nagar → Hmt Nagar
    "hmt": "Hmt Nagar",

    # Bhoiguda / Boiguda — same place
    "boiguda": "Bhoiguda",

    # Chaitanya Nagar / Chaithnya Nagar
    "chaithnya nagar": "Chaitanya Nagar",
    "chaithyanagar": "Chaitanya Nagar",

    # Chilkur / Chilkuru — same place (near Gandipet)
    "chilkur": "Chilkuru",

    # Katedan / Kattedan
    "kattedan": "Katedan",

    # Kompally / Komplly typo
    "komplly": "Kompally",

    # Maktha Mahaboobpet / Maqtha Mahaboob
    "maqtha mahaboob": "Maktha Mahaboobpet",
    "maqtha mahaboobpet": "Maktha Mahaboobpet",

    # Mailardevpally / Maillardevpalli
    "maillardevpalli": "Mailardevpally",
    "maillardevpally": "Mailardevpally",

    # Nandigam / Nandigama
    "nandigam": "Nandigama",

    # Uppal Bhagat / Uppal Bhagyath / Uppal Bagayath
    "uppal bhagyath": "Uppal Bhagat",
    "uppal bagayath": "Uppal Bhagat",

    # Bagh Amberpet → Bagh Amberpet (drop ward number suffix)
    "bagh amberpet 20": "Bagh Amberpet",

    # Mankal / Mankhal / Mankahl — same place
    "mankahl": "Mankhal",
    "mankal": "Mankhal",

    # Metkanigudem / Mettakanigudem
    "metkanigudem": "Mettakanigudem",

    # Naglo/Nagole to Bandlaguda — address descriptions → null
    "naglo to bandlaguda": None,
    "nagole to bandlaguda": None,
    "nagol bandla guda": None,

    # Dilshuknagar → Dilsukh Nagar
    "dilshuknagar": "Dilsukh Nagar",

    # Gaandi Nagar → Gandhi Nagar
    "gaandi nagar": "Gandhi Nagar",
    "gaandinagar": "Gandhi Nagar",

    # Kongar Khurd A / Kongarakhurd
    "kongar khurd a": "Kongarakhurd",

    # Domaripochampally → Dommara Pochampally
    "domaripochampally": "Dommara Pochampally",

    # Cheriapally → Cherlapally (standard 25)
    "cheriapally": "Cherlapally",

    # Ibrahimbagh / Inrahimbagh typo
    "inrahimbagh": "Ibrahimbagh",
    "inrahimbagh 7b": "Ibrahimbagh",

    # Kaithalapur → Khaitalapur (both small, pick K form)
    "kaithalapur": "Khaitalapur",

    # Kavadipally / Kawadipally — same
    "kavadipally": "Kawadipally",

    # Mahaboob Pet / Mehaboobpet
    "mehaboobpet": "Mahaboob Pet",
    "mahaboobpet": "Mahaboob Pet",

    # Mamidipalle / Mamidipally / Mamidpalli
    "mamidipalle": "Mamidipally",
    "mamidpalli": "Mamidipally",

    # Medhidi Jung / Mehdi Jung
    "medhidi jung": "Mehdi Jung",

    # Vidyanagar 9A → Vidya nagar (drop door/sector)
    "vidyanagar 9a": "Vidya nagar",

    # Vinayak Nagar / Vinaynagar
    "vinaynagar": "Vinayak Nagar",
    "vinayaknagar": "Vinayak Nagar",

    # Walker Town 18 → Walker Town (drop ward)
    "walker town 18": "Walker Town",

    # Deepthi sree Nagar / Deepthisri Nagar
    "deepthi sree nagar": "Deepthisri Nagar",
    "deepthisreenagar": "Deepthisri Nagar",

    # Rameshwar Banda / Rameswaram Banda
    "rameshwar banda": "Rameswaram Banda",

    # Rhoda Mistri Nagar / Rodamestri Nagar
    "rhoda mistri nagar": "Rodamestri Nagar",

    # Yellareddy Guda 10A → Yellareddyguda (drop door/sector)
    "yellareddy guda 10a": "Yellareddyguda",

    # Beerumguda → Beeramguda (standard 70)
    "beerumguda": "Beeramguda",

    # Hanamkonda / Hanmakonda
    "hanmakonda": "Hanamkonda",

    # Kalvakunta / Kalwakunta
    "kalvakunta": "Kalwakunta",

    # Kavadiguda (79) / Kawadiguda (2) — merge to Kavadiguda
    "kawadiguda": "Kavadiguda",

    # Vattapally / Vattepally
    "vattapally": "Vattepally",

    # New Nallakunta - 20 → New Nallakunta (drop ward)
    "new nallakunta - 20": "New Nallakunta",
    "new nallakunta-20": "New Nallakunta",

    # Madhavapuri Township / Modhavapuri Toen Ship
    "modhavapuri toen ship": "Madhavapuri Township",
    "modhavapuritoenship": "Madhavapuri Township",

    # Begumpet 18 → Begumpet (drop ward)
    "begumpet 18": "Begumpet",

    # Brindavan / Brundavan
    "brindavan": "Brundavan",

    # IDA Pashamylaram → Pashamylaram
    "ida pashamylaram": "Pashamylaram",

    # Jubilee Hills - 19 → Jubilee Hills (drop ward)
    "jubilee hills - 19": "Jubilee Hills",
    "jubilee hills-19": "Jubilee Hills",

    # Mettuguda / Muttuguda
    "muttuguda": "Mettuguda",

    # Narepally → Narapally (standard form)
    "narepally": "Narapally",

    # Nekanmpur → Neknampur (standard 133)
    "nekanmpur": "Neknampur",

    # Pasmamla / Pasumamula
    "pasmamla": "Pasumamula",

    # Rajampet / Rajapeta
    "rajampet": "Rajapeta",

    # Thumukunta / Tumkunta
    "tumkunta": "Thumukunta",

    # Bhagya Lakshmi Nagar / Bhagyalaxmi Nagar
    "bhagya lakshmi nagar": "Bhagyalaxmi Nagar",
    "bhagyalakshminar": "Bhagyalaxmi Nagar",

    # Chanakyapuri / Chanukuyapuri typo
    "chanukuyapuri": "Chanakyapuri",

    # Saroor Nagar Old → Saroornagar (sub-area merges to parent)
    "saroor nagar old": "Saroornagar",

    # Opp Jntu / Opp Jntuc — landmark descriptions → null
    "opp jntu": None,
    "opp jntuc": None,

    # KARMANGHAT OLD VILLAGE (Champapet-3B/38) — too-specific → Karmanghat
    "karmanghat old village (champapet-3b)": "Karmanghat",
    "karmaghat old village (champapet-38)": "Karmanghat",

    # Srinivas → Srinivasa Nagar
    "srinivas": "Srinivasa Nagar",

    # Sri (standalone) → null (too generic)
    "sri": None,

    # Hmt Swarna Puri → Hmt Swarnapuri (standard)
    "hmt swarna puri": "Hmt Swarnapuri",

    # Prashanth / Prashanth alone → Prasanth Nagar
    "prashanth": "Prasanth Nagar",

    # Padmarao Nagar 18 → Padmarao Nagar (drop ward)
    "padmarao nagar 18": "Padmarao Nagar",
    "padmaraonagar 18": "Padmarao Nagar",

    # Venkatapur / Venkatapuram
    "venkatapur": "Venkatapuram",

    # Vivekananda Nagar / Vivekanandapuram — same
    "vivekananda nagar": "Vivekanandapuram",
    "vivekanandanagar": "Vivekanandapuram",

    # Khajiguda → Khajaguda (standard 27 > 1)
    "khajiguda": "Khajaguda",

    # Raghavendra (standalone 14) → keep; Raghavendra Nagar is separate
    # Skip: unclear if they're the same

    # Matrusri Nagar self-alias
    "matrusrinagar": "Matrusri Nagar",

    # New Nallakunta - 20 lower
    "new nallakunta20": "New Nallakunta",

    # ════════════════════════════════════════════════════════════════
    # BATCH 3 — prefix/junk-suffix, X Roads, and remaining fuzzy
    # (full audit via 7-strategy scan, human-verified merges only)
    # ════════════════════════════════════════════════════════════════

    # ── Junk administrative suffixes → base locality ─────────────────
    "saroornagar revenue": "Saroornagar",
    "chanda nagar (serilingampally)": "Chanda Nagar",
    "gajularamaram quthbullapur": "Gajularamaram",
    "gajularamaram medchal": "Gajularamaram",
    "kothapet fruit market": "Kothapet",
    "jubilee hills check post": "Jubilee Hills",
    "jeedimetla village 1": "Jeedimetla",
    "tarnaka 18": "Tarnaka",
    "neknampur grama khantam": "Neknampur",
    "mansoorabad l b nagar": "Mansoorabad",
    "ghatkesar-orrgc": "Ghatkesar",
    "ghatkesar orrgc": "Ghatkesar",
    "ghatkesar medchal malkajgiri district": "Ghatkesar",
    "gundlapochampally muncipality": "Gundlapochampally",
    "gundlapochampally municipality": "Gundlapochampally",
    "ramanthapur khalsa": "Ramanthapur",
    "suraram village 1": "Suraram",
    "suraram village": "Suraram",
    "manneguda sarecas": "Manneguda",
    "sangareddy mandal and district": "Sangareddy",
    "gaddiannaram prabath nagar": "Gaddiannaram",
    "gandimaisamma orrgc": "Gandimaisamma",
    "film nagar junction": "Film Nagar",
    "keesara orr": "Keesara",
    "secunderabad court": "Secunderabad",
    "velimela (v) rc puram (m)": "Velimela",
    "velimela rc puram": "Velimela",
    "kukatpally housing board colony": "Kukatpally Housing Board",
    "zaheerabad town": "Zaheerabad",
    "pashamylaram of tsiic": "Pashamylaram",
    "pakalakunta hamlet of alwal": "Pakalakunta",
    "chikkadpally part": "Chikkadpally",
    "gandipet - orrgc": "Gandipet",
    "gandipet orrgc": "Gandipet",
    "bollaram industrial area": "Bollaram",
    "fathullaguda ( fathullaguda (v))": "Fathullaguda",
    "fathullaguda (v)": "Fathullaguda",
    "turkapally alwal": "Turkapally",
    "tellapur muncipality": "Tellapur",
    "tellapur municipality": "Tellapur",
    "narsingi -kokapet": "Narsingi",
    "narsingi kokapet": "Narsingi",
    "kollur orrgc": "Kollur",
    "dundigal municipality": "Dundigal",
    "sai nagar ( nagole)": "Sai Nagar",
    "sai nagar (nagole)": "Sai Nagar",
    "anmagal hayathnagar": "Anmagal",
    "kalwakunta w19": "Kalwakunta",
    "nizampet municipality": "Nizampet",
    "nizampet muncipal corporation": "Nizampet",
    "nizampet municipal corporation": "Nizampet",
    "nizampet opp": "Nizampet",
    "kokapet orrgc": "Kokapet",
    "rampally dayara": "Rampally",
    "brahmanwadi locality": "Brahmanwadi",
    "kakatiya nagar habsiguda": "Kakatiya Nagar",
    "sai baba temple": "Sai Baba",
    "hanuman temple back side": "Hanuman Temple",
    "nagarjuna sagar highway": "Nagarjuna Sagar",
    "ganesh nagar chintal": "Ganesh Nagar",
    "karmanghat old": "Kharmanghat",
    "sainikpuri post": "Sainikpuri",
    "gopanpally serilingampally": "Gopanpally",
    "gopanpally and osman nagar villages": "Gopanpally",
    "old bowenpally kukatpally": "Old Bowenpally",
    "padma nagar 2": "Padma Nagar",
    "padmanagar 2": "Padma Nagar",
    "pudoor vilage": "Pudoor",
    "pudoor village": "Pudoor",
    "nizamabad municipality corporation": "Nizamabad",
    "golconda fort area": "Golconda",
    "rock town residents": "Rock Town",
    "rock town -thummabowli": "Rock Town",
    "velmula (tm)": "Velmula",
    "vasanth nagar bus stop": "Vasanth Nagar",
    "kompally village and gram panchayat": "Kompally",
    "patancheru shankarpalli": "Patancheru",
    "malakpet extension": "Malakpet",
    "yapral village alwal": "Yapral",
    "yapral village": "Yapral",
    "yapral-sainikpuri": "Yapral",
    "shamirpet-orrgc": "Shamirpet",
    "shamirpet orrgc": "Shamirpet",
    "snehitha hills phase - ii": "Snehitha Hills",
    "snehitha hills phase ii": "Snehitha Hills",
    "vijayapuri east": "Vijayapuri",
    "gollapalle kalan": "Gollapalle",
    "central excise colony 9b": "Central Excise Colony",
    "central excise colony": "Central Excise Colony",
    "central excise": "Central Excise Colony",
    "sagar highway": "Sagar",
    "sagar society": "Sagar",
    "vasavi siva sai nagar": "Vasavi",
    "uppal bagath vilalge": "Uppal Bhagat",
    "uppal bagath village": "Uppal Bhagat",
    "high tension line": "High Tension",
    "madhapur town ship": "Madhapur",

    # ── X Roads / Cross Roads suffix → base name ─────────────────────
    "nizampet x roads": "Nizampet",
    "nizampet xroads": "Nizampet",
    "sagar x roads": "Sagar",
    "sagar xroads": "Sagar",
    # RTC X Roads — canonical is the plural form
    "rtc x roads": "RTC X Roads",
    "rtc x road": "RTC X Roads",
    "rtcxroads": "RTC X Roads",
    "rtcxroad": "RTC X Roads",

    # ── Remaining fuzzy / phonetic fixes ─────────────────────────────
    # Kapra / Khapra — same place near Uppal
    "kapra": "Khapra",

    # Madhavapuri Township / Madhapur Town Ship already handled above
    # (madhapur town ship → Madhapur; Madhavapuri Township stays separate)

    # Gaddiannaram self-alias (catches raw "Gaddiannaram Prabath Nagar")
    # already handled above

    # Uppal Bagath Vilalge → Uppal Bhagat (correct spelling)
    # already handled above

    # ── Appa Junction / Appa Junction Peerancheru ────────────────────
    "appa junction peerancheru": "Appa Junction",
    "appajunctionpeerancheru": "Appa Junction",

    # ── Remaining standalone → full-name merges ───────────────────────
    "raghavendra": "Raghavendra Nagar",
    "saraswathi": "Saraswathi Nagar",
    "padma": "Padma Nagar",

    # ── Jeedimetla / Suraram — catch additional raw DB variants ──────
    "jeedimetla village": "Jeedimetla",
    "jeedimetla vill": "Jeedimetla",
    "jeedimetla v": "Jeedimetla",
    "suraram v": "Suraram",

    # ── Malakpet / Old Malakpet — keep separate (different areas) ────
    # ── Bowenpally / New Bowenpally / Old Bowenpally — keep separate ──

    # ════════════════════════════════════════════════════════════════
    # BATCH 4 — explicit None suppressions (junk entries not caught
    # by _REJECT_RE patterns), plus remaining canonical fixes
    # ════════════════════════════════════════════════════════════════

    # ── RTC X Roads canonical fix ─────────────────────────────────
    # (already set in Batch 3, entry here ensures dict-final wins)
    "rtcxroads": "RTC X Roads",
    "rtcxroad": "RTC X Roads",
    "rtc x roads": "RTC X Roads",
    "rtc x road": "RTC X Roads",

    # ── "Village: Karmanghat" → canonical ────────────────────────
    "village: karmanghat": "Kharmanghat",
    "village:karmanghat": "Kharmanghat",
    "village karmanghat": "Kharmanghat",

    # ── Cantan City Petbasheerabad → Cantan City ──────────────────
    "cantan city petbasheerabad": "Cantan City",
    "cantan city pet basheerabad": "Cantan City",

    # ── D D Colony Bagh Amberpet → Bagh Amberpet ─────────────────
    "d d colony bagh amberpet": "Bagh Amberpet",
    "dd colony bagh amberpet": "Bagh Amberpet",

    # ── Pagoda Plazaa Manneguda → Manneguda ──────────────────────
    "pagoda plazaa manneguda": "Manneguda",
    "pagoda plaza manneguda": "Manneguda",

    # ── Junk shop / venue names → None ───────────────────────────
    "gvk one mall": None,
    "manjeera mall": None,
    "hitex exhibition center": None,
    "hitex exhibition centre": None,
    "tanishq jewellers": None,
    "khajana jewellary": None,
    "khajana jewellers": None,
    "nandi oven fresh baker": None,
    "ohm book store": None,
    "more super market": None,
    "zam zam dhaba back side": None,
    "zam zam dhaba backside": None,
    "zamzam dhaba": None,

    # ── Junk building / project names → None ─────────────────────
    "indu fortune fields gardenia": None,
    "aparna sarovar zenith": None,
    "the aurum": None,
    "trendset winz": None,
    "dream one": None,
    "antilia": None,
    "mytri brundavanam": None,
    "godavari homes": None,
    "happy homes": None,
    "bhabha housing": None,
    "central park 1": None,

    # ── Junk institutional / company names → None ─────────────────
    "etdc": None,
    "iict": None,
    "gpra campus": None,
    "vishweshwarayya engineering": None,
    "medha company": None,
    "medical & health": None,
    "azone badminton": None,
    "giridhari art": None,
    "web pixon": None,
    "cafe city line": None,
    "klr njr": None,
    "klr venture": None,
    "drs shcool": None,
    "drs school": None,
    "czech": None,
    "dynamics": None,

    # ── Junk abbreviations / single-word generics → None ──────────
    "etdc": None,
    "nfc": None,
    "pjr": None,
    "pnr": None,
    "c.e": None,
    "c.b.c.i.d": None,
    "cbcid": None,
    "m g": None,
    "m v": None,
    "o u": None,
    "b block": None,
    "s d": None,
    "mai": None,
    "hal": None,
    "apau": None,
    "awho": None,
    "aphb": None,
    "deluxe": None,
    "friends": None,
    "fine arts": None,
    "journalists": None,
    "information": None,
    "telephone": None,
    "bypass": None,
    "house": None,
    "revenue": None,
    "grampanchayat": None,
    "muncipal corporation": None,
    "open land": None,

    # ── Address-fragment / description entries → None ─────────────
    "opp. kapra municipal office": None,
    "opp kapra municipal office": None,
    "opp-jubilee ridge hotel": None,
    "water tank oppposite": None,
    "water tank opposite": None,
    "patancheruv- gmr convention hall": None,
    "patancheruv gmr convention hall": None,
    "captain veer raj pandey marg": None,
    "mouryas ranga prasad avenue": None,
    "north park avenue": None,
    "century avenue": None,
    "chacha nehru park backside": None,
    "chacha nehru park back side": None,
    "back side land of subway": None,
    "more backside kothapet": None,
    "more back side kothapet": None,
    "cross roads": None,
    "secretariat employees": None,
    "sbi officers": None,
    "se side of": None,
    "war no 3": None,
    "vv nagar bustop": None,
    "vv nagar bus stop": None,
    "richshaw colony w16": None,
    "north gate no.3": None,
    "north gate no 3": None,
    "tad bun": None,
    "pedso 2": None,
    "apollo": None,

    # ── Temple entries that are not locality names → None ─────────
    "bramaramba mallikarjuna temple": None,
    "tuljaram temple": None,
    "sairama temple": None,
    "srirama temple": None,
    "balaji temple": None,

    # ── Seetha Ram Puram - 13 → Sriram Nagar ──────────────────────
    "seetha ram puram - 13": "Sriram Nagar",
    "seetha ram puram": "Sriram Nagar",

    # ── Mehdiptnam - 7 → Mehdipatnam ─────────────────────────────
    "mehdiptnam - 7": "Mehdipatnam",
    "mehdiptnam-7": "Mehdipatnam",
    "mehdiptnam": "Mehdipatnam",

    # ── Batch 2: variants found in UI screenshots + fuzzy-match audit ──────────

    # Gachibowli — Kancha Gachibowli is an absorbed village, same locality
    "kancha gachibowli": "Gachibowli",
    "kancha gachibowli village": "Gachibowli",
    "kanchi gachibowli": "Gachibowli",

    # Kukatpally cluster
    "kukatpally housing board": "Kukatpally",
    "kukatpally(medchal - malkajgiri)": "Kukatpally",
    "kukatpally (medchal-malkajgiri)": "Kukatpally",
    "kukatpally medchal malkajgiri": "Kukatpally",
    "madhavi nagar kukatpally": "Kukatpally",
    "madhavi nagar, kukatpally": "Kukatpally",
    "kukatpally(medchal malkajgiri)": "Kukatpally",

    # Pothireddypally / Pothreddipalle (25 docs vs 7) → Pothreddipalle
    "pothireddypally": "Pothreddipalle",
    "pothaipalle": "Pothreddipalle",

    # Kollur(Tm) → Kollur
    "kollur(tm)": "Kollur",
    "kollur tm": "Kollur",

    # Thummaloor / Thummalur
    "thummalur": "Thummaloor",

    # Kowkur typos
    "kpwkoor": "Kowkur",
    "kawkoor": "Kowkur",

    # Nadargul / Nadergul (13 vs 8 docs) → Nadargul
    "nadergul": "Nadargul",

    # Turkapally typo
    "turkapply": "Turkapally",

    # Pudoor / Pudur
    "pudur": "Pudoor",

    # Old Alwal (with newline or double suffix)
    "old alwal\nalwal": "Old Alwal",
    "old alwal alwal": "Old Alwal",

    # Uppal Bhagat case variant
    "uppal bhagat": "Uppal Bhagat",

    # Bachpalle → Bachupally
    "bachpalle": "Bachupally",
    "bachpalle(v) bachuoally(m)": "Bachupally",
    "bachpalle(v) bachupally(m)": "Bachupally",

    # Bolaram Railway Station → Bolarum
    "bolaram railway station": "Bolarum",

    # Bonguluru / Bongloor
    "bonguluru": "Bongloor",

    # KPHB typo
    "khphb": "KPHB",

    # Nizampet typo
    "nijampet": "Nizampet",

    # Kompalle → Kompally
    "kompalle": "Kompally",

    # Ameenpur typo
    "ammenpur": "Ameenpur",

    # Chevalla → Chevella
    "chevalla": "Chevella",

    # Shaikpet typo
    "sheikpet": "Shaikpet",

    # Dulapally typo
    "doolapally": "Dulapally",

    # Turkayamjal typos
    "thurkaemjal": "Turkayamjal",
    "thrkayanjal": "Turkayamjal",

    # Munganoor typo
    "munganur": "Munganoor",

    # Sree Nagar → Sri Nagar
    "sree nagar": "Sri Nagar",

    # Peeranchuruvu typos
    "peeramchruvula": "Peeranchuruvu",
    "peeramchruvu orrgc": "Peeranchuruvu",
    "peeramchruvu": "Peeranchuruvu",

    # Gonenna Basthi / Gonemma Basthi
    "gonemma basthi": "Gonenna Basthi",

    # Gollur / Golluru
    "gollor": "Golluru",

    # Mamatha Nagr New → Mamatha Nagar
    "mamatha nagr new": "Mamatha Nagar",

    # Maridepally → Marredpally
    "maridepally": "Marredpally",

    # Kaziguda → Kachiguda
    "kaziguda": "Kachiguda",

    # Kucharam → Kacharam
    "kucharam": "Kacharam",

    # Yapral Old → Yapral
    "yapral old": "Yapral",

    # Bollaram (near Patancheru) — keep distinct from Bolarum (Secunderabad)
    # no merge needed

    # Peeramchruvu ORRGC → Peeranchuruvu
    "peeramchruvu-orrgc": "Peeranchuruvu",

    # Street / road entries that are NOT localities
    "street no 3": None,
    "street no. 3": None,
    "street no3": None,

    # Bollaram → Bolarum (same area, different transliteration)
    "bollaram": "Bolarum",

    # Bandlaguda Depot → Bandlaguda
    "bandlaguda depot": "Bandlaguda",

    # HMT Officers → Hmt Nagar (same HMT campus)
    "hmt officers": "Hmt Nagar",
    "hmt officers colony": "Hmt Nagar",
}

_NORM_ALIASES: dict[str, str | None] = {_norm_key(k): v for k, v in LOCALITY_ALIASES.items()}

# ─────────────────────────────────────────────────────────────────────────────
# LOCALITY → (mandals, villages) for regulatory data joining
# HMDA + Fire NOC have NULL locality field; matching requires village/mandal.
# Coverage: top ~60 Hyderabad localities by project count.
# Add new entries here as you encounter unknowns in get_regulatory_summary logs.
# ─────────────────────────────────────────────────────────────────────────────

LOCALITY_TO_AREAS: dict[str, dict[str, list[str]]] = {
    # ── Serilingampally mandal cluster (West Hyderabad / HITEC corridor) ────
    "Gachibowli":         {"mandal": ["Serilingampally"], "village": ["Gachibowli", "Kothaguda"]},
    "Kondapur":           {"mandal": ["Serilingampally"], "village": ["Kondapur"]},
    "Madhapur":           {"mandal": ["Serilingampally"], "village": ["Madhapur"]},
    "Hitech City":        {"mandal": ["Serilingampally"], "village": ["Madhapur", "Kondapur"]},
    "Miyapur":            {"mandal": ["Serilingampally", "Bachupally"], "village": ["Miyapur"]},
    "Nallagandla":        {"mandal": ["Serilingampally"], "village": ["Nallagandla", "Tellapur"]},
    "Kothaguda":          {"mandal": ["Serilingampally"], "village": ["Kothaguda"]},
    "Tellapur":           {"mandal": ["Ramachandrapuram"], "village": ["Tellapur"]},
    "Hafeezpet":          {"mandal": ["Serilingampally"], "village": ["Hafeezpet"]},
    "Kukatpally":         {"mandal": ["Kukatpally"],      "village": ["Kukatpally"]},
    "KPHB":               {"mandal": ["Kukatpally"],      "village": ["Kukatpally"]},
    "Moosapet":           {"mandal": ["Balanagar"],       "village": ["Moosapet"]},
    "Balanagar":          {"mandal": ["Balanagar"],       "village": ["Balanagar"]},
    "Kukatpally Housing Board": {"mandal":["Kukatpally"], "village":["Kukatpally"]},
    "Pragathi Nagar":     {"mandal": ["Bachupally"],      "village": ["Bachupally"]},
    "Bachupally":         {"mandal": ["Bachupally"],      "village": ["Bachupally"]},
    "Nizampet":           {"mandal": ["Bachupally"],      "village": ["Nizampet", "Bachupally"]},
    "Chanda Nagar":       {"mandal": ["Serilingampally"], "village": ["Chanda Nagar", "Lingampally"]},
    "Lingampally":        {"mandal": ["Serilingampally"], "village": ["Lingampally"]},
    "Madinaguda":         {"mandal": ["Serilingampally"], "village": ["Madinaguda"]},

    # ── Gandipet mandal (Financial District / Manikonda / Narsingi) ─────────
    "Financial District": {"mandal": ["Gandipet"], "village": ["Nanakramguda", "Gachibowli"]},
    "Nanakramguda":       {"mandal": ["Gandipet"], "village": ["Nanakramguda"]},
    "Manikonda":          {"mandal": ["Gandipet"], "village": ["Manikonda"]},
    "Narsingi":           {"mandal": ["Gandipet"], "village": ["Narsingi", "Puppalguda"]},
    "Puppalguda":         {"mandal": ["Gandipet"], "village": ["Puppalguda"]},
    "Gandipet":           {"mandal": ["Gandipet"], "village": ["Gandipet"]},
    "Khajaguda":          {"mandal": ["Gandipet"], "village": ["Khajaguda"]},
    "Kokapet":            {"mandal": ["Gandipet"], "village": ["Kokapet"]},

    # ── Shaikpet mandal (Jubilee Hills / Banjara Hills / Tolichowki) ────────
    "Jubilee Hills":      {"mandal": ["Shaikpet"], "village": ["Shaikpet"]},
    "Banjara Hills":      {"mandal": ["Shaikpet"], "village": ["Shaikpet"]},
    "Tolichowki":         {"mandal": ["Shaikpet"], "village": ["Shaikpet"]},
    "Film Nagar":         {"mandal": ["Shaikpet"], "village": ["Shaikpet"]},

    # ── Rajendranagar mandal ────────────────────────────────────────────────
    "Rajendranagar":      {"mandal": ["Rajendranagar"], "village": ["Rajendranagar"]},
    "Bandlaguda Jagir":   {"mandal": ["Rajendranagar"], "village": ["Bandlaguda"]},
    "Attapur":            {"mandal": ["Rajendranagar"], "village": ["Attapur"]},
    "Mailardevpally":     {"mandal": ["Rajendranagar"], "village": ["Mailardevpally"]},
    "Upparpalli":         {"mandal": ["Rajendranagar"], "village": ["Upparpalli"]},
    "Mehdipatnam":        {"mandal": ["Asifnagar"],     "village": ["Mehdipatnam"]},

    # ── Quthbullapur / north corridor ───────────────────────────────────────
    "Kompally":           {"mandal": ["Quthbullapur"], "village": ["Kompally", "Bahadurpally"]},
    "Bahadurpally":       {"mandal": ["Quthbullapur"], "village": ["Bahadurpally"]},
    "Gajularamaram":      {"mandal": ["Quthbullapur"], "village": ["Gajularamaram"]},
    "Suchitra":           {"mandal": ["Quthbullapur"], "village": ["Quthbullapur"]},
    "Jeedimetla":         {"mandal": ["Quthbullapur"], "village": ["Jeedimetla"]},
    "Petbasheerabad":     {"mandal": ["Quthbullapur"], "village": ["Petbasheerabad"]},
    "Suraram":            {"mandal": ["Quthbullapur"], "village": ["Suraram"]},

    # ── East corridor (Uppal / Boduppal / ECIL / A S Rao Nagar) ─────────────
    "Uppal":              {"mandal": ["Uppal"], "village": ["Uppal"]},
    "Boduppal":           {"mandal": ["Uppal"], "village": ["Boduppal"]},
    "Pocharam":           {"mandal": ["Ghatkesar"], "village": ["Pocharam"]},
    "Kushaiguda":         {"mandal": ["Kapra"], "village": ["Kushaiguda"]},
    "A S Rao Nagar":      {"mandal": ["Kapra"], "village": ["Kushaiguda"]},
    "Kapra":              {"mandal": ["Kapra"], "village": ["Kapra"]},
    "Nagaram":            {"mandal": ["Keesara"], "village": ["Nagaram"]},
    "Dammaiguda":         {"mandal": ["Keesara"], "village": ["Dammaiguda"]},
    "Malkajgiri":         {"mandal": ["Malkajgiri"], "village": ["Malkajgiri"]},
    "Alwal":              {"mandal": ["Alwal"], "village": ["Alwal"]},

    # ── South corridor (LB Nagar / Saroornagar / Hayathnagar) ───────────────
    "LB Nagar":           {"mandal": ["Saroornagar"], "village": ["Bandlaguda", "L.B. Nagar"]},
    "Nagole":             {"mandal": ["Saroornagar"], "village": ["Nagole"]},
    "Saroornagar":        {"mandal": ["Saroornagar"], "village": ["Saroornagar"]},
    "Mansoorabad":        {"mandal": ["Saroornagar"], "village": ["Mansoorabad"]},
    "Hastinapuram":       {"mandal": ["Saroornagar"], "village": ["Hastinapuram"]},
    "Karmanghat":         {"mandal": ["Saroornagar"], "village": ["Karmanghat"]},
    "Vanasthalipuram":    {"mandal": ["Hayathnagar"], "village": ["Vanasthalipuram"]},
    "Hayathnagar":        {"mandal": ["Hayathnagar"], "village": ["Hayathnagar"]},
    "Bandlaguda":         {"mandal": ["Saroornagar"], "village": ["Bandlaguda"]},

    # ── Southeast (Adibatla / Maheshwaram / airport corridor) ───────────────
    "Adibatla":           {"mandal": ["Ibrahimpatnam"], "village": ["Adibatla"]},
    "Tukkuguda":          {"mandal": ["Maheshwaram"],   "village": ["Tukkuguda"]},
    "Maheshwaram":        {"mandal": ["Maheshwaram"],   "village": ["Maheshwaram"]},
    "Shamshabad":         {"mandal": ["Shamshabad"],    "village": ["Shamshabad"]},

    # ── Central / old city ──────────────────────────────────────────────────
    "Begumpet":           {"mandal": ["Ameerpet"],   "village": ["Begumpet"]},
    "Ameerpet":           {"mandal": ["Ameerpet"],   "village": ["Ameerpet"]},
    "Punjagutta":         {"mandal": ["Khairatabad"],"village": ["Punjagutta"]},
    "Khairatabad":        {"mandal": ["Khairatabad"],"village": ["Khairatabad"]},
    "Somajiguda":         {"mandal": ["Khairatabad"],"village": ["Khairatabad"]},
    "Lakdikapul":         {"mandal": ["Khairatabad"],"village": ["Lakdikapul"]},
    "Himayat Nagar":      {"mandal": ["Himayatnagar"],"village":["Himayatnagar"]},
    "Abids":              {"mandal": ["Nampally"],   "village": ["Abids"]},
    "Nampally":           {"mandal": ["Nampally"],   "village": ["Nampally"]},
    "Koti":               {"mandal": ["Nampally"],   "village": ["Koti"]},
    "Tarnaka":            {"mandal": ["Tarnaka"],    "village": ["Tarnaka"]},

    # ── Misc edge cases ─────────────────────────────────────────────────────
    "Mokila":             {"mandal": ["Shankarpally"], "village": ["Mokila"]},
    "Shankarpally":       {"mandal": ["Shankarpally"], "village": ["Shankarpally"]},
    "Patancheru":         {"mandal": ["Patancheru"],   "village": ["Patancheru"]},
    "Ameenpur":           {"mandal": ["Ameenpur", "Patancheru"], "village": ["Ameenpur"]},
    "Beeramguda":         {"mandal": ["Ameenpur"],     "village": ["Beeramguda"]},

    # ── North-east / Sainikpuri / Yapral / Bowenpally cluster ────────────
    "Sainikpuri":         {"mandal": ["Secunderabad"],  "village": ["Sainikpuri"]},
    "Yapral":             {"mandal": ["Secunderabad"],  "village": ["Yapral"]},
    "Habsiguda":          {"mandal": ["Secunderabad"],  "village": ["Habsiguda"]},
    "Bowenpally":         {"mandal": ["Secunderabad"],  "village": ["Bowenpally"]},
    "West Marredpally":   {"mandal": ["Secunderabad"],  "village": ["West Marredpally"]},
    "East Marredpally":   {"mandal": ["Secunderabad"],  "village": ["East Marredpally"]},
    "Old Bowenpally":     {"mandal": ["Secunderabad"],  "village": ["Bowenpally"]},
    "Moula Ali":          {"mandal": ["Secunderabad"],  "village": ["Moula Ali"]},
    "Tirumalagiri":       {"mandal": ["Secunderabad"],  "village": ["Tirumalagiri"]},
    "Bolarum":            {"mandal": ["Alwal"],         "village": ["Bolarum"]},
    "Suraram":            {"mandal": ["Quthbullapur"],  "village": ["Suraram"]},

    # ── Old city / central ───────────────────────────────────────────────
    "Malakpet":           {"mandal": ["Malakpet"],      "village": ["Malakpet"]},
    "Old Malakpet":       {"mandal": ["Malakpet"],      "village": ["Malakpet"]},
    "Mallepally":         {"mandal": ["Asifnagar"],     "village": ["Mallepally"]},
    "Kachiguda":          {"mandal": ["Asifnagar"],     "village": ["Kachiguda"]},
    "Mushirabad":         {"mandal": ["Mushirabad"],    "village": ["Mushirabad"]},
    "Narayanguda":        {"mandal": ["Himayatnagar"],  "village": ["Narayanguda"]},
    "Nallakunta":         {"mandal": ["Himayatnagar"],  "village": ["Nallakunta"]},
    "New Nallakunta":     {"mandal": ["Himayatnagar"],  "village": ["Nallakunta"]},
    "Kavadiguda":         {"mandal": ["Mushirabad"],    "village": ["Kavadiguda"]},
    "Sanath Nagar":       {"mandal": ["Balanagar"],     "village": ["Sanath Nagar"]},

    # ── South-east extension ─────────────────────────────────────────────
    "Ghatkesar":          {"mandal": ["Ghatkesar"],     "village": ["Ghatkesar"]},
    "Pocharam":           {"mandal": ["Ghatkesar"],     "village": ["Pocharam"]},
    "Peerzadiguda":       {"mandal": ["Ghatkesar"],     "village": ["Peerzadiguda"]},
    "Nacharam":           {"mandal": ["Uppal"],         "village": ["Nacharam"]},
    "Ramanthapur":        {"mandal": ["Uppal"],         "village": ["Ramanthapur"]},
    "Mallapur":           {"mandal": ["Uppal"],         "village": ["Mallapur"]},
    "Chengicherla":       {"mandal": ["Uppal"],         "village": ["Chengicherla"]},
    "Habsiguda":          {"mandal": ["Uppal"],         "village": ["Habsiguda"]},
    "Uppal Kalan":        {"mandal": ["Uppal"],         "village": ["Uppal Kalan"]},
    "Thumukunta":         {"mandal": ["Ghatkesar"],     "village": ["Thumukunta"]},

    # ── North corridor additions ─────────────────────────────────────────
    "Dundigal":           {"mandal": ["Dundigal-Gandimaisamma"], "village": ["Dundigal"]},
    "Mallampet":          {"mandal": ["Quthbullapur"],  "village": ["Mallampet"]},
    "Dulapally":          {"mandal": ["Quthbullapur"],  "village": ["Dulapally"]},
    "Muthangi":           {"mandal": ["Patancheru"],    "village": ["Muthangi"]},
    "Isnapur":            {"mandal": ["Patancheru"],    "village": ["Isnapur"]},

    # ── South corridor additions ─────────────────────────────────────────
    "Kothur":             {"mandal": ["Maheshwaram"],   "village": ["Kothur"]},
    "Kandukur":           {"mandal": ["Rajendranagar"], "village": ["Kandukur"]},
    "Badangpet":          {"mandal": ["Saroornagar"],   "village": ["Badangpet"]},
    "Meerpet":            {"mandal": ["Saroornagar"],   "village": ["Meerpet"]},
    "Balapur":            {"mandal": ["Saroornagar"],   "village": ["Balapur"]},
    "Jillelguda":         {"mandal": ["Saroornagar"],   "village": ["Jillelguda"]},
    "Rampally":           {"mandal": ["Keesara"],       "village": ["Rampally"]},
    "Peeranchuruvu":      {"mandal": ["Rajendranagar"], "village": ["Peeranchuruvu"]},
}

_AREA_INDEX: dict[str, dict[str, list[str]]] = {
    k.lower(): v for k, v in LOCALITY_TO_AREAS.items()
}

def get_locality_areas(locality: str) -> dict[str, list[str]]:
    canonical = canonicalize_locality(locality) or (locality or "").strip()
    d = _AREA_INDEX.get(canonical.lower())
    if d:
        return {"mandal":  list(d.get("mandal")  or []),
                "village": list(d.get("village") or [])}
    return {"mandal":  [canonical] if canonical else [],
            "village": [canonical] if canonical else []}



# Junk patterns — discard any locality matching these
_JUNK_PATTERNS = re.compile(
    r"^(road|highway|na|null|none|unknown|hyderabad\s*city?)$", re.I
)
# Patterns that mark a locality string as junk (address fragment, landmark, etc.)
_REJECT_RE = re.compile(
    # Address fragments
    r"plot\s*no|sy\s*no|sy\s*bo|s\.y\.|survey\s*no|h\.?\s*no|hno|dno|d\s*no|"
    # Directional / landmark
    r"opposite|beside|near\s|behind|adjacent|"
    r"\bbank\b|bus\s*depot|hospital|school|college|police|"
    r"\babove\b|\badj\s*to\b|\badj\.?\s|"
    # Starts with "Opp" (abbreviation of Opposite) — address fragments
    r"^opp[\s\.\-]|^opp$|"
    # Starts with "Back Side" or "Back"
    r"^back\s*side|"
    # Avenue / Marg / Road names (not locality names)
    r"\bmarg\b|\bavenue\b|"
    # Generic standalone words that are never locality names
    r"^(house|bypass|telephone|revenue|information|open\s*land|grampanchayat|"
    r"muncipal\s*corporation|municipal\s*corporation|"
    r"cross\s*roads?|dynamics|deluxe|friends|"
    r"journalists|huda|awho|aphb|apau|"
    r"near|from|via|backside|water\s*tank)$|"
    # Shop/commercial names
    r"\bjewell?e?rs?\b|\bjewellery\b|\bbaker[sy]?\b|\bdhaba\b|"
    r"\bmall\b|\bexhibition\s*cent(er|re)\b|\bshowroom\b|"
    r"\bbook\s*store\b|\bsuper\s*market\b|"
    # Company/project brand names
    r"\bdynamics\b|\bventure\b.*\b(klr|nlr)\b|"
    # Institutional (not locality)
    r"\boffice\b.*\bside\b|\bback\s*side\b|\bgvk\s+one\b|"
    r"\bcampus\b|\bengineering\s*college\b|"
    # Codes / numbers
    r"\bnh[-\s]?\d|\bnumber\b|\bcircle\b|\bward\b|\bzone\b|\bfloor\b|"
    r"\bblock\s*no\b|\bblock-?[a-z]\b|\bpillar\s*no\b|"
    r"\bphase[-\s]?(i+|iv|v|vi+|ix)\b|"
    r"enclave,|\btowers?$|"
    r"\bhig\b|\blig\b|\bmig\b|\bteachers?\b\s*colony\s*circle|"
    r"\bunder\s+(ghmc|peerzadiguda|manikonda|nizampet|gundlapochampally)|"
    # Starts with junk
    r"^\d|^[a-z]\d|^[a-z],|^level\s+\d+$|^block\s+[a-z\d]$|"
    r"\-\d+$|"
    # Project-only suffix words
    r"\b(estate|estates|bellezza|paradise|palace|"
    r"bloom|blossom|bellagio|bellevue|signature|symphony|crown|"
    r"residency|apartment|apartments|building|society|"
    r"co[-\s]?op\b|co[-\s]?operative|housing\s*society|"
    r"international\s*airport|survey\s*of\s*india|"
    r"showroom|temple\s*back|opp\.|opp\s|"
    r"agency|advocates?|gas\s*agency|petrol\s*pump|"
    r"agriculture|agricultural|agricluture|"
    r"villa\s+grand|villa\s+royale)\b\s*[,]?\s*$"
    r"shelters?|nilayam|sadan|"           # society/project suffixes
    r"nav\s*khalsa\s*[-]?(cir|cr)\s*\d+|"     # admin code tails like "Nawkhalsa-Cir11"
    r"\bnav\s*calsa\b",
    re.IGNORECASE
)

_STRIP_SUFFIXES = (

    "huda residential complex", "residential complex", "residential layout",

    "village", "mandal",

    "phase 1", "phase 2", "phase 3", "phase 4", "phase 5", "phase 6",

    "hyderabad", "telangana", "ranga reddy", "rangareddy",

    "road", "nav khalsa", "navkhalsa",
    "nav calsa",
    "shelters",
    "main",

)

_GENERIC_SUFFIX_ALLOWLIST: set[str] = {
    # Hills
    "banjara hills", "jubilee hills", "lanco hills", "red hills",
    "kavuri hills", "dollar hills", "kakatiya hills", "green hills",
    "shilpa hills", "madhava hills", "krishnaja hills", "snehitha hills",
    "prashanthi hills", "nandagiri hills",
    # Park / Gardens
    "temple park", "central park",
    # Layout
    "golden mile layout",
    # Colony — real named neighbourhoods, not generic suffix
    "sri nagar colony", "srinagar colony",
    "vijayanagar colony", "suraram colony", "hanuman nagar colony",
    "sanjeeva reddy nagar", "adarsh nagar", "madhura nagar",
    "indira nagar", "moti nagar", "kalyan nagar", "ram nagar",
    "maruthi nagar", "sai nagar", "anjaneya nagar",
    # Enclave (known localities)
    "kancha gachibowli",
}

_SUSPECT_GENERIC_RE = re.compile(
    r"\b(hills?|heights?|park|gardens?|villa|villas|enclave|residency|"
    r"layout|estates?|hamlet|meadows?|valley|colony|complex)\s*$",
    re.IGNORECASE,
)

def _strip_trailing(s: str) -> str:

    changed = True

    while changed:

        changed = False

        lo = s.lower().rstrip(" ,.-")

        for suf in _STRIP_SUFFIXES:

            if lo.endswith(suf):

                s = s[: len(s) - len(suf)].rstrip(" ,.-")

                changed = True

                break

    return s

# 5. _JUNK_PATTERNS (keep the existing one if you already have it)

_JUNK_PATTERNS = re.compile(

    r"^(road|highway|na|null|none|unknown|hyderabad\s*city?)$", re.I

)

# 6. canonicalize_locality using _NORM_ALIASES (replaces all three lookup points)

def canonicalize_locality(name: str) -> str | None:

    if not name or not name.strip():

        return None

    stripped = name.strip().rstrip(" ,.-")

    if _norm_key(stripped) in _NORM_ALIASES:

        return _NORM_ALIASES[_norm_key(stripped)]

    if len(stripped) > 50:

        return None

    if _REJECT_RE.search(stripped):

        return None

    if stripped.count(",") >= 2:

        return None

    if "," in stripped:
        parts = [p.strip() for p in stripped.split(",")]
        # Try each part — first one that canonicalizes wins
        for p in parts:
            if not p or len(p) < 3:
                continue
            if _norm_key(p) in _NORM_ALIASES:
                v = _NORM_ALIASES[_norm_key(p)]
                if v: return v
        # Fallback: keep only first segment if it's not junk-flagged
        if parts[0] and not _REJECT_RE.search(parts[0]):
            stripped = parts[0]
        else:
            return None

    stripped = _strip_trailing(stripped)

    if not stripped or len(stripped) < 3:

        return None

    if _norm_key(stripped) in _NORM_ALIASES:

        return _NORM_ALIASES[_norm_key(stripped)]

    if stripped == stripped.upper() and len(stripped) > 2:

        stripped = stripped.title()

        if _norm_key(stripped) in _NORM_ALIASES:

            return _NORM_ALIASES[_norm_key(stripped)]

    if stripped == stripped.lower() and len(stripped) > 2 and not any(c.isdigit() for c in stripped):

        stripped = stripped.title()

        if _norm_key(stripped) in _NORM_ALIASES:

            return _NORM_ALIASES[_norm_key(stripped)]

    if _JUNK_PATTERNS.match(stripped):

        return None

    if _SUSPECT_GENERIC_RE.search(stripped):
        if stripped.lower() not in _GENERIC_SUFFIX_ALLOWLIST:
            return None    
 
    return stripped


log = logging.getLogger("mongo_supply")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/reddit")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "reddit")

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000, tlsCAFile=certifi.where())
    return _client

def _db():
    client = _get_client()
    try:
        default_db = client.get_default_database()
        if default_db is not None:
            return default_db
    except Exception:
        pass
    return client[MONGO_DB_NAME]

def _re():
    return _db()

def _ig():
    return _db()


# ─────────────────────────────────────────────────────────────────────────────
# SCALAR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _si(v):
    try:   return int(str(v).replace(",", "").replace(".0",""))
    except: return None

def _sf(v):
    try:   return float(v)
    except: return None

def _get_loc(doc):
    """Extract lat/lng from insightforge location field. Handles {lat,lng} dict."""
    loc = doc.get("location") or {}
    if isinstance(loc, dict):
        lat = _sf(loc.get("lat") or loc.get("latitude"))
        lng = _sf(loc.get("lng") or loc.get("longitude") or loc.get("lon"))
        return lat, lng
    return None, None


def _get_loc_any(doc):
    """Extract lat/lng from location {lat,lng} dict or GeoJSON geometry.
    Handles Point [lng,lat] and Polygon (uses first exterior ring point as centroid approx).
    """
    lat, lng = _get_loc(doc)
    if lat and lng:
        return lat, lng
    geo = doc.get("geometry") or {}
    if not isinstance(geo, dict):
        return None, None
    geo_type = geo.get("type", "")
    coords = geo.get("coordinates")
    if not coords:
        return None, None
    try:
        if geo_type == "Point":
            # [lng, lat]
            return _sf(coords[1]), _sf(coords[0])
        elif geo_type in ("Polygon", "MultiPolygon"):
            # Compute centroid of exterior ring
            ring = coords[0] if geo_type == "Polygon" else coords[0][0]
            if not ring:
                return None, None
            lngs = [c[0] for c in ring if isinstance(c, (list, tuple)) and len(c) >= 2]
            lats = [c[1] for c in ring if isinstance(c, (list, tuple)) and len(c) >= 2]
            if lats and lngs:
                return sum(lats)/len(lats), sum(lngs)/len(lngs)
        elif geo_type in ("LineString", "MultiLineString"):
            pts = coords if geo_type == "LineString" else coords[0]
            mid = pts[len(pts)//2]
            return _sf(mid[1]), _sf(mid[0])
    except Exception:
        pass
    return None, None

def _haversine(lat1, lng1, lat2, lng2):
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(max(0, a)))

def _bbox(lat, lng, radius_km):
    dlat = radius_km / 111.0
    dlng = radius_km / (111.0 * math.cos(math.radians(lat)))
    return lat - dlat, lat + dlat, lng - dlng, lng + dlng

def _fmt_dist(m):
    if m < 1000: return f"{round(m)}m"
    return f"{round(m/1000, 1)} km"


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHTFORGE QUERIES
# ─────────────────────────────────────────────────────────────────────────────

def get_rera_absorption(rera_number: str) -> dict:
    """
    Source: insightforge.rera_scraped_data
    Match field: cert_no (RERA registration number)
    Returns: total_apartments, total_booked_apartments, construction_progress
    """
    if not rera_number:
        return {}
    try:
        doc = _ig()["rera_scraped_data"].find_one(
            {"cert_no": rera_number},
            {"total_apartments":1, "total_booked_apartments":1,
             "construction_progress":1, "project_status":1,
             "has_litigations":1, "is_msb_or_highrise":1,
             "total_floors":1, "buildings_count":1, "_id":0}
        )
        if not doc:
            return {}
        total  = _si(doc.get("total_apartments")) or 0
        booked = _si(doc.get("total_booked_apartments")) or 0
        avail  = max(0, total - booked) if total else None
        pct    = round(booked / total * 100, 1) if (total and booked is not None) else None
        progress = doc.get("construction_progress") or []
        latest_progress = None
        if isinstance(progress, list) and progress:
            latest_progress = progress[-1].get("progress_pct") if isinstance(progress[-1], dict) else None
        return {
            "total_apartments":  total,
            "booked_apartments": booked,
            "available_units":   avail,
            "absorption_pct":    pct,
            "construction_progress_pct": latest_progress,
            "project_status":    doc.get("project_status"),
            "has_litigations":   doc.get("has_litigations", False),
            "is_highrise":       doc.get("is_msb_or_highrise", False),
            "total_floors":      _si(doc.get("total_floors")),
            "buildings_count":   _si(doc.get("buildings_count")),
        }
    except Exception as e:
        log.warning(f"rera_absorption({rera_number}): {e}")
        return {}


def get_nearby_pois(lat: float, lng: float, radius_km: float = 3.0, city: str = "Hyderabad") -> dict:
    """
    Source: insightforge.points_of_interest (91,595 docs)
    Returns top-N nearest POIs per category within radius.
    Keys are normalized to match get_infra_summary() category names.
    Coordinates stored in geometry.coordinates (GeoJSON), not location field.
    """
    if not lat or not lng:
        return {}
    try:
        s_lat, n_lat, w_lng, e_lng = _bbox(lat, lng, radius_km)
        # Use geometry.coordinates for spatial filter (not location.lat/lng)
        bbox_q = {
            "geometry.coordinates.0": {"$gte": w_lng, "$lte": e_lng},
            "geometry.coordinates.1": {"$gte": s_lat, "$lte": n_lat},
        }
        raw = list(_ig()["points_of_interest"].find(
            {
                "city": re.compile(re.escape(city or "Hyderabad"), re.I),
                **bbox_q,
            },
            {"poi_type":1, "sub_type":1, "name":1, "location":1,
             "rating":1, "address":1, "geometry":1, "_id":0},
            limit=1000
        ))

        # Normalize poi_type → infra_summary key mapping
        _POI_KEY_MAP = {
            "metro_station": "metro", "metro": "metro",
            "hospital": "hospital", "clinic": "hospital",
            "school": "school",
            "mall": "mall", "shopping_mall": "mall",
            "it_company": "it_company", "tech_park": "it_company",
            "university": "university", "college": "university",
            "junior_college": "junior_college",
            "park": "park", "garden": "park",
            "bus_stop": "bus_stop",
            "industry": "industry", "factory": "industry",
            "bank": "bank", "atm": "bank",
            "cafe": "cafe", "restaurant": "restaurant",
            "temple": "temple", "mosque": "mosque", "church": "church",
            "petrol_pump": "petrol_pump",
        }
        # Skip low-quality entries
        _SKIP_NAMES = {"unnamed", "unknown", ""}

        by_type: dict = {}
        for doc in raw:
            p_lat, p_lng = _get_loc_any(doc)
            if not p_lat or not p_lng:
                continue
            dist = _haversine(lat, lng, p_lat, p_lng)
            if dist > radius_km * 1000:
                continue
            raw_type = (doc.get("poi_type") or "other").lower()
            key = _POI_KEY_MAP.get(raw_type, raw_type)
            name = (doc.get("name") or "").strip()
            if name.lower() in _SKIP_NAMES:
                continue
            item = {
                "name":    name,
                "address": doc.get("address",""),
                "dist_m":  round(dist),
                "dist":    _fmt_dist(dist),
                "rating":  doc.get("rating"),
                "lat":     p_lat,
                "lng":     p_lng,
            }
            if key not in by_type:
                by_type[key] = []
            by_type[key].append(item)

        result = {}
        for ptype, items in by_type.items():
            items.sort(key=lambda x: x["dist_m"])
            result[ptype] = items[:20]
        return result
    except Exception as e:
        log.warning(f"get_nearby_pois: {e}")
        return {}


def get_locality_centroid(locality: str) -> tuple[float | None, float | None]:
    """Fallback lat/lng from buyer_persona.localities when project coords are missing."""
    if not locality:
        return None, None
    try:
        canon = canonicalize_locality(locality) or locality.strip()
        loc_re = re.compile(r"^\s*" + re.escape(canon) + r"\s*$", re.I)
        doc = _bp()["localities"].find_one({"name": loc_re}, {"centroid": 1, "_id": 0})
        c = (doc or {}).get("centroid") or {}
        return _sf(c.get("lat")), _sf(c.get("lng"))
    except Exception as e:
        log.warning(f"get_locality_centroid({locality}): {e}")
        return None, None


def get_infra_summary(lat: float, lng: float, radius_km: float = 5.0, city: str = "Hyderabad") -> dict:
    """
    Source: insightforge individual collections (metro_stations, hospitals, schools, malls, it_companies, lakes)
    Returns nearest item per category + count within radius.
    Coordinates stored as GeoJSON: geometry.coordinates = [lng, lat]
    """
    if not lat or not lng:
        return {}
    s_lat, n_lat, w_lng, e_lng = _bbox(lat, lng, radius_km)
    # GeoJSON Point coordinates: [lng, lat]
    bbox_q = {
        "geometry.coordinates.0": {"$gte": w_lng, "$lte": e_lng},
        "geometry.coordinates.1": {"$gte": s_lat, "$lte": n_lat},
    }
    hyd_q  = {"city": re.compile(re.escape(city or "Hyderabad"), re.I)}

    result = {}
    specs = [
        ("metro",          "metro_stations",  {"name":1,"line_name":1,"location":1,"geometry":1},              "line_name"),
        ("hospital",       "hospitals",       {"name":1,"hospital_type":1,"beds":1,"location":1,"geometry":1}, "hospital_type"),
        ("school",         "schools",         {"name":1,"school_type":1,"board":1,"location":1,"geometry":1},  "board"),
        ("mall",           "malls",           {"name":1,"mall_status":1,"area_sqft":1,"location":1,"geometry":1}, "mall_status"),
        ("it_company",     "it_companies",    {"name":1,"tech_park":1,"employees":1,"sector":1,"location":1,"geometry":1}, "tech_park"),
        ("university",     "universities",    {"name":1,"university_type":1,"naac_grade":1,"location":1,"geometry":1}, "university_type"),
        ("junior_college", "junior_colleges", {"name":1,"location":1,"geometry":1}, None),
        ("park",           "parks",           {"name":1,"park_type":1,"location":1,"geometry":1}, "park_type"),
        ("bus_stop",       "bus_stops",       {"name":1,"location":1,"geometry":1},               None),
        ("industry",       "industries",      {"name":1,"sector":1,"employees":1,"location":1,"geometry":1}, "sector"),
        ("bank",           "banks",           {"name":1,"location":1,"geometry":1},               None),
    ]
    for key, col, proj, extra_field in specs:
        try:
            candidates = list(_ig()[col].find({**hyd_q, **bbox_q}, proj, limit=200))
            if not candidates:
                candidates = list(_ig()[col].find(hyd_q, proj, limit=300))
            nearest, count = None, 0
            nearby_list = []
            min_dist = float("inf")
            for doc in candidates:
                p_lat, p_lng = _get_loc_any(doc)
                if not p_lat or not p_lng: continue
                d = _haversine(lat, lng, p_lat, p_lng)
                if d <= radius_km * 1000:
                    count += 1
                    item = {"name": doc.get("name",""), "dist": _fmt_dist(d),
                            "dist_m": round(d),
                            "extra": doc.get(extra_field,"") if extra_field else "",
                            "lat": p_lat, "lng": p_lng}
                    nearby_list.append(item)
                    if d < min_dist:
                        min_dist = d
                        nearest = item
            nearby_list.sort(key=lambda x: x["dist_m"])
            result[key] = {
                "nearest": nearest,
                "count_within_radius": count,
                "radius_km": radius_km,
                "nearby": nearby_list[:20],   # top 20 for map markers
            }
        except Exception as e:
            log.warning(f"infra_summary/{key}: {e}")

    # Lakes — check if in buffer zone; use _get_loc_any for geometry-based coords
    try:
        lakes = list(_ig()["lakes"].find(
            {**hyd_q},
            {"name":1,"buffer_m":1,"ftl_level":1,"location":1,"area_sq_m":1,
             "is_protected":1,"geometry":1,"district":1},
            limit=500
        ))
        nearby_lakes = []
        for doc in lakes:
            p_lat, p_lng = _get_loc_any(doc)  # handles Polygon centroid
            if not p_lat or not p_lng: continue
            d = _haversine(lat, lng, p_lat, p_lng)
            buffer = _sf(doc.get("buffer_m")) or 30
            if d <= radius_km * 1000:
                nearby_lakes.append({
                    "name": doc.get("name",""),
                    "dist": _fmt_dist(d),
                    "dist_m": round(d),
                    "in_buffer_zone": d <= buffer,
                    "buffer_m": buffer,
                    "area_sq_m": doc.get("area_sq_m"),
                    "is_protected": doc.get("is_protected", False),
                    "lat": p_lat, "lng": p_lng,
                })
        nearby_lakes.sort(key=lambda x: x["dist_m"])
        result["lakes"] = nearby_lakes[:8]
    except Exception as e:
        log.warning(f"infra_summary/lakes: {e}")

    return result


def get_height_restrictions(lat: float, lng: float) -> list:
    """
    Source: insightforge.airport_height_restriction_zones (58 docs)
    Returns all zones (frontend renders polygon overlays).
    No spatial index needed — only 58 docs.
    """
    try:
        zones = []
        for doc in _ig()["airport_height_restriction_zones"].find(
            {"city": re.compile("hyderabad", re.I)},
            {"airport_name":1,"zone_type":1,"max_height_m":1,
             "max_height_description":1,"color_code":1,"geometry":1,"_id":0}
        ):
            zones.append({
                "airport":     doc.get("airport_name",""),
                "zone_type":   doc.get("zone_type",""),
                "max_height_m":doc.get("max_height_m"),
                "description": doc.get("max_height_description",""),
                "color":       doc.get("color_code","#888888"),
                "geometry":    doc.get("geometry"),
            })
        return zones
    except Exception as e:
        log.warning(f"get_height_restrictions: {e}")
        return []


def get_approval_stats(locality: str = "") -> dict:
    """
    Source: insightforge.approval_projects (location data) + approval_project_matches (stages).
    approval_projects has district/mandal/location; approval_project_matches has stage + status.
    Also returns market-wide totals when no locality match found.
    Stage types: aai_noc, environmental_clearance, fire_noc, municipal_permission, rera
    """
    try:
        def _aggregate_matches(project_ids: list | None) -> dict:
            """Aggregate stage counts from approval_project_matches."""
            q = {} if project_ids is None else {"project_id": {"$in": project_ids}}
            pipeline = [
                {"$match": q},
                {"$group": {
                    "_id": {"stage": "$stage_type", "status": "$status"},
                    "count": {"$sum": 1}
                }},
            ]
            stats: dict[str, dict] = defaultdict(lambda: {"approved":0,"pending":0,"not_started":0,"rejected":0,"total":0})
            for r in _ig()["approval_project_matches"].aggregate(pipeline):
                stage  = r["_id"]["stage"]
                status = r["_id"]["status"]
                cnt    = r["count"]
                stats[stage]["total"] += cnt
                if status == "approved":                     stats[stage]["approved"]     += cnt
                elif status in ("pending", "in_progress"):  stats[stage]["pending"]      += cnt
                elif status == "not_started":                stats[stage]["not_started"] += cnt
                elif status == "rejected":                   stats[stage]["rejected"]     += cnt
            return stats

        # Market-wide totals (all project_ids in approval_project_matches)
        market_stats = _aggregate_matches(None)

        # Try locality-specific via approval_projects — use LOCALITY_TO_AREAS for mandal/village expansion
        locality_project_ids: list = []
        if locality:
            canonical = canonicalize_locality(locality) or locality.strip()
            areas = get_locality_areas(canonical)
            villages = areas.get("village", [])
            mandals  = areas.get("mandal", [])

            # Build name set: canonical + aliases + mandal/village names from LOCALITY_TO_AREAS
            aliases = {canonical, locality.strip()}
            for raw, canon in LOCALITY_ALIASES.items():
                if canon and canon.lower() == canonical.lower():
                    aliases.add(raw); aliases.add(raw.title())
            # Also include the actual village/mandal names since approval_projects uses those
            aliases.update(villages)
            aliases.update(mandals)
            alias_res = [re.compile(r"^\s*" + re.escape(a) + r"\s*$", re.IGNORECASE) for a in aliases if a]

            # Match against approval_projects district/mandal/village fields
            loc_projects = list(_ig()["approval_projects"].find(
                {"$or": [
                    {"district": {"$in": alias_res}},
                    {"mandal":   {"$in": alias_res}},
                    {"village":  {"$in": alias_res}},
                ]},
                {"id": 1, "_id": 0}
            ))
            locality_project_ids = [d["id"] for d in loc_projects if d.get("id")]

        if locality_project_ids:
            loc_stats = _aggregate_matches(locality_project_ids)
            n_projects = len(locality_project_ids)
        else:
            loc_stats  = market_stats
            n_projects = _ig()["approval_projects"].count_documents({})

        def _fmt(stats: dict, key: str) -> dict:
            d = dict(stats.get(key, {}))
            # Compute 'missing' = projects with not_started
            return d

        return {
            "total_projects":          n_projects,
            "is_locality_specific":    bool(locality_project_ids),
            "municipal_permission":    _fmt(loc_stats, "municipal_permission"),
            "rera":                    _fmt(loc_stats, "rera"),
            "fire_noc":                _fmt(loc_stats, "fire_noc"),
            "aai_noc":                 _fmt(loc_stats, "aai_noc"),
            "environmental_clearance": _fmt(loc_stats, "environmental_clearance"),
            # Market-wide totals always included for overview KPIs
            "market_wide": {
                "municipal_permission":    dict(market_stats.get("municipal_permission", {})),
                "rera":                    dict(market_stats.get("rera", {})),
                "fire_noc":               dict(market_stats.get("fire_noc", {})),
                "aai_noc":                dict(market_stats.get("aai_noc", {})),
                "environmental_clearance": dict(market_stats.get("environmental_clearance", {})),
            },
        }
    except Exception as e:
        log.warning(f"get_approval_stats({locality}): {e}")
        return {}


def get_regulatory_summary(locality: str, city: str = "Hyderabad") -> dict:
    """
    Sources: insightforge.{hmda_all_records, fire_noc_r4, approval_project_matches}
    Joins via LOCALITY_TO_AREAS expansion (HMDA/NOC have NULL locality field).
    Strategy:
      HMDA       → match village OR mandal (village is cleaner, ~99% populated)
      fire_noc   → match mandal preferentially (village field is heavily corrupted)
      approvals  → via approval_project_matches (existing get_approval_stats logic)
    """
    result = {}
    areas = get_locality_areas(locality)
    villages = areas["village"]
    mandals  = areas["mandal"]

    def _re_in(names: list[str]) -> dict:
        if not names: return None
        return {"$in": [re.compile(r"^\s*" + re.escape(n) + r"\s*$", re.IGNORECASE) for n in names]}

    village_clause = _re_in(villages)
    mandal_clause  = _re_in(mandals)

    # ── HMDA: match (village OR mandal) ─────────────────────────────────────
    try:
        hmda_or = []
        if village_clause: hmda_or.append({"village": village_clause})
        if mandal_clause:  hmda_or.append({"mandal":  mandal_clause})
        hmda_q = {"$or": hmda_or} if hmda_or else None

        if hmda_q:
            total    = _ig()["hmda_all_records"].count_documents(hmda_q)
            approved = _ig()["hmda_all_records"].count_documents(
                {**hmda_q, "status": re.compile("approved|sanctioned|issued|granted", re.I)})
            pending  = _ig()["hmda_all_records"].count_documents(
                {**hmda_q, "status": re.compile("pending|process|under", re.I)})
            rejected = _ig()["hmda_all_records"].count_documents(
                {**hmda_q, "status": re.compile("rejected|denied|cancelled", re.I)})
        else:
            total = approved = pending = rejected = 0

        result["hmda"] = {
            "total": total, "approved": approved, "pending": pending,
            "rejected": rejected,
            "approval_rate_pct": round(approved/total*100, 1) if total else None,
            "matched_villages":  villages,
            "matched_mandals":   mandals,
        }
    except Exception as e:
        log.warning(f"hmda_summary({locality}): {e}")
        result["hmda"] = {}

    # ── Fire NOC: mandal-only match (village field is too dirty) ────────────
    try:
        if mandal_clause:
            noc_q = {"mandal": mandal_clause}
            total       = _ig()["fire_noc_r4"].count_documents(noc_q)
            issued      = _ig()["fire_noc_r4"].count_documents(
                {**noc_q, "noc_status": re.compile("approved|final|granted", re.I)})
            provisional = _ig()["fire_noc_r4"].count_documents(
                {**noc_q, "noc_status": re.compile("provisional", re.I)})
            rejected    = _ig()["fire_noc_r4"].count_documents(
                {**noc_q, "noc_status": re.compile("rejected|denied", re.I)})
            result["fire_noc"] = {
                "total": total, "issued": issued, "provisional": provisional,
                "rejected": rejected,
                "pending": max(0, total - issued - provisional - rejected),
                "matched_mandals": mandals,
            }
        else:
            result["fire_noc"] = {}
    except Exception as e:
        log.warning(f"fire_noc_summary({locality}): {e}")
        result["fire_noc"] = {}

    # ── Project-level approval stages (unchanged) ───────────────────────────
    try:
        result["approval_matches"] = get_approval_stats(locality)
    except Exception as e:
        log.warning(f"approval_matches({locality}): {e}")
        result["approval_matches"] = {}

    return result

def get_buyer_insights(locality: str = None) -> dict:
    """
    Sources: insightforge.customer_lifestyle_survey (262), customer_property_survey (250)
    Returns aggregated buyer profile for nearby localities.
    """
    try:
        # Lifestyle survey
        ls_docs = list(_ig()["customer_lifestyle_survey"].find(
            {}, {"age":1,"household_income":1,"occupation":1,"property_purpose":1,
                 "primary_commute_mode":1,"living_environment_preference":1,
                 "current_location":1,"household_size":1,"marital_status":1}
        ))
        # Property survey
        ps_docs = list(_ig()["customer_property_survey"].find(
            {}, {"age":1,"household_income":1,"occupation":1,"current_home_config":1,
                 "preferred_property_config":1,"preferred_builtup_area":1,
                 "comfortable_emi_range":1,"current_location":1,"household_size":1}
        ))

        # Income distribution from lifestyle survey
        incomes = [d.get("household_income") for d in ls_docs if d.get("household_income")]
        income_dist = defaultdict(int)
        for i in incomes: income_dist[str(i)] += 1

        # Preferred config from property survey
        configs = [d.get("preferred_property_config") for d in ps_docs if d.get("preferred_property_config")]
        config_dist = defaultdict(int)
        for c in configs: config_dist[str(c)] += 1

        # Age distribution
        ages = [_si(d.get("age")) for d in ls_docs if d.get("age")]
        ages = [a for a in ages if a and 18 <= a <= 80]
        avg_age = round(sum(ages)/len(ages)) if ages else None

        # Purpose
        purposes = [d.get("property_purpose") for d in ls_docs if d.get("property_purpose")]
        purpose_dist = defaultdict(int)
        for p in purposes: purpose_dist[str(p)] += 1

        # EMI range
        emis = [d.get("comfortable_emi_range") for d in ps_docs if d.get("comfortable_emi_range")]
        emi_dist = defaultdict(int)
        for e in emis: emi_dist[str(e)] += 1

        return {
            "sample_size": len(ls_docs) + len(ps_docs),
            "avg_age": avg_age,
            "income_distribution": dict(sorted(income_dist.items(), key=lambda x: -x[1])[:6]),
            "config_preference": dict(sorted(config_dist.items(), key=lambda x: -x[1])[:5]),
            "purchase_purpose": dict(sorted(purpose_dist.items(), key=lambda x: -x[1])[:4]),
            "emi_preference": dict(sorted(emi_dist.items(), key=lambda x: -x[1])[:5]),
        }
    except Exception as e:
        log.warning(f"buyer_insights: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# PROJECTS_MASTER NORMALIZER  (corrected field paths)
# ─────────────────────────────────────────────────────────────────────────────

def _norm_master(doc: dict, rera_cache: dict = None) -> dict:
    """
    Map a projects_master document to the frontend shape.
    All field paths verified against actual projects_master schema.
    """
    iden    = doc.get("identity")        or {}
    loc     = doc.get("location")        or {}
    pricing = doc.get("pricing")         or {}
    rera    = doc.get("rera")            or {}
    specs   = doc.get("specifications")  or {}
    cfg     = doc.get("configurations")  or {}
    reviews = doc.get("reviews")         or {}
    bldg    = doc.get("building")        or {}
    meta    = doc.get("_meta")           or {}
    lo      = doc.get("locality_overview") or {}
    # ── CORRECT: amenities lives at doc.amenities.all (not doc.facilities) ──
    amen_doc = doc.get("amenities")      or {}
    media    = doc.get("media")          or {}

    # ── BHK — use pre-normalized bhk_list from configurations ────────────────
    bhk_list = cfg.get("bhk_list") or []

    # Also read units[].apartment_type as a primary BHK source
    for unit in (doc.get("units") or []):
        apt_type = (unit.get("apartment_type") or "").strip()
        if apt_type:
            # Normalise: "3.5 BHK", "3.5BHK", "3BHK" → "3.5 BHK" / "3 BHK"
            mu = re.search(r'(\d+(?:\.\d+)?)\s*BHK', apt_type, re.I)
            if mu:
                val = f"{mu.group(1)} BHK"
                if val not in bhk_list:
                    bhk_list.append(val)

    # Fallback: derive from config cards (decimal-aware regex)
    if not bhk_list:
        for card in (cfg.get("cards") or []):
            lbl = card.get("label") or ""
            m = re.search(r'(\d+(?:\.\d+)?)\s*BHK', lbl.upper())
            if m:
                val = f"{m.group(1)} BHK"
                if val not in bhk_list:
                    bhk_list.append(val)

    # ── Pricing ───────────────────────────────────────────────────────────────
    # ponytail: ₹50Cr upper bound filters out ₹60,000,000,000 sentinel values in DB
    _PRICE_MAX_VALID = 500_000_000   # ₹50 Cr
    price_min = _si(pricing.get("min_price")) or 0
    price_max = _si(pricing.get("max_price")) or 0
    if price_min > _PRICE_MAX_VALID: price_min = 0
    if price_max > _PRICE_MAX_VALID: price_max = 0
    cards = cfg.get("cards") or []
    if cards:
        card_mins = [_si(c.get("price_min")) or 0 for c in cards]
        card_maxs = [_si(c.get("price_max")) or 0 for c in cards]
        # Only use card prices if they are non-zero and within valid range
        valid_mins = [v for v in card_mins if v and 100000 < v <= _PRICE_MAX_VALID]
        valid_maxs = [v for v in card_maxs if v and 100000 < v <= _PRICE_MAX_VALID]
        if not price_min and valid_mins:
            price_min = min(valid_mins)
        if not price_max and valid_maxs:
            price_max = max(valid_maxs)

    # ── PSF — must be computed before price_on_request (which references psf) ──
    # ponytail: tier 1 = real observed value; tiers 2+3 = estimates, flagged separately
    psf = 0
    psf_is_estimated = False
    raw_psf = pricing.get("price_per_sqft")
    if raw_psf:
        psf = _si(raw_psf) or 0
    if not psf:
        nums = re.findall(r"[\d,]+", str(pricing.get("sqft_range_str","")).replace("₹",""))
        vals = [int(s.replace(",","")) for s in nums if s.replace(",","").isdigit()]
        if vals:
            psf = int(sum(vals)/len(vals))
            psf_is_estimated = True
    if not psf:
        area_min = _si(bldg.get("min_size_sqft")) or 0
        area_max = _si(bldg.get("max_size_sqft")) or 0
        avg_area = (area_min + area_max)/2 if area_max else area_min
        if avg_area and price_min:
            psf = round(price_min / avg_area)
            psf_is_estimated = True

    # ── price_on_request flag (uses psf, must come after psf block above) ────
    price_on_request = False
    if cards:
        price_on_request = (
            not price_min and
            not price_max and
            not psf and
            any(
                (c.get("price_label") or "").lower() in ("price on request", "por", "")
                for c in cards
            )
        )

    # ── Area ──────────────────────────────────────────────────────────────────
    area_min = area_max = 0
    if cards:
        card_amins = [_si(c.get("area_min")) or 0 for c in cards]
        card_amaxs = [_si(c.get("area_max")) or 0 for c in cards]
        area_min = min((v for v in card_amins if v), default=0)
        area_max = max((v for v in card_amaxs if v), default=0)
    if not area_min: area_min = _si(bldg.get("min_size_sqft")) or 0
    if not area_max: area_max = _si(bldg.get("max_size_sqft")) or 0

    # ── Coordinates ───────────────────────────────────────────────────────────
    lat = _sf(loc.get("lat"))
    lng = _sf(loc.get("lng"))

    # ── Status ────────────────────────────────────────────────────────────────
    raw_status = iden.get("construction_status") or rera.get("status") or ""
    r = raw_status.lower()
    if "ready" in r or "rtm" in r:             status = "Ready to Move"
    elif "new launch" in r or "new_launch" in r: status = "New Launch"
    elif "under" in r or "construction" in r:   status = "Under Construction"
    elif "pre launch" in r or "pre_launch" in r: status = "Pre Launch"
    else:                                        status = raw_status.title() if raw_status else "Unknown"

    # ── Segment — CORRECT: use identity.project_segment (set by classify_segments.py) ──
    segment = iden.get("project_segment") or "Standalone"

    # ── is_gated — CORRECT: use project_segment ──────────────────────────────
    is_gated = (segment == "Gated Community")

    # ── Amenities — CORRECT: use amenities.all (not facilities.items) ────────
    amenities = amen_doc.get("all") or []
    amenity_flags = amen_doc.get("flags") or {}

    # ── RERA ──────────────────────────────────────────────────────────────────
    rera_id = rera.get("number") or ""

    # ── Building stats ────────────────────────────────────────────────────────
    total_units = _si(bldg.get("total_apartments")) or _si(rera.get("total_units"))
    towers      = _si(bldg.get("total_towers"))
    floors      = _si(bldg.get("total_floors"))

    # ── Absorption — CORRECT: from insightforge.rera_scraped_data ────────────
    booked_units = None
    available    = None
    absorption   = None
    construction_pct = None
    has_litigations  = False
    is_highrise      = False

    if rera_id:
        if rera_cache and rera_id in rera_cache:
            abs_data = rera_cache[rera_id]
        else:
            abs_data = get_rera_absorption(rera_id)
            if rera_cache is not None:
                rera_cache[rera_id] = abs_data
        if abs_data:
            booked_units     = abs_data.get("booked_apartments")
            if not total_units:
                total_units  = abs_data.get("total_apartments")
            available        = abs_data.get("available_units")
            absorption       = abs_data.get("absorption_pct")
            construction_pct = abs_data.get("construction_progress_pct")
            has_litigations  = abs_data.get("has_litigations", False)
            is_highrise      = abs_data.get("is_highrise", False)
            if not floors:
                floors = abs_data.get("total_floors")
            if not towers:
                towers = abs_data.get("buildings_count")

    if total_units and booked_units is not None and available is None:
        available  = max(0, total_units - booked_units)
    if total_units and booked_units is not None and absorption is None:
        absorption = round(booked_units / total_units * 100, 1)

    # ── Highlights ────────────────────────────────────────────────────────────
    highlights = specs.get("key_highlights") or []
    if not highlights:
        usps = specs.get("usps") or []
        highlights = [u.get("heading","") if isinstance(u,dict) else str(u) for u in usps if u]

    # ── Cover image ───────────────────────────────────────────────────────────
    cover_img = iden.get("cover_image_url") or ""
    if not cover_img:
        images = media.get("images") or []
        if images and isinstance(images[0], dict):
            cover_img = images[0].get("url","")

    # ── Sources ───────────────────────────────────────────────────────────────
    sources = meta.get("source_platforms") or []

    # ── Listing URL ───────────────────────────────────────────────────────────
    listing_url = ""
    for s in (meta.get("sources") or []):
        if isinstance(s, dict) and s.get("platform") == "99acres":
            listing_url = s.get("url",""); break
    if not listing_url:
        for s in (meta.get("sources") or []):
            if isinstance(s, dict) and s.get("url"):
                listing_url = s.get("url",""); break

    # ── Locality overview ─────────────────────────────────────────────────────
    points = lo.get("points") or []
    locality_points = {
        "positives": [p["title"] for p in points if isinstance(p,dict) and p.get("type")=="WHATS_GREAT_HERE" and p.get("title")],
        "negatives": [p["title"] for p in points if isinstance(p,dict) and p.get("type")!="WHATS_GREAT_HERE" and p.get("title")],
    }

    return {
        # identity
        "master_id":         meta.get("master_id",""),
        "project_name":      iden.get("project_name",""),
        "developer":         iden.get("builder_name",""),
        "cover_image_url":   cover_img,
        "source":            "+".join(sources),
        "platform_count":    meta.get("platform_count",1),
        "listing_url":       listing_url,
        # location
        "locality": canonicalize_locality(loc.get("locality","")) or loc.get("locality",""),
        "city":              loc.get("city",""),
        "district":          loc.get("district",""),
        "mandal":            loc.get("mandal",""),
        "address":           loc.get("address",""),
        "pincode":           loc.get("pincode",""),
        "latitude":          lat,
        "longitude":         lng,
        # pricing
        "price_per_sqft":      psf,
        "psf_is_estimated":    psf_is_estimated,
        "min_price":           price_min,
        "max_price":           price_max,
        "price_on_request":    bool(doc.get("pricing", {}).get("price_on_request")),
        "area_min_sqft":     area_min,
        "area_max_sqft":     area_max,
        # classification — sourced from identity.project_segment
        "segment":           segment,
        "is_gated":          is_gated,
        "status":            status,
        "possession_date":   iden.get("possession_date") or rera.get("proposed_completion",""),
        # building
        "total_units":       total_units,
        "booked_units":      booked_units,
        "available_units":   available,
        "absorption_pct":    absorption,
        "construction_pct":  construction_pct,
        "towers":            towers,
        "floors":            floors,
        # RERA
        "rera_id":           rera_id,
        "rera_status":       rera.get("status",""),
        "rera_authority":    rera.get("authority",""),
        "rera_approved":     rera.get("approved_date",""),
        "rera_completion":   rera.get("proposed_completion",""),
        "has_litigations":   has_litigations,
        "is_highrise":       is_highrise,
        # amenities — from amenities.all (fixed)
        "amenities":         amenities,
        "amenity_count":     amen_doc.get("count") or len(amenities),
        "amenity_flags":     amenity_flags,
        "highlights":        highlights[:8],
        # config
        "configurations":    bhk_list,
        "config_cards":      cards[:8],
        # reviews
        "rating":            reviews.get("overall_rating"),
        "review_count":      reviews.get("total_count"),
        # locality overview
        "locality_overview": locality_points,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY AGGREGATION
# ─────────────────────────────────────────────────────────────────────────────

def _has_valid_absorption(p: dict) -> bool:
    """Verified absorption only — no imputation of missing booked/total units."""
    tu, bu, ap = p.get("total_units"), p.get("booked_units"), p.get("absorption_pct")
    return tu is not None and tu > 0 and bu is not None and ap is not None

# ponytail: self-check — missing booked must not count as valid absorption
assert not _has_valid_absorption({"total_units": 100, "booked_units": None, "absorption_pct": 50})
assert _has_valid_absorption({"total_units": 100, "booked_units": 40, "absorption_pct": 40.0})


def _compute_summary(projects: list) -> dict:
    if not projects:
        return {}
    # Only use confirmed, non-estimated PSF for averages (exclude POR and derived values)
    psf_vals  = [p["price_per_sqft"] for p in projects
                 if p.get("price_per_sqft") and p["price_per_sqft"] > 0
                 and not p.get("price_on_request")
                 and not p.get("psf_is_estimated")]
    seg_dist  = defaultdict(int)
    stat_dist = defaultdict(int)
    cfg_dist  = defaultdict(int)
    gated = total_u = booked_u = rera_count = rera_approved_count = ghmc_count = hmda_count = 0

    units_by_status: dict[str, int] = defaultdict(int)
    avail_by_status: dict[str, int] = defaultdict(int)

    for p in projects:
        seg_dist[p.get("segment","Unknown")]  += 1
        stat_dist[p.get("status","Unknown")]  += 1
        for c in (p.get("configurations") or []):
            cfg_dist[c] += 1
        if p.get("is_gated"):           gated    += 1
        if p.get("rera_id"):            rera_count += 1
        if p.get("rera_approved"):      rera_approved_count += 1   # has approved_date
        tu = p.get("total_units") or 0
        bu = p.get("booked_units") or 0
        if tu:  total_u  += tu
        if bu:  booked_u += bu

        st = p.get("status","Unknown")
        units_by_status[st] += tu
        avail_by_status[st] += max(0, tu - bu)

        auth = (p.get("rera_authority") or "").upper()
        if "GHMC" in auth: ghmc_count += 1
        if "HMDA" in auth: hmda_count += 1

    # Absorption rate: unit-weighted, only projects with verified booked + total units
    abs_projects = [p for p in projects if _has_valid_absorption(p)]
    if abs_projects:
        abs_tu = sum(p["total_units"] for p in abs_projects)
        abs_bu = sum(p["booked_units"] for p in abs_projects)
        abs_rt = round(abs_bu / abs_tu * 100, 1) if abs_tu else None
    else:
        abs_rt = None
    abs_count = len(abs_projects)

    avail_u = max(0, total_u - booked_u) if total_u else 0

    # Price distribution
    price_buckets = defaultdict(int)
    for p in projects:
        if p.get("min_price"):
            cr = p["min_price"] / 1e7
            if cr < 0.5:   price_buckets["<50L"] += 1
            elif cr < 1:   price_buckets["50L-1Cr"] += 1
            elif cr < 1.5: price_buckets["1-1.5Cr"] += 1
            elif cr < 2:   price_buckets["1.5-2Cr"] += 1
            elif cr < 3:   price_buckets["2-3Cr"] += 1
            else:          price_buckets["3Cr+"] += 1

    # Builder market share (top 8) + active/total developer counts
    builder_dist  = defaultdict(int)
    active_status = {"Under Construction", "New Launch", "Pre Launch"}
    active_devs   = set()
    for p in projects:
        b = (p.get("developer") or "Unknown").strip()
        if b: builder_dist[b] += 1
        if p.get("status") in active_status and b and b.lower() != "unknown":
            active_devs.add(b)

    # total_developers = all unique named developers (excluding "Unknown")
    total_devs = {k for k in builder_dist if k.lower() != "unknown"}
    # top_builders capped at 8 for chart display; full builder_distribution for filtering
    top_builders = dict(sorted(builder_dist.items(), key=lambda x: -x[1])[:8])

    return {
        "total_projects":       len(projects),
        "gated_projects":       gated,
        "rera_count":           rera_count,           # projects with rera.number (registration ID)
        "rera_approved_count":  rera_approved_count,  # projects with rera.approved_date
        "ghmc_projects":        ghmc_count,           # full-dataset count (before 400-cap)
        "hmda_projects":        hmda_count,           # full-dataset count (before 400-cap)
        "active_developers":    len(active_devs),
        "total_developers":     len(total_devs),
        "avg_price_per_sqft":   round(sum(psf_vals)/len(psf_vals)) if psf_vals else 0,
        "median_price_per_sqft": sorted(psf_vals)[len(psf_vals)//2] if psf_vals else 0,
        "total_units":          total_u,
        "booked_units":         booked_u,
        "available_units":      avail_u,
        "absorption_rate_pct":  abs_rt,
        "absorption_project_count": abs_count,
        "segment_distribution": dict(seg_dist),
        "status_distribution":  dict(stat_dist),
        "config_distribution":  dict(cfg_dist),
        "price_distribution":   dict(price_buckets),
        "builder_distribution": top_builders,
        "inventory_units": {
            "total":     total_u,
            "sold":      booked_u,
            "available": avail_u,
            "by_status": dict(units_by_status),
            "avail_by_status": dict(avail_by_status),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# PRICING INTELLIGENCE
# ─────────────────────────────────────────────────────────────────────────────

def get_pricing_intel(locality: str, city: str = "Hyderabad") -> dict:
    """
    Returns ASBL vs market PSF benchmarks + competitor comparison for a locality.
    Source: real_estate.projects_master
    """
    try:
        canonical = canonicalize_locality(locality) or locality.strip()
        aliases = {canonical, locality.strip()}
        for raw, canon in LOCALITY_ALIASES.items():
            if canon and canon.lower() == canonical.lower():
                aliases.add(raw)
        alias_res = [re.compile(r"^" + re.escape(a) + r"$", re.IGNORECASE) for a in aliases if a]

        docs = list(_re()["projects_master"].find(
            {"$or": [
                {"location.locality": {"$in": alias_res}},
                {"location.mandal":   {"$in": alias_res}},
            ]},
            {"identity.builder_name":1, "identity.project_segment":1,
             "pricing.price_per_sqft":1, "pricing.min_price":1, "pricing.max_price":1,
             "identity.project_name":1, "identity.construction_status":1,
             "configurations.bhk_list":1, "_id":0}
        ))
        projects = []
        for doc in docs:
            iden    = doc.get("identity") or {}
            pricing = doc.get("pricing") or {}
            psf     = _si(pricing.get("price_per_sqft")) or 0
            min_p   = _si(pricing.get("min_price")) or 0
            max_p   = _si(pricing.get("max_price")) or 0
            dev     = (iden.get("builder_name") or "Unknown").strip() or "Unknown"
            projects.append({
                "project_name": iden.get("project_name",""),
                "developer": dev,
                "segment": iden.get("project_segment","Standalone"),
                "status": iden.get("construction_status",""),
                "price_per_sqft": psf,
                "min_price": min_p, "max_price": max_p,
                "configurations": (doc.get("configurations") or {}).get("bhk_list",[]),
            })

        # Market stats (use all projects, not capped)
        market_psf = [p["price_per_sqft"] for p in projects if p["price_per_sqft"]>0]
        asbl_projects = [p for p in projects
                         if "asbl" in p["developer"].lower()
                         or "asbl" in p["project_name"].lower()]
        asbl_psf = [p["price_per_sqft"] for p in asbl_projects if p["price_per_sqft"]>0]

        market_avg = round(sum(market_psf)/len(market_psf)) if market_psf else 0
        asbl_avg   = round(sum(asbl_psf)/len(asbl_psf)) if asbl_psf else 0
        premium    = round((asbl_avg - market_avg) / market_avg * 100, 1) if market_avg and asbl_avg else None

        return {
            "locality":       locality,
            "city":           city,
            "projects":       projects,          # full list for dev comparison chart
            "market_avg_psf": market_avg,
            "asbl_avg_psf":   asbl_avg,
            "premium_pct":    premium,
            "psf_min":        min(market_psf) if market_psf else 0,
            "psf_max":        max(market_psf) if market_psf else 0,
            "asbl_projects":  asbl_projects,
            "total_with_psf": len(market_psf),
        }
    except Exception as e:
        log.warning(f"get_pricing_intel({locality}): {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# PROJECT INTELLIGENCE
# ─────────────────────────────────────────────────────────────────────────────

def get_project_intelligence(locality: str, city: str = "Hyderabad") -> dict:
    """
    Source: real_estate.projects_master + insightforge.approval_project_matches
    Returns developer rankings, market share, lifecycle, unit distribution for locality.
    """
    try:
        canonical = canonicalize_locality(locality) or locality.strip()
        aliases = {canonical, locality.strip()}
        for raw, canon in LOCALITY_ALIASES.items():
            if canon and canon.lower() == canonical.lower():
                aliases.add(raw); aliases.add(raw.title())
        alias_res = [re.compile(r"^" + re.escape(a) + r"$", re.IGNORECASE) for a in aliases if a]

        docs = list(_re()["projects_master"].find(
            {"$or": [
                {"location.locality": {"$in": alias_res}},
                {"location.mandal":   {"$in": alias_res}},
            ]},
            {"identity":1, "building":1, "pricing":1, "rera":1, "_meta":1, "_id":0}
        ))

        if not docs:
            return {"locality": locality, "total": 0}

        developers:   dict[str, dict] = defaultdict(lambda: {"projects":0,"units":0,"psf_vals":[]})
        category_dist: dict[str, int] = defaultdict(int)
        status_dist:   dict[str, int] = defaultdict(int)
        unit_buckets:  dict[str, int] = defaultdict(int)

        for doc in docs:
            iden    = doc.get("identity") or {}
            bldg    = doc.get("building") or {}
            pricing = doc.get("pricing")  or {}
            dev = (iden.get("builder_name") or "Unknown").strip() or "Unknown"
            seg = iden.get("project_segment") or "Standalone"
            status_raw = iden.get("construction_status") or ""
            s = status_raw.lower()
            if "ready" in s or "rtm" in s:             status = "Ready to Move"
            elif "new launch" in s:                     status = "New Launch"
            elif "under" in s or "construction" in s:   status = "Under Construction"
            elif "pre launch" in s:                     status = "Pre Launch"
            else:                                        status = status_raw.title() or "Unknown"

            units = _si(bldg.get("total_apartments")) or 0
            psf   = _si(pricing.get("price_per_sqft")) or 0

            developers[dev]["projects"] += 1
            developers[dev]["units"] += units
            if psf: developers[dev]["psf_vals"].append(psf)
            category_dist[seg] += 1
            status_dist[status] += 1

            # Unit distribution buckets
            if units:
                if units < 50:       unit_buckets["<50"] += 1
                elif units < 100:    unit_buckets["50-100"] += 1
                elif units < 200:    unit_buckets["100-200"] += 1
                elif units < 500:    unit_buckets["200-500"] += 1
                else:                unit_buckets["500+"] += 1

        # Rank developers by units (then by project count)
        ranked = []
        for dev, d in developers.items():
            avg_psf = round(sum(d["psf_vals"])/len(d["psf_vals"])) if d["psf_vals"] else 0
            ranked.append({"developer": dev, "projects": d["projects"],
                           "units": d["units"], "avg_psf": avg_psf})
        ranked.sort(key=lambda x: (-x["units"], -x["projects"]))

        total_units = sum(r["units"] for r in ranked)

        # Market share by units
        for r in ranked:
            r["market_share_pct"] = round(r["units"] / total_units * 100, 1) if total_units else 0

        # Approval stats from approval_project_matches
        approvals = get_approval_stats(locality)

        return {
            "locality":              locality,
            "city":                  city,
            "total_projects":        len(docs),
            "total_units":           total_units,
            "developer_rankings":    ranked[:15],
            "category_distribution": dict(category_dist),
            "status_distribution":   dict(status_dist),
            "unit_distribution":     dict(unit_buckets),
            "approval_stats":        approvals,
        }
    except Exception as e:
        log.warning(f"get_project_intelligence({locality}): {e}")
        return {"locality": locality, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# BUYER PERSONA (buyer_persona DB)
# ─────────────────────────────────────────────────────────────────────────────

def _bp():
    """buyer_persona database accessor."""
    return _db()


def _extract_v(field: dict):
    """Safely pull .v from a buyer_profile field dict; return None if unavailable."""
    if not isinstance(field, dict):
        return None
    v = field.get("v")
    if field.get("status") == "unavailable" or field.get("basis") == "unavailable":
        return None
    return v


# company_type values that are NOT real employers (locations, gig-platforms, training)
_EXCLUDE_COMPANY_TYPES = {"SAP", "Uber", "Ola", "it_park", "tech_park",
                           "DLF Cyber City", "HITEC City"}
# Name keywords that indicate a training/coaching centre
_TRAINING_KEYWORDS = {"training", "institute", "academy", "school", "coaching",
                      "course", "placement", "classes", "tutorial", "certification"}


def _get_nearby_employers(locality: str, extra_localities: list = None, limit: int = 8) -> list:
    """
    Return real company names from insightforge.it_companies for a locality.
    Filters by company_type and name keywords; deduplicates case-insensitively.
    """
    try:
        db_insight = _ig()
        locs = [locality] + [l for l in (extra_localities or []) if l and l != locality]
        seen_lower, result = set(), []
        for loc in locs:
            if not loc:
                continue
            docs = db_insight["it_companies"].find(
                {"location": re.compile(re.escape(loc), re.I)},
                {"name": 1, "company_type": 1}
            ).limit(40)
            for doc in docs:
                ctype = (doc.get("company_type") or "").strip()
                if ctype in _EXCLUDE_COMPANY_TYPES:
                    continue
                name = (doc.get("name") or "").strip()
                if not name or len(name) > 80:
                    continue
                if any(kw in name.lower() for kw in _TRAINING_KEYWORDS):
                    continue
                key = name.lower()
                if key not in seen_lower:
                    seen_lower.add(key)
                    result.append(name)
                if len(result) >= limit:
                    return result
        return result
    except Exception:
        return []


def get_buyer_persona_full(locality: str = "", radius_km: int = 0) -> dict:
    """
    Single source of truth for buyer persona — reads all 3 buyer_persona collections.
      localities   → market tier, median_budget, rera_unit_mix, centroid (100 docs)
      micromarkets → structured claims with confidence, coverage metadata (100 docs)
      reports      → pre-generated locality_report (radius_km=0) or
                     radius_report (radius_km 1–10): market_summary + buyer_profile

    Args:
        locality:  Locality name to look up (optional; returns market-wide data if empty)
        radius_km: Radius in km (1–10).  When > 0, uses the radius_report for that
                   exact radius rather than the locality_report.  Falls back to the
                   nearest available radius if an exact match is missing.
    """
    try:
        bp = _bp()

        # ── Market-wide: all localities ──────────────────────────────────────
        all_loc_docs = list(bp["localities"].find({}, {"_id": 0}))
        tier_dist   = defaultdict(int)
        budget_dist = defaultdict(int)
        all_localities = []
        for d in all_loc_docs:
            t = d.get("market_tier")
            if t:
                tier_dist[t] += 1
            b = d.get("median_budget") or 0
            if b:
                bucket = ("<1 Cr" if b < 1 else "1–1.5 Cr" if b < 1.5 else
                          "1.5–2 Cr" if b < 2 else "2–3 Cr" if b < 3 else "3 Cr+")
                budget_dist[bucket] += 1
            all_localities.append({
                "name":                 d.get("name"),
                "market_tier":          t or "unknown",
                "median_budget":        b or None,
                "centroid":             d.get("centroid"),
                "rera_registered_units": d.get("rera_registered_units"),
            })

        # ── Locality-specific lookup ─────────────────────────────────────────
        loc_report = {}
        mm_doc     = {}
        loc_meta   = {}
        _radius_fallback_source = None
        _radius_included        = []
        _used_radius_km         = None

        if locality:
            canon = canonicalize_locality(locality) or locality.strip()
            loc_re = re.compile(r"^\s*" + re.escape(canon) + r"\s*$", re.I)

            # 1. localities meta
            loc_meta = bp["localities"].find_one({"name": loc_re}, {"_id": 0}) or {}

            # 2. micromarket (claims + coverage)
            mm_doc = bp["micromarkets"].find_one({"name": loc_re}, {"_id": 0}) or {}

            if radius_km and int(radius_km) > 0:
                # ── Radius mode: fetch radius_report for the requested radius ──
                r = int(radius_km)
                report_doc = bp["reports"].find_one(
                    {"_locality": loc_re, "_radius_km": r}
                )
                if not report_doc:
                    # Fallback: pick the closest available radius
                    all_radius_docs = list(bp["reports"].find(
                        {"_locality": loc_re, "_radius_km": {"$gt": 0}},
                        {"_id": 0, "_radius_km": 1}
                    ))
                    if all_radius_docs:
                        closest = min(all_radius_docs, key=lambda x: abs(x["_radius_km"] - r))
                        report_doc = bp["reports"].find_one(
                            {"_locality": loc_re, "_radius_km": closest["_radius_km"]}
                        )

                if report_doc:
                    rr = report_doc.get("radius_report") or {}
                    _used_radius_km = report_doc.get("_radius_km")
                    loc_report = {
                        "buyer_profile":  rr.get("buyer_profile") or {},
                        "market_summary": rr.get("market_summary") or {},
                    }
                    _radius_included = rr.get("included_localities") or []

            else:
                # ── Exact locality (radius_km == 0): never pick a radius_report doc ──
                report_doc = bp["reports"].find_one({"_locality": loc_re, "_radius_km": 0})
                if not report_doc:
                    report_doc = bp["reports"].find_one({
                        "_locality": loc_re,
                        "locality_report": {"$exists": True, "$nin": [None, {}]},
                    })
                if report_doc:
                    loc_report = report_doc.get("locality_report") or {}

                # Radius fallback — if still no data, find a radius_report from any
                # center locality that lists this locality as included.
                if not loc_report:
                    radius_doc = bp["reports"].find_one(
                        {"radius_report.included_localities.name": loc_re},
                        {"radius_report": 1, "_locality": 1}
                    )
                    if radius_doc:
                        rr = radius_doc.get("radius_report") or {}
                        loc_report = {
                            "buyer_profile":  rr.get("buyer_profile") or {},
                            "market_summary": rr.get("market_summary") or {},
                        }
                        _radius_fallback_source = (
                            f"{radius_doc.get('_locality')} "
                            f"{rr.get('radius_km', '?')} km radius"
                        )
                        _radius_included = rr.get("included_localities") or []

        # ── Parse buyer_profile ───────────────────────────────────────────────
        bp_raw = loc_report.get("buyer_profile") or {}
        ms_raw = loc_report.get("market_summary") or {}

        # Persona mix  {it_professional: 0.58, business_owner: 0.30, hni: 0.12}
        persona_mix_raw = _extract_v(bp_raw.get("buyer_persona_mix")) or {}
        persona_mix = {k.replace("_", " ").title(): round(v * 100)
                       for k, v in persona_mix_raw.items() if isinstance(v, (int, float))}

        # Buying intent  {end_use: 0.647, investment: 0.287, rental_yield: 0.067}
        intent_raw = _extract_v(bp_raw.get("buying_intent")) or {}
        buying_intent = {k.replace("_", " ").title(): round(v * 100)
                         for k, v in intent_raw.items() if isinstance(v, (int, float))}

        # BHK demand  {"2": 0.2, "3": 0.5, "4": 0.3}
        bhk_demand_raw = _extract_v(bp_raw.get("bhk_demand")) or {}
        bhk_demand = {f"{k} BHK": round(v * 100)
                      for k, v in bhk_demand_raw.items() if isinstance(v, (int, float))}

        # Budget by BHK  {"2": {"p50": 1.2}, "3": {"p50": 1.8, "p75": 2.5}}
        budget_by_bhk_raw = _extract_v(bp_raw.get("avg_budget_by_bhk_cr")) or {}
        budget_by_bhk = {}
        for bhk, bvals in budget_by_bhk_raw.items():
            if isinstance(bvals, dict):
                budget_by_bhk[f"{bhk} BHK"] = {
                    "median": bvals.get("p50"),
                    "p75":    bvals.get("p75"),
                }

        # Income
        income_raw = _extract_v(bp_raw.get("avg_annual_income_inr")) or {}
        income = None
        if isinstance(income_raw, dict) and income_raw.get("mean"):
            income = {
                "min":  income_raw.get("min"),
                "max":  income_raw.get("max"),
                "mean": income_raw.get("mean"),
            }

        # Pain points  [{issue: "high prices", conf: 0.6}, ...]
        pain_raw = _extract_v(bp_raw.get("pain_points")) or []
        pain_points = [
            {"issue": p.get("issue", "").replace("_", " ").title(),
             "conf":  round((p.get("conf") or 0) * 100)}
            for p in (pain_raw if isinstance(pain_raw, list) else [])
            if p.get("issue")
        ][:8]

        # Employer cluster — prefer survey/direct data; fall back to real it_companies data.
        # Inferred/pooled data (basis='inferred') is not locality-specific and shows
        # generic IT hub names, so we use insightforge.it_companies as the real source.
        _emp_field = bp_raw.get("employer_cluster")
        _emp_basis = _emp_field.get("basis", "") if isinstance(_emp_field, dict) else ""
        if _emp_basis not in ("inferred", "") and _emp_basis != "unavailable":
            # Real survey or direct data — use it
            emp_raw = _extract_v(_emp_field) or []
            if isinstance(emp_raw, dict):
                employer_cluster_pct = {k: round(v * 100) if isinstance(v, float) and v <= 1 else int(v)
                                         for k, v in emp_raw.items() if isinstance(v, (int, float))}
                employers = list(emp_raw.keys())
            else:
                employer_cluster_pct = {}
                employers = [str(e) for e in emp_raw] if isinstance(emp_raw, list) else []
        else:
            # No reliable survey data — derive from actual it_companies in this locality
            employer_cluster_pct = {}
            extra_locs = [l.get("name", "") for l in _radius_included if isinstance(l, dict)]
            employers = _get_nearby_employers(locality, extra_locs)

        # Designation tier — v can be a list of strings OR a dict {name: pct}
        desig_raw = _extract_v(bp_raw.get("designation_tier")) or []
        if isinstance(desig_raw, dict):
            designation_pct = {
                k.replace("_", " ").title(): round(v * 100) if isinstance(v, float) and v <= 1 else int(v)
                for k, v in desig_raw.items() if isinstance(v, (int, float))
            }
            designations = [k.replace("_", " ").title() for k in desig_raw.keys()]
        elif isinstance(desig_raw, list) and desig_raw:
            # Equal-weight distribution across listed designations
            n = len(desig_raw)
            share = round(100 / n)
            designation_pct = {
                str(d).replace("_", " ").title(): share
                for d in desig_raw
            }
            designations = [str(d).replace("_", " ").title() for d in desig_raw]
        else:
            designation_pct = {}
            designations = []

        # Sector mix  {it: 0.55, banking: 0.15, ...}
        sector_raw = _extract_v(bp_raw.get("sector_mix")) or {}
        sector_mix = {k.replace("_", " ").title(): round(v * 100) if isinstance(v, float) and v <= 1 else int(v)
                      for k, v in (sector_raw.items() if isinstance(sector_raw, dict) else [])
                      if isinstance(v, (int, float))}

        def _cap_salary(mn, mx, med, lower_mult=0.3, upper_mult=3.5):
            """Cap salary/income outliers relative to median."""
            if mn is None or mx is None or med is None or med <= 0:
                return mn, mx
            lo = max(mn, med * lower_mult)
            hi = min(mx, med * upper_mult)
            return (round(lo), round(hi)) if lo < hi else (mn, mx)

        # Average household income
        hh_raw = _extract_v(bp_raw.get("avg_household_income_inr")) or {}
        household_income = None
        if isinstance(hh_raw, dict) and hh_raw.get("mean"):
            hh_mn, hh_mx = _cap_salary(hh_raw.get("min"), hh_raw.get("max"), hh_raw.get("mean"))
            household_income = {"min": hh_mn, "max": hh_mx, "mean": hh_raw.get("mean")}

        # Age band  {25_35: 0.45, 35_45: 0.38, ...}
        age_band_raw = _extract_v(bp_raw.get("avg_age_band")) or {}
        age_band = {k.replace("_", "–"): round(v * 100) if isinstance(v, float) and v <= 1 else int(v)
                    for k, v in (age_band_raw.items() if isinstance(age_band_raw, dict) else [])
                    if isinstance(v, (int, float))}

        # Age-wise salary bracket  {25_35: {min: 800000, max: 1500000}, ...}
        age_salary_raw = _extract_v(bp_raw.get("age_wise_salary_bracket")) or {}
        age_salary = {}
        if isinstance(age_salary_raw, dict):
            for age_grp, sal in age_salary_raw.items():
                if isinstance(sal, dict):
                    med_s = sal.get("median") or sal.get("p50")
                    mn_s, mx_s = _cap_salary(sal.get("min"), sal.get("max"), med_s)
                    age_salary[age_grp.replace("_", "–")] = {
                        "min": mn_s, "max": mx_s, "median": med_s
                    }

        # Area sqft range  {"2": {"min": 900, "max": 1400}, "3": {...}}
        # Outlier guard: cap extremes using median-relative bounds (IQR-style).
        # Raw DB values sometimes contain data-quality spikes (e.g., min=10 or max=99999).
        _BHK_SQFT_CEIL = {"1": 1500, "2": 2500, "3": 4000, "4": 6000, "5": 8000, "villa": 12000}

        def _cap_range(mn, mx, med, lower_mult: float = 0.35, upper_mult: float = 2.5):
            """Trim min/max relative to median; raw DB values may contain spikes."""
            if mn is None or mx is None or med is None or med <= 0:
                return mn, mx
            lo = max(mn, med * lower_mult)
            hi = min(mx, med * upper_mult)
            if lo >= hi:
                lo, hi = mn, mx
            return round(lo), round(hi)

        def _sanitize_area(bhk_key, mn, mx, med):
            """Reject/correct absurd medians (e.g. 10k+ sqft for 2 BHK)."""
            if med is None:
                return None, None, None
            bhk_n = re.sub(r"[^0-9a-z]", "", str(bhk_key).lower())
            ceil = _BHK_SQFT_CEIL.get(bhk_n, 10000)
            if med > ceil or med < 250:
                if mn and mx and 250 <= (mn + mx) / 2 <= ceil:
                    med = round((mn + mx) / 2)
                else:
                    return None, None, None
            mn, mx = _cap_range(mn, mx, med)
            if mn is None or mx is None:
                return None, None, None
            if med > mx:
                med = round((mn + mx) / 2)
            return mn, mx, round(med)

        area_raw = _extract_v(bp_raw.get("area_sqft_range")) or {}
        area_sqft_range = {}
        if isinstance(area_raw, dict):
            for bhk, avals in area_raw.items():
                if isinstance(avals, dict):
                    mn  = avals.get("min")
                    mx  = avals.get("max")
                    med = avals.get("median") or avals.get("p50")
                    mn, mx, med = _sanitize_area(bhk, mn, mx, med)
                    if mn is not None and mx is not None and med is not None:
                        area_sqft_range[f"{bhk} BHK"] = {"min": mn, "max": mx, "median": med}

        # Property type demand  {flat: 0.75, villa: 0.15, plot: 0.10}
        prop_raw = _extract_v(bp_raw.get("property_type_demand")) or {}
        property_type_demand = {k.replace("_", " ").title(): round(v * 100) if isinstance(v, float) and v <= 1 else int(v)
                                 for k, v in (prop_raw.items() if isinstance(prop_raw, dict) else [])
                                 if isinstance(v, (int, float))}

        # Evidence refs
        evidence_refs = bp_raw.get("evidence_refs") or []
        if not isinstance(evidence_refs, list):
            evidence_refs = []

        # Coverage from micromarkets
        coverage = mm_doc.get("coverage") or {}

        return {
            "locality":        locality or "Market-Wide",
            "total_localities": len(all_loc_docs),
            "all_localities":   all_localities,
            "tier_distribution":   dict(tier_dist),
            "budget_distribution": dict(budget_dist),
            # Locality-specific market summary
            "market_tier":     loc_meta.get("market_tier") or mm_doc.get("market_tier") or ms_raw.get("supply_signal"),
            "median_budget":   ms_raw.get("median_budget_cr") or loc_meta.get("median_budget") or mm_doc.get("median_budget"),
            "dominant_bhk":    ms_raw.get("dominant_bhk"),
            "supply_signal":   ms_raw.get("supply_signal"),
            "rera_unit_mix":   loc_meta.get("rera_unit_mix") or {},
            # Buyer profile
            "profile_label":        bp_raw.get("profile_label"),
            "persona_mix":          persona_mix,
            "buying_intent":        buying_intent,
            "bhk_demand":           bhk_demand,
            "budget_by_bhk":        budget_by_bhk,
            "income":               income,
            "household_income":     household_income,
            "employers":            employers,
            "employer_cluster_pct": employer_cluster_pct,
            "designations":         [d.replace("_", " ").title() for d in designations],
            "designation_pct":      designation_pct,
            "sector_mix":           sector_mix,
            "age_band":             age_band,
            "age_salary":           age_salary,
            "area_sqft_range":      area_sqft_range,
            "property_type_demand": property_type_demand,
            "pain_points":          pain_points,
            "evidence_refs":        evidence_refs[:6],
            "confidence_score":     bp_raw.get("confidence_score"),
            "data_coverage":        loc_meta.get("data_coverage") or coverage.get("data_coverage"),
            "claims_count":         coverage.get("claims_count", 0),
            "source":               "buyer_persona.reports+micromarkets+localities",
            # Radius metadata
            "used_radius_km":         _used_radius_km,
            "radius_fallback_source": _radius_fallback_source,
            "radius_included":        [
                {"name": x.get("name"), "distance_km": x.get("distance_km"),
                 "market_tier": x.get("market_tier")}
                for x in _radius_included if x.get("name")
            ],
        }
    except Exception as e:
        log.warning(f"get_buyer_persona_full({locality}): {e}")
        return {"error": str(e)}


# Keep old name as alias so nothing else breaks
def get_buyer_persona_data(locality: str = "", radius_km: int = 0) -> dict:
    return get_buyer_persona_full(locality, radius_km=radius_km)


def list_bp_localities() -> list:
    """Return sorted list of locality names that have buyer_persona report data."""
    try:
        bp = _bp()
        raw = bp["reports"].distinct("_locality")
        return sorted(set(r for r in raw if r and isinstance(r, str)))
    except Exception as e:
        log.warning(f"list_bp_localities: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# LOCALITY INTELLIGENCE (99acres + Google Reviews)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_parse(val):
    """Parse a value that may be a Python-literal string or already a list/dict."""
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        import ast
        try:
            return ast.literal_eval(val)
        except Exception:
            return val
    return val


def get_locality_intelligence(locality: str, city: str = "Hyderabad") -> dict:
    """
    Combine 99acres locality report + Google Reviews for a locality.
    Sources: real_estate.99a_locality_report, real_estate.google_reviews
    """
    try:
        db = _re()
        canon = canonicalize_locality(locality) or locality.strip()

        # ── 99acres locality report ────────────────────────────────────────────
        doc99 = db["99a_locality_report"].find_one(
            {"locality": re.compile(r"^\s*" + re.escape(canon) + r"\s*$", re.I)},
            {"_id": 0}
        )
        result: dict = {"locality": locality, "has_99a": bool(doc99), "has_google": False}

        if doc99:
            rs = _safe_parse(doc99.get("ratings_summary") or {})
            avg_rate_str = (doc99.get("average_rate") or "").replace("₹","").replace(",","").strip()
            try:
                avg_rate_psf = int(avg_rate_str) if avg_rate_str else None
            except ValueError:
                avg_rate_psf = None

            likes    = [_safe_parse(x) for x in (_safe_parse(doc99.get("likes") or []))]
            dislikes = [_safe_parse(x) for x in (_safe_parse(doc99.get("dislikes") or []))]
            feats    = [_safe_parse(x) for x in (_safe_parse(doc99.get("features_ratings") or []))]
            revs_raw = _safe_parse(doc99.get("reviews_list") or [])
            revs = [
                {
                    "author":  r.get("author") or "Anonymous",
                    "role":    r.get("role",""),
                    "rating":  r.get("rating"),
                    "positives": r.get("positives",""),
                    "negatives": r.get("negatives",""),
                    "posted_at": r.get("posted_at",""),
                }
                for r in (revs_raw if isinstance(revs_raw, list) else [])
                if isinstance(r, dict)
            ][:8]
            whats_great = _safe_parse(doc99.get("whats_great") or [])
            whats_attention = _safe_parse(doc99.get("whats_needs_attention") or [])
            price_trends = _safe_parse(doc99.get("price_trends") or [])
            sidebar_p = _safe_parse(doc99.get("sidebar_prices") or {})

            result.update({
                "avg_rate_psf":        avg_rate_psf,
                "overall_rating_99a":  rs.get("overall_rating") if isinstance(rs, dict) else None,
                "total_reviews_99a":   rs.get("total_reviews_count") if isinstance(rs, dict) else None,
                "star_counts":         rs.get("star_counts") if isinstance(rs, dict) else {},
                "positive_pct":        doc99.get("positive_mentions_percentage"),
                "likes":               likes[:8],
                "dislikes":            dislikes[:8],
                "features_ratings":    feats,
                "whats_great":         whats_great[:6] if isinstance(whats_great, list) else [],
                "whats_needs_attention": whats_attention[:4] if isinstance(whats_attention, list) else [],
                "reviews_99a":         revs,
                "price_trends_99a":    price_trends[-8:] if isinstance(price_trends, list) else [],
                "sidebar_prices":      sidebar_p if isinstance(sidebar_p, dict) else {},
            })

        # ── Google Reviews (projects in this locality) ─────────────────────────
        loc_re_g = re.compile(re.escape(canon), re.I)
        google_docs = list(db["google_reviews"].find(
            {"neighborhood": loc_re_g},
            {"totalScore": 1, "reviewsCount": 1, "reviews": {"$slice": 5},
             "title": 1, "address": 1, "_id": 0}
        ).limit(60))

        if google_docs:
            result["has_google"] = True
            scores = []
            total_rev_count = 0
            top_projects = []
            for d in google_docs:
                try:
                    sc = float(d.get("totalScore") or 0)
                    rc = int(d.get("reviewsCount") or 0)
                except (TypeError, ValueError):
                    sc, rc = 0, 0
                if sc > 0: scores.append(sc)
                total_rev_count += rc

                reviews_raw = _safe_parse(d.get("reviews") or [])
                top_revs = []
                if isinstance(reviews_raw, list):
                    for r in reviews_raw[:3]:
                        if isinstance(r, dict):
                            txt = r.get("text") or r.get("textTranslated") or ""
                            if txt:
                                top_revs.append({
                                    "text": txt[:200],
                                    "stars": r.get("stars"),
                                    "name": r.get("name","Anonymous"),
                                    "date": r.get("publishAt",""),
                                })
                if sc > 0 or top_revs:
                    top_projects.append({
                        "title":        d.get("title",""),
                        "rating":       sc,
                        "review_count": rc,
                        "reviews":      top_revs[:2],
                    })

            top_projects.sort(key=lambda x: (-x["review_count"], -x["rating"]))
            result.update({
                "avg_google_rating":         round(sum(scores)/len(scores), 1) if scores else None,
                "total_google_reviews":      total_rev_count,
                "reviewed_projects_count":   len(google_docs),
                "top_reviewed_projects":     top_projects[:6],
            })

        return result
    except Exception as e:
        log.warning(f"get_locality_intelligence({locality}): {e}")
        return {"locality": locality, "error": str(e)}

def fetch_supply_by_radius(lat: float, lng: float, radius_km: float = 3.0) -> dict:
    """
    Fetch projects within radius_km of lat/lng using bounding box + haversine filter.
    Returns same shape as fetch_supply().
    """
    if not lat or not lng:
        return {"supply_projects":[], "supply_summary":{}, "meta":{}}

    s_lat, n_lat, w_lng, e_lng = _bbox(lat, lng, radius_km)
    col = _re()["projects_master"]

    projects = []
    rera_cache = {}
    for doc in col.find({
        "location.lat": {"$gte": s_lat, "$lte": n_lat},
        "location.lng": {"$gte": w_lng, "$lte": e_lng},
    }, {"_id":0}):
        try:
            p = _norm_master(doc, rera_cache)
            if p.get("latitude") and p.get("longitude"):
                dist = _haversine(lat, lng, p["latitude"], p["longitude"])
                if dist <= radius_km * 1000:
                    p["dist_from_center"] = round(dist)
                    projects.append(p)
        except Exception as e:
            log.warning(f"radius norm error: {e}")

    projects.sort(key=lambda p: p.get("dist_from_center",0))
    summary = _compute_summary(projects)
    return {
        "locality":        f"{radius_km}km radius ({round(lat,4)},{round(lng,4)})",
        "city":            "Hyderabad",
        "supply_projects": projects,
        "supply_summary":  summary,
        "regulatory":      {},
        "demand":          {},
        "personas":        {},
        "persona_meta":    {},
        "meta": {
            "total":       len(projects),
            "page":        1,
            "page_size":   len(projects),
            "pages":       1,
            "radius_km":   radius_km,
            "center_lat":  lat,
            "center_lng":  lng,
            "sources":     ["projects_master"],
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN QUERY FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def fetch_supply(locality: str, city: str = "", fetch_infra: bool = False,
                 page: int = 1, page_size: int = 200) -> dict:
    city = city or "Hyderabad"
    col = _re()["projects_master"]
    city_wide = not (locality or "").strip()

    if city_wide:
        # ponytail: empty locality → all projects in city (Hyderabad-wide default view)
        city_re = re.compile(re.escape(city), re.I)
        if city.lower() == "hyderabad":
            query = {"$or": [
                {"location.city": city_re},
                {"location.city": {"$in": [None, ""]}},
            ]}
        else:
            query = {"location.city": city_re}
        canonical = ""
        exact_re = None
    else:
        # Canonicalize and build list of aliases to query
        canonical = canonicalize_locality(locality) or locality.strip()
        aliases = {canonical, locality.strip()}
        for raw, canon in LOCALITY_ALIASES.items():
            if canon and canon.lower() == canonical.lower():
                aliases.add(raw)
                aliases.add(raw.title())
        alias_res = [re.compile(r"^" + re.escape(a) + r"$", re.IGNORECASE) for a in aliases]
        query = {"location.locality": {"$in": alias_res}}
        exact_re = re.compile(r"^" + re.escape(canonical) + r"$")  # case-sensitive

    # Batch RERA absorption lookups
    rera_cache = {}
    projects = []
    exact_count = 0
    for doc in col.find(query, {"_id": 0}):
        try:
            if exact_re:
                loc_val = (doc.get("location") or {}).get("locality", "")
                if exact_re.match(loc_val):
                    exact_count += 1
            projects.append(_norm_master(doc, rera_cache))
        except Exception as e:
            log.warning(f"norm_master error: {e}")

    summary = _compute_summary(projects)
    if not city_wide:
        summary["exact_locality_count"]   = exact_count
        summary["variant_locality_count"] = len(projects) - exact_count

    # Infra data from insightforge (based on locality centroid)
    infra   = {}
    pois    = {}
    regulatory = {}
    if fetch_infra and projects and not city_wide:
        lats = [p["latitude"]  for p in projects if p.get("latitude")]
        lngs = [p["longitude"] for p in projects if p.get("longitude")]
        clat = clng = None
        if lats and lngs:
            clat = sum(lats)/len(lats)
            clng = sum(lngs)/len(lngs)
        else:
            clat, clng = get_locality_centroid(locality)
        if clat and clng:
            infra      = get_infra_summary(clat, clng, radius_km=5.0, city=city)
            pois       = get_nearby_pois(clat, clng, radius_km=3.0, city=city)
            regulatory = get_regulatory_summary(locality, city)

    # In-memory dedup guard: group by (project_name_lower, developer_lower), keep most-complete
    seen_keys: dict = {}
    deduped: list = []
    for p in projects:
        key = (
            (p.get("project_name") or "").strip().lower(),
            (p.get("developer") or "").strip().lower(),
        )
        if key[0] == "":  # unnamed projects: always keep
            deduped.append(p)
            continue
        if key not in seen_keys:
            seen_keys[key] = len(deduped)
            deduped.append(p)
        else:
            # Replace if current entry has more data fields populated
            prev = deduped[seen_keys[key]]
            def _score(x):
                return sum(1 for v in x.values() if v not in (None, "", [], {}))
            if _score(p) > _score(prev):
                deduped[seen_keys[key]] = p
    projects = deduped

    # Pagination: slice the requested page from all projects
    total = len(projects)
    page      = max(1, int(page))
    page_size = max(10, min(500, int(page_size)))
    start     = (page - 1) * page_size
    page_projects = projects[start : start + page_size]

    # Recompute price_distribution on the FULL set (not just the page) so summary stats are correct
    price_buckets = defaultdict(int)
    for p in projects:
        mp = p.get("min_price")
        if mp and not p.get("price_on_request"):
            cr = mp / 1e7
            if cr < 0.5:   price_buckets["<50L"] += 1
            elif cr < 1:   price_buckets["50L-1Cr"] += 1
            elif cr < 1.5: price_buckets["1-1.5Cr"] += 1
            elif cr < 2:   price_buckets["1.5-2Cr"] += 1
            elif cr < 3:   price_buckets["2-3Cr"] += 1
            elif cr <= 50: price_buckets["3Cr+"] += 1
    summary["price_distribution"] = dict(price_buckets)

    return {
        "locality":         locality if not city_wide else "",
        "city":             city,
        "city_wide":        city_wide,
        "supply_projects":  page_projects,
        "supply_summary":   summary,
        "infra":            infra,
        "pois":             pois,
        "regulatory":       regulatory,
        "meta": {
            "total":     total,
            "page":      page,
            "page_size": page_size,
            "pages":     max(1, (total + page_size - 1) // page_size),
            "city_wide": city_wide,
            "sources":   ["projects_master", "insightforge.rera_scraped_data"],
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# LOCALITY LISTS
# ─────────────────────────────────────────────────────────────────────────────

def list_localities_from_mongo() -> list:
    """
    Returns distinct canonical (locality, city) pairs sorted by project count.
    Deduplicates by _norm_key so display variants like
    "Almas Guda" / "Almasguda" / "Almas guda" collapse into one entry.
    """
    db = _re()
    # (norm_key, city_lc) → aggregated dict
    groups: dict[tuple, dict] = {}

    for row in db["projects_master"].aggregate([
        {"$match":  {"location.locality": {"$nin": [None,"","NA"]}}},
        {"$group":  {"_id": {"locality":"$location.locality","city":"$location.city"},
                     "count":{"$sum":1}}},
        {"$sort":   {"count": -1}},
    ]):
        raw_loc  = row["_id"].get("locality","")
        raw_city = (row["_id"].get("city") or "").strip()
        city_lc  = raw_city.lower()
        city = "Hyderabad" if city_lc in ("","none","null","hyderabad") else raw_city

        canonical = canonicalize_locality(raw_loc)
        if not canonical:
            continue

        nkey = _norm_key(canonical)
        gkey = (nkey, city.lower())

        if gkey in groups:
            g = groups[gkey]
            g["count"] += row["count"]
            # Prefer display form with higher project count (most-frequent wins)
            if row["count"] > g["_top_count"]:
                g["locality"]   = canonical
                g["_top_count"] = row["count"]
        else:
            groups[gkey] = {
                "locality":   canonical,
                "city":       city,
                "count":      row["count"],
                "source":     "projects_master",
                "_top_count": row["count"],
            }

    MIN_PROJECTS = 1  # show all localities present in the database
    out = sorted(
        [g for g in groups.values() if g["count"] >= MIN_PROJECTS],
        key=lambda x: -x["count"],
    )
    for g in out: g.pop("_top_count", None)
    return out


def get_localities_by_city() -> dict:
    """Source: real_estate.projects_master. Returns {city: [locality,...]}"""
    result = defaultdict(list)
    for item in list_localities_from_mongo():
        result[item["city"]].append(item["locality"])
    return dict(result)
