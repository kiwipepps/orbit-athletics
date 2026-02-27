import sys
import os
import time
import re
import unicodedata
from datetime import datetime, timezone, timedelta
import requests

# 🟢 BULLETPROOF IMPORT PATHING
# Tells Python to look one folder up to find 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import supabase
from utils.country_constants import COUNTRY_MAP

# =========================
# CONFIG
# =========================
WIKI_API = "https://en.wikipedia.org/w/api.php"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
UA = "athletics-image-matcher/7.0 (contact: your_email@example.com)"

BATCH_LIMIT = 50
SLEEP_S = 1.0
ENABLE_REFRESH_MODE = True
REFRESH_DAYS = 90  

session = requests.Session()
session.headers.update({"User-Agent": UA})

# =========================
# 🟢 FILTERING LISTS
# =========================

# ✅ POSITIVE LIST
ATHLETICS_OK = {
    "athletics", "track and field", "track & field", "runner", "sprinter", 
    "hurdler", "marathoner", "marathon", "half marathon", "cross country", "xc",
    "decathlete", "heptathlete", "pentathlete", "long jump", "high jump", 
    "triple jump", "pole vault", "shot put", "discus", "javelin", "hammer throw",
    "race walk", "race walker", "steeplechase", "steeplechaser", "middle distance",
    "long distance", "olympic champion", "world champion", "diamond league",
    "olympian", "olympic", "world championship", "national record", "personal best",
    "100m", "200m", "400m", "800m", "1500m", "5000m", "10,000m", "4x100m", "4x400m"
}

# ❌ STRICT BAD LIST
STRICT_BAD_WORDS = {
    "squash", "badminton", "pickleball", "padel", "table tennis", "ping pong",
    "footballer", "soccer", "rugby", "cricket", "baseball", "basketball", 
    "volleyball", "netball", "handball", "hockey", "lacrosse",
    "swimmer", "swimming", "diver", "water polo", "artistic swimming",
    "cyclist", "cycling", "bmx", "velodrome", "tour de france",
    "gymnast", "gymnastics", "cheerleading",
    "boxer", "boxing", "wrestler", "wrestling", "mma", "ufc", "judo", "karate",
    "golfer", "pga tour", "motorsport", "f1", "nascar", "rally driver",
    "skier", "skiing", "snowboarder", "biathlon", "bobsleigh",
    "rower", "rowing", "canoe", "kayak", "surfer", "skateboarder",
    "weightlifter", "bodybuilder", "crossfit", "equestrian", "jockey", "triathlete",
    "actor", "actress", "film director", "movie star", "screenwriter", 
    "tv host", "comedian", "stand-up", "supermodel",
    "musician", "singer", "songwriter", "guitarist", "drummer", "rapper", "pianist",
    "politician", "senator", "congressman", "parliament", "prime minister",
    "attorney", "lawyer", "surgeon", "professor", "triathlon", "triathlete"
}

# ⚠️ BAD PHRASES
BAD_PHRASES = [
    "academy award", "oscar winner", "oscar nominee", "emmy award", "grammy award",
    "golden globe", "bafta", "film festival", "movie premiere",
    "royal family", "crown prince", "his royal highness",
    "tennis player", "football player", "basketball player", "horse riding"
]

# =========================
# HELPER FUNCTIONS
# =========================

def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def first_last(name: str):
    parts = normalize(name).split()
    return (parts[0], parts[-1]) if len(parts) >= 2 else (None, None)

def is_valid_athletics_profile(text_blob, nationality_keywords=None):
    if not text_blob: return False
    text_lower = text_blob.lower()
    
    # 1. Check STRICT Single Words
    for word in STRICT_BAD_WORDS:
        if f" {word} " in f" {text_lower} ": 
            return False

    # 2. Check BAD PHRASES
    for phrase in BAD_PHRASES:
        if phrase in text_lower:
            return False

    # 3. Check NATIONALITY (If we know it)
    if nationality_keywords:
        has_nat = any(nk.lower() in text_lower for nk in nationality_keywords)
        if not has_nat:
            return False

    # 4. Check POSITIVE signals
    for word in ATHLETICS_OK:
        if word in text_lower:
            return True

    return False

def is_athletics_page(title: str, snippet: str, nationality_keywords=None) -> bool:
    blob = normalize(title + " " + re.sub(r"<.*?>", "", snippet))
    return is_valid_athletics_profile(blob, nationality_keywords)

def pick_best_title(name: str, nationality_code: str = None):
    first, last = first_last(name)
    if not first or not last: return None
    
    # 1. Resolve Nationality Code to keywords
    nat_keywords = COUNTRY_MAP.get(nationality_code, []) if nationality_code else []
    
    # 2. Build Queries
    queries = []
    
    if nat_keywords:
        primary_country = nat_keywords[0]
        queries.append(f"{first} {last} {primary_country} athlete")
        queries.append(f"{first} {last} {primary_country}")
    
    queries.append(f"{first} {last} athlete")
    queries.append(f"{first} {last}")
    
    for q in queries:
        try:
            r = session.get(WIKI_API, params={
                "action": "query", "list": "search", "srsearch": q,
                "srlimit": 5, "format": "json"
            }, timeout=10)
            results = r.json().get("query", {}).get("search", [])
        except: 
            continue
            
        for res in results:
            # 3. Pass Nationality Keywords to Validation
            if is_athletics_page(res["title"], res.get("snippet", ""), nat_keywords):
                tnorm = normalize(res["title"])
                if first in tnorm and last in tnorm: 
                    return res["title"]
    return None

def get_commons_package(title: str):
    try:
        r = session.get(WIKI_API, params={"action":"query","titles":title,"prop":"pageimages","format":"json"})
        pg = next(iter(r.json().get("query",{}).get("pages",{}).values()), {})
        img_file = pg.get("pageimage")
        if not img_file: return None
        
        cr = session.get(COMMONS_API, params={"action":"query","titles":f"File:{img_file}","prop":"imageinfo","iiprop":"url|extmetadata","iiurlwidth":800,"format":"json"})
        info = next(iter(cr.json().get("query",{}).get("pages",{}).values()), {}).get("imageinfo",[{}])[0]
        return {"original": info.get("thumburl") or info.get("url"), "license": info.get("extmetadata",{}).get("LicenseShortName",{}).get("value")}
    except: return None

# =========================
# DATABASE OPS
# =========================

def fetch_entities(limit: int):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)).isoformat()
    q = supabase.table("entities").select("id,name,image_source,nationality").order("id").limit(limit)
    if ENABLE_REFRESH_MODE:
        q = q.or_(f"image_checked_at.is.null,image_checked_at.lt.{cutoff}")
    return q.execute().data or []

def update_staging_status(entity_id, status, pending_download=False, source_url=None, license_text=None):
    payload = {
        "image_scrape_status": status,
        "image_checked_at": datetime.now(timezone.utc).isoformat(),
        "image_pending_download": pending_download
    }
    if source_url: payload["image_source"] = source_url 
    if license_text: payload["image_license"] = license_text
    
    supabase.table("entities").update(payload).eq("id", entity_id).execute()

# =========================
# MAIN
# =========================
def run():
    print("🔎 Starting Image Linker (with Comprehensive Nationality Check)...")
    while True:
        batch = fetch_entities(BATCH_LIMIT)
        if not batch:
            print("💤 No candidates found. Sleeping...")
            time.sleep(60)
            continue

        print(f"\nProcessing batch of {len(batch)}...")
        for ent in batch:
            name = ent["name"]
            entity_id = ent["id"]
            nationality = ent.get("nationality") 
            current_source_url = ent.get("image_source")

            title = pick_best_title(name, nationality)
            
            if not title:
                print(f"   ❌ No Match: {name} ({nationality or 'UNK'})")
                update_staging_status(entity_id, "no_match")
                continue

            pkg = get_commons_package(title)
            if not pkg or not pkg["original"]:
                print(f"   ❌ No Image: {name} (Wiki page exists)")
                update_staging_status(entity_id, "no_image")
                continue

            new_url = pkg["original"]

            if current_source_url == new_url:
                print(f"   -> Image unchanged for {name}. Touching timestamp.")
                update_staging_status(entity_id, status="verified_unchanged", pending_download=False)
            else:
                print(f"   ✅ NEW IMAGE: {name} ({nationality or 'UNK'}) -> {new_url}")
                update_staging_status(
                    entity_id, 
                    status="candidate_found", 
                    pending_download=True, 
                    source_url=new_url,
                    license_text=pkg["license"]
                )
            
            time.sleep(SLEEP_S)
    print("Done.")

if __name__ == "__main__":
    run()