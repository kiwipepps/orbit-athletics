import os
import re
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. SETUP & CONNECTION ---
# 🟢 BULLETPROOF .ENV PATHING (Forces it to look one folder up)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(BASE_DIR, "..", ".env"))
load_dotenv(dotenv_path=env_path)

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    raise ValueError(f"Missing SUPABASE_URL or SUPABASE_KEY. Please check your .env file at {env_path}")

supabase: Client = create_client(url, key)

# --- 2. HELPER FUNCTIONS ---

def create_slug(name, nationality):
    nat_str = nationality if nationality and nationality.lower() != "none" else "unk"
    raw_string = f"{name} {nat_str}"
    slug = raw_string.lower().strip()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s]+', '-', slug)
    return slug

def upsert_entity(data_or_name, nationality=None, discipline=None):
    """
    Smart Upsert with 'UNK' merging logic.
    """
    if isinstance(data_or_name, dict):
        athlete_data = data_or_name
        name = athlete_data.get("name")
        nationality = athlete_data.get("nationality")
        gender = athlete_data.get("gender", "male")
        dob = athlete_data.get("date_of_birth") or athlete_data.get("dob")
    else:
        name = data_or_name
        gender = "male"
        dob = None

    if nationality in ["UNK", "None", ""]: 
        nationality = None

    target_slug = create_slug(name, nationality) 
    fallback_slug = create_slug(name, "unk")     
    
    new_details = {}
    if isinstance(data_or_name, dict) and "details" in data_or_name:
         new_details = data_or_name["details"]

    entity_id = None
    existing_data = None
    
    try:
        response = supabase.table("entities").select("*").eq("slug", target_slug).execute()
        if response.data:
            existing_data = response.data[0]
        elif nationality:
            response_fallback = supabase.table("entities").select("*").eq("slug", fallback_slug).execute()
            if response_fallback.data:
                existing_data = response_fallback.data[0]
                supabase.table("entities").update({
                    "slug": target_slug,
                    "nationality": nationality
                }).eq("id", existing_data["id"]).execute()

    except Exception as e:
        print(f"Error querying Supabase: {e}")
        return None

    if existing_data:
        entity_id = existing_data["id"]
        current_details = existing_data.get("details") or {}
        needs_update = False
        for k, v in new_details.items():
            if k not in current_details or current_details[k] != v:
                current_details[k] = v
                needs_update = True
        update_payload = {}
        if needs_update: update_payload["details"] = current_details
        if dob and not existing_data.get("date_of_birth"): update_payload["date_of_birth"] = dob
        if update_payload:
            supabase.table("entities").update(update_payload).eq("id", entity_id).execute()
        return entity_id
    else:
        payload = {
            "name": name,
            "slug": target_slug,
            "category": "Sport",
            "subcategory": "Athletics",
            "nationality": nationality or "UNK",
            "gender": gender,
            "details": new_details,
            "date_of_birth": dob
        }
        insert_res = supabase.table("entities").insert(payload).execute()
        return insert_res.data[0]["id"]

def upsert_athlete_image(entity_id, public_url):
    try:
        supabase.table("entity_images").upsert({
            "entity_id": entity_id,
            "image_url": public_url,
            "updated_at": "now()"
        }, on_conflict="entity_id").execute()
    except Exception as e:
        print(f"   ❌ Database update failed: {e}")

# 🟢 NEW: Find or Create the Main 'Decathlon' Card
def get_or_create_parent_event(entity_id, meet_name, date_iso, combined_type):
    # Unique key for the summary card: "Decathlon|Overall|Meet Name"
    parent_key = f"{combined_type}|Overall|{meet_name}"
    
    # 1. Check if it exists
    res = supabase.table("events").select("id").match({
        "entity_id": entity_id,
        "event_key": parent_key
    }).execute()
    
    if res.data:
        return res.data[0]['id']

    # 2. Create if missing
    new_parent = {
        "entity_id": entity_id,
        "title": combined_type, # Just "Decathlon"
        "start_time": f"{date_iso}T00:00:00Z",
        "category": "Athletics",
        "status": "completed",
        "event_key": parent_key,
        "is_parent": True,
        "result": {"status": "Aggregated"} 
    }
    
    ins = supabase.table("events").insert(new_parent).execute()
    return ins.data[0]['id']

# 🟢 UPDATED: Handles Parent/Child Linking
def upsert_event(entity_id, event_data, combined_context=None):
    iso_timestamp = f"{event_data['date']}T00:00:00Z"
    full_title = event_data["meet_name"]

    event_key = event_data.get("event_key")
    if not event_key:
        evt_name = event_data.get("event_name", "")
        if evt_name:
            event_key = f"{evt_name}|Upcoming"
        else:
            res = event_data.get("result_data") or {}
            disc = res.get("discipline_clean") or ""
            rnd = res.get("round_label") or ""
            event_key = f"{disc}|{rnd}".strip("|") if (disc or rnd) else full_title

    parent_id = None
    is_parent = False
    
    # Logic: If this is a Decathlon sub-event, link it to the main card
    if combined_context:
        c_type = combined_context['type'] # "Decathlon"
        
        if combined_context['is_child']:
            # Find/Create the Parent Card
            parent_id = get_or_create_parent_event(entity_id, full_title, event_data['date'], c_type)
        else:
            # This IS the Summary Card
            event_key = f"{c_type}|Overall|{full_title}"
            is_parent = True
            full_title = c_type 

    payload = {
        "entity_id": entity_id,
        "title": full_title,
        "start_time": iso_timestamp,
        "category": "Athletics",
        "status": event_data["status"],
        "result": event_data["result_data"],
        "event_key": event_key,
        "parent_event_id": parent_id,
        "is_parent": is_parent
    }

    try:
        supabase.table("events").upsert(
            payload,
            on_conflict="entity_id,event_key"
        ).execute()
    except Exception as e:
        print(f"Error upserting event: {e}")

def standardize_event_name(name):
    """
    Centralized logic to clean event names from any source (URL slugs, Result tables, CSVs).
    Input examples: "shot-put", "Women's 100m Final", "10000m", "3000mSC"
    Output examples: "Shot Put", "100m", "10,000m", "3,000mSC"
    """
    if not name: return ""
    
    # 1. Normalize basic text
    name = name.lower()
    name = name.replace("short track", "")
    name = name.replace("cross country", "XC")
    # Remove noise words often found in result tables
    name = re.sub(r"\b(women's|men's|women|men|final|heats|heat|semi-final|round \d+|qualification|group [a-z]|senior race|u20 race|race)\b", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    
    # 2. Map Common Shortcodes & Slugs
    slug_map = {
        "sp": "Shot Put", "shotput": "Shot Put", "shot-put": "Shot Put",
        "dt": "Discus", "discus": "Discus", "discus-throw": "Discus",
        "jt": "Javelin", "javelin": "Javelin", "javelin-throw": "Javelin",
        "ht": "Hammer Throw", "hammer": "Hammer Throw", "hammer-throw": "Hammer Throw",
        "lj": "Long Jump", "longjump": "Long Jump", "long-jump": "Long Jump",
        "tj": "Triple Jump", "triplejump": "Triple Jump", "triple-jump": "Triple Jump",
        "hj": "High Jump", "highjump": "High Jump", "high-jump": "High Jump",
        "pv": "Pole Vault", "polevault": "Pole Vault", "pole-vault": "Pole Vault",
        "wt": "Weight Throw", "weightthrow": "Weight Throw", "weight-throw": "Weight Throw",
        "dec": "Decathlon", "hep": "Heptathlon", "pen": "Pentathlon"
    }
    if name in slug_map:
        return slug_map[name]

    # 3. Hurdles Standardization
    if "110m" in name and "hurdles" in name: return "110mH"
    if "100m" in name and "hurdles" in name: return "100mH"
    if "400m" in name and "hurdles" in name: return "400mH"
    
    # 4. Steeplechase
    if "3000m" in name and "steeplechase" in name: return "3,000mSC"
    if "2000m" in name and "steeplechase" in name: return "2,000mSC"

    # 5. Walks
    if "walk" in name:
        if "20km" in name: return "20km Walk"
        if "35km" in name: return "35km Walk"
        if "50km" in name: return "50km Walk"
        if "10000" in name: return "10,000m Walk"
        # Generic fallback
        return name.replace("walk", " Walk").title()

    # 6. Distance Formatting (Add commas: 10000m -> 10,000m)
    # Fix spacing first: "10000 m" -> "10000m"
    name = name.replace("metres", "m").replace("meters", "m")
    name = re.sub(r"(\d)\s+m\b", r"\1m", name)
    
    match = re.match(r"^(\d+)m$", name)
    if match:
        dist = int(match.group(1))
        return f"{dist:,}m"

    # 7. Fallback Title Case
    return name.title()