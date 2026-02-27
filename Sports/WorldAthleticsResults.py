import time
import os
import re
import pandas as pd
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
from tqdm import tqdm

# 🟢 IMPORT SHARED LOGIC
from db_utils import upsert_entity, upsert_event, supabase, standardize_event_name

# ==========================================
# 🔧 PART 1: HELPER FUNCTIONS (Cleaned)
# ==========================================
# (clean_discipline removed; using standardize_event_name from db_utils)

# ==========================================
# 🔧 PART 2: ENTITY DETAILS UPDATER
# ==========================================
def update_entity_details(entity_id, discipline_clean):
    if not entity_id or not discipline_clean: return
    # Basic key cleaning for DB column compatibility
    disc_key = discipline_clean.lower().replace(" ", "").replace("shorttrack", "").replace(",", "")
    try:
        res = supabase.table("entities").select("details").eq("id", entity_id).single().execute()
        if not res.data: return
        current_details = res.data.get("details") or {}
        updates = {}
        # Only initialize N/A if missing; do not overwrite existing data
        if f"points_{disc_key}" not in current_details: updates[f"points_{disc_key}"] = "N/A"
        if f"ranking_{disc_key}" not in current_details: updates[f"ranking_{disc_key}"] = "N/A"
        if updates:
            supabase.table("entities").update({"details": {**current_details, **updates}}).eq("id", entity_id).execute()
    except Exception as e: pass

# ==========================================
# 🔧 PART 3: POST-PROCESSING FIX LOGIC
# ==========================================
def normalize_str(s):
    if not s: return ""
    s = s.lower().replace("short track", "").replace("shorttrack", "").replace(",", "")
    return s.replace(" ", "").strip()

def run_combined_events_fix():
    print("\n🧹 Running Post-Scrape Cleanup (Linking Combined Events)...")
    
    SUB_EVENT_whitelist = {
        'Decathlon': ['100m', '400m', '1,500m', '110mH', 'Long Jump', 'High Jump', 'Pole Vault', 'Shot Put', 'Discuss', 'Javelin'],
        'Heptathlon': ['100mH', '200m', '800m', 'High Jump', 'Shot Put', 'Long Jump', 'Javelin', '60mH'],
        'Pentathlon': ['60mH', '800m', 'High Jump', 'Shot Put', 'Long Jump']
    }

    res = supabase.table("events").select("entity_id, start_time, result, event_key, title").or_("event_key.ilike.%Decathlon%,event_key.ilike.%Heptathlon%,event_key.ilike.%Pentathlon%").execute()
    triggers = res.data
    if not triggers: return

    targets = {}
    for t in triggers:
        if not t['start_time']: continue
        date_str = t['start_time'][:10]
        key = (t['entity_id'], date_str)
        raw_str = str(t.get('result', {}))
        evt_key = str(t.get('event_key', ''))
        
        if "Decathlon" in raw_str or "Decathlon" in evt_key: c_type = "Decathlon"
        elif "Heptathlon" in raw_str or "Heptathlon" in evt_key: c_type = "Heptathlon"
        else: c_type = "Pentathlon"
        
        current_title = t.get('title', '')
        if key not in targets:
            targets[key] = {'type': c_type, 'meet_name': current_title}
        elif targets[key]['meet_name'] in ['Decathlon', 'Heptathlon', 'Pentathlon'] and current_title not in ['Decathlon', 'Heptathlon', 'Pentathlon']:
            targets[key]['meet_name'] = current_title

    count = 0
    for (entity_id, date_str), info in targets.items():
        c_type = info['type']
        all_events = supabase.table("events").select("*").eq("entity_id", entity_id).execute().data
        dt_start = datetime.strptime(date_str, "%Y-%m-%d")
        dt_end = dt_start + timedelta(days=3)
        meet_events = [e for e in all_events if e['start_time'] and dt_start <= datetime.strptime(e['start_time'][:10], "%Y-%m-%d") < dt_end]
        
        if len(meet_events) < 2: continue

        real_meet_name = info['meet_name']
        if real_meet_name in ['Decathlon', 'Heptathlon', 'Pentathlon']:
            for child in meet_events:
                if child['title'] not in ['Decathlon', 'Heptathlon', 'Pentathlon']:
                    real_meet_name = child['title']; break

        parent_rows = [e for e in meet_events if e.get('is_parent') is True]
        if parent_rows:
            parent_id = parent_rows[0]['id']
            if parent_rows[0]['title'] in ['Decathlon', 'Heptathlon', 'Pentathlon'] and real_meet_name != parent_rows[0]['title']:
                supabase.table("events").update({"title": real_meet_name}).eq("id", parent_id).execute()
        else:
            parent_key = f"{c_type}|Overall|{real_meet_name}"
            supabase.table("events").upsert({
                "entity_id": entity_id, "title": real_meet_name, "start_time": meet_events[0]['start_time'],
                "category": "Athletics", "status": "completed", "is_parent": True,
                "event_key": parent_key, "result": {"status": "Aggregated"} 
            }, on_conflict="entity_id,event_key").execute()
            
            p_fetch = supabase.table("events").select("id").eq("entity_id", entity_id).eq("event_key", parent_key).execute()
            if not p_fetch.data: continue
            parent_id = p_fetch.data[0]['id']

        valid_subs = SUB_EVENT_whitelist[c_type]
        for child in meet_events:
            if child['id'] == parent_id or child.get('parent_event_id'): continue 
            norm_raw = normalize_str(child.get('result', {}).get('event_name_raw', ''))
            norm_disc = normalize_str(child.get('result', {}).get('discipline_clean', ''))
            norm_type = normalize_str(c_type)
            is_sub = (norm_type in normalize_str(child.get('event_key', '')) or norm_type in norm_raw)
            if not is_sub:
                for sub in valid_subs:
                    if sub.lower() in norm_disc or sub.lower() in norm_raw: is_sub = True; break
            
            if is_sub:
                if norm_disc == norm_type:
                    supabase.table("events").update({"result": child['result']}).eq("id", parent_id).execute()
                    supabase.table("events").delete().eq("id", child['id']).execute()
                else:
                    supabase.table("events").update({"parent_event_id": parent_id}).eq("id", child['id']).execute()
        count += 1
    print(f"✅ Cleanup Complete. Processed {count} groups.")

# ==========================================
# 🚀 PART 4: MAIN SCRAPER
# ==========================================

def parse_any_date_to_iso(date_str: str) -> str | None:
    if not date_str or not isinstance(date_str, str): return None
    s = date_str.strip().replace("–", "-")
    s = re.sub(r"\s+", " ", s)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%b-%y", "%d %b %Y", "%d %b %y"):
        try: return datetime.strptime(s.title(), fmt).strftime("%Y-%m-%d")
        except: continue
    return None

def normalize_meta_label(txt: str) -> str:
    return re.sub(r"\s+", " ", txt).strip() if txt else ""

def extract_round_from_table(table) -> str:
    try:
        strong = table.find_element(By.XPATH, "./ancestor::div[contains(@class,'EventResults_tableWrap')]/preceding-sibling::span[contains(@class,'EventResults_eventMeta')][1]//strong")
        return normalize_meta_label(strong.text)
    except: pass
    try:
        strong = table.find_element(By.XPATH, "./ancestor::section[contains(@class,'EventResults_eventResult')]//span[contains(@class,'EventResults_eventMeta')]//strong")
        return normalize_meta_label(strong.text)
    except: return ""

def build_event_key(event_name_raw: str, round_label: str, meet_name: str) -> str:
    base = (event_name_raw or "").strip()
    rnd = (round_label or "").strip()
    meet = (meet_name or "").strip()
    key = f"{base}|{rnd}" if rnd else base
    return f"{key}|{meet}"

current_dir = os.path.dirname(os.path.abspath(__file__))
events_dir = os.path.join(current_dir, "World Athletics Events")
log_file = os.path.join(current_dir, "scraped_log.txt")

# 🟢 TOGGLE THIS to FORCE RESCRAPE
FORCE_RESCRAPE = False

print("🚀 Launching Browser...")
options = uc.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
prefs = {"profile.managed_default_content_settings.images": 2}
options.add_experimental_option("prefs", prefs)
options.page_load_strategy = "eager"

# 🟢 FIX: Force version 144 to match your browser
driver = uc.Chrome(options=options, version_main=144)
wait = WebDriverWait(driver, 10)

processed_urls = set()
if os.path.exists(log_file) and not FORCE_RESCRAPE:
    with open(log_file, "r") as f: processed_urls = set(line.strip() for line in f)
    print(f"🔄 Resuming... Found {len(processed_urls)} scraped events.")

df = None
try:
    if not os.path.exists(events_dir): exit()
    csv_files = [f for f in os.listdir(events_dir) if f.startswith("world_athletics_events") and f.endswith(".csv")]
    if not csv_files: exit()
    latest_file = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(events_dir, f)))
    df = pd.read_csv(os.path.join(events_dir, latest_file))
    print(f"📂 Loaded {len(df)} meets.")
except: driver.quit(); exit()

link_to_date = {}
if df is not None:
    for _, row in df.iterrows():
        rl = row.get("Result Link")
        if not isinstance(rl, str) or not rl.strip(): continue
        key = rl.replace("https://worldathletics.org", "").strip()
        start_date_val = row.get("Start Date")
        iso_date = parse_any_date_to_iso(str(start_date_val)) if start_date_val else None
        link_to_date[key] = iso_date or ""

links_to_scrape = []
if df is not None:
    links_to_scrape = [url for url in df.get("Result Link", pd.Series()).dropna() if isinstance(url, str)]

try:
    for base_url in tqdm(links_to_scrape, desc="Scraping Meets", unit="meet"):
        full_url = base_url if base_url.startswith("http") else f"https://worldathletics.org{base_url}"
        if not FORCE_RESCRAPE and full_url in processed_urls: continue

        key = full_url.replace("https://worldathletics.org", "")
        iso_date = link_to_date.get(key, "") or None

        tqdm.write(f"🔄 Opening: {full_url}")
        driver.get(full_url)
        try: WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.ID, "CybotCookiebotDialogBodyButtonDecline"))).click()
        except: pass

        if not iso_date:
            try: iso_date = parse_any_date_to_iso(driver.find_element(By.CLASS_NAME, "competitions-header__date").text.strip())
            except: iso_date = datetime.now().strftime("%Y-%m-%d")

        days = ["1"]
        try:
            day_select = driver.find_elements(By.CSS_SELECTOR, "select[name='day-select'] option")
            if day_select: days = [opt.get_attribute("value") for opt in day_select if opt.get_attribute("value")]
        except: pass

        try: meet_name_text = driver.find_element(By.TAG_NAME, "h1").text.strip()
        except: meet_name_text = "Unknown Meet"

        for day in days:
            if len(days) > 1 or day != "1":
                driver.get(f"{full_url}?day={day}")
                time.sleep(1.5)

            try:
                results_tables = driver.find_elements(By.TAG_NAME, "table")
                for table in results_tables:
                    try: event_name_raw = table.find_element(By.XPATH, "./preceding::h2[1]").text.strip()
                    except: continue
                    if "4x" in event_name_raw.lower() or "relay" in event_name_raw.lower(): continue

                    # 🟢 USE SHARED STANDARDIZATION LOGIC
                    clean_disc_name = standardize_event_name(event_name_raw)
                    
                    gender = "male" if "men" in event_name_raw.lower() else "female"
                    
                    combined_context = None
                    raw_lower = event_name_raw.lower()
                    for c_type in ["Decathlon", "Heptathlon", "Pentathlon"]:
                        if c_type.lower() in raw_lower:
                            is_summary = (clean_disc_name == c_type) 
                            combined_context = {'type': c_type, 'is_child': not is_summary}
                            break

                    round_label = extract_round_from_table(table)
                    event_key = build_event_key(event_name_raw, round_label, meet_name_text)

                    for row in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) < 5: continue
                        try:
                            place = cols[0].text.strip()
                            if place.lower() == "pos": continue
                            name = cols[1].text.strip().split("\n")[0].strip()
                            nationality = cols[3].text.strip()
                            mark = cols[4].text.strip()

                            entity_id = upsert_entity({
                                "name": name, "nationality": nationality, "gender": gender, "category": "Sport"
                            }, discipline=clean_disc_name)
                            
                            update_entity_details(entity_id, clean_disc_name)

                            if entity_id:
                                upsert_event(entity_id, {
                                    "meet_name": meet_name_text, "event_name": event_name_raw,
                                    "event_key": event_key, "date": iso_date, "status": "completed",
                                    "result_data": {
                                        "place": place, "mark": mark, "discipline_clean": clean_disc_name,
                                        "round_label": round_label, "event_name_raw": event_name_raw
                                    }
                                }, combined_context=combined_context)
                        except: continue
            except Exception as e: tqdm.write(f"   ⚠️ Error Day {day}: {e}")

        with open(log_file, "a") as f:
            f.write(full_url + "\n")
            processed_urls.add(full_url)

except KeyboardInterrupt:
    tqdm.write("\n🛑 Stopped by user.")
except Exception as e:
    tqdm.write(f"\n❌ Critical Error: {e}")
finally:
    try:
        if driver.service.process: driver.quit()
    except: pass
    
    run_combined_events_fix()