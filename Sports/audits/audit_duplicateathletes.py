from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import sys
import os
import collections
import time
import re

# 🟢 BULLETPROOF IMPORT PATHING
# Tells Python to look one folder up to find 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import supabase

# ===========================
# CONFIGURATION
# ===========================
# 🔴 Set to True ONLY after verifying the logs!
COMMIT_CHANGES = True  

# EVENT DICTIONARY (Mappings)
EVENT_MAPPING = {
    "SP": "SHOT PUT", "HJ": "HIGH JUMP", "PV": "POLE VAULT",
    "LJ": "LONG JUMP", "TJ": "TRIPLE JUMP", "DT": "DISCUS",
    "HT": "HAMMER", "JT": "JAVELIN", "WT": "WEIGHT THROW",
    "MH": "HURDLES", "SC": "STEEPLECHASE", "XC": "CROSS COUNTRY",
    "DEC": "DECATHLON", "HEP": "HEPTATHLON", "PEN": "PENTATHLON",
    "MAR": "MARATHON", "HM": "HALF MARATHON",
    "100M": "100 METRES", "200M": "200 METRES",
    "400M": "400 METRES", "800M": "800 METRES",
    "1500M": "1500 METRES"
}

def setup_driver():
    chrome_options = Options()
    # chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--window-size=1920,1080")
    prefs = {"credentials_enable_service": False, "profile.password_manager_enabled": False}
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def clean_text_set(text):
    if not text: return set()
    clean = re.sub(r'[^a-zA-Z0-9\s]', '', text).upper()
    return set(clean.split())

def get_match_score(target_name, web_name):
    """
    Returns a score based on how many name parts match.
    Target: "Fatouma CONDE" (2 parts)
    Web 1: "CONDE Fatouma" -> Matches {CONDE, FATOUMA} -> Score 2
    Web 2: "CONDE ." -> Matches {CONDE} -> Score 1
    """
    s1 = clean_text_set(target_name)
    s2 = clean_text_set(web_name)
    if not s1 or not s2: return 0
    return len(s1.intersection(s2))

def extract_local_disciplines(details):
    if not details or not isinstance(details, dict): return set()
    final_tokens = set()
    for key in details.keys():
        clean_key = key.replace("ranking_", "").replace("points_", "").replace("-", "").upper()
        if clean_key in EVENT_MAPPING:
            final_tokens.update(EVENT_MAPPING[clean_key].split())
        else:
            parts = re.findall(r'[A-Za-z]+|\d+', clean_key)
            for part in parts:
                if part in EVENT_MAPPING:
                    final_tokens.update(EVENT_MAPPING[part].split())
                else:
                    final_tokens.add(part)
    return final_tokens

def disciplines_compatible(local_events, wa_text):
    if not local_events: return True 
    if not wa_text: return True
    wa_tokens = clean_text_set(wa_text)
    for token in local_events:
        if token in wa_tokens: return True
        if token.replace("M", "") in wa_tokens: return True
    return False

def handle_cookies(driver):
    try:
        banner_btn = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Allow all cookies') or contains(text(), 'Accept')]"))
        )
        banner_btn.click()
        time.sleep(1.0)
    except Exception: pass

def search_world_athletics(driver, name):
    clean_name = name.replace("%20", " ")
    driver.get("https://worldathletics.org/athletes/search")
    handle_cookies(driver)
    
    try:
        search_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[class*='AthleteSearch_searchInput'], input[type='search']"))
        )
        search_input.click()
        search_input.send_keys(Keys.CONTROL + "a")
        search_input.send_keys(Keys.DELETE)
        search_input.send_keys(clean_name)
        time.sleep(0.5)
        search_input.send_keys(Keys.RETURN)
        
        first_name = clean_name.split()[0]
        WebDriverWait(driver, 5).until(
            EC.text_to_be_present_in_element((By.CSS_SELECTOR, "tbody"), first_name)
        )

        rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
        results = []
        
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 4: continue
            
            wa_name = driver.execute_script("return arguments[0].innerText;", cols[0]).strip()
            wa_discipline = driver.execute_script("return arguments[0].innerText;", cols[1]).strip()
            wa_sex = driver.execute_script("return arguments[0].innerText;", cols[2]).strip()
            raw_country = driver.execute_script("return arguments[0].innerText;", cols[3]).strip()
            wa_country = re.sub(r'[^A-Z]', '', raw_country)[:3]
            
            results.append({
                "name": wa_name, 
                "country": wa_country,
                "discipline": wa_discipline,
                "sex": wa_sex
            })
        return results
    except Exception: return []

def analyze_group(driver, group):
    target_name = group[0]['name']
    all_wa_results = search_world_athletics(driver, target_name)
    
    # 🟢 NEW: BEST MATCH SCORING
    # 1. Score every result
    scored_results = []
    for wa in all_wa_results:
        score = get_match_score(target_name, wa['name'])
        if score > 0:
            scored_results.append((score, wa))
    
    if not scored_results:
        return "SKIP", f"Target '{target_name}' not found."

    # 2. Find the Highest Score (e.g., 2)
    max_score = max(s[0] for s in scored_results)
    
    # 3. Keep ONLY the winners (Score 2) and discard losers (Score 1)
    best_wa_results = [s[1] for s in scored_results if s[0] == max_score]
    
    # 4. Filter by Discipline/Gender Compatibility
    relevant_wa_results = []
    for wa in best_wa_results:
        is_useful = False
        for local_p in group:
            local_events = extract_local_disciplines(local_p.get('details'))
            if disciplines_compatible(local_events, wa['discipline']):
                is_useful = True
                break
        if is_useful:
            relevant_wa_results.append(wa)

    if not relevant_wa_results:
         # Debug: Show why we skipped (e.g. "CONDE ." had score 1, but "Fatouma" had score 2)
        return "SKIP", f"Filtered noise. Best match score was {max_score}. No compatible profiles left."

    # --- LOGIC 1: SEPARATE (Country Conflict) ---
    matched_countries = set()
    for local_p in group:
        local_nat = local_p['nationality']
        for wa in relevant_wa_results:
            if wa['country'] == local_nat:
                matched_countries.add(local_nat)
                
    if len(matched_countries) >= 2:
        return "SEPARATE", f"Verified distinct countries: {list(matched_countries)}"

    # Check for Multiple WA Profiles for SAME Country
    country_counts = collections.Counter([r['country'] for r in relevant_wa_results])
    for country, count in country_counts.items():
        if count >= 2:
            return "SEPARATE", f"Found multiple high-quality matches for {country}."

    # --- LOGIC 2: MERGE (Single Valid Match) ---
    if len(relevant_wa_results) == 1:
        wa_p = relevant_wa_results[0]
        wa_nat = wa_p['country']
        
        master_match = next((p for p in group if p['nationality'] == wa_nat), None)
        
        if master_match:
            # VETO CHECK
            duplicates = [p for p in group if p['id'] != master_match['id']]
            for dup in duplicates:
                dup_events = extract_local_disciplines(dup.get('details'))
                if not disciplines_compatible(dup_events, wa_p['discipline']):
                    return "SEPARATE", f"Discipline Mismatch! Duplicate ({dup['nationality']}) clashes with Master."

            return "MERGE", f"Single Best Match ({wa_nat}) found. Merging."

    wa_countries = [r['country'] for r in relevant_wa_results]
    return "SKIP", f"Ambiguous. Local: {[p['nationality'] for p in group]} vs WA: {wa_countries}"

def run_auto_audit():
    print("🤖 Starting Best-Match Auditor...")
    print("⏳ Fetching profiles...")
    all_rows = []
    start = 0
    while True:
        res = supabase.table("entities").select("id,name,nationality,details").is_("name_audited", "false").range(start, start + 999).execute()
        if not res.data: break
        all_rows.extend(res.data)
        start += 1000

    groups = collections.defaultdict(list)
    for row in all_rows:
        if row['name']: groups[row['name'].strip().lower()].append(row)
    
    duplicates = [g for g in groups.values() if len(g) > 1]
    print(f"🔍 Found {len(duplicates)} duplicate groups.")

    driver = setup_driver()
    try:
        for i, group in enumerate(duplicates):
            name = group[0]['name']
            nats = [p['nationality'] for p in group]
            print(f"\n[{i+1}/{len(duplicates)}] Checking: {name} {nats}")
            
            action, reason = analyze_group(driver, group)
            print(f"   👉 Decision: {action} ({reason})")
            
            if COMMIT_CHANGES:
                if action == "SEPARATE":
                    ids = [p['id'] for p in group]
                    supabase.table("entities").update({"name_audited": True}).in_("id", ids).execute()
                    print("      ✅ Saved: Marked as Different")
                elif action == "MERGE":
                    all_wa = search_world_athletics(driver, name)
                    scored = [(get_match_score(name, r['name']), r) for r in all_wa]
                    if not scored: continue
                    max_s = max(s[0] for s in scored)
                    best = [s[1] for s in scored if s[0] == max_s]
                    
                    # Logic reuse for safety in commit block
                    if len(best) == 1:
                        master_nat = best[0]['country']
                        master = next((p for p in group if p['nationality'] == master_nat), None)
                        if master:
                            dupes = [p for p in group if p['id'] != master['id']]
                            for d in dupes:
                                try:
                                    supabase.rpc("merge_entities", {"master_id": master['id'], "duplicate_id": d['id']}).execute()
                                    print(f"      ✅ Merged {d['nationality']} -> {master['nationality']}")
                                except: pass
                            supabase.table("entities").update({"name_audited": True}).eq("id", master['id']).execute()
            time.sleep(1.0)
    finally:
        driver.quit()
        print("👋 Browser closed.")

if __name__ == "__main__":
    run_auto_audit()