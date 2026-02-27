from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime, timedelta
from tqdm import tqdm  # 🟢 IMPORT TQDM
import sys
import os

# 🟢 BULLETPROOF IMPORT PATHING
# Tells Python to look one folder up to find 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import supabase

# ===========================
# CONFIGURATION
# ===========================
COMMIT_CHANGES = True
BATCH_SIZE = 1000
AUDIT_INTERVAL_DAYS = 90

def setup_driver():
    chrome_options = Options()
    # chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--window-size=1920,1080")
    prefs = {"credentials_enable_service": False, "profile.password_manager_enabled": False}
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def get_cutoff_date():
    return (datetime.utcnow() - timedelta(days=AUDIT_INTERVAL_DAYS)).isoformat()

def handle_cookies(driver):
    try:
        banner_btn = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Allow all cookies') or contains(text(), 'Accept')]"))
        )
        banner_btn.click()
        time.sleep(1.0)
    except Exception: pass

def get_wa_gender(driver, name):
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
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 4: continue
            
            wa_name = driver.execute_script("return arguments[0].innerText;", cols[0]).strip()
            wa_sex = driver.execute_script("return arguments[0].innerText;", cols[2]).strip().lower()
            
            if first_name.lower() in wa_name.lower():
                if "women" in wa_sex or "female" in wa_sex: return "female"
                if "men" in wa_sex or "male" in wa_sex: return "male"
        return None
    except Exception: return None

def update_gender_and_timestamp(entity_id, new_gender, current_meta):
    if not current_meta: current_meta = {}
    current_meta["gender_checked_at"] = datetime.utcnow().isoformat()
    
    payload = {"audit_meta": current_meta}
    if new_gender:
        payload["gender"] = new_gender
        
    supabase.table("entities").update(payload).eq("id", entity_id).execute()

def run_gender_cleanse():
    cutoff = get_cutoff_date()
    
    # 1. Count Total (For Progress Bar)
    print("📊 Calculating total records to audit...")
    count_res = supabase.table("entities")\
        .select("*", count="exact", head=True)\
        .or_(f"audit_meta->>gender_checked_at.is.null,audit_meta->>gender_checked_at.lt.{cutoff}")\
        .execute()
    
    total_to_audit = count_res.count
    print(f"🚀 Found {total_to_audit} records to process.")

    if total_to_audit == 0:
        print("✅ Everything is up to date!")
        return

    driver = setup_driver()
    total_fixed = 0
    
    # 2. Initialize Progress Bar
    with tqdm(total=total_to_audit, desc="Auditing Genders", unit="athlete") as pbar:
        while True:
            # Fetch batch
            res = supabase.table("entities")\
                .select("id, name, gender, audit_meta")\
                .or_(f"audit_meta->>gender_checked_at.is.null,audit_meta->>gender_checked_at.lt.{cutoff}")\
                .limit(BATCH_SIZE)\
                .execute()
            
            rows = res.data
            if not rows:
                break
            
            for row in rows:
                wa_gender = get_wa_gender(driver, row['name'])
                
                if wa_gender and wa_gender != row['gender']:
                    # Use tqdm.write so it doesn't break the progress bar layout
                    tqdm.write(f"🔍 FIX: {row['name']} | {row['gender']} -> {wa_gender}")
                    if COMMIT_CHANGES:
                        update_gender_and_timestamp(row['id'], wa_gender, row.get('audit_meta'))
                    total_fixed += 1
                else:
                    if COMMIT_CHANGES:
                        update_gender_and_timestamp(row['id'], None, row.get('audit_meta'))
                
                time.sleep(0.5)
                pbar.update(1)  # 🟢 Update Bar per athlete

    driver.quit()
    print(f"🎉 Done. Fixed {total_fixed} profiles.")

if __name__ == "__main__":
    run_gender_cleanse()