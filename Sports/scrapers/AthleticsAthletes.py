import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
from datetime import datetime
from tqdm import tqdm
import sys
import os

# 🟢 BULLETPROOF IMPORT PATHING
# This tells Python to look one folder up (in the Sports folder) so it can find 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import upsert_entity, standardize_event_name

def convert_date(date_str):
    try:
        return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
    except Exception:
        return None

def format_name(raw_name):
    parts = raw_name.lstrip('. ').strip().split()
    if len(parts) < 2:
        return parts[0].title()
    surname_prefixes = {"de", "van", "von", "da", "del", "di", "la", "le"}
    surname_parts = []
    first_names = []
    for part in reversed(parts):
        if part.lower() in surname_prefixes or len(surname_parts) == 0:
            surname_parts.insert(0, part.upper())
        else:
            first_names.insert(0, part.title())
    return f"{' '.join(first_names)} {' '.join(surname_parts)}"

# --- SETUP DRIVER ---
print("🚀 Launching Browser...")
options = uc.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
# options.add_argument("--headless") 
driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 20)

# --- CONFIG ---
genders = [
    {
        "label": "women", "gender": "female",
        "events": ["100m", "200m", "400m", "800m", "1500m", "5000m", "10000m", "100mh", "400mh", "high-jump", "pole-vault", "long-jump", "triple-jump", "shot-put", "discus-throw", "javelin-throw", "heptathlon"]
    },
    {
        "label": "men", "gender": "male",
        "events": ["100m", "200m", "400m", "800m", "1500m", "5000m", "10000m", "110mh", "400mh", "high-jump", "pole-vault", "long-jump", "triple-jump", "shot-put", "discus-throw", "javelin-throw", "decathlon"]
    }
]

total_synced = 0

try:
    for g in genders:
        gender_label = g["label"]
        gender_value = g["gender"]
        base_url = f"https://worldathletics.org/world-rankings/{{}}/{gender_label}"

        for event_slug in tqdm(g["events"], desc=f"Scraping {gender_label.upper()}"):
            # 🟢 Clean the event name using shared logic
            clean_event = standardize_event_name(event_slug)
            
            url = base_url.format(event_slug)
            driver.get(url)
            
            # --- HANDLE COOKIE POPUP ---
            try:
                cookie_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "CybotCookiebotDialogBodyButtonDecline"))
                )
                cookie_btn.click()
            except:
                pass 

            # Scrape first 2 pages
            page = 1
            while page <= 2:
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
                    
                    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                    if not rows:
                        print(f"⚠️ No rows found for {clean_event} page {page}")
                        break

                    for row in rows:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) >= 5:
                            try:
                                rank_txt = cols[0].text.strip()
                                rank = int(rank_txt) if rank_txt.isdigit() else 999
                                name = format_name(cols[1].text.strip())
                                dob_raw = cols[2].text.strip()
                                nationality = cols[3].text.strip()
                                points_txt = cols[4].text.strip()
                                points = int(points_txt) if points_txt.isdigit() else 0
                                
                                dob = convert_date(dob_raw)

                                # --- PREPARE DATA (With Clean Keys) ---
                                entity_data = {
                                    "name": name,
                                    "nationality": nationality,
                                    "dob": dob,
                                    "gender": gender_value,
                                    "category": "Sport",
                                    "subcategory": "Athletics",
                                    "details": {
                                        # 🟢 Using 'clean_event' (e.g., 'Shot Put')
                                        f"ranking_{clean_event}": rank, 
                                        f"points_{clean_event}": points
                                    }
                                }
                                
                                upsert_entity(entity_data)
                                total_synced += 1
                            except Exception as e:
                                continue

                    # Next Page
                    try:
                        next_btn = driver.find_element(By.CSS_SELECTOR, "a.btn--pag-next")
                        driver.execute_script("arguments[0].click();", next_btn)
                        page += 1
                        time.sleep(2)
                    except:
                        break 

                except Exception as e:
                    break

except KeyboardInterrupt:
    print("\n🛑 Stopped by user.")
except Exception as e:
    print(f"\n❌ Critical Error: {e}")
finally:
    print(f"\n✅ Sync Complete. {total_synced} athletes processed.")
    driver.quit()