import time
import re
import pandas as pd  # 🟢 Added pandas for CSV export
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

# 🔴 REMOVED: from db_utils import upsert_entity, upsert_event

# --- CONFIG ---
schedule_url = "https://www.watchathletics.com/schedule/cat/upcoming-athletics-events"
output_csv = "upcoming_entries_test.csv" # 🟢 File name for export

print("🚀 Launching Browser for Start Lists...")
options = uc.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = uc.Chrome(options=options)

# List to hold all scraped data before saving
scraped_data = [] 

def format_name(raw_name):
    parts = raw_name.strip().split()
    if len(parts) < 2: return raw_name.title()
    first_name = parts[-1].title()
    last_name = ' '.join(parts[:-1]).title()
    return f"{first_name} {last_name}"

def get_startlist_urls(url, driver):
    driver.get(url)
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text().lower()
        title = a.get("title", "").lower()
        if "start list" in text or "start list" in title:
            if "watchathletics.com/article/" in a['href']:
                full_link = a['href'] if a['href'].startswith("http") else f"https://www.watchathletics.com{a['href']}"
                links.append(full_link)
    return list(set(links))

try:
    # --- MAIN SCRAPER LOOP ---
    print("🔍 Searching for start list links...")
    urls = get_startlist_urls(schedule_url, driver)
    print(f"found {len(urls)} urls.")

    for url in urls:
        print(f"📋 Processing: {url}")
        driver.get(url)
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "page-content")))
        except:
            print("   ⚠️ Timed out waiting for content.")
            continue

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Extract Metadata
        title_tag = soup.find("title")
        if title_tag:
             title_text = title_tag.get_text(strip=True)
             meet_name = title_text.split("|")[0].replace("Start List", "").strip()
        else:
             meet_name = "Unknown Meet"
        
        # Find Date
        meet_date = datetime.now().strftime("%Y-%m-%d") # Default
        date_p = soup.find("p", string=re.compile(r"Date:", re.IGNORECASE))
        if date_p:
            try:
                raw_date = date_p.get_text(strip=True).replace("Date:", "").strip()
                raw_date_with_year = f"{raw_date} {datetime.now().year}" 
                meet_date = datetime.strptime(raw_date_with_year, "%A, %B %d %Y").strftime("%Y-%m-%d")
            except:
                pass 

        # Extract Table Rows
        tables = soup.find_all("table")
        for table in tables:
            prev_el = table.find_previous(["h1", "h2", "h3", "h4", "h5", "p", "div", "strong", "span"])
            event_label = prev_el.get_text(strip=True) if prev_el else "Unknown Event"
            
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not headers and table.find("tr"):
                headers = [td.get_text(strip=True).lower() for td in table.find("tr").find_all("td")]
            
            if not any(k in str(headers) for k in ["name", "athlete", "nation", "country", "bib", "lane"]):
                continue

            label_lower = event_label.lower()
            if any(x in label_lower for x in ["women", "woman", "female", "girls", "(w)", "mixed"]): 
                gender = "female"
            elif any(x in label_lower for x in ["men", "man", "male", "boys", "(m)"]):
                gender = "male"
            else:
                print(f"   ⚠️ Skipping table. Could not detect gender from label: '{event_label}'")
                continue 
            
            rows = table.select("tbody tr")
            for row in rows:
                cols = row.find_all("td")
                if not cols: continue
                
                txts = [c.get_text(strip=True) for c in cols]
                
                name = ""
                nationality = "UNK"
                
                for t in txts:
                    if len(t) > 4 and not any(char.isdigit() for char in t) and " " in t: 
                        name = format_name(t)
                    if len(t) == 3 and t.isupper():
                        nationality = t
                
                if name:
                    # 🟢 INSTEAD OF DB, APPEND TO LIST
                    scraped_data.append({
                        "meet_name": meet_name,
                        "date": meet_date,
                        "event_label": event_label,
                        "gender": gender,
                        "athlete_name": name,
                        "nationality": nationality,
                        "url": url
                    })

    # 🟢 SAVE TO CSV AT THE END
    if scraped_data:
        df = pd.DataFrame(scraped_data)
        df.to_csv(output_csv, index=False)
        print(f"✅ Successfully saved {len(df)} rows to {output_csv}")
    else:
        print("⚠️ No data found to save.")

except Exception as e:
    print(f"❌ Critical Error: {e}")
finally:
    print("✅ Finished processing start lists.")
    driver.quit()