import time
import os
import csv
import re
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

# Set up undetected Chrome driver
options = uc.ChromeOptions()
# options.add_argument("--headless")  # Uncomment to run headless
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = uc.Chrome(options=options)

schedule_url = "https://www.watchathletics.com/schedule/cat/2"

# Extract all result URLs from the schedule page
def get_results_urls(schedule_url, driver):
    print(f"◎️ Accessing schedule page: {schedule_url}")
    driver.get(schedule_url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    results_links = []
    for a_tag in soup.find_all("a", href=True):
        text = a_tag.get_text(strip=True).lower()
        href = a_tag["href"]
        if "results" in text and href.startswith("https://www.watchathletics.com/page/"):
            results_links.append(href)

    print(f"✅ Found {len(results_links)} results pages.")
    return results_links

def format_name(raw_name):
    parts = raw_name.strip().split()
    if len(parts) < 2:
        return raw_name.title()
    first_name = parts[-1].title()
    last_name = ' '.join(parts[:-1]).title()
    return f"{first_name} {last_name}"

def match_event(label_text, gender):
    label_lower = label_text.lower()

    # Prioritise specific hurdles naming
    if "100m hurdles" in label_lower:
        return "100mh" if gender == "female" else "110mh"
    if "110m hurdles" in label_lower:
        return "110mh"
    if "400m hurdles" in label_lower:
        return "400mh"

    event_keywords = [
        "100m", "200m", "400m", "800m", "1500m", "5000m", "10000m",
        "100mh", "110mh", "400mh", "3000msc", "high-jump", "pole-vault", "long-jump",
        "triple-jump", "shot-put", "discus-throw", "hammer-throw", "javelin-throw",
        "road-running", "marathon", "race-walking", "20km-race-walking", "35km-race-walking",
        "heptathlon", "decathlon", "cross-country"
    ]

    replacements = {
        "steeplechase": "3000msc",
        "race walk": "race-walking",
        "20km race walk": "20km-race-walking",
        "35km race walk": "35km-race-walking",
        "high jump": "high-jump",
        "pole vault": "pole-vault",
        "long jump": "long-jump",
        "triple jump": "triple-jump",
        "shot put": "shot-put",
        "discus throw": "discus-throw",
        "hammer throw": "hammer-throw",
        "javelin throw": "javelin-throw"
    }

    for key, val in replacements.items():
        if key in label_lower:
            return val

    for ev in event_keywords:
        if ev in label_lower or ev.replace("-", " ") in label_lower:
            return ev

    clean_label = re.sub(r"(?i)women's|men's|women|men", "", label_text).strip()
    return clean_label

all_data_rows = []

for results_url in get_results_urls(schedule_url, driver):
    print(f"\n⚜️ Loading page: {results_url}")
    driver.get(results_url)

    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "page-content")))
    except:
        print(f"❌ Timeout waiting for content on {results_url}")
        continue

    soup = BeautifulSoup(driver.page_source, "html.parser")
    content = soup.find("div", id="page-content")
    if not content:
        print(f"❌ Could not find content on page: {results_url}")
        continue

    elements = content.find_all(['p', 'figure'])

    # Meet metadata
    title_text = soup.find("title").get_text(strip=True)
    meet_name_raw = title_text.split("|")[0].strip() if "|" in title_text else title_text
    meet_name = re.sub(r"results|\b20\d{2}\b", "", meet_name_raw, flags=re.IGNORECASE).strip()
    meet_name = re.sub(r"^[-=+@]\s*", "", meet_name)

    year_match = re.search(r"20\d{2}", results_url)
    year = year_match.group() if year_match else "2024"

    meet_date = ""
    date_p = soup.find("p", string=re.compile(r"Date:", re.IGNORECASE))
    if date_p:
        raw_date = re.sub(r"Date:\s*", "", date_p.get_text(strip=True), flags=re.IGNORECASE)
        try:
            parsed_date = datetime.strptime(f"{raw_date} {year}", "%A, %B %d %Y")
            meet_date = parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            meet_date = raw_date

    latest_label = None
    latest_gender = None

    for el in elements:
        if el.name == 'p':
            label = el.get_text(strip=True)
            if not label or label == '\u00a0':
                continue
            label_lower = label.lower()
            if "women" in label_lower:
                latest_gender = "female"
            elif "men" in label_lower:
                latest_gender = "male"
            latest_label = label

        elif el.name == 'figure' and el.find('table') and latest_label and latest_gender:
            matched_event = match_event(latest_label, latest_gender)
            table = el.find('table')

            field_map = {
                'rank': ['rank', 'place', 'pos.'],
                'name': ['name', 'athlete'],
                'nationality': ['nat', 'country', 'nation'],
                'time': ['time', 'mark', 'result', 'score', 'best']
            }

            header_mapping = {}
            header_row = table.select_one("thead tr") or table.select_one("tr")
            if header_row:
                header_cells = header_row.find_all(['th', 'td'])
                for idx, cell in enumerate(header_cells):
                    header_text = cell.get_text(strip=True).lower()
                    for key, aliases in field_map.items():
                        if any(alias in header_text for alias in aliases):
                            header_mapping[key] = idx

            for row in table.select("tbody tr"):
                cells = [td.get_text(strip=True) for td in row.select("td")]
                try:
                    raw_rank = cells[header_mapping['rank']]
                    rank = re.sub(r'[\.\=]+$', '', raw_rank).strip()
                    if not rank.isdigit():
                        print(f"⚠️ Skipping row with non-numeric rank: {rank}")
                        continue

                    raw_name = cells[header_mapping['name']]
                    name = format_name(raw_name)

                    if 'nationality' in header_mapping:
                        nationality = cells[header_mapping['nationality']]
                    else:
                        nationality = "UNK"
                        for cell in cells:
                            if re.fullmatch(r'[A-Z]{3}', cell):
                                nationality = cell
                                break

                    time_or_score = cells[header_mapping['time']]

                    all_data_rows.append([
                        meet_name, meet_date, rank, name, matched_event, latest_gender, nationality, time_or_score
                    ])
                except Exception as e:
                    print(f"⚠️ Skipped row due to missing field: {e}")

# Save results
if all_data_rows:
    output_dir = "Athletics - Results"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "results.csv")
    with open(filename, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerow(["meet", "date", "rank", "name", "event", "gender", "nationality", "result"])
        writer.writerows(all_data_rows)
    print(f"✅ Saved {len(all_data_rows)} rows to {filename}")
else:
    print("❌ No data found.")

driver.quit()
