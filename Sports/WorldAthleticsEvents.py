from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import csv
import os
from datetime import datetime
import re

# =========================
# Date parsing helpers
# =========================

def try_parse_date(text: str, default_year: int | None = None) -> datetime | None:
    """
    Parse a single date that may look like:
      - '27 Aug 2025'
      - '22-Aug-25'
      - '27 Aug'  (year missing; use default_year if provided)
    """
    if not text or not isinstance(text, str):
        return None

    t = text.strip().replace("–", "-")
    t = re.sub(r"\s+", " ", t)

    # If missing year and we have a default year, append it
    if default_year and re.match(r"^\d{1,2}\s+[A-Za-z]{3}$", t, re.I):
        t = f"{t} {default_year}"

    # Normalise case for month names
    t_norm = t.title()

    for fmt in ("%d %b %Y", "%d-%b-%y", "%d %b %y"):
        try:
            return datetime.strptime(t_norm, fmt)
        except ValueError:
            continue

    return None


def parse_wa_date_range(raw: str) -> tuple[datetime | None, datetime | None]:
    """
    Parse World Athletics calendar date strings that can be:
      - '27 Aug 2025'
      - '27 Aug 2025 - 28 Aug 2025'
      - '27 Aug - 28 Aug 2025'            (left side missing year)
      - '28 FEB-01 MAR 2025'              (compact uppercase cross-month)
      - '03-05 OCT 2025'                  (same-month day range)
      - '12-14 SEP 2025'
    Returns (start_dt, end_dt) or (None, None).
    """
    if not raw or not isinstance(raw, str):
        return None, None

    s = raw.strip().replace("–", "-")
    s = re.sub(r"\s+", " ", s)

    # Case: "28 FEB-01 MAR 2025" (cross-month compact range)
    m = re.match(
        r"^(\d{1,2})\s*([A-Za-z]{3})\s*-\s*(\d{1,2})\s*([A-Za-z]{3})\s*(\d{4})$",
        s, re.I
    )
    if m:
        d1, mon1, d2, mon2, y = m.groups()
        start = try_parse_date(f"{d1} {mon1} {y}")
        end = try_parse_date(f"{d2} {mon2} {y}")
        return start, end

    # ✅ NEW Case: "03-05 OCT 2025" or "3-5 Oct 2025" (same-month range)
    m = re.match(r"^(\d{1,2})\s*-\s*(\d{1,2})\s*([A-Za-z]{3})\s*(\d{4})$", s, re.I)
    if m:
        d1, d2, mon, y = m.groups()
        start = try_parse_date(f"{d1} {mon} {y}")
        end   = try_parse_date(f"{d2} {mon} {y}")
        return start, end

    # Standard range: "X - Y"
    if "-" in s:
        parts = [p.strip() for p in s.split("-")]
        if len(parts) == 2:
            left, right = parts

            # Parse right first (usually includes year)
            end = try_parse_date(right)
            if not end:
                return None, None

            # Left may not include year
            start = try_parse_date(left, default_year=end.year)
            if not start:
                return None, None

            return start, end

    # Single date
    dt = try_parse_date(s)
    if dt:
        return dt, dt

    return None, None


def to_iso(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%d") if dt else ""


# =========================
# Setup browser
# =========================

options = Options()
# options.add_argument("--headless")  # Uncomment to run without opening browser
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 20)

# --- Date range ---
start_date = "2026-01-01"
end_date = "2026-02-13"

# --- Output folder and file ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(BASE_DIR, "World Athletics Events")
os.makedirs(output_dir, exist_ok=True)
csv_filename = os.path.join(output_dir, f"world_athletics_events_{start_date}_to_{end_date}.csv")

# --- Competition groups ---
competition_groups = [
    "Olympic Games",
    "Wanda Diamond League Meeting",
    "Wanda Diamond League Finals",
    "World Athletics Series",
    "World Athletics Championships",
    "World Athletics Indoor Tour",
    "World Athletics Continental Tour",
    "World Athletics Indoor Championships",
    "World Athletics Label Road Races",
    "World Athletics Combined Events Tour",
    "World Athletics Race Walking Tour",
    "Area Senior Outdoor Championships",
    "Area Indoor Championships",
    "Area U23 Championships",
    "Area U20 Championships",
    "Area U18 Championships",
    "Area RR Championships",
    "Area Marathon Championships",
    "Area Regional Senior Championships",
    "National Senior Outdoor Championships",
    "National Senior Indoor Championships",
    "National Senior Outdoor Combined Events Championships",
    "National Senior Indoor Combined Events Championships",
    "National Senior Outdoor Race Walking Championships",
    "National Senior Marathon Championships",
    "National Senior Half Marathon Championships",
    "National Senior Road Running Championships",
    "National Senior 10,000m Championships"
]

# =========================
# CSV setup
# =========================

with open(csv_filename, mode="w", newline="", encoding="utf-8-sig") as csvfile:
    writer = csv.writer(csvfile)

    # Added Raw Date for debugging + ISO outputs for Start/End
    writer.writerow([
        "Event Name",
        "Raw Date",
        "Start Date",   # ISO YYYY-MM-DD
        "End Date",     # ISO YYYY-MM-DD
        "Venue",
        "Country",
        "Discipline",
        "Competition Group",
        "Result Link"
    ])

    for group in competition_groups:
        print(f"\n🔍 Searching for: {group}")

        url = (
            "https://worldathletics.org/competition/calendar-results"
            f"?isSearchReset=true&startDate={start_date}&endDate={end_date}"
        )
        driver.get(url)

        # Dismiss cookie popup
        try:
            decline_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "CybotCookiebotDialogBodyButtonDecline"))
            )
            decline_btn.click()
        except:
            pass

        # Open and select group
        try:
            comp_group_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[text()='Competition Group']"))
            )
            comp_group_button.click()
            time.sleep(2)
        except:
            print(f"⚠️ Could not open Competition Group filter for: {group}")
            continue

        try:
            group_option = wait.until(EC.element_to_be_clickable((
                By.XPATH,
                f"//li[contains(@class, 'Filter_filterOption')][.//span[contains(text(),\"{group}\")]]"
            )))
            driver.execute_script("arguments[0].scrollIntoView(true);", group_option)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", group_option)
            time.sleep(4)
        except:
            print(f"⚠️ Could not find filter for: {group}")
            continue

        # --- Pagination ---
        page = 1

        while True:
            try:
                wait.until(EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    "table.ResultsTable_resultsTable__JBH1Y tbody"
                )))
                time.sleep(1)

                rows = driver.find_elements(
                    By.CSS_SELECTOR,
                    "table.ResultsTable_resultsTable__JBH1Y tbody tr"
                )
                print(f"📄 Page {page}: {len(rows)} events")

                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) < 8:
                        continue

                    raw_date = cols[0].text.strip().replace("–", "-")
                    start_dt, end_dt = parse_wa_date_range(raw_date)

                    start_date_iso = to_iso(start_dt)
                    end_date_iso = to_iso(end_dt)

                    name = cols[1].text.strip()
                    venue = cols[3].text.strip()
                    country = cols[4].text.strip()
                    discipline = cols[6].text.strip()
                    comp_group = cols[7].text.strip()

                    # Get result link
                    result_link = ""
                    for col in cols:
                        try:
                            a_tag = col.find_element(By.TAG_NAME, "a")
                            href = a_tag.get_attribute("href")
                            if href and "/results/" in href:
                                result_link = href
                                break
                        except:
                            continue

                    if result_link and not result_link.startswith("http"):
                        result_link = f"https://worldathletics.org{result_link}"

                    writer.writerow([
                        name,
                        raw_date,
                        start_date_iso,
                        end_date_iso,
                        venue,
                        country,
                        discipline,
                        comp_group,
                        result_link
                    ])

                # Go to next page
                try:
                    next_btn = driver.find_element(By.CSS_SELECTOR, "ul.EventCalendar_pagination__SLTyn li.next a")
                    if "aria-disabled=\"true\"" in (next_btn.get_attribute("outerHTML") or ""):
                        break
                    driver.execute_script("arguments[0].click();", next_btn)
                    page += 1
                    time.sleep(2)
                except:
                    break

            except:
                print(f"⚠️ No results found or failed to load page {page} for: {group}")
                break

# --- Done ---
driver.quit()
print(f"\n✅ Saved CSV: {csv_filename}")
