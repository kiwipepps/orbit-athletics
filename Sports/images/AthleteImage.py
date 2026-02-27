import time
import requests
import re
# Import shared connection and NEW upsert function
from Sports.utils.db_utils import supabase, upsert_athlete_image

# =========================
# CONFIG
# =========================
SUPABASE_BUCKET = "entity-images/athletes" 
BATCH_SIZE = 10
SLEEP_S = 2.0 # Increased wait time slightly
HEADERS = {
    "User-Agent": "athletics-image-downloader/1.0 (contact: spicysports@gmail.com)" 
}

# =========================
# LOGIC
# =========================

def fetch_pending_downloads():
    """Finds athletes stuck in the 'Staging' phase."""
    response = supabase.table("entities")\
        .select("id, name, nationality, image_source")\
        .eq("image_pending_download", True)\
        .neq("image_source", None)\
        .limit(BATCH_SIZE)\
        .execute()
    return response.data or []

def download_and_upload_to_supabase(entity_id, source_url, filename_base):
    """Downloads from Wiki, uploads to Supabase Storage."""
    
    # 1. VALIDATION
    if not source_url or not source_url.startswith("http"):
        print(f"   ❌ Invalid URL: '{source_url}'")
        return None

    try:
        # 2. Download (With Retry Logic for 429 Errors)
        print(f"   -> Downloading from source...")
        
        for attempt in range(3):
            r = requests.get(source_url, headers=HEADERS, timeout=15)
            
            if r.status_code == 200:
                break # Success!
            elif r.status_code == 429:
                wait_time = int(r.headers.get("Retry-After", 10))
                print(f"      ⚠️ Rate Limited (429). Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                r.raise_for_status()
        else:
            print("      ❌ Failed after 3 retries.")
            return None

        # 3. Upload to Supabase Storage
        # FIX: Send RAW BYTES (r.content) instead of BytesIO object
        clean_name = re.sub(r'[^a-z0-9]', '', filename_base.lower())
        timestamp = int(time.time())
        file_path = f"{clean_name}_{entity_id}_{timestamp}.jpg"
        
        print(f"   -> Uploading to Storage bucket: {SUPABASE_BUCKET}...")
        
        supabase.storage.from_(SUPABASE_BUCKET).upload(
            path=file_path,
            file=r.content, # <--- FIX: Passing raw bytes here
            file_options={"content-type": "image/jpeg", "x-upsert": "true"}
        )
        
        # 4. Get Public URL
        res = supabase.storage.from_(SUPABASE_BUCKET).get_public_url(file_path)
        
        # We need to extract the string from the object
        # In the current supabase-py library, it's stored in .public_url
        if hasattr(res, 'public_url'):
            final_hosted_url = res.public_url
        else:
            final_hosted_url = str(res) # Fallback if library version differs

        return final_hosted_url
            
    except Exception as e:
        print(f"   ❌ Download/Upload failed: {e}")
        return None

def mark_download_complete(entity_id, success=True, error_type=None):
    """Clears the pending flag in the staging area."""
    status = "downloaded" if success else f"failed_{error_type}" if error_type else "download_failed"
    
    supabase.table("entities").update({
        "image_pending_download": False,
        "image_scrape_status": status
    }).eq("id", entity_id).execute()


# =========================
# MAIN LOOP
# =========================
def run():
    print("⬇️ Starting Image Downloader...")
    
    # Fix URL warning by stripping trailing slash if present in env (Optional helper)
    # (Supabase client usually handles this, but printing for sanity)
    
    try: supabase.storage.get_bucket(SUPABASE_BUCKET)
    except: print(f"⚠️ Warning: Bucket '{SUPABASE_BUCKET}' might not exist.")

    while True:
        pending = fetch_pending_downloads()
        if not pending:
            print("💤 No pending downloads. Sleeping...")
            time.sleep(60)
            continue

        print(f"\nProcessing {len(pending)} pending downloads...")
        for ent in pending:
            eid = ent["id"]
            name = ent["name"]
            src = ent["image_source"]
            print(f"Processing: {name}...")

            final_hosted_url = download_and_upload_to_supabase(eid, src, name)

            if final_hosted_url:
                upsert_athlete_image(eid, final_hosted_url)
                mark_download_complete(eid, success=True)
                print(f"   ✅ Success. Staging cleared.")
            else:
                # Mark as failed so we don't loop forever
                mark_download_complete(eid, success=False, error_type="download_error")

            time.sleep(SLEEP_S)

if __name__ == "__main__":
    run()