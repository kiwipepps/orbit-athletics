from Sports.utils.db_utils import supabase

BUCKET = "entity-images"
PREFIX = "athletes/"

def cleanup_storage():
    print(f"🧹 Starting Storage Cleanup for {BUCKET}/{PREFIX}...")

    # 1. List all files in the folder
    # We use a large limit to catch everything
    res = supabase.storage.from_(BUCKET).list(PREFIX, {"limit": 10000})
    
    if not res:
        print("Empty bucket or error fetching list.")
        return

    # 2. Group files by athlete ID
    # Filename format: name_ID_timestamp.jpg
    files_by_athlete = {}

    for file in res:
        name = file['name']
        if name == ".emptyFolderPlaceholder": continue
        
        parts = name.replace(".jpg", "").split("_")
        if len(parts) < 2: continue
        
        # The ID is the second to last part (before the timestamp)
        athlete_id = parts[-2]
        
        if athlete_id not in files_by_athlete:
            files_by_athlete[athlete_id] = []
        
        files_by_athlete[athlete_id].append({
            "full_path": f"{PREFIX}{name}",
            "created_at": file['created_at']
        })

    # 3. Identify and Delete Old Files
    total_deleted = 0
    for athlete_id, versions in files_by_athlete.items():
        if len(versions) <= 1:
            continue
            
        # Sort by creation date (newest first)
        versions.sort(key=lambda x: x['created_at'], reverse=True)
        
        # Keep the first one, delete the rest
        to_delete = [v['full_path'] for v in versions[1:]]
        
        print(f"   - Athlete {athlete_id}: Keeping newest, deleting {len(to_delete)} old files.")
        supabase.storage.from_(BUCKET).remove(to_delete)
        total_deleted += len(to_delete)

    print(f"✅ Cleanup Complete. Removed {total_deleted} duplicate images.")

if __name__ == "__main__":
    cleanup_storage()