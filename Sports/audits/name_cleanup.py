import collections
from Sports.utils.db_utils import supabase

def fetch_all_entities():
    print("⏳ Fetching ALL entities from database (this may take a moment)...")
    
    all_rows = []
    start = 0
    batch_size = 1000  # Max allowed by Supabase
    
    while True:
        # Fetch range: 0-999, then 1000-1999, etc.
        print(f"   -> Fetching rows {start} to {start + batch_size}...")
        res = supabase.table("entities")\
            .select("id,name,nationality")\
            .range(start, start + batch_size - 1)\
            .execute()
        
        rows = res.data
        if not rows:
            break  # Stop when no more data comes back
            
        all_rows.extend(rows)
        start += batch_size

    print(f"✅ Total entities fetched: {len(all_rows)}")
    return all_rows

def group_duplicates(rows):
    print(f"🔍 Analyzing {len(rows)} records for duplicates...")
    groups = collections.defaultdict(list)
    
    for row in rows:
        # Normalize name: remove extra spaces and lowercase
        if not row['name']: continue
        name_key = row['name'].strip().lower()
        groups[name_key].append(row)
        
    # Return only names with > 1 entry
    duplicates = {name: entries for name, entries in groups.items() if len(entries) > 1}
    return duplicates

def perform_safe_merges():
    # 1. GET ALL DATA
    rows = fetch_all_entities()
    duplicates = group_duplicates(rows)

    if not duplicates:
        print("🎉 No duplicates found!")
        return

    print(f"👉 Found {len(duplicates)} duplicate groups to analyze.\n")

    success_count = 0
    skip_count = 0

    for name_key, entries in duplicates.items():
        # Use the first real name for display (since key is lowercase)
        display_name = entries[0]['name']
        
        ids = [e['id'] for e in entries]
        nationalities = [e['nationality'] for e in entries]

        # Filter out None/Null nationalities for checking conflict
        valid_nats = set(n for n in nationalities if n and n != "UNK")

        # CASE 1: SAFE MERGE
        # Either 0 valid nationalities (all UNK) OR exactly 1 valid nationality (all USA)
        if len(valid_nats) <= 1:
            unique_nat_label = list(valid_nats)[0] if valid_nats else "UNK"
            print(f"✅ Safe Merge: {display_name} ({unique_nat_label})")
            
            # Prefer to keep the ID that HAS the nationality if possible
            # Sort entries so the "best" one is first (e.g., has nationality)
            entries.sort(key=lambda x: x['nationality'] or "", reverse=True)
            
            master_id = entries[0]['id']
            dupes_to_merge = [e['id'] for e in entries[1:]]

            for dup_id in dupes_to_merge:
                try:
                    supabase.rpc("merge_entities", {
                        "master_id": master_id, 
                        "duplicate_id": dup_id
                    }).execute()
                    print(f"   -> Merged {dup_id} into {master_id}")
                    success_count += 1
                except Exception as e:
                    print(f"   ❌ Error merging {display_name}: {e}")

        # CASE 2: CONFLICT (Skip)
        else:
            print(f"⚠️ Skipping Mixed Nationalities: {display_name} {list(valid_nats)}")
            skip_count += 1

    print("\n------------------------------------------------")
    print(f"🎉 DONE! Merged {success_count} profiles.")
    print(f"👉 Skipped {skip_count} profiles (Use the Audit Tool for these).")

if __name__ == "__main__":
    perform_safe_merges()