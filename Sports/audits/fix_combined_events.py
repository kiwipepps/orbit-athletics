import sys
import os
from datetime import datetime, timedelta

# 🟢 BULLETPROOF IMPORT PATHING
# Tells Python to look one folder up to find 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import supabase

# 🟢 ROBUST WHITELIST (Space-Insensitive)
SUB_EVENT_whitelist = {
    'Decathlon': [
        '100m', '400m', '1500m', '110mH', '110mHurdles',
        'LongJump', 'HighJump', 'PoleVault', 'ShotPut', 'Discus', 'Javelin',
        '100Metres', '400Metres', '1500Metres', '110MetresHurdles'
    ],
    'Heptathlon': [
        '100mH', '100mHurdles', '200m', '800m',
        'HighJump', 'ShotPut', 'LongJump', 'Javelin',
        '60m', '1000m', '60mH', '60mHurdles', 'PoleVault',
        '100MetresHurdles', '200Metres', '800Metres', '60Metres'
    ],
    'Pentathlon': [
        '60mH', '60mHurdles', '800m',
        'HighJump', 'ShotPut', 'LongJump',
        '60MetresHurdles', '800Metres'
    ]
}

def normalize(s):
    return str(s).replace(" ", "").lower() if s else ""

def fix_combined_events():
    print("🔧 Starting Date-Based Combined Events Fix...")

    # 1. Find Triggers
    print("   -> Scanning for entries...")
    res = supabase.table("events").select("entity_id, start_time, result, event_key, title").or_("event_key.ilike.%Decathlon%,event_key.ilike.%Heptathlon%,event_key.ilike.%Pentathlon%").execute()
    triggers = res.data
    
    if not triggers:
        print("   ❌ No trigger rows found.")
        return

    # 2. Group by (Athlete, Date)
    # We strip time to just YYYY-MM-DD to handle slight time diffs
    targets = {}
    for t in triggers:
        if not t['start_time']: continue
        date_str = t['start_time'][:10] # YYYY-MM-DD
        key = (t['entity_id'], date_str)
        
        raw_str = str(t.get('result', {}))
        evt_key = str(t.get('event_key', ''))
        
        if "Decathlon" in raw_str or "Decathlon" in evt_key: c_type = "Decathlon"
        elif "Heptathlon" in raw_str or "Heptathlon" in evt_key: c_type = "Heptathlon"
        else: c_type = "Pentathlon"
        
        # We store the 'meet_name' from the trigger to use for Parent naming if needed
        # Prefer the title that ISN'T just "Decathlon" if possible
        current_title = t.get('title', '')
        if key not in targets:
            targets[key] = {'type': c_type, 'meet_name': current_title}
        elif targets[key]['meet_name'] in ['Decathlon', 'Heptathlon', 'Pentathlon'] and current_title not in ['Decathlon', 'Heptathlon', 'Pentathlon']:
            # Upgrade to a better meet name if we find one (e.g. "World Championships")
            targets[key]['meet_name'] = current_title

    print(f"   -> Identified {len(targets)} date-groups to process.")

    processed_count = 0
    
    # 3. Process
    for (entity_id, date_str), info in targets.items():
        c_type = info['type']
        meet_name_guess = info['meet_name']
        
        # Define Date Window (Date to Date+2 to catch multi-day events)
        # However, supabase filtering is tricky. Let's just grab everything for that athlete and filter in python.
        # Fetching all events for the athlete is safer to ensure we get everything.
        all_athlete_events = supabase.table("events").select("*").eq("entity_id", entity_id).execute().data
        
        # Filter strictly by date window (Start Date <= E_Date <= Start Date + 2 days)
        dt_start = datetime.strptime(date_str, "%Y-%m-%d")
        dt_end = dt_start + timedelta(days=3) # Wide net for multi-day
        
        meet_events = []
        for e in all_athlete_events:
            if not e['start_time']: continue
            e_dt = datetime.strptime(e['start_time'][:10], "%Y-%m-%d")
            if dt_start <= e_dt < dt_end:
                meet_events.append(e)
        
        if len(meet_events) < 2:
            continue

        print(f"\n   📍 Processing {c_type} on {date_str} ({len(meet_events)} rows)")

        # Find/Create Parent
        parent_rows = [e for e in meet_events if e.get('is_parent') is True]
        
        if parent_rows:
            parent_id = parent_rows[0]['id']
            # If the parent has a generic title "Decathlon", usually that's fine/preferred for UI.
            # But the key needs to be unique.
        else:
            # Check if we have a better meet name from the children
            # If 'meet_name_guess' is "Decathlon", try to find a real meet name from children
            real_meet_name = meet_name_guess
            if real_meet_name in ['Decathlon', 'Heptathlon', 'Pentathlon']:
                for child in meet_events:
                    if child['title'] not in ['Decathlon', 'Heptathlon', 'Pentathlon']:
                        real_meet_name = child['title']
                        break
            
            parent_key = f"{c_type}|Overall|{real_meet_name}"
            
            # Use the date from the first event
            start_time_iso = meet_events[0]['start_time']

            parent_payload = {
                "entity_id": entity_id,
                "title": c_type, # UI shows "Decathlon"
                "start_time": start_time_iso,
                "category": "Athletics",
                "status": "completed",
                "is_parent": True,
                "event_key": parent_key,
                "result": {"status": "Aggregated"} 
            }
            supabase.table("events").upsert(parent_payload, on_conflict="entity_id,event_key").execute()
            
            p_fetch = supabase.table("events").select("id").eq("entity_id", entity_id).eq("event_key", parent_key).execute()
            if not p_fetch.data: 
                print("      ❌ Failed to create parent.")
                continue
            parent_id = p_fetch.data[0]['id']

        # Link Children
        valid_sub_names = SUB_EVENT_whitelist[c_type]
        
        for child in meet_events:
            if child['id'] == parent_id: continue 
            if child.get('parent_event_id'): continue 

            raw_name = child.get('result', {}).get('event_name_raw', '')
            disc_clean = child.get('result', {}).get('discipline_clean', '')
            evt_key = child.get('event_key', '')

            norm_raw = normalize(raw_name)
            norm_disc = normalize(disc_clean)
            norm_key = normalize(evt_key)
            norm_type = normalize(c_type)

            is_sub_event = False
            
            if norm_type in norm_key or norm_type in norm_raw: is_sub_event = True
            
            if not is_sub_event:
                for sub in valid_sub_names:
                    if sub.lower() in norm_disc or sub.lower() in norm_raw:
                        is_sub_event = True
                        break
            
            if is_sub_event:
                if norm_disc == norm_type:
                    supabase.table("events").update({"result": child['result']}).eq("id", parent_id).execute()
                    supabase.table("events").delete().eq("id", child['id']).execute()
                    print(f"      Merged Summary -> Parent")
                else:
                    supabase.table("events").update({"parent_event_id": parent_id}).eq("id", child['id']).execute()
                    print(f"      Linked {disc_clean} ({child['title']})")
        
        processed_count += 1

    print(f"\n✅ Fix Complete. Processed {processed_count} date-groups.")

if __name__ == "__main__":
    fix_combined_events()