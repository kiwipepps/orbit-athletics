import sys
import os
import time
from datetime import datetime, timedelta

# 🟢 BULLETPROOF IMPORT PATHING
# Tells Python to look one folder up to find 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import supabase

def clean_stale_events():
    """
    Deletes events that are still marked as 'upcoming' 
    but their start time passed more than 30 days ago.
    """
    print("🧹 Starting cleanup of stale events...")

    # 1. Calculate the cutoff date (30 days ago)
    # We use ISO format because Supabase expects strings for dates
    cutoff_date = (datetime.now() - timedelta(days=30)).isoformat()
    
    try:
        # 2. Perform the Delete
        # Logic: DELETE FROM events WHERE status = 'upcoming' AND start_time < cutoff_date
        response = supabase.table('events').delete().eq('status', 'upcoming').lt('start_time', cutoff_date).execute()
        
        # 3. Report Results
        # Supabase returns the rows it deleted in 'data'
        deleted_rows = response.data
        count = len(deleted_rows)
        
        if count > 0:
            print(f"✅ Cleanup Complete: Removed {count} stale events.")
            # Optional: List what was removed
            # for row in deleted_rows:
            #     print(f"   - Deleted: {row.get('title')} ({row.get('start_time')})")
        else:
            print("✨ No stale events found. Database is clean.")

    except Exception as e:
        print(f"❌ Error during cleanup: {e}")

if __name__ == "__main__":
    clean_stale_events()