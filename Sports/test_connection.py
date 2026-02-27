import os
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. Load Keys
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

print(f"🔗 URL Found: {url}")
print(f"🔑 Key Found: {'Yes' if key else 'No'} (First 5 chars: {key[:5] if key else 'None'})")

if not url or not key:
    print("❌ Error: Missing keys in .env file")
    exit()

# 2. Connect
try:
    supabase: Client = create_client(url, key)
    print("✅ Client initialized.")
except Exception as e:
    print(f"❌ Client Init Failed: {e}")
    exit()

# 3. Test Insert into 'entities'
print("\n--- 1. Testing INSERT into 'entities' ---")
try:
    # We will try to insert a fake test user
    test_slug = "debug-test-user-usa"
    
    # Check if exists first to avoid duplicate error
    existing = supabase.table("entities").select("id").eq("slug", test_slug).execute()
    
    if existing.data:
        print(f"ℹ️ Test user already exists. ID: {existing.data[0]['id']}")
        entity_id = existing.data[0]['id']
    else:
        payload = {
            "name": "Debug Test User",
            "slug": test_slug,
            "category": "Test",
            "nationality": "USA",
            "gender": "male",
            "details": {}
        }
        res = supabase.table("entities").insert(payload).execute()
        entity_id = res.data[0]['id']
        print(f"✅ SUCCESS: Created Entity. ID: {entity_id}")

except Exception as e:
    print(f"🚨 FAILED to insert Entity. Error details:\n{e}")
    # Stop here if entity fails, because event will fail too
    exit()

# 4. Test Insert into 'events'
print("\n--- 2. Testing INSERT into 'events' ---")
try:
    event_payload = {
        "entity_id": entity_id,
        "name": "Debug Meet 2024",
        "date": "2024-01-01",
        "category": "Test",
        "description": "100m Debug",
        "status": "test",
        "metadata": {"mark": "9.99"}
    }
    
    res_event = supabase.table("events").insert(event_payload).execute()
    print(f"✅ SUCCESS: Created Event. ID: {res_event.data[0]['id']}")

except Exception as e:
    print(f"🚨 FAILED to insert Event. Error details:\n{e}")