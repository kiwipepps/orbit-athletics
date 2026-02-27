from flask import Flask, render_template_string, request, jsonify
from db_utils import supabase
import collections
import json

app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>👯 Smart Duplicate Audit</title>
    <style>
        body { font-family: -apple-system, sans-serif; background: #f4f4f4; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }
        h1 { text-align: center; color: #333; }
        .comparison { display: flex; gap: 30px; margin-top: 30px; align-items: stretch; justify-content: center; }
        .profile { flex: 1; padding: 20px; border: 2px solid #eee; border-radius: 12px; text-align: center; background: #fff; max-width: 400px; display: flex; flex-direction: column; justify-content: space-between; }
        img { width: 200px; height: 200px; object-fit: cover; border-radius: 10px; margin-bottom: 15px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        h2 { margin: 10px 0 5px; font-size: 24px; }
        .tags { display: flex; flex-wrap: wrap; gap: 5px; justify-content: center; margin-bottom: 15px; }
        .tag { background: #e3f2fd; color: #007bff; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; border: 1px solid #b3d7ff; }
        .tag.empty { background: #f8f9fa; color: #aaa; border-color: #ddd; font-weight: normal; }
        .meta { text-align: left; font-size: 13px; color: #777; margin-top: 15px; border-top: 1px solid #eee; padding-top: 10px; }
        .btn { padding: 15px 20px; border: none; border-radius: 8px; cursor: pointer; font-weight: bold; font-size: 16px; width: 100%; margin-top: 10px; transition: transform 0.1s; }
        .btn:active { transform: scale(0.98); }
        .btn-merge { background: #007bff; color: white; }
        .btn-merge:hover { background: #0056b3; }
        .btn-skip { background: #28a745; color: white; margin-top: 40px; width: 300px; display: block; margin-left: auto; margin-right: auto; font-size: 18px; padding: 15px; }
        .btn-skip:hover { background: #218838; }
        .arrow { font-size: 40px; align-self: center; color: #ccc; }
    </style>
</head>
<body>
    <div class="container">
        <h1>👯 Duplicate Audit Queue</h1>
        <p style="text-align:center; color:#666;">Fetched {{ total_scanned }} UNAUDITED profiles</p>
        
        {% if group %}
            <div style="text-align:center; margin-bottom:20px;">
                <span style="font-size: 18px; font-weight: bold;">Matches Found: "{{ group[0].name }}"</span>
            </div>
            <div class="comparison">
                <div class="profile">
                    <div>
                        <img src="{{ group[0].image_source }}" onerror="this.src='https://via.placeholder.com/200?text=No+Image'">
                        <h2>{{ group[0].nationality or 'UNK' }}</h2>
                        <div class="tags">
                            {% for d in group[0].disciplines %}<span class="tag">{{ d }}</span>{% endfor %}
                        </div>
                    </div>
                    <div>
                        <div class="meta"><strong>ID:</strong> ...{{ group[0].id[-6:] }}<br><strong>Category:</strong> {{ group[0].category or '-' }}</div>
                        <br>
                        <button class="btn btn-merge" onclick="merge('{{ group[1].id }}', '{{ group[0].id }}')">⬅️ Keep Left (Merge Right into Left)</button>
                    </div>
                </div>
                <div class="arrow">↔️</div>
                <div class="profile">
                    <div>
                        <img src="{{ group[1].image_source }}" onerror="this.src='https://via.placeholder.com/200?text=No+Image'">
                        <h2>{{ group[1].nationality or 'UNK' }}</h2>
                        <div class="tags">
                            {% for d in group[1].disciplines %}<span class="tag">{{ d }}</span>{% endfor %}
                        </div>
                    </div>
                    <div>
                        <div class="meta"><strong>ID:</strong> ...{{ group[1].id[-6:] }}<br><strong>Category:</strong> {{ group[1].category or '-' }}</div>
                        <br>
                        <button class="btn btn-merge" onclick="merge('{{ group[0].id }}', '{{ group[1].id }}')">Keep Right ➡️ (Merge Left into Right)</button>
                    </div>
                </div>
            </div>
            <button class="btn btn-skip" onclick="markDifferent(['{{ group[0].id }}', '{{ group[1].id }}'])">✅ Confirm: Different People</button>
        {% else %}
            <div style="text-align:center; padding: 50px;">
                <h2>🎉 Queue Empty!</h2>
                <p>No more duplicates found among the unaudited profiles.</p>
            </div>
        {% endif %}
    </div>
    <script>
        function merge(duplicate_id, master_id) {
            if(!confirm("Are you sure? This moves history to the Master and deletes the duplicate.")) return;
            fetch('/perform_merge', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ master_id: master_id, duplicate_id: duplicate_id })
            }).then(res => res.json()).then(data => {
                if(data.success) location.reload();
                else alert("Error: " + data.error);
            });
        }
        function markDifferent(ids) {
            fetch('/mark_audited', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ ids: ids })
            }).then(res => res.json()).then(data => {
                if(data.success) location.reload();
                else alert("Error: " + data.error);
            });
        }
    </script>
</body>
</html>
"""

def fetch_unaudited_entities():
    """
    Pagination loop to fetch ONLY entities that have NOT been audited.
    """
    print("⏳ Fetching UNAUDITED profiles...")
    all_rows = []
    start = 0
    batch_size = 1000
    
    while True:
        # 🟢 OPTIMIZED: Only fetch rows where name_audited is NULL or FALSE
        # Supabase syntax: or=(name_audited.is.null,name_audited.is.false)
        res = supabase.table("entities")\
            .select("id,name,nationality,image_source,details,category,name_audited")\
            .or_("name_audited.is.null,name_audited.is.false")\
            .range(start, start + batch_size - 1)\
            .execute()
        
        rows = res.data
        if not rows:
            break
            
        all_rows.extend(rows)
        start += batch_size
        # Stop if we have a decent amount to work with to save memory (optional)
        if len(all_rows) > 5000: 
            print("   -> Hit batch limit (5000), processing chunk...")
            break
        print(f"   -> Fetched {len(all_rows)} rows so far...")

    print(f"✅ Total loaded for audit: {len(all_rows)}")
    return all_rows

def extract_disciplines_from_details(details):
    if not details or not isinstance(details, dict): return []
    events = set()
    for key in details.keys():
        if key.startswith("ranking_"):
            events.add(key.replace("ranking_", ""))
    return sorted(list(events))

@app.route('/')
def home():
    rows = fetch_unaudited_entities()
    
    groups = collections.defaultdict(list)
    for row in rows:
        if not row['name']: continue
        name_key = row['name'].strip().lower()
        row['disciplines'] = extract_disciplines_from_details(row.get('details'))
        groups[name_key].append(row)
    
    duplicates = [g for g in groups.values() if len(g) > 1]
    
    if not duplicates:
        return render_template_string(HTML_TEMPLATE, group=None, total_scanned=len(rows), total_dupes=0)

    return render_template_string(HTML_TEMPLATE, group=duplicates[0][:2], total_scanned=len(rows), total_dupes=len(duplicates))

@app.route('/perform_merge', methods=['POST'])
def perform_merge():
    data = request.json
    try:
        supabase.rpc("merge_entities", {
            "master_id": data['master_id'], 
            "duplicate_id": data['duplicate_id']
        }).execute()
        supabase.table("entities").update({"name_audited": True}).eq("id", data['master_id']).execute()
        return jsonify({"success": True})
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route('/mark_audited', methods=['POST'])
def mark_audited():
    ids = request.json.get('ids', [])
    try:
        supabase.table("entities").update({"name_audited": True}).in_("id", ids).execute()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

if __name__ == '__main__':
    print("🚀 Smart Audit Tool running on http://127.0.0.1:5001")
    app.run(debug=True, port=5001)