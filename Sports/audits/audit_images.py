from flask import Flask, render_template_string, request, jsonify
from Sports.utils.db_utils import supabase
from datetime import datetime, timezone
import json

app = Flask(__name__)

# Config
IMAGES_PER_BATCH = 100

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Athlete Image Queue</title>
    <style>
        body { font-family: -apple-system, sans-serif; background: #f0f2f5; padding: 20px; }
        .header { text-align: center; margin-bottom: 20px; position: sticky; top: 0; background: #f0f2f5; z-index: 100; padding: 10px; border-bottom: 1px solid #ddd; }
        
        /* GRID */
        .grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); 
            gap: 20px; 
            max-width: 1400px; 
            margin: 0 auto; 
        }
        .card { 
            background: white; border-radius: 12px; overflow: hidden; 
            box-shadow: 0 2px 8px rgba(0,0,0,0.1); position: relative;
        }
        .card img { 
            width: 100%; height: 200px; object-fit: cover; border-bottom: 1px solid #eee; 
        }
        .info { padding: 15px; }
        .name { font-weight: bold; font-size: 16px; margin-bottom: 5px; display: block; }
        .nat { color: #666; font-size: 13px; background: #eee; padding: 2px 6px; border-radius: 4px; }
        
        /* DELETE BUTTON */
        .btn-delete {
            background: #ff4d4f; color: white; border: none; padding: 10px 0;
            width: 100%; cursor: pointer; font-weight: bold; font-size: 14px;
        }
        .btn-delete:hover { background: #d9363e; }
        
        /* MARK AUDITED BUTTON */
        .btn-next {
            background: #28a745; color: white; border: none; padding: 15px 40px;
            font-size: 18px; font-weight: bold; border-radius: 8px; cursor: pointer;
            box-shadow: 0 4px 6px rgba(0,0,0,0.2);
        }
        .btn-next:hover { background: #218838; }

        .deleted { display: none; } /* Hide deleted cards completely so they aren't marked as audited */
    </style>
</head>
<body>
    <div class="header">
        <h1>📸 Unaudited Queue ({{ count }} Remaining in Batch)</h1>
        <p>Review these images. Delete the bad ones.</p>
        <button class="btn-next" onclick="finishBatch()">✅ Mark These as Good & Load Next Batch</button>
    </div>

    <div class="grid" id="grid">
        {% for a in athletes %}
        <div class="card" id="card-{{ a.id }}" data-id="{{ a.id }}">
            <img src="{{ a.image_source }}" loading="lazy" onerror="this.src='https://via.placeholder.com/200?text=Broken+Link'">
            <div class="info">
                <span class="name">{{ a.name }}</span>
                <span class="nat">{{ a.nationality or 'UNK' }}</span>
            </div>
            <button class="btn-delete" onclick="deleteImage('{{ a.id }}')">🗑️ WRONG PERSON</button>
        </div>
        {% endfor %}
    </div>

    {% if count == 0 %}
    <div style="text-align: center; margin-top: 50px;">
        <h2>🎉 All caught up! No unaudited images found.</h2>
    </div>
    {% endif %}

    <script>
        // 1. DELETE LOGIC (Same as before)
        function deleteImage(id) {
            const card = document.getElementById('card-' + id);
            card.style.opacity = "0.5";
            
            fetch('/delete/' + id, { method: 'POST' })
                .then(res => res.json())
                .then(data => {
                    if (data.success) {
                        card.remove(); // Remove from DOM so it's not included in "Finish Batch"
                        updateCount();
                    } else {
                        alert("Error deleting");
                        card.style.opacity = "1";
                    }
                });
        }

        function updateCount() {
            const count = document.querySelectorAll('.card').length;
            document.querySelector('h1').innerText = `📸 Unaudited Queue (${count} Remaining in Batch)`;
        }

        // 2. FINISH BATCH LOGIC
        function finishBatch() {
            const cards = document.querySelectorAll('.card');
            const ids = Array.from(cards).map(c => c.getAttribute('data-id'));

            if (ids.length === 0) {
                location.reload(); // Just reload if empty
                return;
            }

            const btn = document.querySelector('.btn-next');
            btn.innerText = "Processing...";
            btn.disabled = true;

            fetch('/mark_batch_audited', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ids: ids })
            })
            .then(res => res.json())
            .then(data => {
                if (data.success) {
                    location.reload(); // Reloads page to fetch the NEXT 100 unaudited
                } else {
                    alert("Error marking batch: " + data.error);
                    btn.innerText = "Try Again";
                    btn.disabled = false;
                }
            });
        }
    </script>
</body>
</html>
"""

@app.route('/')
def queue():
    print("📥 Fetching next batch of UNAUDITED images...")
    
    # 🟢 NEW QUERY: Only fetch items where image_audited is NOT TRUE
    # We use .is_("image_audited", "false") generally, or check for null
    # Supabase filter syntax for "False OR Null" can be tricky, 
    # so usually defaulting the column to FALSE in SQL is best.
    
    res = supabase.table("entities")\
        .select("id, name, nationality, image_source")\
        .neq("image_source", "null")\
        .is_("image_audited", "false")\
        .order("name")\
        .limit(IMAGES_PER_BATCH)\
        .execute()
    
    athletes = res.data
    return render_template_string(HTML_TEMPLATE, athletes=athletes, count=len(athletes))

@app.route('/delete/<uuid:entity_id>', methods=['POST'])
def delete_image(entity_id):
    try:
        # If deleted, we also mark it audited so it doesn't reappear if we reset images
        supabase.table("entities").update({
            "image_source": None,
            "image_scrape_status": "rejected_manual",
            "image_audited": True, 
            "image_checked_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", str(entity_id)).execute()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/mark_batch_audited', methods=['POST'])
def mark_batch():
    try:
        ids = request.json.get('ids', [])
        if not ids:
            return jsonify({"success": True})

        print(f"✅ Marking {len(ids)} images as AUDITED...")
        
        # 🟢 BULK UPDATE
        # We update all these IDs to image_audited = True
        supabase.table("entities").update({
            "image_audited": True
        }).in_("id", ids).execute()
        
        return jsonify({"success": True})
    except Exception as e:
        print(e)
        return jsonify({"success": False, "error": str(e)})

if __name__ == '__main__':
    print("🚀 Queue Tool running on http://127.0.0.1:5000")
    app.run(debug=True, port=5000)