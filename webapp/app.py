import threading
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

# Thread-safe in-memory store
_lock = threading.Lock()
ranking_data = {"windowStart": 0, "windowEnd": 0, "entries": []}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="5">
    <title>Upbit Real-time Ranking</title>
    <style>
        body { font-family: sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }
        h1 { color: #e94560; }
        table { border-collapse: collapse; width: 100%; max-width: 800px; }
        th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #333; }
        th { background: #16213e; color: #0f3460; }
        td { background: #1a1a2e; }
        .rank-1 { color: gold; font-weight: bold; }
        .rank-2 { color: silver; }
        .rank-3 { color: #cd7f32; }
        .code { text-align: left; }
        .metric { color: #00ff88; }
        .window { color: #888; font-size: 0.9em; margin-bottom: 10px; }
    </style>
</head>
<body>
    <h1>Upbit Real-time Ranking</h1>
    <div class="window">
        Window: {{ window_start }} ~ {{ window_end }}
        ({{ entries|length }} entries)
    </div>
    <table>
        <tr><th>Rank</th><th>Code</th><th>Volume</th><th>Amount</th><th>Metric</th></tr>
        {% for e in entries %}
        <tr>
            <td class="rank-{{ e.rank if e.rank <= 3 else '' }}">{{ e.rank }}</td>
            <td class="code">{{ e.code }}</td>
            <td>{{ "%.2f"|format(e.volume) }}</td>
            <td>{{ "%.0f"|format(e.amount) }}</td>
            <td class="metric">{{ "%.2f"|format(e.metric) }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
"""

@app.route("/api/ranking", methods=["POST"])
def receive():
    data = request.get_json(force=True)
    with _lock:
        ranking_data.clear()
        ranking_data.update(data)
    return "", 204

@app.route("/api/ranking")
def get_json():
    with _lock:
        return jsonify(dict(ranking_data))

@app.route("/")
def index():
    with _lock:
        return render_template_string(HTML_TEMPLATE,
            window_start=ranking_data.get("windowStart", 0),
            window_end=ranking_data.get("windowEnd", 0),
            entries=ranking_data.get("entries", [])
        )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
