"""AI Agent Office Dashboard - A visual office-themed dashboard."""

import time
import sys
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import json
import random

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.worker.agent import WorkerAgent


WORKER_AVATARS = {
    "worker_1": "👨‍💻",
    "worker_2": "👩‍🔬",
    "worker_3": "🤖",
    "worker_4": "👨‍🔧",
    "worker_5": "👩‍💼",
}


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            html = self.get_dashboard_html()
            self.wfile.write(html.encode())
        elif self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            status = {
                'workers': self.server.orch.get_workers_status(),
                'tasks': {tid: {
                    'status': t.status,
                    'task_type': t.task_type,
                    'result': t.result,
                    'error': t.error
                } for tid, t in self.server.orch.tasks.items()}
            }
            self.wfile.write(json.dumps(status).encode())
        else:
            super().do_GET()

    def get_dashboard_html(self):
        return '''<!DOCTYPE html>
<html>
<head>
    <title>AI Agent Office</title>
    <meta http-equiv="refresh" content="1.5">
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 0;
            padding: 0;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            min-height: 100vh;
            color: #eee;
        }
        .header {
            background: rgba(0,0,0,0.3);
            padding: 20px 40px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 2px solid #00d9ff;
        }
        .header h1 {
            margin: 0;
            color: #00d9ff;
            font-size: 28px;
            text-shadow: 0 0 20px rgba(0,217,255,0.5);
        }
        .header h1 span { font-size: 32px; }
        .clock {
            font-size: 24px;
            color: #00ff88;
            font-family: monospace;
        }
        .container {
            padding: 30px 40px;
            max-width: 1400px;
            margin: 0 auto;
        }
        .floor {
            background: rgba(255,255,255,0.05);
            border-radius: 20px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
        }
        .floor-title {
            font-size: 20px;
            color: #00d9ff;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px solid rgba(0,217,255,0.3);
        }
        .workers-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 20px;
        }
        .worker-card {
            background: linear-gradient(145deg, #1e3a5f, #152238);
            border-radius: 15px;
            padding: 25px;
            position: relative;
            overflow: hidden;
            transition: transform 0.3s, box-shadow 0.3s;
        }
        .worker-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0,217,255,0.2);
        }
        .worker-card.online { border-left: 4px solid #00ff88; }
        .worker-card.offline { border-left: 4px solid #ff4757; opacity: 0.7; }
        .worker-card.busy { border-left: 4px solid #ffa502; }

        .worker-avatar {
            font-size: 50px;
            float: right;
            opacity: 0.8;
        }
        .worker-name {
            font-size: 18px;
            font-weight: bold;
            color: #fff;
            margin-bottom: 5px;
        }
        .worker-type {
            font-size: 12px;
            color: #00d9ff;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 15px;
        }
        .worker-status {
            display: inline-block;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
        }
        .worker-status.online { background: #00ff8833; color: #00ff88; }
        .worker-status.offline { background: #ff475733; color: #ff4757; }
        .worker-status.busy { background: #ffa50233; color: #ffa502; }
        .worker-stats {
            margin-top: 15px;
            font-size: 13px;
            color: #aaa;
        }
        .worker-stats span { margin-right: 15px; }
        .task-bubble {
            background: rgba(0,0,0,0.3);
            border-radius: 10px;
            padding: 15px;
            margin-top: 15px;
            font-size: 13px;
        }
        .task-bubble.active {
            border: 1px solid #ffa502;
            animation: pulse 1.5s infinite;
        }
        @keyframes pulse {
            0%, 100% { box-shadow: 0 0 0 0 rgba(255,165,2,0.4); }
            50% { box-shadow: 0 0 0 10px rgba(255,165,2,0); }
        }

        .tasks-section {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 15px;
        }
        .task-card {
            background: rgba(0,0,0,0.3);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .task-card.completed { border-color: #00ff88; }
        .task-card.failed { border-color: #ff4757; }
        .task-card.pending { border-color: #ffa502; }
        .task-card.running { border-color: #00d9ff; }

        .task-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .task-id {
            font-family: monospace;
            font-size: 11px;
            color: #888;
        }
        .task-status-badge {
            padding: 3px 10px;
            border-radius: 10px;
            font-size: 11px;
            font-weight: bold;
        }
        .task-status-badge.completed { background: #00ff8833; color: #00ff88; }
        .task-status-badge.failed { background: #ff475733; color: #ff4757; }
        .task-status-badge.pending { background: #ffa50233; color: #ffa502; }
        .task-status-badge.running { background: #00d9ff33; color: #00d9ff; }

        .task-type-tag {
            display: inline-block;
            background: #00d9ff22;
            color: #00d9ff;
            padding: 3px 10px;
            border-radius: 5px;
            font-size: 11px;
            margin-bottom: 10px;
        }
        .task-result {
            background: rgba(0,255,136,0.1);
            border-radius: 8px;
            padding: 10px;
            font-size: 12px;
            color: #00ff88;
            word-break: break-all;
        }
        .task-error {
            background: rgba(255,71,87,0.1);
            border-radius: 8px;
            padding: 10px;
            font-size: 12px;
            color: #ff4757;
        }

        .empty-office {
            text-align: center;
            padding: 60px;
            color: #666;
            font-size: 18px;
        }
        .empty-office span { font-size: 50px; display: block; margin-bottom: 20px; }

        .floor-indicator {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        .indicator {
            padding: 5px 15px;
            border-radius: 5px;
            font-size: 12px;
            background: rgba(255,255,255,0.1);
        }
        .indicator.online { color: #00ff88; }
        .indicator.offline { color: #ff4757; }
        .indicator.busy { color: #ffa502; }

        .activity-feed {
            max-height: 300px;
            overflow-y: auto;
        }
        .activity-item {
            padding: 10px;
            border-bottom: 1px solid rgba(255,255,255,0.05);
            font-size: 13px;
            display: flex;
            gap: 10px;
            align-items: center;
        }
        .activity-time { color: #666; font-size: 11px; min-width: 60px; }
        .activity-text { flex: 1; }
        .activity-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
        }
        .activity-dot.completed { background: #00ff88; }
        .activity-dot.failed { background: #ff4757; }
        .activity-dot.started { background: #00d9ff; }
    </style>
</head>
<body>
    <div class="header">
        <h1><span>🏢</span> AI Agent Office</h1>
        <div class="clock" id="clock">--:--:--</div>
    </div>
    <div class="container">
        <div class="floor">
            <div class="floor-title">👥 Employee Directory</div>
            <div class="floor-indicator">
                <span class="indicator online">● Online</span>
                <span class="indicator busy">● Busy</span>
                <span class="indicator offline">● Offline</span>
            </div>
            <div class="workers-grid" id="workers">
                <div class="empty-office">
                    <span>🕵️</span>
                    <p>No agents in the office yet...</p>
                </div>
            </div>
        </div>

        <div class="floor">
            <div class="floor-title">📋 Task Assignment Desk</div>
            <div class="tasks-section" id="tasks">
                <div class="empty-office">
                    <span>📝</span>
                    <p>No tasks in the queue...</p>
                </div>
            </div>
        </div>

        <div class="floor">
            <div class="floor-title">📊 Activity Log</div>
            <div class="activity-feed" id="activity">
                <div class="empty-office"><span>📜</span><p>No activity yet...</p></div>
            </div>
        </div>
    </div>

    <script>
        function updateClock() {
            const now = new Date();
            document.getElementById('clock').textContent = now.toLocaleTimeString();
        }
        setInterval(updateClock, 1000);
        updateClock();

        const avatars = ''' + json.dumps(WORKER_AVATARS) + ''';

        async function update() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();

                // Update workers
                let workersHtml = '';
                for (const w of data.workers) {
                    const avatar = avatars[w.worker_id] || '🤖';
                    const statusClass = w.status;
                    const currentTask = w.current_task ?
                        `<div class="task-bubble active">Working on: ${w.current_task.substring(0,8)}...</div>` : '';

                    workersHtml += `<div class="worker-card ${statusClass}">
                        <div class="worker-avatar">${avatar}</div>
                        <div class="worker-name">${w.worker_id}</div>
                        <div class="worker-type">${w.worker_type}</div>
                        <div class="worker-status ${statusClass}">${statusClass.toUpperCase()}</div>
                        <div class="worker-stats">
                            <span>✅ ${w.completed}</span>
                            <span>❌ ${w.failed}</span>
                        </div>
                        ${currentTask}
                    </div>`;
                }
                document.getElementById('workers').innerHTML = workersHtml || '<div class="empty-office">No workers</div>';

                // Update tasks
                let tasksHtml = '';
                const taskEntries = Object.entries(data.tasks);
                for (const [tid, t] of taskEntries) {
                    const statusClass = t.status;
                    let resultHtml = '';
                    if (t.result) {
                        resultHtml = `<div class="task-result">✓ ${JSON.stringify(t.result)}</div>`;
                    } else if (t.error) {
                        resultHtml = `<div class="task-error">✗ ${t.error}</div>`;
                    }

                    tasksHtml += `<div class="task-card ${statusClass}">
                        <div class="task-header">
                            <span class="task-id">#${tid.substring(0, 8)}</span>
                            <span class="task-status-badge ${statusClass}">${statusClass.toUpperCase()}</span>
                        </div>
                        <div class="task-type-tag">${t.task_type}</div>
                        ${resultHtml}
                    </div>`;
                }
                document.getElementById('tasks').innerHTML = tasksHtml || '<div class="empty-office">No tasks</div>';

                // Update activity
                if (taskEntries.length > 0) {
                    let activityHtml = '';
                    const latest = taskEntries.slice(-5).reverse();
                    for (const [tid, t] of latest) {
                        const dotClass = t.status;
                        const text = t.result ? `Task ${tid.substring(0,8)} completed` :
                                    t.error ? `Task ${tid.substring(0,8)} failed` :
                                    t.status === 'running' ? `Task ${tid.substring(0,8)} started` :
                                    `Task ${tid.substring(0,8)} pending`;
                        const time = new Date().toLocaleTimeString();
                        activityHtml += `<div class="activity-item">
                            <span class="activity-time">${time}</span>
                            <span class="activity-dot ${dotClass}"></span>
                            <span class="activity-text">${text}</span>
                        </div>`;
                    }
                    document.getElementById('activity').innerHTML = activityHtml;
                }
            } catch (e) {
                console.error(e);
            }
        }
        update();
        setInterval(update, 1500);
    </script>
</body>
</html>'''


def run_dashboard(orch, port=8080):
    server = HTTPServer(('localhost', port), DashboardHandler)
    server.orch = orch
    print(f'')
    print(f'  ╔═══════════════════════════════════════════╗')
    print(f'  ║   🏢 AI Agent Office Dashboard          ║')
    print(f'  ╠═══════════════════════════════════════════╣')
    print(f'  ║   Running at: http://localhost:{port}    ║')
    print(f'  ║   Press Ctrl+C to stop                  ║')
    print(f'  ╚═══════════════════════════════════════════╝')
    print(f'')
    server.serve_forever()


if __name__ == "__main__":
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    worker1 = WorkerAgent("worker_1", "Analysis", ["analysis"], mq)
    worker2 = WorkerAgent("worker_2", "Research", ["research"], mq)
    worker3 = WorkerAgent("worker_3", "Coding", ["coding"], mq)

    def analysis_handler(task_data):
        query = task_data.get("task_data", {}).get("query", "")
        time.sleep(random.uniform(1, 3))
        return {"analysis": f"Analyzed: {query}"}

    def research_handler(task_data):
        query = task_data.get("task_data", {}).get("query", "")
        time.sleep(random.uniform(1, 3))
        return {"research": f"Researched: {query}"}

    def coding_handler(task_data):
        query = task_data.get("task_data", {}).get("query", "")
        time.sleep(random.uniform(1, 3))
        return {"code": f"Generated code for: {query}"}

    worker1.register_handler("analysis", analysis_handler)
    worker2.register_handler("research", research_handler)
    worker3.register_handler("coding", coding_handler)

    worker1.start()
    worker2.start()
    worker3.start()

    time.sleep(2)

    # Submit sample tasks
    tasks = [
        ("analysis", {"query": "Q1 financial trends"}),
        ("research", {"query": "competitor analysis"}),
        ("coding", {"query": "user authentication"}),
        ("analysis", {"query": "market segmentation"}),
        ("research", {"query": "tech landscape 2026"}),
    ]

    print("📋 Submitting tasks to the office...")
    for task_type, task_data in tasks:
        task_id = orch.submit_task(task_type, task_data)
        print(f"   ✓ Task submitted: {task_type} - {task_id[:8]}...")

    print("")
    print("🚀 Starting dashboard server...")
    print("")

    run_dashboard(orch)