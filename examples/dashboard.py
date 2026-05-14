"""Simple web dashboard for multi-agent system."""

import time
import sys
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import json

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.worker.agent import WorkerAgent


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
    <title>Multi-Agent Dashboard</title>
    <meta http-equiv="refresh" content="2">
    <style>
        body { font-family: Arial; margin: 40px; background: #1a1a2e; color: #eee; }
        h1 { color: #00d9ff; }
        .card { background: #16213e; padding: 20px; margin: 10px 0; border-radius: 10px; }
        .worker { display: inline-block; width: 200px; }
        .status-online { color: #00ff88; }
        .status-offline { color: #ff4444; }
        .status-busy { color: #ffaa00; }
        table { border-collapse: collapse; width: 100%; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #333; }
        th { color: #00d9ff; }
        .completed { color: #00ff88; }
        .failed { color: #ff4444; }
        .pending, .running { color: #ffaa00; }
    </style>
</head>
<body>
    <h1>Multi-Agent System Dashboard</h1>
    <div id="content">
        <div class="card">
            <h2>Workers</h2>
            <div id="workers">Loading...</div>
        </div>
        <div class="card">
            <h2>Tasks</h2>
            <div id="tasks">Loading...</div>
        </div>
    </div>
    <script>
        async function update() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();

                let workersHtml = '';
                for (const w of data.workers) {
                    const statusClass = 'status-' + w.status;
                    workersHtml += `<div class="worker">
                        <div class="${statusClass}">● ${w.worker_id}</div>
                        <div>Status: ${w.status}</div>
                        <div>Completed: ${w.completed} | Failed: ${w.failed}</div>
                    </div>`;
                }
                document.getElementById('workers').innerHTML = workersHtml || '<p>No workers</p>';

                let tasksHtml = '<table><tr><th>Task ID</th><th>Status</th><th>Result</th></tr>';
                for (const [tid, t] of Object.entries(data.tasks)) {
                    tasksHtml += `<tr>
                        <td>${tid.substring(0, 8)}...</td>
                        <td class="${t.status}">${t.status}</td>
                        <td>${t.result ? JSON.stringify(t.result) : t.error || '-'}</td>
                    </tr>`;
                }
                tasksHtml += '</table>';
                document.getElementById('tasks').innerHTML = tasksHtml || '<p>No tasks</p>';
            } catch (e) {
                console.error(e);
            }
        }
        update();
        setInterval(update, 2000);
    </script>
</body>
</html>'''


def run_dashboard(orch, port=8080):
    server = HTTPServer(('localhost', port), DashboardHandler)
    server.orch = orch
    print(f'Dashboard running at http://localhost:{port}')
    print('Press Ctrl+C to stop')
    server.serve_forever()


if __name__ == "__main__":
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    worker1 = WorkerAgent("worker_1", "analysis", ["analysis"], mq)
    worker2 = WorkerAgent("worker_2", "research", ["research"], mq)

    def analysis_handler(task_data):
        query = task_data.get("task_data", {}).get("query", "")
        return {"analysis": f"Result for: {query}"}

    def research_handler(task_data):
        query = task_data.get("task_data", {}).get("query", "")
        return {"research": f"Research result for: {query}"}

    worker1.register_handler("analysis", analysis_handler)
    worker2.register_handler("research", research_handler)

    worker1.start()
    worker2.start()

    time.sleep(2)

    task1 = orch.submit_task("analysis", {"query": "market trends"})
    task2 = orch.submit_task("research", {"query": "AI agents"})

    print(f"Submitted tasks: {task1}, {task2}")
    print("Starting dashboard...")

    run_dashboard(orch)