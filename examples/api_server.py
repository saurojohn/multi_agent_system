"""API Server for 3D Dashboard - serves API only."""

import time
import sys
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
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
            self.path = '/3d_agent_office.html'
        if self.path == '/3d_agent_office.html':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            try:
                with open('/home/laodaboss/Desktop/3d_agent_office.html', 'r') as f:
                    self.wfile.write(f.read().encode())
            except:
                self.wfile.write(b'<html><body><h1>File not found</h1></body></html>')
        elif self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            status = {
                'workers': self.server.orch.get_workers_status(),
                'tasks': {tid: {
                    'status': t.status,
                    'task_type': t.task_type,
                    'task_data': t.task_data,
                    'result': t.result,
                    'error': t.error
                } for tid, t in self.server.orch.tasks.items()}
            }
            self.wfile.write(json.dumps(status).encode())
        else:
            self.send_response(404)
            self.end_headers()


def run_dashboard(orch, port=8080):
    server = HTTPServer(('localhost', port), DashboardHandler)
    server.orch = orch
    print(f'API Server running at http://localhost:{port}')
    print('Press Ctrl+C to stop')
    server.serve_forever()


if __name__ == "__main__":
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    # Create workers with handlers
    worker_defs = [
        ("worker_1", "Analysis", ["analysis"]),
        ("worker_2", "Research", ["research"]),
        ("worker_3", "Coding", ["coding"]),
        ("worker_4", "Design", ["design"]),
        ("worker_5", "Data", ["data"]),
    ]

    def make_handler(task_type):
        def handler(task_data):
            query = task_data.get("task_data", {}).get("query", "")
            time.sleep(0.5)
            return {task_type: f"Completed: {query}"}
        return handler

    for worker_id, worker_type, caps in worker_defs:
        task_type = caps[0]
        w = WorkerAgent(worker_id, worker_type, caps, mq)
        w.register_handler(task_type, make_handler(task_type))
        w.start()
        time.sleep(2)

    time.sleep(10)

    # Submit sample tasks
    tasks = [
        ("analysis", {"query": "Q1 revenue trends"}),
        ("research", {"query": "market analysis"}),
        ("coding", {"query": "API integration"}),
        ("design", {"query": "UI mockups"}),
        ("data", {"query": "data pipeline"}),
    ]

    for task_type, task_data in tasks:
        orch.submit_task(task_type, task_data)

    print("Workers and tasks ready. Starting API server...")
    run_dashboard(orch)