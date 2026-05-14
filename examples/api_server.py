"""Enhanced API Server for Multi-Agent System with full REST API."""

import time
import sys
import os
import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.worker.agent import WorkerAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('api_server')


class EnhancedDashboardHandler(BaseHTTPRequestHandler):
    """Enhanced HTTP handler with REST API."""

    def send_json_response(self, status_code, data):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # Serve 3D dashboard HTML
        if path == '/' or path == '/index.html':
            self.path = '/3d_agent_office.html'
        if path == '/3d_agent_office.html':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            try:
                with open('/home/laodaboss/Desktop/3d_agent_office.html', 'r') as f:
                    self.wfile.write(f.read().encode())
            except:
                self.wfile.write(b'<html><body><h1>File not found</h1></body></html>')
            return

        # GET /api/status - system status
        if path == '/api/status':
            workers = self.server.orch.get_workers_status()
            tasks = {}
            for tid, t in self.server.orch.tasks.items():
                tasks[tid] = {
                    'status': t.status,
                    'task_type': t.task_type,
                    'task_data': t.task_data,
                    'result': t.result,
                    'error': t.error,
                    'assigned_worker': t.assigned_worker,
                    'created_at': t.created_at,
                    'started_at': t.started_at,
                    'completed_at': t.completed_at
                }
            self.send_json_response(200, {'workers': workers, 'tasks': tasks})
            return

        # GET /api/tasks - list all tasks
        if path == '/api/tasks':
            tasks = []
            for tid, t in self.server.orch.tasks.items():
                tasks.append({
                    'task_id': tid,
                    'status': t.status,
                    'task_type': t.task_type,
                    'task_data': t.task_data,
                    'result': t.result,
                    'error': t.error,
                    'assigned_worker': t.assigned_worker
                })
            self.send_json_response(200, {'tasks': tasks, 'count': len(tasks)})
            return

        # GET /api/tasks/{id} - get specific task
        if path.startswith('/api/tasks/'):
            task_id = path.split('/')[-1]
            status = self.server.orch.get_task_status(task_id)
            if status:
                self.send_json_response(200, status)
            else:
                self.send_json_response(404, {'error': 'Task not found'})
            return

        # GET /api/workers - list all workers
        if path == '/api/workers':
            workers = self.server.orch.get_workers_status()
            self.send_json_response(200, {'workers': workers, 'count': len(workers)})
            return

        # GET /api/workers/{id} - get specific worker
        if path.startswith('/api/workers/'):
            worker_id = path.split('/')[-1]
            if worker_id in self.server.orch.workers:
                w = self.server.orch.workers[worker_id]
                self.send_json_response(200, {
                    'worker_id': w.worker_id,
                    'worker_type': w.worker_type,
                    'status': w.status,
                    'capabilities': w.capabilities,
                    'current_task': w.current_task_id,
                    'completed_tasks': w.completed_tasks,
                    'failed_tasks': w.failed_tasks
                })
            else:
                self.send_json_response(404, {'error': 'Worker not found'})
            return

        # 404
        self.send_json_response(404, {'error': 'Not found'})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # Read body
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode() if content_length > 0 else ''
        data = json.loads(body) if body else {}

        # POST /api/tasks - submit new task
        if path == '/api/tasks':
            task_type = data.get('task_type')
            task_data = data.get('task_data', {})
            priority = data.get('priority', 2)
            timeout = data.get('timeout', 300)

            if not task_type:
                self.send_json_response(400, {'error': 'task_type is required'})
                return

            task_id = self.server.orch.submit_task(task_type, task_data, priority, timeout)
            logger.info(f'Task submitted: {task_id} ({task_type})')
            self.send_json_response(201, {'task_id': task_id, 'status': 'pending'})
            return

        # POST /api/workers - register new worker
        if path == '/api/workers':
            worker_id = data.get('worker_id')
            worker_type = data.get('worker_type', 'general')
            capabilities = data.get('capabilities', [])

            if not worker_id:
                self.send_json_response(400, {'error': 'worker_id is required'})
                return

            if not capabilities:
                self.send_json_response(400, {'error': 'capabilities is required'})
                return

            # Check if worker already exists
            if worker_id in self.server.orch.workers:
                self.send_json_response(409, {'error': 'Worker already exists'})
                return

            # Create and register worker
            def make_handler(caps):
                def handler(task_data):
                    query = task_data.get("task_data", {}).get("query", "")
                    time.sleep(0.5)
                    return {cap: f"Completed: {query}" for cap in caps}
                return handler

            w = WorkerAgent(worker_id, worker_type, capabilities, self.server.mq)
            for cap in capabilities:
                w.register_handler(cap, make_handler([cap]))
            w.start()

            # Store reference to prevent garbage collection
            self.server.workers[worker_id] = w

            logger.info(f'Worker registered: {worker_id} with capabilities {capabilities}')
            self.send_json_response(201, {
                'worker_id': worker_id,
                'status': 'online',
                'capabilities': capabilities
            })
            return

        # POST /api/tasks/{id}/cancel - cancel task
        if path.startswith('/api/tasks/') and path.endswith('/cancel'):
            task_id = path.split('/')[-2]
            # For now, just return success (actual cancellation depends on implementation)
            self.send_json_response(200, {'task_id': task_id, 'status': 'cancellation_requested'})
            return

        self.send_json_response(404, {'error': 'Not found'})

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # DELETE /api/tasks/{id}
        if path.startswith('/api/tasks/'):
            task_id = path.split('/')[-1]
            # For now, just return success
            logger.info(f'Task deletion requested: {task_id}')
            self.send_json_response(200, {'task_id': task_id, 'status': 'deletion_requested'})
            return

        # DELETE /api/workers/{id}
        if path.startswith('/api/workers/'):
            worker_id = path.split('/')[-1]
            if worker_id in self.server.workers:
                self.server.workers[worker_id].stop()
                del self.server.workers[worker_id]
                if worker_id in self.server.orch.workers:
                    self.server.orch.workers[worker_id].status = 'offline'
                logger.info(f'Worker unregistered: {worker_id}')
                self.send_json_response(200, {'worker_id': worker_id, 'status': 'unregistered'})
            else:
                self.send_json_response(404, {'error': 'Worker not found'})
            return

        self.send_json_response(404, {'error': 'Not found'})

    def log_message(self, format, *args):
        logger.info(f'{self.address_string()} - {format % args}')


def run_dashboard(orch, mq, port=8080):
    server = HTTPServer(('localhost', port), EnhancedDashboardHandler)
    server.orch = orch
    server.mq = mq
    server.workers = {}
    logger.info(f'Enhanced API Server running at http://localhost:{port}')
    logger.info('Endpoints:')
    logger.info('  GET  /api/status          - System status')
    logger.info('  GET  /api/tasks           - List all tasks')
    logger.info('  GET  /api/tasks/{id}      - Get task status')
    logger.info('  POST /api/tasks           - Submit new task')
    logger.info('  GET  /api/workers         - List all workers')
    logger.info('  GET  /api/workers/{id}     - Get worker status')
    logger.info('  POST /api/workers         - Register new worker')
    logger.info('  DELETE /api/workers/{id}   - Unregister worker')
    logger.info('Press Ctrl+C to stop')
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

    workers = {}
    for worker_id, worker_type, caps in worker_defs:
        task_type = caps[0]
        w = WorkerAgent(worker_id, worker_type, caps, mq)
        w.register_handler(task_type, make_handler(task_type))
        w.start()
        workers[worker_id] = w
        logger.info(f'Started {worker_id}')
        time.sleep(2)

    time.sleep(5)

    # Submit sample tasks
    tasks = [
        ("analysis", {"query": "Q1 revenue trends"}),
        ("research", {"query": "market analysis"}),
        ("coding", {"query": "API integration"}),
        ("design", {"query": "UI mockups"}),
        ("data", {"query": "data pipeline"}),
    ]

    for task_type, task_data in tasks:
        task_id = orch.submit_task(task_type, task_data)
        logger.info(f'Submitted task: {task_id} ({task_type})')

    logger.info('Workers and tasks ready. Starting API server...')
    run_dashboard(orch, mq)
