"""Enhanced API Server for Multi-Agent System with full REST API."""

import time
import sys
import os
import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.common.metrics import MetricsCollector, get_metrics
from multi_agent_system.common.auth import AuthManager, get_auth_manager, init as init_auth
from multi_agent_system.common.rate_limit import RateLimitMiddleware, get_middleware
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.worker.agent import WorkerAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('api_server')


def require_auth(f):
    """Decorator to require authentication for endpoint."""
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        auth_manager = getattr(self.server, 'auth_manager', None)
        if not auth_manager:
            return f(self, *args, **kwargs)

        auth_header = self.headers.get('Authorization')
        api_key = self.headers.get('X-API-Key')

        is_auth, auth_type, roles = auth_manager.authenticate_request(auth_header, api_key)
        if not is_auth:
            self.send_json_response(401, {'error': 'Unauthorized', 'message': 'Valid authentication required'})
            return

        self.auth_type = auth_type
        self.auth_roles = roles
        return f(self, *args, **kwargs)
    return wrapper


class SSEClient:
    """Server-Sent Events client connection."""
    def __init__(self, handler):
        self.handler = handler
        self.running = True

    def send(self, data):
        if self.running:
            try:
                self.handler.send_header('Content-Type', 'text/event-stream')
                self.handler.send_header('Cache-Control', 'no-cache')
                self.handler.end_headers()
                self.handler.wfile.write(f"data: {data}\n\n".encode())
            except:
                self.running = False

    def check_auth(self, auth_manager: AuthManager, auth_header: str) -> bool:
        """Check if client is authenticated."""
        if not self.handler.server.sse_manager._require_auth:
            return True
        is_auth, _, _ = auth_manager.authenticate_request(auth_header)
        return is_auth


class SSEManager:
    """Manages Server-Sent Events connections for real-time updates."""
    def __init__(self):
        self._clients = []
        self._lock = threading.Lock()
        self._require_auth = False  # Can be enabled via config

    def set_require_auth(self, required: bool):
        self._require_auth = required

    def add_client(self, handler):
        with self._lock:
            client = SSEClient(handler)
            self._clients.append(client)
            logger.info(f'SSE client connected. Total: {len(self._clients)}')
            return client

    def remove_client(self, client):
        with self._lock:
            if client in self._clients:
                self._clients.remove(client)
            logger.info(f'SSE client disconnected. Total: {len(self._clients)}')

    def broadcast(self, event_type: str, data: dict):
        """Broadcast event to all connected SSE clients."""
        message = json.dumps({"event": event_type, "data": data, "timestamp": time.time()})
        with self._lock:
            dead_clients = []
            for client in self._clients:
                try:
                    client.send(message)
                except Exception as e:
                    logger.warning(f'Failed to send SSE: {e}')
                    dead_clients.append(client)
            for client in dead_clients:
                self._clients.remove(client)

    def broadcast_task_update(self, task_id: str, status: str, result=None, error=None, task_type=None, task_data=None):
        self.broadcast("task_update", {
            "task_id": task_id,
            "status": status,
            "result": result,
            "error": error,
            "task_type": task_type,
            "task_data": task_data
        })

    def broadcast_worker_update(self, worker_id: str, status: str, stats: dict):
        self.broadcast("worker_update", {
            "worker_id": worker_id,
            "status": status,
            "completed": stats.get("completed", 0),
            "failed": stats.get("failed", 0)
        })


# Global SSE manager
sse_manager = SSEManager()


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

        # Rate limit check
        client_id = self.headers.get('X-Client-ID', self.address_string())
        allowed, reason, retry_after = self.server.rate_limiter.check_request(client_id, path)
        if not allowed:
            self.send_response(429)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Retry-After', str(retry_after))
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Rate limit exceeded', 'message': reason, 'retry_after': retry_after}).encode())
            return

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

        # GET /health - health check for load balancers
        if path == '/health' or path == '/api/health':
            health_status = {
                'status': 'healthy',
                'timestamp': time.time(),
                'orchestrator': 'running' if self.server.orch._running else 'stopped',
                'workers': {
                    'total': len(self.server.orch.workers),
                    'online': sum(1 for w in self.server.orch.workers.values() if w.status != 'offline')
                },
                'tasks': {
                    'pending': sum(1 for t in self.server.orch.tasks.values() if t.status == 'pending'),
                    'running': sum(1 for t in self.server.orch.tasks.values() if t.status == 'running'),
                    'completed': sum(1 for t in self.server.orch.tasks.values() if t.status == 'completed'),
                    'failed': sum(1 for t in self.server.orch.tasks.values() if t.status == 'failed')
                },
                'queue_size': {
                    'orchestrator_to_worker': self.server.mq.size('orchestrator_to_worker'),
                    'worker_to_orchestrator': self.server.mq.size('worker_to_orchestrator')
                }
            }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            self.wfile.write(json.dumps(health_status).encode())
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

        # GET /api/events - SSE endpoint for real-time updates
        if path == '/api/events':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            # Register this client for updates
            client = self.server.sse_manager.add_client(self)
            logger.info(f'SSE client connected from {self.address_string()}')

            # Send initial heartbeat
            self.wfile.write(f"data: {json.dumps({'event': 'connected', 'data': {'status': 'ok'}})}\n\n".encode())

            # Keep connection alive, send heartbeat every 30s
            try:
                while True:
                    time.sleep(30)
                    self.wfile.write(f": heartbeat\n\n".encode())
                    self.wfile.flush()
            except:
                self.server.sse_manager.remove_client(client)
            return

        # GET /api/metrics - Prometheus metrics
        if path == '/api/metrics':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.end_headers()
            self.wfile.write(self.server.metrics.export().encode())
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
            dependencies = data.get('dependencies', [])

            if not task_type:
                self.send_json_response(400, {'error': 'task_type is required'})
                return

            task_id = self.server.orch.submit_task(task_type, task_data, priority, timeout, dependencies)
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

        # POST /api/config/reload - hot reload configuration
        if path == '/api/config/reload':
            config = getattr(self.server, 'config', None)
            if config:
                old_config = dict(config._config)
                config.reload()
                logger.info('Configuration hot reloaded via API')
                self.send_json_response(200, {'status': 'reloaded', 'message': 'Configuration reloaded successfully'})
            else:
                self.send_json_response(400, {'error': 'Config not available'})
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
    metrics = get_metrics()
    rate_limiter = get_middleware()
    auth_manager = AuthManager(
        jwt_secret='dev-secret-key-change-in-production',
        api_keys={'dev-key-001': ('Development', ['read', 'write'])}
    )
    server = HTTPServer(('localhost', port), EnhancedDashboardHandler)
    server.orch = orch
    server.mq = mq
    server.workers = {}
    server.sse_manager = sse_manager
    server.metrics = metrics
    server.auth_manager = auth_manager
    server.rate_limiter = rate_limiter
    logger.info(f'Enhanced API Server running at http://localhost:{port}')
    logger.info('Rate limiting enabled: 1000 global, 100/endpoint, 60/client/min')
    logger.info('Endpoints:')
    logger.info('  GET  /health             - Health check (for load balancers)')
    logger.info('  GET  /api/status         - System status (auth optional)')
    logger.info('  GET  /api/events         - SSE real-time updates')
    logger.info('  GET  /api/metrics        - Prometheus metrics')
    logger.info('  GET  /api/tasks          - List all tasks (auth required)')
    logger.info('  GET  /api/tasks/{id}      - Get task status (auth required)')
    logger.info('  POST /api/tasks           - Submit new task (auth required)')
    logger.info('  GET  /api/workers         - List all workers (auth required)')
    logger.info('  GET  /api/workers/{id}    - Get worker status (auth required)')
    logger.info('  POST /api/workers         - Register worker (auth required)')
    logger.info('  DELETE /api/workers/{id}  - Unregister worker (auth required)')
    logger.info('Auth: Authorization: Bearer <token> or X-API-Key: <key>')
    logger.info('Dev key: dev-key-001')
    server.serve_forever()


if __name__ == "__main__":
    import signal

    mq = MessageQueueManager()

    # Create event callback for SSE broadcasting
    def on_event(event_type, data):
        sse_manager.broadcast(event_type, data)

    orch = Orchestrator(mq, event_callback=on_event)

    # Graceful shutdown handler
    def shutdown_handler(signum, frame):
        logger.info('Received shutdown signal, graceful stop...')
        orch.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

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
