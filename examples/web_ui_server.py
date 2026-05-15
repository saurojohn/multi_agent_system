#!/usr/bin/env python3
"""Web UI Server for multi-agent system management."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import threading
import json
import time
from typing import Dict, List
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.common.web_ui import WebUIManager, AgentConfig


class WebUIServer:
    """Web UI server with REST API."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8081):
        self.host = host
        self.port = port
        self.ui_manager = WebUIManager()
        self._running = False
        self._server = None
        self._web_dir = os.path.dirname(__file__)

    def set_orchestrator(self, orchestrator):
        """Set the orchestrator."""
        self.ui_manager.set_orchestrator(orchestrator)

    def start(self):
        """Start the web server."""
        self._running = True
        self._server = HTTPServer((self.host, self.port), self._handler_factory)
        thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        thread.start()
        print(f"Web UI Server running at http://{self.host}:{self.port}")
        print(f"Open http://localhost:{self.port}/agent_web_ui.html in your browser")

    def stop(self):
        """Stop the web server."""
        self._running = False
        if self._server:
            self._server.shutdown()

    def _handler_factory(self, request, client_address, server):
        """Create request handler with access to self."""
        return WebUIHandler(request, client_address, server, self)


class WebUIHandler(SimpleHTTPRequestHandler):
    """Handler for web UI requests."""

    def __init__(self, request, client_address, server, ui_server: WebUIServer):
        self.ui_server = ui_server
        super().__init__(request, client_address, server)

    def do_GET(self):
        """Handle GET requests."""
        path = urlparse(self.path).path

        if path == "/" or path == "/index.html":
            self.send_response(302)
            self.send_header("Location", "/agent_web_ui.html")
            self.end_headers()
            return

        if path == "/api/dashboard":
            self.send_json_response(self.ui_server.ui_manager.get_dashboard_data())
            return

        if path == "/api/agents":
            self.send_json_response({
                "agents": [
                    {"agent_id": "worker_1", "name": "分析Agent", "type": "analysis",
                     "capabilities": ["analysis"], "enabled": True},
                    {"agent_id": "worker_2", "name": "研究Agent", "type": "research",
                     "capabilities": ["research"], "enabled": True},
                    {"agent_id": "worker_3", "name": "编码Agent", "type": "coding",
                     "capabilities": ["coding"], "enabled": True},
                    {"agent_id": "worker_4", "name": "设计Agent", "type": "design",
                     "capabilities": ["design"], "enabled": False},
                    {"agent_id": "worker_5", "name": "数据Agent", "type": "data",
                     "capabilities": ["data"], "enabled": True}
                ]
            })
            return

        if path == "/api/tasks":
            tasks = []
            if self.ui_server.ui_manager.orchestrator:
                try:
                    tasks = self.ui_server.ui_manager.orchestrator.get_all_tasks()
                except:
                    pass
            self.send_json_response({"tasks": tasks})
            return

        if path == "/api/workers":
            workers = []
            if self.ui_server.ui_manager.orchestrator:
                try:
                    workers = self.ui_server.ui_manager.orchestrator.get_workers_status()
                except:
                    pass
            self.send_json_response({"workers": workers})
            return

        if path == "/api/messages":
            messages = self.ui_server.ui_manager.get_messages(50)
            self.send_json_response({"messages": messages})
            return

        # Serve static files
        if path.startswith("/"):
            file_path = os.path.join(os.path.dirname(__file__), path.lstrip("/"))
            if os.path.exists(file_path) and os.path.isfile(file_path):
                self.send_file(file_path)
                return

        self.send_error(404, "Not Found")

    def do_POST(self):
        """Handle POST requests."""
        path = urlparse(self.path).path
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else '{}'

        try:
            data = json.loads(body) if body else {}
        except:
            data = {}

        if path == "/api/tasks/submit":
            task_type = data.get("type", "analysis")
            task_data = data.get("data", {})
            priority = int(data.get("priority", 2))
            task_id = self.ui_server.ui_manager.submit_task(task_type, task_data, priority)
            self.send_json_response({"success": True, "task_id": task_id})
            return

        if path == "/api/agents/add":
            config = AgentConfig(
                agent_id=data.get("agent_id"),
                name=data.get("name"),
                agent_type=data.get("type"),
                capabilities=data.get("capabilities", []),
                enabled=data.get("enabled", True),
                max_concurrent_tasks=data.get("max_tasks", 3)
            )
            self.ui_server.ui_manager.register_agent(config)
            self.send_json_response({"success": True})
            return

        if path == "/api/agents/toggle":
            agent_id = data.get("agent_id")
            enabled = data.get("enabled", True)
            self.ui_server.ui_manager.update_agent(agent_id, enabled=enabled)
            self.send_json_response({"success": True})
            return

        if path == "/api/agents/delete":
            agent_id = data.get("agent_id")
            self.ui_server.ui_manager.delete_agent(agent_id)
            self.send_json_response({"success": True})
            return

        if path == "/api/chat":
            message = data.get("message", "")
            response = self._process_chat(message)
            self.send_json_response({"response": response})
            return

        self.send_error(404, "Not Found")

    def _process_chat(self, message: str) -> str:
        """Process chat message."""
        msg_lower = message.lower().strip()

        if msg_lower in ["你好", "hi", "hello"]:
            return "你好！我是Multi-Agent系统的管理助手。有什么我可以帮助你的吗？"
        elif msg_lower in ["帮助", "help"]:
            return "可用命令:\n- 查看状态\n- 提交任务\n- 管理Agent\n- 系统设置"
        elif msg_lower.startswith("查看状态"):
            return "当前系统状态:\n- 在线Worker: 3\n- 待处理任务: 2\n- 运行中任务: 1"
        elif msg_lower.startswith("提交任务"):
            return "好的，要提交任务请访问任务管理页面，或告诉我任务类型和数据。"
        elif msg_lower in ["status", "状态"]:
            data = self.ui_server.ui_manager.get_dashboard_data()
            return f"系统状态:\n- Agent: {data['agents']['total']}个\n- 在线Worker: {data['workers']['online']}\n- 任务: {data['tasks']['pending']}待处理, {data['tasks']['running']}运行中"
        else:
            return f"收到消息: {message} - 我会帮你处理。功能开发中..."

    def send_json_response(self, data: Dict):
        """Send JSON response."""
        response = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(response))
        self.end_headers()
        self.wfile.write(response)

    def send_file(self, filepath: str):
        """Send a file."""
        content_type = "text/html"
        if filepath.endswith(".js"):
            content_type = "application/javascript"
        elif filepath.endswith(".css"):
            content_type = "text/css"
        elif filepath.endswith(".json"):
            content_type = "application/json"

        with open(filepath, 'rb') as f:
            content = f.read()

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", len(content))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        """Override to suppress logging."""
        pass


def main():
    # Initialize
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    # Start workers
    from multi_agent_system.worker.agent import WorkerAgent

    workers = [
        {"id": "worker_1", "type": "Analysis", "capabilities": ["analysis"]},
        {"id": "worker_2", "type": "Research", "capabilities": ["research"]},
        {"id": "worker_3", "type": "Coding", "capabilities": ["coding"]}
    ]

    for w in workers:
        worker = WorkerAgent(
            worker_id=w["id"],
            worker_type=w["type"],
            capabilities=w["capabilities"],
            mq=mq
        )
        worker.register_handler("analysis", lambda d: {"result": f"分析完成: {d.get('query', '')}"})
        worker.register_handler("research", lambda d: {"result": f"研究完成: {d.get('topic', '')}"})
        worker.register_handler("coding", lambda d: {"result": f"编码完成: {d.get('code', '')}"})
        worker.start()

    # Start web UI
    server = WebUIServer(port=8081)
    server.set_orchestrator(orch)
    server.start()

    print("\nWeb UI is ready!")
    print("Open http://localhost:8081/agent_web_ui.html in your browser")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
        for w in workers:
            pass  # workers will be stopped on interrupt
        orch.stop()
        server.stop()


if __name__ == "__main__":
    main()