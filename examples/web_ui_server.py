#!/usr/bin/env python3
"""Web UI Server for multi-agent system management."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import threading
import json
import time
import logging
from typing import Dict, List
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger('web_ui_server')

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
            # Get agents from orchestrator workers and UI-managed workers
            agents = []
            ui_workers = getattr(self.ui_server.ui_manager, '_workers', {}) or {}

            if self.ui_server.ui_manager.orchestrator:
                try:
                    workers = self.ui_server.ui_manager.orchestrator.get_workers_status()
                    for w in workers:
                        worker_id = w.get('worker_id', '')
                        # Extract type from worker_id or use default
                        if 'worker_1' in worker_id or 'analysis' in worker_id.lower():
                            agent_type = 'analysis'
                            name = '分析Agent'
                        elif 'worker_2' in worker_id or 'research' in worker_id.lower():
                            agent_type = 'research'
                            name = '研究Agent'
                        elif 'worker_3' in worker_id or 'coding' in worker_id.lower():
                            agent_type = 'coding'
                            name = '编码Agent'
                        elif 'worker_4' in worker_id or 'design' in worker_id.lower():
                            agent_type = 'design'
                            name = '设计Agent'
                        elif 'worker_5' in worker_id or 'data' in worker_id.lower():
                            agent_type = 'data'
                            name = '数据Agent'
                        else:
                            agent_type = worker_id.replace('worker_', '') or 'unknown'
                            name = f'{agent_type.title()} Agent'

                        # Check if this worker has AI config
                        ai_provider = None
                        ai_model = None
                        if worker_id in ui_workers:
                            wobj = ui_workers[worker_id]
                            ai_provider = getattr(wobj, '_ai_provider', None)
                            ai_model = getattr(wobj, '_ai_model', None)

                        agents.append({
                            "agent_id": worker_id,
                            "name": name,
                            "type": agent_type,
                            "capabilities": [agent_type],
                            "enabled": w.get('status') == 'online',
                            "ai_provider": ai_provider,
                            "ai_model": ai_model
                        })
                except Exception as e:
                    logger.error(f"Error getting agents: {e}")

            # Also add agents from UI-managed workers that are not in orchestrator
            for wid, wobj in ui_workers.items():
                if not any(a['agent_id'] == wid for a in agents):
                    agent_type = getattr(wobj, '_agent_type', 'unknown')
                    name = getattr(wobj, '_name', wid)
                    ai_provider = getattr(wobj, '_ai_provider', None)
                    ai_model = getattr(wobj, '_ai_model', None)
                    agents.append({
                        "agent_id": wid,
                        "name": name,
                        "type": agent_type,
                        "capabilities": wobj.capabilities if hasattr(wobj, 'capabilities') else [agent_type],
                        "enabled": True,
                        "ai_provider": ai_provider,
                        "ai_model": ai_model
                    })
            self.send_json_response({"agents": agents or [
                {"agent_id": "worker_1", "name": "分析Agent", "type": "analysis", "capabilities": ["analysis"], "enabled": True},
                {"agent_id": "worker_2", "name": "研究Agent", "type": "research", "capabilities": ["research"], "enabled": True},
                {"agent_id": "worker_3", "name": "编码Agent", "type": "coding", "capabilities": ["coding"], "enabled": True}
            ]})
            return

        if path == "/api/tasks":
            tasks = []
            if self.ui_server.ui_manager.orchestrator:
                try:
                    tasks = self.ui_server.ui_manager.orchestrator.get_all_tasks()
                except Exception as e:
                    logger.error(f"Error getting tasks: {e}")
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
            from multi_agent_system.worker.agent import WorkerAgent
            from multi_agent_system.common.ai_agent import AIAgent, AIConfig, AIProvider, create_ai_handler

            agent_id = data.get("agent_id")
            name = data.get("name")
            agent_type = data.get("type", "analysis")
            capabilities = data.get("capabilities", [agent_type])
            ai_provider = data.get("ai_provider", "") or None
            ai_model = data.get("ai_model", "") or None
            ai_api_key = data.get("ai_api_key", "") or None

            # Create and register the worker
            worker = WorkerAgent(
                worker_id=agent_id,
                worker_type=agent_type.title(),
                capabilities=capabilities,
                mq=self.ui_server.ui_manager.orchestrator.mq
            )

            # Use AI handler if provider is specified
            if ai_provider:
                try:
                    # Get global AI config as base
                    global_ai_config = getattr(self.ui_server, '_ai_config', {}) or {}
                    # Agent-specific API key takes priority over global
                    config_api_key = ai_api_key if ai_api_key else global_ai_config.get('api_key', '')
                    config_base_url = global_ai_config.get('base_url', '')

                    # Set provider-specific default base URLs
                    if ai_provider == 'minimax' and not config_base_url:
                        config_base_url = "https://api.minimax.chat/v1"
                    elif ai_provider == 'deepseek' and not config_base_url:
                        config_base_url = "https://api.deepseek.com/v1"
                    elif ai_provider == 'zhipu' and not config_base_url:
                        config_base_url = "https://open.bigmodel.cn/api/paas/v4"

                    ai_config = AIConfig(
                        provider=AIProvider(ai_provider),
                        api_key=config_api_key,
                        model=ai_model or global_ai_config.get('model', 'gpt-4'),
                        base_url=config_base_url
                    )
                    ai_handler = create_ai_handler(ai_config)

                    # Register AI handler for all capabilities
                    for cap in capabilities:
                        worker.register_handler(cap, ai_handler)

                    logger.info(f"Registered AI handler for {agent_id}: {ai_provider}/{ai_model or 'default'}")
                except Exception as e:
                    logger.error(f"Error creating AI handler for {agent_id}: {e}")
                    # Fallback to default handlers
                    for cap in capabilities:
                        worker.register_handler(cap, lambda d: {"result": f"处理完成: {d}"})
            else:
                # Register default handlers based on type
                if "analysis" in capabilities:
                    worker.register_handler("analysis", lambda d: {"result": f"分析完成: {d.get('query', '')}"})
                if "research" in capabilities:
                    worker.register_handler("research", lambda d: {"result": f"研究完成: {d.get('topic', '')}"})
                if "coding" in capabilities:
                    worker.register_handler("coding", lambda d: {"result": f"编码完成: {d.get('code', '')}"})
                if "design" in capabilities:
                    worker.register_handler("design", lambda d: {"result": f"设计完成: {d.get('spec', '')}"})
                if "data" in capabilities:
                    worker.register_handler("data", lambda d: {"result": f"数据处理完成: {d.get('dataset', '')}"})

            # Store AI config on worker object for API display
            worker._ai_provider = ai_provider
            worker._ai_model = ai_model
            worker._agent_type = agent_type
            worker._name = name

            worker.start()

            # Store worker reference
            self.ui_server.ui_manager._workers = getattr(self.ui_server.ui_manager, '_workers', {})
            self.ui_server.ui_manager._workers[agent_id] = worker

            self.send_json_response({"success": True, "agent_id": agent_id, "ai_provider": ai_provider, "ai_model": ai_model})
            return

        if path == "/api/agents/toggle":
            agent_id = data.get("agent_id")
            enabled = data.get("enabled", True)

            # Toggle worker status in orchestrator
            if self.ui_server.ui_manager.orchestrator and agent_id in self.ui_server.ui_manager.orchestrator.workers:
                worker_info = self.ui_server.ui_manager.orchestrator.workers[agent_id]
                worker_info.status = "online" if enabled else "offline"

            self.send_json_response({"success": True})
            return

        if path == "/api/agents/delete":
            agent_id = data.get("agent_id")

            # Stop and remove the worker
            workers = getattr(self.ui_server.ui_manager, '_workers', {})
            if agent_id in workers:
                workers[agent_id].stop()
                del workers[agent_id]

            self.send_json_response({"success": True})
            return

        if path == "/api/ai/configure":
            provider = data.get("provider", "openai")
            api_key = data.get("api_key", "")
            model = data.get("model", "gpt-4")
            base_url = data.get("base_url", "")

            # Store AI config
            if not hasattr(self.ui_server, '_ai_config'):
                self.ui_server._ai_config = {}
            self.ui_server._ai_config['provider'] = provider
            self.ui_server._ai_config['api_key'] = api_key
            self.ui_server._ai_config['model'] = model
            self.ui_server._ai_config['base_url'] = base_url

            self.send_json_response({"success": True, "message": "AI配置已更新"})
            return

        if path == "/api/ai/chat":
            from multi_agent_system.common.ai_agent import AIAgent, AIConfig, AIProvider, AIChatSession

            # Get AI config
            ai_config = getattr(self.ui_server, '_ai_config', {}) or {}

            # Check if API key is configured
            api_key = ai_config.get('api_key', '')
            if not api_key:
                self.send_json_response({
                    "error": "请先在设置中配置AI API Key",
                    "provider": ai_config.get('provider', 'openai'),
                    "model": ai_config.get('model', 'gpt-4o')
                })
                return

            config = AIConfig(
                provider=AIProvider(ai_config.get('provider', 'openai')),
                api_key=api_key,
                model=ai_config.get('model', 'gpt-4o'),
                base_url=ai_config.get('base_url', '')
            )

            message = data.get("message", "")
            session_id = data.get("session_id")

            if not hasattr(self.ui_server, '_chat_sessions'):
                self.ui_server._chat_sessions = {}

            if session_id and session_id in self.ui_server._chat_sessions:
                session = self.ui_server._chat_sessions[session_id]
            else:
                session = AIChatSession(AIAgent(config))
                session_id = f"sess_{int(time.time())}"
                self.ui_server._chat_sessions[session_id] = session

            response = session.send(message)

            if response.success:
                self.send_json_response({
                    "response": response.content,
                    "session_id": session_id,
                    "provider": response.provider,
                    "model": response.model
                })
            else:
                self.send_json_response({
                    "error": response.error,
                    "session_id": session_id
                })
            return

        if path == "/api/chat":
            message = data.get("message", "")

            # Check if AI is configured for chat
            ai_config = getattr(self.ui_server, '_ai_config', {}) or {}
            api_key = ai_config.get('api_key', '')

            if api_key and message:
                # Use AI chat
                try:
                    from multi_agent_system.common.ai_agent import AIAgent, AIConfig, AIProvider, AIChatSession
                    config = AIConfig(
                        provider=AIProvider(ai_config.get('provider', 'openai')),
                        api_key=api_key,
                        model=ai_config.get('model', 'gpt-4o'),
                        base_url=ai_config.get('base_url', '')
                    )
                    agent = AIAgent(config)
                    response = agent.chat([{"role": "user", "content": message}])
                    if response.success:
                        self.send_json_response({"response": response.content})
                    else:
                        self.send_json_response({"response": f"AI错误: {response.error}"})
                except Exception as e:
                    self.send_json_response({"response": f"AI错误: {str(e)}"})
            else:
                # Use simple chatbot
                response = self._process_chat(message)
                self.send_json_response({"response": response})
            return

        if path == "/api/telegram/configure":
            token = data.get("token", "")
            action = data.get("action", "configure")

            if not token:
                self.send_json_response({"success": False, "error": "Token is required"})
                return

            try:
                from multi_agent_system.common.telegram_bot import TelegramBot

                if not hasattr(self.ui_server, '_telegram_bot'):
                    self.ui_server._telegram_bot = None
                if not hasattr(self.ui_server, '_telegram_thread'):
                    self.ui_server._telegram_thread = None

                if action == "test":
                    # Test the token by sending a simple request
                    import urllib.request
                    test_url = f"https://api.telegram.org/bot{token}/getMe"
                    try:
                        with urllib.request.urlopen(test_url, timeout=10) as response:
                            result = json.loads(response.read().decode())
                            if result.get("ok"):
                                self.send_json_response({"success": True, "message": "连接成功", "connected": True, "bot_name": result.get("result", {}).get("username", "")})
                            else:
                                self.send_json_response({"success": False, "error": "无效Token", "connected": False})
                    except Exception as e:
                        self.send_json_response({"success": False, "error": f"连接失败: {str(e)}", "connected": False})
                    return

                # Configure and start
                self.ui_server._telegram_bot = TelegramBot(token=token)
                self.ui_server._telegram_bot.set_orchestrator(self.ui_server.ui_manager.orchestrator)

                # Start bot in background thread
                def run_telegram():
                    self.ui_server._telegram_bot.start(polling=True)
                self.ui_server._telegram_thread = threading.Thread(target=run_telegram, daemon=True)
                self.ui_server._telegram_thread.start()

                self.send_json_response({"success": True, "message": "Telegram Bot已启动", "running": True})
            except Exception as e:
                logger.error(f"Telegram configure error: {e}")
                self.send_json_response({"success": False, "error": str(e)})
            return

        if path == "/api/telegram/start":
            try:
                if hasattr(self.ui_server, '_telegram_bot') and self.ui_server._telegram_bot:
                    if self.ui_server._telegram_bot._running:
                        self.send_json_response({"success": True, "message": "Telegram Bot已在运行"})
                        return
                    def run_telegram():
                        self.ui_server._telegram_bot.start(polling=True)
                    self.ui_server._telegram_thread = threading.Thread(target=run_telegram, daemon=True)
                    self.ui_server._telegram_thread.start()
                    self.send_json_response({"success": True, "message": "Telegram Bot已启动"})
                else:
                    self.send_json_response({"success": False, "error": "请先配置Telegram Bot"})
            except Exception as e:
                self.send_json_response({"success": False, "error": str(e)})
            return

        if path == "/api/telegram/stop":
            try:
                if hasattr(self.ui_server, '_telegram_bot') and self.ui_server._telegram_bot:
                    self.ui_server._telegram_bot.stop()
                    self.ui_server._telegram_bot = None
                    self.ui_server._telegram_thread = None
                self.send_json_response({"success": True, "message": "Telegram Bot已停止"})
            except Exception as e:
                self.send_json_response({"success": False, "error": str(e)})
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