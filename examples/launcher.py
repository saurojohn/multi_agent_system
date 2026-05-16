#!/usr/bin/env python3
"""
Simple launcher - starts Web UI and optionally Telegram Bot
Usage:
  python3 examples/launcher.py                    # Web UI only
  python3 examples/launcher.py --token XXX         # Web UI + Telegram
  TELEGRAM_BOT_TOKEN=XXX python3 examples/launcher.py --token-env
"""

import sys
import os

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'examples'))

import threading
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('launcher')

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Multi-Agent System')
    parser.add_argument('--port', '-p', type=int, default=8081)
    parser.add_argument('--token', '-t', type=str, default=None)
    parser.add_argument('--token-env', action='store_true')
    args = parser.parse_args()

    token = args.token
    if args.token_env:
        token = os.environ.get("TELEGRAM_BOT_TOKEN")

    print("=" * 50)
    print("Multi-Agent System")
    print("=" * 50)

    # Import and start components
    from multi_agent_system.common.queue import MessageQueueManager
    from multi_agent_system.orchestrator.core import Orchestrator
    from multi_agent_system.worker.agent import WorkerAgent

    # Import web_ui_server directly
    import web_ui_server as ws

    # Init
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    # Workers
    workers = []
    for w in [
        {"id": "worker_1", "type": "Analysis", "cap": ["analysis"]},
        {"id": "worker_2", "type": "Research", "cap": ["research"]},
        {"id": "worker_3", "type": "Coding", "cap": ["coding"]}
    ]:
        worker = WorkerAgent(worker_id=w["id"], worker_type=w["type"], capabilities=w["cap"], mq=mq)
        if "analysis" in w["cap"]: worker.register_handler("analysis", lambda d: {"result": f"分析: {d.get('query','')}"})
        if "research" in w["cap"]: worker.register_handler("research", lambda d: {"result": f"研究: {d.get('topic','')}"})
        if "coding" in w["cap"]: worker.register_handler("coding", lambda d: {"result": f"编码: {d.get('code','')}"})
        worker.start()
        workers.append(worker)

    # Web UI
    server = ws.WebUIServer(port=args.port)
    server.set_orchestrator(orch)

    web_thread = threading.Thread(target=server.start, daemon=True)
    web_thread.start()

    # Telegram (if token)
    bot_thread = None
    if token:
        try:
            from multi_agent_system.common.telegram_bot import configure_bot, start_bot
            bot = configure_bot(token, orch)
            bot_thread = threading.Thread(target=start_bot, args=(True,), daemon=True)
            bot_thread.start()
            print("  Telegram: Started")
        except Exception as e:
            print(f"  Telegram: Failed ({e})")

    print(f"\n  Web UI: http://localhost:{args.port}/agent_web_ui.html")
    print(f"  Telegram: {'Enabled' if token else 'Disabled (no token)'}")
    print("\nPress Ctrl+C to stop\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
        for w in workers:
            w.stop()
        orch.stop()

if __name__ == "__main__":
    main()