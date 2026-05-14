"""API Server using YAML configuration."""

import time
import sys
import os

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.config import Config, setup_logging
from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.worker.agent import WorkerAgent


def main():
    # Load configuration
    config = Config()

    # Setup logging
    setup_logging(
        level=config.get('logging.level', 'INFO'),
        format=config.get('logging.format')
    )

    import logging
    logger = logging.getLogger('api_server')

    # Initialize message queue and orchestrator
    mq = MessageQueueManager()
    orch = Orchestrator(
        mq,
        heartbeat_interval=config.get('orchestrator.heartbeat_interval', 10),
        heartbeat_timeout=config.get('orchestrator.heartbeat_timeout', 30),
        max_task_retries=config.get('orchestrator.max_task_retries', 3)
    )
    orch.start()

    # Create workers from config
    workers = {}
    for worker_def in config.workers:
        if not worker_def.get('enabled', True):
            continue

        worker_id = worker_def['worker_id']
        worker_type = worker_def['worker_type']
        capabilities = worker_def['capabilities']

        def make_handler(caps):
            def handler(task_data):
                query = task_data.get("task_data", {}).get("query", "")
                time.sleep(0.5)
                return {cap: f"Completed: {query}" for cap in caps}
            return handler

        w = WorkerAgent(worker_id, worker_type, capabilities, mq)
        for cap in capabilities:
            w.register_handler(cap, make_handler([cap]))
        w.start()
        workers[worker_id] = w
        logger.info(f'Started {worker_id} with capabilities {capabilities}')
        time.sleep(2)

    time.sleep(5)

    # Submit sample tasks
    sample_tasks = [
        ("analysis", {"query": "Q1 revenue trends"}),
        ("research", {"query": "market analysis"}),
        ("coding", {"query": "API integration"}),
        ("design", {"query": "UI mockups"}),
        ("data", {"query": "data pipeline"}),
    ]

    for task_type, task_data in sample_tasks:
        task_id = orch.submit_task(task_type, task_data)
        logger.info(f'Submitted task: {task_id} ({task_type})')

    # Start server
    from examples.api_server import run_dashboard
    port = config.get('server.port', 8080)
    logger.info(f'Starting API server on port {port}...')
    run_dashboard(orch, mq, port)


if __name__ == "__main__":
    main()
