"""Worker client example - can be run as separate process or container."""

import argparse
import time
import sys
import os
import logging

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.worker.agent import WorkerAgent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('worker_client')


def main():
    parser = argparse.ArgumentParser(description='Worker Client')
    parser.add_argument('--worker-id', required=True, help='Worker ID')
    parser.add_argument('--worker-type', default='general', help='Worker Type')
    parser.add_argument('--capabilities', required=True, help='Comma-separated capabilities')
    args = parser.parse_args()

    capabilities = [c.strip() for c in args.capabilities.split(',')]

    mq = MessageQueueManager()
    worker = WorkerAgent(args.worker_id, args.worker_type, capabilities, mq)

    # Register handlers for each capability
    def make_handler(cap):
        def handler(task_data):
            query = task_data.get("task_data", {}).get("query", "unknown")
            logger.info(f'[{args.worker_id}] Processing {cap}: {query}')
            time.sleep(1)  # Simulate work
            return {cap: f"Completed: {query}"}
        return handler

    for cap in capabilities:
        worker.register_handler(cap, make_handler(cap))

    logger.info(f'Starting worker {args.worker_id} with capabilities {capabilities}')
    worker.start()

    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info(f'Stopping worker {args.worker_id}')
        worker.stop()


if __name__ == "__main__":
    main()
