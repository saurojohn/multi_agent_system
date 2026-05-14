"""Simple demo showing Orchestrator and Worker collaboration."""

import time
import sys
import os

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.worker.agent import WorkerAgent


def main():
    # Shared message queue
    mq = MessageQueueManager()

    # Create and start orchestrator
    orchestrator = Orchestrator(mq)
    orchestrator.start()
    print("[Main] Orchestrator started")

    # Create and start workers
    worker1 = WorkerAgent(
        worker_id="worker_1",
        worker_type="analysis",
        capabilities=["analysis", "research"],
        mq=mq,
        max_concurrent=2
    )

    worker2 = WorkerAgent(
        worker_id="worker_2",
        worker_type="research",
        capabilities=["research"],
        mq=mq,
        max_concurrent=2
    )

    # Register task handlers
    def analysis_handler(task_data):
        query = task_data.get("task_data", {}).get("query", "")
        print(f"[Worker1] Processing analysis: {query}")
        time.sleep(1)
        return {"analysis": f"Result for: {query}"}

    def research_handler(task_data):
        query = task_data.get("task_data", {}).get("query", "")
        print(f"[Worker2] Processing research: {query}")
        time.sleep(1)
        return {"research": f"Research result for: {query}"}

    worker1.register_handler("analysis", analysis_handler)
    worker2.register_handler("research", research_handler)

    worker1.start()
    worker2.start()
    print("[Main] Workers started")

    # Wait for workers to register
    print("[Main] Waiting for workers to register...")
    for _ in range(10):
        workers_status = orchestrator.get_workers_status()
        online_workers = [w for w in workers_status if w['status'] != 'offline']
        print(f"[Main] Online workers: {len(online_workers)}/{len(workers_status)}")
        if len(online_workers) >= 2:
            break
        time.sleep(1)

    # Submit tasks
    print("\n[Main] Submitting tasks...")
    task1 = orchestrator.submit_task(
        task_type="analysis",
        task_data={"query": "analyze market trends"},
        priority=2
    )
    print(f"[Main] Submitted task1: {task1}")

    task2 = orchestrator.submit_task(
        task_type="research",
        task_data={"query": "research AI agents"},
        priority=2
    )
    print(f"[Main] Submitted task2: {task2}")

    # Wait for tasks to complete
    print("\n[Main] Waiting for results...")
    for _ in range(10):
        status1 = orchestrator.get_task_status(task1)
        status2 = orchestrator.get_task_status(task2)
        print(f"[Main] Task1 status: {status1['status']}, Task2 status: {status2['status']}")
        if status1['status'] in ('completed', 'failed') and status2['status'] in ('completed', 'failed'):
            break
        time.sleep(1)

    print("\n[Main] Final Results:")
    print(f"Task1: {orchestrator.get_task_status(task1)}")
    print(f"Task2: {orchestrator.get_task_status(task2)}")
    print(f"\nWorkers status: {orchestrator.get_workers_status()}")

    # Cleanup
    worker1.stop()
    worker2.stop()
    orchestrator.stop()
    print("\n[Main] Done!")


if __name__ == "__main__":
    main()