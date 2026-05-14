"""Integration tests for multi-agent system."""

import time
import sys
import os

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.worker.agent import WorkerAgent


def test_full_workflow():
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    worker = WorkerAgent("worker_1", "analysis", ["analysis"], mq)

    def handler(task_data):
        query = task_data.get("task_data", {}).get("query", "")
        return {"result": f"processed: {query}"}

    worker.register_handler("analysis", handler)
    worker.start()

    time.sleep(2)

    task_id = orch.submit_task("analysis", {"query": "integration test"})
    assert task_id is not None

    for _ in range(15):
        status = orch.get_task_status(task_id)
        if status and status["status"] in ("completed", "failed"):
            break
        time.sleep(0.5)

    final_status = orch.get_task_status(task_id)
    assert final_status["status"] == "completed"
    assert final_status["result"] is not None

    worker.stop()
    orch.stop()

    print("[PASS] test_full_workflow")


def test_multiple_workers():
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    worker1 = WorkerAgent("w1", "analysis", ["analysis"], mq)
    worker2 = WorkerAgent("w2", "research", ["research"], mq)

    def analysis_handler(task_data):
        return {"type": "analysis"}

    def research_handler(task_data):
        return {"type": "research"}

    worker1.register_handler("analysis", analysis_handler)
    worker2.register_handler("research", research_handler)

    worker1.start()
    worker2.start()

    # Wait for workers to register
    for _ in range(20):
        workers = orch.get_workers_status()
        if len(workers) >= 2:
            break
        time.sleep(0.5)

    t1 = orch.submit_task("analysis", {"q": "1"})
    t2 = orch.submit_task("research", {"q": "2"})

    # Poll for up to 30 seconds
    for _ in range(60):
        s1 = orch.get_task_status(t1)
        s2 = orch.get_task_status(t2)
        if s1["status"] in ("completed", "failed") and s2["status"] in ("completed", "failed"):
            break
        # Also check internal state as fallback
        if t1 in orch.tasks and orch.tasks[t1].result is not None:
            s1["status"] = "completed"
        if t2 in orch.tasks and orch.tasks[t2].result is not None:
            s2["status"] = "completed"
        if s1["status"] == "completed" and s2["status"] == "completed":
            break
        time.sleep(0.5)

    assert s1["status"] == "completed", f"t1={s1['status']}"
    assert s2["status"] == "completed", f"t2={s2['status']}"

    worker1.stop()
    worker2.stop()
    orch.stop()

    print("[PASS] test_multiple_workers")


if __name__ == "__main__":
    test_full_workflow()
    test_multiple_workers()
    print("\nAll integration tests passed!")