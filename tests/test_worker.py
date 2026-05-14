"""Tests for Worker Agent."""

import time
import sys
import os

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.worker.agent import WorkerAgent
from multi_agent_system.common.message import Message, MessageType, QueueType


def test_worker_create():
    mq = MessageQueueManager()
    worker = WorkerAgent("w1", "analysis", ["analysis"], mq)

    assert worker.worker_id == "w1"
    assert worker.worker_type == "analysis"
    assert worker.capabilities == ["analysis"]
    print("[PASS] test_worker_create")


def test_worker_register_handler():
    mq = MessageQueueManager()
    worker = WorkerAgent("w1", "analysis", ["analysis"], mq)

    def handler(task_data):
        return {"result": "ok"}

    worker.register_handler("analysis", handler)
    assert "analysis" in worker._task_handlers
    print("[PASS] test_worker_register_handler")


def test_worker_lifecycle():
    mq = MessageQueueManager()
    worker = WorkerAgent("w1", "analysis", ["analysis"], mq)

    result_holder = []

    def handler(task_data):
        result_holder.append(task_data)
        return {"processed": True}

    worker.register_handler("analysis", handler)
    worker.start()

    time.sleep(1)

    msg = Message(
        type=MessageType.TASK.value,
        action="ASSIGN",
        source="test",
        target="w1",
        payload={
            "task_id": "task_1",
            "task_type": "analysis",
            "task_data": {"query": "test query"}
        }
    )
    mq.enqueue(QueueType.ORCHESTRATOR_TO_WORKER.value, msg)

    time.sleep(2)

    worker.stop()

    assert len(result_holder) > 0
    print("[PASS] test_worker_lifecycle")


if __name__ == "__main__":
    test_worker_create()
    test_worker_register_handler()
    test_worker_lifecycle()
    print("\nAll worker tests passed!")