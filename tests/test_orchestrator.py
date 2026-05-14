"""Tests for Orchestrator."""

import time
import sys
import os

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator, Task, WorkerInfo


def test_orchestrator_create():
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    assert orch.state.name == "IDLE"
    assert len(orch.tasks) == 0
    assert len(orch.workers) == 0
    print("[PASS] test_orchestrator_create")


def test_submit_task():
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    task_id = orch.submit_task("analysis", {"query": "test"}, priority=2, timeout=60)
    assert task_id is not None
    assert task_id in orch.tasks
    assert orch.tasks[task_id].task_type == "analysis"
    assert orch.tasks[task_id].status == "pending"

    orch.stop()
    print("[PASS] test_submit_task")


def test_worker_register():
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    from multi_agent_system.common.message import Message, MessageType, QueueType

    register_msg = Message(
        type=MessageType.WORKER_REGISTER.value,
        action="REGISTER",
        source="worker_1",
        payload={
            "worker_id": "worker_1",
            "worker_type": "analysis",
            "capabilities": ["analysis"],
            "max_concurrent": 2
        }
    )
    mq.enqueue(QueueType.WORKER_TO_ORCHESTRATOR.value, register_msg)

    time.sleep(0.5)

    assert "worker_1" in orch.workers
    assert orch.workers["worker_1"].status == "online"
    assert orch.workers["worker_1"].capabilities == ["analysis"]

    orch.stop()
    print("[PASS] test_worker_register")


def test_task_status():
    mq = MessageQueueManager()
    orch = Orchestrator(mq)

    task_id = orch.submit_task("test", {"data": "test"})
    status = orch.get_task_status(task_id)

    assert status is not None
    assert status["task_id"] == task_id
    assert status["status"] == "pending"

    print("[PASS] test_task_status")


if __name__ == "__main__":
    test_orchestrator_create()
    test_submit_task()
    test_worker_register()
    test_task_status()
    print("\nAll orchestrator tests passed!")