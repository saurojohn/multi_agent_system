"""Tests for message queue."""

import time
import sys
import os

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.common.message import Message, MessageType, MessagePriority


def test_queue_basic():
    mq = MessageQueueManager()

    msg1 = Message(type="TEST", action="test1", payload={"data": "1"})
    msg2 = Message(type="TEST", action="test2", payload={"data": "2"})

    assert mq.enqueue("test_q", msg1)
    assert mq.enqueue("test_q", msg2)
    assert mq.size("test_q") == 2

    retrieved = mq.dequeue("test_q")
    assert retrieved is not None
    assert "data" in retrieved.payload

    assert mq.size("test_q") == 1
    print("[PASS] test_queue_basic")


def test_queue_priority():
    mq = MessageQueueManager()

    msg_low = Message(type="TEST", action="low", priority=MessagePriority.LOW)
    msg_high = Message(type="TEST", action="high", priority=MessagePriority.HIGH)

    mq.enqueue("priority_q", msg_low)
    mq.enqueue("priority_q", msg_high)

    first = mq.dequeue("priority_q")
    assert first.priority == MessagePriority.HIGH
    print("[PASS] test_queue_priority")


def test_queue_empty():
    mq = MessageQueueManager()
    result = mq.dequeue("nonexistent", timeout=0.1)
    assert result is None
    print("[PASS] test_queue_empty")


if __name__ == "__main__":
    test_queue_basic()
    test_queue_priority()
    test_queue_empty()
    print("\nAll queue tests passed!")