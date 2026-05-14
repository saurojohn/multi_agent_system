"""Thread-safe message queue manager."""

import threading
from collections import defaultdict
from queue import PriorityQueue, Empty
from typing import Dict, Optional

from .message import Message, MessagePriority


class MessageQueueManager:
    def __init__(self):
        self._queues: Dict[str, PriorityQueue] = defaultdict(
            lambda: PriorityQueue(maxsize=10000)
        )
        self._lock = threading.Lock()

    def enqueue(self, queue_name: str, message: Message, timeout: float = None) -> bool:
        with self._lock:
            q = self._queues[queue_name]
            try:
                q.put((message.priority.value, message.id, message), timeout=timeout)
                return True
            except Exception:
                return False

    def dequeue(self, queue_name: str, timeout: float = 1.0) -> Optional[Message]:
        q = self._queues.get(queue_name)
        if not q:
            return None
        try:
            _, _, message = q.get(timeout=timeout)
            return message
        except Empty:
            return None

    def peek(self, queue_name: str) -> Optional[Message]:
        with self._lock:
            q = self._queues.get(queue_name)
            if not q or q.empty():
                return None
            try:
                _, _, msg = q.get_nowait()
                q.put((msg.priority.value, msg.id, msg))
                return msg
            except Empty:
                return None

    def size(self, queue_name: str) -> int:
        return self._queues.get(queue_name, PriorityQueue()).qsize()

    def clear(self, queue_name: str):
        with self._lock:
            if queue_name in self._queues:
                self._queues[queue_name].clear()