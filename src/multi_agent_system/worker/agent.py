"""Worker Agent - executes tasks from orchestrator."""

import threading
import time
from typing import Dict, Any, Optional, Callable, List

from ..common.message import Message, MessageType, QueueType
from ..common.queue import MessageQueueManager


class WorkerAgent:
    def __init__(self, worker_id: str, worker_type: str,
                 capabilities: List[str], mq: MessageQueueManager,
                 max_concurrent: int = 1, heartbeat_interval: int = 10):
        self.worker_id = worker_id
        self.worker_type = worker_type
        self.capabilities = capabilities
        self.mq = mq
        self.max_concurrent = max_concurrent
        self.heartbeat_interval = heartbeat_interval

        self._running = False
        self._threads: List[threading.Thread] = []
        self._task_handlers: Dict[str, Callable] = {}
        self._active_tasks: Dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    def register_handler(self, task_type: str, handler: Callable):
        self._task_handlers[task_type] = handler

    def start(self):
        self._running = True
        self._register()
        self._threads = [
            threading.Thread(target=self._message_listener, daemon=True),
            threading.Thread(target=self._heartbeat_sender, daemon=True),
        ]
        for t in self._threads:
            t.start()

    def stop(self):
        self._running = False
        self._unregister()
        with self._lock:
            for t in self._active_tasks.values():
                t.join(timeout=5)

    def _register(self):
        msg = Message(
            type=MessageType.WORKER_REGISTER.value,
            action="REGISTER",
            source=self.worker_id,
            payload={
                "worker_id": self.worker_id,
                "worker_type": self.worker_type,
                "capabilities": self.capabilities,
                "max_concurrent": self.max_concurrent
            }
        )
        self.mq.enqueue(QueueType.WORKER_TO_ORCHESTRATOR.value, msg)

    def _unregister(self):
        msg = Message(
            type=MessageType.WORKER_UNREGISTER.value,
            action="UNREGISTER",
            source=self.worker_id,
            payload={"worker_id": self.worker_id}
        )
        self.mq.enqueue(QueueType.WORKER_TO_ORCHESTRATOR.value, msg)

    def _message_listener(self):
        while self._running:
            msg = self.mq.dequeue(QueueType.ORCHESTRATOR_TO_WORKER.value, timeout=1.0)
            if msg and (msg.target == self.worker_id or msg.target == "*"):
                self._handle_message(msg)

    def _handle_message(self, msg: Message):
        if msg.type == MessageType.TASK.value:
            # Workers only process ASSIGN and RETRY (not NEW_TASK which is for scheduler)
            if msg.action in ("ASSIGN", "RETRY"):
                self._handle_task(msg)
        elif msg.type == MessageType.SHUTDOWN.value:
            self._running = False

    def _handle_task(self, msg: Message):
        task_id = msg.payload.get("task_id")
        task_type = msg.payload.get("task_type")

        # Skip if this worker isn't the intended target (check before lock to avoid wasted work)
        if msg.target != "*" and msg.target != self.worker_id:
            return

        with self._lock:
            # Skip if already processing this task
            if task_id in self._active_tasks:
                return
            if len(self._active_tasks) >= self.max_concurrent:
                self._send_reject(task_id, "worker_busy")
                return
            t = threading.Thread(
                target=self._execute_task,
                args=(task_id, task_type, msg.payload)
            )
            self._active_tasks[task_id] = t
            t.start()

    def _execute_task(self, task_id: str, task_type: str, task_data: Dict):
        # Check if task is still valid (not already completed/failed by another worker)
        # This prevents duplicate processing when multiple workers receive broadcast messages
        with self._lock:
            if task_id not in self._active_tasks:
                return  # Already handled by another worker

        handler = self._task_handlers.get(task_type)
        result = None
        error = None
        status = "completed"

        try:
            if handler:
                result = handler(task_data)
            else:
                error = f"No handler for task type: {task_type}"
                status = "failed"
        except Exception as e:
            error = str(e)
            status = "failed"
        finally:
            with self._lock:
                if task_id in self._active_tasks:
                    del self._active_tasks[task_id]
            self._send_result(task_id, status, result, error)

    def _send_result(self, task_id: str, status: str,
                     result: Any, error: Optional[str] = None):
        msg = Message(
            type=MessageType.TASK_RESULT.value,
            action="COMPLETE",
            payload={
                "task_id": task_id,
                "status": status,
                "result": result,
                "error": error,
                "worker_id": self.worker_id
            }
        )
        self.mq.enqueue(QueueType.WORKER_TO_ORCHESTRATOR.value, msg)

    def _send_reject(self, task_id: str, reason: str):
        msg = Message(
            type=MessageType.ERROR.value,
            action="REJECT",
            payload={
                "task_id": task_id,
                "error": reason,
                "worker_id": self.worker_id
            }
        )
        self.mq.enqueue(QueueType.WORKER_TO_ORCHESTRATOR.value, msg)

    def _heartbeat_sender(self):
        while self._running:
            msg = Message(
                type=MessageType.HEARTBEAT.value,
                source=self.worker_id,
                payload={
                    "worker_id": self.worker_id,
                    "status": "alive",
                    "active_tasks": list(self._active_tasks.keys())
                }
            )
            self.mq.enqueue(QueueType.WORKER_HEARTBEAT.value, msg)
            time.sleep(self.heartbeat_interval)