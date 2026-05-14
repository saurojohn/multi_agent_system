"""Orchestrator - Master Agent that coordinates workers."""

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from ..common.message import Message, MessageType, QueueType, MessagePriority
from ..common.queue import MessageQueueManager
from ..common.timeout import TimeoutManager


class OrchestratorState(Enum):
    IDLE = "idle"
    PLANNING = "planning"
    DISTRIBUTING = "distributing"
    MONITORING = "monitoring"
    AGGREGATING = "aggregating"
    SHUTDOWN = "shutdown"


@dataclass
class Task:
    task_id: str
    task_type: str
    task_data: Dict
    priority: int = 2
    timeout: int = 300
    dependencies: List[str] = field(default_factory=list)
    status: str = "pending"
    assigned_worker: Optional[str] = None
    result: Optional[Dict] = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    retry_count: int = 0


@dataclass
class WorkerInfo:
    worker_id: str
    worker_type: str
    capabilities: List[str]
    status: str = "offline"
    current_task_id: Optional[str] = None
    max_concurrent: int = 1
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    completed_tasks: int = 0
    failed_tasks: int = 0


class Orchestrator:
    def __init__(self, mq: MessageQueueManager,
                 heartbeat_interval: int = 10,
                 heartbeat_timeout: int = 30,
                 max_task_retries: int = 3):
        self.mq = mq
        self.state = OrchestratorState.IDLE
        self.tasks: Dict[str, Task] = {}
        self.workers: Dict[str, WorkerInfo] = {}
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.max_task_retries = max_task_retries
        self.timeout_manager = TimeoutManager()

        self._running = False
        self._threads: List[threading.Thread] = []
        self._lock = threading.RLock()

    def start(self):
        self._running = True
        self._threads = [
            threading.Thread(target=self._message_processor, daemon=True),
            threading.Thread(target=self._heartbeat_monitor, daemon=True),
            threading.Thread(target=self._task_scheduler, daemon=True),
            threading.Thread(target=self._timeout_checker, daemon=True),
        ]
        for t in self._threads:
            t.start()

    def stop(self):
        self._running = False
        for t in self._threads:
            t.join(timeout=5)

    def submit_task(self, task_type: str, task_data: Dict,
                    priority: int = 2, timeout: int = 300) -> str:
        task_id = str(uuid.uuid4())
        task = Task(
            task_id=task_id,
            task_type=task_type,
            task_data=task_data,
            priority=priority,
            timeout=timeout
        )
        with self._lock:
            self.tasks[task_id] = task

        self.timeout_manager.start_timer(task_id, timeout)

        # Find target worker and send ASSIGN directly (no broadcast to avoid race conditions)
        available = [
            w for w in self.workers.values()
            if w.status in ("idle", "online") and
               (w.capabilities is None or task_type in w.capabilities)
        ]
        target_worker = None
        if available:
            target_worker = min(available, key=lambda w: w.completed_tasks)

        if target_worker:
            task.status = "running"
            task.assigned_worker = target_worker.worker_id
            target_worker.status = "busy"
            target_worker.current_task_id = task_id
            target = target_worker.worker_id
        else:
            # No worker available, keep as pending for scheduler
            target = "*"

        msg = Message(
            type=MessageType.TASK.value,
            action="ASSIGN",  # Workers only process ASSIGN
            payload={
                "task_id": task_id,
                "task_type": task_type,
                "task_data": task_data,
                "timeout": timeout
            },
            target=target,
            correlation_id=task_id
        )
        self.mq.enqueue(QueueType.ORCHESTRATOR_TO_WORKER.value, msg)
        return task_id

    def _message_processor(self):
        while self._running:
            msg = self.mq.dequeue(QueueType.WORKER_TO_ORCHESTRATOR.value, timeout=1.0)
            if msg:
                self._handle_message(msg)

    def _handle_message(self, msg: Message):
        handlers = {
            MessageType.WORKER_REGISTER.value: self._handle_worker_register,
            MessageType.WORKER_UNREGISTER.value: self._handle_worker_unregister,
            MessageType.TASK_RESULT.value: self._handle_task_result,
            MessageType.HEARTBEAT.value: self._handle_heartbeat,
            MessageType.ERROR.value: self._handle_error,
        }
        handler = handlers.get(msg.type)
        if handler:
            handler(msg)

    def _handle_worker_register(self, msg: Message):
        payload = msg.payload
        worker_info = WorkerInfo(
            worker_id=payload["worker_id"],
            worker_type=payload.get("worker_type", "general"),
            capabilities=payload.get("capabilities", []),
            max_concurrent=payload.get("max_concurrent", 1),
            status="online"
        )
        with self._lock:
            self.workers[worker_info.worker_id] = worker_info

        ack = Message(
            type=MessageType.HEARTBEAT_ACK.value,
            payload={"status": "registered"},
            target=msg.source
        )
        self.mq.enqueue(QueueType.WORKER_HEARTBEAT.value, ack)

    def _handle_worker_unregister(self, msg: Message):
        worker_id = msg.payload.get("worker_id")
        with self._lock:
            if worker_id in self.workers:
                self.workers[worker_id].status = "offline"

    def _handle_task_result(self, msg: Message):
        task_id = msg.payload.get("task_id")
        status = msg.payload.get("status")
        self.timeout_manager.cancel_timer(task_id)

        with self._lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                # Only update if task is still in running state
                # (could have been processed by another worker already)
                if task.status == "running":
                    task.status = status
                    task.completed_at = time.time()

                    if status == "completed":
                        task.result = msg.payload.get("result")
                        if task.assigned_worker and task.assigned_worker in self.workers:
                            worker = self.workers[task.assigned_worker]
                            worker.status = "idle"
                            worker.current_task_id = None
                        worker.completed_tasks += 1
                else:
                    task.error = msg.payload.get("error")
                    if task.assigned_worker and task.assigned_worker in self.workers:
                        worker = self.workers[task.assigned_worker]
                        worker.failed_tasks += 1

    def _handle_heartbeat(self, msg: Message):
        worker_id = msg.payload.get("worker_id")
        with self._lock:
            if worker_id in self.workers:
                self.workers[worker_id].last_heartbeat = time.time()

    def _handle_error(self, msg: Message):
        task_id = msg.payload.get("task_id")
        error = msg.payload.get("error")

        with self._lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.error = error
                task.retry_count += 1

                if task.retry_count < self.max_task_retries:
                    self._reschedule_task(task)
                else:
                    task.status = "failed"

    def _heartbeat_monitor(self):
        while self._running:
            now = time.time()
            with self._lock:
                for worker_id, worker in list(self.workers.items()):
                    if worker.status != "offline":
                        if now - worker.last_heartbeat > self.heartbeat_timeout:
                            worker.status = "offline"
                            self._reassign_worker_tasks(worker_id)
            time.sleep(self.heartbeat_interval)

    def _reassign_worker_tasks(self, worker_id: str):
        with self._lock:
            for task in self.tasks.values():
                if task.assigned_worker == worker_id and task.status == "running":
                    task.status = "pending"
                    task.assigned_worker = None
                    self._reschedule_task(task)

    def _task_scheduler(self):
        while self._running:
            self._schedule_pending_tasks()
            time.sleep(0.1)

    def _schedule_pending_tasks(self):
        with self._lock:
            for task in self.tasks.values():
                # Skip tasks that are pending but already have an assigned worker
                # (those were assigned directly via submit_task)
                if task.status == "pending" and not task.assigned_worker:
                    worker = self._select_worker(task)
                    if worker:
                        self._assign_task(task, worker)

    def _select_worker(self, task: Task) -> Optional[WorkerInfo]:
        available = [
            w for w in self.workers.values()
            if w.status in ("idle", "online") and
               (w.capabilities is None or task.task_type in w.capabilities)
        ]
        if not available:
            return None
        return min(available, key=lambda w: w.completed_tasks)

    def _assign_task(self, task: Task, worker: WorkerInfo):
        task.status = "running"
        task.assigned_worker = worker.worker_id
        task.started_at = time.time()
        worker.status = "busy"
        worker.current_task_id = task.task_id

        msg = Message(
            type=MessageType.TASK.value,
            action="ASSIGN",
            payload={
                "task_id": task.task_id,
                "task_type": task.task_type,
                "task_data": task.task_data,
                "timeout": task.timeout
            },
            target=worker.worker_id
        )
        self.mq.enqueue(QueueType.ORCHESTRATOR_TO_WORKER.value, msg)

    def _reschedule_task(self, task: Task):
        task.status = "pending"
        task.assigned_worker = None
        task.started_at = None

        # Find a worker that can handle this task type
        available_workers = [
            w for w in self.workers.values()
            if w.status in ("idle", "online") and
               (w.capabilities is None or task.task_type in w.capabilities)
        ]

        target_worker = "*"
        if available_workers:
            # Assign to the worker with least workload
            target_worker = min(available_workers, key=lambda w: w.completed_tasks).worker_id

        msg = Message(
            type=MessageType.TASK.value,
            action="RETRY",
            payload={
                "task_id": task.task_id,
                "task_type": task.task_type,
                "task_data": task.task_data,
                "retry_count": task.retry_count
            },
            target=target_worker
        )
        self.mq.enqueue(QueueType.ORCHESTRATOR_TO_WORKER.value, msg)

    def _timeout_checker(self):
        while self._running:
            timed_out = self.timeout_manager.check_timeouts()
            for task in timed_out:
                self._handle_task_timeout(task.task_id)
            time.sleep(1)

    def _handle_task_timeout(self, task_id: str):
        self.timeout_manager.trigger_timeout(task_id)
        with self._lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.status = "failed"
                task.error = "Task timeout"
                if task.assigned_worker:
                    self._reassign_worker_tasks(task.assigned_worker)

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        with self._lock:
            if task_id in self.tasks:
                t = self.tasks[task_id]
                return {
                    "task_id": t.task_id,
                    "status": t.status,
                    "result": t.result,
                    "error": t.error,
                    "assigned_worker": t.assigned_worker
                }
        return None

    def get_workers_status(self) -> List[Dict]:
        with self._lock:
            return [
                {
                    "worker_id": w.worker_id,
                    "status": w.status,
                    "current_task": w.current_task_id,
                    "completed": w.completed_tasks,
                    "failed": w.failed_tasks
                }
                for w in self.workers.values()
            ]