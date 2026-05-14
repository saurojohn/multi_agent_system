"""Orchestrator - Master Agent that coordinates workers."""

import threading
import time
import uuid
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from ..common.message import Message, MessageType, QueueType, MessagePriority
from ..common.queue import MessageQueueManager
from ..common.timeout import TimeoutManager
from ..common.metrics import get_metrics

logger = logging.getLogger('orchestrator')


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
                 max_task_retries: int = 3,
                 event_callback=None,
                 dead_letter_callback=None):
        self.mq = mq
        self.state = OrchestratorState.IDLE
        self.tasks: Dict[str, Task] = {}
        self.workers: Dict[str, WorkerInfo] = {}
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.max_task_retries = max_task_retries
        self.timeout_manager = TimeoutManager()
        self.event_callback = event_callback
        self.dead_letter_callback = dead_letter_callback  # Callback for DLQ handling

        self._running = False
        self._threads: List[threading.Thread] = []
        self._lock = threading.RLock()

    def _emit_event(self, event_type: str, data: dict):
        if self.event_callback:
            try:
                self.event_callback(event_type, data)
            except Exception as e:
                logger.warning(f'Event callback failed: {e}')

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
        """Graceful stop - wait for pending tasks to complete."""
        self._running = False

        # Wait for running tasks with timeout
        pending_timeout = 30  # seconds to wait for pending tasks
        start_time = time.time()

        with self._lock:
            running_tasks = [t for t in self.tasks.values() if t.status == 'running']
            pending_tasks = [t for t in self.tasks.values() if t.status == 'pending']

        if running_tasks or pending_tasks:
            logger.info(f'Graceful shutdown: waiting for {len(running_tasks)} running, {len(pending_tasks)} pending tasks')
            while time.time() - start_time < pending_timeout:
                with self._lock:
                    still_running = sum(1 for t in self.tasks.values() if t.status == 'running')
                if still_running == 0:
                    break
                time.sleep(0.5)

        for t in self._threads:
            t.join(timeout=5)

        logger.info('Orchestrator stopped')

    def submit_task(self, task_type: str, task_data: Dict,
                    priority: int = 2, timeout: int = 300,
                    dependencies: List[str] = None) -> str:
        task_id = str(uuid.uuid4())
        task = Task(
            task_id=task_id,
            task_type=task_type,
            task_data=task_data,
            priority=priority,
            timeout=timeout,
            dependencies=dependencies or []
        )
        with self._lock:
            self.tasks[task_id] = task

        self.timeout_manager.start_timer(task_id, timeout)
        get_metrics().record_task_submitted()

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
            # Check dependencies before starting
            if task.dependencies and not self._all_dependencies_met(task.dependencies):
                # Dependencies not met, keep as pending
                task.status = "pending"
                target = "*"
                target_worker = None
            else:
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
        logger.info(f'Task submitted: {task_id} type={task_type} target={target}')
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
        worker_id = payload["worker_id"]
        capabilities = payload.get("capabilities", [])
        logger.info(f'Worker registered: {worker_id} with capabilities {capabilities}')

        worker_info = WorkerInfo(
            worker_id=worker_id,
            worker_type=payload.get("worker_type", "general"),
            capabilities=payload.get("capabilities", []),
            max_concurrent=payload.get("max_concurrent", 1),
            status="online"
        )
        with self._lock:
            self.workers[worker_info.worker_id] = worker_info

        self._emit_event("worker_registered", {
            "worker_id": worker_id,
            "worker_type": payload.get("worker_type", "general"),
            "capabilities": capabilities
        })

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
                logger.info(f'Worker unregistered: {worker_id}')

    def _handle_task_result(self, msg: Message):
        task_id = msg.payload.get("task_id")
        status = msg.payload.get("status")
        worker_id = msg.payload.get("worker_id")
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
                        logger.info(f'Task completed: {task_id} by {worker_id}')
                        if task.assigned_worker and task.assigned_worker in self.workers:
                            worker = self.workers[task.assigned_worker]
                            worker.status = "idle"
                            worker.current_task_id = None
                        worker.completed_tasks += 1
                        # Trigger dependent tasks
                        self._trigger_dependent_tasks(task_id, task.result)
                        self._emit_event("task_update", {
                            "task_id": task_id,
                            "status": "completed",
                            "result": task.result,
                            "task_type": task.task_type
                        })
                        # Record metrics
                        if task.started_at:
                            latency = task.completed_at - task.started_at
                            get_metrics().record_task_completed(latency)
                    else:
                        logger.warning(f'Task failed: {task_id} error={msg.payload.get("error")}')
                        self._emit_event("task_update", {
                            "task_id": task_id,
                            "status": "failed",
                            "error": msg.payload.get("error"),
                            "task_type": task.task_type
                        })
                        get_metrics().record_task_failed()
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
                logger.debug(f'Heartbeat received from {worker_id}')

    def _handle_error(self, msg: Message):
        task_id = msg.payload.get("task_id")
        error = msg.payload.get("error")

        with self._lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.error = error
                task.retry_count += 1

                if task.retry_count < self.max_task_retries:
                    logger.info(f'Rescheduling task {task_id} (retry {task.retry_count}/{self.max_task_retries})')
                    self._reschedule_task(task)
                else:
                    task.status = "failed"
                    logger.error(f'Task {task_id} failed permanently after {task.retry_count} retries')
                    # Send to dead letter queue
                    self._send_to_dead_letter(task)

    def _heartbeat_monitor(self):
        while self._running:
            now = time.time()
            with self._lock:
                for worker_id, worker in list(self.workers.items()):
                    if worker.status != "offline":
                        if now - worker.last_heartbeat > self.heartbeat_timeout:
                            worker.status = "offline"
                            logger.warning(f'Worker timed out: {worker_id}')
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
                        logger.info(f'Scheduled task {task.task_id} to worker {worker.worker_id}')

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
        logger.info(f'Task {task.task_id} assigned to {worker.worker_id}')

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
        logger.info(f'Task {task.task_id} rescheduled to {target_worker} (retry {task.retry_count})')

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
                logger.warning(f'Task timeout: {task_id}')
                if task.assigned_worker:
                    self._reassign_worker_tasks(task.assigned_worker)
                # Send to dead letter queue
                self._send_to_dead_letter(task)

    def _all_dependencies_met(self, dependencies: List[str]) -> bool:
        """Check if all dependency tasks are completed."""
        for dep_id in dependencies:
            if dep_id not in self.tasks:
                return False
            if self.tasks[dep_id].status != "completed":
                return False
        return True

    def _trigger_dependent_tasks(self, completed_task_id: str, result: Dict):
        """Trigger tasks that depend on the completed task."""
        for task in self.tasks.values():
            if completed_task_id in task.dependencies:
                # Re-check if all dependencies are now met
                if self._all_dependencies_met(task.dependencies):
                    # Schedule the dependent task
                    logger.info(f'All dependencies met for task {task.task_id}, scheduling now')
                    worker = self._select_worker(task)
                    if worker:
                        self._assign_task(task, worker)

    def chain_tasks(self, task_chain: List[Dict]) -> List[str]:
        """
        Create a chain of dependent tasks.
        task_chain: List of dicts with 'task_type', 'task_data', 'timeout' (optional)
        Returns list of task_ids in order.
        """
        task_ids = []
        prev_task_id = None

        for i, task_spec in enumerate(task_chain):
            deps = [prev_task_id] if prev_task_id else []
            task_id = self.submit_task(
                task_type=task_spec['task_type'],
                task_data=task_spec['task_data'],
                timeout=task_spec.get('timeout', 300),
                dependencies=deps
            )
            task_ids.append(task_id)
            prev_task_id = task_id

        logger.info(f'Created task chain with {len(task_ids)} tasks')
        return task_ids

    def _send_to_dead_letter(self, task: Task):
        """Send failed task to dead letter queue."""
        dlq_message = Message(
            type="DEAD_LETTER",
            action="DLQ",
            payload={
                "task_id": task.task_id,
                "task_type": task.task_type,
                "task_data": task.task_data,
                "error": task.error,
                "retry_count": task.retry_count,
                "failed_at": time.time()
            },
            target=QueueType.DEAD_LETTER.value
        )
        self.mq.enqueue(QueueType.DEAD_LETTER.value, dlq_message)
        logger.warning(f'Task {task.task_id} sent to dead letter queue')

        if self.dead_letter_callback:
            try:
                self.dead_letter_callback(task)
            except Exception as e:
                logger.error(f'DLQ callback failed: {e}')

    def get_dead_letter_tasks(self) -> List[Dict]:
        """Get all tasks in dead letter queue."""
        dlq_tasks = []
        while True:
            msg = self.mq.dequeue(QueueType.DEAD_LETTER.value, timeout=0.1)
            if not msg:
                break
            dlq_tasks.append(msg.payload)
        return dlq_tasks

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