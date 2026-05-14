"""Timeout management for tasks."""

import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Callable, List


@dataclass
class TimeoutTask:
    task_id: str
    start_time: float
    timeout: int
    callback: Optional[Callable]
    cancelled: bool = False


class TimeoutManager:
    def __init__(self, default_timeout: int = 300):
        self.default_timeout = default_timeout
        self._tasks: Dict[str, TimeoutTask] = {}
        self._lock = threading.Lock()

    def start_timer(self, task_id: str, timeout: Optional[int] = None,
                    callback: Optional[Callable] = None):
        with self._lock:
            self._tasks[task_id] = TimeoutTask(
                task_id=task_id,
                start_time=time.time(),
                timeout=timeout or self.default_timeout,
                callback=callback
            )

    def cancel_timer(self, task_id: str) -> bool:
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].cancelled = True
                del self._tasks[task_id]
                return True
            return False

    def is_timeout(self, task_id: str) -> Optional[bool]:
        with self._lock:
            if task_id not in self._tasks:
                return None
            task = self._tasks[task_id]
            if task.cancelled:
                return False
            elapsed = time.time() - task.start_time
            return elapsed > task.timeout

    def check_timeouts(self) -> List[TimeoutTask]:
        now = time.time()
        timed_out = []
        with self._lock:
            for task_id, task in list(self._tasks.items()):
                if task.cancelled:
                    continue
                if now - task.start_time > task.timeout:
                    timed_out.append(task)
        return timed_out

    def trigger_timeout(self, task_id: str):
        with self._lock:
            if task_id in self._tasks:
                task = self._tasks[task_id]
                del self._tasks[task_id]
                if task.callback:
                    try:
                        task.callback(task_id)
                    except Exception:
                        pass