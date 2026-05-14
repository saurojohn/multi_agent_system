"""Timeout management for tasks with custom behaviors."""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, List
from enum import Enum

logger = logging.getLogger('timeout')


class TimeoutAction(Enum):
    """What to do when task times out."""
    FAIL = "fail"           # Mark as failed
    RETRY = "retry"        # Retry the task
    EXTEND = "extend"       # Extend timeout
    IGNORE = "ignore"       # Do nothing


@dataclass
class TimeoutTask:
    task_id: str
    start_time: float
    timeout: int
    callback: Optional[Callable] = None
    cancelled: bool = False
    action: TimeoutAction = TimeoutAction.FAIL
    retry_count: int = 0
    max_retries: int = 3
    extensions: int = 0
    max_extensions: int = 2
    extension_time: int = 60  # seconds to extend


class TimeoutPolicy:
    """Defines timeout behavior for a task type."""

    def __init__(self,
                 timeout: int = 300,
                 action: TimeoutAction = TimeoutAction.FAIL,
                 max_retries: int = 3,
                 max_extensions: int = 2,
                 extension_time: int = 60):
        self.timeout = timeout
        self.action = action
        self.max_retries = max_retries
        self.max_extensions = max_extensions
        self.extension_time = extension_time


class TimeoutManager:
    def __init__(self, default_timeout: int = 300):
        self.default_timeout = default_timeout
        self._tasks: Dict[str, TimeoutTask] = {}
        self._policies: Dict[str, TimeoutPolicy] = {}
        self._lock = threading.Lock()

    def set_policy(self, task_type: str, policy: TimeoutPolicy):
        """Set timeout policy for a task type."""
        self._policies[task_type] = policy

    def get_policy(self, task_type: str) -> TimeoutPolicy:
        """Get timeout policy for task type."""
        return self._policies.get(task_type, TimeoutPolicy())

    def start_timer(self, task_id: str, timeout: Optional[int] = None,
                    callback: Optional[Callable] = None,
                    task_type: str = None,
                    action: TimeoutAction = TimeoutAction.FAIL,
                    max_retries: int = 3):
        """Start a timeout timer for a task."""
        # Get policy if task_type specified
        if task_type and task_type in self._policies:
            policy = self._policies[task_type]
            timeout = timeout or policy.timeout
            action = action or policy.action
            max_retries = max_retries or policy.max_retries

        with self._lock:
            self._tasks[task_id] = TimeoutTask(
                task_id=task_id,
                start_time=time.time(),
                timeout=timeout or self.default_timeout,
                callback=callback,
                action=action,
                max_retries=max_retries
            )

    def cancel_timer(self, task_id: str) -> bool:
        """Cancel a timeout timer."""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].cancelled = True
                del self._tasks[task_id]
                return True
            return False

    def is_timeout(self, task_id: str) -> Optional[bool]:
        """Check if task has timed out."""
        with self._lock:
            if task_id not in self._tasks:
                return None
            task = self._tasks[task_id]
            if task.cancelled:
                return False
            elapsed = time.time() - task.start_time
            return elapsed > task.timeout

    def check_timeouts(self) -> List[TimeoutTask]:
        """Check for timed out tasks."""
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
        """Trigger timeout handling for a task."""
        with self._lock:
            if task_id in self._tasks:
                task = self._tasks[task_id]
                del self._tasks[task_id]
                if task.callback:
                    try:
                        task.callback(task_id, task.action)
                    except Exception as e:
                        logger.error(f'Timeout callback failed: {e}')

    def extend_timeout(self, task_id: str, extra_time: int = None) -> bool:
        """Extend the timeout for a task."""
        with self._lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            if task.extensions >= task.max_extensions:
                return False

            extra_time = extra_time or task.extension_time
            task.timeout += extra_time
            task.extensions += 1
            logger.debug(f'Extended timeout for {task_id} by {extra_time}s ({task.extensions}/{task.max_extensions})')
            return True

    def get_timeout_status(self, task_id: str) -> Optional[Dict]:
        """Get timeout status for a task."""
        with self._lock:
            if task_id not in self._tasks:
                return None
            task = self._tasks[task_id]
            elapsed = time.time() - task.start_time
            remaining = max(0, task.timeout - elapsed)
            return {
                'task_id': task_id,
                'elapsed': elapsed,
                'remaining': remaining,
                'timeout': task.timeout,
                'extensions_used': task.extensions,
                'action': task.action.value
            }

    def get_stats(self) -> Dict:
        """Get timeout manager statistics."""
        with self._lock:
            return {
                'active_timers': len(self._tasks),
                'policies_configured': len(self._policies)
            }


# Pre-built timeout policies
POLICIES = {
    'fast': TimeoutPolicy(timeout=30, action=TimeoutAction.FAIL, max_retries=2),
    'standard': TimeoutPolicy(timeout=300, action=TimeoutAction.FAIL, max_retries=3),
    'slow': TimeoutPolicy(timeout=600, action=TimeoutAction.RETRY, max_retries=5),
    'critical': TimeoutPolicy(timeout=60, action=TimeoutAction.EXTEND, max_extensions=3, extension_time=30),
}