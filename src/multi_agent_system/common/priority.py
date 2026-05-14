"""Dynamic task priority adjustment based on wait time."""

import logging
import time
import threading
from typing import Dict, List, Optional
from collections import defaultdict

logger = logging.getLogger('priority')


class PriorityAdjuster:
    """
    Adjusts task priorities dynamically based on:
    - Time waiting in queue
    - Task age
    - Priority inversion prevention
    """

    def __init__(self,
                 base_priority: int = 2,
                 max_priority: int = 1,  # Lower number = higher priority
                 boost_interval: float = 30.0,  # Seconds between priority boosts
                 boost_amount: int = 1):  # How much to boost priority
        self.base_priority = base_priority
        self.max_priority = max_priority
        self.boost_interval = boost_interval
        self.boost_amount = boost_amount

        self._pending_tasks: Dict[str, float] = {}  # task_id -> queued_at
        self._priority_boosts: Dict[str, int] = {}  # task_id -> boost_count
        self._lock = threading.RLock()
        self._running = False
        self._adjustment_thread = None

    def track_task(self, task_id: str):
        """Start tracking a task for priority adjustment."""
        with self._lock:
            self._pending_tasks[task_id] = time.time()
            self._priority_boosts[task_id] = 0
        logger.debug(f'Tracking task for priority adjustment: {task_id}')

    def untrack_task(self, task_id: str):
        """Stop tracking a task (completed or failed)."""
        with self._lock:
            self._pending_tasks.pop(task_id, None)
            self._priority_boosts.pop(task_id, None)

    def get_adjusted_priority(self, task_id: str, original_priority: int) -> int:
        """Get priority with dynamic adjustments applied."""
        with self._lock:
            if task_id not in self._pending_tasks:
                return original_priority

            queued_at = self._pending_tasks.get(task_id, time.time())
            wait_time = time.time() - queued_at
            boost_count = self._priority_boosts.get(task_id, 0)

            # Calculate new priority (lower is higher)
            priority = original_priority - boost_count
            priority = max(priority, self.max_priority)  # Don't go below max

            return priority

    def start(self):
        """Start the priority adjustment background process."""
        if self._running:
            return
        self._running = True
        self._adjustment_thread = threading.Thread(target=self._adjustment_loop, daemon=True)
        self._adjustment_thread.start()
        logger.info('Priority adjuster started')

    def stop(self):
        """Stop the priority adjustment process."""
        self._running = False
        if self._adjustment_thread:
            self._adjustment_thread.join(timeout=5)
        logger.info('Priority adjuster stopped')

    def _adjustment_loop(self):
        """Background loop to boost priorities."""
        while self._running:
            now = time.time()

            with self._lock:
                for task_id, queued_at in list(self._pending_tasks.items()):
                    wait_time = now - queued_at

                    # Apply boost every boost_interval seconds
                    num_boosts = int(wait_time / self.boost_interval)
                    if num_boosts > self._priority_boosts.get(task_id, 0):
                        self._priority_boosts[task_id] = num_boosts
                        logger.debug(f'Boosted priority for task {task_id}: {num_boosts} boosts')

            time.sleep(self.boost_interval / 2)

    def get_queue_stats(self) -> Dict:
        """Get current queue statistics."""
        with self._lock:
            stats = {
                'tracked_tasks': len(self._pending_tasks),
                'total_boosts': sum(self._priority_boosts.values()),
                'oldest_task_age': 0
            }
            if self._pending_tasks:
                oldest = min(self._pending_tasks.values())
                stats['oldest_task_age'] = time.time() - oldest
            return stats


class AgingPriorityQueue:
    """
    Priority queue that considers task age.
    Uses a combination of priority and wait time to determine ordering.
    """

    def __init__(self, age_weight: float = 1.0):
        """
        Args:
            age_weight: How much to weight age vs priority (0-1)
        """
        self.age_weight = age_weight
        self._tasks: List[Dict] = []
        self._lock = threading.RLock()

    def add(self, task_id: str, priority: int, queued_at: float = None):
        """Add a task to the queue."""
        queued_at = queued_at or time.time()
        score = self._calculate_score(priority, queued_at)

        with self._lock:
            self._tasks.append({
                'task_id': task_id,
                'priority': priority,
                'queued_at': queued_at,
                'score': score
            })
            self._tasks.sort(key=lambda x: x['score'])

    def pop(self) -> Optional[str]:
        """Remove and return the highest priority task."""
        with self._lock:
            if self._tasks:
                return self._tasks.pop(0)['task_id']
        return None

    def peek(self) -> Optional[str]:
        """View the highest priority task without removing."""
        with self._lock:
            if self._tasks:
                return self._tasks[0]['task_id']
        return None

    def remove(self, task_id: str) -> bool:
        """Remove a specific task from the queue."""
        with self._lock:
            for i, task in enumerate(self._tasks):
                if task['task_id'] == task_id:
                    del self._tasks[i]
                    return True
        return False

    def _calculate_score(self, priority: int, queued_at: float) -> float:
        """Lower score = higher priority."""
        age = time.time() - queued_at

        # Score = priority + (age * age_weight)
        # Priority is 1-3, age is in seconds
        # At age_weight=0.1, a task waiting 10s gets priority boost of 1
        score = priority + (age * self.age_weight)
        return score

    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._tasks)

    def get_all(self) -> List[Dict]:
        """Get all tasks sorted by priority."""
        with self._lock:
            return sorted(self._tasks, key=lambda x: x['score'])


class PriorityInversionPreventor:
    """
    Prevents priority inversion by boosting waiting tasks.
    A high-priority task waiting on a low-priority result gets temporarily boosted.
    """

    def __init__(self, threshold_seconds: float = 60.0):
        self.threshold_seconds = threshold_seconds
        self._waiting_on: Dict[str, str] = {}  # task_id -> dependency_task_id

    def register_dependency(self, task_id: str, depends_on: str):
        """Register that task_id is waiting on depends_on."""
        self._waiting_on[task_id] = depends_on

    def clear_dependency(self, task_id: str):
        """Clear dependency when task completes."""
        self._waiting_on.pop(task_id, None)

    def should_boost(self, task_id: str) -> bool:
        """Check if task should be boosted due to priority inversion."""
        if task_id not in self._waiting_on:
            return False

        # Would need task creation time to properly implement
        # This is a simplified version
        return True

    def get_stats(self) -> Dict:
        """Get statistics about waiting tasks."""
        return {
            'waiting_tasks': len(self._waiting_on),
            'blocked_tasks': list(self._waiting_on.keys())
        }


# Global priority adjuster instance
_priority_adjuster = PriorityAdjuster()


def get_priority_adjuster() -> PriorityAdjuster:
    return _priority_adjuster