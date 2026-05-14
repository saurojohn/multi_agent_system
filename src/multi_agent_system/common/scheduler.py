"""Scheduled task execution for cron-like jobs."""

import time
import threading
import logging
import uuid
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger('scheduler')


class ScheduleType(Enum):
    INTERVAL = "interval"    # Run every N seconds
    CRON = "cron"           # Cron expression (minute hour day month weekday)
    ONCE = "once"           # Run once at specific time


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    task_id: str
    name: str
    task_type: str
    task_data: Dict
    schedule_type: ScheduleType
    schedule_value: str  # e.g., "60" for 60 seconds, or "30 * * * *" for cron
    next_run: float
    enabled: bool = True
    last_run: Optional[float] = None
    run_count: int = 0
    callback: Optional[Callable] = None


class TaskScheduler:
    """Schedules tasks for periodic or cron-based execution."""

    def __init__(self):
        self._scheduled_tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._submit_callback: Optional[Callable] = None

    def set_submit_callback(self, callback: Callable):
        """Set callback to submit tasks to orchestrator."""
        self._submit_callback = callback

    def schedule_interval(self, name: str, task_type: str, task_data: Dict,
                          interval_seconds: int, start_now: bool = False) -> str:
        """Schedule a task to run every N seconds."""
        task_id = f"sched-{uuid.uuid4().hex[:8]}"
        next_run = time.time() if start_now else time.time() + interval_seconds

        scheduled = ScheduledTask(
            task_id=task_id,
            name=name,
            task_type=task_type,
            task_data=task_data,
            schedule_type=ScheduleType.INTERVAL,
            schedule_value=str(interval_seconds),
            next_run=next_run
        )

        with self._lock:
            self._scheduled_tasks[task_id] = scheduled

        logger.info(f'Scheduled interval task: {name} every {interval_seconds}s (id: {task_id})')
        return task_id

    def schedule_cron(self, name: str, task_type: str, task_data: Dict,
                      cron_expression: str) -> str:
        """Schedule a task using cron expression."""
        task_id = f"sched-{uuid.uuid4().hex[:8]}"
        next_run = self._calculate_next_cron_run(cron_expression)

        scheduled = ScheduledTask(
            task_id=task_id,
            name=name,
            task_type=task_type,
            task_data=task_data,
            schedule_type=ScheduleType.CRON,
            schedule_value=cron_expression,
            next_run=next_run
        )

        with self._lock:
            self._scheduled_tasks[task_id] = scheduled

        logger.info(f'Scheduled cron task: {name} ({cron_expression}) (id: {task_id})')
        return task_id

    def schedule_once(self, name: str, task_type: str, task_data: Dict,
                     run_at_timestamp: float) -> str:
        """Schedule a one-time task at specific timestamp."""
        task_id = f"sched-{uuid.uuid4().hex[:8]}"

        scheduled = ScheduledTask(
            task_id=task_id,
            name=name,
            task_type=task_type,
            task_data=task_data,
            schedule_type=ScheduleType.ONCE,
            schedule_value=str(run_at_timestamp),
            next_run=run_at_timestamp
        )

        with self._lock:
            self._scheduled_tasks[task_id] = scheduled

        logger.info(f'Scheduled one-time task: {name} at {time.ctime(run_at_timestamp)} (id: {task_id})')
        return task_id

    def unschedule(self, task_id: str) -> bool:
        """Remove a scheduled task."""
        with self._lock:
            if task_id in self._scheduled_tasks:
                del self._scheduled_tasks[task_id]
                logger.info(f'Unscheduled task: {task_id}')
                return True
        return False

    def pause(self, task_id: str) -> bool:
        """Pause a scheduled task."""
        with self._lock:
            if task_id in self._scheduled_tasks:
                self._scheduled_tasks[task_id].enabled = False
                logger.info(f'Paused scheduled task: {task_id}')
                return True
        return False

    def resume(self, task_id: str) -> bool:
        """Resume a paused scheduled task."""
        with self._lock:
            if task_id in self._scheduled_tasks:
                self._scheduled_tasks[task_id].enabled = True
                logger.info(f'Resumed scheduled task: {task_id}')
                return True
        return False

    def start(self):
        """Start the scheduler."""
        if self._running:
            return
        self._running = True
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        logger.info('Task scheduler started')

    def stop(self):
        """Stop the scheduler."""
        self._running = False
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        logger.info('Task scheduler stopped')

    def _scheduler_loop(self):
        """Main scheduler loop."""
        while self._running:
            now = time.time()

            with self._lock:
                tasks_to_run = [
                    task for task in self._scheduled_tasks.values()
                    if task.enabled and now >= task.next_run
                ]

            for task in tasks_to_run:
                self._execute_scheduled_task(task)

            time.sleep(1)

    def _execute_scheduled_task(self, task: ScheduledTask):
        """Execute a scheduled task."""
        logger.info(f'Executing scheduled task: {task.name}')

        # Update next run time
        if task.schedule_type == ScheduleType.INTERVAL:
            interval = int(task.schedule_value)
            task.next_run = time.time() + interval
        elif task.schedule_type == ScheduleType.CRON:
            task.next_run = self._calculate_next_cron_run(task.schedule_value)
        elif task.schedule_type == ScheduleType.ONCE:
            task.enabled = False

        task.last_run = time.time()
        task.run_count += 1

        # Submit to orchestrator
        if self._submit_callback:
            try:
                self._submit_callback(task.task_type, task.task_data)
            except Exception as e:
                logger.error(f'Scheduled task submission failed: {e}')

        # Call callback if set
        if task.callback:
            try:
                task.callback(task)
            except Exception as e:
                logger.error(f'Scheduled task callback failed: {e}')

    def _calculate_next_cron_run(self, cron_expr: str) -> float:
        """
        Calculate next run time from cron expression.
        Simplified cron: "minute hour day month weekday"
        Examples: "30 * * * *" = every hour at :30
                  "0 9 * * *" = every day at 9:00 AM
        """
        parts = cron_expr.split()
        if len(parts) != 5:
            logger.warning(f'Invalid cron expression: {cron_expr}, using 60s interval')
            return time.time() + 60

        minute, hour, day, month, weekday = parts

        now = time.localtime()
        year = now.tm_year

        # Simple implementation: if minute is *, run every minute
        if minute == '*':
            next_minute = now.tm_min + 1
            next_hour = now.tm_hour
            if next_minute >= 60:
                next_minute = 0
                next_hour += 1
            return time.mktime((year, now.tm_mon, now.tm_mday, next_hour, next_minute, 0, 0, 0, 0))

        # If specific minute
        try:
            target_minute = int(minute)
            target_hour = int(hour) if hour != '*' else now.tm_hour
            # Default to today
            next_run = time.mktime((year, now.tm_mon, now.tm_mday, target_hour, target_minute, 0, 0, 0, 0))
            # If past, schedule for tomorrow
            if next_run <= now.time():
                next_run += 86400  # Add one day
            return next_run
        except ValueError:
            return time.time() + 60

    def get_scheduled_tasks(self) -> List[Dict]:
        """Get list of all scheduled tasks."""
        with self._lock:
            return [
                {
                    'task_id': t.task_id,
                    'name': t.name,
                    'task_type': t.task_type,
                    'schedule_type': t.schedule_type.value,
                    'schedule_value': t.schedule_value,
                    'next_run': t.next_run,
                    'enabled': t.enabled,
                    'last_run': t.last_run,
                    'run_count': t.run_count
                }
                for t in self._scheduled_tasks.values()
            ]

    def get_status(self) -> Dict:
        """Get scheduler status."""
        with self._lock:
            return {
                'running': self._running,
                'total_tasks': len(self._scheduled_tasks),
                'enabled_tasks': sum(1 for t in self._scheduled_tasks.values() if t.enabled),
                'tasks': self.get_scheduled_tasks()
            }


# Global scheduler instance
_scheduler = TaskScheduler()


def get_scheduler() -> TaskScheduler:
    return _scheduler