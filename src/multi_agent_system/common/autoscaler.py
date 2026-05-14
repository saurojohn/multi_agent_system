"""Worker autoscaler for dynamic scaling based on queue depth."""

import logging
import threading
import time
from typing import Dict, List, Optional, Callable

logger = logging.getLogger('autoscaler')


class WorkerScaler:
    """Automatically scales workers based on queue depth."""

    def __init__(self,
                 min_workers: int = 1,
                 max_workers: int = 10,
                 scale_up_threshold: float = 5.0,
                 scale_down_threshold: float = 1.0,
                 scale_interval: float = 10.0,
                 cooldown_period: float = 60.0):
        """
        Args:
            min_workers: Minimum number of workers
            max_workers: Maximum number of workers
            scale_up_threshold: Queue depth to trigger scale up
            scale_down_threshold: Queue depth to trigger scale down
            scale_interval: Seconds between scaling checks
            cooldown_period: Seconds to wait between scaling operations
        """
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.scale_interval = scale_interval
        self.cooldown_period = cooldown_period

        self._current_workers = 0
        self._target_workers = 0
        self._last_scale_time = 0
        self._lock = threading.Lock()
        self._running = False
        self._scaling_thread = None

        # Callbacks for worker management
        self._scale_callback: Optional[Callable] = None
        self._get_queue_depth_callback: Optional[Callable] = None

    def set_scale_callback(self, callback: Callable[[int], None]):
        """Set callback to actually scale workers. Receives target count."""
        self._scale_callback = callback

    def set_queue_depth_callback(self, callback: Callable[[], int]):
        """Set callback to get current queue depth."""
        self._get_queue_depth_callback = callback

    def start(self):
        """Start the autoscaling loop."""
        self._running = True
        self._scaling_thread = threading.Thread(target=self._scaling_loop, daemon=True)
        self._scaling_thread.start()
        logger.info(f'Worker autoscaler started: min={self.min_workers}, max={self.max_workers}')

    def stop(self):
        """Stop the autoscaling loop."""
        self._running = False
        if self._scaling_thread:
            self._scaling_thread.join(timeout=5)
        logger.info('Worker autoscaler stopped')

    def _scaling_loop(self):
        """Main scaling loop."""
        while self._running:
            try:
                self._check_and_scale()
            except Exception as e:
                logger.error(f'Scaling loop error: {e}')
            time.sleep(self.scale_interval)

    def _check_and_scale(self):
        """Check queue depth and scale workers if needed."""
        if not self._get_queue_depth_callback:
            logger.warning('No queue depth callback set')
            return

        queue_depth = self._get_queue_depth_callback()
        now = time.time()

        with self._lock:
            # Scale up if queue is deep and cooldown passed
            if queue_depth >= self.scale_up_threshold:
                if self._current_workers < self.max_workers:
                    if now - self._last_scale_time >= self.cooldown_period:
                        new_workers = min(self._current_workers + 1, self.max_workers)
                        self._do_scale(new_workers)
                        return

            # Scale down if queue is empty and cooldown passed
            if queue_depth <= self.scale_down_threshold:
                if self._current_workers > self.min_workers:
                    if now - self._last_scale_time >= self.cooldown_period:
                        new_workers = max(self._current_workers - 1, self.min_workers)
                        self._do_scale(new_workers)

    def _do_scale(self, target: int):
        """Execute scaling to target count."""
        logger.info(f'Scaling workers from {self._current_workers} to {target}')
        self._current_workers = target
        self._target_workers = target
        self._last_scale_time = time.time()

        if self._scale_callback:
            try:
                self._scale_callback(target)
            except Exception as e:
                logger.error(f'Scale callback failed: {e}')

    def get_status(self) -> Dict:
        """Get autoscaler status."""
        return {
            'current_workers': self._current_workers,
            'target_workers': self._target_workers,
            'min_workers': self.min_workers,
            'max_workers': self.max_workers,
            'running': self._running
        }

    def force_scale(self, target: int):
        """Force scaling to specific worker count."""
        with self._lock:
            target = max(self.min_workers, min(target, self.max_workers))
            self._do_scale(target)


class TaskQueueAutoscaler:
    """Autoscaler for task queues with multiple worker types."""

    def __init__(self):
        self._scalers: Dict[str, WorkerScaler] = {}
        self._lock = threading.Lock()

    def create_scaler(self, worker_type: str, **kwargs) -> WorkerScaler:
        """Create a scaler for specific worker type."""
        with self._lock:
            if worker_type not in self._scalers:
                self._scalers[worker_type] = WorkerScaler(**kwargs)
            return self._scalers[worker_type]

    def get_scaler(self, worker_type: str) -> Optional[WorkerScaler]:
        return self._scalers.get(worker_type)

    def get_all_status(self) -> Dict:
        """Get status of all scalers."""
        with self._lock:
            return {
                worker_type: scaler.get_status()
                for worker_type, scaler in self._scalers.items()
            }


# Global autoscaler instance
_autoscaler = TaskQueueAutoscaler()


def get_autoscaler() -> TaskQueueAutoscaler:
    return _autoscaler