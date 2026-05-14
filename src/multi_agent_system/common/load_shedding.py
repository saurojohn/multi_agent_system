"""Load shedding mechanisms to protect system under high load."""

import logging
import threading
import time
import random
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('load_shedding')


class LoadSheddingStrategy(Enum):
    """Strategies for load shedding."""
    REJECT = "reject"           # Immediately reject requests
    QUEUE = "queue"             # Queue requests with timeout
    DEGRADE = "degrade"         # Serve degraded responses
    RANDOM_DROP = "random_drop" # Randomly drop requests
    PRIORITY = "priority"       # Drop lower priority requests


@dataclass
class LoadSheddingConfig:
    """Configuration for load shedding."""
    strategy: LoadSheddingStrategy = LoadSheddingStrategy.REJECT
    max_queue_size: int = 1000
    queue_timeout: float = 5.0  # Seconds to wait in queue
    degrade_threshold: float = 0.8  # 80% capacity triggers degradation
    shed_threshold: float = 0.95   # 95% capacity triggers load shedding
    cpu_threshold: float = 0.9     # 90% CPU triggers shedding
    memory_threshold: float = 0.9  # 90% memory triggers shedding


@dataclass
class LoadMetrics:
    """Current system load metrics."""
    cpu_usage: float
    memory_usage: float
    request_rate: float  # requests per second
    active_requests: int
    queue_size: int
    timestamp: float


@dataclass
class SheddingDecision:
    """Decision from load shedder."""
    should_shed: bool
    reason: str
    strategy_used: LoadSheddingStrategy
    wait_time: float = 0  # Estimated wait time if queued


class LoadShedder:
    """
    Load shedding to protect system under high load.
    """

    def __init__(self, config: LoadSheddingConfig = None):
        self.config = config or LoadSheddingConfig()
        self._enabled = True
        self._lock = threading.Lock()
        self._current_load: Optional[LoadMetrics] = None
        self._request_queue: List[tuple] = []  # (timestamp, priority, callback)
        self._queue_thread: threading.Thread = None
        self._running = False

    def start(self):
        """Start the load shedder."""
        self._running = True
        self._queue_thread = threading.Thread(target=self._process_queue, daemon=True)
        self._queue_thread.start()

    def stop(self):
        """Stop the load shedder."""
        self._running = False
        if self._queue_thread:
            self._queue_thread.join(timeout=5)

    def update_metrics(self, metrics: LoadMetrics):
        """Update current load metrics."""
        self._current_load = metrics

    def should_shed_request(self, priority: int = 5) -> SheddingDecision:
        """
        Determine if request should be shed.
        Priority: 1 (highest) to 10 (lowest).
        """
        if not self._enabled:
            return SheddingDecision(False, "disabled", self.config.strategy)

        if not self._current_load:
            return SheddingDecision(False, "no metrics", self.config.strategy)

        # Check CPU
        if self._current_load.cpu_usage > self.config.cpu_threshold:
            return SheddingDecision(
                True,
                f"CPU too high: {self._current_load.cpu_usage:.2%}",
                self.config.strategy
            )

        # Check memory
        if self._current_load.memory_usage > self.config.memory_threshold:
            return SheddingDecision(
                True,
                f"Memory too high: {self._current_load.memory_usage:.2%}",
                self.config.strategy
            )

        # Check queue size
        if self._current_load.queue_size >= self.config.max_queue_size:
            return SheddingDecision(
                True,
                f"Queue full: {self._current_load.queue_size}",
                self.config.strategy
            )

        # Check request rate against threshold
        active_ratio = self._current_load.active_requests / max(1, self.config.max_queue_size)

        if active_ratio >= self.config.shed_threshold:
            if self.config.strategy == LoadSheddingStrategy.REJECT:
                return SheddingDecision(True, "system overloaded", self.config.strategy)
            elif self.config.strategy == LoadSheddingStrategy.PRIORITY:
                if priority > 5:  # Lower priority
                    return SheddingDecision(True, "low priority request", self.config.strategy)
            elif self.config.strategy == LoadSheddingStrategy.RANDOM_DROP:
                if random.random() < 0.1:  # 10% chance
                    return SheddingDecision(True, "random drop", self.config.strategy)

        return SheddingDecision(False, "load acceptable", self.config.strategy)

    def queue_request(self, callback: Callable, priority: int = 5,
                      timeout: float = None) -> bool:
        """
        Queue a request for later processing.
        Returns True if queued, False if rejected.
        """
        timeout = timeout or self.config.queue_timeout

        decision = self.should_shed_request(priority)
        if decision.should_shed:
            if self.config.strategy == LoadSheddingStrategy.QUEUE:
                # Add to queue
                with self._lock:
                    self._request_queue.append((time.time(), priority, callback))
                    self._request_queue.sort(key=lambda x: x[1])  # Sort by priority
                return True
            return False

        # Execute immediately
        try:
            callback()
            return True
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return False

    def _process_queue(self):
        """Background queue processing."""
        while self._running:
            time.sleep(0.1)

            # Check if we should process from queue
            if not self._current_load:
                continue

            if self._current_load.active_requests < self.config.max_queue_size * 0.5:
                with self._lock:
                    if self._request_queue:
                        timestamp, priority, callback = self._request_queue.pop(0)

                        # Check timeout
                        if time.time() - timestamp > self.config.queue_timeout:
                            logger.warning("Request timed out in queue")
                            continue

                        try:
                            callback()
                        except Exception as e:
                            logger.error(f"Queued request failed: {e}")

    def get_degraded_response(self, request_type: str) -> Dict:
        """Get a degraded response for the request type."""
        return {
            'status': 'degraded',
            'message': 'System under high load',
            'request_type': request_type,
            'timestamp': time.time()
        }

    def enable(self):
        """Enable load shedding."""
        self._enabled = True

    def disable(self):
        """Disable load shedding."""
        self._enabled = False

    def get_stats(self) -> Dict:
        """Get load shedder statistics."""
        with self._lock:
            return {
                'enabled': self._enabled,
                'strategy': self.config.strategy.value,
                'queue_size': len(self._request_queue),
                'current_load': {
                    'cpu': self._current_load.cpu_usage if self._current_load else 0,
                    'memory': self._current_load.memory_usage if self._current_load else 0,
                    'active': self._current_load.active_requests if self._current_load else 0
                } if self._current_load else None
            }


class AdaptiveLoadShedder(LoadShedder):
    """
    Adaptive load shedder that adjusts based on system behavior.
    """

    def __init__(self, config: LoadSheddingConfig = None):
        super().__init__(config)
        self._success_count = 0
        self._failure_count = 0
        self._rejected_count = 0

    def record_success(self):
        """Record a successful request."""
        self._success_count += 1
        self._adjust_threshold()

    def record_failure(self):
        """Record a failed request."""
        self._failure_count += 1

    def record_rejected(self):
        """Record a rejected request."""
        self._rejected_count += 1

    def _adjust_threshold(self):
        """Adjust thresholds based on success/failure ratio."""
        total = self._success_count + self._failure_count
        if total < 100:
            return

        failure_rate = self._failure_count / total

        # Increase shedding if failure rate is high
        if failure_rate > 0.1:
            self.config.shed_threshold = min(0.99, self.config.shed_threshold * 0.95)
        elif failure_rate < 0.01:
            self.config.shed_threshold = max(0.8, self.config.shed_threshold * 1.05)

        # Reset counters
        self._success_count = 0
        self._failure_count = 0


class LoadSheddingMiddleware:
    """
    Middleware for applying load shedding to requests.
    """

    def __init__(self, shedder: LoadShedder):
        self.shedder = shedder

    def wrap_handler(self, handler: Callable, priority_fn: Callable = None) -> Callable:
        """Wrap an HTTP handler with load shedding."""
        def wrapped(request: Dict, **kwargs) -> Dict:
            priority = priority_fn(request) if priority_fn else 5

            decision = self.shedder.should_shed_request(priority)

            if decision.should_shed:
                if self.shedder.config.strategy == LoadSheddingStrategy.DEGRADE:
                    return self.shedder.get_degraded_response(request.get('type', 'unknown'))
                elif self.shedder.config.strategy == LoadSheddingStrategy.QUEUE:
                    # Queue and wait
                    queued = self.shedder.queue_request(
                        lambda: handler(request, **kwargs),
                        priority
                    )
                    if queued:
                        return {'status': 'queued', 'wait_time': decision.wait_time}
                    return {'status': 'rejected', 'reason': decision.reason}

                return {'status': 'rejected', 'reason': decision.reason}

            try:
                result = handler(request, **kwargs)
                if hasattr(self.shedder, 'record_success'):
                    self.shedder.record_success()
                return result
            except Exception as e:
                if hasattr(self.shedder, 'record_failure'):
                    self.shedder.record_failure()
                raise

        return wrapped


class PriorityLoadShedder(LoadShedder):
    """
    Load shedder that drops requests based on priority.
    """

    def __init__(self, config: LoadSheddingConfig = None):
        super().__init__(config)
        self._priority_thresholds: Dict[int, float] = {}

    def set_priority_threshold(self, priority: int, max_rate: float):
        """Set maximum rate for a priority level."""
        self._priority_thresholds[priority] = max_rate

    def should_shed_request(self, priority: int = 5) -> SheddingDecision:
        """Check if request should be shed based on priority."""
        if priority in self._priority_thresholds:
            max_rate = self._priority_thresholds[priority]

            if self._current_load and self._current_load.request_rate > max_rate:
                return SheddingDecision(
                    True,
                    f"Priority {priority} rate exceeded: {self._current_load.request_rate:.1f} > {max_rate}",
                    LoadSheddingStrategy.PRIORITY
                )

        return super().should_shed_request(priority)


# Global load shedder
_load_shedder = LoadShedder()


def get_load_shedder() -> LoadShedder:
    return _load_shedder


def create_adaptive_shedder() -> AdaptiveLoadShedder:
    """Create an adaptive load shedder."""
    return AdaptiveLoadShedder()


def create_priority_shedder() -> PriorityLoadShedder:
    """Create a priority-based load shedder."""
    return PriorityLoadShedder()