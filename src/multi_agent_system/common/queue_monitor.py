"""Queue monitoring and metrics collection."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('queue_monitor')


class MetricType(Enum):
    """Types of queue metrics."""
    SIZE = "size"
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    ERROR_RATE = "error_rate"
    PROCESSING_TIME = "processing_time"


@dataclass
class QueueMetrics:
    """Metrics for a queue."""
    queue_name: str
    current_size: int
    total_enqueued: int
    total_dequeued: int
    total_failed: int
    avg_wait_time: float
    avg_processing_time: float
    throughput: float  # items per second
    timestamp: float


@dataclass
class AlertThreshold:
    """Threshold for alerts."""
    metric: MetricType
    operator: str  # "gt", "lt", "eq"
    value: float
    duration_seconds: float = 0  # Must persist for this duration


class QueueMonitor:
    """
    Monitors queue performance and collects metrics.
    """

    def __init__(self):
        self._queues: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._alerts: List[Callable] = []
        self._thresholds: List[AlertThreshold] = []
        self._running = False
        self._monitor_thread: threading.Thread = None
        self._metrics_history: Dict[str, List[QueueMetrics]] = defaultdict(list)
        self._max_history = 1000

    def start(self):
        """Start the monitor."""
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("Queue monitor started")

    def stop(self):
        """Stop the monitor."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logger.info("Queue monitor stopped")

    def register_queue(self, queue_name: str, queue_getter: Callable = None):
        """Register a queue for monitoring."""
        with self._lock:
            self._queues[queue_name] = {
                'getter': queue_getter,
                'size': 0,
                'enqueued': 0,
                'dequeued': 0,
                'failed': 0,
                'wait_times': [],
                'processing_times': []
            }
        logger.info(f"Registered queue for monitoring: {queue_name}")

    def record_enqueue(self, queue_name: str):
        """Record an enqueue operation."""
        with self._lock:
            if queue_name in self._queues:
                self._queues[queue_name]['enqueued'] += 1

    def record_dequeue(self, queue_name: str):
        """Record a dequeue operation."""
        with self._lock:
            if queue_name in self._queues:
                self._queues[queue_name]['dequeued'] += 1

    def record_failure(self, queue_name: str):
        """Record a failure."""
        with self._lock:
            if queue_name in self._queues:
                self._queues[queue_name]['failed'] += 1

    def record_wait_time(self, queue_name: str, wait_time: float):
        """Record queue wait time."""
        with self._lock:
            if queue_name in self._queues:
                self._queues[queue_name]['wait_times'].append(wait_time)
                # Keep only recent wait times
                if len(self._queues[queue_name]['wait_times']) > 100:
                    self._queues[queue_name]['wait_times'].pop(0)

    def record_processing_time(self, queue_name: str, processing_time: float):
        """Record processing time."""
        with self._lock:
            if queue_name in self._queues:
                self._queues[queue_name]['processing_times'].append(processing_time)
                if len(self._queues[queue_name]['processing_times']) > 100:
                    self._queues[queue_name]['processing_times'].pop(0)

    def update_queue_size(self, queue_name: str, size: int):
        """Update the current queue size."""
        with self._lock:
            if queue_name in self._queues:
                self._queues[queue_name]['size'] = size

    def get_metrics(self, queue_name: str = None) -> Dict[str, QueueMetrics]:
        """Get current metrics."""
        with self._lock:
            metrics = {}
            now = time.time()

            queues_to_check = [queue_name] if queue_name else list(self._queues.keys())

            for q_name in queues_to_check:
                if q_name not in self._queues:
                    continue

                q = self._queues[q_name]

                # Calculate throughput (items per second over last minute)
                throughput = q['enqueued'] / max(1, now - self._queues[q_name].get('start_time', now))

                avg_wait = sum(q['wait_times']) / len(q['wait_times']) if q['wait_times'] else 0
                avg_processing = sum(q['processing_times']) / len(q['processing_times']) if q['processing_times'] else 0

                metrics[q_name] = QueueMetrics(
                    queue_name=q_name,
                    current_size=q['size'],
                    total_enqueued=q['enqueued'],
                    total_dequeued=q['dequeued'],
                    total_failed=q['failed'],
                    avg_wait_time=avg_wait,
                    avg_processing_time=avg_processing,
                    throughput=throughput,
                    timestamp=now
                )

                # Store in history
                self._metrics_history[q_name].append(metrics[q_name])
                if len(self._metrics_history[q_name]) > self._max_history:
                    self._metrics_history[q_name].pop(0)

            return metrics

    def get_historical_metrics(self, queue_name: str,
                               duration_seconds: float = 60) -> List[QueueMetrics]:
        """Get historical metrics."""
        cutoff = time.time() - duration_seconds
        with self._lock:
            history = self._metrics_history.get(queue_name, [])
            return [m for m in history if m.timestamp >= cutoff]

    def add_alert_handler(self, handler: Callable[[str, QueueMetrics], None]):
        """Add an alert handler."""
        self._alerts.append(handler)

    def add_threshold(self, threshold: AlertThreshold):
        """Add an alert threshold."""
        self._thresholds.append(threshold)

    def _check_thresholds(self, metrics: Dict[str, QueueMetrics]):
        """Check if any thresholds are violated."""
        for queue_name, metric in metrics.items():
            for threshold in self._thresholds:
                violated = False

                if threshold.metric == MetricType.SIZE:
                    value = metric.current_size
                elif threshold.metric == MetricType.THROUGHPUT:
                    value = metric.throughput
                elif threshold.metric == MetricType.ERROR_RATE:
                    total = metric.total_dequeued or 1
                    value = metric.total_failed / total
                elif threshold.metric == MetricType.LATENCY:
                    value = metric.avg_wait_time
                elif threshold.metric == MetricType.PROCESSING_TIME:
                    value = metric.avg_processing_time
                else:
                    continue

                # Check operator
                if threshold.operator == "gt" and value > threshold.value:
                    violated = True
                elif threshold.operator == "lt" and value < threshold.value:
                    violated = True
                elif threshold.operator == "eq" and abs(value - threshold.value) < 0.001:
                    violated = True

                if violated:
                    for handler in self._alerts:
                        try:
                            handler(queue_name, metric)
                        except Exception as e:
                            logger.error(f"Alert handler failed: {e}")

    def _monitor_loop(self):
        """Background monitoring loop."""
        while self._running:
            metrics = self.get_metrics()
            self._check_thresholds(metrics)
            time.sleep(1)

    def get_stats(self) -> Dict:
        """Get monitor statistics."""
        with self._lock:
            return {
                'monitored_queues': len(self._queues),
                'alerts_registered': len(self._alerts),
                'thresholds': len(self._thresholds),
                'history_entries': sum(len(h) for h in self._metrics_history.values())
            }


class QueueDepthAlert:
    """
    Alert when queue depth exceeds threshold.
    """

    def __init__(self, monitor: QueueMonitor, queue_name: str, threshold: int):
        self.monitor = monitor
        self.queue_name = queue_name
        self.threshold = threshold

    def start(self):
        """Start monitoring for alerts."""
        self.monitor.add_threshold(AlertThreshold(
            metric=MetricType.SIZE,
            operator="gt",
            value=self.threshold
        ))


class ThroughputMonitor:
    """
    Monitors throughput over time.
    """

    def __init__(self):
        self._windows: Dict[str, List[float]] = defaultdict(list)

    def record(self, queue_name: str, throughput: float):
        """Record throughput."""
        self._windows[queue_name].append(throughput)
        # Keep last 60 readings
        if len(self._windows[queue_name]) > 60:
            self._windows[queue_name].pop(0)

    def get_average(self, queue_name: str, window: int = 60) -> float:
        """Get average throughput."""
        readings = self._windows.get(queue_name, [])
        if not readings:
            return 0.0
        return sum(readings) / len(readings)

    def get_trend(self, queue_name: str) -> str:
        """Get throughput trend."""
        readings = self._windows.get(queue_name, [])
        if len(readings) < 2:
            return "stable"

        recent = sum(readings[-5:]) / min(5, len(readings))
        older = sum(readings[-10:-5]) / min(5, len(readings) - 5)

        if recent > older * 1.1:
            return "increasing"
        elif recent < older * 0.9:
            return "decreasing"
        return "stable"


# Global monitor
_queue_monitor = QueueMonitor()


def get_queue_monitor() -> QueueMonitor:
    return _queue_monitor