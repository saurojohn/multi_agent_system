"""Metrics aggregation and reporting."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('metrics_agg')


class MetricType(Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricPoint:
    """A metric data point."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class AggregatedMetric:
    """Aggregated metric result."""
    name: str
    count: int
    sum: float
    min: float
    max: float
    avg: float
    p50: float
    p95: float
    p99: float
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsAggregator:
    """
    Aggregates metrics for reporting.
    """

    def __init__(self):
        self._metrics: Dict[str, List[float]] = defaultdict(list)
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)
        self._lock = threading.RLock()
        self._labels: Dict[str, Dict[str, str]] = defaultdict(dict)

    def increment_counter(self, name: str, value: float = 1.0, labels: Dict[str, str] = None):
        """Increment a counter metric."""
        with self._lock:
            key = self._make_key(name, labels)
            self._counters[key] += value
            if labels:
                self._labels[key] = labels

    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge metric."""
        with self._lock:
            key = self._make_key(name, labels)
            self._gauges[key] = value
            if labels:
                self._labels[key] = labels

    def record_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record a histogram value."""
        with self._lock:
            key = self._make_key(name, labels)
            self._metrics[name].append(value)
            if labels:
                self._labels[key] = labels

    def _make_key(self, name: str, labels: Dict[str, str] = None) -> str:
        """Create a key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def _percentile(self, values: List[float], p: float) -> float:
        """Calculate percentile."""
        if not values:
            return 0.0
        sorted_vals = sorted(values)
        idx = int(len(sorted_vals) * p / 100)
        idx = min(idx, len(sorted_vals) - 1)
        return sorted_vals[idx]

    def get_counter(self, name: str, labels: Dict[str, str] = None) -> float:
        """Get counter value."""
        key = self._make_key(name, labels)
        return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, labels: Dict[str, str] = None) -> float:
        """Get gauge value."""
        key = self._make_key(name, labels)
        return self._gauges.get(key, 0.0)

    def get_histogram_stats(self, name: str, labels: Dict[str, str] = None) -> AggregatedMetric:
        """Get histogram statistics."""
        key = self._make_key(name, labels)
        values = self._metrics.get(name, [])

        if not values:
            return AggregatedMetric(
                name=name,
                count=0, sum=0.0, min=0.0, max=0.0, avg=0.0,
                p50=0.0, p95=0.0, p99=0.0,
                labels=labels or {}
            )

        return AggregatedMetric(
            name=name,
            count=len(values),
            sum=sum(values),
            min=min(values),
            max=max(values),
            avg=sum(values) / len(values),
            p50=self._percentile(values, 50),
            p95=self._percentile(values, 95),
            p99=self._percentile(values, 99),
            labels=labels or {}
        )

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics."""
        with self._lock:
            result = {
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'histograms': {}
            }

            for name in self._metrics:
                result['histograms'][name] = self.get_histogram_stats(name).__dict__

            return result

    def reset(self):
        """Reset all metrics."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._metrics.clear()
            self._labels.clear()


class MetricsReporter:
    """
    Reports metrics to various backends.
    """

    def __init__(self, aggregator: MetricsAggregator = None):
        self.aggregator = aggregator or MetricsAggregator()
        self._reporters: List[Callable] = []
        self._running = False
        self._report_thread: threading.Thread = None
        self._interval = 60

    def add_reporter(self, reporter: Callable):
        """Add a reporter function."""
        self._reporters.append(reporter)

    def start(self, interval: int = 60):
        """Start periodic reporting."""
        self._interval = interval
        self._running = True
        self._report_thread = threading.Thread(target=self._report_loop, daemon=True)
        self._report_thread.start()

    def stop(self):
        """Stop periodic reporting."""
        self._running = False
        if self._report_thread:
            self._report_thread.join(timeout=5)

    def report_now(self):
        """Trigger immediate report."""
        metrics = self.aggregator.get_all_metrics()
        for reporter in self._reporters:
            try:
                reporter(metrics)
            except Exception as e:
                logger.error(f"Reporter failed: {e}")

    def _report_loop(self):
        """Background reporting loop."""
        while self._running:
            time.sleep(self._interval)
            self.report_now()


class PrometheusFormatter:
    """Formats metrics for Prometheus."""

    @staticmethod
    def format(metrics: Dict[str, Any]) -> str:
        """Format metrics as Prometheus text format."""
        lines = []

        # Format counters
        for key, value in metrics.get('counters', {}).items():
            lines.append(f"# TYPE {key} counter")
            lines.append(f"{key} {value}")

        # Format gauges
        for key, value in metrics.get('gauges', {}).items():
            lines.append(f"# TYPE {key} gauge")
            lines.append(f"{key} {value}")

        # Format histograms
        for name, hist in metrics.get('histograms', {}).items():
            if hist['count'] > 0:
                lines.append(f"# TYPE {name} histogram")
                lines.append(f"{name}_count {hist['count']}")
                lines.append(f"{name}_sum {hist['sum']}")
                lines.append(f"{name}_avg {hist['avg']}")
                lines.append(f"{name}_min {hist['min']}")
                lines.append(f"{name}_max {hist['max']}")

        return "\n".join(lines)


class StatsDFormatter:
    """Formats metrics for StatsD."""

    @staticmethod
    def format(metrics: Dict[str, Any]) -> str:
        """Format metrics as StatsD format."""
        lines = []

        for key, value in metrics.get('counters', {}).items():
            lines.append(f"{key}:{value}|c")

        for key, value in metrics.get('gauges', {}).items():
            lines.append(f"{key}:{value}|g")

        for name, hist in metrics.get('histograms', {}).items():
            if hist['count'] > 0:
                lines.append(f"{name}.count:{hist['count']}|c")
                lines.append(f"{name}.avg:{hist['avg']}|g")

        return "\n".join(lines)


class MetricsCollector:
    """
    Collects metrics from system components.
    """

    def __init__(self, aggregator: MetricsAggregator = None):
        self.aggregator = aggregator or MetricsAggregator()
        self._collectors: Dict[str, Callable] = {}

    def register_collector(self, name: str, collector_fn: Callable):
        """Register a collector function."""
        self._collectors[name] = collector_fn

    def collect(self):
        """Collect metrics from all registered collectors."""
        for name, collector in self._collectors.items():
            try:
                collector(self.aggregator)
            except Exception as e:
                logger.error(f"Collector {name} failed: {e}")


# Global instances
_metrics_aggregator = MetricsAggregator()
_metrics_reporter = MetricsReporter(_metrics_aggregator)


def get_aggregator() -> MetricsAggregator:
    return _metrics_aggregator


def get_reporter() -> MetricsReporter:
    return _metrics_reporter


def increment_counter(name: str, value: float = 1.0, **labels):
    """Increment a counter."""
    _metrics_aggregator.increment_counter(name, value, labels)


def set_gauge(name: str, value: float, **labels):
    """Set a gauge."""
    _metrics_aggregator.set_gauge(name, value, labels)


def record_histogram(name: str, value: float, **labels):
    """Record a histogram value."""
    _metrics_aggregator.record_histogram(name, value, labels)