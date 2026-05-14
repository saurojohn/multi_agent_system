"""Real-time data streaming and analytics."""

import json
import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable, Iterator
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
from abc import ABC, abstractmethod

logger = logging.getLogger('streaming')


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    TIMER = "timer"


@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)
    metadata: Dict = field(default_factory=dict)


@dataclass
class StreamConfig:
    """Configuration for a data stream."""
    name: str
    buffer_size: int = 1000
    flush_interval: float = 5.0
    enabled: bool = True


class DataStream:
    """
    A data stream for real-time metric collection.
    """

    def __init__(self, name: str, config: StreamConfig = None):
        self.name = name
        self.config = config or StreamConfig(name=name)
        self._buffer: deque = deque(maxlen=self.config.buffer_size)
        self._lock = threading.RLock()
        self._subscribers: List[Callable[[MetricPoint], None]] = []
        self._aggregators: Dict[str, Callable] = {}
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None

    def start(self):
        """Start the stream."""
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def stop(self):
        """Stop the stream."""
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=5)

    def emit(self, name: str, value: float, metric_type: MetricType = MetricType.GAUGE,
             labels: Dict = None, metadata: Dict = None):
        """Emit a metric point."""
        point = MetricPoint(
            name=name,
            value=value,
            metric_type=metric_type,
            timestamp=time.time(),
            labels=labels or {},
            metadata=metadata or {}
        )

        with self._lock:
            self._buffer.append(point)

        # Notify subscribers
        for subscriber in self._subscribers:
            try:
                subscriber(point)
            except Exception as e:
                logger.error(f"Stream subscriber failed: {e}")

    def subscribe(self, callback: Callable[[MetricPoint], None]):
        """Subscribe to metric points."""
        self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable):
        """Unsubscribe from metric points."""
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def get_points(self, since: float = None, name: str = None,
                   labels: Dict = None) -> List[MetricPoint]:
        """Get metric points matching criteria."""
        with self._lock:
            points = list(self._buffer)

        if since:
            points = [p for p in points if p.timestamp >= since]

        if name:
            points = [p for p in points if p.name == name]

        if labels:
            points = [
                p for p in points
                if all(p.labels.get(k) == v for k, v in labels.items())
            ]

        return points

    def aggregate(self, name: str, window_seconds: float = 60,
                  agg_fn: Callable = None) -> float:
        """Aggregate metrics over a time window."""
        points = self.get_points(since=time.time() - window_seconds, name=name)

        if not points:
            return 0.0

        if agg_fn:
            return agg_fn([p.value for p in points])

        # Default aggregation: average
        return sum(p.value for p in points) / len(points)

    def _flush_loop(self):
        """Background flush loop."""
        while self._running:
            time.sleep(self.config.flush_interval)
            self._flush()

    def _flush(self):
        """Flush buffer if needed."""
        pass  # Could send to external system

    def get_stats(self) -> Dict:
        """Get stream statistics."""
        with self._lock:
            return {
                'name': self.name,
                'buffer_size': len(self._buffer),
                'max_buffer': self.config.buffer_size,
                'subscribers': len(self._subscribers)
            }


class MetricAggregator:
    """
    Aggregates metrics over time windows.
    """

    def __init__(self):
        self._windows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self._lock = threading.RLock()

    def add(self, metric_name: str, value: float, timestamp: float = None):
        """Add a value to the aggregator."""
        timestamp = timestamp or time.time()

        with self._lock:
            self._windows[metric_name].append({
                'value': value,
                'timestamp': timestamp
            })

    def get_window(self, metric_name: str, window_seconds: float) -> List[float]:
        """Get values within a time window."""
        cutoff = time.time() - window_seconds

        with self._lock:
            if metric_name not in self._windows:
                return []

            return [
                entry['value']
                for entry in self._windows[metric_name]
                if entry['timestamp'] >= cutoff
            ]

    def sum(self, metric_name: str, window_seconds: float = 60) -> float:
        """Sum values in a window."""
        values = self.get_window(metric_name, window_seconds)
        return sum(values) if values else 0.0

    def avg(self, metric_name: str, window_seconds: float = 60) -> float:
        """Average values in a window."""
        values = self.get_window(metric_name, window_seconds)
        return sum(values) / len(values) if values else 0.0

    def min(self, metric_name: str, window_seconds: float = 60) -> float:
        """Minimum value in a window."""
        values = self.get_window(metric_name, window_seconds)
        return min(values) if values else 0.0

    def max(self, metric_name: str, window_seconds: float = 60) -> float:
        """Maximum value in a window."""
        values = self.get_window(metric_name, window_seconds)
        return max(values) if values else 0.0

    def count(self, metric_name: str, window_seconds: float = 60) -> int:
        """Count values in a window."""
        return len(self.get_window(metric_name, window_seconds))

    def percentile(self, metric_name: str, p: float,
                   window_seconds: float = 60) -> float:
        """Calculate percentile of values."""
        values = sorted(self.get_window(metric_name, window_seconds))
        if not values:
            return 0.0

        idx = int(len(values) * p / 100)
        idx = min(idx, len(values) - 1)
        return values[idx]

    def clear(self, metric_name: str = None):
        """Clear metric data."""
        with self._lock:
            if metric_name:
                if metric_name in self._windows:
                    self._windows[metric_name].clear()
            else:
                self._windows.clear()


class StreamProcessor:
    """
    Processes data from multiple streams.
    """

    def __init__(self):
        self._streams: Dict[str, DataStream] = {}
        self._processors: Dict[str, Callable] = {}
        self._lock = threading.RLock()
        self._running = False
        self._process_thread: Optional[threading.Thread] = None

    def create_stream(self, name: str, **config) -> DataStream:
        """Create or get a stream."""
        with self._lock:
            if name not in self._streams:
                stream_config = StreamConfig(name=name, **config)
                self._streams[name] = DataStream(name, stream_config)
            return self._streams[name]

    def get_stream(self, name: str) -> Optional[DataStream]:
        """Get a stream by name."""
        with self._lock:
            return self._streams.get(name)

    def emit(self, stream_name: str, name: str, value: float,
             metric_type: MetricType = MetricType.GAUGE,
             labels: Dict = None, metadata: Dict = None):
        """Emit to a specific stream."""
        stream = self.get_stream(stream_name)
        if stream:
            stream.emit(name, value, metric_type, labels, metadata)

    def register_processor(self, name: str, processor: Callable):
        """Register a processor function."""
        self._processors[name] = processor

    def process(self, stream_name: str, processor_name: str) -> List[Any]:
        """Process stream data with a processor."""
        stream = self.get_stream(stream_name)
        processor = self._processors.get(processor_name)

        if not stream or not processor:
            return []

        points = stream.get_points()
        return processor(points)

    def start(self):
        """Start the stream processor."""
        self._running = True
        for stream in self._streams.values():
            stream.start()

    def stop(self):
        """Stop the stream processor."""
        self._running = False
        for stream in self._streams.values():
            stream.stop()


class TimeSeriesWindow:
    """
    A time-series window for analyzing metrics over time.
    """

    def __init__(self, window_size: int, slide: int = 1):
        """
        Args:
            window_size: Number of points in window
            slide: Number of points to slide forward
        """
        self.window_size = window_size
        self.slide = slide
        self._points: deque = deque(maxlen=window_size)
        self._lock = threading.Lock()

    def add(self, value: float, timestamp: float = None):
        """Add a point to the window."""
        timestamp = timestamp or time.time()
        with self._lock:
            self._points.append({'value': value, 'timestamp': timestamp})

    def get_values(self) -> List[float]:
        """Get all values in the window."""
        with self._lock:
            return [p['value'] for p in self._points]

    def get_points(self) -> List[Dict]:
        """Get all points in the window."""
        with self._lock:
            return list(self._points)

    def is_full(self) -> bool:
        """Check if window is full."""
        with self._lock:
            return len(self._points) >= self.window_size

    def slide_window(self) -> List[Dict]:
        """Slide the window and return exiting points."""
        with self._lock:
            if len(self._points) < self.window_size:
                return []

            # Return oldest points that will exit
            exiting = [self._points[i] for i in range(self.slide)]
            return exiting


class StreamAggregationPipeline:
    """
    A pipeline for aggregating streaming data.
    """

    def __init__(self, name: str):
        self.name = name
        self._stages: List[Callable] = []
        self._source_stream: Optional[DataStream] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def source(self, stream: DataStream):
        """Set the source stream."""
        self._source_stream = stream
        return self

    def add_stage(self, stage: Callable[[List[MetricPoint]], List[MetricPoint]]):
        """Add a processing stage."""
        self._stages.append(stage)
        return self

    def window(self, size_seconds: float,
               agg_fn: Callable[[List[MetricPoint]], float] = None) -> 'StreamAggregationPipeline':
        """Add a windowing stage."""
        def window_stage(points: List[MetricPoint]) -> List[MetricPoint]:
            cutoff = time.time() - size_seconds
            windowed = [p for p in points if p.timestamp >= cutoff]

            if not windowed:
                return []

            result_value = agg_fn(windowed) if agg_fn else windowed[-1].value

            return [MetricPoint(
                name=f"{windowed[0].name}_windowed",
                value=result_value,
                metric_type=MetricType.GAUGE,
                timestamp=time.time(),
                labels=windowed[0].labels
            )]

        self._stages.append(window_stage)
        return self

    def filter(self, filter_fn: Callable[[MetricPoint], bool]) -> 'StreamAggregationPipeline':
        """Add a filtering stage."""
        def filter_stage(points: List[MetricPoint]) -> List[MetricPoint]:
            return [p for p in points if filter_fn(p)]

        self._stages.append(filter_stage)
        return self

    def transform(self, transform_fn: Callable[[MetricPoint], MetricPoint]) -> 'StreamAggregationPipeline':
        """Add a transformation stage."""
        def transform_stage(points: List[MetricPoint]) -> List[MetricPoint]:
            return [transform_fn(p) for p in points]

        self._stages.append(transform_stage)
        return self

    def sink(self, sink_fn: Callable[[List[MetricPoint]], None]) -> 'StreamAggregationPipeline':
        """Add a sink stage."""
        def sink_stage(points: List[MetricPoint]) -> List[MetricPoint]:
            try:
                sink_fn(points)
            except Exception as e:
                logger.error(f"Sink failed: {e}")
            return points  # Pass through

        self._stages.append(sink_stage)
        return self

    def start(self):
        """Start the pipeline."""
        if not self._source_stream:
            raise ValueError("Pipeline has no source stream")

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the pipeline."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self):
        """Run the pipeline."""
        while self._running:
            points = self._source_stream.get_points()

            for stage in self._stages:
                points = stage(points)

            time.sleep(1.0)


# Global instances
_stream_processor = StreamProcessor()
_metric_aggregator = MetricAggregator()


def get_stream_processor() -> StreamProcessor:
    return _stream_processor


def get_metric_aggregator() -> MetricAggregator:
    return _metric_aggregator


def create_stream(name: str, **config) -> DataStream:
    """Create a new data stream."""
    return _stream_processor.create_stream(name, **config)