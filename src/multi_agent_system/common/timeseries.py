"""Time series data handling and aggregation."""

import logging
import time
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

logger = logging.getLogger('timeseries')


class AggregationType(Enum):
    """Aggregation types for time series."""
    AVG = "avg"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    LAST = "last"
    FIRST = "first"


@dataclass
class DataPoint:
    """A single data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class TimeSeries:
    """A time series."""
    name: str
    points: List[DataPoint] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)


class TimeSeriesBucket:
    """A bucket for aggregating time series data."""

    def __init__(self, bucket_size: float, start_time: float):
        self.bucket_size = bucket_size
        self.start_time = start_time
        self.end_time = start_time + bucket_size
        self._values: List[float] = []
        self._count = 0

    def add(self, value: float):
        """Add a value to the bucket."""
        self._values.append(value)
        self._count += 1

    def is_full(self, timestamp: float) -> bool:
        """Check if bucket is full based on timestamp."""
        return timestamp >= self.end_time

    def get_aggregated(self, agg_type: AggregationType) -> float:
        """Get aggregated value."""
        if not self._values:
            return 0.0

        if agg_type == AggregationType.AVG:
            return sum(self._values) / len(self._values)
        elif agg_type == AggregationType.SUM:
            return sum(self._values)
        elif agg_type == AggregationType.MIN:
            return min(self._values)
        elif agg_type == AggregationType.MAX:
            return max(self._values)
        elif agg_type == AggregationType.COUNT:
            return float(self._count)
        elif agg_type == AggregationType.LAST:
            return self._values[-1]
        elif agg_type == AggregationType.FIRST:
            return self._values[0]

        return 0.0


class TimeSeriesAggregator:
    """
    Aggregates time series data over time windows.
    """

    def __init__(self, window_size: float = 60.0):
        self.window_size = window_size  # seconds
        self._buckets: Dict[str, List[TimeSeriesBucket]] = {}
        self._current_bucket_start: Dict[str, float] = {}

    def add(self, series_name: str, value: float, timestamp: float = None,
            labels: Dict[str, str] = None):
        """Add a data point to a time series."""
        timestamp = timestamp or time.time()

        # Get or create bucket
        bucket_start = self._get_bucket_start(series_name, timestamp)

        # Find or create bucket
        if series_name not in self._buckets:
            self._buckets[series_name] = []

        buckets = self._buckets[series_name]

        # Find matching bucket
        bucket = None
        for b in buckets:
            if abs(b.start_time - bucket_start) < 0.001:
                bucket = b
                break

        if not bucket:
            bucket = TimeSeriesBucket(self.window_size, bucket_start)
            buckets.append(bucket)

        bucket.add(value)

    def _get_bucket_start(self, series_name: str, timestamp: float) -> float:
        """Calculate bucket start time."""
        if series_name in self._current_bucket_start:
            prev_start = self._current_bucket_start[series_name]
            if timestamp >= prev_start + self.window_size:
                self._current_bucket_start[series_name] = int(timestamp / self.window_size) * self.window_size
        else:
            self._current_bucket_start[series_name] = int(timestamp / self.window_size) * self.window_size

        return self._current_bucket_start[series_name]

    def query(self, series_name: str, start_time: float, end_time: float,
             agg_type: AggregationType = AggregationType.AVG) -> List[Tuple[float, float]]:
        """Query aggregated data."""
        if series_name not in self._buckets:
            return []

        results = []
        for bucket in self._buckets[series_name]:
            if start_time <= bucket.start_time < end_time:
                value = bucket.get_aggregated(agg_type)
                results.append((bucket.start_time, value))

        return results

    def get_latest(self, series_name: str) -> Optional[float]:
        """Get the latest value."""
        if series_name not in self._buckets or not self._buckets[series_name]:
            return None

        buckets = self._buckets[series_name]
        latest_bucket = max(buckets, key=lambda b: b.start_time)

        if latest_bucket._values:
            return latest_bucket._values[-1]

        return None


class TimeSeriesStore:
    """
    Store for time series data.
    """

    def __init__(self, max_points: int = 10000):
        self.max_points = max_points
        self._series: Dict[str, deque] = {}
        self._lock = __import__('threading').Lock()

    def append(self, name: str, value: float, timestamp: float = None,
              labels: Dict[str, str] = None):
        """Append a data point."""
        timestamp = timestamp or time.time()
        point = DataPoint(timestamp=timestamp, value=value, labels=labels or {})

        with self._lock:
            if name not in self._series:
                self._series[name] = deque(maxlen=self.max_points)

            self._series[name].append(point)

    def query(self, name: str, start_time: float = None,
             end_time: float = None) -> List[DataPoint]:
        """Query data points."""
        with self._lock:
            if name not in self._series:
                return []

            points = list(self._series[name])

        if start_time:
            points = [p for p in points if p.timestamp >= start_time]
        if end_time:
            points = [p for p in points if p.timestamp <= end_time]

        return points

    def get_series(self, name: str) -> List[DataPoint]:
        """Get all points for a series."""
        with self._lock:
            return list(self._series.get(name, []))

    def get_stats(self, name: str) -> Dict:
        """Get statistics for a series."""
        points = self.query(name)
        if not points:
            return {}

        values = [p.value for p in points]
        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'avg': sum(values) / len(values),
            'first_timestamp': points[0].timestamp,
            'last_timestamp': points[-1].timestamp
        }


class RollingWindow:
    """Rolling window for time series."""

    def __init__(self, window_size: int, max_age: float = None):
        self.window_size = window_size
        self.max_age = max_age
        self._values: deque = deque(maxlen=window_size)
        self._timestamps: deque = deque(maxlen=window_size)

    def add(self, value: float, timestamp: float = None):
        """Add a value."""
        timestamp = timestamp or time.time()
        self._values.append(value)
        self._timestamps.append(timestamp)
        self._cleanup(timestamp)

    def _cleanup(self, now: float):
        """Clean up old values."""
        if not self.max_age:
            return

        while self._timestamps and now - self._timestamps[0] > self.max_age:
            self._values.popleft()
            self._timestamps.popleft()

    def get_values(self) -> List[float]:
        """Get current values."""
        return list(self._values)

    def get_avg(self) -> float:
        """Get average of current values."""
        if not self._values:
            return 0.0
        return sum(self._values) / len(self._values)

    def get_sum(self) -> float:
        """Get sum of current values."""
        return sum(self._values)

    def get_min(self) -> float:
        """Get minimum of current values."""
        return min(self._values) if self._values else 0.0

    def get_max(self) -> float:
        """Get maximum of current values."""
        return max(self._values) if self._values else 0.0


class TimeSeriesPredictor:
    """
    Simple time series prediction using moving average.
    """

    def __init__(self, window_size: int = 10):
        self.window_size = window_size
        self._history = RollingWindow(window_size)

    def add(self, value: float):
        """Add a value."""
        self._history.add(value)

    def predict(self) -> float:
        """Predict next value based on moving average."""
        return self._history.get_avg()

    def predict_trend(self) -> str:
        """Predict trend direction."""
        values = self._history.get_values()
        if len(values) < 2:
            return "stable"

        recent = sum(values[-3:]) / min(3, len(values))
        older = sum(values[-5:-2]) / min(3, len(values) - 2) if len(values) > 2 else recent

        if recent > older * 1.05:
            return "increasing"
        elif recent < older * 0.95:
            return "decreasing"
        return "stable"


# Global store
_time_series_store = TimeSeriesStore()


def get_time_series_store() -> TimeSeriesStore:
    return _time_series_store


def add_data_point(name: str, value: float, **kwargs):
    """Add a data point to the global store."""
    _time_series_store.append(name, value, **kwargs)


def query_series(name: str, **kwargs) -> List[DataPoint]:
    """Query a series from the global store."""
    return _time_series_store.query(name, **kwargs)