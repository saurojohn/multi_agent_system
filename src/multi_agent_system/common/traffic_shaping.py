"""Traffic shaping and rate limiting for network traffic."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

logger = logging.getLogger('traffic_shaping')


class TrafficShapeStrategy(Enum):
    """Traffic shaping strategies."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_RATE = "fixed_rate"
    BURST = "burst"


@dataclass
class TrafficShapeConfig:
    """Configuration for traffic shaping."""
    strategy: TrafficShapeStrategy = TrafficShapeStrategy.TOKEN_BUCKET
    rate: float = 100.0          # units per second
    burst_size: float = 200.0     # burst capacity
    min_rate: float = 10.0        # minimum rate
    max_rate: float = 1000.0      # maximum rate


@dataclass
class TrafficMetrics:
    """Traffic shaping metrics."""
    total_tokens: float
    available_tokens: float
    consumed_tokens: float
    wait_time: float
    dropped: int
    shaped: int


class TokenBucketShaper:
    """Token bucket traffic shaper."""

    def __init__(self, config: TrafficShapeConfig):
        self.config = config
        self._tokens = config.burst_size
        self._last_update = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> tuple:
        """
        Check if traffic is allowed.
        Returns (allowed, wait_time).
        """
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update

            # Refill tokens
            self._tokens = min(
                self.config.burst_size,
                self._tokens + elapsed * self.config.rate
            )
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True, 0.0
            else:
                wait_time = (tokens - self._tokens) / self.config.rate
                return False, wait_time

    def get_metrics(self) -> TrafficMetrics:
        """Get current metrics."""
        with self._lock:
            return TrafficMetrics(
                total_tokens=self.config.burst_size,
                available_tokens=self._tokens,
                consumed_tokens=self.config.burst_size - self._tokens,
                wait_time=0,
                dropped=0,
                shaped=0
            )


class LeakyBucketShaper:
    """Leaky bucket traffic shaper."""

    def __init__(self, config: TrafficShapeConfig):
        self.config = config
        self._level = 0.0
        self._last_leak = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> tuple:
        """
        Check if traffic is allowed.
        Returns (allowed, wait_time).
        """
        with self._lock:
            now = time.time()
            elapsed = now - self._last_leak

            # Leak
            leak_amount = elapsed * self.config.rate
            self._level = max(0, self._level - leak_amount)
            self._last_leak = now

            # Add incoming
            if self._level + tokens <= self.config.burst_size:
                self._level += tokens
                return True, 0.0
            else:
                wait_time = (self._level + tokens - self.config.burst_size) / self.config.rate
                return False, wait_time

    def get_metrics(self) -> TrafficMetrics:
        """Get current metrics."""
        with self._lock:
            return TrafficMetrics(
                total_tokens=self.config.burst_size,
                available_tokens=self.config.burst_size - self._level,
                consumed_tokens=self._level,
                wait_time=0,
                dropped=0,
                shaped=0
            )


class FixedRateShaper:
    """Fixed rate traffic shaper."""

    def __init__(self, config: TrafficShapeConfig):
        self.config = config
        self._interval = 1.0 / config.rate
        self._next_allowed = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> tuple:
        """Check if traffic is allowed."""
        with self._lock:
            now = time.time()

            if now >= self._next_allowed:
                self._next_allowed = now + self._interval * tokens
                return True, 0.0
            else:
                wait_time = self._next_allowed - now
                return False, wait_time

    def get_metrics(self) -> TrafficMetrics:
        """Get current metrics."""
        with self._lock:
            return TrafficMetrics(
                total_tokens=self.config.rate,
                available_tokens=1.0 if time.time() >= self._next_allowed else 0.0,
                consumed_tokens=0,
                wait_time=max(0, self._next_allowed - time.time()),
                dropped=0,
                shaped=0
            )


class AdaptiveTrafficShaper:
    """Adaptive traffic shaper that adjusts rate based on load."""

    def __init__(self, config: TrafficShapeConfig):
        self.config = config
        self._current_rate = config.rate
        self._shaper = TokenBucketShaper(config)
        self._lock = threading.Lock()
        self._success_count = 0
        self._failure_count = 0
        self._dropped_count = 0

    def allow(self, tokens: int = 1) -> tuple:
        """Check if traffic is allowed."""
        with self._lock:
            result = self._shaper.allow(tokens)

            if not result[0]:
                self._failure_count += 1
            else:
                self._success_count += 1

            self._adjust_rate()
            return result

    def _adjust_rate(self):
        """Adjust rate based on success/failure ratio."""
        total = self._success_count + self._failure_count
        if total < 100:
            return

        failure_rate = self._failure_count / total

        if failure_rate > 0.1:
            # High failure rate - reduce rate
            self._current_rate = max(
                self.config.min_rate,
                self._current_rate * 0.9
            )
        elif failure_rate < 0.01:
            # Low failure rate - increase rate
            self._current_rate = min(
                self.config.max_rate,
                self._current_rate * 1.1
            )

        # Update shaper config
        self._shaper = TokenBucketShaper(TrafficShapeConfig(
            strategy=TrafficShapeStrategy.TOKEN_BUCKET,
            rate=self._current_rate,
            burst_size=self.config.burst_size
        ))

        # Reset counters
        self._success_count = 0
        self._failure_count = 0

    def record_drop(self):
        """Record a dropped request."""
        self._dropped_count += 1

    def get_current_rate(self) -> float:
        """Get current rate."""
        return self._current_rate


class TrafficRouter:
    """
    Routes traffic based on shaper availability.
    """

    def __init__(self):
        self._shapers: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def add_shaper(self, name: str, shaper: Any):
        """Add a traffic shaper."""
        with self._lock:
            self._shapers[name] = shaper

    def select_shaper(self) -> tuple:
        """Select an available shaper."""
        with self._lock:
            for name, shaper in self._shapers.items():
                allowed, wait_time = shaper.allow(1)
                if allowed:
                    return name, shaper
            return None, None


class TrafficShapeMiddleware:
    """
    Middleware for applying traffic shaping.
    """

    def __init__(self, shaper: Any):
        self.shaper = shaper

    def wrap_handler(self, handler: Callable) -> Callable:
        """Wrap a handler with traffic shaping."""
        def wrapped(request: Dict, **kwargs) -> Any:
            allowed, wait_time = self.shaper.allow(1)

            if not allowed:
                return {
                    'status': 'shaped',
                    'wait_time': wait_time,
                    'message': 'Traffic shaping applied'
                }

            return handler(request, **kwargs)

        return wrapped


class BandwidthAllocator:
    """
    Allocates bandwidth across multiple traffic classes.
    """

    def __init__(self, total_bandwidth: float):
        self.total_bandwidth = total_bandwidth
        self._allocations: Dict[str, float] = {}
        self._lock = threading.Lock()

    def allocate(self, class_name: str, bandwidth: float) -> bool:
        """Allocate bandwidth to a class."""
        with self._lock:
            total_allocated = sum(self._allocations.values())
            remaining = self.total_bandwidth - total_allocated

            if bandwidth > remaining:
                return False

            self._allocations[class_name] = bandwidth
            return True

    def release(self, class_name: str):
        """Release bandwidth allocation."""
        with self._lock:
            if class_name in self._allocations:
                del self._allocations[class_name]

    def get_allocation(self, class_name: str) -> float:
        """Get allocation for a class."""
        return self._allocations.get(class_name, 0.0)

    def get_total_allocated(self) -> float:
        """Get total allocated bandwidth."""
        return sum(self._allocations.values())


class PriorityBandwidthAllocator(BandwidthAllocator):
    """
    Priority-based bandwidth allocator.
    Higher priority classes get bandwidth first.
    """

    def __init__(self, total_bandwidth: float):
        super().__init__(total_bandwidth)
        self._priorities: Dict[str, int] = {}

    def allocate_with_priority(self, class_name: str, bandwidth: float, priority: int):
        """Allocate bandwidth with priority."""
        with self._lock:
            self._priorities[class_name] = priority

            # Sort by priority (lower number = higher priority)
            sorted_classes = sorted(
                self._allocations.keys(),
                key=lambda c: self._priorities.get(c, 5)
            )

            total_allocated = sum(self._allocations.values())

            if bandwidth <= self.total_bandwidth - total_allocated:
                self._allocations[class_name] = bandwidth
                return True

            # Try to reclaim from lower priority classes
            for class_name in sorted_classes:
                if self._priorities.get(class_name, 5) > priority:
                    excess = self._allocations[class_name] - 10  # Keep minimum
                    if excess > 0:
                        self._allocations[class_name] = 10
                        total_allocated = sum(self._allocations.values())
                        if bandwidth <= self.total_bandwidth - total_allocated:
                            self._allocations[class_name] += bandwidth
                            return True

            return False


# Global shaper
_default_shaper = TokenBucketShaper(TrafficShapeConfig())


def get_default_shaper() -> Any:
    return _default_shaper


def create_shaper(strategy: TrafficShapeStrategy, **config) -> Any:
    """Create a traffic shaper."""
    shape_config = TrafficShapeConfig(strategy=strategy, **config)

    if strategy == TrafficShapeStrategy.TOKEN_BUCKET:
        return TokenBucketShaper(shape_config)
    elif strategy == TrafficShapeStrategy.LEAKY_BUCKET:
        return LeakyBucketShaper(shape_config)
    elif strategy == TrafficShapeStrategy.FIXED_RATE:
        return FixedRateShaper(shape_config)
    else:
        return TokenBucketShaper(shape_config)