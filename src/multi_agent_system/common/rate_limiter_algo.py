"""Rate limiting algorithm implementations and strategies."""

import logging
import threading
import time
import math
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('rate_limit_algo')


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    CONCURRENCY = "concurrency"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_window: int
    window_size_seconds: int
    burst_size: int = 0


class FixedWindowLimiter:
    """
    Fixed window rate limiter.
    Simple but may have boundary burst issues.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._window_start: float = time.time()
        self._request_count: int = 0
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        now = time.time()

        with self._lock:
            # Check if window has passed
            if now - self._window_start >= self.config.window_size_seconds:
                self._window_start = now
                self._request_count = 0

            # Check limit
            if self._request_count + tokens <= self.config.requests_per_window:
                self._request_count += tokens
                return True

            return False

    def get_remaining(self) -> int:
        """Get remaining requests in current window."""
        with self._lock:
            return max(0, self.config.requests_per_window - self._request_count)

    def get_reset_time(self) -> float:
        """Get window reset time."""
        with self._lock:
            return self._window_start + self.config.window_size_seconds

    def reset(self):
        """Reset the limiter."""
        with self._lock:
            self._window_start = time.time()
            self._request_count = 0


class SlidingWindowLimiter:
    """
    Sliding window rate limiter.
    More accurate than fixed window.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._timestamps: List[float] = []
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        now = time.time()
        window_start = now - self.config.window_size_seconds

        with self._lock:
            # Remove old timestamps
            self._timestamps = [t for t in self._timestamps if t > window_start]

            # Check limit
            if len(self._timestamps) + tokens <= self.config.requests_per_window:
                for _ in range(tokens):
                    self._timestamps.append(now)
                return True

            return False

    def get_remaining(self) -> int:
        """Get remaining requests."""
        now = time.time()
        window_start = now - self.config.window_size_seconds

        with self._lock:
            active = [t for t in self._timestamps if t > window_start]
            return max(0, self.config.requests_per_window - len(active))

    def get_reset_time(self) -> float:
        """Get time when oldest request expires."""
        with self._lock:
            if not self._timestamps:
                return time.time()
            return max(time.time(), self._timestamps[0] + self.config.window_size_seconds)


class TokenBucketLimiter:
    """
    Token bucket rate limiter.
    Allows burst traffic while maintaining average rate.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._tokens = float(config.burst_size or config.requests_per_window)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        with self._lock:
            # Refill tokens
            now = time.time()
            elapsed = now - self._last_refill
            refill_rate = self.config.requests_per_window / self.config.window_size_seconds
            self._tokens = min(
                self.config.requests_per_window,
                self._tokens + elapsed * refill_rate
            )
            self._last_refill = now

            # Check if we have enough tokens
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            return False

    def get_remaining(self) -> float:
        """Get remaining tokens."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_refill
            refill_rate = self.config.requests_per_window / self.config.window_size_seconds
            tokens = min(
                self.config.requests_per_window,
                self._tokens + elapsed * refill_rate
            )
            return max(0, tokens)

    def get_wait_time(self, tokens: int = 1) -> float:
        """Get time to wait before request can be allowed."""
        with self._lock:
            if self._tokens >= tokens:
                return 0

            needed = tokens - self._tokens
            refill_rate = self.config.requests_per_window / self.config.window_size_seconds
            return needed / refill_rate


class LeakyBucketLimiter:
    """
    Leaky bucket rate limiter.
    Smooths out burst traffic.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._level: float = 0.0
        self._last_leak = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        with self._lock:
            # Leak
            now = time.time()
            leak_rate = self.config.requests_per_window / self.config.window_size_seconds
            elapsed = now - self._last_leak
            self._level = max(0, self._level - elapsed * leak_rate)
            self._last_leak = now

            # Add incoming
            self._level += tokens

            # Check if bucket has capacity
            if self._level <= self.config.requests_per_window + self.config.burst_size:
                return True

            return False

    def get_remaining(self) -> int:
        """Get remaining capacity."""
        with self._lock:
            return max(0, int(self.config.requests_per_window + self.config.burst_size - self._level))


class ConcurrencyLimiter:
    """
    Concurrency rate limiter.
    Limits simultaneous operations.
    """

    def __init__(self, max_concurrent: int):
        self.max_concurrent = max_concurrent
        self._current: int = 0
        self._lock = threading.Lock()
        self._waiters: List[threading.Event] = []

    def acquire(self, timeout: float = None) -> bool:
        """Acquire a slot."""
        start = time.time()

        with self._lock:
            if self._current < self.max_concurrent:
                self._current += 1
                return True

            # Need to wait
            event = threading.Event()
            self._waiters.append(event)

        # Wait for slot
        while True:
            if event.wait(timeout=0.1):
                # Got signaled
                with self._lock:
                    if self._current < self.max_concurrent:
                        self._current += 1
                        return True
            else:
                # Timeout
                with self._lock:
                    if event in self._waiters:
                        self._waiters.remove(event)

                if timeout and time.time() - start >= timeout:
                    return False

            if timeout and time.time() - start >= timeout:
                return False

    def release(self):
        """Release a slot."""
        with self._lock:
            self._current = max(0, self._current - 1)

            # Signal a waiter
            if self._waiters:
                event = self._waiters.pop(0)
                event.set()

    def get_current(self) -> int:
        """Get current concurrency."""
        with self._lock:
            return self._current


class AlgorithmSelector:
    """
    Selects appropriate rate limiting algorithm.
    """

    @staticmethod
    def select(algorithm: RateLimitAlgorithm, config: RateLimitConfig) -> Any:
        """Select a rate limiter algorithm."""
        if algorithm == RateLimitAlgorithm.FIXED_WINDOW:
            return FixedWindowLimiter(config)
        elif algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            return SlidingWindowLimiter(config)
        elif algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            return TokenBucketLimiter(config)
        elif algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
            return LeakyBucketLimiter(config)
        else:
            return TokenBucketLimiter(config)


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts based on system load.
    """

    def __init__(self, base_config: RateLimitConfig,
                 min_requests: int = 10,
                 max_requests: int = 1000):
        self.base_config = base_config
        self.min_requests = min_requests
        self.max_requests = max_requests
        self._current_limit = base_config.requests_per_window
        self._limiter = TokenBucketLimiter(base_config)
        self._lock = threading.Lock()
        self._success_count = 0
        self._failure_count = 0

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        with self._lock:
            return self._limiter.allow(tokens)

    def record_success(self):
        """Record a successful request."""
        with self._lock:
            self._success_count += 1
            self._adjust_limit()

    def record_failure(self):
        """Record a failed request."""
        with self._lock:
            self._failure_count += 1
            self._adjust_limit()

    def _adjust_limit(self):
        """Adjust rate limit based on success/failure ratio."""
        total = self._success_count + self._failure_count
        if total < 100:
            return

        failure_rate = self._failure_count / total

        # Adjust based on failure rate
        if failure_rate > 0.1:
            # High failure rate - decrease limit
            self._current_limit = max(self.min_requests, int(self._current_limit * 0.9))
        elif failure_rate < 0.01:
            # Very low failure rate - increase limit
            self._current_limit = min(self.max_requests, int(self._current_limit * 1.1))

        # Recreate limiter with new limit
        config = RateLimitConfig(
            requests_per_window=self._current_limit,
            window_size_seconds=self.base_config.window_size_seconds,
            burst_size=self.base_config.burst_size
        )
        self._limiter = AlgorithmSelector.select(
            RateLimitAlgorithm.TOKEN_BUCKET, config
        )

        # Reset counters
        self._success_count = 0
        self._failure_count = 0

    def get_current_limit(self) -> int:
        """Get current rate limit."""
        with self._lock:
            return self._current_limit


class MultiTierRateLimiter:
    """
    Multi-tier rate limiter for different consumer levels.
    """

    def __init__(self):
        self._tiers: Dict[str, TokenBucketLimiter] = {}
        self._default_tier: str = "default"
        self._lock = threading.Lock()

    def add_tier(self, tier_name: str, requests_per_window: int,
                 window_size_seconds: int = 60,
                 burst_size: int = 0):
        """Add a tier."""
        config = RateLimitConfig(
            requests_per_window=requests_per_window,
            window_size_seconds=window_size_seconds,
            burst_size=burst_size
        )
        self._tiers[tier_name] = TokenBucketLimiter(config)

    def set_default_tier(self, tier_name: str):
        """Set default tier."""
        self._default_tier = tier_name

    def allow(self, tier: str = None) -> bool:
        """Check if request is allowed for tier."""
        tier = tier or self._default_tier

        with self._lock:
            limiter = self._tiers.get(tier, self._tiers.get(self._default_tier))
            if limiter:
                return limiter.allow()
            return True

    def get_remaining(self, tier: str = None) -> float:
        """Get remaining requests for tier."""
        tier = tier or self._default_tier

        with self._lock:
            limiter = self._tiers.get(tier, self._tiers.get(self._default_tier))
            if limiter:
                return limiter.get_remaining()
            return float('inf')


class RateLimitContext:
    """
    Context manager for rate limiting.
    """

    def __init__(self, limiter):
        self.limiter = limiter

    def __enter__(self):
        if not self.limiter.allow():
            raise Exception("Rate limit exceeded")

    def __exit__(self, *args):
        pass


# Utility functions
def create_limiter(algorithm: RateLimitAlgorithm,
                   requests_per_window: int,
                   window_size_seconds: int = 60,
                   burst_size: int = 0):
    """Create a rate limiter."""
    config = RateLimitConfig(
        requests_per_window=requests_per_window,
        window_size_seconds=window_size_seconds,
        burst_size=burst_size
    )
    return AlgorithmSelector.select(algorithm, config)


def rate_limit(max_requests: int, window_seconds: int):
    """Decorator for rate limiting a function."""
    config = RateLimitConfig(max_requests, window_seconds)
    limiter = TokenBucketLimiter(config)

    def decorator(func):
        def wrapper(*args, **kwargs):
            if not limiter.allow():
                raise Exception(f"Rate limit exceeded: {max_requests}/{window_seconds}s")
            return func(*args, **kwargs)
        return wrapper
    return decorator