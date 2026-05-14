"""Rate limiting for API endpoints."""

import time
import threading
import logging
from typing import Dict, Optional
from collections import defaultdict

logger = logging.getLogger('rate_limit')


class TokenBucket:
    """Token bucket rate limiter implementation."""

    def __init__(self, rate: float, capacity: int):
        """
        Args:
            rate: Tokens per second
            capacity: Maximum tokens in bucket
        """
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last_update = time.time()
        self._lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if allowed."""
        with self._lock:
            now = time.time()
            # Refill tokens based on elapsed time
            elapsed = now - self._last_update
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def available(self) -> float:
        """Get current available tokens."""
        with self._lock:
            elapsed = time.time() - self._last_update
            return min(self.capacity, self._tokens + elapsed * self.rate)


class SlidingWindowCounter:
    """Sliding window counter for rate limiting."""

    def __init__(self, window_seconds: int, max_requests: int):
        self.window_seconds = window_seconds
        self.max_requests = max_requests
        self._requests = []
        self._lock = threading.Lock()

    def is_allowed(self) -> bool:
        """Check if request is allowed under rate limit."""
        with self._lock:
            now = time.time()
            # Remove old requests outside window
            self._requests = [t for t in self._requests if now - t < self.window_seconds]

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    def current_count(self) -> int:
        """Get current request count in window."""
        with self._lock:
            now = time.time()
            self._requests = [t for t in self._requests if now - t < self.window_seconds]
            return len(self._requests)


class RateLimiter:
    """Manages multiple rate limiters for different clients/endpoints."""

    def __init__(self):
        self._limiters: Dict[str, TokenBucket] = {}
        self._window_limiters: Dict[str, SlidingWindowCounter] = {}
        self._lock = threading.Lock()
        self._default_rate = 100  # requests per second
        self._default_capacity = 200

    def create_endpoint_limit(self, endpoint: str, rate: float = None,
                              capacity: int = None) -> TokenBucket:
        """Create or get endpoint rate limiter."""
        with self._lock:
            if endpoint not in self._limiters:
                r = rate or self._default_rate
                c = capacity or self._default_capacity
                self._limiters[endpoint] = TokenBucket(r, c)
                logger.info(f'Created rate limiter for {endpoint}: {r} req/s, capacity {c}')
            return self._limiters[endpoint]

    def create_client_limit(self, client_id: str, rate: float = 10,
                            capacity: int = 20) -> SlidingWindowCounter:
        """Create per-client rate limiter using sliding window."""
        with self._lock:
            if client_id not in self._window_limiters:
                self._window_limiters[client_id] = SlidingWindowCounter(60, rate)
                logger.info(f'Created client rate limiter for {client_id}: {rate} req/min')
            return self._window_limiters[client_id]

    def check_endpoint(self, endpoint: str, tokens: int = 1) -> bool:
        """Check if request to endpoint is allowed."""
        limiter = self.create_endpoint_limit(endpoint)
        return limiter.consume(tokens)

    def check_client(self, client_id: str) -> bool:
        """Check if client request is allowed."""
        if client_id not in self._window_limiters:
            self.create_client_limit(client_id)
        return self._window_limiters[client_id].is_allowed()

    def get_status(self) -> Dict:
        """Get rate limiter status."""
        with self._lock:
            endpoint_status = {
                name: {
                    'rate': lb.rate,
                    'capacity': lb.capacity,
                    'available': lb.available()
                }
                for name, lb in self._limiters.items()
            }
            client_status = {
                name: counter.current_count()
                for name, counter in self._window_limiters.items()
            }
            return {
                'endpoints': endpoint_status,
                'clients': client_status
            }

    def set_rates(self, rates: Dict[str, tuple]):
        """Update rate limits. rates = {endpoint: (rate, capacity)}"""
        with self._lock:
            for endpoint, (rate, capacity) in rates.items():
                self._limiters[endpoint] = TokenBucket(rate, capacity)
                logger.info(f'Updated rate limit for {endpoint}: {rate} req/s, capacity {capacity}')


class RateLimitMiddleware:
    """Middleware for applying rate limits to HTTP requests."""

    def __init__(self, global_rate: float = 1000, global_capacity: int = 2000):
        self.global_limiter = TokenBucket(global_rate, global_capacity)
        self.endpoint_limiters: Dict[str, TokenBucket] = {}
        self.client_limiters: Dict[str, SlidingWindowCounter] = {}

    def check_request(self, client_id: str, endpoint: str) -> tuple:
        """
        Check if request is allowed.
        Returns: (allowed: bool, reason: str, retry_after: int)
        """
        # Check global limit
        if not self.global_limiter.consume(1):
            return False, "Global rate limit exceeded", 1

        # Check endpoint limit
        if endpoint not in self.endpoint_limiters:
            self.endpoint_limiters[endpoint] = TokenBucket(100, 200)

        if not self.endpoint_limiters[endpoint].consume(1):
            return False, "Endpoint rate limit exceeded", 1

        # Check client limit (60 requests per minute)
        if client_id not in self.client_limiters:
            self.client_limiters[client_id] = SlidingWindowCounter(60, 60)

        if not self.client_limiters[client_id].is_allowed():
            available = self.client_limiters[client_id].max_requests - self.client_limiters[client_id].current_count()
            return False, f"Client rate limit exceeded. {available} requests remaining", 60

        return True, "OK", 0

    def get_limit_headers(self, client_id: str, endpoint: str) -> Dict[str, str]:
        """Get rate limit headers for response."""
        headers = {}
        if endpoint in self.endpoint_limiters:
            limiter = self.endpoint_limiters[endpoint]
            headers['X-RateLimit-Limit'] = str(int(limiter.rate))
            headers['X-RateLimit-Remaining'] = str(int(limiter.available()))
        return headers


# Global rate limiter instance
_rate_limiter = RateLimiter()


def get_rate_limiter() -> RateLimiter:
    return _rate_limiter


def get_middleware() -> RateLimitMiddleware:
    return _middleware


_middleware = RateLimitMiddleware()