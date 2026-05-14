"""Rate limiting for API endpoints with endpoint-specific limits."""

import time
import threading
import logging
from typing import Dict, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger('rate_limit')


class EndpointRateLimit:
    """Rate limit configuration for a specific endpoint."""

    def __init__(self, rate: float, capacity: int, window: int = 60):
        """
        Args:
            rate: Requests per window
            capacity: Burst capacity
            window: Time window in seconds
        """
        self.rate = rate
        self.capacity = capacity
        self.window = window


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
    """
    Middleware for applying endpoint-specific rate limits to HTTP requests.
    Supports different rate limits per endpoint.
    """

    # Default rate limits per endpoint type
    DEFAULT_ENDPOINT_LIMITS = {
        '/api/tasks': EndpointRateLimit(rate=50, capacity=100),     # Moderate
        '/api/tasks/batch': EndpointRateLimit(rate=10, capacity=20),  # Lower for batch
        '/api/workers': EndpointRateLimit(rate=30, capacity=60),
        '/api/status': EndpointRateLimit(rate=100, capacity=200),  # Higher for status
        '/api/metrics': EndpointRateLimit(rate=20, capacity=40),
        '/health': EndpointRateLimit(rate=200, capacity=500),  # Higher for health
    }

    def __init__(self, global_rate: float = 1000, global_capacity: int = 2000,
                 endpoint_limits: Dict[str, EndpointRateLimit] = None):
        self.global_limiter = TokenBucket(global_rate, global_capacity)
        self.endpoint_limits = endpoint_limits or self.DEFAULT_ENDPOINT_LIMITS.copy()
        self.endpoint_limiters: Dict[str, TokenBucket] = {}
        self.client_limiters: Dict[str, SlidingWindowCounter] = {}

    def set_endpoint_limit(self, endpoint: str, limit: EndpointRateLimit):
        """Set rate limit for a specific endpoint."""
        self.endpoint_limits[endpoint] = limit
        logger.info(f'Set endpoint limit for {endpoint}: {limit.rate} req/s')

    def check_request(self, client_id: str, endpoint: str) -> Tuple[bool, str, int]:
        """
        Check if request is allowed.
        Returns: (allowed: bool, reason: str, retry_after: int)
        """
        # Check global limit
        if not self.global_limiter.consume(1):
            return False, "Global rate limit exceeded", 1

        # Get endpoint-specific limit
        endpoint_limit = self._get_endpoint_limiter(endpoint)

        if not endpoint_limit.consume(1):
            return False, f"Endpoint rate limit exceeded for {endpoint}", 1

        # Check client limit (60 requests per minute)
        if client_id not in self.client_limiters:
            self.client_limiters[client_id] = SlidingWindowCounter(60, 60)

        if not self.client_limiters[client_id].is_allowed():
            available = self.client_limiters[client_id].max_requests - self.client_limiters[client_id].current_count()
            return False, f"Client rate limit exceeded. {available} requests remaining", 60

        return True, "OK", 0

    def _get_endpoint_limiter(self, endpoint: str) -> TokenBucket:
        """Get or create rate limiter for endpoint."""
        if endpoint not in self.endpoint_limiters:
            # Find matching limit configuration
            limit_config = None
            for pattern, config in self.endpoint_limits.items():
                if endpoint.startswith(pattern):
                    limit_config = config
                    break

            if limit_config:
                self.endpoint_limiters[endpoint] = TokenBucket(
                    limit_config.rate, limit_config.capacity
                )
            else:
                # Use default
                self.endpoint_limiters[endpoint] = TokenBucket(100, 200)

        return self.endpoint_limiters[endpoint]

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