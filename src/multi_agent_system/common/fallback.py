"""Fallback mechanisms for handling service failures."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('fallback')


class FallbackState(Enum):
    """Fallback states."""
    ACTIVE = "active"         # Primary service active
    FALLBACK = "fallback"     # Using fallback
    DEGRADED = "degraded"     # Degraded mode
    FAILED = "failed"        # All services failed


@dataclass
class FallbackConfig:
    """Configuration for fallback."""
    max_retries: int = 3
    retry_delay: float = 0.5
    fallback_timeout: float = 5.0
    enable_circuit_breaker: bool = True
    consecutive_failures_threshold: int = 5


@dataclass
class ServiceEndpoint:
    """A service endpoint."""
    name: str
    url: str
    priority: int = 1  # Lower = higher priority
    is_healthy: bool = True
    last_check: float = 0
    response_time: float = 0
    metadata: Dict = field(default_factory=dict)


class FallbackHandler:
    """
    Handles fallback when primary service fails.
    """

    def __init__(self, config: FallbackConfig = None):
        self.config = config or FallbackConfig()
        self._services: Dict[str, List[ServiceEndpoint]] = {}
        self._current_state: Dict[str, FallbackState] = {}
        self._failure_counts: Dict[str, int] = {}
        self._lock = threading.Lock()

    def register_service(self, service_name: str, endpoint: ServiceEndpoint):
        """Register a service endpoint."""
        with self._lock:
            if service_name not in self._services:
                self._services[service_name] = []

            self._services[service_name].append(endpoint)
            self._services[service_name].sort(key=lambda e: e.priority)

            if service_name not in self._current_state:
                self._current_state[service_name] = FallbackState.ACTIVE

    def unregister_service(self, service_name: str, endpoint_name: str):
        """Unregister a service endpoint."""
        with self._lock:
            if service_name in self._services:
                self._services[service_name] = [
                    e for e in self._services[service_name]
                    if e.name != endpoint_name
                ]

    def execute(self, service_name: str, operation: Callable,
                *args, **kwargs) -> Any:
        """
        Execute operation with fallback support.
        """
        if service_name not in self._services:
            return operation(*args, **kwargs)

        endpoints = self._services[service_name]
        if not endpoints:
            return operation(*args, **kwargs)

        last_error = None

        for endpoint in endpoints:
            try:
                # Try endpoint
                result = self._execute_with_timeout(
                    operation, endpoint, args, kwargs
                )

                # Success - reset failure count
                self._record_success(service_name)
                return result

            except Exception as e:
                last_error = e
                self._record_failure(service_name, endpoint)

                if endpoint.is_healthy:
                    endpoint.is_healthy = False

                continue

        # All endpoints failed
        self._current_state[service_name] = FallbackState.FAILED

        if last_error:
            raise last_error

    def _execute_with_timeout(self, operation: Callable, endpoint: ServiceEndpoint,
                             args: tuple, kwargs: dict) -> Any:
        """Execute operation with timeout."""
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(operation, *args, **kwargs)
            return future.result(timeout=self.config.fallback_timeout)

    def _record_success(self, service_name: str):
        """Record successful operation."""
        with self._lock:
            self._failure_counts[service_name] = 0
            self._current_state[service_name] = FallbackState.ACTIVE

    def _record_failure(self, service_name: str, endpoint: ServiceEndpoint):
        """Record failed operation."""
        with self._lock:
            count = self._failure_counts.get(service_name, 0) + 1
            self._failure_counts[service_name] = count

            if count >= self.config.consecutive_failures_threshold:
                self._current_state[service_name] = FallbackState.FALLBACK

    def get_state(self, service_name: str) -> FallbackState:
        """Get current state for a service."""
        return self._current_state.get(service_name, FallbackState.ACTIVE)

    def reset(self, service_name: str):
        """Reset fallback state."""
        with self._lock:
            self._failure_counts[service_name] = 0
            self._current_state[service_name] = FallbackState.ACTIVE

            for endpoint in self._services.get(service_name, []):
                endpoint.is_healthy = True


class FallbackChain:
    """
    Chain of fallback handlers.
    """

    def __init__(self):
        self._handlers: List[FallbackHandler] = []
        self._lock = threading.Lock()

    def add_handler(self, handler: FallbackHandler, condition: Callable = None):
        """Add a fallback handler to the chain."""
        with self._lock:
            self._handlers.append((handler, condition))

    def execute(self, operation: Callable, *args, **kwargs) -> Any:
        """Execute operation through fallback chain."""
        last_error = None

        for handler, condition in self._handlers:
            if condition and not condition():
                continue

            try:
                return handler.execute('default', operation, *args, **kwargs)
            except Exception as e:
                last_error = e
                continue

        if last_error:
            raise last_error


class CircuitBreakerFallback:
    """
    Fallback with circuit breaker pattern.
    """

    def __init__(self, config: FallbackConfig = None):
        self.config = config or FallbackConfig()
        self._state: Dict[str, FallbackState] = {}
        self._failure_counts: Dict[str, int] = {}
        self._last_failure_time: Dict[str, float] = {}
        self._lock = threading.Lock()

    def is_open(self, service_name: str) -> bool:
        """Check if circuit is open."""
        state = self._state.get(service_name, FallbackState.ACTIVE)
        if state == FallbackState.FAILED:
            # Check if we should try again
            last_failure = self._last_failure_time.get(service_name, 0)
            if time.time() - last_failure > 30:  # Try after 30 seconds
                return False
            return True
        return False

    def record_success(self, service_name: str):
        """Record success and close circuit."""
        with self._lock:
            self._state[service_name] = FallbackState.ACTIVE
            self._failure_counts[service_name] = 0

    def record_failure(self, service_name: str):
        """Record failure and potentially open circuit."""
        with self._lock:
            count = self._failure_counts.get(service_name, 0) + 1
            self._failure_counts[service_name] = count
            self._last_failure_time[service_name] = time.time()

            if count >= self.config.consecutive_failures_threshold:
                self._state[service_name] = FallbackState.FAILED

    def execute(self, service_name: str, primary: Callable,
                fallback: Callable = None, *args, **kwargs) -> Any:
        """Execute with circuit breaker."""
        if self.is_open(service_name):
            if fallback:
                return fallback(*args, **kwargs)
            raise Exception(f"Circuit open for {service_name}")

        try:
            result = primary(*args, **kwargs)
            self.record_success(service_name)
            return result
        except Exception as e:
            self.record_failure(service_name)

            if fallback:
                return fallback(*args, **kwargs)
            raise


class StaticFallback:
    """
    Static fallback with pre-configured responses.
    """

    def __init__(self):
        self._fallbacks: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def set_fallback(self, key: str, value: Any):
        """Set a fallback response."""
        with self._lock:
            self._fallbacks[key] = value

    def get_fallback(self, key: str) -> Optional[Any]:
        """Get fallback response."""
        return self._fallbacks.get(key)

    def execute(self, operation: Callable, fallback_key: str,
                *args, **kwargs) -> Any:
        """Execute with static fallback."""
        try:
            return operation(*args, **kwargs)
        except Exception:
            fallback = self.get_fallback(fallback_key)
            if fallback is not None:
                return fallback
            raise


class FallbackMiddleware:
    """
    Middleware for applying fallback to handlers.
    """

    def __init__(self, fallback_handler: FallbackHandler):
        self.handler = fallback_handler

    def wrap_handler(self, service_name: str, handler: Callable) -> Callable:
        """Wrap a handler with fallback."""
        def wrapped(request: Dict, **kwargs) -> Any:
            return self.handler.execute(
                service_name,
                lambda: handler(request, **kwargs)
            )

        return wrapped


class GracefulDegradation:
    """
    Graceful degradation when services fail.
    """

    def __init__(self):
        self._degradation_levels: Dict[str, int] = {}
        self._lock = threading.Lock()

    def set_level(self, service: str, level: int):
        """Set degradation level (0 = full service, higher = more degraded)."""
        with self._lock:
            self._degradation_levels[service] = level

    def get_level(self, service: str) -> int:
        """Get degradation level."""
        return self._degradation_levels.get(service, 0)

    def should_use_degraded_response(self, service: str, threshold: int = 2) -> bool:
        """Check if should use degraded response."""
        return self.get_level(service) >= threshold


def create_fallback_handler() -> FallbackHandler:
    """Create a fallback handler."""
    return FallbackHandler()


def with_fallback(primary: Callable, fallback: Callable = None, **kwargs) -> Any:
    """
    Decorator/function for adding fallback to an operation.
    """
    handler = create_fallback_handler()
    return handler.execute('default', primary, **kwargs)