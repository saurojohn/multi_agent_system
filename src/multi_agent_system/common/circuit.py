"""Circuit breaker pattern implementation for fault tolerance."""

import logging
import threading
import time
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('circuit_breaker')


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject all
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5        # Failures before opening
    success_threshold: int = 2        # Successes to close
    timeout: float = 60.0             # Seconds before trying half-open
    expected_exception: type = Exception


class CircuitBreaker:
    """
    Circuit breaker implementation.
    Prevents cascading failures by failing fast.
    """

    def __init__(self, name: str, config: CircuitConfig = None):
        self.name = name
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._failure_count: int = 0
        self._success_count: int = 0
        self._last_failure_time: float = 0
        self._lock = threading.Lock()
        self._handlers: Dict[CircuitState, List[Callable]] = {
            CircuitState.OPEN: [],
            CircuitState.HALF_OPEN: [],
            CircuitState.CLOSED: []
        }

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            self._check_state_transition()
            return self._state

    def allow_request(self) -> bool:
        """Check if request is allowed."""
        state = self.state
        return state == CircuitState.CLOSED or state == CircuitState.HALF_OPEN

    def record_success(self):
        """Record a successful call."""
        with self._lock:
            self._failure_count = 0

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)

    def record_failure(self):
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._transition_to(CircuitState.OPEN)

            elif self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)

    def _check_state_transition(self):
        """Check if state should transition."""
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure_time >= self.config.timeout:
                self._transition_to(CircuitState.HALF_OPEN)

    def _transition_to(self, new_state: CircuitState):
        """Transition to a new state."""
        if self._state == new_state:
            return

        logger.info(f"Circuit {self.name}: {self._state.value} -> {new_state.value}")
        self._state = new_state

        # Reset counters
        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
        elif new_state == CircuitState.HALF_OPEN:
            self._success_count = 0

        # Call handlers
        for handler in self._handlers.get(new_state, []):
            try:
                handler(self.name, new_state)
            except Exception as e:
                logger.error(f"Circuit handler failed: {e}")

    def on_open(self, handler: Callable):
        """Register handler for OPEN state."""
        self._handlers[CircuitState.OPEN].append(handler)

    def on_half_open(self, handler: Callable):
        """Register handler for HALF_OPEN state."""
        self._handlers[CircuitState.HALF_OPEN].append(handler)

    def on_close(self, handler: Callable):
        """Register handler for CLOSED state."""
        self._handlers[CircuitState.CLOSED].append(handler)

    def reset(self):
        """Manually reset the circuit."""
        with self._lock:
            self._failure_count = 0
            self._success_count = 0
            self._state = CircuitState.CLOSED

    def get_stats(self) -> Dict:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                'name': self.name,
                'state': self._state.value,
                'failures': self._failure_count,
                'successes': self._success_count,
                'last_failure': self._last_failure_time
            }


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers.
    """

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get_or_create(self, name: str, config: CircuitConfig = None) -> CircuitBreaker:
        """Get or create a circuit breaker."""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, config)
            return self._breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker."""
        with self._lock:
            return self._breakers.get(name)

    def all_stats(self) -> Dict[str, Dict]:
        """Get statistics for all circuit breakers."""
        with self._lock:
            return {name: cb.get_stats() for name, cb in self._breakers.items()}


class CircuitBreakerExecutor:
    """
    Executor that wraps calls with circuit breaker.
    """

    def __init__(self, circuit_breaker: CircuitBreaker):
        self.cb = circuit_breaker

    def execute(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if not self.cb.allow_request():
            raise Exception(f"Circuit {self.cb.name} is open")

        try:
            result = func(*args, **kwargs)
            self.cb.record_success()
            return result
        except self.cb.config.expected_exception as e:
            self.cb.record_failure()
            raise


def circuit_protected(breaker_name: str, registry: CircuitBreakerRegistry = None):
    """
    Decorator for circuit breaker protection.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            reg = registry or _default_registry
            cb = reg.get_or_create(breaker_name)

            if not cb.allow_request():
                raise Exception(f"Circuit {breaker_name} is open")

            try:
                result = func(*args, **kwargs)
                cb.record_success()
                return result
            except Exception as e:
                cb.record_failure()
                raise

        return wrapper
    return decorator


# Global registry
_default_registry = CircuitBreakerRegistry()


def get_registry() -> CircuitBreakerRegistry:
    return _default_registry


def get_circuit_breaker(name: str, config: CircuitConfig = None) -> CircuitBreaker:
    return _default_registry.get_or_create(name, config)