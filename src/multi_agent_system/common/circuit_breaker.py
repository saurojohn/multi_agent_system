"""Circuit breaker pattern for preventing cascading failures."""

import time
import threading
import logging
from typing import Callable, Optional
from enum import Enum

logger = logging.getLogger('circuit_breaker')


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject all
    HALF_OPEN = "half_open"  # Testing if recovered


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.

    States:
    - CLOSED: Normal operation, calls go through
    - OPEN: Too many failures, calls are rejected immediately
    - HALF_OPEN: Testing if the service recovered
    """

    def __init__(self,
                 failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 half_open_max_calls: int = 3,
                 name: str = "default"):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if self._last_failure_time and \
                   time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info(f'Circuit {self.name}: OPEN -> HALF_OPEN (recovery timeout)')
            return self._state

    def is_available(self) -> bool:
        """Check if calls can go through."""
        return self.state != CircuitState.OPEN

    def call(self, func: Callable, *args, **kwargs):
        """
        Execute function with circuit breaker protection.
        Raises CircuitBreakerOpen if circuit is open.
        """
        if not self.is_available():
            raise CircuitBreakerOpen(f'Circuit {self.name} is OPEN')

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        with self._lock:
            self._failure_count = 0
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.half_open_max_calls:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info(f'Circuit {self.name}: HALF_OPEN -> CLOSED (recovered)')

    def _on_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open goes back to open
                self._state = CircuitState.OPEN
                logger.warning(f'Circuit {self.name}: HALF_OPEN -> OPEN (failure in half-open)')
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                logger.warning(f'Circuit {self.name}: CLOSED -> OPEN (failure threshold exceeded)')

    def reset(self):
        """Manually reset the circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            logger.info(f'Circuit {self.name}: RESET to CLOSED')

    def get_status(self) -> dict:
        """Get circuit breaker status."""
        with self._lock:
            return {
                'name': self.name,
                'state': self.state.value,
                'failure_count': self._failure_count,
                'success_count': self._success_count,
                'last_failure_time': self._last_failure_time,
                'is_available': self.is_available()
            }


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreakerManager:
    """Manages multiple circuit breakers for different services."""

    def __init__(self):
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()

    def get_breaker(self, name: str, **kwargs) -> CircuitBreaker:
        """Get or create a circuit breaker for a service."""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name=name, **kwargs)
                logger.info(f'Created circuit breaker for: {name}')
            return self._breakers[name]

    def call(self, service_name: str, func: Callable, *args, **kwargs):
        """Call a function protected by a circuit breaker."""
        breaker = self.get_breaker(service_name)
        return breaker.call(func, *args, **kwargs)

    def get_all_status(self) -> dict:
        """Get status of all circuit breakers."""
        with self._lock:
            return {
                name: breaker.get_status()
                for name, breaker in self._breakers.items()
            }

    def reset_all(self):
        """Reset all circuit breakers."""
        with self._lock:
            for breaker in self._breakers.values():
                breaker.reset()


# Global circuit breaker manager
_manager = CircuitBreakerManager()


def get_breaker_manager() -> CircuitBreakerManager:
    return _manager


def get_breaker(name: str, **kwargs) -> CircuitBreaker:
    return _manager.get_breaker(name, **kwargs)