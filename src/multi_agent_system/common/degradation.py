"""Graceful degradation for partial system failures."""

import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Any
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger('degradation')


class ServiceHealth(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILING = "failing"
    OFFLINE = "offline"


@dataclass
class ServiceStatus:
    name: str
    health: ServiceHealth
    last_check: float
    consecutive_failures: int
    total_requests: int
    failed_requests: int

    @property
    def failure_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests


class GracefulDegradationManager:
    """
    Manages system-wide graceful degradation.
    When components fail, system continues operating with reduced functionality.
    """

    def __init__(self,
                 failure_threshold: float = 0.5,
                 recovery_threshold: float = 0.2,
                 check_interval: float = 10.0):
        """
        Args:
            failure_threshold: Failure rate to trigger degradation (50%)
            recovery_threshold: Failure rate to consider recovered (20%)
            check_interval: Seconds between health checks
        """
        self.failure_threshold = failure_threshold
        self.recovery_threshold = recovery_threshold
        self.check_interval = check_interval

        self._services: Dict[str, ServiceStatus] = {}
        self._fallback_handlers: Dict[str, Callable] = {}
        self._degraded_features: Dict[str, bool] = {}
        self._lock = threading.RLock()
        self._running = False
        self._health_thread: Optional[threading.Thread] = None

    def register_service(self, service_name: str):
        """Register a service for health monitoring."""
        with self._lock:
            self._services[service_name] = ServiceStatus(
                name=service_name,
                health=ServiceHealth.HEALTHY,
                last_check=time.time(),
                consecutive_failures=0,
                total_requests=0,
                failed_requests=0
            )
        logger.info(f'Registered service for monitoring: {service_name}')

    def record_request(self, service_name: str, success: bool):
        """Record request result for a service."""
        with self._lock:
            if service_name not in self._services:
                self.register_service(service_name)

            service = self._services[service_name]
            service.total_requests += 1
            if not success:
                service.failed_requests += 1
                service.consecutive_failures += 1
            else:
                service.consecutive_failures = 0

    def register_fallback(self, service_name: str, fallback: Callable):
        """Register fallback handler for when service fails."""
        with self._lock:
            self._fallback_handlers[service_name] = fallback
        logger.info(f'Registered fallback for: {service_name}')

    def get_service_health(self, service_name: str) -> ServiceHealth:
        """Get current health status of a service."""
        with self._lock:
            if service_name not in self._services:
                return ServiceHealth.HEALTHY
            return self._services[service_name].health

    def is_degraded(self) -> bool:
        """Check if any service is degraded."""
        with self._lock:
            for service in self._services.values():
                if service.health != ServiceHealth.HEALTHY:
                    return True
        return False

    def get_degraded_features(self) -> List[str]:
        """Get list of currently degraded features."""
        with self._lock:
            return [name for name, degraded in self._degraded_features.items() if degraded]

    def start(self):
        """Start health monitoring."""
        if self._running:
            return
        self._running = True
        self._health_thread = threading.Thread(target=self._health_loop, daemon=True)
        self._health_thread.start()
        logger.info('Graceful degradation manager started')

    def stop(self):
        """Stop health monitoring."""
        self._running = False
        if self._health_thread:
            self._health_thread.join(timeout=5)
        logger.info('Graceful degradation manager stopped')

    def _health_loop(self):
        """Background health monitoring loop."""
        while self._running:
            self._check_services()
            time.sleep(self.check_interval)

    def _check_services(self):
        """Check all services and update health status."""
        with self._lock:
            for name, service in self._services.items():
                # Update based on failure rate
                if service.total_requests > 0:
                    rate = service.failed_requests / service.total_requests

                    if rate >= self.failure_threshold or service.consecutive_failures >= 5:
                        if service.health != ServiceHealth.FAILING:
                            service.health = ServiceHealth.FAILING
                            self._degraded_features[name] = True
                            logger.warning(f'Service {name} is failing (rate: {rate:.2%})')

                            # Try to use fallback
                            if name in self._fallback_handlers:
                                logger.info(f'Using fallback for {name}')

                    elif rate <= self.recovery_threshold and service.consecutive_failures == 0:
                        if service.health != ServiceHealth.HEALTHY:
                            service.health = ServiceHealth.HEALTHY
                            self._degraded_features[name] = False
                            logger.info(f'Service {name} recovered')

                # Reset counters periodically
                if service.total_requests > 100:
                    service.total_requests = 0
                    service.failed_requests = 0

    def execute_with_fallback(self, service_name: str,
                             primary_func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with fallback on failure.
        Returns result from primary or fallback handler.
        """
        service_health = self.get_service_health(service_name)

        # If service is failing, skip primary and use fallback
        if service_health == ServiceHealth.FAILING:
            if service_name in self._fallback_handlers:
                logger.info(f'Using fallback for {service_name} instead of primary')
                try:
                    return self._fallback_handlers[service_name](*args, **kwargs)
                except Exception as e:
                    logger.error(f'Fallback also failed: {e}')
                    return self._get_default_result(service_name)

        # Try primary
        try:
            self.record_request(service_name, True)
            return primary_func(*args, **kwargs)
        except Exception as e:
            self.record_request(service_name, False)
            logger.error(f'Primary failed for {service_name}: {e}')

            # Try fallback
            if service_name in self._fallback_handlers:
                logger.info(f'Falling back for {service_name}')
                try:
                    return self._fallback_handlers[service_name](*args, **kwargs)
                except Exception as e2:
                    logger.error(f'Fallback also failed: {e2}')

            return self._get_default_result(service_name)

    def _get_default_result(self, service_name: str) -> Any:
        """Get default result when all options fail."""
        return {'status': 'degraded', 'service': service_name, 'error': 'service unavailable'}

    def get_all_status(self) -> Dict:
        """Get status of all monitored services."""
        with self._lock:
            return {
                name: {
                    'health': service.health.value,
                    'failure_rate': service.failure_rate,
                    'consecutive_failures': service.consecutive_failures,
                    'is_degraded': self._degraded_features.get(name, False)
                }
                for name, service in self._services.items()
            }


class CircuitBreakerDegradation(GracefulDegradationManager):
    """
    Extended degradation manager with circuit breaker integration.
    Combines circuit breaker pattern with graceful degradation.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._circuit_breakers: Dict[str, object] = {}

    def register_circuit_breaker(self, service_name: str, breaker):
        """Register circuit breaker for a service."""
        self._circuit_breakers[service_name] = breaker

    def get_circuit_state(self, service_name: str) -> str:
        """Get circuit breaker state for a service."""
        if service_name in self._circuit_breakers:
            try:
                return self._circuit_breakers[service_name].state.value
            except:
                pass
        return 'unknown'


# Global degradation manager
_degradation_manager = GracefulDegradationManager()


def get_degradation_manager() -> GracefulDegradationManager:
    return _degradation_manager