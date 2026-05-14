"""Health checks and readiness probes for orchestrator and workers."""

import logging
import threading
import time
import psutil
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('health')


class HealthStatus(Enum):
    """Health status values."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ReadinessState(Enum):
    """Readiness state values."""
    READY = "ready"
    NOT_READY = "not_ready"
    STARTING = "starting"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    check_name: str
    status: HealthStatus
    message: str
    duration_ms: float
    timestamp: float = field(default_factory=time.time)
    metadata: Dict = field(default_factory=dict)


@dataclass
class ComponentHealth:
    """Health status of a component."""
    component_id: str
    name: str
    status: HealthStatus
    readiness: ReadinessState
    checks: List[HealthCheckResult] = field(default_factory=list)
    last_check: float = field(default_factory=time.time)
    uptime_seconds: float = 0


class HealthCheck:
    """Base class for health checks."""

    def __init__(self, name: str, enabled: bool = True, timeout: float = 5.0):
        self.name = name
        self.enabled = enabled
        self.timeout = timeout

    def check(self) -> HealthCheckResult:
        """Run the health check."""
        raise NotImplementedError


class CPUHealthCheck(HealthCheck):
    """Check CPU usage."""

    def __init__(self, threshold: float = 0.9):
        super().__init__("cpu")
        self.threshold = threshold

    def check(self) -> HealthCheckResult:
        start = time.time()
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            duration = (time.time() - start) * 1000

            if cpu_percent > self.threshold * 100:
                status = HealthStatus.UNHEALTHY
                message = f"CPU usage high: {cpu_percent:.1f}%"
            elif cpu_percent > self.threshold * 70:
                status = HealthStatus.DEGRADED
                message = f"CPU usage elevated: {cpu_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"CPU usage normal: {cpu_percent:.1f}%"

            return HealthCheckResult(
                check_name=self.name,
                status=status,
                message=message,
                duration_ms=duration,
                metadata={"cpu_percent": cpu_percent, "threshold": self.threshold}
            )
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                status=HealthStatus.UNKNOWN,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )


class MemoryHealthCheck(HealthCheck):
    """Check memory usage."""

    def __init__(self, threshold: float = 0.9):
        super().__init__("memory")
        self.threshold = threshold

    def check(self) -> HealthCheckResult:
        start = time.time()
        try:
            mem = psutil.virtual_memory()
            duration = (time.time() - start) * 1000

            if mem.percent > self.threshold * 100:
                status = HealthStatus.UNHEALTHY
                message = f"Memory usage high: {mem.percent:.1f}%"
            elif mem.percent > self.threshold * 70:
                status = HealthStatus.DEGRADED
                message = f"Memory usage elevated: {mem.percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"Memory usage normal: {mem.percent:.1f}%"

            return HealthCheckResult(
                check_name=self.name,
                status=status,
                message=message,
                duration_ms=duration,
                metadata={"mem_percent": mem.percent, "threshold": self.threshold}
            )
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                status=HealthStatus.UNKNOWN,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )


class DiskHealthCheck(HealthCheck):
    """Check disk usage."""

    def __init__(self, threshold: float = 0.9, path: str = "/"):
        super().__init__("disk")
        self.threshold = threshold
        self.path = path

    def check(self) -> HealthCheckResult:
        start = time.time()
        try:
            disk = psutil.disk_usage(self.path)
            duration = (time.time() - start) * 1000

            if disk.percent > self.threshold * 100:
                status = HealthStatus.UNHEALTHY
                message = f"Disk usage high: {disk.percent:.1f}%"
            elif disk.percent > self.threshold * 70:
                status = HealthStatus.DEGRADED
                message = f"Disk usage elevated: {disk.percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"Disk usage normal: {disk.percent:.1f}%"

            return HealthCheckResult(
                check_name=self.name,
                status=status,
                message=message,
                duration_ms=duration,
                metadata={"disk_percent": disk.percent, "threshold": self.threshold}
            )
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                status=HealthStatus.UNKNOWN,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )


class QueueHealthCheck(HealthCheck):
    """Check message queue health."""

    def __init__(self, queue_getter: Callable[[], Dict]):
        super().__init__("queue")
        self.queue_getter = queue_getter

    def check(self) -> HealthCheckResult:
        start = time.time()
        try:
            queue_stats = self.queue_getter()
            duration = (time.time() - start) * 1000

            # Check for queue depth
            total_depth = sum(q.get('size', 0) for q in queue_stats.values())

            if total_depth > 10000:
                status = HealthStatus.UNHEALTHY
                message = f"Queue backlog high: {total_depth}"
            elif total_depth > 5000:
                status = HealthStatus.DEGRADED
                message = f"Queue backlog elevated: {total_depth}"
            else:
                status = HealthStatus.HEALTHY
                message = f"Queue normal: {total_depth}"

            return HealthCheckResult(
                check_name=self.name,
                status=status,
                message=message,
                duration_ms=duration,
                metadata=queue_stats
            )
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                status=HealthStatus.UNKNOWN,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )


class WorkerHealthCheck(HealthCheck):
    """Check worker availability."""

    def __init__(self, worker_getter: Callable[[], List[Dict]]):
        super().__init__("workers")
        self.worker_getter = worker_getter

    def check(self) -> HealthCheckResult:
        start = time.time()
        try:
            workers = self.worker_getter()
            duration = (time.time() - start) * 1000

            online = sum(1 for w in workers if w.get('status') == 'online')
            offline = len(workers) - online

            if offline > len(workers) * 0.5:
                status = HealthStatus.UNHEALTHY
                message = f"Many workers offline: {offline}/{len(workers)}"
            elif offline > 0:
                status = HealthStatus.DEGRADED
                message = f"Some workers offline: {offline}/{len(workers)}"
            else:
                status = HealthStatus.HEALTHY
                message = f"All workers online: {online}"

            return HealthCheckResult(
                check_name=self.name,
                status=status,
                message=message,
                duration_ms=duration,
                metadata={"online": online, "offline": offline, "total": len(workers)}
            )
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                status=HealthStatus.UNKNOWN,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )


class HealthMonitor:
    """
    Central health monitoring system.
    """

    def __init__(self, component_id: str = "orchestrator",
                 component_name: str = "Orchestrator"):
        self.component_id = component_id
        self.component_name = component_name
        self._checks: Dict[str, HealthCheck] = {}
        self._custom_checks: List[Callable[[], HealthCheckResult]] = []
        self._start_time = time.time()
        self._lock = threading.RLock()
        self._running = False
        self._check_interval = 10.0
        self._monitor_thread: Optional[threading.Thread] = None
        self._listeners: List[Callable[[HealthStatus], None]] = []

    def register_check(self, check: HealthCheck):
        """Register a health check."""
        with self._lock:
            self._checks[check.name] = check
            logger.info(f"Registered health check: {check.name}")

    def add_custom_check(self, check_fn: Callable[[], HealthCheckResult]):
        """Add a custom health check function."""
        self._custom_checks.append(check_fn)

    def add_listener(self, listener: Callable[[HealthStatus], None]):
        """Add a listener for health status changes."""
        self._listeners.append(listener)

    def _notify_listeners(self, status: HealthStatus):
        """Notify listeners of health status change."""
        for listener in self._listeners:
            try:
                listener(status)
            except Exception as e:
                logger.error(f"Health listener failed: {e}")

    def run_checks(self) -> ComponentHealth:
        """Run all health checks."""
        results = []

        # Run registered checks
        for name, check in self._checks.items():
            if check.enabled:
                result = self._run_check(check)
                results.append(result)

        # Run custom checks
        for check_fn in self._custom_checks:
            try:
                result = check_fn()
                results.append(result)
            except Exception as e:
                results.append(HealthCheckResult(
                    check_name="custom",
                    status=HealthStatus.UNKNOWN,
                    message=str(e),
                    duration_ms=0
                ))

        # Determine overall status
        overall_status = HealthStatus.HEALTHY
        for result in results:
            if result.status == HealthStatus.UNHEALTHY:
                overall_status = HealthStatus.UNHEALTHY
                break
            elif result.status == HealthStatus.DEGRADED:
                overall_status = HealthStatus.DEGRADED

        # Determine readiness
        readiness = ReadinessState.READY
        if overall_status == HealthStatus.UNHEALTHY:
            readiness = ReadinessState.NOT_READY
        elif overall_status == HealthStatus.DEGRADED:
            readiness = ReadinessState.STARTING

        return ComponentHealth(
            component_id=self.component_id,
            name=self.component_name,
            status=overall_status,
            readiness=readiness,
            checks=results,
            last_check=time.time(),
            uptime_seconds=time.time() - self._start_time
        )

    def _run_check(self, check: HealthCheck) -> HealthCheckResult:
        """Run a single health check with timeout."""
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(check.check)
            try:
                return future.result(timeout=check.timeout)
            except concurrent.futures.TimeoutError:
                return HealthCheckResult(
                    check_name=check.name,
                    status=HealthStatus.UNKNOWN,
                    message=f"Check timed out after {check.timeout}s",
                    duration_ms=check.timeout * 1000
                )
            except Exception as e:
                return HealthCheckResult(
                    check_name=check.name,
                    status=HealthStatus.UNKNOWN,
                    message=str(e),
                    duration_ms=0
                )

    def get_status(self) -> ComponentHealth:
        """Get current health status."""
        with self._lock:
            return self.run_checks()

    def is_healthy(self) -> bool:
        """Check if component is healthy."""
        health = self.get_status()
        return health.status == HealthStatus.HEALTHY

    def is_ready(self) -> bool:
        """Check if component is ready."""
        health = self.get_status()
        return health.readiness == ReadinessState.READY

    def start_monitoring(self):
        """Start background health monitoring."""
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("Health monitoring started")

    def stop_monitoring(self):
        """Stop background health monitoring."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logger.info("Health monitoring stopped")

    def _monitor_loop(self):
        """Background monitoring loop."""
        while self._running:
            health = self.run_checks()
            self._notify_listeners(health.status)
            time.sleep(self._check_interval)

    def get_stats(self) -> Dict:
        """Get health monitor statistics."""
        with self._lock:
            return {
                'component_id': self.component_id,
                'checks_registered': len(self._checks),
                'custom_checks': len(self._custom_checks),
                'listeners': len(self._listeners),
                'uptime_seconds': time.time() - self._start_time
            }


class HealthProbeServer:
    """
    HTTP server for health probes.
    Provides /health and /ready endpoints.
    """

    def __init__(self, health_monitor: HealthMonitor, port: int = 8080):
        self.health_monitor = health_monitor
        self.port = port
        self._running = False

    def start(self, port: int = None):
        """Start the probe server."""
        import http.server
        import socketserver

        self.port = port or self.port

        class HealthHandler(http.server.BaseHTTPRequestHandler):
            monitor = self.health_monitor

            def do_GET(self):
                if self.path == '/health':
                    health = self.monitor.get_status()
                    status_code = 200 if health.status == HealthStatus.HEALTHY else 503
                    self.send_response(status_code)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    response = {
                        'status': health.status.value,
                        'uptime': health.uptime_seconds,
                        'checks': [
                            {'name': c.check_name, 'status': c.status.value}
                            for c in health.checks
                        ]
                    }
                    self.wfile.write(str(response).encode())

                elif self.path == '/ready':
                    health = self.monitor.get_status()
                    status_code = 200 if health.readiness == ReadinessState.READY else 503
                    self.send_response(status_code)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    response = {
                        'ready': health.readiness == ReadinessState.READY,
                        'status': health.status.value
                    }
                    self.wfile.write(str(response).encode())

                elif self.path == '/live':
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"alive": true}')

                else:
                    self.send_response(404)

        with socketserver.TCPServer(("", self.port), HealthHandler) as httpd:
            httpd.serve_forever()

    def start_background(self):
        """Start probe server in background thread."""
        thread = threading.Thread(target=self.start, daemon=True)
        thread.start()
        logger.info(f"Health probe server started on port {self.port}")


# Default health monitor
_default_monitor = HealthMonitor()


def get_health_monitor() -> HealthMonitor:
    return _default_monitor


def create_health_monitor(component_id: str, component_name: str) -> HealthMonitor:
    return HealthMonitor(component_id, component_name)