"""Adaptive routing for intelligent request distribution."""

import logging
import threading
import time
import random
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('routing')


class RoutingStrategy(Enum):
    """Routing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    WEIGHTED = "weighted"
    RANDOM = "random"
    LATENCY_BASED = "latency_based"
    CONSISTENT_HASH = "consistent_hash"


@dataclass
class Endpoint:
    """A routing endpoint."""
    endpoint_id: str
    url: str
    weight: float = 1.0
    max_load: int = 100
    current_load: int = 0
    avg_latency: float = 0.0
    last_error: Optional[float] = None
    is_healthy: bool = True
    metadata: Dict = field(default_factory=dict)


@dataclass
class RouteResult:
    """Result of routing decision."""
    endpoint: Endpoint
    strategy: RoutingStrategy
    decision_ms: float


class RoutingRule:
    """A custom routing rule."""

    def __init__(self, name: str, matcher: Callable[[Dict], bool],
                 selector: Callable[[List[Endpoint]], Endpoint]):
        self.name = name
        self.matcher = matcher
        self.selector = selector


class AdaptiveRouter:
    """
    Adaptive routing with multiple strategies.
    """

    def __init__(self):
        self._endpoints: Dict[str, Endpoint] = {}
        self._strategy: RoutingStrategy = RoutingStrategy.ROUND_ROBIN
        self._rules: List[RoutingRule] = []
        self._lock = threading.RLock()
        self._round_robin_index: Dict[str, int] = defaultdict(int)
        self._hash_ring: Dict[str, int] = {}  # For consistent hashing

    def add_endpoint(self, endpoint: Endpoint):
        """Add an endpoint to the router."""
        with self._lock:
            self._endpoints[endpoint.endpoint_id] = endpoint
            # Add to hash ring
            self._hash_ring[endpoint.endpoint_id] = len(self._hash_ring)
            logger.info(f"Added endpoint: {endpoint.endpoint_id} ({endpoint.url})")

    def remove_endpoint(self, endpoint_id: str):
        """Remove an endpoint."""
        with self._lock:
            if endpoint_id in self._endpoints:
                del self._endpoints[endpoint_id]
                if endpoint_id in self._hash_ring:
                    del self._hash_ring[endpoint_id]
                logger.info(f"Removed endpoint: {endpoint_id}")

    def set_strategy(self, strategy: RoutingStrategy):
        """Set the routing strategy."""
        self._strategy = strategy
        logger.info(f"Routing strategy set to: {strategy.value}")

    def add_rule(self, rule: RoutingRule):
        """Add a custom routing rule."""
        self._rules.append(rule)

    def route(self, request: Dict = None) -> RouteResult:
        """
        Route a request to an endpoint.
        Returns RouteResult with selected endpoint.
        """
        start = time.time()
        request = request or {}

        with self._lock:
            if not self._endpoints:
                raise ValueError("No endpoints available")

            # Check custom rules first
            for rule in self._rules:
                if rule.matcher(request):
                    endpoint = rule.selector(list(self._endpoints.values()))
                    return RouteResult(
                        endpoint=endpoint,
                        strategy=self._strategy,
                        decision_ms=(time.time() - start) * 1000
                    )

            # Use configured strategy
            endpoint = self._select_endpoint()
            return RouteResult(
                endpoint=endpoint,
                strategy=self._strategy,
                decision_ms=(time.time() - start) * 1000
            )

    def _select_endpoint(self) -> Endpoint:
        """Select endpoint based on current strategy."""
        healthy = [e for e in self._endpoints.values() if e.is_healthy]

        if not healthy:
            # Fall back to unhealthy if no healthy
            healthy = list(self._endpoints.values())

        if self._strategy == RoutingStrategy.ROUND_ROBIN:
            return self._round_robin(healthy)
        elif self._strategy == RoutingStrategy.LEAST_LOADED:
            return self._least_loaded(healthy)
        elif self._strategy == RoutingStrategy.WEIGHTED:
            return self._weighted(healthy)
        elif self._strategy == RoutingStrategy.RANDOM:
            return self._random(healthy)
        elif self._strategy == RoutingStrategy.LATENCY_BASED:
            return self._latency_based(healthy)
        elif self._strategy == RoutingStrategy.CONSISTENT_HASH:
            return self._consistent_hash(healthy)
        else:
            return self._round_robin(healthy)

    def _round_robin(self, endpoints: List[Endpoint]) -> Endpoint:
        """Round robin selection."""
        if not endpoints:
            raise ValueError("No endpoints")

        # Get endpoint IDs sorted for consistent ordering
        endpoint_ids = sorted(e.endpoint_id for e in endpoints)
        index_key = ",".join(endpoint_ids)

        idx = self._round_robin_index[index_key]
        selected = endpoints[idx % len(endpoints)]
        self._round_robin_index[index_key] = idx + 1

        return selected

    def _least_loaded(self, endpoints: List[Endpoint]) -> Endpoint:
        """Select endpoint with least load."""
        return min(endpoints, key=lambda e: e.current_load)

    def _weighted(self, endpoints: List[Endpoint]) -> Endpoint:
        """Weighted random selection."""
        weights = [e.weight for e in endpoints]
        total = sum(weights)
        r = random.random() * total

        cumulative = 0
        for endpoint, w in zip(endpoints, weights):
            cumulative += w
            if r <= cumulative:
                return endpoint

        return endpoints[-1]

    def _random(self, endpoints: List[Endpoint]) -> Endpoint:
        """Random selection."""
        return random.choice(endpoints)

    def _latency_based(self, endpoints: List[Endpoint]) -> Endpoint:
        """Select endpoint with lowest latency."""
        # Weight by inverse latency (faster = higher weight)
        return min(endpoints, key=lambda e: e.avg_latency if e.avg_latency > 0 else float('inf'))

    def _consistent_hash(self, endpoints: List[Endpoint]) -> Endpoint:
        """Consistent hash based selection."""
        if not endpoints:
            raise ValueError("No endpoints")

        # Simple hash-based selection
        import hashlib
        timestamp = str(time.time()).encode()
        hash_value = int(hashlib.md5(timestamp).hexdigest(), 16)
        idx = hash_value % len(endpoints)
        return endpoints[idx]

    def update_endpoint_stats(self, endpoint_id: str,
                             latency: float = None,
                             load: int = None,
                             error: bool = False):
        """Update endpoint statistics."""
        with self._lock:
            if endpoint_id not in self._endpoints:
                return

            endpoint = self._endpoints[endpoint_id]

            if latency is not None:
                # Exponential moving average
                if endpoint.avg_latency == 0:
                    endpoint.avg_latency = latency
                else:
                    endpoint.avg_latency = 0.7 * endpoint.avg_latency + 0.3 * latency

            if load is not None:
                endpoint.current_load = load

            if error:
                endpoint.last_error = time.time()
                # Mark unhealthy if too many errors
                if endpoint.last_error and time.time() - endpoint.last_error < 60:
                    endpoint.is_healthy = False

    def record_success(self, endpoint_id: str, latency: float):
        """Record successful request."""
        self.update_endpoint_stats(endpoint_id, latency=latency, error=False)
        if endpoint_id in self._endpoints:
            with self._lock:
                e = self._endpoints[endpoint_id]
                e.current_load = max(0, e.current_load - 1)
                if not e.is_healthy and e.last_error:
                    # Recover if no errors in 5 minutes
                    if time.time() - e.last_error > 300:
                        e.is_healthy = True

    def record_failure(self, endpoint_id: str):
        """Record failed request."""
        self.update_endpoint_stats(endpoint_id, error=True)
        if endpoint_id in self._endpoints:
            with self._lock:
                self._endpoints[endpoint_id].current_load += 1

    def get_healthy_endpoints(self) -> List[Endpoint]:
        """Get list of healthy endpoints."""
        with self._lock:
            return [e for e in self._endpoints.values() if e.is_healthy]

    def get_stats(self) -> Dict:
        """Get router statistics."""
        with self._lock:
            return {
                'total_endpoints': len(self._endpoints),
                'healthy': sum(1 for e in self._endpoints.values() if e.is_healthy),
                'strategy': self._strategy.value,
                'rules': len(self._rules),
                'endpoints': {
                    eid: {
                        'url': e.url,
                        'load': e.current_load,
                        'latency': e.avg_latency,
                        'healthy': e.is_healthy
                    }
                    for eid, e in self._endpoints.items()
                }
            }


class LoadBalancerRouter(AdaptiveRouter):
    """
    Load balancer with health checking.
    """

    def __init__(self, health_check_interval: int = 30):
        super().__init__()
        self.health_check_interval = health_check_interval
        self._health_check_thread: threading.Thread = None
        self._running = False
        self._health_check_urls: Dict[str, str] = {}  # endpoint_id -> health check URL

    def add_endpoint_with_health_check(self, endpoint: Endpoint, health_check_url: str = None):
        """Add endpoint with health check URL."""
        self.add_endpoint(endpoint)
        if health_check_url:
            self._health_check_urls[endpoint.endpoint_id] = health_check_url

    def start_health_checks(self):
        """Start background health checks."""
        self._running = True
        self._health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self._health_check_thread.start()

    def stop_health_checks(self):
        """Stop health checks."""
        self._running = False
        if self._health_check_thread:
            self._health_check_thread.join(timeout=5)

    def _health_check_loop(self):
        """Background health check loop."""
        while self._running:
            for endpoint_id, url in list(self._health_check_urls.items()):
                try:
                    import urllib.request
                    with urllib.request.urlopen(url, timeout=5) as response:
                        if response.status == 200:
                            self.update_endpoint_stats(endpoint_id, error=False)
                        else:
                            self.update_endpoint_stats(endpoint_id, error=True)
                except Exception:
                    self.update_endpoint_stats(endpoint_id, error=True)

            time.sleep(self.health_check_interval)


class ServiceMeshRouter:
    """
    Service mesh compatible router.
    Supports sidecar-style routing.
    """

    def __init__(self):
        self._router = AdaptiveRouter()
        self._services: Dict[str, List[Endpoint]] = defaultdict(list)
        self._lock = threading.RLock()

    def register_service(self, service_name: str, endpoint: Endpoint):
        """Register a service endpoint."""
        with self._lock:
            self._services[service_name].append(endpoint)
            self._router.add_endpoint(endpoint)

    def route_to_service(self, service_name: str,
                        request: Dict = None) -> RouteResult:
        """Route request to a service."""
        with self._lock:
            endpoints = self._services.get(service_name, [])

        if not endpoints:
            raise ValueError(f"Service {service_name} not found")

        return self._router.route(request)

    def get_service_endpoints(self, service_name: str) -> List[Endpoint]:
        """Get endpoints for a service."""
        with self._lock:
            return list(self._services.get(service_name, []))


# Global router instance
_router = AdaptiveRouter()


def get_router() -> AdaptiveRouter:
    return _router


def create_load_balancer(health_check_interval: int = 30) -> LoadBalancerRouter:
    """Create a load balancer router."""
    return LoadBalancerRouter(health_check_interval)


def create_service_mesh_router() -> ServiceMeshRouter:
    """Create a service mesh router."""
    return ServiceMeshRouter()