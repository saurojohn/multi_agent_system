"""Service mesh integration for multi-agent communication."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('mesh')


class ServiceStatus(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ServiceEndpoint:
    """A service endpoint."""
    name: str
    host: str
    port: int
    weight: int = 1
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class ServiceNode:
    """A node in the service mesh."""
    node_id: str
    name: str
    endpoints: List[ServiceEndpoint] = field(default_factory=list)
    status: ServiceStatus = ServiceStatus.UNKNOWN
    last_heartbeat: float = field(default_factory=time.time)


class ServiceRegistry:
    """Registry for service discovery."""

    def __init__(self):
        self._services: Dict[str, List[ServiceEndpoint]] = {}
        self._nodes: Dict[str, ServiceNode] = {}
        self._lock = threading.RLock()

    def register_service(self, name: str, endpoint: ServiceEndpoint):
        """Register a service endpoint."""
        with self._lock:
            if name not in self._services:
                self._services[name] = []
            self._services[name].append(endpoint)
            logger.info(f"Registered service: {name} -> {endpoint.host}:{endpoint.port}")

    def unregister_service(self, name: str, endpoint: ServiceEndpoint):
        """Unregister a service endpoint."""
        with self._lock:
            if name in self._services:
                self._services[name] = [
                    e for e in self._services[name]
                    if not (e.host == endpoint.host and e.port == endpoint.port)
                ]

    def get_endpoints(self, name: str) -> List[ServiceEndpoint]:
        """Get endpoints for a service."""
        with self._lock:
            return list(self._services.get(name, []))

    def register_node(self, node: ServiceNode):
        """Register a mesh node."""
        with self._lock:
            self._nodes[node.node_id] = node

    def get_node(self, node_id: str) -> Optional[ServiceNode]:
        """Get a node by ID."""
        return self._nodes.get(node_id)

    def get_all_nodes(self) -> List[ServiceNode]:
        """Get all registered nodes."""
        return list(self._nodes.values())


class LoadBalancer:
    """Load balancing strategies."""

    def __init__(self, strategy: str = "round_robin"):
        self.strategy = strategy
        self._counters: Dict[str, int] = {}

    def select(self, endpoints: List[ServiceEndpoint]) -> Optional[ServiceEndpoint]:
        """Select an endpoint based on strategy."""
        if not endpoints:
            return None

        if self.strategy == "round_robin":
            return self._round_robin(endpoints)
        elif self.strategy == "weighted":
            return self._weighted_select(endpoints)
        elif self.strategy == "random":
            import random
            return random.choice(endpoints)
        elif self.strategy == "least_connections":
            return self._least_connections(endpoints)
        else:
            return endpoints[0]

    def _round_robin(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Round robin selection."""
        key = id(endpoints)
        if key not in self._counters:
            self._counters[key] = 0
        self._counters[key] = (self._counters[key] + 1) % len(endpoints)
        return endpoints[self._counters[key]]

    def _weighted_select(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Weighted selection."""
        total_weight = sum(e.weight for e in endpoints)
        if total_weight == 0:
            return endpoints[0]
        import random
        r = random.randint(1, total_weight)
        cumulative = 0
        for e in endpoints:
            cumulative += e.weight
            if r <= cumulative:
                return e
        return endpoints[-1]

    def _least_connections(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        """Least connections (placeholder)."""
        return endpoints[0]


class MeshRouter:
    """Routes requests through the service mesh."""

    def __init__(self, registry: ServiceRegistry = None):
        self.registry = registry or ServiceRegistry()
        self.load_balancer = LoadBalancer()
        self._middlewares: List[Callable] = []

    def add_middleware(self, middleware: Callable):
        """Add a middleware function."""
        self._middlewares.append(middleware)

    def route(self, service_name: str, request: Dict) -> Any:
        """Route a request to a service."""
        endpoints = self.registry.get_endpoints(service_name)
        if not endpoints:
            raise Exception(f"No endpoints for service: {service_name}")

        endpoint = self.load_balancer.select(endpoints)
        if not endpoint:
            raise Exception("No endpoint selected")

        ctx = {'request': request, 'endpoint': endpoint}
        for mw in self._middlewares:
            ctx = mw(ctx)
            if ctx.get('stop'):
                return ctx.get('response')

        return self._make_request(endpoint, ctx['request'])

    def _make_request(self, endpoint: ServiceEndpoint, request: Dict) -> Any:
        """Make request to endpoint."""
        return {'status': 'forwarded', 'to': f"{endpoint.host}:{endpoint.port}"}


class ServiceMesh:
    """Main service mesh orchestrator."""

    def __init__(self, registry: ServiceRegistry = None):
        self.registry = registry or ServiceRegistry()
        self.router = MeshRouter(self.registry)
        self._policies: Dict[str, Any] = {}

    def add_policy(self, name: str, policy: Any):
        """Add a mesh policy."""
        self._policies[name] = policy

    def get_mesh_stats(self) -> Dict:
        """Get mesh statistics."""
        return {
            'services': len(self.registry._services),
            'nodes': len(self.registry._nodes),
            'policies': len(self._policies)
        }


# Global mesh instance
_mesh = ServiceMesh()


def get_mesh() -> ServiceMesh:
    """Get global mesh instance."""
    return _mesh


def register_service(name: str, host: str, port: int, **kwargs):
    """Register a service in the mesh."""
    endpoint = ServiceEndpoint(name=name, host=host, port=port, **kwargs)
    _mesh.registry.register_service(name, endpoint)


def get_service_endpoints(name: str) -> List[ServiceEndpoint]:
    """Get endpoints for a service."""
    return _mesh.registry.get_endpoints(name)