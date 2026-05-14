"""Service catalog for managing service metadata and dependencies."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('service_catalog')


class ServiceStatus(Enum):
    """Service status values."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"
    EXPERIMENTAL = "experimental"


class ServiceTier(Enum):
    """Service tier levels."""
    GOLD = "gold"       # Fully supported
    SILVER = "silver"   # Standard support
    BRONZE = "bronze"   # Basic support


@dataclass
class ServiceDependency:
    """A service dependency."""
    service_name: str
    version_range: str  # e.g., ">=1.0.0,<2.0.0"
    required: bool = True
    metadata: Dict = field(default_factory=dict)


@dataclass
class ServiceEndpoint:
    """A service endpoint definition."""
    url: str
    type: str  # "http", "grpc", "ws"
    port: int = None
    path: str = None
    metadata: Dict = field(default_factory=dict)


@dataclass
class ServiceMetadata:
    """Service metadata."""
    name: str
    version: str
    description: str = ""
    owner: str = ""
    team: str = ""
    tags: List[str] = field(default_factory=list)
    tier: ServiceTier = ServiceTier.SILVER
    dependencies: List[ServiceDependency] = field(default_factory=list)
    endpoints: List[ServiceEndpoint] = field(default_factory=list)
    health_check_url: str = None
    documentation_url: str = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict = field(default_factory=dict)


@dataclass
class ServiceRecord:
    """A registered service."""
    metadata: ServiceMetadata
    status: ServiceStatus = ServiceStatus.ACTIVE
    last_heartbeat: float = field(default_factory=time.time)
    instance_id: str = None


class ServiceCatalog:
    """
    Service catalog for managing service information.
    """

    def __init__(self):
        self._services: Dict[str, ServiceRecord] = {}
        self._lock = threading.RLock()
        self._version_cache: Dict[str, List[str]] = {}

    def register(self, metadata: ServiceMetadata, instance_id: str = None) -> ServiceRecord:
        """Register a service."""
        with self._lock:
            record = ServiceRecord(
                metadata=metadata,
                instance_id=instance_id or f"{metadata.name}_{int(time.time())}"
            )
            self._services[metadata.name] = record
            logger.info(f"Registered service: {metadata.name} ({metadata.version})")
            return record

    def unregister(self, service_name: str) -> bool:
        """Unregister a service."""
        with self._lock:
            if service_name in self._services:
                del self._services[service_name]
                logger.info(f"Unregistered service: {service_name}")
                return True
        return False

    def get(self, service_name: str) -> Optional[ServiceRecord]:
        """Get a service by name."""
        with self._lock:
            return self._services.get(service_name)

    def get_all(self) -> List[ServiceRecord]:
        """Get all registered services."""
        with self._lock:
            return list(self._services.values())

    def update_heartbeat(self, service_name: str):
        """Update service heartbeat."""
        with self._lock:
            if service_name in self._services:
                self._services[service_name].last_heartbeat = time.time()

    def update_status(self, service_name: str, status: ServiceStatus):
        """Update service status."""
        with self._lock:
            if service_name in self._services:
                self._services[service_name].status = status

    def find_by_tag(self, tag: str) -> List[ServiceRecord]:
        """Find services by tag."""
        with self._lock:
            return [
                record for record in self._services.values()
                if tag in record.metadata.tags
            ]

    def find_by_tier(self, tier: ServiceTier) -> List[ServiceRecord]:
        """Find services by tier."""
        with self._lock:
            return [
                record for record in self._services.values()
                if record.metadata.tier == tier
            ]

    def find_by_team(self, team: str) -> List[ServiceRecord]:
        """Find services by team."""
        with self._lock:
            return [
                record for record in self._services.values()
                if record.metadata.team == team
            ]

    def get_active_services(self) -> List[ServiceRecord]:
        """Get all active services."""
        with self._lock:
            return [
                record for record in self._services.values()
                if record.status == ServiceStatus.ACTIVE
            ]

    def get_dependencies(self, service_name: str) -> List[ServiceDependency]:
        """Get service dependencies."""
        record = self.get(service_name)
        if record:
            return record.metadata.dependencies
        return []

    def check_dependencies(self, service_name: str) -> tuple:
        """
        Check if all dependencies are satisfied.
        Returns (satisfied, missing_dependencies).
        """
        dependencies = self.get_dependencies(service_name)
        missing = []

        for dep in dependencies:
            if not dep.required:
                continue

            service = self.get(dep.service_name)
            if not service or service.status != ServiceStatus.ACTIVE:
                missing.append(dep.service_name)

        return len(missing) == 0, missing

    def add_endpoint(self, service_name: str, endpoint: ServiceEndpoint):
        """Add an endpoint to a service."""
        with self._lock:
            if service_name in self._services:
                self._services[service_name].metadata.endpoints.append(endpoint)

    def get_endpoint(self, service_name: str, endpoint_type: str = None) -> Optional[ServiceEndpoint]:
        """Get a service endpoint."""
        record = self.get(service_name)
        if not record:
            return None

        for endpoint in record.metadata.endpoints:
            if endpoint_type is None or endpoint.type == endpoint_type:
                return endpoint

        return None

    def search(self, query: str) -> List[ServiceRecord]:
        """Search services by name or description."""
        query_lower = query.lower()
        with self._lock:
            return [
                record for record in self._services.values()
                if query_lower in record.metadata.name.lower() or
                   query_lower in record.metadata.description.lower()
            ]

    def get_stats(self) -> Dict:
        """Get catalog statistics."""
        with self._lock:
            return {
                'total_services': len(self._services),
                'active': sum(1 for r in self._services.values() if r.status == ServiceStatus.ACTIVE),
                'by_tier': {
                    tier.value: sum(1 for r in self._services.values() if r.metadata.tier == tier)
                    for tier in ServiceTier
                },
                'by_team': {
                    team: sum(1 for r in self._services.values() if r.metadata.team == team)
                    for team in set(r.metadata.team for r in self._services.values())
                }
            }


class ServiceRegistry:
    """
    Service registry with service discovery integration.
    """

    def __init__(self, catalog: ServiceCatalog = None):
        self.catalog = catalog or ServiceCatalog()
        self._discovery_handlers: List[Callable] = []

    def register_service(self, name: str, version: str, **metadata) -> ServiceRecord:
        """Register a service with metadata."""
        meta = ServiceMetadata(name=name, version=version, **metadata)
        return self.catalog.register(meta)

    def discover_service(self, service_name: str) -> Optional[ServiceEndpoint]:
        """Discover a service endpoint."""
        return self.catalog.get_endpoint(service_name, "http")

    def add_discovery_handler(self, handler: Callable):
        """Add a discovery handler."""
        self._discovery_handlers.append(handler)

    def notify_discovery(self, service_name: str):
        """Notify discovery handlers of service update."""
        for handler in self._discovery_handlers:
            try:
                handler(service_name)
            except Exception as e:
                logger.error(f"Discovery handler failed: {e}")


class ServiceCatalogBuilder:
    """Builder for creating service catalog entries."""

    def __init__(self, name: str, version: str):
        self._metadata = ServiceMetadata(name=name, version=version)

    def description(self, desc: str) -> 'ServiceCatalogBuilder':
        self._metadata.description = desc
        return self

    def owner(self, owner: str) -> 'ServiceCatalogBuilder':
        self._metadata.owner = owner
        return self

    def team(self, team: str) -> 'ServiceCatalogBuilder':
        self._metadata.team = team
        return self

    def tier(self, tier: ServiceTier) -> 'ServiceCatalogBuilder':
        self._metadata.tier = tier
        return self

    def add_tag(self, tag: str) -> 'ServiceCatalogBuilder':
        self._metadata.tags.append(tag)
        return self

    def add_endpoint(self, url: str, endpoint_type: str = "http",
                    port: int = None, path: str = None) -> 'ServiceCatalogBuilder':
        endpoint = ServiceEndpoint(
            url=url,
            type=endpoint_type,
            port=port,
            path=path
        )
        self._metadata.endpoints.append(endpoint)
        return self

    def add_dependency(self, service_name: str, version_range: str,
                      required: bool = True) -> 'ServiceCatalogBuilder':
        dep = ServiceDependency(
            service_name=service_name,
            version_range=version_range,
            required=required
        )
        self._metadata.dependencies.append(dep)
        return self

    def build(self) -> ServiceMetadata:
        """Build the service metadata."""
        self._metadata.updated_at = time.time()
        return self._metadata


# Global catalog
_catalog = ServiceCatalog()


def get_service_catalog() -> ServiceCatalog:
    return _catalog


def register_service(name: str, version: str, **kwargs) -> ServiceRecord:
    """Register a service."""
    meta = ServiceMetadata(name=name, version=version, **kwargs)
    return _catalog.register(meta)


def discover_service(service_name: str) -> Optional[ServiceEndpoint]:
    """Discover a service."""
    return _catalog.get_endpoint(service_name)