"""Multi-tenancy support for multi-agent system."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger('tenant')


@dataclass
class Tenant:
    """Represents an organization/tenant."""
    tenant_id: str
    name: str
    quota: Dict = field(default_factory=dict)  # max_workers, max_tasks, etc.
    settings: Dict = field(default_factory=dict)  # custom settings
    created_at: float = field(default_factory=time.time)
    is_active: bool = True


@dataclass
class TenantContext:
    """Context for current tenant."""
    tenant_id: str
    user_id: Optional[str] = None
    roles: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)


class TenantManager:
    """Manages multiple tenants with resource isolation."""

    def __init__(self):
        self._tenants: Dict[str, Tenant] = {}
        self._context: Optional[TenantContext] = None
        self._lock = threading.RLock()

    def create_tenant(self, name: str, quota: Dict = None) -> str:
        """Create a new tenant."""
        tenant_id = f"tenant-{uuid.uuid4().hex[:8]}"

        default_quota = {
            'max_workers': 50,
            'max_tasks': 1000,
            'max_concurrent_tasks': 100,
            'rate_limit': 1000  # requests per minute
        }
        if quota:
            default_quota.update(quota)

        tenant = Tenant(
            tenant_id=tenant_id,
            name=name,
            quota=default_quota
        )

        with self._lock:
            self._tenants[tenant_id] = tenant

        logger.info(f'Created tenant: {name} ({tenant_id})')
        return tenant_id

    def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant by ID."""
        with self._lock:
            return self._tenants.get(tenant_id)

    def delete_tenant(self, tenant_id: str) -> bool:
        """Delete a tenant."""
        with self._lock:
            if tenant_id in self._tenants:
                del self._tenants[tenant_id]
                logger.info(f'Deleted tenant: {tenant_id}')
                return True
        return False

    def update_quota(self, tenant_id: str, quota: Dict) -> bool:
        """Update tenant quota."""
        with self._lock:
            if tenant_id in self._tenants:
                self._tenants[tenant_id].quota.update(quota)
                logger.info(f'Updated quota for tenant: {tenant_id}')
                return True
        return False

    def set_context(self, tenant_id: str, user_id: str = None, roles: List[str] = None):
        """Set current tenant context."""
        self._context = TenantContext(
            tenant_id=tenant_id,
            user_id=user_id,
            roles=roles or []
        )
        logger.debug(f'Set tenant context: {tenant_id}')

    def clear_context(self):
        """Clear current tenant context."""
        self._context = None

    def get_context(self) -> Optional[TenantContext]:
        """Get current tenant context."""
        return self._context

    def check_quota(self, tenant_id: str, resource: str, current: int) -> bool:
        """Check if tenant is within quota for a resource."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return True  # Allow if no quota set

        limit = tenant.quota.get(f'max_{resource}', 0)
        if limit <= 0:
            return True  # No limit

        return current < limit

    def get_all_tenants(self) -> List[Dict]:
        """Get all tenants as dict."""
        with self._lock:
            return [
                {
                    'tenant_id': t.tenant_id,
                    'name': t.name,
                    'quota': t.quota,
                    'is_active': t.is_active
                }
                for t in self._tenants.values()
            ]

    def isolate_queue(self, tenant_id: str) -> str:
        """Get tenant-isolated queue name."""
        return f"tenant:{tenant_id}"

    def isolate_worker(self, worker_id: str, tenant_id: str) -> str:
        """Get tenant-isolated worker ID."""
        return f"{tenant_id}:{worker_id}"


# Global tenant manager
_tenant_manager = TenantManager()


def get_tenant_manager() -> TenantManager:
    return _tenant_manager


def require_tenant(f):
    """Decorator to require tenant context."""
    def wrapper(self, *args, **kwargs):
        ctx = get_tenant_manager().get_context()
        if not ctx:
            return {'error': 'Tenant context required', 'status': 401}
        return f(self, *args, **kwargs)
    return wrapper