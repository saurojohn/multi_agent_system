"""Quota management for resource usage control."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('quota')


class QuotaScope(Enum):
    """Scope of quota limits."""
    GLOBAL = "global"       # System-wide
    TENANT = "tenant"       # Per-tenant
    USER = "user"          # Per-user
    WORKER = "worker"      # Per-worker


@dataclass
class QuotaLimit:
    """A quota limit definition."""
    resource: str
    limit: int
    window_seconds: int = 60  # Time window for limit
    burst: int = 0            # Allow burst up to this amount
    block_duration: int = 60   # How long to block when exceeded


@dataclass
class QuotaUsage:
    """Current quota usage."""
    consumed: int
    remaining: int
    reset_at: float
    blocked: bool = False
    blocked_until: float = 0


class QuotaManager:
    """
    Manages resource quotas across different scopes.
    """

    def __init__(self):
        self._quotas: Dict[str, Dict[str, QuotaLimit]] = {}  # scope -> resource -> limit
        self._usage: Dict[str, Dict[str, List[float]]] = {}   # scope:id -> resource -> timestamps
        self._blocked: Dict[str, float] = {}  # scope:id:resource -> blocked_until
        self._lock = threading.RLock()
        self._handlers: Dict[str, Callable] = {}  # When quota exceeded

    def set_limit(self, scope: str, scope_id: str, resource: str, limit: int,
                  window_seconds: int = 60, burst: int = 0):
        """Set a quota limit for a resource."""
        key = f"{scope}:{scope_id}"
        if key not in self._quotas:
            self._quotas[key] = {}

        self._quotas[key][resource] = QuotaLimit(
            resource=resource,
            limit=limit,
            window_seconds=window_seconds,
            burst=burst
        )
        logger.info(f"Set quota: {scope}/{scope_id}/{resource} = {limit}/{window_seconds}s")

    def get_usage(self, scope: str, scope_id: str, resource: str) -> QuotaUsage:
        """Get current quota usage for a resource."""
        key = f"{scope}:{scope_id}"
        now = time.time()

        # Check if blocked
        blocked_key = f"{key}:{resource}"
        blocked_until = self._blocked.get(blocked_key, 0)
        if blocked_until > now:
            return QuotaUsage(
                consumed=0,
                remaining=0,
                reset_at=blocked_until,
                blocked=True,
                blocked_until=blocked_until
            )

        # Get limit
        limit = self._get_limit(scope, scope_id, resource)
        if not limit:
            return QuotaUsage(consumed=0, remaining=float('inf'), reset_at=now + 3600)

        # Get usage within window
        usage_key = f"{key}:{resource}"
        if usage_key not in self._usage:
            self._usage[usage_key] = []

        # Clean old entries
        cutoff = now - limit.window_seconds
        self._usage[usage_key] = [t for t in self._usage[usage_key] if t > cutoff]

        consumed = len(self._usage[usage_key])
        remaining = max(0, limit.limit - consumed)
        reset_at = now + limit.window_seconds

        return QuotaUsage(
            consumed=consumed,
            remaining=remaining,
            reset_at=reset_at,
            blocked=False
        )

    def check_and_consume(self, scope: str, scope_id: str,
                         resource: str, amount: int = 1) -> bool:
        """
        Check if quota available and consume if so.
        Returns True if allowed, False if quota exceeded.
        """
        usage = self.get_usage(scope, scope_id, resource)

        if usage.blocked:
            return False

        if usage.remaining < amount:
            # Quota exceeded - block
            self._block(scope, scope_id, resource)
            return False

        # Consume
        key = f"{scope}:{scope_id}:{resource}"
        if key not in self._usage:
            self._usage[key] = []

        self._usage[key].append(time.time())
        return True

    def _get_limit(self, scope: str, scope_id: str, resource: str) -> Optional[QuotaLimit]:
        """Get quota limit for resource."""
        key = f"{scope}:{scope_id}"
        if key not in self._quotas:
            return None
        return self._quotas[key].get(resource)

    def _block(self, scope: str, scope_id: str, resource: str):
        """Block a resource for the configured duration."""
        key = f"{scope}:{scope_id}:{resource}"
        limit = self._get_limit(scope, scope_id, resource)
        duration = limit.block_duration if limit else 60

        self._blocked[key] = time.time() + duration
        logger.warning(f"Quota exceeded, blocking {key} for {duration}s")

        # Call handler if registered
        handler_key = f"{scope}:{scope_id}:{resource}"
        if handler_key in self._handlers:
            try:
                self._handlers[handler_key](scope, scope_id, resource)
            except Exception as e:
                logger.error(f"Quota handler failed: {e}")

    def set_quota_exceeded_handler(self, scope: str, scope_id: str,
                                    resource: str, handler: Callable):
        """Set handler for when quota is exceeded."""
        key = f"{scope}:{scope_id}:{resource}"
        self._handlers[key] = handler

    def reset_usage(self, scope: str, scope_id: str, resource: str = None):
        """Reset usage for a scope."""
        key_prefix = f"{scope}:{scope_id}"
        if resource:
            key = f"{key_prefix}:{resource}"
            if key in self._usage:
                self._usage[key] = []
            if key in self._blocked:
                del self._blocked[key]
        else:
            # Reset all resources for this scope:id
            keys_to_remove = [k for k in self._usage if k.startswith(key_prefix)]
            for k in keys_to_remove:
                del self._usage[k]
            keys_to_remove = [k for k in self._blocked if k.startswith(key_prefix)]
            for k in keys_to_remove:
                del self._blocked[k]

        logger.info(f"Reset quota usage: {scope}/{scope_id}" + (f"/{resource}" if resource else ""))

    def get_stats(self) -> Dict:
        """Get quota manager statistics."""
        with self._lock:
            return {
                'scopes': len(self._quotas),
                'tracked_usages': len(self._usage),
                'blocked': len(self._blocked)
            }


class RateQuotaLimiter:
    """
    Rate-based quota limiter using token bucket.
    """

    def __init__(self, rate: int, capacity: int, refill_rate: float = 1.0):
        self.rate = rate
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def _refill(self):
        """Refill tokens based on time elapsed."""
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def get_remaining(self) -> float:
        """Get remaining tokens."""
        with self._lock:
            self._refill()
            return self._tokens


class QuotaInterceptor:
    """
    Intercepts requests to enforce quotas.
    """

    def __init__(self, quota_manager: QuotaManager):
        self.quota_manager = quota_manager
        self._interceptors: Dict[str, Callable] = {}

    def register_resource(self, scope: str, scope_id: str,
                         resource: str, limit: int, **config):
        """Register a resource with quota."""
        self.quota_manager.set_limit(scope, scope_id, resource, limit, **config)

    def check(self, scope: str, scope_id: str, resource: str) -> tuple:
        """
        Check if request is allowed.
        Returns (allowed, usage).
        """
        allowed = self.quota_manager.check_and_consume(scope, scope_id, resource)
        usage = self.quota_manager.get_usage(scope, scope_id, resource)
        return allowed, usage

    def add_interceptor(self, scope: str, scope_id: str,
                       resource: str, interceptor: Callable):
        """Add an interceptor for when quota is exceeded."""
        key = f"{scope}:{scope_id}:{resource}"
        self._interceptors[key] = interceptor

    def intercept(self, scope: str, scope_id: str, resource: str) -> bool:
        """Run interceptor for exceeded quota."""
        key = f"{scope}:{scope_id}:{resource}"
        interceptor = self._interceptors.get(key)
        if interceptor:
            try:
                interceptor(scope, scope_id, resource)
                return True
            except Exception as e:
                logger.error(f"Interceptor failed: {e}")
        return False


# Global quota manager
_quota_manager = QuotaManager()


def get_quota_manager() -> QuotaManager:
    return _quota_manager


def create_rate_limiter(rate: int, capacity: int) -> RateQuotaLimiter:
    """Create a rate-based quota limiter."""
    return RateQuotaLimiter(rate, capacity)