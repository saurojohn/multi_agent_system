"""Idempotency handling for preventing duplicate operations."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field

logger = logging.getLogger('idempotency')


@dataclass
class IdempotencyRecord:
    """Record of an idempotent operation."""
    key: str
    operation_id: str
    status: str  # "pending", "completed", "failed"
    created_at: float = field(default_factory=time.time)
    result: Any = None
    completed_at: Optional[float] = None
    ttl_seconds: int = 3600


class IdempotencyStore:
    """
    Store for idempotency records.
    """

    def __init__(self):
        self._records: Dict[str, IdempotencyRecord] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[IdempotencyRecord]:
        """Get a record by key."""
        with self._lock:
            return self._records.get(key)

    def set(self, record: IdempotencyRecord) -> bool:
        """Set a record."""
        with self._lock:
            self._records[record.key] = record
            return True

    def update(self, key: str, status: str, result: Any = None):
        """Update a record's status."""
        with self._lock:
            if key in self._records:
                self._records[key].status = status
                self._records[key].completed_at = time.time()
                if result is not None:
                    self._records[key].result = result

    def delete(self, key: str) -> bool:
        """Delete a record."""
        with self._lock:
            if key in self._records:
                del self._records[key]
                return True
            return False

    def cleanup_expired(self, ttl_seconds: int = 3600) -> int:
        """Clean up expired records."""
        now = time.time()
        removed = 0

        with self._lock:
            expired = [
                key for key, record in self._records.items()
                if now - record.created_at > ttl_seconds
            ]

            for key in expired:
                del self._records[key]
                removed += 1

        return removed


class IdempotencyKeyGenerator:
    """Generates idempotency keys."""

    @staticmethod
    def from_request(method: str, path: str, body: Any = None) -> str:
        """Generate key from request components."""
        import hashlib
        import json

        parts = [method.upper(), path]
        if body:
            body_str = json.dumps(body, sort_keys=True) if isinstance(body, dict) else str(body)
            parts.append(hashlib.md5(body_str.encode()).hexdigest()[:16])

        combined = "|".join(parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:32]

    @staticmethod
    def from_operation(operation_name: str, *args, **kwargs) -> str:
        """Generate key from operation name and arguments."""
        import hashlib
        import json

        key_parts = [operation_name]
        key_parts.extend(str(a) for a in args)
        key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))

        combined = "|".join(key_parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:32]


class IdempotencyHandler:
    """
    Handles idempotent operations.
    """

    def __init__(self, store: IdempotencyStore = None):
        self.store = store or IdempotencyStore()
        self._default_ttl = 3600
        self._running = False
        self._cleanup_thread: threading.Thread = None

    def start(self):
        """Start the handler."""
        self._running = True
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()

    def stop(self):
        """Stop the handler."""
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)

    def _cleanup_loop(self):
        """Background cleanup loop."""
        while self._running:
            time.sleep(300)  # Run cleanup every 5 minutes
            removed = self.store.cleanup_expired(self._default_ttl)
            if removed:
                logger.debug(f"Cleaned up {removed} expired idempotency records")

    def check(self, key: str) -> tuple:
        """
        Check if operation was already executed.
        Returns (already_exists, operation_id, result).
        """
        record = self.store.get(key)

        if record:
            if record.status == "completed":
                return True, record.operation_id, record.result
            elif record.status == "pending":
                return True, record.operation_id, None  # In progress

        return False, None, None

    def begin(self, key: str, ttl_seconds: int = None) -> str:
        """Begin an idempotent operation."""
        ttl = ttl_seconds or self._default_ttl
        operation_id = str(uuid.uuid4())

        record = IdempotencyRecord(
            key=key,
            operation_id=operation_id,
            status="pending",
            created_at=time.time(),
            ttl_seconds=ttl
        )

        self.store.set(record)
        return operation_id

    def complete(self, key: str, result: Any = None):
        """Mark operation as completed."""
        self.store.update(key, "completed", result)

    def fail(self, key: str, error: str = None):
        """Mark operation as failed."""
        self.store.update(key, "failed", error)

    def execute(self, key: str, operation: Callable,
               ttl_seconds: int = None) -> tuple:
        """
        Execute operation with idempotency protection.
        Returns (result, is_duplicate, operation_id).
        """
        # Check if already executed
        exists, op_id, cached_result = self.check(key)
        if exists:
            return cached_result, True, op_id

        # Begin new operation
        operation_id = self.begin(key, ttl_seconds)

        try:
            result = operation()
            self.complete(key, result)
            return result, False, operation_id
        except Exception as e:
            self.fail(key, str(e))
            raise


class IdempotentOperation:
    """
    Wrapper for making operations idempotent.
    """

    def __init__(self, handler: IdempotencyHandler):
        self.handler = handler

    def execute(self, key: str, operation: Callable, **kwargs) -> Any:
        """Execute operation with idempotency."""
        result, _, _ = self.handler.execute(key, operation, **kwargs)
        return result


class IdempotencyMiddleware:
    """
    Middleware for making HTTP endpoints idempotent.
    """

    def __init__(self, handler: IdempotencyHandler):
        self.handler = handler

    def wrap_handler(self, handler: Callable) -> Callable:
        """Wrap an HTTP handler with idempotency."""
        def wrapped(request_data: Dict, **kwargs) -> Any:
            # Extract or generate idempotency key
            key = request_data.get('idempotency_key')
            if not key:
                method = request_data.get('method', 'POST')
                path = request_data.get('path', '/')
                body = request_data.get('body')
                key = IdempotencyKeyGenerator.from_request(method, path, body)

            # Execute with idempotency
            def operation():
                return handler(request_data, **kwargs)

            result, is_duplicate, op_id = self.handler.execute(key, operation)

            return {
                'result': result,
                'idempotent': not is_duplicate,
                'operation_id': op_id
            }

        return wrapped


# Global handler
_idempotency_handler = IdempotencyHandler()


def get_idempotency_handler() -> IdempotencyHandler:
    return _idempotency_handler


def idempotent(operation: Callable = None, key_generator: Callable = None):
    """
    Decorator for making operations idempotent.
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            handler = _idempotency_handler

            # Generate key
            if key_generator:
                key = key_generator(*args, **kwargs)
            else:
                key = IdempotencyKeyGenerator.from_operation(func.__name__, *args, **kwargs)

            def operation():
                return func(*args, **kwargs)

            result, _, _ = handler.execute(key, operation)
            return result

        return wrapper

    if operation:
        return decorator(operation)

    return decorator