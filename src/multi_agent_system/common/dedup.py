"""Request deduplication to prevent duplicate processing."""

import hashlib
import json
import logging
import threading
import time
from typing import Dict, Optional, Any, Callable, List
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger('dedup')


@dataclass
class DedupConfig:
    """Configuration for deduplication."""
    ttl_seconds: int = 3600  # Time to live for dedup keys
    max_entries: int = 100000  # Maximum entries to store
    cleanup_interval: int = 300  # Cleanup interval in seconds
    enabled: bool = True


@dataclass
class DedupEntry:
    """An entry in the deduplication store."""
    key: str
    request_id: str
    created_at: float
    expires_at: float
    metadata: Dict = field(default_factory=dict)


class DedupKeyBuilder:
    """Builds deduplication keys from request data."""

    @staticmethod
    def build(task_type: str, task_data: Dict,
              include_priority: bool = False) -> str:
        """Build a dedup key from task type and data."""
        data_str = json.dumps(task_data, sort_keys=True)
        key_parts = [task_type, data_str]

        if include_priority:
            key_parts.append(str(task_data.get('priority', 0)))

        combined = ":".join(key_parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    @staticmethod
    def build_from_request(method: str, path: str,
                           params: Dict = None,
                           body: Dict = None) -> str:
        """Build a dedup key from an HTTP request."""
        parts = [method.upper(), path]

        if params:
            sorted_params = sorted(params.items())
            parts.append(json.dumps(sorted_params, sort_keys=True))

        if body:
            parts.append(json.dumps(body, sort_keys=True))

        combined = "|".join(parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]


class RequestDeduplicator:
    """
    Deduplicates requests to prevent duplicate processing.
    Uses in-memory storage with TTL support.
    """

    def __init__(self, config: DedupConfig = None):
        self.config = config or DedupConfig()
        self._store: Dict[str, DedupEntry] = {}
        self._lock = threading.RLock()
        self._cleanup_thread: Optional[threading.Thread] = None
        self._running = False
        self._hit_count = 0
        self._miss_count = 0

    def start(self):
        """Start the deduplicator."""
        if not self.config.enabled:
            return

        self._running = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True
        )
        self._cleanup_thread.start()
        logger.info("Request deduplicator started")

    def stop(self):
        """Stop the deduplicator."""
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
        logger.info("Request deduplicator stopped")

    def check(self, key: str) -> Optional[str]:
        """
        Check if a request with this key was already seen.
        Returns the original request_id if duplicate, None otherwise.
        """
        if not self.config.enabled:
            return None

        with self._lock:
            entry = self._store.get(key)

            if entry and entry.expires_at > time.time():
                self._hit_count += 1
                return entry.request_id

            # Clean up expired entry
            if entry:
                del self._store[key]

            self._miss_count += 1
            return None

    def record(self, key: str, request_id: str = None,
               metadata: Dict = None) -> str:
        """
        Record a new request.
        Returns the request_id.
        """
        if not self.config.enabled:
            return request_id or "disabled"

        request_id = request_id or f"req_{int(time.time() * 1000)}"

        with self._lock:
            # Enforce max entries
            if len(self._store) >= self.config.max_entries:
                self._cleanup_expired()

            now = time.time()
            entry = DedupEntry(
                key=key,
                request_id=request_id,
                created_at=now,
                expires_at=now + self.config.ttl_seconds,
                metadata=metadata or {}
            )
            self._store[key] = entry

        return request_id

    def is_duplicate(self, key: str) -> bool:
        """Check if a request is a duplicate (already exists)."""
        return self.check(key) is not None

    def mark_processing(self, key: str) -> bool:
        """Mark a key as currently being processed."""
        # Extend the TTL while processing
        with self._lock:
            if key in self._store:
                self._store[key].expires_at = time.time() + self.config.ttl_seconds
                return True
            return False

    def mark_completed(self, key: str):
        """Mark a key as completed (optional early removal)."""
        with self._lock:
            if key in self._store:
                del self._store[key]

    def _cleanup_loop(self):
        """Background cleanup loop."""
        while self._running:
            time.sleep(self.config.cleanup_interval)
            self._cleanup_expired()

    def _cleanup_expired(self):
        """Remove expired entries."""
        now = time.time()
        expired = [
            key for key, entry in self._store.items()
            if entry.expires_at <= now
        ]

        for key in expired:
            del self._store[key]

        if expired:
            logger.debug(f"Cleaned up {len(expired)} expired dedup entries")

    def get_stats(self) -> Dict:
        """Get deduplicator statistics."""
        with self._lock:
            total = self._hit_count + self._miss_count
            hit_rate = self._hit_count / total if total > 0 else 0

            return {
                'enabled': self.config.enabled,
                'entries': len(self._store),
                'max_entries': self.config.max_entries,
                'hits': self._hit_count,
                'misses': self._miss_count,
                'hit_rate': hit_rate,
                'ttl_seconds': self.config.ttl_seconds
            }

    def clear(self):
        """Clear all entries."""
        with self._lock:
            self._store.clear()
            self._hit_count = 0
            self._miss_count = 0


class TaskDeduplicator:
    """
    Deduplicates tasks based on their content.
    Prevents the same task from being processed multiple times.
    """

    def __init__(self, config: DedupConfig = None):
        self.config = config or DedupConfig()
        self._dedup = RequestDeduplicator(config)

    def start(self):
        """Start the task deduplicator."""
        self._dedup.start()

    def stop(self):
        """Stop the task deduplicator."""
        self._dedup.stop()

    def is_duplicate_task(self, task_type: str, task_data: Dict) -> bool:
        """Check if a task is a duplicate."""
        key = DedupKeyBuilder.build(task_type, task_data)
        return self._dedup.is_duplicate(key)

    def record_task(self, task_type: str, task_data: Dict,
                    task_id: str = None) -> str:
        """Record a task for deduplication."""
        key = DedupKeyBuilder.build(task_type, task_data)
        return self._dedup.record(key, task_id)

    def check_and_record(self, task_type: str, task_data: Dict) -> tuple:
        """
        Check if duplicate and record if not.
        Returns (is_duplicate, request_id).
        """
        key = DedupKeyBuilder.build(task_type, task_data)

        existing = self._dedup.check(key)
        if existing:
            return True, existing

        new_id = self._dedup.record(key)
        return False, new_id

    def get_stats(self) -> Dict:
        """Get deduplicator statistics."""
        return self._dedup.get_stats()


class RequestDedupMiddleware:
    """
    Middleware for deduplicating HTTP requests.
    """

    def __init__(self, dedup: RequestDeduplicator):
        self._dedup = dedup

    def is_duplicate(self, method: str, path: str,
                    params: Dict = None,
                    body: Dict = None) -> bool:
        """Check if request is a duplicate."""
        key = DedupKeyBuilder.build_from_request(method, path, params, body)
        return self._dedup.is_duplicate(key)

    def record_request(self, method: str, path: str,
                       params: Dict = None,
                       body: Dict = None,
                       request_id: str = None) -> str:
        """Record a request."""
        key = DedupKeyBuilder.build_from_request(method, path, params, body)
        return self._dedup.record(key, request_id)

    def handle_request(self, method: str, path: str,
                       params: Dict = None,
                       body: Dict = None) -> tuple:
        """
        Handle a request: check for duplicate, record if not.
        Returns (is_duplicate, request_id).
        """
        key = DedupKeyBuilder.build_from_request(method, path, params, body)

        existing = self._dedup.check(key)
        if existing:
            return True, existing

        new_id = self._dedup.record(key)
        return False, new_id


class DedupFilter:
    """
    Filter for use in message queues.
    Prevents duplicate messages from being processed.
    """

    def __init__(self, dedup: RequestDeduplicator):
        self._dedup = dedup

    def should_process(self, message_id: str, content: Dict) -> bool:
        """Check if message should be processed."""
        key = hashlib.sha256(
            f"{message_id}:{json.dumps(content, sort_keys=True)}".encode()
        ).hexdigest()[:16]

        existing = self._dedup.check(key)
        if existing:
            return False

        self._dedup.record(key, message_id)
        return True


# Global deduplicator
_request_dedup = RequestDeduplicator()
_task_dedup = TaskDeduplicator()


def get_request_dedup() -> RequestDeduplicator:
    return _request_dedup


def get_task_dedup() -> TaskDeduplicator:
    return _task_dedup


def get_dedup_middleware() -> RequestDedupMiddleware:
    return RequestDedupMiddleware(_request_dedup)