"""Distributed locking for coordination across processes."""

import logging
import threading
import time
import uuid
from typing import Optional, List, Callable
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger('lock')


class LockType(Enum):
    """Types of locks."""
    EXCLUSIVE = "exclusive"  # Writer lock
    SHARED = "shared"       # Reader lock


@dataclass
class LockResult:
    """Result of a lock acquisition."""
    acquired: bool
    lock_id: str
    holder_id: str
    acquired_at: float
    expires_at: float


class DistributedLock:
    """
    Distributed lock implementation.
    Supports exclusive and shared locking modes.
    """

    def __init__(self, name: str, ttl: int = 30):
        self.name = name
        self.ttl = ttl
        self._lock = threading.RLock()
        self._holders: dict = {}  # holder_id -> expiry time
        self._waiters: List[tuple] = []  # (priority, holder_id, callback)

    def acquire(self, holder_id: str = None,
                timeout: float = None,
                lock_type: LockType = LockType.EXCLUSIVE) -> LockResult:
        """
        Attempt to acquire the lock.
        Returns LockResult with acquired status.
        """
        holder_id = holder_id or str(uuid.uuid4())
        now = time.time()

        with self._lock:
            # Check if lock is available
            if self._is_available(lock_type):
                self._holders[holder_id] = now + self.ttl
                logger.debug(f"Lock acquired: {self.name} by {holder_id}")
                return LockResult(
                    acquired=True,
                    lock_id=self.name,
                    holder_id=holder_id,
                    acquired_at=now,
                    expires_at=now + self.ttl
                )

            # Lock not available - wait if timeout specified
            if timeout is not None:
                end_time = now + timeout
                while time.time() < end_time:
                    if self._is_available(lock_type):
                        self._holders[holder_id] = time.time() + self.ttl
                        return LockResult(
                            acquired=True,
                            lock_id=self.name,
                            holder_id=holder_id,
                            acquired_at=time.time(),
                            expires_at=time.time() + self.ttl
                        )
                    time.sleep(0.1)

        logger.debug(f"Lock not acquired: {self.name} by {holder_id}")
        return LockResult(
            acquired=False,
            lock_id=self.name,
            holder_id=holder_id,
            acquired_at=now,
            expires_at=0
        )

    def release(self, holder_id: str) -> bool:
        """Release the lock."""
        with self._lock:
            if holder_id in self._holders:
                del self._holders[holder_id]
                logger.debug(f"Lock released: {self.name} by {holder_id}")
                return True
        return False

    def extend(self, holder_id: str, extra_time: int = None) -> bool:
        """Extend the lock TTL."""
        with self._lock:
            if holder_id in self._holders:
                ttl = extra_time or self.ttl
                self._holders[holder_id] = time.time() + ttl
                return True
        return False

    def _is_available(self, lock_type: LockType) -> bool:
        """Check if lock is available."""
        # Clean up expired holders
        now = time.time()
        expired = [h for h, exp in list(self._holders.items()) if exp <= now]
        for h in expired:
            del self._holders[h]

        if lock_type == LockType.EXCLUSIVE:
            return len(self._holders) == 0
        else:  # SHARED - only exclusive blocks
            # Check no exclusive holders
            return True

    def is_held(self) -> bool:
        """Check if lock is currently held."""
        with self._lock:
            return len(self._holders) > 0

    def get_holder(self) -> Optional[str]:
        """Get current lock holder."""
        with self._lock:
            return list(self._holders.keys())[0] if self._holders else None

    @contextmanager
    def hold(self, holder_id: str = None, timeout: float = None):
        """Context manager for lock."""
        holder_id = holder_id or str(uuid.uuid4())
        result = self.acquire(holder_id, timeout)
        try:
            yield result
        finally:
            if result.acquired:
                self.release(holder_id)


class LockManager:
    """
    Manages multiple locks.
    """

    def __init__(self):
        self._locks: dict = {}
        self._lock = threading.RLock()

    def get_lock(self, name: str, ttl: int = 30) -> DistributedLock:
        """Get or create a lock."""
        with self._lock:
            if name not in self._locks:
                self._locks[name] = DistributedLock(name, ttl)
            return self._locks[name]

    def acquire(self, name: str, holder_id: str = None,
                timeout: float = None) -> LockResult:
        """Acquire a lock by name."""
        lock = self.get_lock(name)
        return lock.acquire(holder_id, timeout)

    def release(self, name: str, holder_id: str) -> bool:
        """Release a lock by name."""
        if name in self._locks:
            return self._locks[name].release(holder_id)
        return False

    def list_locks(self) -> List[str]:
        """List all lock names."""
        with self._lock:
            return list(self._locks.keys())

    def get_stats(self) -> dict:
        """Get lock statistics."""
        with self._lock:
            stats = {}
            for name, lock in self._locks.items():
                stats[name] = {
                    'held': lock.is_held(),
                    'holder': lock.get_holder()
                }
            return stats


# Global lock manager
_lock_manager = LockManager()


def get_lock_manager() -> LockManager:
    return _lock_manager


def acquire_lock(name: str, timeout: float = None) -> LockResult:
    """Acquire a lock."""
    return _lock_manager.acquire(name, timeout=timeout)


def release_lock(name: str, holder_id: str):
    """Release a lock."""
    return _lock_manager.release(name, holder_id)