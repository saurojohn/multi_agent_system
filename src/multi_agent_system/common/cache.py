"""Task result cache to avoid recomputing identical tasks."""

import hashlib
import json
import logging
import threading
import time
from typing import Dict, Optional, Any

logger = logging.getLogger('cache')


class TaskResultCache:
    """LRU cache for task results."""

    def __init__(self, max_size: int = 1000, ttl: int = 3600):
        """
        Args:
            max_size: Maximum number of cached results
            ttl: Time-to-live in seconds for cached results
        """
        self.max_size = max_size
        self.ttl = ttl
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def _make_key(self, task_type: str, task_data: Dict) -> str:
        """Generate cache key from task type and data."""
        data_str = json.dumps(task_data, sort_keys=True)
        hash_str = hashlib.sha256(f"{task_type}:{data_str}".encode()).hexdigest()[:16]
        return f"{task_type}:{hash_str}"

    def get(self, task_type: str, task_data: Dict) -> Optional[Any]:
        """Get cached result if available and not expired."""
        key = self._make_key(task_type, task_data)

        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None

            entry = self._cache[key]
            # Check TTL
            if time.time() - entry['cached_at'] > self.ttl:
                del self._cache[key]
                self._misses += 1
                return None

            # Move to end (LRU)
            del self._cache[key]
            self._cache[key] = entry

            self._hits += 1
            logger.debug(f'Cache hit for {key}')
            return entry['result']

    def set(self, task_type: str, task_data: Dict, result: Any):
        """Cache a task result."""
        key = self._make_key(task_type, task_data)

        with self._lock:
            # Evict oldest if at capacity
            if len(self._cache) >= self.max_size:
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
                logger.debug(f'Evicted oldest cache entry: {oldest_key}')

            self._cache[key] = {
                'result': result,
                'cached_at': time.time(),
                'task_type': task_type
            }
            logger.debug(f'Cached result for {key}')

    def invalidate(self, task_type: str = None, task_data: Dict = None):
        """Invalidate cache entries. If task_type is None, clears all."""
        with self._lock:
            if task_type is None:
                self._cache.clear()
                logger.info('Cache cleared')
            else:
                if task_data:
                    key = self._make_key(task_type, task_data)
                    if key in self._cache:
                        del self._cache[key]
                        logger.debug(f'Invalidated cache entry: {key}')
                else:
                    # Clear all entries for this task type
                    keys_to_delete = [k for k, v in self._cache.items()
                                     if v['task_type'] == task_type]
                    for key in keys_to_delete:
                        del self._cache[key]
                    logger.info(f'Invalidated {len(keys_to_delete)} entries for {task_type}')

    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': f"{hit_rate:.2%}"
            }

    def enable(self, enabled: bool = True):
        """Enable or disable the cache."""
        self._enabled = enabled
        logger.info(f'Cache {"enabled" if enabled else "disabled"}')


# Global cache instance
_cache = TaskResultCache()


def get_cache() -> TaskResultCache:
    return _cache