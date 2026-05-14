"""Cache strategies for improving performance."""

import logging
import threading
import time
import hashlib
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import OrderedDict

logger = logging.getLogger('cache_strat')


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"         # Least Recently Used
    LFU = "lfu"         # Least Frequently Used
    FIFO = "fifo"       # First In First Out
    TTL = "ttl"         # Time To Live based
    RANDOM = "random"   # Random eviction


@dataclass
class CacheEntry:
    """A cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    size_bytes: int = 0
    tags: List[str] = field(default_factory=list)


@dataclass
class CacheStats:
    """Cache statistics."""
    max_size: int
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    size: int = 0
    hit_rate: float = 0.0


class CacheStrategy:
    """
    Base class for cache strategies.
    """

    def __init__(self, max_size: int = 1000, ttl: int = 3600):
        self.max_size = max_size
        self.ttl = ttl

    def select_eviction(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        """Select a key to evict."""
        raise NotImplementedError

    def should_evict(self, current_size: int) -> bool:
        """Check if eviction is needed."""
        return current_size >= self.max_size

    def is_expired(self, entry: CacheEntry) -> bool:
        """Check if entry is expired."""
        return time.time() - entry.created_at > self.ttl


class LRUCache(CacheStrategy):
    """Least Recently Used cache strategy."""

    def select_eviction(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        """Evict least recently used entry."""
        if not entries:
            return None

        lru_key = min(entries.keys(), key=lambda k: entries[k].last_accessed)
        return lru_key


class LFUCache(CacheStrategy):
    """Least Frequently Used cache strategy."""

    def select_eviction(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        """Evict least frequently used entry."""
        if not entries:
            return None

        lfu_key = min(entries.keys(), key=lambda k: entries[k].access_count)
        return lfu_key


class FIFOCache(CacheStrategy):
    """First In First Out cache strategy."""

    def select_eviction(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        """Evict oldest entry."""
        if not entries:
            return None

        fifo_key = min(entries.keys(), key=lambda k: entries[k].created_at)
        return fifo_key


class TTLCache(CacheStrategy):
    """Time To Live based cache strategy."""

    def select_eviction(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        """Evict oldest non-expired entry."""
        if not entries:
            return None

        # First try to find expired entries
        for key, entry in entries.items():
            if self.is_expired(entry):
                return key

        # If no expired, evict oldest
        oldest_key = min(entries.keys(), key=lambda k: entries[k].created_at)
        return oldest_key


class RandomCache(CacheStrategy):
    """Random eviction cache strategy."""

    def select_eviction(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        """Evict a random entry."""
        if not entries:
            return None

        import random
        return random.choice(list(entries.keys()))


class WriteBackCache:
    """
    Write-back cache strategy.
    Writes are accumulated and flushed periodically.
    """

    def __init__(self, base_cache, flush_interval: int = 60):
        self.base_cache = base_cache
        self.flush_interval = flush_interval
        self._dirty_keys: set = set()
        self._lock = threading.RLock()
        self._flush_thread: threading.Thread = None
        self._running = False

    def start(self):
        """Start the write-back cache."""
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def stop(self):
        """Stop the write-back cache."""
        self._running = False
        self._flush_thread.join(timeout=5)

    def mark_dirty(self, key: str):
        """Mark a key as dirty (needs write)."""
        with self._lock:
            self._dirty_keys.add(key)

    def _flush_loop(self):
        """Periodically flush dirty entries."""
        while self._running:
            time.sleep(self.flush_interval)
            self.flush()

    def flush(self):
        """Flush all dirty entries."""
        with self._lock:
            for key in list(self._dirty_keys):
                # Write to underlying cache
                self._dirty_keys.remove(key)


class CacheWarmer:
    """
    Pre-warms cache by loading expected data.
    """

    def __init__(self, cache):
        self.cache = cache
        self._warming = False
        self._prefetch_functions: List[Callable] = []

    def add_prefetch(self, fn: Callable[[List[str]], Dict]):
        """Add a prefetch function."""
        self._prefetch_functions.append(fn)

    def warm(self, keys: List[str]):
        """Warm cache with specific keys."""
        if not self._prefetch_functions:
            return

        self._warming = True
        try:
            # Gather data using prefetch functions
            for fn in self._prefetch_functions:
                results = fn(keys)
                for key, value in results.items():
                    self.cache.set(key, value)
        finally:
            self._warming = False

    def warm_pattern(self, pattern: str):
        """Warm cache based on a key pattern."""
        # This would typically scan for matching keys
        pass


class TieredCache:
    """
    Multi-level cache with L1 (memory) and L2 (disk/persistent).
    """

    def __init__(self, l1_cache, l2_cache=None):
        self.l1 = l1_cache
        self.l2 = l2_cache
        self._hit_count = 0
        self._miss_count = 0

    def get(self, key: str) -> Optional[Any]:
        """Get from tiered cache."""
        # Try L1 first
        value = self.l1.get(key)
        if value is not None:
            self._hit_count += 1
            return value

        # Try L2
        if self.l2:
            value = self.l2.get(key)
            if value is not None:
                # Promote to L1
                self.l1.set(key, value)
                self._hit_count += 1
                return value

        self._miss_count += 1
        return None

    def set(self, key: str, value: Any):
        """Set in tiered cache."""
        self.l1.set(key, value)
        if self.l2:
            self.l2.set(key, value)

    def get_stats(self) -> Dict:
        """Get tiered cache statistics."""
        total = self._hit_count + self._miss_count
        hit_rate = self._hit_count / total if total > 0 else 0

        return {
            'l1_stats': self.l1.get_stats() if hasattr(self.l1, 'get_stats') else {},
            'l2_stats': self.l2.get_stats() if self.l2 and hasattr(self.l2, 'get_stats') else {},
            'hit_rate': hit_rate,
            'hits': self._hit_count,
            'misses': self._miss_count
        }


class CacheStrategyFactory:
    """Factory for creating cache strategies."""

    _strategies = {
        EvictionPolicy.LRU: LRUCache,
        EvictionPolicy.LFU: LFUCache,
        EvictionPolicy.FIFO: FIFOCache,
        EvictionPolicy.TTL: TTLCache,
        EvictionPolicy.RANDOM: RandomCache,
    }

    @classmethod
    def create(cls, policy: EvictionPolicy, **kwargs) -> CacheStrategy:
        """Create a cache strategy by policy."""
        strategy_class = cls._strategies.get(policy)
        if not strategy_class:
            raise ValueError(f"Unknown policy: {policy}")
        return strategy_class(**kwargs)


# Factory instance for common strategies
_default_strategies = {
    'memory': lambda: LRUCache(max_size=1000, ttl=300),
    'persistent': lambda: TTLCache(max_size=10000, ttl=3600),
    'high-throughput': lambda: LFUCache(max_size=5000, ttl=600),
}


def get_strategy(name: str) -> CacheStrategy:
    """Get a pre-configured strategy by name."""
    factory = _default_strategies.get(name)
    if factory:
        return factory()
    return LRUCache()


def create_lru_cache(max_size: int = 1000, ttl: int = 3600) -> CacheStrategy:
    """Create an LRU cache strategy."""
    return LRUCache(max_size=max_size, ttl=ttl)


def create_lfu_cache(max_size: int = 1000, ttl: int = 3600) -> CacheStrategy:
    """Create an LFU cache strategy."""
    return LFUCache(max_size=max_size, ttl=ttl)


def create_ttl_cache(max_size: int = 1000, ttl: int = 3600) -> CacheStrategy:
    """Create a TTL cache strategy."""
    return TTLCache(max_size=max_size, ttl=ttl)