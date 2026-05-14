"""Connection pool management for databases and external services."""

import logging
import threading
import time
import queue
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager

logger = logging.getLogger('connection_pool')


class PoolStrategy(Enum):
    """Connection pool allocation strategy."""
    FIFO = "fifo"           # First in first out
    LIFO = "lifo"           # Last in first out (recently used)
    LRU = "lru"             # Least recently used
    RANDOM = "random"       # Random selection


@dataclass
class ConnectionConfig:
    """Configuration for a connection pool."""
    min_size: int = 5
    max_size: int = 20
    max_overflow: int = 10      # Allow temporary overflow
    timeout: float = 30.0       # Connection acquisition timeout
    retry_attempts: int = 3     # Retry attempts on failure
    retry_delay: float = 0.5    # Delay between retries
    validation_interval: int = 60  # Validate connections every N seconds
    idle_timeout: int = 300     # Close idle connections after N seconds


@dataclass
class PooledConnection:
    """A pooled connection wrapper."""
    conn_id: str
    connection: Any  # Actual connection object
    created_at: float
    last_used: float
    use_count: int = 0
    is_valid: bool = True
    metadata: Dict = field(default_factory=dict)


@dataclass
class PoolStats:
    """Connection pool statistics."""
    pool_name: str
    size: int
    available: int
    in_use: int
    overflow: int
    total_connections: int
    max_size: int
    waiters: int = 0
    timeout_count: int = 0


class ConnectionPool:
    """
    Generic connection pool with configurable strategies.
    """

    def __init__(self, name: str, factory: Callable[[], Any],
                 config: ConnectionConfig = None):
        self.name = name
        self.factory = factory
        self.config = config or ConnectionConfig()
        self._pool: queue.Queue = queue.Queue(maxsize=self.config.max_size)
        self._overflow_pool: queue.Queue = queue.Queue(maxsize=self.config.max_overflow)
        self._in_use: Dict[str, PooledConnection] = {}
        self._lock = threading.Lock()
        self._strategy = PoolStrategy.FIFO
        self._waiters = 0
        self._timeout_count = 0
        self._running = False
        self._validation_thread: threading.Thread = None

    def start(self):
        """Start the connection pool."""
        self._running = True

        # Initialize connections
        for _ in range(self.config.min_size):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)

        # Start validation thread
        self._validation_thread = threading.Thread(
            target=self._validation_loop,
            daemon=True
        )
        self._validation_thread.start()

        logger.info(f"Connection pool started: {self.name} (size={self.size()})")

    def stop(self):
        """Stop the connection pool."""
        self._running = False

        # Close all connections
        while not self._pool.empty():
            try:
                pooled = self._pool.get_nowait()
                self._close_connection(pooled)
            except:
                pass

        while not self._overflow_pool.empty():
            try:
                pooled = self._overflow_pool.get_nowait()
                self._close_connection(pooled)
            except:
                pass

        logger.info(f"Connection pool stopped: {self.name}")

    def acquire(self, timeout: float = None) -> Optional[PooledConnection]:
        """
        Acquire a connection from the pool.
        Blocks until a connection is available or timeout.
        """
        timeout = timeout or self.config.timeout
        start_time = time.time()

        while time.time() - start_time < timeout:
            # Try to get from main pool
            try:
                pooled = self._pool.get(timeout=0.1)
                if self._is_connection_valid(pooled):
                    with self._lock:
                        self._in_use[pooled.conn_id] = pooled
                    pooled.last_used = time.time()
                    pooled.use_count += 1
                    return pooled
                else:
                    self._close_connection(pooled)
                    continue
            except queue.Empty:
                pass

            # Try to create overflow connection
            with self._lock:
                overflow_size = self._overflow_pool.qsize()
                total_in_use = len(self._in_use)

            if overflow_size + total_in_use < self.config.max_size + self.config.max_overflow:
                conn = self._create_connection()
                if conn:
                    pooled = conn
                    with self._lock:
                        self._in_use[pooled.conn_id] = pooled
                    pooled.last_used = time.time()
                    pooled.use_count += 1
                    return pooled

            self._waiters += 1

        # Timeout
        self._timeout_count += 1
        logger.warning(f"Connection acquisition timeout: {self.name}")
        return None

    def release(self, pooled: PooledConnection):
        """Release a connection back to the pool."""
        if not pooled:
            return

        with self._lock:
            if pooled.conn_id in self._in_use:
                del self._in_use[pooled.conn_id]

        # Check if connection is still valid
        if self._is_connection_valid(pooled) and pooled.is_valid:
            pooled.last_used = time.time()
            try:
                self._pool.put(pooled, timeout=0.1)
            except queue.Full:
                # Pool full, close the connection
                self._close_connection(pooled)
        else:
            self._close_connection(pooled)

    def _create_connection(self) -> Optional[PooledConnection]:
        """Create a new pooled connection."""
        try:
            conn = self.factory()
            return PooledConnection(
                conn_id=f"{self.name}_{int(time.time() * 1000000)}",
                connection=conn,
                created_at=time.time(),
                last_used=time.time()
            )
        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            return None

    def _close_connection(self, pooled: PooledConnection):
        """Close a connection."""
        try:
            if hasattr(pooled.connection, 'close'):
                pooled.connection.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

    def _is_connection_valid(self, pooled: PooledConnection) -> bool:
        """Check if a connection is still valid."""
        if not pooled.is_valid:
            return False

        # Check idle timeout
        idle_time = time.time() - pooled.last_used
        if idle_time > self.config.idle_timeout:
            return False

        return True

    def _validation_loop(self):
        """Background connection validation."""
        while self._running:
            time.sleep(self.config.validation_interval)
            self._validate_connections()

    def _validate_connections(self):
        """Validate all connections in the pool."""
        valid_count = 0
        closed_count = 0

        # Check main pool
        temp_connections = []
        while not self._pool.empty():
            try:
                pooled = self._pool.get_nowait()
                if self._is_connection_valid(pooled):
                    temp_connections.append(pooled)
                    valid_count += 1
                else:
                    self._close_connection(pooled)
                    closed_count += 1
            except queue.Empty:
                break

        for conn in temp_connections:
            self._pool.put(conn)

        # Check overflow pool
        temp_connections = []
        while not self._overflow_pool.empty():
            try:
                pooled = self._overflow_pool.get_nowait()
                if self._is_connection_valid(pooled):
                    temp_connections.append(pooled)
                else:
                    self._close_connection(pooled)
                    closed_count += 1
            except queue.Empty:
                break

        for conn in temp_connections:
            self._overflow_pool.put(conn)

        if closed_count > 0:
            logger.debug(f"Pool {self.name}: validated {valid_count}, closed {closed_count}")

    @contextmanager
    def connection(self, timeout: float = None):
        """Context manager for working with a connection."""
        pooled = self.acquire(timeout)
        try:
            yield pooled.connection if pooled else None
        finally:
            if pooled:
                self.release(pooled)

    def size(self) -> int:
        """Get current pool size."""
        return self._pool.qsize() + self._overflow_pool.qsize() + len(self._in_use)

    def available(self) -> int:
        """Get available connections."""
        return self._pool.qsize()

    def in_use(self) -> int:
        """Get connections currently in use."""
        with self._lock:
            return len(self._in_use)

    def get_stats(self) -> PoolStats:
        """Get pool statistics."""
        return PoolStats(
            pool_name=self.name,
            size=self.size(),
            available=self.available(),
            in_use=self.in_use(),
            overflow=self._overflow_pool.qsize(),
            total_connections=self.size(),
            max_size=self.config.max_size,
            waiters=self._waiters,
            timeout_count=self._timeout_count
        )


class PoolManager:
    """
    Manages multiple connection pools.
    """

    def __init__(self):
        self._pools: Dict[str, ConnectionPool] = {}
        self._lock = threading.Lock()

    def create_pool(self, name: str, factory: Callable[[], Any],
                   config: ConnectionConfig = None) -> ConnectionPool:
        """Create or get a connection pool."""
        with self._lock:
            if name not in self._pools:
                self._pools[name] = ConnectionPool(name, factory, config)
                logger.info(f"Created connection pool: {name}")
            return self._pools[name]

    def get_pool(self, name: str) -> Optional[ConnectionPool]:
        """Get a pool by name."""
        with self._lock:
            return self._pools.get(name)

    def close_pool(self, name: str):
        """Close and remove a pool."""
        with self._lock:
            if name in self._pools:
                self._pools[name].stop()
                del self._pools[name]

    def close_all(self):
        """Close all pools."""
        with self._lock:
            for pool in self._pools.values():
                pool.stop()
            self._pools.clear()

    def get_stats(self) -> Dict:
        """Get statistics for all pools."""
        stats = {}
        with self._lock:
            for name, pool in self._pools.items():
                pool_stats = pool.get_stats()
                stats[name] = {
                    'size': pool_stats.size,
                    'available': pool_stats.available,
                    'in_use': pool_stats.in_use,
                    'max_size': pool_stats.max_size
                }
        return stats


# Global pool manager
_pool_manager = PoolManager()


def get_pool_manager() -> PoolManager:
    return _pool_manager


def create_pool(name: str, factory: Callable[[], Any],
               **config) -> ConnectionPool:
    """Create a new connection pool."""
    conn_config = ConnectionConfig(**config)
    return _pool_manager.create_pool(name, factory, conn_config)


# Example factory functions
def create_redis_factory(host: str = 'localhost', port: int = 6379):
    """Factory for Redis connections."""
    import redis
    def factory():
        return redis.Redis(host=host, port=port)
    return factory


def create_db_factory(url: str):
    """Factory for database connections."""
    import sqlite3
    def factory():
        return sqlite3.connect(url)
    return factory