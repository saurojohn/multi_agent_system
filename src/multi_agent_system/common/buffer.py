"""Request buffering for handling burst traffic."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

logger = logging.getLogger('buffer')


class Buffer策略(Enum):
    """Buffer strategies."""
    FIFO = "fifo"           # First in first out
    LIFO = "lifo"           # Last in first out
    PRIORITY = "priority"   # Priority based
    RANDOM = "random"       # Random selection


@dataclass
class BufferedRequest:
    """A buffered request."""
    request_id: str
    data: Any
    priority: int = 5  # 1 (highest) to 10 (lowest)
    enqueued_at: float = field(default_factory=time.time)
    metadata: Dict = field(default_factory=dict)


@dataclass
class BufferConfig:
    """Configuration for request buffer."""
    max_size: int = 1000
    flush_threshold: int = 100  # Auto-flush when this many requests buffered
    flush_interval: float = 1.0  # Seconds between automatic flushes
    timeout: float = 30.0  # Request timeout in buffer
    strategy: Buffer策略 = Buffer策略.FIFO


class RequestBuffer:
    """
    Buffer for incoming requests during high load.
    """

    def __init__(self, config: BufferConfig = None):
        self.config = config or BufferConfig()
        self._buffer: deque = deque(maxlen=self.config.max_size)
        self._lock = threading.RLock()
        self._running = False
        self._flush_thread: threading.Thread = None
        self._flush_handler: Optional[Callable] = None
        self._stats = {
            'enqueued': 0,
            'dequeued': 0,
            'expired': 0,
            'dropped': 0
        }

    def start(self):
        """Start the buffer."""
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def stop(self):
        """Stop the buffer."""
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=5)

    def set_flush_handler(self, handler: Callable[[List[BufferedRequest]], None]):
        """Set handler for flushing buffered requests."""
        self._flush_handler = handler

    def enqueue(self, request_id: str, data: Any,
                priority: int = 5, metadata: Dict = None) -> bool:
        """
        Add a request to the buffer.
        Returns True if added, False if buffer is full.
        """
        if len(self._buffer) >= self.config.max_size:
            self._stats['dropped'] += 1
            return False

        request = BufferedRequest(
            request_id=request_id,
            data=data,
            priority=priority,
            metadata=metadata or {}
        )

        with self._lock:
            self._buffer.append(request)
            self._stats['enqueued'] += 1

        # Check if we should auto-flush
        if len(self._buffer) >= self.config.flush_threshold:
            self.flush()

        return True

    def dequeue(self) -> Optional[BufferedRequest]:
        """Remove and return the next request based on strategy."""
        with self._lock:
            if not self._buffer:
                return None

            if self.config.strategy == Buffer策略.FIFO:
                request = self._buffer.popleft()
            elif self.config.strategy == Buffer策略.LIFO:
                request = self._buffer.pop()
            elif self.config.strategy == Buffer策略.PRIORITY:
                # Find highest priority (lowest number)
                min_idx = 0
                for i, req in enumerate(self._buffer):
                    if req.priority < self._buffer[min_idx].priority:
                        min_idx = i
                request = self._buffer.pop(min_idx)
            elif self.config.strategy == Buffer策略.RANDOM:
                import random
                idx = random.randint(0, len(self._buffer) - 1)
                request = self._buffer.pop(idx)
            else:
                request = self._buffer.popleft()

        self._stats['dequeued'] += 1
        return request

    def dequeue_batch(self, batch_size: int) -> List[BufferedRequest]:
        """Dequeue multiple requests."""
        batch = []
        for _ in range(min(batch_size, len(self._buffer))):
            request = self.dequeue()
            if request:
                batch.append(request)
        return batch

    def flush(self):
        """Flush all buffered requests using the handler."""
        if not self._flush_handler:
            return

        requests = []
        with self._lock:
            while self._buffer:
                requests.append(self._buffer.popleft())

        if requests:
            try:
                self._flush_handler(requests)
            except Exception as e:
                logger.error(f"Flush handler failed: {e}")
                # Put requests back
                with self._lock:
                    for req in reversed(requests):
                        self._buffer.appendleft(req)

    def _flush_loop(self):
        """Background flush loop."""
        while self._running:
            time.sleep(self.config.flush_interval)
            self._check_expired()
            if self._buffer and self._flush_handler:
                self.flush()

    def _check_expired(self):
        """Remove expired requests."""
        now = time.time()
        expired = []

        with self._lock:
            for req in self._buffer:
                if now - req.enqueued_at > self.config.timeout:
                    expired.append(req)

            for req in expired:
                if req in self._buffer:
                    self._buffer.remove(req)
                    self._stats['expired'] += 1

        if expired:
            logger.warning(f"Expired {len(expired)} buffered requests")

    def size(self) -> int:
        """Get current buffer size."""
        return len(self._buffer)

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return len(self._buffer) == 0

    def is_full(self) -> bool:
        """Check if buffer is full."""
        return len(self._buffer) >= self.config.max_size

    def clear(self):
        """Clear all buffered requests."""
        with self._lock:
            self._buffer.clear()

    def get_stats(self) -> Dict:
        """Get buffer statistics."""
        return {
            'size': len(self._buffer),
            'max_size': self.config.max_size,
            'strategy': self.config.strategy.value,
            'enqueued': self._stats['enqueued'],
            'dequeued': self._stats['dequeued'],
            'expired': self._stats['expired'],
            'dropped': self._stats['dropped']
        }


class BufferedHandler:
    """
    Handler that buffers requests during high load.
    """

    def __init__(self, inner_handler: Callable, config: BufferConfig = None):
        self.buffer = RequestBuffer(config)
        self.inner_handler = inner_handler
        self._running = False

    def start(self):
        """Start the buffered handler."""
        self._running = True
        self.buffer.start()
        self.buffer.set_flush_handler(self._handle_batch)

    def stop(self):
        """Stop the buffered handler."""
        self._running = False
        self.buffer.stop()

    def handle(self, request_data: Any, **kwargs) -> Any:
        """
        Handle a request, buffering if necessary.
        """
        import uuid
        request_id = str(uuid.uuid4())

        # Try to process immediately if buffer is small
        if self.buffer.size() < self.buffer.config.max_size * 0.2:
            try:
                return self.inner_handler(request_data, **kwargs)
            except Exception as e:
                # If immediate processing fails, buffer for retry
                pass

        # Buffer the request
        priority = kwargs.get('priority', 5)
        metadata = kwargs.get('metadata', {})

        if self.buffer.enqueue(request_id, request_data, priority, metadata):
            return {'status': 'buffered', 'request_id': request_id}
        else:
            return {'status': 'rejected', 'reason': 'buffer full'}

    def _handle_batch(self, requests: List[BufferedRequest]):
        """Process a batch of buffered requests."""
        for request in requests:
            try:
                self.inner_handler(request.data)
            except Exception as e:
                logger.error(f"Batch processing failed for {request.request_id}: {e}")


class BackPressureBuffer:
    """
    Buffer that applies back pressure when full.
    """

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._buffer: deque = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._waiters: List[threading.Event] = []

    def put(self, item: Any, timeout: float = None) -> bool:
        """
        Put an item in the buffer with optional timeout.
        Returns True if successful, False if timed out.
        """
        start = time.time()

        while len(self._buffer) >= self.max_size:
            if timeout and time.time() - start >= timeout:
                return False
            time.sleep(0.1)

        with self._lock:
            self._buffer.append(item)

        # Signal one waiter
        with self._lock:
            if self._waiters:
                event = self._waiters.pop(0)
                event.set()

        return True

    def get(self, timeout: float = None) -> Optional[Any]:
        """
        Get an item from the buffer with optional timeout.
        Returns None if timed out.
        """
        start = time.time()

        while len(self._buffer) == 0:
            if timeout and time.time() - start >= timeout:
                return None

            event = threading.Event()
            with self._lock:
                self._waiters.append(event)

            if event.wait(timeout=1.0):
                # Got signaled, but buffer might be empty
                pass

        with self._lock:
            if self._buffer:
                return self._buffer.popleft()

        return None

    def size(self) -> int:
        return len(self._buffer)

    def is_empty(self) -> bool:
        return len(self._buffer) == 0


class BatchingBuffer:
    """
    Buffer that collects items into batches.
    """

    def __init__(self, batch_size: int = 10, timeout: float = 1.0):
        self.batch_size = batch_size
        self.timeout = timeout
        self._buffer: List[Any] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()

    def add(self, item: Any) -> Optional[List[Any]]:
        """
        Add an item to the buffer.
        Returns a batch if ready, None otherwise.
        """
        with self._lock:
            self._buffer.append(item)

            # Check if batch is ready
            if len(self._buffer) >= self.batch_size:
                batch = self._buffer
                self._buffer = []
                self._last_flush = time.time()
                return batch

            # Check timeout
            if time.time() - self._last_flush >= self.timeout and self._buffer:
                batch = self._buffer
                self._buffer = []
                self._last_flush = time.time()
                return batch

        return None

    def flush(self) -> List[Any]:
        """Force flush the buffer."""
        with self._lock:
            batch = self._buffer
            self._buffer = []
            self._last_flush = time.time()
            return batch

    def size(self) -> int:
        return len(self._buffer)


# Global buffer
_default_buffer = RequestBuffer()


def get_buffer() -> RequestBuffer:
    return _default_buffer


def create_batching_buffer(batch_size: int = 10, timeout: float = 1.0) -> BatchingBuffer:
    """Create a batching buffer."""
    return BatchingBuffer(batch_size, timeout)