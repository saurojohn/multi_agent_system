"""Batch processing for efficient task handling."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger('batch')


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    max_size: int = 100  # Max items per batch
    max_wait_ms: int = 500  # Max wait time in milliseconds
    min_size: int = 1  # Minimum items to trigger batch
    enabled: bool = True


@dataclass
class BatchItem:
    """An item in a batch."""
    item_id: str
    data: Any
    priority: int = 2
    callback: Optional[Callable] = None
    metadata: Dict = field(default_factory=dict)


@dataclass
class Batch:
    """A batch of items."""
    batch_id: str
    batch_type: str
    items: List[BatchItem]
    created_at: float = field(default_factory=time.time)
    metadata: Dict = field(default_factory=dict)


class BatchCollector:
    """
    Collects items into batches based on size and time constraints.
    """

    def __init__(self, batch_type: str, config: BatchConfig = None):
        self.batch_type = batch_type
        self.config = config or BatchConfig()
        self._pending: List[BatchItem] = []
        self._lock = threading.RLock()
        self._callbacks: Dict[str, Callable] = {}

    def add(self, item_id: str, data: Any, priority: int = 2,
            callback: Callable = None, metadata: Dict = None) -> Optional[Batch]:
        """
        Add an item to the collector.
        Returns a Batch if it was triggered, None otherwise.
        """
        item = BatchItem(
            item_id=item_id,
            data=data,
            priority=priority,
            callback=callback,
            metadata=metadata or {}
        )

        with self._lock:
            self._pending.append(item)

            # Check if batch should be triggered
            should_trigger = self._should_trigger_batch()

            if should_trigger:
                batch = self._create_batch()
                self._pending = []
                return batch

        return None

    def _should_trigger_batch(self) -> bool:
        """Determine if batch should be triggered."""
        if not self.config.enabled:
            return False

        # Size reached
        if len(self._pending) >= self.config.max_size:
            return True

        # Minimum size with time elapsed (checked on flush)
        if len(self._pending) >= self.config.min_size:
            if self._pending:
                oldest = self._pending[0]
                age_ms = (time.time() - oldest.created_at) * 1000
                if age_ms >= self.config.max_wait_ms:
                    return True

        return False

    def _create_batch(self) -> Batch:
        """Create a batch from pending items."""
        batch = Batch(
            batch_id=str(uuid.uuid4()),
            batch_type=self.batch_type,
            items=list(self._pending)
        )
        return batch

    def flush(self) -> Optional[Batch]:
        """Force flush pending items as a batch."""
        with self._lock:
            if not self._pending:
                return None

            batch = self._create_batch()
            self._pending = []
            return batch

    def size(self) -> int:
        """Get current pending size."""
        with self._lock:
            return len(self._pending)


class BatchProcessor:
    """
    Processes batches of items.
    """

    def __init__(self, process_fn: Callable[[Batch], Any],
                 error_fn: Callable[[Batch, Exception], None] = None):
        self.process_fn = process_fn
        self.error_fn = error_fn or self._default_error_handler
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def _default_error_handler(self, batch: Batch, error: Exception):
        """Default error handler."""
        logger.error(f"Batch processing failed for {batch.batch_id}: {error}")

    def process(self, batch: Batch) -> List[Any]:
        """Process a batch and return results."""
        try:
            results = self.process_fn(batch)
            return results
        except Exception as e:
            self.error_fn(batch, e)
            return []

    def start(self):
        """Start batch processor."""
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop batch processor."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self):
        """Run loop."""
        while self._running:
            time.sleep(0.1)


class BatchManager:
    """
    Manages multiple batch collectors for different batch types.
    """

    def __init__(self):
        self._collectors: Dict[str, BatchCollector] = {}
        self._processors: Dict[str, BatchProcessor] = {}
        self._lock = threading.RLock()

    def create_collector(self, batch_type: str,
                        config: BatchConfig = None) -> BatchCollector:
        """Create or get a batch collector."""
        with self._lock:
            if batch_type not in self._collectors:
                self._collectors[batch_type] = BatchCollector(batch_type, config)
                logger.info(f"Created batch collector: {batch_type}")
            return self._collectors[batch_type]

    def register_processor(self, batch_type: str, processor: BatchProcessor):
        """Register a processor for a batch type."""
        with self._lock:
            self._processors[batch_type] = processor
            logger.info(f"Registered processor for: {batch_type}")

    def add_item(self, batch_type: str, item_id: str, data: Any,
                 priority: int = 2, callback: Callable = None,
                 metadata: Dict = None) -> Optional[Batch]:
        """Add an item to the appropriate collector."""
        collector = self.create_collector(batch_type)
        batch = collector.add(item_id, data, priority, callback, metadata)

        if batch and batch_type in self._processors:
            processor = self._processors[batch_type]
            results = processor.process(batch)

            # Notify callbacks
            for i, item in enumerate(batch.items):
                if item.callback:
                    result = results[i] if i < len(results) else None
                    try:
                        item.callback(result)
                    except Exception as e:
                        logger.error(f"Callback failed for {item.item_id}: {e}")

        return batch

    def flush_all(self) -> List[Batch]:
        """Flush all collectors."""
        batches = []
        with self._lock:
            for collector in self._collectors.values():
                batch = collector.flush()
                if batch:
                    batches.append(batch)
        return batches

    def get_stats(self) -> Dict:
        """Get batch manager statistics."""
        with self._lock:
            stats = {}
            for batch_type, collector in self._collectors.items():
                stats[batch_type] = {
                    'pending': collector.size(),
                    'max_size': collector.config.max_size,
                    'max_wait_ms': collector.config.max_wait_ms
                }
            return stats


class BatchScheduler:
    """
    Schedules batch processing at regular intervals.
    """

    def __init__(self, interval_ms: int = 1000):
        self.interval_ms = interval_ms / 1000.0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._handlers: List[Callable] = []

    def add_handler(self, handler: Callable[[], None]):
        """Add a batch handler callback."""
        self._handlers.append(handler)

    def start(self):
        """Start the scheduler."""
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info(f"Batch scheduler started (interval: {self.interval_ms}s)")

    def stop(self):
        """Stop the scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self):
        """Run loop."""
        while self._running:
            for handler in self._handlers:
                try:
                    handler()
                except Exception as e:
                    logger.error(f"Batch handler failed: {e}")
            time.sleep(self.interval_ms)


# Global batch manager
_batch_manager = BatchManager()


def get_batch_manager() -> BatchManager:
    return _batch_manager


def create_batch_collector(batch_type: str, **config) -> BatchCollector:
    """Helper to create a batch collector."""
    cfg = BatchConfig(**config)
    return _batch_manager.create_collector(batch_type, cfg)