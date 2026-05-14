"""Retry queue for handling failed operations with configurable backoff."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('retry_queue')


class RetryStatus(Enum):
    """Status of a retry item."""
    PENDING = "pending"
    RETRYING = "retrying"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BackoffStrategy(Enum):
    """Backoff strategies for retries."""
    FIXED = "fixed"           # Fixed delay
    LINEAR = "linear"         # Linear increase
    EXPONENTIAL = "exponential"  # Exponential backoff
    FIBONACCI = "fibonacci"     # Fibonacci backoff
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"  # Exponential with random jitter


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 60.0    # seconds
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    multiplier: float = 2.0    # For linear/exponential
    jitter: float = 0.1        # Random jitter factor


@dataclass
class RetryItem:
    """An item in the retry queue."""
    item_id: str
    operation: Callable
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    status: RetryStatus = RetryStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    created_at: float = field(default_factory=time.time)
    next_retry_at: float = field(default_factory=time.time)
    last_error: Optional[str] = None
    result: Any = None
    metadata: Dict = field(default_factory=dict)


class BackoffCalculator:
    """Calculates backoff delays."""

    @staticmethod
    def calculate(strategy: BackoffStrategy, attempt: int,
                 config: RetryConfig) -> float:
        """Calculate delay for given attempt."""
        base_delay = config.initial_delay

        if strategy == BackoffStrategy.FIXED:
            return base_delay

        elif strategy == BackoffStrategy.LINEAR:
            delay = base_delay + (attempt - 1) * config.multiplier * base_delay
            return min(delay, config.max_delay)

        elif strategy == BackoffStrategy.EXPONENTIAL:
            delay = base_delay * (config.multiplier ** (attempt - 1))
            return min(delay, config.max_delay)

        elif strategy == BackoffStrategy.EXPONENTIAL_WITH_JITTER:
            delay = base_delay * (config.multiplier ** (attempt - 1))
            delay = min(delay, config.max_delay)
            # Add jitter
            jitter_range = delay * config.jitter
            delay += random.uniform(-jitter_range, jitter_range)
            return max(0, delay)

        elif strategy == BackoffStrategy.FIBONACCI:
            # Fibonacci: 1, 1, 2, 3, 5, 8, 13...
            fib = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
            idx = min(attempt - 1, len(fib) - 1)
            delay = base_delay * fib[idx]
            return min(delay, config.max_delay)

        return base_delay


class RetryQueue:
    """
    Queue for retrying failed operations.
    """

    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
        self._queue: List[RetryItem] = []
        self._completed: List[RetryItem] = []
        self._failed: List[RetryItem] = []
        self._lock = threading.RLock()
        self._running = False
        self._processor_thread: threading.Thread = None
        self._handlers: Dict[str, List[Callable]] = {
            'retry': [],
            'success': [],
            'failure': []
        }

    def start(self):
        """Start the retry queue processor."""
        self._running = True
        self._processor_thread = threading.Thread(target=self._process_loop, daemon=True)
        self._processor_thread.start()
        logger.info("Retry queue started")

    def stop(self):
        """Stop the retry queue processor."""
        self._running = False
        if self._processor_thread:
            self._processor_thread.join(timeout=5)
        logger.info("Retry queue stopped")

    def add(self, operation: Callable, *args, **kwargs) -> str:
        """Add an operation to the retry queue."""
        item_id = str(uuid.uuid4())

        item = RetryItem(
            item_id=item_id,
            operation=operation,
            args=args,
            kwargs=kwargs,
            max_attempts=self.config.max_attempts,
            next_retry_at=time.time()
        )

        with self._lock:
            self._queue.append(item)

        logger.debug(f"Added to retry queue: {item_id}")
        return item_id

    def add_with_config(self, operation: Callable, config: RetryConfig,
                        *args, **kwargs) -> str:
        """Add operation with custom retry config."""
        item_id = str(uuid.uuid4())

        item = RetryItem(
            item_id=item_id,
            operation=operation,
            args=args,
            kwargs=kwargs,
            max_attempts=config.max_attempts,
            next_retry_at=time.time()
        )

        with self._lock:
            self._queue.append(item)

        return item_id

    def cancel(self, item_id: str) -> bool:
        """Cancel a retry item."""
        with self._lock:
            for i, item in enumerate(self._queue):
                if item.item_id == item_id:
                    item.status = RetryStatus.CANCELLED
                    self._queue.pop(i)
                    return True
        return False

    def get_status(self, item_id: str) -> Optional[RetryItem]:
        """Get status of a retry item."""
        with self._lock:
            for item in self._queue:
                if item.item_id == item_id:
                    return item
            for item in self._completed:
                if item.item_id == item_id:
                    return item
            for item in self._failed:
                if item.item_id == item_id:
                    return item
        return None

    def add_handler(self, event: str, handler: Callable):
        """Add an event handler."""
        if event in self._handlers:
            self._handlers[event].append(handler)

    def _process_loop(self):
        """Background retry processing loop."""
        while self._running:
            time.sleep(0.1)  # Check every 100ms

            with self._lock:
                now = time.time()

                # Find items ready for retry
                ready = [item for item in self._queue
                        if item.status == RetryStatus.PENDING and item.next_retry_at <= now]

            for item in ready:
                self._process_item(item)

    def _process_item(self, item: RetryItem):
        """Process a single retry item."""
        # Notify retry handlers
        for handler in self._handlers.get('retry', []):
            try:
                handler(item)
            except Exception as e:
                logger.error(f"Retry handler failed: {e}")

        item.status = RetryStatus.RETRYING
        item.attempts += 1

        try:
            result = item.operation(*item.args, **item.kwargs)
            item.result = result
            item.status = RetryStatus.COMPLETED

            with self._lock:
                self._queue.remove(item)
                self._completed.append(item)

            # Notify success handlers
            for handler in self._handlers.get('success', []):
                try:
                    handler(item)
                except Exception as e:
                    logger.error(f"Success handler failed: {e}")

            logger.info(f"Retry succeeded: {item.item_id} ({item.attempts} attempts)")

        except Exception as e:
            item.last_error = str(e)

            if item.attempts >= item.max_attempts:
                item.status = RetryStatus.FAILED

                with self._lock:
                    if item in self._queue:
                        self._queue.remove(item)
                    self._failed.append(item)

                # Notify failure handlers
                for handler in self._handlers.get('failure', []):
                    try:
                        handler(item)
                    except Exception as e:
                        logger.error(f"Failure handler failed: {e}")

                logger.warning(f"Retry failed permanently: {item.item_id} ({item.attempts} attempts)")
            else:
                # Schedule next retry
                item.status = RetryStatus.PENDING
                delay = BackoffCalculator.calculate(
                    self.config.backoff,
                    item.attempts,
                    self.config
                )
                item.next_retry_at = time.time() + delay

                logger.info(f"Retry scheduled: {item.item_id} in {delay:.1f}s (attempt {item.attempts + 1})")

    def retry_now(self, item_id: str) -> bool:
        """Immediately retry a pending item."""
        with self._lock:
            for item in self._queue:
                if item.item_id == item_id and item.status == RetryStatus.PENDING:
                    item.next_retry_at = time.time()
                    return True
        return False

    def get_stats(self) -> Dict:
        """Get retry queue statistics."""
        with self._lock:
            return {
                'pending': sum(1 for i in self._queue if i.status == RetryStatus.PENDING),
                'retrying': sum(1 for i in self._queue if i.status == RetryStatus.RETRYING),
                'completed': len(self._completed),
                'failed': len(self._failed)
            }

    def clear_completed(self):
        """Clear completed items."""
        with self._lock:
            self._completed.clear()


class RetryContext:
    """Context for retry operations."""

    def __init__(self, item_id: str, attempt: int, max_attempts: int):
        self.item_id = item_id
        self.attempt = attempt
        self.max_attempts = max_attempts
        self.metadata: Dict = {}


class RetryDecorator:
    """
    Decorator for adding retry behavior to functions.
    """

    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()

    def __call__(self, func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            attempt = 0
            last_error = None

            while attempt < self.config.max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    last_error = e

                    if attempt >= self.config.max_attempts:
                        raise

                    delay = BackoffCalculator.calculate(
                        self.config.backoff,
                        attempt,
                        self.config
                    )
                    logger.info(f"Retry {func.__name__} in {delay:.1f}s (attempt {attempt + 1})")
                    time.sleep(delay)

            raise last_error

        return wrapper


def retry(config: RetryConfig = None):
    """Decorator to add retry to a function."""
    return RetryDecorator(config)


def retry_with_backoff(strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
                      max_attempts: int = 3,
                      initial_delay: float = 1.0):
    """Create a retry decorator with specific settings."""
    config = RetryConfig(
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        backoff=strategy
    )
    return RetryDecorator(config)


# Global retry queue
_retry_queue = RetryQueue()


def get_retry_queue() -> RetryQueue:
    return _retry_queue


def enqueue_retry(operation: Callable, *args, **kwargs) -> str:
    """Add operation to global retry queue."""
    return _retry_queue.add(operation, *args, **kwargs)