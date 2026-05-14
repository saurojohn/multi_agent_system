"""Task retry strategies with configurable backoff."""

import time
import logging
from typing import Dict, Optional, Callable
from enum import Enum

logger = logging.getLogger('retry')


class RetryStrategy(Enum):
    FIXED = "fixed"           # Fixed delay between retries
    EXPONENTIAL = "exponential"  # Exponential backoff
    LINEAR = "linear"         # Linear increasing delay
    FIBONACCI = "fibonacci"   # Fibonacci backoff


class RetryPolicy:
    """Defines retry behavior for failed tasks."""

    def __init__(self,
                 strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
                 max_retries: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 jitter: float = 0.1):
        """
        Args:
            strategy: Backoff strategy
            max_retries: Maximum retry attempts
            base_delay: Base delay in seconds
            max_delay: Maximum delay cap
            jitter: Random jitter factor (0-1)
        """
        self.strategy = strategy
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number (0-indexed)."""
        if attempt <= 0:
            return 0

        # Apply strategy
        if self.strategy == RetryStrategy.FIXED:
            delay = self.base_delay
        elif self.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.base_delay * (2 ** (attempt - 1))
        elif self.strategy == RetryStrategy.LINEAR:
            delay = self.base_delay * attempt
        elif self.strategy == RetryStrategy.FIBONACCI:
            delay = self._fibonacci(attempt) * self.base_delay
        else:
            delay = self.base_delay

        # Cap at max
        delay = min(delay, self.max_delay)

        # Add jitter
        if self.jitter > 0:
            import random
            jitter_range = delay * self.jitter
            delay = delay + random.uniform(-jitter_range, jitter_range)

        return delay

    def _fibonacci(self, n: int) -> int:
        """Calculate fibonacci number."""
        if n <= 0:
            return 0
        elif n == 1:
            return 1

        a, b = 0, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    def should_retry(self, attempt: int, error: str = None) -> bool:
        """Check if should retry this attempt."""
        if attempt >= self.max_retries:
            return False

        # Could add custom logic for specific errors
        # e.g., don't retry on validation errors
        if error and 'validation' in error.lower():
            return False

        return True

    def get_next_delay(self, current_attempt: int) -> Optional[float]:
        """Get delay for next retry. Returns None if no more retries."""
        next_attempt = current_attempt + 1
        if self.should_retry(next_attempt):
            return self.get_delay(next_attempt)
        return None


class RetryManager:
    """Manages retry policies and execution."""

    def __init__(self):
        self._policies: Dict[str, RetryPolicy] = {}
        self._default_policy = RetryPolicy()
        self._retry_hooks: Dict[str, Callable] = {}  # hooks on retry events
        self._lock = None

    def set_policy(self, task_type: str, policy: RetryPolicy):
        """Set retry policy for a task type."""
        self._policies[task_type] = policy
        logger.info(f'Set retry policy for {task_type}: {policy.strategy.value}')

    def get_policy(self, task_type: str) -> RetryPolicy:
        """Get retry policy for task type."""
        return self._policies.get(task_type, self._default_policy)

    def execute_with_retry(self, func: Callable, task_type: str = None,
                           on_retry: Callable = None, *args, **kwargs):
        """
        Execute function with retry logic.
        Returns (success, result_or_error, attempts)
        """
        policy = self.get_policy(task_type or 'default')
        attempt = 0
        last_error = None

        while True:
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f'Succeeded on retry attempt {attempt}')
                return True, result, attempt + 1
            except Exception as e:
                last_error = str(e)
                attempt += 1

                if not policy.should_retry(attempt, last_error):
                    logger.warning(f'No more retries after {attempt} attempts: {last_error}')
                    return False, last_error, attempt

                delay = policy.get_delay(attempt)
                logger.info(f'Retry {attempt}/{policy.max_retries} in {delay:.2f}s: {last_error}')

                if on_retry:
                    on_retry(task_type, attempt, last_error)

                # Call retry hooks
                if task_type and task_type in self._retry_hooks:
                    try:
                        self._retry_hooks[task_type](attempt, last_error)
                    except Exception as e:
                        logger.error(f'Retry hook failed: {e}')

                time.sleep(delay)

    def register_retry_hook(self, task_type: str, hook: Callable):
        """Register hook to be called on retry."""
        self._retry_hooks[task_type] = hook

    def get_retry_stats(self) -> Dict:
        """Get retry statistics."""
        return {
            'configured_types': len(self._policies),
            'default_policy': {
                'strategy': self._default_policy.strategy.value,
                'max_retries': self._default_policy.max_retries,
                'base_delay': self._default_policy.base_delay
            }
        }


class RetryPolicyBuilder:
    """Builder for creating retry policies."""

    def __init__(self):
        self._strategy = RetryStrategy.EXPONENTIAL
        self._max_retries = 3
        self._base_delay = 1.0
        self._max_delay = 60.0
        self._jitter = 0.1

    def with_strategy(self, strategy: RetryStrategy) -> 'RetryPolicyBuilder':
        self._strategy = strategy
        return self

    def with_max_retries(self, max_retries: int) -> 'RetryPolicyBuilder':
        self._max_retries = max_retries
        return self

    def with_base_delay(self, delay: float) -> 'RetryPolicyBuilder':
        self._base_delay = delay
        return self

    def with_max_delay(self, delay: float) -> 'RetryPolicyBuilder':
        self._max_delay = delay
        return self

    def with_jitter(self, jitter: float) -> 'RetryPolicyBuilder':
        self._jitter = jitter
        return self

    def build(self) -> RetryPolicy:
        return RetryPolicy(
            strategy=self._strategy,
            max_retries=self._max_retries,
            base_delay=self._base_delay,
            max_delay=self._max_delay,
            jitter=self._jitter
        )


# Pre-built policies
POLICIES = {
    'fast': RetryPolicyBuilder().with_strategy(RetryStrategy.FIXED).with_max_retries(2).with_base_delay(0.5).build(),
    'standard': RetryPolicyBuilder().with_strategy(RetryStrategy.EXPONENTIAL).with_max_retries(3).with_base_delay(1.0).build(),
    'slow': RetryPolicyBuilder().with_strategy(RetryStrategy.EXPONENTIAL).with_max_retries(5).with_base_delay(2.0).with_max_delay(120).build(),
    'aggressive': RetryPolicyBuilder().with_strategy(RetryStrategy.FIBONACCI).with_max_retries(10).with_base_delay(0.5).build(),
}


# Global retry manager
_retry_manager = RetryManager()


def get_retry_manager() -> RetryManager:
    return _retry_manager


def create_retry_policy(**kwargs) -> RetryPolicy:
    """Create retry policy from kwargs."""
    builder = RetryPolicyBuilder()
    if 'strategy' in kwargs:
        builder.with_strategy(kwargs['strategy'])
    if 'max_retries' in kwargs:
        builder.with_max_retries(kwargs['max_retries'])
    if 'base_delay' in kwargs:
        builder.with_base_delay(kwargs['base_delay'])
    if 'max_delay' in kwargs:
        builder.with_max_delay(kwargs['max_delay'])
    if 'jitter' in kwargs:
        builder.with_jitter(kwargs['jitter'])
    return builder.build()