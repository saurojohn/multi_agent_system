"""Request/Response middleware chain for processing pipeline."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('middleware')


class MiddlewarePhase(Enum):
    """Phases of middleware execution."""
    PRE_PROCESS = "pre_process"      # Before main processing
    POST_PROCESS = "post_process"    # After main processing
    ERROR = "error"                   # On error
    FINALLY = "finally"              # Always runs


@dataclass
class MiddlewareResult:
    """Result from middleware execution."""
    success: bool
    response: Any = None
    error: Optional[str] = None
    metadata: Dict = field(default_factory=dict)


@dataclass
class MiddlewareContext:
    """Context passed through middleware chain."""
    request_id: str
    request: Any
    response: Any = None
    state: Dict = field(default_factory=dict)
    metadata: Dict = field(default_factory=dict)


class Middleware:
    """
    Base middleware class.
    """

    def __init__(self, name: str = None):
        self.name = name or self.__class__.__name__

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        """Process the middleware."""
        raise NotImplementedError


class MiddlewareChain:
    """
    A chain of middleware to process requests.
    """

    def __init__(self):
        self._middleware: List[tuple] = []  # (phase, middleware)
        self._lock = threading.Lock()

    def add(self, middleware: Middleware, phase: MiddlewarePhase = MiddlewarePhase.PRE_PROCESS):
        """Add middleware to the chain."""
        with self._lock:
            self._middleware.append((phase, middleware))

    def insert_before(self, before_name: str, middleware: Middleware,
                     phase: MiddlewarePhase = MiddlewarePhase.PRE_PROCESS):
        """Insert middleware before another."""
        with self._lock:
            for i, (p, m) in enumerate(self._middleware):
                if m.name == before_name:
                    self._middleware.insert(i, (phase, middleware))
                    return
            # Not found, add at end
            self._middleware.append((phase, middleware))

    def insert_after(self, after_name: str, middleware: Middleware,
                    phase: MiddlewarePhase = MiddlewarePhase.PRE_PROCESS):
        """Insert middleware after another."""
        with self._lock:
            for i, (p, m) in enumerate(self._middleware):
                if m.name == after_name:
                    self._middleware.insert(i + 1, (phase, middleware))
                    return
            # Not found, add at end
            self._middleware.append((phase, middleware))

    def remove(self, middleware_name: str) -> bool:
        """Remove middleware by name."""
        with self._lock:
            for i, (p, m) in enumerate(self._middleware):
                if m.name == middleware_name:
                    del self._middleware[i]
                    return True
        return False

    def execute(self, context: MiddlewareContext,
               final_handler: Callable[[MiddlewareContext], Any] = None) -> MiddlewareResult:
        """Execute the middleware chain."""
        try:
            # Pre-process phase
            for phase, middleware in self._middleware:
                if phase == MiddlewarePhase.PRE_PROCESS:
                    result = middleware.process(context)
                    if not result.success:
                        return result

            # Main processing
            if final_handler:
                try:
                    context.response = final_handler(context)
                except Exception as e:
                    context.state['error'] = str(e)
                    # Error phase
                    for phase, middleware in self._middleware:
                        if phase == MiddlewarePhase.ERROR:
                            middleware.process(context)
                    raise

            # Post-process phase
            for phase, middleware in self._middleware:
                if phase == MiddlewarePhase.POST_PROCESS:
                    middleware.process(context)

            return MiddlewareResult(success=True, response=context.response)

        except Exception as e:
            return MiddlewareResult(success=False, error=str(e))
        finally:
            # Finally phase
            for phase, middleware in self._middleware:
                if phase == MiddlewarePhase.FINALLY:
                    try:
                        middleware.process(context)
                    except:
                        pass


class LoggingMiddleware(Middleware):
    """Middleware for logging requests."""

    def __init__(self, log_request: bool = True, log_response: bool = True):
        super().__init__("LoggingMiddleware")
        self.log_request = log_request
        self.log_response = log_response

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        if self.log_request:
            logger.info(f"Request: {context.request_id}")

        if self.log_response and context.response is not None:
            logger.info(f"Response: {context.request_id} - {context.response}")

        return MiddlewareResult(success=True)


class AuthMiddleware(Middleware):
    """Middleware for authentication."""

    def __init__(self, auth_fn: Callable[[Any], bool] = None):
        super().__init__("AuthMiddleware")
        self.auth_fn = auth_fn or (lambda x: True)

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        if not self.auth_fn(context.request):
            return MiddlewareResult(success=False, error="Authentication failed")

        return MiddlewareResult(success=True)


class ValidationMiddleware(Middleware):
    """Middleware for request validation."""

    def __init__(self, validator: Callable[[Any], bool] = None,
                 error_message: str = "Validation failed"):
        super().__init__("ValidationMiddleware")
        self.validator = validator
        self.error_message = error_message

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        if self.validator and not self.validator(context.request):
            return MiddlewareResult(success=False, error=self.error_message)

        return MiddlewareResult(success=True)


class RateLimitMiddleware(Middleware):
    """Middleware for rate limiting."""

    def __init__(self, limiter):
        super().__init__("RateLimitMiddleware")
        self.limiter = limiter

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        if not self.limiter.allow():
            return MiddlewareResult(
                success=False,
                error="Rate limit exceeded"
            )

        return MiddlewareResult(success=True)


class CacheMiddleware(Middleware):
    """Middleware for caching responses."""

    def __init__(self, cache_get: Callable[[str], Any] = None,
                 cache_set: Callable[[str, Any], None] = None,
                 cache_key_fn: Callable[[Any], str] = None):
        super().__init__("CacheMiddleware")
        self.cache_get = cache_get or (lambda k: None)
        self.cache_set = cache_set or (lambda k, v: None)
        self.cache_key_fn = cache_key_fn or (lambda x: str(hash(str(x))))

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        cache_key = self.cache_key_fn(context.request)

        cached = self.cache_get(cache_key)
        if cached is not None:
            context.response = cached
            context.metadata['cache_hit'] = True
            return MiddlewareResult(success=True, response=cached)

        context.metadata['cache_hit'] = False
        return MiddlewareResult(success=True)


class CompressionMiddleware(Middleware):
    """Middleware for compression."""

    def __init__(self, compress_fn: Callable[[Any], bytes] = None,
                 decompress_fn: Callable[[bytes], Any] = None):
        super().__init__("CompressionMiddleware")
        self.compress_fn = compress_fn or (lambda x: x)
        self.decompress_fn = decompress_fn or (lambda x: x)

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        # Compress response
        if context.response is not None:
            try:
                context.response = self.compress_fn(context.response)
                context.metadata['compressed'] = True
            except:
                pass

        return MiddlewareResult(success=True)


class MetricsMiddleware(Middleware):
    """Middleware for recording metrics."""

    def __init__(self, metrics_fn: Callable[[str, float], None] = None):
        super().__init__("MetricsMiddleware")
        self.metrics_fn = metrics_fn or (lambda n, d: None)
        self.start_time = None

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        if context.response is None:
            self.start_time = time.time()
        else:
            duration = time.time() - self.start_time if self.start_time else 0
            self.metrics_fn(context.request_id, duration)

        return MiddlewareResult(success=True)


class CircuitBreakerMiddleware(Middleware):
    """Middleware for circuit breaker pattern."""

    def __init__(self, circuit_breaker):
        super().__init__("CircuitBreakerMiddleware")
        self.cb = circuit_breaker

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        if not self.cb.allow_request():
            return MiddlewareResult(
                success=False,
                error="Circuit breaker open"
            )

        return MiddlewareResult(success=True)


class RetryMiddleware(Middleware):
    """Middleware for automatic retry."""

    def __init__(self, max_retries: int = 3,
                 retry_delay: float = 0.5,
                 retry_fn: Callable[[Any], bool] = None):
        super().__init__("RetryMiddleware")
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_fn = retry_fn or (lambda x: True)

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        attempt = context.metadata.get('retry_attempt', 0)

        while attempt < self.max_retries:
            if self.retry_fn(context.response):
                break

            attempt += 1
            context.metadata['retry_attempt'] = attempt
            time.sleep(self.retry_delay * attempt)

        return MiddlewareResult(success=True)


class RequestIDMiddleware(Middleware):
    """Middleware for adding request ID."""

    def __init__(self):
        super().__init__("RequestIDMiddleware")

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        if 'request_id' not in context.metadata:
            import uuid
            context.metadata['request_id'] = str(uuid.uuid4())

        return MiddlewareResult(success=True)


class CORSMiddleware(Middleware):
    """Middleware for CORS handling."""

    def __init__(self, allowed_origins: List[str] = None,
                 allowed_methods: List[str] = None):
        super().__init__("CORSMiddleware")
        self.allowed_origins = allowed_origins or ['*']
        self.allowed_methods = allowed_methods or ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']

    def process(self, context: MiddlewareContext) -> MiddlewareResult:
        context.metadata['cors_headers'] = {
            'Access-Control-Allow-Origin': ', '.join(self.allowed_origins),
            'Access-Control-Allow-Methods': ', '.join(self.allowed_methods)
        }

        return MiddlewareResult(success=True)


class MiddlewareBuilder:
    """Builder for creating middleware chains."""

    def __init__(self):
        self._chain = MiddlewareChain()

    def add_logging(self) -> 'MiddlewareBuilder':
        """Add logging middleware."""
        self._chain.add(LoggingMiddleware())
        return self

    def add_auth(self, auth_fn: Callable = None) -> 'MiddlewareBuilder':
        """Add auth middleware."""
        self._chain.add(AuthMiddleware(auth_fn))
        return self

    def add_validation(self, validator: Callable, error_msg: str = None) -> 'MiddlewareBuilder':
        """Add validation middleware."""
        self._chain.add(ValidationMiddleware(validator, error_msg))
        return self

    def add_cache(self, cache_get: Callable = None, cache_set: Callable = None) -> 'MiddlewareBuilder':
        """Add cache middleware."""
        self._chain.add(CacheMiddleware(cache_get, cache_set))
        return self

    def add_metrics(self, metrics_fn: Callable = None) -> 'MiddlewareBuilder':
        """Add metrics middleware."""
        self._chain.add(MetricsMiddleware(metrics_fn))
        return self

    def add_request_id(self) -> 'MiddlewareBuilder':
        """Add request ID middleware."""
        self._chain.add(RequestIDMiddleware())
        return self

    def build(self) -> MiddlewareChain:
        """Build the middleware chain."""
        return self._chain


# Global helper
def create_middleware_chain() -> MiddlewareBuilder:
    """Create a new middleware chain builder."""
    return MiddlewareBuilder()