"""Request lifecycle management for tracking requests through the system."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('lifecycle')


class LifecycleState(Enum):
    """Lifecycle states for a request."""
    CREATED = "created"
    VALIDATED = "validated"
    AUTHENTICATED = "authenticated"
    AUTHORIZED = "authorized"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


@dataclass
class LifecycleEvent:
    """An event in the request lifecycle."""
    event_id: str
    request_id: str
    timestamp: float
    state: LifecycleState
    source: str  # Component that generated the event
    data: Dict = field(default_factory=dict)
    duration_ms: float = 0


@dataclass
class RequestLifecycle:
    """Lifecycle tracking for a request."""
    request_id: str
    request_type: str
    created_at: float
    current_state: LifecycleState
    events: List[LifecycleEvent] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    context: Dict = field(default_factory=dict)


class LifecycleManager:
    """
    Manages request lifecycles across the system.
    """

    def __init__(self):
        self._lifecycles: Dict[str, RequestLifecycle] = {}
        self._lock = threading.RLock()
        self._state_handlers: Dict[LifecycleState, List[Callable]] = defaultdict(list)
        self._max_events = 1000

    def create_lifecycle(self, request_id: str, request_type: str,
                       metadata: Dict = None) -> RequestLifecycle:
        """Create a new lifecycle for a request."""
        lifecycle = RequestLifecycle(
            request_id=request_id,
            request_type=request_type,
            created_at=time.time(),
            current_state=LifecycleState.CREATED,
            metadata=metadata or {}
        )

        with self._lock:
            self._lifecycles[request_id] = lifecycle

        # Record creation event
        self._record_event(request_id, LifecycleState.CREATED, "lifecycle_manager")

        return lifecycle

    def transition_to(self, request_id: str, state: LifecycleState,
                    source: str, data: Dict = None, duration_ms: float = 0) -> bool:
        """Transition a request to a new state."""
        with self._lock:
            if request_id not in self._lifecycles:
                return False

            lifecycle = self._lifecycles[request_id]
            old_state = lifecycle.current_state
            lifecycle.current_state = state

        # Record event
        self._record_event(request_id, state, source, data, duration_ms)

        # Notify handlers
        self._notify_handlers(state, lifecycle)

        logger.debug(f"Lifecycle {request_id}: {old_state.value} -> {state.value}")
        return True

    def _record_event(self, request_id: str, state: LifecycleState,
                    source: str, data: Dict = None, duration_ms: float = 0):
        """Record an event in the lifecycle."""
        event = LifecycleEvent(
            event_id=str(uuid.uuid4()),
            request_id=request_id,
            timestamp=time.time(),
            state=state,
            source=source,
            data=data or {},
            duration_ms=duration_ms
        )

        with self._lock:
            if request_id in self._lifecycles:
                lifecycle = self._lifecycles[request_id]
                lifecycle.events.append(event)

                # Limit events
                if len(lifecycle.events) > self._max_events:
                    lifecycle.events = lifecycle.events[-self._max_events:]

    def _notify_handlers(self, state: LifecycleState, lifecycle: RequestLifecycle):
        """Notify handlers of state change."""
        handlers = self._state_handlers.get(state, [])
        for handler in handlers:
            try:
                handler(lifecycle)
            except Exception as e:
                logger.error(f"Lifecycle handler failed: {e}")

    def add_state_handler(self, state: LifecycleState, handler: Callable):
        """Add a handler for a specific state."""
        self._state_handlers[state].append(handler)

    def get_lifecycle(self, request_id: str) -> Optional[RequestLifecycle]:
        """Get lifecycle for a request."""
        with self._lock:
            return self._lifecycles.get(request_id)

    def get_events(self, request_id: str) -> List[LifecycleEvent]:
        """Get all events for a request."""
        lifecycle = self.get_lifecycle(request_id)
        return lifecycle.events if lifecycle else []

    def get_current_state(self, request_id: str) -> Optional[LifecycleState]:
        """Get current state of a request."""
        lifecycle = self.get_lifecycle(request_id)
        return lifecycle.current_state if lifecycle else None

    def cancel(self, request_id: str, source: str = "lifecycle_manager") -> bool:
        """Cancel a request lifecycle."""
        return self.transition_to(request_id, LifecycleState.CANCELLED, source)

    def complete(self, request_id: str, source: str = "lifecycle_manager") -> bool:
        """Complete a request lifecycle."""
        return self.transition_to(request_id, LifecycleState.COMPLETED, source)

    def fail(self, request_id: str, source: str = "lifecycle_manager",
            error: str = None) -> bool:
        """Fail a request lifecycle."""
        return self.transition_to(
            request_id,
            LifecycleState.FAILED,
            source,
            {'error': error}
        )

    def cleanup_old(self, max_age_seconds: float = 3600) -> int:
        """Clean up old lifecycle records."""
        cutoff = time.time() - max_age_seconds
        removed = 0

        with self._lock:
            old = [
                request_id for request_id, lifecycle in self._lifecycles.items()
                if lifecycle.created_at < cutoff and
                   lifecycle.current_state in (LifecycleState.COMPLETED, LifecycleState.FAILED, LifecycleState.CANCELLED)
            ]

            for request_id in old:
                del self._lifecycles[request_id]
                removed += 1

        return removed

    def get_stats(self) -> Dict:
        """Get lifecycle statistics."""
        with self._lock:
            by_state = defaultdict(int)
            for lifecycle in self._lifecycles.values():
                by_state[lifecycle.current_state.value] += 1

            return {
                'total_lifecycles': len(self._lifecycles),
                'by_state': dict(by_state),
                'handlers_registered': sum(len(h) for h in self._state_handlers.values())
            }


class LifecycleContext:
    """
    Context manager for tracking lifecycle within a scope.
    """

    def __init__(self, manager: LifecycleManager, request_id: str):
        self.manager = manager
        self.request_id = request_id

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class LifecycleTracker:
    """
    Decorator for tracking lifecycle of a function.
    """

    def __init__(self, manager: LifecycleManager = None):
        self.manager = manager or _default_manager

    def track(self, request_id: str = None, request_type: str = None):
        """Decorator to track function execution."""
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                rid = request_id or str(uuid.uuid4())
                rtype = request_type or func.__name__

                self.manager.create_lifecycle(rid, rtype)
                self.manager.transition_to(rid, LifecycleState.PROCESSING, func.__module__)

                try:
                    result = func(*args, **kwargs)
                    self.manager.complete(rid, func.__module__)
                    return result
                except Exception as e:
                    self.manager.fail(rid, func.__module__, str(e))
                    raise

            return wrapper
        return decorator


class LifecycleProbe:
    """
    Probe for checking lifecycle state.
    """

    def __init__(self, manager: LifecycleManager = None):
        self.manager = manager or _default_manager

    def is_complete(self, request_id: str) -> bool:
        """Check if request is complete."""
        state = self.manager.get_current_state(request_id)
        return state == LifecycleState.COMPLETED if state else False

    def is_failed(self, request_id: str) -> bool:
        """Check if request failed."""
        state = self.manager.get_current_state(request_id)
        return state == LifecycleState.FAILED if state else False

    def is_terminal(self, request_id: str) -> bool:
        """Check if request is in terminal state."""
        state = self.manager.get_current_state(request_id)
        if not state:
            return False
        return state in (LifecycleState.COMPLETED, LifecycleState.FAILED,
                       LifecycleState.TIMED_OUT, LifecycleState.CANCELLED)

    def get_duration(self, request_id: str) -> float:
        """Get lifecycle duration in seconds."""
        lifecycle = self.manager.get_lifecycle(request_id)
        if not lifecycle:
            return 0
        return time.time() - lifecycle.created_at

    def get_event_count(self, request_id: str) -> int:
        """Get number of events in lifecycle."""
        return len(self.manager.get_events(request_id))


# Global lifecycle manager
_default_manager = LifecycleManager()


def get_lifecycle_manager() -> LifecycleManager:
    return _default_manager


def create_lifecycle(request_id: str, request_type: str, **kwargs) -> RequestLifecycle:
    """Create a new lifecycle."""
    return _default_manager.create_lifecycle(request_id, request_type, **kwargs)


def track_function(func: Callable = None, **kwargs) -> Callable:
    """Decorator for tracking function lifecycle."""
    tracker = LifecycleTracker()
    if func:
        return tracker.track(**kwargs)(func)
    return tracker.track(**kwargs)