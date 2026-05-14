"""Event sourcing for audit trail and state reconstruction."""

import json
import logging
import time
import uuid
import threading
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('event_sourcing')


class EventType(Enum):
    """Event types for the system."""
    # Task events
    TASK_SUBMITTED = "task.submitted"
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_CANCELLED = "task.cancelled"
    TASK_RETRY = "task.retry"
    TASK_TIMEOUT = "task.timeout"

    # Worker events
    WORKER_REGISTERED = "worker.registered"
    WORKER_UNREGISTERED = "worker.unregistered"
    WORKER_HEARTBEAT = "worker.heartbeat"
    WORKER_IDLE = "worker.idle"
    WORKER_BUSY = "worker.busy"
    WORKER_ERROR = "worker.error"

    # System events
    SYSTEM_STARTUP = "system.startup"
    SYSTEM_SHUTDOWN = "system.shutdown"
    CONFIG_CHANGED = "config.changed"
    SCALING_TRIGGERED = "scaling.triggered"


@dataclass
class Event:
    """An immutable event in the system."""
    event_id: str
    event_type: str
    timestamp: float
    aggregate_id: str  # e.g., task_id or worker_id
    aggregate_type: str  # e.g., "task" or "worker"
    payload: Dict
    metadata: Dict = field(default_factory=dict)
    causation_id: Optional[str] = None  # link to causing event
    correlation_id: Optional[str] = None  # link related events

    @classmethod
    def create(cls, event_type: str, aggregate_id: str,
              aggregate_type: str, payload: Dict,
              metadata: Dict = None,
              causation_id: str = None,
              correlation_id: str = None) -> 'Event':
        """Create a new event."""
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            timestamp=time.time(),
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            payload=payload,
            metadata=metadata or {},
            causation_id=causation_id,
            correlation_id=correlation_id
        )


@dataclass
class AggregateSnapshot:
    """Snapshot of aggregate state at a point in time."""
    aggregate_id: str
    aggregate_type: str
    version: int
    timestamp: float
    state: Dict


class EventStore:
    """
    Event store for persisting and querying events.
    Events are append-only and immutable.
    """

    def __init__(self, persist_dir: str = "/tmp/multi_agent_events"):
        self.persist_dir = persist_dir
        self._events: List[Event] = []
        self._snapshots: Dict[str, AggregateSnapshot] = {}
        self._lock = threading.RLock()
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)

    def append(self, event: Event) -> str:
        """Append an event to the store."""
        with self._lock:
            self._events.append(event)
            logger.debug(f"Appended event: {event.event_id} ({event.event_type})")
            self._notify_subscribers(event)
        return event.event_id

    def append_batch(self, events: List[Event]) -> List[str]:
        """Append multiple events."""
        with self._lock:
            event_ids = []
            for event in events:
                self._events.append(event)
                event_ids.append(event.event_id)
            logger.debug(f"Appended {len(events)} events")
            for event in events:
                self._notify_subscribers(event)
        return event_ids

    def get_events(self, aggregate_id: str,
                   aggregate_type: Optional[str] = None) -> List[Event]:
        """Get all events for an aggregate."""
        with self._lock:
            events = [e for e in self._events if e.aggregate_id == aggregate_id]
            if aggregate_type:
                events = [e for e in events if e.aggregate_type == aggregate_type]
            return events

    def get_events_by_type(self, event_type: str) -> List[Event]:
        """Get all events of a specific type."""
        with self._lock:
            return [e for e in self._events if e.event_type == event_type]

    def get_events_by_correlation(self, correlation_id: str) -> List[Event]:
        """Get all events with the same correlation ID."""
        with self._lock:
            return [e for e in self._events if e.correlation_id == correlation_id]

    def get_events_in_range(self, start_time: float,
                            end_time: float) -> List[Event]:
        """Get events within a time range."""
        with self._lock:
            return [e for e in self._events
                    if start_time <= e.timestamp <= end_time]

    def get_events_since(self, timestamp: float) -> List[Event]:
        """Get all events since a given timestamp."""
        return self.get_events_in_range(timestamp, time.time())

    def replay_events(self, aggregate_id: str,
                      aggregate_type: str,
                      event_handlers: Dict[str, Callable]) -> Dict:
        """
        Replay events to reconstruct aggregate state.
        event_handlers: {event_type: handler_function}
        """
        events = self.get_events(aggregate_id, aggregate_type)
        state = {}

        for event in events:
            handler = event_handlers.get(event.event_type)
            if handler:
                state = handler(state, event)

        return state

    def create_snapshot(self, aggregate_id: str,
                       aggregate_type: str,
                       state: Dict,
                       version: int = None) -> AggregateSnapshot:
        """Create a snapshot of aggregate state."""
        events = self.get_events(aggregate_id, aggregate_type)
        v = version or len(events)

        snapshot = AggregateSnapshot(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            version=v,
            timestamp=time.time(),
            state=state
        )

        with self._lock:
            self._snapshots[aggregate_id] = snapshot

        return snapshot

    def get_snapshot(self, aggregate_id: str) -> Optional[AggregateSnapshot]:
        """Get the latest snapshot for an aggregate."""
        with self._lock:
            return self._snapshots.get(aggregate_id)

    def replay_from_snapshot(self, aggregate_id: str, aggregate_type: str,
                             event_handlers: Dict[str, Callable]) -> Dict:
        """Replay events from the last snapshot."""
        snapshot = self.get_snapshot(aggregate_id)
        start_version = 0

        if snapshot:
            state = snapshot.state
            start_version = snapshot.version
        else:
            state = {}

        events = self.get_events(aggregate_id, aggregate_type)
        events = [e for e in events if e.aggregate_id == aggregate_id]

        for event in events:
            if start_version > 0:
                start_version -= 1
                continue
            handler = event_handlers.get(event.event_type)
            if handler:
                state = handler(state, event)

        return state

    def subscribe(self, event_type: str, callback: Callable):
        """Subscribe to events of a specific type."""
        self._subscribers[event_type].append(callback)

    def _notify_subscribers(self, event: Event):
        """Notify subscribers of a new event."""
        callbacks = self._subscribers.get(event.event_type, [])
        for callback in callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Subscriber callback failed: {e}")

    def get_stats(self) -> Dict:
        """Get event store statistics."""
        with self._lock:
            by_type = defaultdict(int)
            for event in self._events:
                by_type[event.event_type] += 1

            return {
                'total_events': len(self._events),
                'by_type': dict(by_type),
                'snapshots': len(self._snapshots),
                'subscribers': len(self._subscribers)
            }

    def persist_to_disk(self, filepath: str = None):
        """Persist events to disk."""
        import os
        filepath = filepath or f"{self.persist_dir}/events.json"

        os.makedirs(self.persist_dir, exist_ok=True)

        with self._lock:
            data = {
                'events': [
                    {
                        'event_id': e.event_id,
                        'event_type': e.event_type,
                        'timestamp': e.timestamp,
                        'aggregate_id': e.aggregate_id,
                        'aggregate_type': e.aggregate_type,
                        'payload': e.payload,
                        'metadata': e.metadata,
                        'causation_id': e.causation_id,
                        'correlation_id': e.correlation_id
                    }
                    for e in self._events
                ],
                'snapshots': {
                    aid: {
                        'aggregate_id': s.aggregate_id,
                        'aggregate_type': s.aggregate_type,
                        'version': s.version,
                        'timestamp': s.timestamp,
                        'state': s.state
                    }
                    for aid, s in self._snapshots.items()
                }
            }

            with open(filepath, 'w') as f:
                json.dump(data, f)

            logger.info(f"Persisted {len(self._events)} events to {filepath}")

    def load_from_disk(self, filepath: str = None):
        """Load events from disk."""
        filepath = filepath or f"{self.persist_dir}/events.json"

        if not os.path.exists(filepath):
            return

        with self._lock:
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)

                self._events = [
                    Event(
                        event_id=e['event_id'],
                        event_type=e['event_type'],
                        timestamp=e['timestamp'],
                        aggregate_id=e['aggregate_id'],
                        aggregate_type=e['aggregate_type'],
                        payload=e['payload'],
                        metadata=e.get('metadata', {}),
                        causation_id=e.get('causation_id'),
                        correlation_id=e.get('correlation_id')
                    )
                    for e in data.get('events', [])
                ]

                self._snapshots = {
                    aid: AggregateSnapshot(
                        aggregate_id=s['aggregate_id'],
                        aggregate_type=s['aggregate_type'],
                        version=s['version'],
                        timestamp=s['timestamp'],
                        state=s['state']
                    )
                    for aid, s in data.get('snapshots', {}).items()
                }

                logger.info(f"Loaded {len(self._events)} events from {filepath}")
            except Exception as e:
                logger.error(f"Failed to load events: {e}")


class EventPublisher:
    """
    Publishes events to external systems.
    Supports multiple output channels.
    """

    def __init__(self):
        self._handlers: List[Callable] = []
        self._filter: Optional[Callable] = None

    def add_handler(self, handler: Callable):
        """Add an event handler."""
        self._handlers.append(handler)

    def set_filter(self, event_filter: Callable):
        """Set a filter function for events."""
        self._filter = event_filter

    def publish(self, event: Event):
        """Publish an event to all handlers."""
        if self._filter and not self._filter(event):
            return

        for handler in self._handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Event handler failed: {e}")

    def publish_to_queue(self, event: Event, queue: Any):
        """Publish event to a message queue."""
        try:
            queue.put({
                'event_id': event.event_id,
                'event_type': event.event_type,
                'timestamp': event.timestamp,
                'aggregate_id': event.aggregate_id,
                'payload': event.payload
            })
        except Exception as e:
            logger.error(f"Failed to publish to queue: {e}")


class EventQueryBuilder:
    """
    Query builder for event sourcing.
    Allows flexible querying of event history.
    """

    def __init__(self, event_store: EventStore):
        self.event_store = event_store
        self._filters: List[Callable] = []
        self._limit_count: Optional[int] = None
        self._time_range: Optional[tuple] = None

    def by_aggregate(self, aggregate_id: str,
                    aggregate_type: str = None) -> 'EventQueryBuilder':
        """Filter by aggregate ID."""
        def filter_fn(event):
            if aggregate_type and event.aggregate_type != aggregate_type:
                return False
            return event.aggregate_id == aggregate_id

        self._filters.append(filter_fn)
        return self

    def by_type(self, event_type: str) -> 'EventQueryBuilder':
        """Filter by event type."""
        self._filters.append(lambda e: e.event_type == event_type)
        return self

    def by_types(self, event_types: List[str]) -> 'EventQueryBuilder':
        """Filter by multiple event types."""
        self._filters.append(lambda e: e.event_type in event_types)
        return self

    def by_correlation(self, correlation_id: str) -> 'EventQueryBuilder':
        """Filter by correlation ID."""
        self._filters.append(lambda e: e.correlation_id == correlation_id)
        return self

    def in_time_range(self, start: float, end: float) -> 'EventQueryBuilder':
        """Filter by time range."""
        self._time_range = (start, end)
        return self

    def limit(self, count: int) -> 'EventQueryBuilder':
        """Limit number of results."""
        self._limit_count = count
        return self

    def execute(self) -> List[Event]:
        """Execute the query."""
        events = self.event_store._events

        # Apply time range first
        if self._time_range:
            start, end = self._time_range
            events = [e for e in events if start <= e.timestamp <= end]

        # Apply filters
        for filter_fn in self._filters:
            events = [e for e in events if filter_fn(e)]

        # Apply limit
        if self._limit_count:
            events = events[-self._limit_count:]

        return events


# Global event store
_event_store = EventStore()


def get_event_store() -> EventStore:
    return _event_store


def publish_event(event_type: str, aggregate_id: str,
                  aggregate_type: str, payload: Dict,
                  metadata: Dict = None,
                  causation_id: str = None,
                  correlation_id: str = None) -> Event:
    """Helper to publish an event."""
    event = Event.create(
        event_type=event_type,
        aggregate_id=aggregate_id,
        aggregate_type=aggregate_type,
        payload=payload,
        metadata=metadata,
        causation_id=causation_id,
        correlation_id=correlation_id
    )
    _event_store.append(event)
    return event