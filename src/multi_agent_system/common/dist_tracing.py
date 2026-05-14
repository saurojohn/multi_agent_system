"""Distributed tracing for request tracking across services."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('tracing')


class TraceState(Enum):
    """Trace state."""
    STARTED = "started"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class SpanContext:
    """Context for a trace span."""
    trace_id: str
    span_id: str
    parent_span_id: str = None
    baggage: Dict[str, str] = field(default_factory=dict)


@dataclass
class Span:
    """A trace span."""
    span_id: str
    trace_id: str
    name: str
    service_name: str
    start_time: float
    end_time: float = None
    duration_ms: float = None
    state: TraceState = TraceState.STARTED
    tags: Dict[str, str] = field(default_factory=dict)
    logs: List[Dict] = field(default_factory=list)
    error: str = None


@dataclass
class Trace:
    """A complete trace."""
    trace_id: str
    start_time: float = field(default_factory=time.time)
    spans: List[Span] = field(default_factory=list)
    end_time: float = None
    total_duration_ms: float = None
    metadata: Dict = field(default_factory=dict)


class SpanBuilder:
    """Builder for creating spans."""

    def __init__(self, tracer: 'Tracer', name: str):
        self.tracer = tracer
        self._name = name
        self._service_name = "unknown"
        self._tags: Dict[str, str] = {}
        self._start_time = time.time()

    def service_name(self, name: str) -> 'SpanBuilder':
        """Set service name."""
        self._service_name = name
        return self

    def tag(self, key: str, value: str) -> 'SpanBuilder':
        """Add a tag."""
        self._tags[key] = value
        return self

    def start_time(self, timestamp: float) -> 'SpanBuilder':
        """Set start time."""
        self._start_time = timestamp
        return self

    def start(self) -> 'SpanContext':
        """Start the span."""
        return self.tracer.start_span(
            self._name,
            service_name=self._service_name,
            tags=self._tags,
            start_time=self._start_time
        )


class Tracer:
    """
    Distributed tracing implementation.
    """

    def __init__(self, service_name: str = "unknown"):
        self.service_name = service_name
        self._active_spans: Dict[str, Span] = {}
        self._completed_traces: Dict[str, Trace] = {}
        self._lock = threading.RLock()
        self._exporters: List[Callable] = []
        self._sampler = None  # Sampling strategy

    def start_span(self, name: str, trace_id: str = None,
                   parent_span_id: str = None,
                   service_name: str = None,
                   tags: Dict[str, str] = None,
                   start_time: float = None) -> SpanContext:
        """Start a new span."""
        trace_id = trace_id or str(uuid.uuid4())
        span_id = str(uuid.uuid4())

        context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id
        )

        span = Span(
            span_id=span_id,
            trace_id=trace_id,
            name=name,
            service_name=service_name or self.service_name,
            start_time=start_time or time.time(),
            tags=tags or {}
        )

        with self._lock:
            self._active_spans[span_id] = span

        return context

    def end_span(self, context: SpanContext, tags: Dict[str, str] = None,
                error: str = None):
        """End a span."""
        span_id = context.span_id

        with self._lock:
            if span_id not in self._active_spans:
                return

            span = self._active_spans.pop(span_id)
            span.end_time = time.time()
            span.duration_ms = (span.end_time - span.start_time) * 1000
            span.state = TraceState.COMPLETED if not error else TraceState.FAILED
            span.error = error

            if tags:
                span.tags.update(tags)

            # Get or create trace
            if span.trace_id not in self._completed_traces:
                self._completed_traces[span.trace_id] = Trace(
                    trace_id=span.trace_id,
                    spans=[],
                    start_time=span.start_time
                )

            self._completed_traces[span.trace_id].spans.append(span)

        # Export span
        self._export_span(span)

    def record_span_log(self, context: SpanContext, event: str,
                       timestamp: float = None, **fields):
        """Record a span log event."""
        span_id = context.span_id

        with self._lock:
            if span_id not in self._active_spans:
                return

            span = self._active_spans[span_id]
            span.logs.append({
                'event': event,
                'timestamp': timestamp or time.time(),
                **fields
            })

    def add_tag(self, context: SpanContext, key: str, value: str):
        """Add a tag to active span."""
        span_id = context.span_id

        with self._lock:
            if span_id in self._active_spans:
                self._active_spans[span_id].tags[key] = value

    def create_child(self, parent_context: SpanContext, name: str) -> SpanContext:
        """Create a child span."""
        return self.start_span(
            name=name,
            trace_id=parent_context.trace_id,
            parent_span_id=parent_context.span_id
        )

    def add_exporter(self, exporter: Callable):
        """Add a span exporter."""
        self._exporters.append(exporter)

    def _export_span(self, span: Span):
        """Export a span to registered exporters."""
        for exporter in self._exporters:
            try:
                exporter(span)
            except Exception as e:
                logger.error(f"Span export failed: {e}")

    def get_trace(self, trace_id: str) -> Optional[Trace]:
        """Get a trace by ID."""
        with self._lock:
            return self._completed_traces.get(trace_id)

    def get_active_spans(self) -> List[Span]:
        """Get all active spans."""
        with self._lock:
            return list(self._active_spans.values())

    def cleanup_old_traces(self, max_age_seconds: float = 3600) -> int:
        """Clean up old traces."""
        cutoff = time.time() - max_age_seconds
        removed = 0

        with self._lock:
            old_trace_ids = [
                trace_id for trace_id, trace in self._completed_traces.items()
                if trace.start_time < cutoff
            ]

            for trace_id in old_trace_ids:
                del self._completed_traces[trace_id]
                removed += 1

        return removed

    def get_stats(self) -> Dict:
        """Get tracer statistics."""
        with self._lock:
            return {
                'active_spans': len(self._active_spans),
                'completed_traces': len(self._completed_traces),
                'exporters': len(self._exporters)
            }


class TraceContext:
    """
    Context manager for span tracing.
    """

    def __init__(self, tracer: Tracer, name: str, service_name: str = None):
        self.tracer = tracer
        self.name = name
        self.service_name = service_name
        self.context: SpanContext = None

    def __enter__(self):
        self.context = self.tracer.start_span(self.name, service_name=self.service_name)
        return self.context

    def __exit__(self, *args):
        if self.context:
            self.tracer.end_span(self.context)


class ConsoleExporter:
    """Exporter that writes spans to console."""

    def __init__(self, logger_name: str = "tracing"):
        self.logger = logging.getLogger(logger_name)

    def __call__(self, span: Span):
        """Export a span."""
        self.logger.info(
            f"[{span.trace_id}] {span.name} ({span.duration_ms:.2f}ms) - {span.service_name}"
        )


class NoOpSpan:
    """No-op span for when tracing is disabled."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def record_log(self, *args, **kwargs):
        pass

    def add_tag(self, *args, **kwargs):
        pass


class NoOpTracer:
    """No-op tracer."""

    def start_span(self, *args, **kwargs):
        return NoOpSpan()

    def end_span(self, *args, **kwargs):
        pass

    def create_child(self, *args, **kwargs):
        return NoOpSpan()

    def add_tag(self, *args, **kwargs):
        pass

    def record_span_log(self, *args, **kwargs):
        pass


_global_tracer: Optional[Tracer] = None
_enabled = True


def enable_tracing(service_name: str = "unknown") -> Tracer:
    """Enable tracing globally."""
    global _global_tracer, _enabled
    _global_tracer = Tracer(service_name)
    _enabled = True
    return _global_tracer


def disable_tracing():
    """Disable tracing globally."""
    global _enabled
    _enabled = False


def get_tracer() -> Tracer:
    """Get the global tracer."""
    global _global_tracer, _enabled
    if not _enabled:
        return NoOpTracer()
    if _global_tracer is None:
        return enable_tracing()
    return _global_tracer


def start_span(name: str, **kwargs) -> SpanContext:
    """Start a span using global tracer."""
    return get_tracer().start_span(name, **kwargs)


def end_span(context: SpanContext, **kwargs):
    """End a span using global tracer."""
    get_tracer().end_span(context, **kwargs)


def create_span(name: str) -> SpanBuilder:
    """Create a span builder."""
    return SpanBuilder(get_tracer(), name)


def trace(name: str):
    """Decorator for tracing a function."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            with TraceContext(tracer, name):
                return func(*args, **kwargs)
        return wrapper
    return decorator