"""OpenTelemetry integration for distributed tracing and metrics."""

import logging
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('telemetry')


class SpanKind(Enum):
    """Span kinds for tracing."""
    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


class SpanStatus(Enum):
    """Span status codes."""
    OK = "ok"
    ERROR = "error"
    UNSET = "unset"


@dataclass
class SpanEvent:
    """A span event."""
    name: str
    timestamp: float
    attributes: Dict = field(default_factory=dict)


@dataclass
class SpanLink:
    """A span link."""
    trace_id: str
    span_id: str
    attributes: Dict = field(default_factory=dict)


@dataclass
class TelemetrySpan:
    """A telemetry span."""
    name: str
    trace_id: str
    span_id: str
    parent_span_id: str = None
    kind: SpanKind = SpanKind.INTERNAL
    start_time: float = field(default_factory=time.time)
    end_time: float = None
    attributes: Dict = field(default_factory=dict)
    events: List[SpanEvent] = field(default_factory=list)
    links: List[SpanLink] = field(default_factory=list)
    status: SpanStatus = SpanStatus.UNSET
    status_message: str = None


class TelemetryTracer:
    """
    Telemetry tracer for distributed tracing.
    """

    def __init__(self, service_name: str = "unknown"):
        self.service_name = service_name
        self._spans: Dict[str, TelemetrySpan] = {}
        self._active_spans: Dict[str, TelemetrySpan] = {}
        self._lock = __import__('threading').Lock()
        self._exporters: List[Callable] = []

    def start_span(self, name: str, trace_id: str = None,
                  parent_span_id: str = None,
                  kind: SpanKind = SpanKind.INTERNAL,
                  attributes: Dict = None) -> str:
        """Start a new span."""
        import uuid
        trace_id = trace_id or str(uuid.uuid4())
        span_id = str(uuid.uuid4())

        span = TelemetrySpan(
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            kind=kind,
            attributes=attributes or {}
        )

        with self._lock:
            self._spans[span_id] = span
            self._active_spans[span_id] = span

        return span_id

    def end_span(self, span_id: str, status: SpanStatus = SpanStatus.OK,
                status_message: str = None):
        """End a span."""
        with self._lock:
            if span_id not in self._active_spans:
                return

            span = self._active_spans.pop(span_id)
            span.end_time = time.time()
            span.status = status
            span.status_message = status_message

        # Export span
        self._export_span(span)

    def add_span_event(self, span_id: str, name: str, attributes: Dict = None):
        """Add an event to a span."""
        with self._lock:
            if span_id in self._active_spans:
                span = self._active_spans[span_id]
                span.events.append(SpanEvent(
                    name=name,
                    timestamp=time.time(),
                    attributes=attributes or {}
                ))

    def set_span_attribute(self, span_id: str, key: str, value: Any):
        """Set a span attribute."""
        with self._lock:
            if span_id in self._active_spans:
                self._active_spans[span_id].attributes[key] = value

    def add_exporter(self, exporter: Callable):
        """Add a span exporter."""
        self._exporters.append(exporter)

    def _export_span(self, span: TelemetrySpan):
        """Export a span."""
        for exporter in self._exporters:
            try:
                exporter(span)
            except Exception as e:
                logger.error(f"Span export failed: {e}")

    def get_span(self, span_id: str) -> Optional[TelemetrySpan]:
        """Get a span by ID."""
        return self._spans.get(span_id)

    def get_active_spans(self) -> List[TelemetrySpan]:
        """Get all active spans."""
        return list(self._active_spans.values())

    def get_trace(self, trace_id: str) -> List[TelemetrySpan]:
        """Get all spans for a trace."""
        with self._lock:
            return [s for s in self._spans.values() if s.trace_id == trace_id]


class TelemetryContext:
    """Context manager for telemetry spans."""

    def __init__(self, tracer: TelemetryTracer, name: str, **kwargs):
        self.tracer = tracer
        self.name = name
        self.kwargs = kwargs
        self.span_id = None

    def __enter__(self):
        self.span_id = self.tracer.start_span(self.name, **self.kwargs)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        status = SpanStatus.OK
        message = None
        if exc_type:
            status = SpanStatus.ERROR
            message = str(exc_val)
        self.tracer.end_span(self.span_id, status, message)

    def add_event(self, name: str, **attrs):
        """Add an event to the span."""
        self.tracer.add_span_event(self.span_id, name, attrs)

    def set_attribute(self, key: str, value: Any):
        """Set an attribute."""
        self.tracer.set_span_attribute(self.span_id, key, value)


class MetricsRecorder:
    """Records telemetry metrics."""

    def __init__(self):
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}
        self._lock = __import__('threading').Lock()

    def increment(self, name: str, value: float = 1.0, labels: Dict = None):
        """Increment a counter."""
        key = self._make_key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def record_gauge(self, name: str, value: float, labels: Dict = None):
        """Record a gauge value."""
        key = self._make_key(name, labels)
        with self._lock:
            self._gauges[key] = value

    def record_histogram(self, name: str, value: float, labels: Dict = None):
        """Record a histogram value."""
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = []
            self._histograms[key].append(value)

    def _make_key(self, name: str, labels: Dict = None) -> str:
        """Make a metric key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_metrics(self) -> Dict:
        """Get all metrics."""
        with self._lock:
            return {
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'histograms': {k: list(v) for k, v in self._histograms.items()}
            }


class ConsoleSpanExporter:
    """Exports spans to console."""

    def __call__(self, span: TelemetrySpan):
        """Export a span to console."""
        duration_ms = (span.end_time - span.start_time) * 1000 if span.end_time else 0
        logger.info(
            f"[TRACE] {span.trace_id[:8]} {span.name} "
            f"({duration_ms:.2f}ms) {span.status.value}"
        )


# Global tracer and metrics
_tracer = TelemetryTracer()
_metrics = MetricsRecorder()


def get_tracer() -> TelemetryTracer:
    return _tracer


def get_metrics_recorder() -> MetricsRecorder:
    return _metrics


def create_span(name: str, **kwargs) -> TelemetryContext:
    """Create a telemetry span context."""
    return TelemetryContext(_tracer, name, **kwargs)


def record_metric(name: str, value: float, metric_type: str = "counter", **labels):
    """Record a telemetry metric."""
    if metric_type == "counter":
        _metrics.increment(name, value, labels)
    elif metric_type == "gauge":
        _metrics.record_gauge(name, value, labels)
    elif metric_type == "histogram":
        _metrics.record_histogram(name, value, labels)