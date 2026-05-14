"""Distributed tracing for multi-agent system."""

import time
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('tracing')


class SpanStatus(Enum):
    STARTED = "started"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TraceSpan:
    """Represents a single unit of work in a trace."""
    span_id: str
    operation_name: str
    trace_id: str
    parent_span_id: Optional[str] = None
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    status: SpanStatus = SpanStatus.STARTED
    tags: Dict[str, str] = field(default_factory=dict)
    logs: List[Dict] = field(default_factory=list)
    error: Optional[str] = None

    def finish(self, status: SpanStatus = SpanStatus.COMPLETED, error: str = None):
        self.end_time = time.time()
        self.status = status
        if error:
            self.error = error

    def add_tag(self, key: str, value: str):
        self.tags[key] = value

    def add_log(self, message: str, timestamp: float = None):
        self.logs.append({
            'message': message,
            'timestamp': timestamp or time.time()
        })

    @property
    def duration_ms(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time) * 1000
        return (time.time() - self.start_time) * 1000


class Trace:
    """A collection of spans representing a complete request."""

    def __init__(self, trace_id: str):
        self.trace_id = trace_id
        self.spans: List[TraceSpan] = []
        self.start_time = time.time()
        self.end_time: Optional[float] = None

    def create_span(self, operation: str, parent_span_id: str = None) -> TraceSpan:
        span_id = f"{self.trace_id}-{len(self.spans)}"
        span = TraceSpan(
            span_id=span_id,
            operation_name=operation,
            trace_id=self.trace_id,
            parent_span_id=parent_span_id
        )
        self.spans.append(span)
        return span

    def finish(self):
        self.end_time = time.time()

    @property
    def total_duration_ms(self) -> float:
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000


class DistributedTracer:
    """Manages distributed tracing across workers."""

    def __init__(self, service_name: str = "multi-agent-system"):
        self.service_name = service_name
        self._active_traces: Dict[str, Trace] = {}
        self._completed_traces: List[Trace] = []
        self._max_completed_traces = 1000  # Keep last 1000 traces

    def start_trace(self, trace_id: str = None) -> Trace:
        if trace_id is None:
            trace_id = f"trace-{int(time.time() * 1000)}"
        trace = Trace(trace_id)
        self._active_traces[trace_id] = trace
        logger.debug(f'Started trace {trace_id}')
        return trace

    def get_trace(self, trace_id: str) -> Optional[Trace]:
        if trace_id in self._active_traces:
            return self._active_traces[trace_id]
        # Check completed traces
        for trace in self._completed_traces:
            if trace.trace_id == trace_id:
                return trace
        return None

    def end_trace(self, trace: Trace):
        trace.finish()
        if trace.trace_id in self._active_traces:
            del self._active_traces[trace.trace_id]
        self._completed_traces.append(trace)
        # Limit completed traces
        if len(self._completed_traces) > self._max_completed_traces:
            self._completed_traces = self._completed_traces[-self._max_completed_traces:]
        logger.debug(f'Finished trace {trace.trace_id} ({trace.total_duration_ms:.2f}ms)')

    def create_span(self, trace_id: str, operation: str, parent_span_id: str = None) -> Optional[TraceSpan]:
        trace = self.get_trace(trace_id)
        if not trace:
            trace = self.start_trace(trace_id)
        return trace.create_span(operation, parent_span_id)

    def get_trace_tree(self, trace_id: str) -> Dict:
        """Get trace as a tree structure for visualization."""
        trace = self.get_trace(trace_id)
        if not trace:
            return {}

        span_dict = {s.span_id: s for s in trace.spans}
        root_spans = [s for s in trace.spans if not s.parent_span_id]

        def build_tree(span):
            children = [s for s in trace.spans if s.parent_span_id == span.span_id]
            return {
                'span_id': span.span_id,
                'operation': span.operation_name,
                'status': span.status.value,
                'duration_ms': span.duration_ms,
                'tags': span.tags,
                'children': [build_tree(c) for c in children]
            }

        return {
            'trace_id': trace_id,
            'total_duration_ms': trace.total_duration_ms,
            'spans': [build_tree(r) for r in root_spans]
        }

    def get_recent_traces(self, limit: int = 10) -> List[Dict]:
        """Get recent traces for debugging."""
        recent = []
        for trace in list(self._completed_traces)[-limit:]:
            recent.append({
                'trace_id': trace.trace_id,
                'duration_ms': trace.total_duration_ms,
                'span_count': len(trace.spans),
                'status': 'completed' if trace.end_time else 'incomplete'
            })
        return recent


# Global tracer instance
_tracer = DistributedTracer()


def get_tracer() -> DistributedTracer:
    return _tracer


def start_trace(trace_id: str = None) -> Trace:
    return _tracer.start_trace(trace_id)


def end_trace(trace: Trace):
    _tracer.end_trace(trace)