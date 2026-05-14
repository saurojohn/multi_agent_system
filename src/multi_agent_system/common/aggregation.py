"""Task result aggregation and merging from multiple workers."""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('aggregation')


class AggregationStrategy(Enum):
    """Strategies for aggregating results."""
    FIRST = "first"           # Take first result
    LAST = "last"            # Take last result
    MERGE = "merge"           # Merge all results
    CONCATENATE = "concatenate"  # Concatenate lists
    AVERAGE = "average"       # Average numeric values
    SUM = "sum"              # Sum numeric values
    MIN = "min"              # Take minimum
    MAX = "max"              # Take maximum
    CUSTOM = "custom"        # Custom aggregation function


@dataclass
class AggregationRule:
    """Rule for aggregating specific fields."""
    field_path: str  # e.g., "data.results" or "data.*.score"
    strategy: AggregationStrategy
    custom_fn: Callable = None


@dataclass
class AggregationResult:
    """Result of aggregation."""
    aggregated: Any
    partial_results: List[Any] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    duration_ms: float = 0


class ResultAggregator:
    """
    Aggregates results from multiple workers or tasks.
    """

    def __init__(self):
        self._rules: List[AggregationRule] = []
        self._default_strategy = AggregationStrategy.FIRST
        self._lock = threading.Lock()

    def add_rule(self, field_path: str, strategy: AggregationStrategy,
                 custom_fn: Callable = None):
        """Add an aggregation rule."""
        rule = AggregationRule(
            field_path=field_path,
            strategy=strategy,
            custom_fn=custom_fn
        )
        with self._lock:
            self._rules.append(rule)

    def set_default_strategy(self, strategy: AggregationStrategy):
        """Set the default aggregation strategy."""
        self._default_strategy = strategy

    def aggregate(self, results: List[Dict],
                  field_path: str = None,
                  strategy: AggregationStrategy = None,
                  custom_fn: Callable = None) -> AggregationResult:
        """Aggregate results based on strategy."""
        start_time = time.time()

        if not results:
            return AggregationResult(aggregated=None, duration_ms=0)

        # Use custom aggregation if provided
        if custom_fn:
            return AggregationResult(
                aggregated=custom_fn(results),
                partial_results=results,
                duration_ms=(time.time() - start_time) * 1000
            )

        # Use field-specific rule if exists
        if field_path:
            with self._lock:
                for rule in self._rules:
                    if rule.field_path == field_path:
                        strategy = rule.strategy
                        custom_fn = rule.custom_fn
                        break

        strategy = strategy or self._default_strategy

        # Extract field values
        field_values = []
        for result in results:
            value = self._extract_field(result, field_path)
            if value is not None:
                field_values.append(value)

        # Aggregate based on strategy
        aggregated = self._apply_strategy(strategy, field_values, custom_fn)

        return AggregationResult(
            aggregated=aggregated,
            partial_results=results,
            duration_ms=(time.time() - start_time) * 1000
        )

    def _extract_field(self, data: Dict, field_path: str) -> Any:
        """Extract a field from data using path notation."""
        if not field_path:
            return data

        keys = field_path.split('.')
        value = data

        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None

            if value is None:
                return None

        return value

    def _apply_strategy(self, strategy: AggregationStrategy,
                        values: List[Any],
                        custom_fn: Callable = None) -> Any:
        """Apply aggregation strategy."""
        if not values:
            return None

        if custom_fn:
            return custom_fn(values)

        if strategy == AggregationStrategy.FIRST:
            return values[0]

        elif strategy == AggregationStrategy.LAST:
            return values[-1]

        elif strategy == AggregationStrategy.CONCATENATE:
            result = []
            for v in values:
                if isinstance(v, list):
                    result.extend(v)
                else:
                    result.append(v)
            return result

        elif strategy == AggregationStrategy.MERGE:
            result = {}
            for v in values:
                if isinstance(v, dict):
                    result.update(v)
            return result

        elif strategy == AggregationStrategy.AVERAGE:
            numeric = [v for v in values if isinstance(v, (int, float))]
            return sum(numeric) / len(numeric) if numeric else None

        elif strategy == AggregationStrategy.SUM:
            numeric = [v for v in values if isinstance(v, (int, float))]
            return sum(numeric) if numeric else None

        elif strategy == AggregationStrategy.MIN:
            return min(values)

        elif strategy == AggregationStrategy.MAX:
            return max(values)

        elif strategy == AggregationStrategy.MERGE:
            return self._deep_merge(values)

        return values[0]

    def _deep_merge(self, values: List[Dict]) -> Dict:
        """Deep merge multiple dictionaries."""
        result = {}
        for v in values:
            if isinstance(v, dict):
                for key, val in v.items():
                    if key in result and isinstance(result[key], dict) and isinstance(val, dict):
                        result[key] = self._deep_merge([result[key], val])
                    else:
                        result[key] = val
        return result


class ParallelAggregator:
    """
    Aggregates results from parallel task execution.
    """

    def __init__(self, aggregator: ResultAggregator = None):
        self.aggregator = aggregator or ResultAggregator()
        self._pending: Dict[str, List[Dict]] = {}
        self._completed: Dict[str, AggregationResult] = {}
        self._lock = threading.Lock()

    def start_collection(self, collection_id: str):
        """Start collecting results for a collection."""
        with self._lock:
            self._pending[collection_id] = []

    def add_result(self, collection_id: str, result: Dict):
        """Add a result to the collection."""
        with self._lock:
            if collection_id in self._pending:
                self._pending[collection_id].append(result)

    def complete_collection(self, collection_id: str,
                          field_path: str = None,
                          strategy: AggregationStrategy = None) -> AggregationResult:
        """Complete collection and aggregate results."""
        with self._lock:
            if collection_id not in self._pending:
                return AggregationResult(aggregated=None)

            results = self._pending.pop(collection_id)
            aggregated = self.aggregator.aggregate(results, field_path, strategy)
            self._completed[collection_id] = aggregated
            return aggregated

    def get_result(self, collection_id: str) -> Optional[AggregationResult]:
        """Get aggregation result."""
        with self._lock:
            return self._completed.get(collection_id)


class ChainedAggregator:
    """
    Aggregates results through multiple stages.
    """

    def __init__(self):
        self._stages: List[Tuple[str, AggregationStrategy]] = []
        self._aggregator = ResultAggregator()

    def add_stage(self, field_path: str, strategy: AggregationStrategy) -> 'ChainedAggregator':
        """Add an aggregation stage."""
        self._stages.append((field_path, strategy))
        return self

    def execute(self, results: List[Dict]) -> Dict[str, Any]:
        """Execute chained aggregation."""
        current_results = results

        for field_path, strategy in self._stages:
            result = self._aggregator.aggregate(
                current_results,
                field_path=field_path,
                strategy=strategy
            )
            # For next stage, wrap result in list if single value
            if isinstance(result.aggregated, dict):
                current_results = [result.aggregated]
            elif isinstance(result.aggregated, list):
                current_results = result.aggregated
            else:
                current_results = [{'value': result.aggregated}]

        return {'final': result.aggregated, 'stages': len(self._stages)}


class AggregationBuilder:
    """Builder for creating aggregations."""

    def __init__(self):
        self._rules: List[AggregationRule] = []
        self._default_strategy = AggregationStrategy.FIRST

    def add_rule(self, field_path: str, strategy: AggregationStrategy,
                custom_fn: Callable = None) -> 'AggregationBuilder':
        """Add an aggregation rule."""
        self._rules.append(AggregationRule(field_path, strategy, custom_fn))
        return self

    def first(self, field_path: str = None) -> 'AggregationBuilder':
        """Add first strategy."""
        self._rules.append(AggregationRule(field_path, AggregationStrategy.FIRST))
        return self

    def last(self, field_path: str = None) -> 'AggregationBuilder':
        """Add last strategy."""
        self._rules.append(AggregationRule(field_path, AggregationStrategy.LAST))
        return self

    def average(self, field_path: str) -> 'AggregationBuilder':
        """Add average strategy."""
        self._rules.append(AggregationRule(field_path, AggregationStrategy.AVERAGE))
        return self

    def sum(self, field_path: str) -> 'AggregationBuilder':
        """Add sum strategy."""
        self._rules.append(AggregationRule(field_path, AggregationStrategy.SUM))
        return self

    def merge(self, field_path: str = None) -> 'AggregationBuilder':
        """Add merge strategy."""
        self._rules.append(AggregationRule(field_path, AggregationStrategy.MERGE))
        return self

    def concatenate(self, field_path: str = None) -> 'AggregationBuilder':
        """Add concatenate strategy."""
        self._rules.append(AggregationRule(field_path, AggregationStrategy.CONCATENATE))
        return self

    def custom(self, field_path: str, fn: Callable) -> 'AggregationBuilder':
        """Add custom strategy."""
        self._rules.append(AggregationRule(field_path, AggregationStrategy.CUSTOM, fn))
        return self

    def build(self) -> ResultAggregator:
        """Build the aggregator."""
        aggregator = ResultAggregator()
        for rule in self._rules:
            aggregator.add_rule(rule.field_path, rule.strategy, rule.custom_fn)
        return aggregator


class WeightedAggregator:
    """
    Aggregates results with weights.
    """

    def __init__(self):
        self._weights: Dict[str, float] = {}

    def set_weight(self, source: str, weight: float):
        """Set weight for a source."""
        self._weights[source] = weight

    def aggregate(self, results: List[Tuple[str, Any]]) -> Any:
        """Aggregate weighted results."""
        if not results:
            return None

        numeric_results = [(s, v) for s, v in results if isinstance(v, (int, float))]
        if not numeric_results:
            return results[0][1] if results else None

        total_weight = sum(self._weights.get(s, 1.0) for s, _ in numeric_results)
        weighted_sum = sum(v * self._weights.get(s, 1.0) for s, v in numeric_results)

        return weighted_sum / total_weight if total_weight > 0 else None


# Global aggregator
_default_aggregator = ResultAggregator()


def get_aggregator() -> ResultAggregator:
    return _default_aggregator


def aggregate_results(results: List[Dict], **kwargs) -> AggregationResult:
    """Aggregate results with defaults."""
    return _default_aggregator.aggregate(results, **kwargs)