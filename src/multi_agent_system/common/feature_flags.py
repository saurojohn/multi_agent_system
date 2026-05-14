"""Feature flags for gradual rollouts and experiments."""

import logging
import threading
import time
import hashlib
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('feature_flags')


class FlagType(Enum):
    """Feature flag types."""
    BOOLEAN = "boolean"      # On/Off
    PERCENTAGE = "percentage"  # % of users
    TARGETED = "targeted"   # Specific users/groups
    GRADUAL = "gradual"      # Gradual rollout


@dataclass
class FlagRule:
    """A rule for evaluating a feature flag."""
    rule_id: str
    condition: Callable[[Dict], bool]
    value: Any
    priority: int = 0


@dataclass
class FeatureFlag:
    """A feature flag definition."""
    flag_key: str
    flag_type: FlagType
    default_value: Any
    enabled: bool = True
    rules: List[FlagRule] = field(default_factory=list)
    percentage: float = 0.0  # For percentage flags
    target_users: List[str] = field(default_factory=list)  # For targeted flags
    target_groups: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class FlagEvaluation:
    """Result of flag evaluation."""
    flag_key: str
    value: Any
    matched_rule: Optional[str] = None
    reason: str = ""


class FeatureFlagManager:
    """
    Manages feature flags for gradual rollouts and experiments.
    """

    def __init__(self):
        self._flags: Dict[str, FeatureFlag] = {}
        self._lock = threading.RLock()
        self._evaluation_count: Dict[str, int] = {}
        self._handlers: Dict[str, List[Callable]] = {}  # flag_key -> handlers

    def create_flag(self, flag_key: str, flag_type: FlagType,
                   default_value: Any = False, **config) -> FeatureFlag:
        """Create a new feature flag."""
        flag = FeatureFlag(
            flag_key=flag_key,
            flag_type=flag_type,
            default_value=default_value,
            **{k: v for k, v in config.items() if v is not None}
        )

        with self._lock:
            self._flags[flag_key] = flag

        logger.info(f"Created feature flag: {flag_key} ({flag_type.value})")
        return flag

    def get_flag(self, flag_key: str) -> Optional[FeatureFlag]:
        """Get a feature flag."""
        with self._lock:
            return self._flags.get(flag_key)

    def update_flag(self, flag_key: str, **updates) -> bool:
        """Update a feature flag."""
        with self._lock:
            if flag_key not in self._flags:
                return False

            flag = self._flags[flag_key]
            for key, value in updates.items():
                if hasattr(flag, key):
                    setattr(flag, key, value)
            flag.updated_at = time.time()
            return True

    def delete_flag(self, flag_key: str) -> bool:
        """Delete a feature flag."""
        with self._lock:
            if flag_key in self._flags:
                del self._flags[flag_key]
                logger.info(f"Deleted feature flag: {flag_key}")
                return True
        return False

    def enable_flag(self, flag_key: str):
        """Enable a feature flag."""
        self.update_flag(flag_key, enabled=True)

    def disable_flag(self, flag_key: str):
        """Disable a feature flag."""
        self.update_flag(flag_key, enabled=False)

    def add_rule(self, flag_key: str, rule: FlagRule) -> bool:
        """Add a rule to a flag."""
        with self._lock:
            if flag_key not in self._flags:
                return False
            self._flags[flag_key].rules.append(rule)
            self._flags[flag_key].updated_at = time.time()
            return True

    def evaluate(self, flag_key: str, context: Dict = None) -> FlagEvaluation:
        """
        Evaluate a feature flag for a given context.
        context can include: user_id, groups, attributes, etc.
        """
        context = context or {}
        with self._lock:
            flag = self._flags.get(flag_key)

            if not flag:
                return FlagEvaluation(
                    flag_key=flag_key,
                    value=False,
                    reason="flag_not_found"
                )

            # Check if flag is enabled
            if not flag.enabled:
                return FlagEvaluation(
                    flag_key=flag_key,
                    value=flag.default_value,
                    reason="flag_disabled"
                )

            # Evaluate rules in priority order
            sorted_rules = sorted(flag.rules, key=lambda r: r.priority, reverse=True)
            for rule in sorted_rules:
                try:
                    if rule.condition(context):
                        self._evaluation_count[flag_key] = self._evaluation_count.get(flag_key, 0) + 1
                        return FlagEvaluation(
                            flag_key=flag_key,
                            value=rule.value,
                            matched_rule=rule.rule_id,
                            reason="rule_matched"
                        )
                except Exception as e:
                    logger.error(f"Rule evaluation failed for {flag_key}: {e}")

            # Handle different flag types
            if flag.flag_type == FlagType.BOOLEAN:
                return FlagEvaluation(
                    flag_key=flag_key,
                    value=True,
                    reason="boolean_enabled"
                )

            elif flag.flag_type == FlagType.PERCENTAGE:
                # Hash user_id for consistent percentage assignment
                user_id = context.get('user_id', str(time.time()))
                hash_val = int(hashlib.md5(f"{flag_key}:{user_id}".encode()).hexdigest(), 16)
                in_percentage = (hash_val % 100) < (flag.percentage * 100)
                return FlagEvaluation(
                    flag_key=flag_key,
                    value=in_percentage,
                    reason=f"percentage_{flag.percentage}"
                )

            elif flag.flag_type == FlagType.TARGETED:
                user_id = context.get('user_id')
                if user_id and user_id in flag.target_users:
                    return FlagEvaluation(
                        flag_key=flag_key,
                        value=True,
                        reason="user_targeted"
                    )
                # Check groups
                user_groups = context.get('groups', [])
                for group in user_groups:
                    if group in flag.target_groups:
                        return FlagEvaluation(
                            flag_key=flag_key,
                            value=True,
                            reason="group_targeted"
                        )
                return FlagEvaluation(
                    flag_key=flag_key,
                    value=flag.default_value,
                    reason="not_targeted"
                )

            elif flag.flag_type == FlagType.GRADUAL:
                # Gradual rollout based on user hash
                user_id = context.get('user_id', str(time.time()))
                hash_val = int(hashlib.md5(f"{flag_key}:{user_id}".encode()).hexdigest(), 16)
                rollout_pct = self._calculate_gradual_percentage(flag_key)
                in_rollout = (hash_val % 100) < (rollout_pct * 100)
                return FlagEvaluation(
                    flag_key=flag_key,
                    value=in_rollout,
                    reason=f"gradual_{rollout_pct}"
                )

            # Default
            return FlagEvaluation(
                flag_key=flag_key,
                value=flag.default_value,
                reason="default"
            )

    def _calculate_gradual_percentage(self, flag_key: str) -> float:
        """Calculate gradual rollout percentage based on time and metadata."""
        flag = self._flags.get(flag_key)
        if not flag:
            return 0.0

        metadata = flag.metadata
        start_pct = metadata.get('start_percentage', 0.0)
        end_pct = metadata.get('end_percentage', 100.0)
        start_time = metadata.get('start_time', time.time())
        duration = metadata.get('duration_seconds', 86400)  # Default 24 hours

        elapsed = time.time() - start_time
        progress = min(1.0, elapsed / duration) if duration > 0 else 1.0

        return start_pct + (end_pct - start_pct) * progress

    def is_enabled(self, flag_key: str, context: Dict = None) -> bool:
        """Check if a flag is enabled (shorthand)."""
        result = self.evaluate(flag_key, context)
        return result.value is True

    def add_evaluation_handler(self, flag_key: str, handler: Callable):
        """Add a handler called when flag is evaluated."""
        if flag_key not in self._handlers:
            self._handlers[flag_key] = []
        self._handlers[flag_key].append(handler)

    def get_stats(self) -> Dict:
        """Get flag statistics."""
        with self._lock:
            return {
                'total_flags': len(self._flags),
                'enabled': sum(1 for f in self._flags.values() if f.enabled),
                'evaluations': dict(self._evaluation_count)
            }


class FlagOverride:
    """
    Override feature flags for testing.
    """

    def __init__(self, manager: FeatureFlagManager):
        self.manager = manager
        self._overrides: Dict[str, Any] = {}
        self._user_overrides: Dict[str, Dict[str, Any]] = {}  # user_id -> {flag -> value}
        self._lock = threading.RLock()

    def override(self, flag_key: str, value: Any):
        """Override a flag for all users."""
        with self._lock:
            self._overrides[flag_key] = value
            logger.info(f"Override set for {flag_key}: {value}")

    def clear_override(self, flag_key: str):
        """Clear a flag override."""
        with self._lock:
            if flag_key in self._overrides:
                del self._overrides[flag_key]

    def override_for_user(self, user_id: str, flag_key: str, value: Any):
        """Override a flag for a specific user."""
        with self._lock:
            if user_id not in self._user_overrides:
                self._user_overrides[user_id] = {}
            self._user_overrides[user_id][flag_key] = value

    def get_value(self, flag_key: str, context: Dict = None) -> Any:
        """Get flag value with overrides applied."""
        context = context or {}
        user_id = context.get('user_id')

        # Check user-specific override
        if user_id and user_id in self._user_overrides:
            if flag_key in self._user_overrides[user_id]:
                return self._user_overrides[user_id][flag_key]

        # Check global override
        if flag_key in self._overrides:
            return self._overrides[flag_key]

        # Fall back to manager evaluation
        return self.manager.evaluate(flag_key, context).value


# Global flag manager
_flag_manager = FeatureFlagManager()
_flag_override = FlagOverride(_flag_manager)


def get_flag_manager() -> FeatureFlagManager:
    return _flag_manager


def get_flag_override() -> FlagOverride:
    return _flag_override


def is_enabled(flag_key: str, **context) -> bool:
    """Check if a flag is enabled."""
    return _flag_manager.is_enabled(flag_key, context)


def create_boolean_flag(flag_key: str) -> FeatureFlag:
    """Create a simple boolean flag."""
    return _flag_manager.create_flag(flag_key, FlagType.BOOLEAN, default_value=False)


def create_percentage_flag(flag_key: str, percentage: float) -> FeatureFlag:
    """Create a percentage-based flag."""
    return _flag_manager.create_flag(
        flag_key, FlagType.PERCENTAGE,
        default_value=False,
        percentage=percentage
    )