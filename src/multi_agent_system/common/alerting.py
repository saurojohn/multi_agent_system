"""Real-time alerting system for monitoring thresholds."""

import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Any
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger('alerting')


class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertState(Enum):
    INACTIVE = "inactive"
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


@dataclass
class Alert:
    alert_id: str
    name: str
    level: AlertLevel
    message: str
    metric: str
    value: float
    threshold: float
    timestamp: float
    state: AlertState
    acknowledged_at: Optional[float] = None
    resolved_at: Optional[float] = None


class AlertRule:
    """Defines a condition that triggers an alert."""

    def __init__(self, name: str, metric: str, threshold: float,
                 comparison: str = "gt", level: AlertLevel = AlertLevel.WARNING):
        """
        Args:
            name: Alert name
            metric: Metric to monitor (e.g., 'error_rate', 'task_latency')
            threshold: Value that triggers alert
            comparison: 'gt', 'lt', 'eq', 'gte', 'lte'
            level: Alert severity level
        """
        self.name = name
        self.metric = metric
        self.threshold = threshold
        self.comparison = comparison
        self.level = level

    def evaluate(self, value: float) -> bool:
        """Check if value triggers alert."""
        if self.comparison == "gt":
            return value > self.threshold
        elif self.comparison == "gte":
            return value >= self.threshold
        elif self.comparison == "lt":
            return value < self.threshold
        elif self.comparison == "lte":
            return value <= self.threshold
        elif self.comparison == "eq":
            return value == self.threshold
        return False


class AlertManager:
    """Manages alerting rules and notifications."""

    def __init__(self, check_interval: float = 10.0):
        self.check_interval = check_interval
        self._rules: Dict[str, AlertRule] = {}
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_history: List[Alert] = []
        self._handlers: Dict[AlertLevel, List[Callable]] = {
            AlertLevel.INFO: [],
            AlertLevel.WARNING: [],
            AlertLevel.ERROR: [],
            AlertLevel.CRITICAL: []
        }
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._metrics_callback: Optional[Callable] = None

    def set_metrics_callback(self, callback: Callable[[], Dict[str, float]]):
        """Set callback to retrieve current metrics."""
        self._metrics_callback = callback

    def add_rule(self, rule: AlertRule):
        """Add an alert rule."""
        with self._lock:
            self._rules[rule.name] = rule
        logger.info(f'Added alert rule: {rule.name} (metric={rule.metric}, threshold={rule.threshold})')

    def remove_rule(self, name: str):
        """Remove an alert rule."""
        with self._lock:
            if name in self._rules:
                del self._rules[name]

    def register_handler(self, level: AlertLevel, handler: Callable):
        """Register handler for alert level."""
        with self._lock:
            self._handlers[level].append(handler)

    def start(self):
        """Start monitoring."""
        if self._running:
            return
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info('Alert manager started')

    def stop(self):
        """Stop monitoring."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logger.info('Alert manager stopped')

    def _monitor_loop(self):
        """Main monitoring loop."""
        while self._running:
            try:
                self._check_alerts()
            except Exception as e:
                logger.error(f'Alert monitoring error: {e}')
            time.sleep(self.check_interval)

    def _check_alerts(self):
        """Check all rules against current metrics."""
        if not self._metrics_callback:
            return

        metrics = self._metrics_callback()

        with self._lock:
            for rule_name, rule in self._rules.items():
                if rule.metric not in metrics:
                    continue

                value = metrics[rule.metric]
                if rule.evaluate(value):
                    # Check if we already have an active alert for this rule
                    if rule_name not in self._active_alerts:
                        self._trigger_alert(rule, value)
                else:
                    # Metric back to normal, resolve alert
                    if rule_name in self._active_alerts:
                        self._resolve_alert(rule_name)

    def _trigger_alert(self, rule: AlertRule, value: float):
        """Trigger a new alert."""
        import uuid
        alert = Alert(
            alert_id=str(uuid.uuid4())[:8],
            name=rule.name,
            level=rule.level,
            message=f"{rule.name}: {rule.metric}={value} ({rule.comparison} {rule.threshold})",
            metric=rule.metric,
            value=value,
            threshold=rule.threshold,
            timestamp=time.time(),
            state=AlertState.ACTIVE
        )

        self._active_alerts[rule.name] = alert
        self._alert_history.append(alert)
        logger.warning(f'ALERT [{rule.level.value.upper()}] {alert.message}')

        # Call registered handlers
        for handler in self._handlers[rule.level]:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f'Alert handler failed: {e}')

    def _resolve_alert(self, rule_name: str):
        """Resolve an active alert."""
        if rule_name in self._active_alerts:
            alert = self._active_alerts[rule_name]
            alert.state = AlertState.RESOLVED
            alert.resolved_at = time.time()
            logger.info(f'Alert resolved: {rule_name}')
            del self._active_alerts[rule_name]

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an active alert."""
        with self._lock:
            for alert in self._active_alerts.values():
                if alert.alert_id == alert_id:
                    alert.state = AlertState.ACKNOWLEDGED
                    alert.acknowledged_at = time.time()
                    return True
        return False

    def get_active_alerts(self) -> List[Alert]:
        """Get all currently active alerts."""
        with self._lock:
            return list(self._active_alerts.values())

    def get_alert_history(self, limit: int = 100) -> List[Alert]:
        """Get recent alert history."""
        with self._lock:
            return self._alert_history[-limit:]

    def get_status(self) -> Dict:
        """Get alerting system status."""
        with self._lock:
            return {
                'active_alerts': len(self._active_alerts),
                'rules_count': len(self._rules),
                'critical_count': sum(1 for a in self._active_alerts.values() if a.level == AlertLevel.CRITICAL),
                'warning_count': sum(1 for a in self._active_alerts.values() if a.level == AlertLevel.WARNING),
                'rules': [
                    {
                        'name': r.name,
                        'metric': r.metric,
                        'threshold': r.threshold,
                        'comparison': r.comparison
                    }
                    for r in self._rules.values()
                ]
            }


class DefaultAlertHandlers:
    """Pre-built alert handlers for common scenarios."""

    @staticmethod
    def log_handler(alert: Alert):
        """Log alert to standard logger."""
        if alert.level == AlertLevel.CRITICAL:
            logger.critical(f'CRITICAL ALERT: {alert.message}')
        elif alert.level == AlertLevel.ERROR:
            logger.error(f'ERROR ALERT: {alert.message}')
        elif alert.level == AlertLevel.WARNING:
            logger.warning(f'WARNING ALERT: {alert.message}')
        else:
            logger.info(f'INFO ALERT: {alert.message}')

    @staticmethod
    def callback_handler(callback: Callable[[Alert], None]):
        """Create a callback-based handler."""
        def handler(alert: Alert):
            try:
                callback(alert)
            except Exception as e:
                logger.error(f'Alert callback failed: {e}')
        return handler


# Pre-configured alert rules for multi-agent system
def create_default_alert_rules(manager: AlertManager):
    """Add standard alert rules for multi-agent system."""
    # High error rate
    manager.add_rule(AlertRule(
        name="high_error_rate",
        metric="error_rate",
        threshold=0.1,  # 10% error rate
        comparison="gt",
        level=AlertLevel.ERROR
    ))

    # High task latency
    manager.add_rule(AlertRule(
        name="high_task_latency",
        metric="avg_task_latency",
        threshold=5.0,  # 5 seconds
        comparison="gt",
        level=AlertLevel.WARNING
    ))

    # Worker offline
    manager.add_rule(AlertRule(
        name="workers_offline",
        metric="workers_online_ratio",
        threshold=0.5,  # Less than 50% workers online
        comparison="lt",
        level=AlertLevel.CRITICAL
    ))

    # Queue depth too high
    manager.add_rule(AlertRule(
        name="queue_overflow",
        metric="queue_depth",
        threshold=1000,
        comparison="gt",
        level=AlertLevel.WARNING
    ))


# Global alert manager
_alert_manager = AlertManager()


def get_alert_manager() -> AlertManager:
    return _alert_manager