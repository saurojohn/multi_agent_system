"""Audit logging for compliance and security."""

import logging
import threading
import time
import json
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('audit')


class AuditLevel(Enum):
    """Audit log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditEvent(Enum):
    """Audit event types."""
    USER_LOGIN = "user.login"
    USER_LOGOUT = "user.logout"
    USER_CREATE = "user.create"
    USER_UPDATE = "user.update"
    USER_DELETE = "user.delete"
    PERMISSION_GRANT = "permission.grant"
    PERMISSION_REVOKE = "permission.revoke"
    DATA_ACCESS = "data.access"
    DATA_MODIFY = "data.modify"
    DATA_DELETE = "data.delete"
    CONFIG_CHANGE = "config.change"
    SECURITY_EVENT = "security.event"
    SYSTEM_ACTION = "system.action"


@dataclass
class AuditEntry:
    """An audit log entry."""
    entry_id: str
    timestamp: float
    level: AuditLevel
    event: str
    actor_id: str  # Who performed the action
    actor_type: str  # user, system, api_key, etc.
    resource_type: str  # What type of resource
    resource_id: str  # What specific resource
    action: str  # What action was taken
    result: str  # success, failure, denied
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    metadata: Dict = field(default_factory=dict)
    changes: Dict = field(default_factory=dict)  # For tracking changes


class AuditLogger:
    """
    Audit logger for compliance and security tracking.
    """

    def __init__(self, persist_dir: str = "/tmp/multi_agent_audit"):
        self.persist_dir = persist_dir
        self._entries: List[AuditEntry] = []
        self._lock = threading.RLock()
        self._max_entries = 100000
        self._handlers: List[Callable[[AuditEntry], None]] = []
        self._filters: List[Callable[[AuditEntry], bool]] = []
        self._running = False
        self._persist_thread: threading.Thread = None

        import os
        os.makedirs(persist_dir, exist_ok=True)

    def start(self):
        """Start the audit logger."""
        self._running = True
        self._persist_thread = threading.Thread(target=self._persist_loop, daemon=True)
        self._persist_thread.start()
        logger.info("Audit logger started")

    def stop(self):
        """Stop the audit logger."""
        self._running = False
        self._persist()
        if self._persist_thread:
            self._persist_thread.join(timeout=5)
        logger.info("Audit logger stopped")

    def log(self, event: str, actor_id: str, action: str,
            resource_type: str = None, resource_id: str = None,
            level: AuditLevel = AuditLevel.INFO,
            result: str = "success",
            actor_type: str = "user",
            ip_address: str = None,
            user_agent: str = None,
            metadata: Dict = None,
            changes: Dict = None):
        """Log an audit event."""
        import uuid

        entry = AuditEntry(
            entry_id=str(uuid.uuid4()),
            timestamp=time.time(),
            level=level,
            event=event,
            actor_id=actor_id,
            actor_type=actor_type,
            resource_type=resource_type or "unknown",
            resource_id=resource_id or "unknown",
            action=action,
            result=result,
            ip_address=ip_address,
            user_agent=user_agent,
            metadata=metadata or {},
            changes=changes or {}
        )

        # Apply filters
        for filter_fn in self._filters:
            if not filter_fn(entry):
                return

        with self._lock:
            self._entries.append(entry)

            # Enforce max entries
            if len(self._entries) > self._max_entries:
                self._entries = self._entries[-self._max_entries:]

        # Call handlers
        for handler in self._handlers:
            try:
                handler(entry)
            except Exception as e:
                logger.error(f"Audit handler failed: {e}")

    def add_handler(self, handler: Callable[[AuditEntry], None]):
        """Add an audit event handler."""
        self._handlers.append(handler)

    def add_filter(self, filter_fn: Callable[[AuditEntry], bool]):
        """Add a filter for audit events."""
        self._filters.append(filter_fn)

    def query(self, start_time: float = None,
              end_time: float = None,
              event: str = None,
              actor_id: str = None,
              resource_id: str = None,
              result: str = None,
              limit: int = 1000) -> List[AuditEntry]:
        """Query audit entries."""
        with self._lock:
            entries = list(self._entries)

        # Apply filters
        if start_time:
            entries = [e for e in entries if e.timestamp >= start_time]

        if end_time:
            entries = [e for e in entries if e.timestamp <= end_time]

        if event:
            entries = [e for e in entries if e.event == event]

        if actor_id:
            entries = [e for e in entries if e.actor_id == actor_id]

        if resource_id:
            entries = [e for e in entries if e.resource_id == resource_id]

        if result:
            entries = [e for e in entries if e.result == result]

        # Sort by timestamp descending
        entries.sort(key=lambda e: e.timestamp, reverse=True)

        return entries[:limit]

    def get_user_activity(self, user_id: str,
                          limit: int = 100) -> List[AuditEntry]:
        """Get all activity for a specific user."""
        return self.query(actor_id=user_id, limit=limit)

    def get_resource_history(self, resource_type: str,
                            resource_id: str,
                            limit: int = 100) -> List[AuditEntry]:
        """Get all changes to a specific resource."""
        with self._lock:
            entries = [
                e for e in self._entries
                if e.resource_type == resource_type and e.resource_id == resource_id
            ]
        entries.sort(key=lambda e: e.timestamp, reverse=True)
        return entries[:limit]

    def get_security_events(self, start_time: float = None,
                           limit: int = 100) -> List[AuditEntry]:
        """Get security-related events."""
        security_events = [
            AuditEvent.SECURITY_EVENT.value,
            AuditEvent.PERMISSION_GRANT.value,
            AuditEvent.PERMISSION_REVOKE.value,
            AuditEvent.USER_LOGIN.value,
            AuditEvent.USER_LOGOUT.value
        ]

        return self.query(
            start_time=start_time,
            event=None,  # We'll filter manually
            limit=limit
        )

    def _persist_loop(self):
        """Background persistence loop."""
        while self._running:
            time.sleep(60)  # Persist every minute
            self._persist()

    def _persist(self):
        """Persist entries to disk."""
        with self._lock:
            if not self._entries:
                return

            filepath = f"{self.persist_dir}/audit_{int(time.time())}.json"

            try:
                data = [
                    {
                        'entry_id': e.entry_id,
                        'timestamp': e.timestamp,
                        'level': e.level.value,
                        'event': e.event,
                        'actor_id': e.actor_id,
                        'actor_type': e.actor_type,
                        'resource_type': e.resource_type,
                        'resource_id': e.resource_id,
                        'action': e.action,
                        'result': e.result,
                        'ip_address': e.ip_address,
                        'user_agent': e.user_agent,
                        'metadata': e.metadata,
                        'changes': e.changes
                    }
                    for e in self._entries[-10000:]  # Keep last 10k
                ]

                with open(filepath, 'w') as f:
                    json.dump(data, f)

                logger.debug(f"Persisted {len(data)} audit entries")
            except Exception as e:
                logger.error(f"Failed to persist audit log: {e}")

    def get_stats(self) -> Dict:
        """Get audit logger statistics."""
        with self._lock:
            by_event = {}
            for entry in self._entries:
                by_event[entry.event] = by_event.get(entry.event, 0) + 1

            return {
                'total_entries': len(self._entries),
                'max_entries': self._max_entries,
                'handlers': len(self._handlers),
                'by_event': by_event
            }


class AuditContext:
    """
    Context for audit logging.
    Captures current actor and environment information.
    """

    def __init__(self, actor_id: str, actor_type: str = "user",
                 ip_address: str = None, user_agent: str = None):
        self.actor_id = actor_id
        self.actor_type = actor_type
        self.ip_address = ip_address
        self.user_agent = user_agent
        self._previous_context = None

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'actor_id': self.actor_id,
            'actor_type': self.actor_type,
            'ip_address': self.ip_address,
            'user_agent': self.user_agent
        }


# Global audit logger
_audit_logger = AuditLogger()


def get_audit_logger() -> AuditLogger:
    return _audit_logger


def log_audit(event: str, actor_id: str, action: str, **kwargs):
    """Log an audit event."""
    _audit_logger.log(event, actor_id, action, **kwargs)


def audit_log(event: AuditEvent):
    """Decorator for audit logging."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            log_audit(
                event=event.value,
                actor_id=kwargs.get('actor_id', 'system'),
                action=func.__name__,
                result="success"
            )
            return result
        return wrapper
    return decorator


# Helper functions for common audit events
def audit_login(user_id: str, ip_address: str = None, result: str = "success"):
    """Log a user login."""
    log_audit(
        event=AuditEvent.USER_LOGIN.value,
        actor_id=user_id,
        action="login",
        result=result,
        ip_address=ip_address
    )


def audit_logout(user_id: str, ip_address: str = None):
    """Log a user logout."""
    log_audit(
        event=AuditEvent.USER_LOGOUT.value,
        actor_id=user_id,
        action="logout",
        ip_address=ip_address
    )


def audit_permission_change(user_id: str, permission: str,
                            granted: bool, actor_id: str = "system"):
    """Log a permission change."""
    event = AuditEvent.PERMISSION_GRANT if granted else AuditEvent.PERMISSION_REVOKE
    log_audit(
        event=event.value,
        actor_id=actor_id,
        action=f"{'grant' if granted else 'revoke'}_permission",
        resource_type="permission",
        resource_id=permission,
        result="success",
        metadata={'target_user': user_id}
    )


def audit_data_access(user_id: str, resource_type: str,
                      resource_id: str, action: str = "read"):
    """Log data access."""
    log_audit(
        event=AuditEvent.DATA_ACCESS.value,
        actor_id=user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        result="success"
    )