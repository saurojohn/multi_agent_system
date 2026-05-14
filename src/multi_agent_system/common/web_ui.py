"""Web-based UI for multi-agent system management."""

import logging
import threading
import json
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('web_ui')


class PageRoute(Enum):
    """Page routes."""
    DASHBOARD = "/"
    AGENTS = "/agents"
    TASKS = "/tasks"
    MESSAGES = "/messages"
    SETTINGS = "/settings"
    MONITORING = "/monitoring"


@dataclass
class AgentConfig:
    """Agent configuration."""
    agent_id: str
    name: str
    agent_type: str
    capabilities: List[str]
    enabled: bool = True
    priority: int = 5
    max_concurrent_tasks: int = 3
    timeout: int = 300
    metadata: Dict = field(default_factory=dict)


@dataclass
class WebUIUser:
    """Web UI user."""
    user_id: str
    username: str
    role: str = "user"
    preferences: Dict = field(default_factory=dict)


class WebUIAuth:
    """Web UI authentication."""

    def __init__(self):
        self._users: Dict[str, WebUIUser] = {}
        self._sessions: Dict[str, str] = {}  # session_id -> user_id
        self._lock = threading.RLock()

    def add_user(self, user: WebUIUser):
        """Add a user."""
        with self._lock:
            self._users[user.username] = user

    def authenticate(self, username: str, password: str = None) -> Optional[str]:
        """Authenticate a user, return session_id or None."""
        with self._lock:
            if username in self._users:
                session_id = f"sess_{int(time.time())}_{username}"
                self._sessions[session_id] = username
                return session_id
        return None

    def validate_session(self, session_id: str) -> Optional[WebUIUser]:
        """Validate a session, return user or None."""
        with self._lock:
            username = self._sessions.get(session_id)
            if username and username in self._users:
                return self._users[username]
        return None

    def logout(self, session_id: str):
        """Logout a session."""
        with self._lock:
            if session_id in self._sessions:
                del self._sessions[session_id]


class WebUISession:
    """Web UI session management."""

    def __init__(self, session_id: str, user: WebUIUser):
        self.session_id = session_id
        self.user = user
        self.created_at = time.time()
        self.last_activity = time.time()
        self.data: Dict = {}


class WebUIManager:
    """
    Manages the web UI and its components.
    """

    def __init__(self, orchestrator=None):
        self.orchestrator = orchestrator
        self.auth = WebUIAuth()
        self._sessions: Dict[str, WebUISession] = {}
        self._agents: Dict[str, AgentConfig] = {}
        self._lock = threading.RLock()
        self._notification_handlers: List[callable] = []

    def set_orchestrator(self, orchestrator):
        """Set the orchestrator."""
        self.orchestrator = orchestrator

    def register_agent(self, config: AgentConfig) -> bool:
        """Register an agent configuration."""
        with self._lock:
            self._agents[config.agent_id] = config
            return True

    def get_agent(self, agent_id: str) -> Optional[AgentConfig]:
        """Get agent configuration."""
        return self._agents.get(agent_id)

    def update_agent(self, agent_id: str, **kwargs) -> bool:
        """Update agent configuration."""
        with self._lock:
            if agent_id in self._agents:
                agent = self._agents[agent_id]
                for key, value in kwargs.items():
                    if hasattr(agent, key):
                        setattr(agent, key, value)
                return True
        return False

    def delete_agent(self, agent_id: str) -> bool:
        """Delete an agent configuration."""
        with self._lock:
            if agent_id in self._agents:
                del self._agents[agent_id]
                return True
        return False

    def get_all_agents(self) -> List[AgentConfig]:
        """Get all agent configurations."""
        return list(self._agents.values())

    def create_session(self, user: WebUIUser) -> WebUISession:
        """Create a new session."""
        import uuid
        session_id = str(uuid.uuid4())
        session = WebUISession(session_id, user)
        with self._lock:
            self._sessions[session_id] = session
        return session

    def get_session(self, session_id: str) -> Optional[WebUISession]:
        """Get session by ID."""
        return self._sessions.get(session_id)

    def get_dashboard_data(self) -> Dict:
        """Get dashboard statistics."""
        data = {
            'agents': {
                'total': len(self._agents),
                'enabled': sum(1 for a in self._agents.values() if a.enabled),
                'disabled': sum(1 for a in self._agents.values() if not a.enabled)
            },
            'workers': {'total': 0, 'online': 0, 'offline': 0},
            'tasks': {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0},
            'system': {
                'uptime': time.time(),
                'version': '1.0.0'
            }
        }

        if self.orchestrator:
            try:
                workers = self.orchestrator.get_workers_status()
                data['workers']['total'] = len(workers)
                data['workers']['online'] = sum(1 for w in workers if w.get('status') == 'online')
                data['workers']['offline'] = sum(1 for w in workers if w.get('status') != 'online')

                tasks = self.orchestrator.get_all_tasks()
                data['tasks']['pending'] = sum(1 for t in tasks if t.get('status') == 'pending')
                data['tasks']['running'] = sum(1 for t in tasks if t.get('status') == 'running')
                data['tasks']['completed'] = sum(1 for t in tasks if t.get('status') == 'completed')
                data['tasks']['failed'] = sum(1 for t in tasks if t.get('status') == 'failed')
            except Exception as e:
                logger.error(f"Error getting orchestrator data: {e}")

        return data

    def get_messages(self, limit: int = 100) -> List[Dict]:
        """Get recent messages."""
        if self.orchestrator:
            try:
                return self.orchestrator.get_recent_messages(limit)
            except:
                pass
        return []

    def submit_task(self, task_type: str, task_data: Dict, priority: int = 2) -> Optional[str]:
        """Submit a task via web UI."""
        if self.orchestrator:
            try:
                return self.orchestrator.submit_task(task_type, task_data, priority)
            except Exception as e:
                logger.error(f"Error submitting task: {e}")
        return None

    def add_notification_handler(self, handler: callable):
        """Add a notification handler."""
        self._notification_handlers.append(handler)

    def send_notification(self, title: str, message: str, level: str = "info"):
        """Send notification to all handlers."""
        notification = {
            'title': title,
            'message': message,
            'level': level,
            'timestamp': time.time()
        }
        for handler in self._notification_handlers:
            try:
                handler(notification)
            except Exception as e:
                logger.error(f"Notification handler error: {e}")


# Global UI manager
_ui_manager = WebUIManager()


def get_ui_manager() -> WebUIManager:
    """Get global UI manager."""
    return _ui_manager


def configure_ui(orchestrator) -> WebUIManager:
    """Configure UI with orchestrator."""
    _ui_manager.set_orchestrator(orchestrator)
    return _ui_manager


class Theme(Enum):
    """UI theme options."""
    DARK = "dark"
    LIGHT = "light"
    AUTO = "auto"


class NotificationLevel(Enum):
    """Notification levels."""
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class Notification:
    """A notification."""
    id: str
    title: str
    message: str
    level: NotificationLevel
    timestamp: float
    read: bool = False
    source: str = None


class NotificationCenter:
    """Central notification management."""

    def __init__(self, max_notifications: int = 100):
        self.max_notifications = max_notifications
        self._notifications: List[Notification] = []
        self._lock = threading.RLock()
        self._handlers: List[Callable] = []

    def add(self, title: str, message: str, level: NotificationLevel = NotificationLevel.INFO, source: str = None) -> Notification:
        """Add a notification."""
        import uuid
        notification = Notification(
            id=str(uuid.uuid4())[:8],
            title=title,
            message=message,
            level=level,
            timestamp=time.time(),
            source=source
        )
        with self._lock:
            self._notifications.insert(0, notification)
            if len(self._notifications) > self.max_notifications:
                self._notifications = self._notifications[:self.max_notifications]

        for handler in self._handlers:
            try:
                handler(notification)
            except Exception as e:
                logger.error(f"Notification handler error: {e}")

        return notification

    def mark_read(self, notification_id: str):
        """Mark notification as read."""
        with self._lock:
            for n in self._notifications:
                if n.id == notification_id:
                    n.read = True

    def mark_all_read(self):
        """Mark all notifications as read."""
        with self._lock:
            for n in self._notifications:
                n.read = True

    def clear(self):
        """Clear all notifications."""
        with self._lock:
            self._notifications.clear()

    def get_all(self, unread_only: bool = False) -> List[Notification]:
        """Get all notifications."""
        with self._lock:
            if unread_only:
                return [n for n in self._notifications if not n.read]
            return list(self._notifications)

    def get_unread_count(self) -> int:
        """Get count of unread notifications."""
        with self._lock:
            return sum(1 for n in self._notifications if not n.read)

    def subscribe(self, handler: Callable):
        """Subscribe to new notifications."""
        self._handlers.append(handler)


class UserPreferences:
    """User preferences for web UI."""

    def __init__(self, user_id: str):
        self.user_id = user_id
        self.theme = Theme.DARK
        self.language = "zh-CN"
        self.notifications_enabled = True
        self.refresh_interval = 30
        self.items_per_page = 20
        self.timezone = "Asia/Shanghai"
        self.custom_settings: Dict = {}

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'theme': self.theme.value,
            'language': self.language,
            'notifications_enabled': self.notifications_enabled,
            'refresh_interval': self.refresh_interval,
            'items_per_page': self.items_per_page,
            'timezone': self.timezone,
            'custom_settings': self.custom_settings
        }

    @classmethod
    def from_dict(cls, user_id: str, data: Dict) -> 'UserPreferences':
        """Create from dictionary."""
        prefs = cls(user_id)
        if 'theme' in data:
            prefs.theme = Theme(data['theme'])
        prefs.language = data.get('language', 'zh-CN')
        prefs.notifications_enabled = data.get('notifications_enabled', True)
        prefs.refresh_interval = data.get('refresh_interval', 30)
        prefs.items_per_page = data.get('items_per_page', 20)
        prefs.timezone = data.get('timezone', 'Asia/Shanghai')
        prefs.custom_settings = data.get('custom_settings', {})
        return prefs


class DashboardWidget:
    """A dashboard widget."""

    def __init__(self, widget_id: str, title: str, widget_type: str):
        self.widget_id = widget_id
        self.title = title
        self.widget_type = widget_type
        self.position = 0
        self.size = "medium"
        self.config: Dict = {}

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'widget_id': self.widget_id,
            'title': self.title,
            'type': self.widget_type,
            'position': self.position,
            'size': self.size,
            'config': self.config
        }


class DashboardLayout:
    """Dashboard layout management."""

    def __init__(self, user_id: str):
        self.user_id = user_id
        self._widgets: Dict[str, DashboardWidget] = {}
        self._lock = threading.RLock()

    def add_widget(self, widget: DashboardWidget):
        """Add a widget to dashboard."""
        with self._lock:
            self._widgets[widget.widget_id] = widget

    def remove_widget(self, widget_id: str):
        """Remove a widget."""
        with self._lock:
            if widget_id in self._widgets:
                del self._widgets[widget_id]

    def get_widgets(self) -> List[Dict]:
        """Get all widgets."""
        with self._lock:
            return [w.to_dict() for w in sorted(self._widgets.values(), key=lambda x: x.position)]

    def reorder(self, widget_ids: List[str]):
        """Reorder widgets."""
        with self._lock:
            for i, wid in enumerate(widget_ids):
                if wid in self._widgets:
                    self._widgets[wid].position = i


class ActivityLog:
    """Activity log for web UI."""

    def __init__(self, max_entries: int = 1000):
        self.max_entries = max_entries
        self._entries: List[Dict] = []
        self._lock = threading.RLock()

    def add_entry(self, action: str, details: str, user: str = "system", level: str = "info"):
        """Add an activity log entry."""
        entry = {
            'id': len(self._entries),
            'action': action,
            'details': details,
            'user': user,
            'level': level,
            'timestamp': time.time()
        }
        with self._lock:
            self._entries.insert(0, entry)
            if len(self._entries) > self.max_entries:
                self._entries = self._entries[:self.max_entries]
        return entry

    def get_entries(self, limit: int = 100, level: str = None) -> List[Dict]:
        """Get activity entries."""
        with self._lock:
            entries = list(self._entries)
        if level:
            entries = [e for e in entries if e['level'] == level]
        return entries[:limit]

    def clear(self):
        """Clear all entries."""
        with self._lock:
            self._entries.clear()


# Global instances
_notification_center = NotificationCenter()
_activity_log = ActivityLog()


def get_notification_center() -> NotificationCenter:
    """Get global notification center."""
    return _notification_center


def get_activity_log() -> ActivityLog:
    """Get global activity log."""
    return _activity_log