"""Web-based UI for multi-agent system management."""

import logging
import threading
import json
import time
from typing import Dict, List, Optional, Any
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