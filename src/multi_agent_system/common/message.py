"""Message definitions for multi-agent communication."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional


class MessageType(Enum):
    TASK = "TASK"
    TASK_RESULT = "TASK_RESULT"
    TASK_CANCEL = "TASK_CANCEL"
    WORKER_REGISTER = "WORKER_REGISTER"
    WORKER_UNREGISTER = "WORKER_UNREGISTER"
    HEARTBEAT = "HEARTBEAT"
    HEARTBEAT_ACK = "HEARTBEAT_ACK"
    SHUTDOWN = "SHUTDOWN"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"


class QueueType(Enum):
    ORCHESTRATOR_TO_WORKER = "orchestrator_to_worker"
    WORKER_TO_ORCHESTRATOR = "worker_to_orchestrator"
    WORKER_HEARTBEAT = "worker_heartbeat"
    DEAD_LETTER = "dead_letter"


class MessagePriority(Enum):
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class Message:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: str = ""
    action: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    priority: MessagePriority = MessagePriority.NORMAL
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: Optional[str] = None
    source: str = ""
    target: str = ""
    ttl: int = 300
    retry_count: int = 0
    max_retries: int = 3