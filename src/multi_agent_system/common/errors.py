"""Error handling for multi-agent system."""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any


class ErrorCode(Enum):
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    TASK_TIMEOUT = "TASK_TIMEOUT"
    TASK_CANCELLED = "TASK_CANCELLED"
    TASK_FAILED = "TASK_FAILED"
    WORKER_OFFLINE = "WORKER_OFFLINE"
    WORKER_BUSY = "WORKER_BUSY"
    WORKER_CAPACITY = "WORKER_CAPACITY"
    QUEUE_FULL = "QUEUE_FULL"
    MESSAGE_LOST = "MESSAGE_LOST"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


class RecoveryStrategy(Enum):
    RETRY = "retry"
    REASSIGN = "reassign"
    FALLBACK = "fallback"
    ABORT = "abort"


@dataclass
class ErrorInfo:
    code: ErrorCode
    message: str
    task_id: Optional[str] = None
    worker_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    retry_count: int = 0


class ErrorHandler:
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries

    def handle_error(self, error: ErrorInfo) -> RecoveryStrategy:
        retry_strategies = {
            ErrorCode.TASK_TIMEOUT: RecoveryStrategy.RETRY,
            ErrorCode.WORKER_OFFLINE: RecoveryStrategy.REASSIGN,
            ErrorCode.WORKER_BUSY: RecoveryStrategy.REASSIGN,
            ErrorCode.QUEUE_FULL: RecoveryStrategy.RETRY,
            ErrorCode.UNKNOWN_ERROR: RecoveryStrategy.RETRY,
        }

        strategy = retry_strategies.get(error.code, RecoveryStrategy.ABORT)

        if strategy == RecoveryStrategy.RETRY and error.retry_count >= self.max_retries:
            return RecoveryStrategy.FALLBACK

        return strategy