"""Protocol interfaces for multi-agent system."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional


class IMessageQueue(ABC):
    @abstractmethod
    def enqueue(self, queue_name: str, message: 'Message', timeout: float = None) -> bool:
        pass

    @abstractmethod
    def dequeue(self, queue_name: str, timeout: float = 1.0) -> Optional['Message']:
        pass

    @abstractmethod
    def size(self, queue_name: str) -> int:
        pass


class IOrchestrator(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def submit_task(self, task_type: str, task_data: Dict,
                    priority: int = 2, timeout: int = 300) -> str:
        pass

    @abstractmethod
    def get_task_status(self, task_id: str) -> Optional[Dict]:
        pass


class IWorkerAgent(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def register_handler(self, task_type: str, handler: callable):
        pass


class ITaskHandler(ABC):
    @abstractmethod
    def handle(self, task_data: Dict) -> Any:
        pass

    @abstractmethod
    def validate(self, task_data: Dict) -> bool:
        pass