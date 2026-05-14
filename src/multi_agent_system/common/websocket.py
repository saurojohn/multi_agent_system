"""WebSocket event broadcaster for real-time updates."""

import json
import logging
import threading
from typing import List, Callable

logger = logging.getLogger('websocket')


class EventBroadcaster:
    """Manages WebSocket connections and broadcasts events."""

    def __init__(self):
        self._clients: List[object] = []
        self._lock = threading.Lock()

    def add_client(self, client):
        with self._lock:
            self._clients.append(client)
            logger.info(f'WebSocket client connected. Total: {len(self._clients)}')

    def remove_client(self, client):
        with self._lock:
            if client in self._clients:
                self._clients.remove(client)
            logger.info(f'WebSocket client disconnected. Total: {len(self._clients)}')

    def broadcast(self, event_type: str, data: dict):
        """Broadcast event to all connected clients."""
        message = json.dumps({
            "event": event_type,
            "data": data,
            "timestamp": self._get_timestamp()
        })
        with self._lock:
            dead_clients = []
            for client in self._clients:
                try:
                    client.send(message)
                except Exception as e:
                    logger.warning(f'Failed to send to client: {e}')
                    dead_clients.append(client)
            # Remove dead clients
            for client in dead_clients:
                self._clients.remove(client)

    def broadcast_task_update(self, task_id: str, status: str, result=None, error=None):
        self.broadcast("task_update", {
            "task_id": task_id,
            "status": status,
            "result": result,
            "error": error
        })

    def broadcast_worker_update(self, worker_id: str, status: str, stats: dict):
        self.broadcast("worker_update", {
            "worker_id": worker_id,
            "status": status,
            "completed": stats.get("completed", 0),
            "failed": stats.get("failed", 0)
        })

    def _get_timestamp(self) -> float:
        import time
        return time.time()


# Global broadcaster instance
_broadcaster = EventBroadcaster()


def get_broadcaster() -> EventBroadcaster:
    return _broadcaster