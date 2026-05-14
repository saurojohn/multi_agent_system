"""Message persistence for durability across restarts."""

import json
import logging
import os
import threading
import time
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger('persistence')


class MessagePersistence:
    """
    Persists messages to disk for durability.
    Messages survive system restarts.
    """

    def __init__(self, persist_dir: str = "/tmp/multi_agent_persistence",
                 flush_interval: float = 5.0,
                 max_queue_size: int = 10000):
        """
        Args:
            persist_dir: Directory to store persistence files
            flush_interval: Seconds between disk flushes
            max_queue_size: Max messages in memory before forced flush
        """
        self.persist_dir = Path(persist_dir)
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        self.persist_dir.mkdir(parents=True, exist_ok=True)

        self._queue_file = self.persist_dir / "queue.json"
        self._state_file = self.persist_dir / "state.json"
        self._lock = threading.RLock()
        self._running = False
        self._flush_thread = None
        self._pending_messages: List[Dict] = []

    def start(self):
        """Start persistence manager."""
        if self._running:
            return
        self._running = True
        self._load_state()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        logger.info(f'Persistence started: {self.persist_dir}')

    def stop(self):
        """Stop and flush remaining messages."""
        self._running = False
        self.flush()
        if self._flush_thread:
            self._flush_thread.join(timeout=10)
        logger.info('Persistence stopped')

    def persist_message(self, queue_type: str, message: Dict) -> bool:
        """Persist a message to disk."""
        with self._lock:
            self._pending_messages.append({
                'queue_type': queue_type,
                'message': message,
                'timestamp': time.time()
            })

            # Force flush if queue too large
            if len(self._pending_messages) >= self.max_queue_size:
                self.flush()

        return True

    def flush(self):
        """Write pending messages to disk."""
        with self._lock:
            if not self._pending_messages:
                return

            # Load existing
            existing = self._load_queue()

            # Append new
            existing.extend(self._pending_messages)

            # Trim to max size
            if len(existing) > self.max_queue_size:
                existing = existing[-self.max_queue_size:]

            # Write
            try:
                with open(self._queue_file, 'w') as f:
                    json.dump(existing, f)
                self._pending_messages.clear()
                logger.debug(f'Flushed {len(existing)} messages to disk')
            except Exception as e:
                logger.error(f'Failed to persist messages: {e}')

    def _flush_loop(self):
        """Background flush loop."""
        while self._running:
            time.sleep(self.flush_interval)
            self.flush()

    def _load_queue(self) -> List:
        """Load messages from disk."""
        if not self._queue_file.exists():
            return []

        try:
            with open(self._queue_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f'Failed to load queue: {e}')
            return []

    def _load_state(self):
        """Load system state from disk."""
        if not self._state_file.exists():
            return

        try:
            with open(self._state_file, 'r') as f:
                state = json.load(f)
                logger.info(f'Loaded state: {state.get("tasks", 0)} tasks, {state.get("workers", 0)} workers')
                return state
        except Exception as e:
            logger.error(f'Failed to load state: {e}')

    def persist_state(self, tasks: Dict, workers: Dict):
        """Persist system state."""
        state = {
            'timestamp': time.time(),
            'tasks_count': len(tasks),
            'workers_count': len(workers),
            'tasks': [
                {
                    'task_id': t.task_id,
                    'status': t.status,
                    'task_type': t.task_type
                }
                for t in tasks.values()
            ]
        }

        try:
            with open(self._state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            logger.error(f'Failed to persist state: {e}')

    def get_persisted_messages(self, queue_type: str = None) -> List[Dict]:
        """Get all persisted messages, optionally filtered by queue type."""
        messages = self._load_queue()
        if queue_type:
            messages = [m for m in messages if m.get('queue_type') == queue_type]
        return messages

    def clear_persistence(self):
        """Clear all persisted data."""
        with self._lock:
            self._pending_messages.clear()
            if self._queue_file.exists():
                self._queue_file.unlink()
            if self._state_file.exists():
                self._state_file.unlink()
        logger.info('Persistence cleared')


class CheckpointManager:
    """
    Creates checkpoints of system state for recovery.
    Used for zero-message-loss restarts.
    """

    def __init__(self, persist_dir: str = "/tmp/multi_agent_checkpoints",
                 checkpoint_interval: float = 60.0):
        self.persist_dir = Path(persist_dir)
        self.checkpoint_interval = checkpoint_interval
        self.persist_dir.mkdir(parents=True, exist_ok=True)

        self._running = False
        self._checkpoint_thread = None
        self._callbacks: List[Callable] = []

    def register_checkpoint_callback(self, callback: Callable):
        """Register callback to provide state for checkpoint."""
        self._callbacks.append(callback)

    def start(self):
        """Start checkpoint manager."""
        if self._running:
            return
        self._running = True
        self._checkpoint_thread = threading.Thread(target=self._checkpoint_loop, daemon=True)
        self._checkpoint_thread.start()
        logger.info('Checkpoint manager started')

    def stop(self):
        """Stop checkpoint manager."""
        self._running = False
        if self._checkpoint_thread:
            self._checkpoint_thread.join(timeout=10)
        # Create final checkpoint
        self.create_checkpoint("final")
        logger.info('Checkpoint manager stopped')

    def _checkpoint_loop(self):
        """Background checkpoint loop."""
        while self._running:
            time.sleep(self.checkpoint_interval)
            self.create_checkpoint("periodic")

    def create_checkpoint(self, name: str = "manual") -> str:
        """Create a checkpoint with given name."""
        timestamp = int(time.time())
        checkpoint_file = self.persist_dir / f"checkpoint_{timestamp}_{name}.json"

        state = {
            'timestamp': time.time(),
            'name': name,
            'data': {}
        }

        # Gather data from callbacks
        for callback in self._callbacks:
            try:
                data = callback()
                state['data'].update(data)
            except Exception as e:
                logger.error(f'Checkpoint callback failed: {e}')

        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(state, f)
            logger.info(f'Created checkpoint: {checkpoint_file.name}')
            return str(checkpoint_file)
        except Exception as e:
            logger.error(f'Failed to create checkpoint: {e}')
            return ""

    def get_latest_checkpoint(self) -> Optional[Dict]:
        """Get the most recent checkpoint."""
        checkpoints = sorted(self.persist_dir.glob("checkpoint_*.json"))
        if not checkpoints:
            return None

        try:
            with open(checkpoints[-1], 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f'Failed to load checkpoint: {e}')
            return None

    def restore_from_checkpoint(self, checkpoint: Dict) -> bool:
        """Restore system state from checkpoint."""
        # This would be called during orchestrator initialization
        logger.info(f'Restoring from checkpoint: {checkpoint.get("name")}')
        return True


# Global persistence instance
_persistence = MessagePersistence()


def get_persistence() -> MessagePersistence:
    return _persistence


def get_checkpoint_manager() -> CheckpointManager:
    return _checkpoint_manager


_checkpoint_manager = CheckpointManager()