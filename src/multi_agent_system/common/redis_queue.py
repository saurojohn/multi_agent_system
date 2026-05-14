"""Redis-backed message queue for distributed multi-agent deployment."""

import json
import logging
import time
import uuid
from typing import Optional, List

logger = logging.getLogger('redis_queue')

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis package not available. Install with: pip install redis")


class RedisMessageQueue:
    """Redis-backed message queue for distributed deployment."""

    def __init__(self, host='localhost', port=6379, db=0,
                 password=None, queue_prefix='mas:'):
        if not REDIS_AVAILABLE:
            raise ImportError("Redis package required: pip install redis")

        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        self.queue_prefix = queue_prefix
        self._local_queue = []  # Fallback local queue
        self._use_redis = True

    def _queue_name(self, queue_type: str) -> str:
        return f"{self.queue_prefix}{queue_type}"

    def enqueue(self, queue_type: str, message) -> bool:
        """Add message to queue."""
        try:
            if self._use_redis:
                msg_data = {
                    'id': str(uuid.uuid4()),
                    'type': message.type,
                    'action': getattr(message, 'action', ''),
                    'payload': message.payload,
                    'source': getattr(message, 'source', ''),
                    'target': getattr(message, 'target', '*'),
                    'priority': getattr(message, 'priority', 0),
                    'timestamp': time.time()
                }
                self.redis.rpush(self._queue_name(queue_type), json.dumps(msg_data))
                return True
            else:
                self._local_queue.append(message)
                return True
        except Exception as e:
            logger.error(f"Redis enqueue failed: {e}")
            self._use_redis = False
            self._local_queue.append(message)
            return False

    def dequeue(self, queue_type: str, timeout: float = 1.0) -> Optional:
        """Remove and return message from queue. Blocks for timeout seconds."""
        from ..common.message import Message, MessageType, QueueType, MessagePriority

        start = time.time()
        while time.time() - start < timeout:
            try:
                if self._use_redis:
                    data = self.redis.lpop(self._queue_name(queue_type))
                    if data:
                        msg_dict = json.loads(data)
                        return Message(
                            type=msg_dict.get('type', ''),
                            action=msg_dict.get('action', ''),
                            payload=msg_dict.get('payload', {}),
                            source=msg_dict.get('source', ''),
                            target=msg_dict.get('target', '*'),
                            priority=msg_dict.get('priority', 0),
                            correlation_id=msg_dict.get('id', '')
                        )
                else:
                    if self._local_queue:
                        return self._local_queue.pop(0)
            except Exception as e:
                logger.error(f"Redis dequeue failed: {e}")
                self._use_redis = False

            time.sleep(0.1)

        return None

    def peek(self, queue_type: str) -> Optional:
        """View message without removing."""
        try:
            if self._use_redis:
                data = self.redis.lindex(self._queue_name(queue_type), 0)
                if data:
                    return json.loads(data)
            else:
                if self._local_queue:
                    return self._local_queue[0]
        except Exception as e:
            logger.error(f"Redis peek failed: {e}")

        return None

    def size(self, queue_type: str) -> int:
        """Get queue size."""
        try:
            if self._use_redis:
                return self.redis.llen(self._queue_name(queue_type))
            else:
                return len(self._local_queue)
        except Exception as e:
            logger.error(f"Redis size failed: {e}")
            return 0

    def clear(self, queue_type: str) -> bool:
        """Clear all messages from queue."""
        try:
            if self._use_redis:
                self.redis.delete(self._queue_name(queue_type))
            else:
                self._local_queue.clear()
            return True
        except Exception as e:
            logger.error(f"Redis clear failed: {e}")
            return False

    def is_healthy(self) -> bool:
        """Check if Redis connection is healthy."""
        if not self._use_redis:
            return len(self._local_queue) >= 0  # Local queue always healthy
        try:
            return self.redis.ping()
        except:
            return False


class RedisMessageQueueManager:
    """Manages multiple Redis-backed queues."""

    def __init__(self, host='localhost', port=6379, db=0,
                 password=None, queue_prefix='mas:'):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.queue_prefix = queue_prefix
        self._queues = {}

    def get_queue(self, queue_type: str) -> RedisMessageQueue:
        if queue_type not in self._queues:
            self._queues[queue_type] = RedisMessageQueue(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                queue_prefix=self.queue_prefix
            )
        return self._queues[queue_type]

    def enqueue(self, queue_type: str, message) -> bool:
        return self.get_queue(queue_type).enqueue(queue_type, message)

    def dequeue(self, queue_type: str, timeout: float = 1.0):
        return self.get_queue(queue_type).dequeue(queue_type, timeout)

    def size(self, queue_type: str) -> int:
        return self.get_queue(queue_type).size(queue_type)

    def is_healthy(self) -> bool:
        if self._queues:
            return list(self._queues.values())[0].is_healthy()
        return True