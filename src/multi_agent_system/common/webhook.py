"""Webhook system for event notifications."""

import logging
import threading
import time
import json
import hmac
import hashlib
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('webhook')


class WebhookEvent(Enum):
    """Webhook event types."""
    TASK_SUBMITTED = "task.submitted"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    WORKER_REGISTERED = "worker.registered"
    WORKER_UNREGISTERED = "worker.unregistered"
    SYSTEM_ALERT = "system.alert"
    DEPLOYMENT_STARTED = "deployment.started"
    DEPLOYMENT_COMPLETED = "deployment.completed"


@dataclass
class WebhookConfig:
    """Configuration for a webhook."""
    url: str
    events: List[str]
    secret: Optional[str] = None
    headers: Dict = field(default_factory=dict)
    timeout: int = 30
    retry_count: int = 3
    retry_delay: float = 1.0
    enabled: bool = True


@dataclass
class WebhookDelivery:
    """Record of a webhook delivery attempt."""
    delivery_id: str
    webhook_url: str
    event: str
    payload: Dict
    timestamp: float
    attempts: int = 0
    last_attempt: Optional[float] = None
    response_code: Optional[int] = None
    response_body: Optional[str] = None
    error: Optional[str] = None
    success: bool = False


class WebhookSigner:
    """Signs webhook payloads for verification."""

    @staticmethod
    def sign(payload: Dict, secret: str) -> str:
        """Create HMAC-SHA256 signature of payload."""
        payload_str = json.dumps(payload, sort_keys=True)
        signature = hmac.new(
            secret.encode(),
            payload_str.encode(),
            hashlib.sha256
        ).hexdigest()
        return signature

    @staticmethod
    def verify(payload: Dict, signature: str, secret: str) -> bool:
        """Verify webhook signature."""
        expected = WebhookSigner.sign(payload, secret)
        return hmac.compare_digest(expected, signature)


class Webhook:
    """A webhook endpoint."""

    def __init__(self, name: str, config: WebhookConfig):
        self.name = name
        self.config = config
        self._enabled = config.enabled

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        self._enabled = value

    def matches_event(self, event: str) -> bool:
        """Check if webhook should be triggered for event."""
        if not self._enabled:
            return False
        return event in self.config.events or "*" in self.config.events

    def send(self, event: str, payload: Dict) -> WebhookDelivery:
        """Send webhook notification."""
        import uuid

        delivery = WebhookDelivery(
            delivery_id=str(uuid.uuid4()),
            webhook_url=self.config.url,
            event=event,
            payload=payload,
            timestamp=time.time()
        )

        # Add signature if secret is configured
        headers = dict(self.config.headers)
        if self.config.secret:
            signature = WebhookSigner.sign(payload, self.config.secret)
            headers['X-Webhook-Signature'] = signature

        headers['Content-Type'] = 'application/json'
        headers['X-Webhook-Event'] = event
        headers['X-Webhook-Delivery'] = delivery.delivery_id

        for attempt in range(self.config.retry_count):
            delivery.attempts += 1
            delivery.last_attempt = time.time()

            try:
                data = json.dumps(payload).encode()
                request = urllib.request.Request(
                    self.config.url,
                    data=data,
                    headers=headers,
                    method='POST'
                )

                with urllib.request.urlopen(request, timeout=self.config.timeout) as response:
                    delivery.response_code = response.status
                    delivery.response_body = response.read().decode()
                    delivery.success = True
                    logger.info(f"Webhook delivered: {delivery.delivery_id}")
                    return delivery

            except urllib.error.HTTPError as e:
                delivery.response_code = e.code
                delivery.response_body = e.read().decode()
                delivery.error = str(e)
                logger.warning(f"Webhook failed (attempt {attempt + 1}): {e}")

            except Exception as e:
                delivery.error = str(e)
                logger.warning(f"Webhook error (attempt {attempt + 1}): {e}")

            # Retry with delay
            if attempt < self.config.retry_count - 1:
                time.sleep(self.config.retry_delay * (attempt + 1))

        logger.error(f"Webhook delivery failed after {delivery.attempts} attempts")
        return delivery


class WebhookManager:
    """
    Manages webhooks and handles event dispatching.
    """

    def __init__(self):
        self._webhooks: Dict[str, Webhook] = {}
        self._deliveries: List[WebhookDelivery] = []
        self._lock = threading.RLock()
        self._max_deliveries = 1000
        self._handlers: Dict[str, List[Callable]] = defaultdict(list)

    def register(self, name: str, url: str, events: List[str],
                **config) -> Webhook:
        """Register a new webhook."""
        webhook_config = WebhookConfig(
            url=url,
            events=events,
            **{k: v for k, v in config.items() if v is not None}
        )

        webhook = Webhook(name, webhook_config)

        with self._lock:
            self._webhooks[name] = webhook

        logger.info(f"Registered webhook: {name} for events {events}")
        return webhook

    def unregister(self, name: str) -> bool:
        """Unregister a webhook."""
        with self._lock:
            if name in self._webhooks:
                del self._webhooks[name]
                logger.info(f"Unregistered webhook: {name}")
                return True
        return False

    def get(self, name: str) -> Optional[Webhook]:
        """Get webhook by name."""
        with self._lock:
            return self._webhooks.get(name)

    def list_webhooks(self) -> List[Webhook]:
        """List all registered webhooks."""
        with self._lock:
            return list(self._webhooks.values())

    def enable(self, name: str):
        """Enable a webhook."""
        webhook = self.get(name)
        if webhook:
            webhook.enabled = True
            logger.info(f"Enabled webhook: {name}")

    def disable(self, name: str):
        """Disable a webhook."""
        webhook = self.get(name)
        if webhook:
            webhook.enabled = False
            logger.info(f"Disabled webhook: {name}")

    def trigger(self, event: str, payload: Dict,
                sync: bool = False) -> List[WebhookDelivery]:
        """Trigger webhooks for an event."""
        deliveries = []

        with self._lock:
            matching = [
                (name, wh) for name, wh in self._webhooks.items()
                if wh.matches_event(event)
            ]

        for name, webhook in matching:
            if sync:
                delivery = webhook.send(event, payload)
                deliveries.append(delivery)
                self._record_delivery(delivery)
            else:
                # Async delivery
                thread = threading.Thread(
                    target=self._deliver_async,
                    args=(name, event, payload)
                )
                thread.start()

        return deliveries

    def _deliver_async(self, name: str, event: str, payload: Dict):
        """Deliver webhook asynchronously."""
        webhook = self.get(name)
        if webhook:
            delivery = webhook.send(event, payload)
            self._record_delivery(delivery)

    def _record_delivery(self, delivery: WebhookDelivery):
        """Record a delivery attempt."""
        with self._lock:
            self._deliveries.append(delivery)

            # Trim old deliveries
            if len(self._deliveries) > self._max_deliveries:
                self._deliveries = self._deliveries[-self._max_deliveries:]

    def get_deliveries(self, webhook_url: str = None,
                       event: str = None,
                       limit: int = 100) -> List[WebhookDelivery]:
        """Get delivery history."""
        with self._lock:
            deliveries = self._deliveries

            if webhook_url:
                deliveries = [d for d in deliveries if d.webhook_url == webhook_url]

            if event:
                deliveries = [d for d in deliveries if d.event == event]

            return deliveries[-limit:]

    def add_handler(self, event: str, handler: Callable):
        """Add a handler for an event type."""
        self._handlers[event].append(handler)

    def handle_event(self, event: str, payload: Dict):
        """Handle an event locally (in addition to webhooks)."""
        handlers = self._handlers.get(event, [])
        for handler in handlers:
            try:
                handler(event, payload)
            except Exception as e:
                logger.error(f"Webhook handler failed for {event}: {e}")

    def test_webhook(self, name: str) -> WebhookDelivery:
        """Send a test webhook."""
        webhook = self.get(name)
        if not webhook:
            raise ValueError(f"Webhook {name} not found")

        test_payload = {
            "event": "test",
            "timestamp": time.time(),
            "data": {"test": True}
        }

        return webhook.send("test", test_payload)

    def get_stats(self) -> Dict:
        """Get webhook statistics."""
        with self._lock:
            total_deliveries = len(self._deliveries)
            successful = sum(1 for d in self._deliveries if d.success)
            failed = total_deliveries - successful

            by_event = defaultdict(int)
            for d in self._deliveries:
                by_event[d.event] += 1

            return {
                'total_webhooks': len(self._webhooks),
                'enabled': sum(1 for w in self._webhooks.values() if w.enabled),
                'total_deliveries': total_deliveries,
                'successful': successful,
                'failed': failed,
                'by_event': dict(by_event)
            }


class WebhookEventBuilder:
    """Builds standardized webhook payloads."""

    @staticmethod
    def task_event(event_type: str, task: Dict) -> Dict:
        """Build task event payload."""
        return {
            "event": event_type,
            "timestamp": time.time(),
            "data": {
                "task_id": task.get("task_id"),
                "task_type": task.get("task_type"),
                "status": task.get("status"),
                "result": task.get("result"),
                "error": task.get("error")
            }
        }

    @staticmethod
    def worker_event(event_type: str, worker: Dict) -> Dict:
        """Build worker event payload."""
        return {
            "event": event_type,
            "timestamp": time.time(),
            "data": {
                "worker_id": worker.get("worker_id"),
                "worker_type": worker.get("worker_type"),
                "status": worker.get("status")
            }
        }

    @staticmethod
    def system_event(event_type: str, message: str,
                    severity: str = "info") -> Dict:
        """Build system event payload."""
        return {
            "event": event_type,
            "timestamp": time.time(),
            "data": {
                "message": message,
                "severity": severity
            }
        }


# Global webhook manager
_webhook_manager = WebhookManager()


def get_webhook_manager() -> WebhookManager:
    return _webhook_manager


def register_webhook(name: str, url: str, events: List[str], **config) -> Webhook:
    """Register a webhook."""
    return _webhook_manager.register(name, url, events, **config)