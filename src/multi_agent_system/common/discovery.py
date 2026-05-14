"""Service discovery for automatic worker-orchestrator registration."""

import logging
import socket
import threading
import time
import json
from typing import Dict, List, Optional, Callable

logger = logging.getLogger('discovery')


class ServiceRegistry:
    """
    Service discovery registry.
    Workers can discover orchestrator automatically.
    """

    def __init__(self, service_name: str = "multi-agent-orchestrator",
                 port: int = 8080):
        self.service_name = service_name
        self.port = port
        self._services: Dict[str, Dict] = {}  # service_id -> service_info
        self._discovery_callback: Optional[Callable] = None
        self._running = False
        self._announce_thread = None
        self._lock = threading.RLock()

    def set_discovery_callback(self, callback: Callable):
        """Set callback to be notified when services are discovered."""
        self._discovery_callback = callback

    def register_service(self, service_id: str, host: str, port: int,
                        metadata: Dict = None) -> bool:
        """Register a service."""
        with self._lock:
            self._services[service_id] = {
                'service_id': service_id,
                'host': host,
                'port': port,
                'metadata': metadata or {},
                'registered_at': time.time(),
                'last_seen': time.time()
            }
        logger.info(f'Registered service: {service_id} at {host}:{port}')
        return True

    def unregister_service(self, service_id: str) -> bool:
        """Unregister a service."""
        with self._lock:
            if service_id in self._services:
                del self._services[service_id]
                logger.info(f'Unregistered service: {service_id}')
                return True
        return False

    def get_service(self, service_id: str) -> Optional[Dict]:
        """Get service by ID."""
        with self._lock:
            return self._services.get(service_id)

    def get_all_services(self) -> List[Dict]:
        """Get all registered services."""
        with self._lock:
            return list(self._services.values())

    def get_services_by_tag(self, tag: str) -> List[Dict]:
        """Get services matching a tag."""
        with self._lock:
            return [
                s for s in self._services.values()
                if tag in s.get('metadata', {}).get('tags', [])
            ]

    def update_last_seen(self, service_id: str):
        """Update last_seen timestamp for a service."""
        with self._lock:
            if service_id in self._services:
                self._services[service_id]['last_seen'] = time.time()

    def cleanup_stale(self, max_age: float = 60.0) -> int:
        """Remove services that haven't been seen recently."""
        now = time.time()
        removed = 0
        with self._lock:
            stale = [
                sid for sid, s in self._services.items()
                if now - s['last_seen'] > max_age
            ]
            for sid in stale:
                del self._services[sid]
                removed += 1
        if removed:
            logger.info(f'Removed {removed} stale services')
        return removed


class OrchestratorAdvertiser:
    """
    Advertises orchestrator service for workers to discover.
    Uses UDP broadcast for auto-discovery.
    """

    def __init__(self, service_name: str = "multi-agent-orchestrator",
                 broadcast_port: int = 9999):
        self.service_name = service_name
        self.broadcast_port = broadcast_port
        self._running = False
        self._advertise_thread = None
        self._socket = None

    def start_advertising(self, host: str, port: int, metadata: Dict = None):
        """Start advertising orchestrator service."""
        self._running = True
        self._advertise_thread = threading.Thread(
            target=self._advertise_loop,
            args=(host, port, metadata or {}),
            daemon=True
        )
        self._advertise_thread.start()
        logger.info(f'Started advertising orchestrator at {host}:{port}')

    def stop_advertising(self):
        """Stop advertising."""
        self._running = False
        if self._socket:
            self._socket.close()
        logger.info('Stopped advertising')

    def _advertise_loop(self, host: str, port: int, metadata: Dict):
        """Background advertise loop."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self._socket = sock

            while self._running:
                message = json.dumps({
                    'service': self.service_name,
                    'host': host,
                    'port': port,
                    'metadata': metadata
                })
                sock.sendto(message.encode(), ('<broadcast>', self.broadcast_port))
                time.sleep(5)
        except Exception as e:
            logger.error(f'Advertising error: {e}')

    def _advertise_once(self, host: str, port: int, metadata: Dict = None):
        """Advertise once (for manual discovery)."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            message = json.dumps({
                'service': self.service_name,
                'host': host,
                'port': port,
                'metadata': metadata or {}
            })
            sock.sendto(message.encode(), ('<broadcast>', self.broadcast_port))
            sock.close()
            return True
        except Exception as e:
            logger.error(f'Advertise error: {e}')
            return False


class WorkerDiscovery:
    """
    Worker-side service discovery.
    Discovers orchestrator automatically via broadcast.
    """

    def __init__(self, service_name: str = "multi-agent-orchestrator",
                 broadcast_port: int = 9999,
                 discovery_timeout: float = 10.0):
        self.service_name = service_name
        self.broadcast_port = broadcast_port
        self.discovery_timeout = discovery_timeout
        self._socket = None
        self._running = False

    def discover(self) -> Optional[Dict]:
        """Discover orchestrator service. Returns first match."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(self.discovery_timeout)
            self._socket = sock

            # Send discovery request
            message = json.dumps({'action': 'discover', 'service': self.service_name})
            sock.sendto(message.encode(), ('<broadcast>', self.broadcast_port))

            # Wait for response
            data, addr = sock.recvfrom(4096)
            info = json.loads(data.decode())

            if info.get('service') == self.service_name:
                logger.info(f'Discovered orchestrator at {info["host"]}:{info["port"]}')
                return info

        except socket.timeout:
            logger.warning('Orchestrator discovery timed out')
        except Exception as e:
            logger.error(f'Discovery error: {e}')
        finally:
            if self._socket:
                self._socket.close()

        return None

    def discover_all(self, max_count: int = 3) -> List[Dict]:
        """Discover all orchestrator instances."""
        results = []
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(self.discovery_timeout)
            self._socket = sock

            # Send discovery request
            message = json.dumps({'action': 'discover', 'service': self.service_name})
            sock.sendto(message.encode(), ('<broadcast>', self.broadcast_port))

            # Collect responses
            while len(results) < max_count:
                try:
                    data, addr = sock.recvfrom(4096)
                    info = json.loads(data.decode())
                    if info.get('service') == self.service_name:
                        results.append(info)
                except socket.timeout:
                    break

        except Exception as e:
            logger.error(f'Discovery error: {e}')
        finally:
            if self._socket:
                self._socket.close()

        return results


class StaticServiceResolver:
    """
    Static service resolution.
    Use when broadcast discovery isn't available.
    """

    def __init__(self):
        self._services: Dict[str, Dict] = {}

    def add_service(self, name: str, host: str, port: int, metadata: Dict = None):
        """Add a static service entry."""
        self._services[name] = {
            'service': name,
            'host': host,
            'port': port,
            'metadata': metadata or {}
        }
        logger.info(f'Added static service: {name} at {host}:{port}')

    def resolve(self, service_name: str) -> Optional[Dict]:
        """Resolve service by name."""
        return self._services.get(service_name)


# Global registry
_registry = ServiceRegistry()
_resolver = StaticServiceResolver()


def get_registry() -> ServiceRegistry:
    return _registry


def get_resolver() -> StaticServiceResolver:
    return _resolver