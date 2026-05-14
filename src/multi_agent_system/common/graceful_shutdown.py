"""Graceful shutdown handling for clean process termination."""

import logging
import threading
import time
import signal
import sys
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('graceful_shutdown')


class ShutdownPhase(Enum):
    """Phases of graceful shutdown."""
    PREPARE = "prepare"      # Prepare for shutdown
    DRAIN = "drain"          # Drain in-progress requests
    FORCE = "force"          # Force stop remaining
    COMPLETE = "complete"    # Shutdown complete


@dataclass
class ShutdownConfig:
    """Configuration for graceful shutdown."""
    drain_timeout: int = 30        # Seconds to wait for drain
    force_timeout: int = 10        # Seconds to wait before force
    stop_timeout: int = 5          # Seconds to wait for thread stop
    enable_signal_handler: bool = True
    signal_types: List[int] = field(default_factory=lambda: [signal.SIGTERM, signal.SIGINT])


@dataclass
class ShutdownState:
    """Current shutdown state."""
    phase: ShutdownPhase
    start_time: float
    remaining_tasks: int = 0
    stopped_components: List[str] = field(default_factory=list)


class Component:
    """A component that can be gracefully stopped."""

    def __init__(self, name: str):
        self.name = name
        self._running = False
        self._thread: threading.Thread = None

    def start(self):
        """Start the component."""
        self._running = True

    def stop(self, timeout: float = None) -> bool:
        """Stop the component."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=timeout)
        return True

    def drain(self, timeout: float) -> bool:
        """Drain in-progress work."""
        return self.stop(timeout)

    def is_running(self) -> bool:
        """Check if component is running."""
        return self._running


class GracefulShutdownManager:
    """
    Manages graceful shutdown of the system.
    """

    def __init__(self, config: ShutdownConfig = None):
        self.config = config or ShutdownConfig()
        self._components: Dict[str, Component] = {}
        self._hooks: Dict[ShutdownPhase, List[Callable]] = {
            phase: [] for phase in ShutdownPhase
        }
        self._state: Optional[ShutdownState] = None
        self._lock = threading.Lock()
        self._shutdown_triggered = False
        self._original_signal_handlers: Dict[int, Any] = {}

    def register_component(self, component: Component):
        """Register a component for graceful shutdown."""
        with self._lock:
            self._components[component.name] = component
            logger.info(f"Registered component for shutdown: {component.name}")

    def unregister_component(self, name: str):
        """Unregister a component."""
        with self._lock:
            if name in self._components:
                del self._components[name]

    def add_hook(self, phase: ShutdownPhase, hook: Callable):
        """Add a hook for a shutdown phase."""
        with self._lock:
            self._hooks[phase].append(hook)

    def trigger_shutdown(self):
        """Trigger the graceful shutdown sequence."""
        if self._shutdown_triggered:
            return

        self._shutdown_triggered = True
        logger.info("Graceful shutdown triggered")

        thread = threading.Thread(target=self._execute_shutdown, daemon=True)
        thread.start()

    def _execute_shutdown(self):
        """Execute the shutdown sequence."""
        start_time = time.time()

        try:
            # Phase 1: PREPARE
            self._state = ShutdownState(
                phase=ShutdownPhase.PREPARE,
                start_time=start_time
            )
            self._run_hooks(ShutdownPhase.PREPARE)
            logger.info("Shutdown prepare phase complete")

            # Phase 2: DRAIN
            self._state.phase = ShutdownPhase.DRAIN
            remaining = self._drain_components(self.config.drain_timeout)
            self._state.remaining_tasks = remaining
            logger.info(f"Shutdown drain phase complete: {remaining} tasks remaining")

            # Phase 3: FORCE
            self._state.phase = ShutdownPhase.FORCE
            self._force_stop_components(self.config.force_timeout)
            logger.info("Shutdown force phase complete")

            # Phase 4: COMPLETE
            self._state.phase = ShutdownPhase.COMPLETE
            self._run_hooks(ShutdownPhase.COMPLETE)
            logger.info("Graceful shutdown complete")

        except Exception as e:
            logger.error(f"Shutdown error: {e}")

    def _run_hooks(self, phase: ShutdownPhase):
        """Run hooks for a phase."""
        for hook in self._hooks.get(phase, []):
            try:
                hook()
            except Exception as e:
                logger.error(f"Shutdown hook failed: {e}")

    def _drain_components(self, timeout: float) -> int:
        """Drain all components."""
        remaining = 0
        deadline = time.time() + timeout

        for name, component in list(self._components.items()):
            try:
                if component.is_running():
                    if not component.drain(max(0, deadline - time.time())):
                        remaining += 1
                    self._state.stopped_components.append(name)
            except Exception as e:
                logger.error(f"Component drain failed {name}: {e}")

        return remaining

    def _force_stop_components(self, timeout: float):
        """Force stop all components."""
        per_component_timeout = timeout / max(1, len(self._components))

        for name, component in list(self._components.items()):
            try:
                component.stop(per_component_timeout)
                self._state.stopped_components.append(name)
            except Exception as e:
                logger.error(f"Component stop failed {name}: {e}")

    def get_state(self) -> Optional[ShutdownState]:
        """Get current shutdown state."""
        with self._lock:
            return self._state

    def is_shutting_down(self) -> bool:
        """Check if shutdown is in progress."""
        return self._shutdown_triggered and (
            self._state is None or self._state.phase != ShutdownPhase.COMPLETE
        )


class GracefulServer:
    """
    HTTP server with graceful shutdown support.
    """

    def __init__(self, port: int, shutdown_manager: GracefulShutdownManager = None):
        self.port = port
        self.shutdown_manager = shutdown_manager or GracefulShutdownManager()
        self._server = None
        self._running = False

    def start(self, handler: Callable = None):
        """Start the server."""
        import http.server
        import socketserver

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/health':
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"status": "ok"}')
                elif self.path == '/shutdown':
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(b'{"shutdown": "triggered"}')
                    self.server.shutdown_manager.trigger_shutdown()
                else:
                    if handler:
                        handler(self)
                    else:
                        self.send_response(404)

        class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._server = ThreadedHTTPServer(("", self.port), Handler)
        self._server.shutdown_manager = self.shutdown_manager
        self._running = True

        logger.info(f"Server started on port {self.port}")
        self._server.serve_forever()

    def start_background(self):
        """Start server in background."""
        thread = threading.Thread(target=self.start, daemon=True)
        thread.start()

    def stop(self):
        """Stop the server."""
        self._running = False
        if self._server:
            self._server.shutdown()


class ShutdownContext:
    """
    Context manager for graceful shutdown of a component.
    """

    def __init__(self, name: str, shutdown_manager: GracefulShutdownManager = None):
        self.name = name
        self.shutdown_manager = shutdown_manager or _default_manager
        self.component = Component(name)

    def __enter__(self):
        self.component.start()
        self.shutdown_manager.register_component(self.component)
        return self.component

    def __exit__(self, *args):
        self.shutdown_manager.unregister_component(self.name)


# Global shutdown manager
_default_manager = GracefulShutdownManager()


def get_shutdown_manager() -> GracefulShutdownManager:
    return _default_manager


def setup_signal_handlers(manager: GracefulShutdownManager = None):
    """Setup signal handlers for graceful shutdown."""
    manager = manager or _default_manager

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, triggering shutdown")
        manager.trigger_shutdown()

    for sig in manager.config.signal_types:
        manager._original_signal_handlers[sig] = signal.signal(sig, signal_handler)


def create_server(port: int) -> GracefulServer:
    """Create a server with graceful shutdown."""
    return GracefulServer(port)