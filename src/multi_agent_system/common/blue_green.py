"""Blue-green deployment for zero-downtime updates."""

import json
import logging
import threading
import time
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urljoin

logger = logging.getLogger('blue_green')


class DeploymentState(Enum):
    """Deployment states."""
    IDLE = "idle"
    PREPARE_BLUE = "prepare_blue"
    DEPLOY_BLUE = "deploy_blue"
    TEST_BLUE = "test_blue"
    SWITCH = "switch"
    ROLLBACK_BLUE = "rollback_blue"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class DeploymentConfig:
    """Configuration for blue-green deployment."""
    name: str
    blue_url: str
    green_url: str
    health_check_path: str = "/health"
    health_check_timeout: int = 30
    test_duration: int = 60
    rollback_threshold: float = 0.5  # 50% errors triggers rollback
    auto_rollback: bool = True


@dataclass
class DeploymentStatus:
    """Current deployment status."""
    deployment_id: str
    config_name: str
    state: DeploymentState
    active_color: str  # "blue" or "green"
    inactive_color: str
    started_at: float
    completed_at: Optional[float] = None
    error: Optional[str] = None
    metrics: Dict = None


class BlueGreenDeployer:
    """
    Blue-green deployment manager.
    Enables zero-downtime deployments with instant rollback.
    """

    def __init__(self):
        self._deployments: Dict[str, DeploymentConfig] = {}
        self._statuses: Dict[str, DeploymentStatus] = {}
        self._lock = threading.RLock()
        self._callbacks: Dict[str, List[Callable]] = {
            'pre_deploy': [],
            'post_deploy': [],
            'pre_rollback': [],
            'post_rollback': []
        }

    def register_deployment(self, name: str, blue_url: str,
                            green_url: str, **kwargs) -> DeploymentConfig:
        """Register a blue-green deployment."""
        config = DeploymentConfig(
            name=name,
            blue_url=blue_url,
            green_url=green_url,
            **kwargs
        )
        with self._lock:
            self._deployments[name] = config
        logger.info(f"Registered blue-green deployment: {name}")
        return config

    def update_deployment_url(self, name: str, color: str, url: str):
        """Update the URL for a specific color."""
        with self._lock:
            if name not in self._deployments:
                raise ValueError(f"Deployment {name} not found")
            config = self._deployments[name]
            if color == "blue":
                config.blue_url = url
            elif color == "green":
                config.green_url = url
            else:
                raise ValueError(f"Invalid color: {color}")
        logger.info(f"Updated {color} URL for {name}: {url}")

    def deploy(self, name: str, initial_metric: float = None) -> DeploymentStatus:
        """
        Execute blue-green deployment.
        Deploys to inactive environment, tests, then switches.
        """
        with self._lock:
            if name not in self._deployments:
                raise ValueError(f"Deployment {name} not found")

            config = self._deployments[name]
            status = DeploymentStatus(
                deployment_id=f"{name}_{int(time.time())}",
                config_name=name,
                state=DeploymentState.PREPARE_BLUE,
                active_color=self._get_active_color(name),
                inactive_color=self._get_inactive_color(name),
                started_at=time.time(),
                metrics={'requests': 0, 'errors': 0, 'latency': 0}
            )

            if initial_metric is not None:
                status.metrics['initial_metric'] = initial_metric

            self._statuses[status.deployment_id] = status

        # Execute pre-deploy callbacks
        self._trigger_callbacks('pre_deploy', name, status)

        try:
            # Phase 1: Prepare inactive environment
            self._transition_state(status, DeploymentState.DEPLOY_BLUE)
            inactive_url = self._get_inactive_url(config, name)
            logger.info(f"Deploying to inactive environment: {inactive_url}")

            # Phase 2: Health check
            self._transition_state(status, DeploymentState.TEST_BLUE)
            if not self._health_check(inactive_url, config.health_check_path,
                                      config.health_check_timeout):
                raise Exception(f"Health check failed for {inactive_url}")

            # Phase 3: Switch traffic
            self._transition_state(status, DeploymentState.SWITCH)
            self._switch_traffic(name, status.inactive_color)

            # Complete
            self._transition_state(status, DeploymentState.COMPLETE)
            status.completed_at = time.time()

            self._trigger_callbacks('post_deploy', name, status)
            logger.info(f"Blue-green deployment complete: {name}")

        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            status.state = DeploymentState.FAILED
            status.error = str(e)

            if config.auto_rollback:
                self._rollback(name, status)

        return status

    def rollback(self, name: str) -> DeploymentStatus:
        """Manually trigger rollback to previous environment."""
        with self._lock:
            if name not in self._deployments:
                raise ValueError(f"Deployment {name} not found")

            # Find most recent deployment
            active_color = self._get_active_color(name)
            inactive_color = "green" if active_color == "blue" else "blue"

            status = DeploymentStatus(
                deployment_id=f"{name}_rollback_{int(time.time())}",
                config_name=name,
                state=DeploymentState.ROLLBACK_BLUE,
                active_color=active_color,
                inactive_color=inactive_color,
                started_at=time.time(),
                metrics={}
            )
            self._statuses[status.deployment_id] = status

        self._trigger_callbacks('pre_rollback', name, status)

        self._rollback(name, status)

        self._trigger_callbacks('post_rollback', name, status)

        return status

    def _rollback(self, name: str, status: DeploymentStatus):
        """Execute rollback logic."""
        try:
            config = self._deployments[name]
            logger.warning(f"Rolling back {name} to {status.active_color}")

            # Switch back to active color
            self._switch_traffic(name, status.active_color)

            status.completed_at = time.time()
            logger.info(f"Rollback complete for {name}")
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            status.state = DeploymentState.FAILED
            status.error = str(e)

    def _switch_traffic(self, name: str, to_color: str):
        """Switch traffic to the specified color."""
        with self._lock:
            config = self._deployments[name]
            if to_color == "blue":
                config.blue_url, config.green_url = config.green_url, config.blue_url
            # Save the switch
            self._save_switch_state(name, to_color)

        logger.info(f"Switched traffic to {to_color} for {name}")

    def _get_active_color(self, name: str) -> str:
        """Get current active color from state."""
        state = self._load_switch_state(name)
        return state.get('active_color', 'blue')

    def _get_inactive_color(self, name: str) -> str:
        """Get current inactive color."""
        return "green" if self._get_active_color(name) == "blue" else "blue"

    def _get_inactive_url(self, config: DeploymentConfig, name: str) -> str:
        """Get URL for the inactive environment."""
        active = self._get_active_color(name)
        return config.blue_url if active == "green" else config.green_url

    def _health_check(self, url: str, path: str, timeout: int) -> bool:
        """Perform health check on target environment."""
        try:
            full_url = urljoin(url, path)
            request = urllib.request.Request(full_url, method='GET')

            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.status == 200
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    def _transition_state(self, status: DeploymentStatus, state: DeploymentState):
        """Transition to a new state."""
        status.state = state
        logger.debug(f"Deployment {status.config_name}: {state.value}")

    def _trigger_callbacks(self, event: str, name: str, status: DeploymentStatus):
        """Trigger registered callbacks."""
        for callback in self._callbacks.get(event, []):
            try:
                callback(name, status)
            except Exception as e:
                logger.error(f"Callback {event} failed: {e}")

    def register_callback(self, event: str, callback: Callable):
        """Register a callback for deployment events."""
        if event in self._callbacks:
            self._callbacks[event].append(callback)

    def _save_switch_state(self, name: str, active_color: str):
        """Save switch state to disk."""
        import os
        state_dir = "/tmp/multi_agent_deployments"
        os.makedirs(state_dir, exist_ok=True)

        state_file = f"{state_dir}/{name}_state.json"
        with open(state_file, 'w') as f:
            json.dump({'active_color': active_color, 'updated_at': time.time()}, f)

    def _load_switch_state(self, name: str) -> Dict:
        """Load switch state from disk."""
        import os
        state_file = f"/tmp/multi_agent_deployments/{name}_state.json"

        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                return json.load(f)
        return {'active_color': 'blue'}

    def get_deployment_status(self, name: str) -> Optional[DeploymentStatus]:
        """Get status of a deployment."""
        with self._lock:
            # Find most recent status for this deployment
            for status in sorted(self._statuses.values(),
                               key=lambda s: s.started_at,
                               reverse=True):
                if status.config_name == name:
                    return status
        return None

    def get_all_statuses(self) -> List[DeploymentStatus]:
        """Get all deployment statuses."""
        with self._lock:
            return list(self._statuses.values())

    def get_active_url(self, name: str) -> Optional[str]:
        """Get the currently active URL for a deployment."""
        with self._lock:
            if name not in self._deployments:
                return None
            config = self._deployments[name]
            active = self._get_active_color(name)
            return config.blue_url if active == "blue" else config.green_url

    def record_metrics(self, deployment_id: str, requests: int,
                      errors: int, latency: float):
        """Record deployment metrics for monitoring."""
        with self._lock:
            if deployment_id in self._statuses:
                status = self._statuses[deployment_id]
                status.metrics['requests'] = requests
                status.metrics['errors'] = errors
                status.metrics['latency'] = latency

                # Check for rollback threshold
                if requests > 0:
                    error_rate = errors / requests
                    if error_rate > 0.5:  # 50% error rate
                        logger.warning(f"High error rate detected: {error_rate:.2%}")
                        # Trigger automatic rollback if enabled
                        config = self._deployments.get(status.config_name)
                        if config and config.auto_rollback:
                            self.rollback(status.config_name)


class LoadBalancer:
    """
    Simple load balancer for blue-green deployments.
    Routes traffic based on active color.
    """

    def __init__(self, deployer: BlueGreenDeployer):
        self.deployer = deployer
        self._routes: Dict[str, str] = {}  # path -> deployment name

    def add_route(self, path: str, deployment_name: str):
        """Add a route to a deployment."""
        self._routes[path] = deployment_name

    def remove_route(self, path: str):
        """Remove a route."""
        if path in self._routes:
            del self._routes[path]

    def get_active_url(self, path: str) -> Optional[str]:
        """Get active URL for a path."""
        deployment_name = self._routes.get(path)
        if not deployment_name:
            return None
        return self.deployer.get_active_url(deployment_name)

    def route_request(self, path: str, method: str, headers: Dict,
                      body: Any = None) -> Optional[Dict]:
        """Route a request to the active deployment."""
        url = self.get_active_url(path)
        if not url:
            return None

        full_url = urljoin(url, path)

        try:
            request = urllib.request.Request(
                full_url,
                data=json.dumps(body).encode() if body else None,
                headers=headers,
                method=method
            )

            with urllib.request.urlopen(request, timeout=30) as response:
                return {
                    'status': response.status,
                    'headers': dict(response.headers),
                    'body': response.read().decode()
                }
        except urllib.error.HTTPError as e:
            return {
                'status': e.code,
                'headers': dict(e.headers),
                'body': e.read().decode()
            }
        except Exception as e:
            logger.error(f"Route request failed: {e}")
            return None


# Global deployer
_deployer = BlueGreenDeployer()


def get_deployer() -> BlueGreenDeployer:
    return _deployer


def get_load_balancer() -> LoadBalancer:
    return LoadBalancer(_deployer)