"""Configuration loader for multi-agent system."""

import os
import yaml
import logging
import threading
import time
from typing import Dict, Any, List, Optional, Callable

logger = logging.getLogger('config')


class Config:
    """Configuration manager with hot reload support."""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._get_default_config_path()
        self._config: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._reload_callbacks: List[Callable] = []
        self._watcher_running = False
        self._watcher_thread = None
        self._last_modified = 0
        self.load()

    def _get_default_config_path(self) -> str:
        """Get default config path."""
        this_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.abspath(os.path.join(this_dir, '..', '..'))
        return os.path.join(src_dir, 'config', 'default.yaml')

    def load(self):
        """Load configuration from YAML file."""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                self._config = yaml.safe_load(f) or {}
            logger.info(f'Loaded config from {self.config_path}')
        else:
            logger.warning(f'Config file not found: {self.config_path}, using defaults')
            self._config = self._get_defaults()

    def _get_defaults(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'server': {
                'host': 'localhost',
                'port': 8080
            },
            'orchestrator': {
                'heartbeat_interval': 10,
                'heartbeat_timeout': 30,
                'max_task_retries': 3,
                'scheduler_interval': 0.1
            },
            'workers': [],
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'tasks': {
                'default_timeout': 300,
                'default_priority': 2,
                'max_concurrent': 100
            }
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key (supports dot notation like 'server.port')."""
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            if value is None:
                return default
        return value

    @property
    def server(self) -> Dict[str, Any]:
        return self._config.get('server', {})

    @property
    def orchestrator(self) -> Dict[str, Any]:
        return self._config.get('orchestrator', {})

    @property
    def workers(self) -> List[Dict[str, Any]]:
        return self._config.get('workers', [])

    @property
    def logging_config(self) -> Dict[str, Any]:
        return self._config.get('logging', {})

    @property
    def tasks(self) -> Dict[str, Any]:
        return self._config.get('tasks', {})

    def reload(self):
        """Manually reload configuration from file."""
        with self._lock:
            old_config = self._config.copy()
            self.load()
            if old_config != self._config:
                logger.info('Configuration reloaded')
                self._notify_callbacks(old_config, self._config)

    def _notify_callbacks(self, old_config: Dict, new_config: Dict):
        """Notify registered callbacks of config changes."""
        for callback in self._reload_callbacks:
            try:
                callback(new_config)
            except Exception as e:
                logger.error(f'Config reload callback failed: {e}')

    def on_reload(self, callback: Callable[[Dict], None]):
        """Register callback to be called when config is reloaded."""
        self._reload_callbacks.append(callback)

    def start_watcher(self, interval: float = 5.0):
        """Start automatic config file watcher."""
        self._watcher_running = True
        self._watcher_thread = threading.Thread(
            target=self._watch_config_file,
            args=(interval,),
            daemon=True
        )
        self._watcher_thread.start()
        logger.info(f'Started config watcher (interval: {interval}s)')

    def stop_watcher(self):
        """Stop automatic config file watcher."""
        self._watcher_running = False
        if self._watcher_thread:
            self._watcher_thread.join(timeout=2)
        logger.info('Stopped config watcher')

    def _watch_config_file(self, interval: float):
        """Watch for config file changes."""
        if os.path.exists(self.config_path):
            self._last_modified = os.path.getmtime(self.config_path)

        while self._watcher_running:
            time.sleep(interval)
            if os.path.exists(self.config_path):
                current_mtime = os.path.getmtime(self.config_path)
                if current_mtime != self._last_modified:
                    self._last_modified = current_mtime
                    logger.info('Config file changed, reloading...')
                    self.reload()

    def set(self, key: str, value: Any):
        """Set configuration value (runtime only, not persisted)."""
        with self._lock:
            keys = key.split('.')
            config = self._config
            for k in keys[:-1]:
                if k not in config:
                    config[k] = {}
                config = config[k]
            config[keys[-1]] = value
            logger.info(f'Runtime config set: {key} = {value}')


def setup_logging(level: str = 'INFO', format: str = None):
    """Setup logging configuration."""
    log_format = format or '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=log_format)
