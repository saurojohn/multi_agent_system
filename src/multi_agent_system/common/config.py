"""Configuration loader for multi-agent system."""

import os
import yaml
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger('config')


class Config:
    """Configuration manager."""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._get_default_config_path()
        self._config: Dict[str, Any] = {}
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


def setup_logging(level: str = 'INFO', format: str = None):
    """Setup logging configuration."""
    log_format = format or '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=log_format)
