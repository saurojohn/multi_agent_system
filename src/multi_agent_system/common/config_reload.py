"""Configuration hot-reloading without restart."""

import logging
import threading
import time
import os
import hashlib
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('config_reload')


class ReloadStrategy(Enum):
    """How to handle config changes."""
    IMMEDIATE = "immediate"  # Apply immediately
    GRACEFUL = "graceful"     # Wait for idle period
    SCHEDULED = "scheduled"   # Apply at specific times


@dataclass
class ConfigChange:
    """A configuration change."""
    key: str
    old_value: Any
    new_value: Any
    timestamp: float
    source: str  # file, api, cli
    triggered_by: str  # user or system


@dataclass
class ConfigVersion:
    """Version information for config."""
    version: int
    timestamp: float
    changed_keys: List[str]
    checksum: str


class ConfigSource:
    """Base class for configuration sources."""

    def __init__(self, name: str):
        self.name = name

    def load(self) -> Dict:
        """Load configuration."""
        raise NotImplementedError

    def watch(self, callback: Callable):
        """Watch for changes."""
        raise NotImplementedError

    def stop_watching(self):
        """Stop watching for changes."""
        pass


class FileConfigSource(ConfigSource):
    """File-based configuration source."""

    def __init__(self, filepath: str, poll_interval: int = 5):
        super().__init__("file")
        self.filepath = filepath
        self.poll_interval = poll_interval
        self._running = False
        self._thread: threading.Thread = None
        self._last_checksum: str = None
        self._callbacks: List[Callable] = []

    def load(self) -> Dict:
        """Load configuration from file."""
        import json

        if not os.path.exists(self.filepath):
            return {}

        try:
            with open(self.filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {self.filepath}: {e}")
            return {}

    def _calculate_checksum(self) -> str:
        """Calculate file checksum."""
        if not os.path.exists(self.filepath):
            return None

        with open(self.filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    def watch(self, callback: Callable):
        """Watch file for changes."""
        self._callbacks.append(callback)

        if not self._running:
            self._running = True
            self._last_checksum = self._calculate_checksum()
            self._thread = threading.Thread(target=self._watch_loop, daemon=True)
            self._thread.start()

    def stop_watching(self):
        """Stop watching."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _watch_loop(self):
        """Background watch loop."""
        while self._running:
            time.sleep(self.poll_interval)

            new_checksum = self._calculate_checksum()
            if new_checksum and new_checksum != self._last_checksum:
                self._last_checksum = new_checksum
                logger.info(f"Config file changed: {self.filepath}")

                for callback in self._callbacks:
                    try:
                        callback(self)
                    except Exception as e:
                        logger.error(f"Config change callback failed: {e}")


class ConfigHotReloader:
    """
    Manages configuration with hot-reload support.
    """

    def __init__(self):
        self._config: Dict = {}
        self._sources: Dict[str, ConfigSource] = {}
        self._lock = threading.RLock()
        self._change_handlers: List[Callable[[ConfigChange], None]] = []
        self._version_history: List[ConfigVersion] = []
        self._version: int = 0
        self._strategy: ReloadStrategy = ReloadStrategy.IMMEDIATE

    def add_source(self, name: str, source: ConfigSource):
        """Add a configuration source."""
        with self._lock:
            self._sources[name] = source

            # Watch for changes
            source.watch(self._on_config_change)

        logger.info(f"Added config source: {name}")

    def load_all(self) -> Dict:
        """Load configuration from all sources."""
        config = {}

        with self._lock:
            for name, source in self._sources.items():
                try:
                    source_config = source.load()
                    config.update(source_config)
                except Exception as e:
                    logger.error(f"Failed to load config from {name}: {e}")

            self._config = config
            self._version += 1

        return config

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        with self._lock:
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

    def set(self, key: str, value: Any, source: str = "memory"):
        """Set a configuration value."""
        with self._lock:
            keys = key.split('.')
            config = self._config

            for k in keys[:-1]:
                if k not in config:
                    config[k] = {}
                config = config[k]

            old_value = config.get(keys[-1])
            config[keys[-1]] = value

            self._version += 1

            change = ConfigChange(
                key=key,
                old_value=old_value,
                new_value=value,
                timestamp=time.time(),
                source=source,
                triggered_by="api"
            )

            self._notify_change_handlers(change)

        logger.info(f"Config set: {key} = {value}")

    def reload(self, source_name: str = None):
        """Reload configuration."""
        if source_name:
            if source_name in self._sources:
                source = self._sources[source_name]
                new_config = source.load()

                with self._lock:
                    old_keys = set(self._flatten_dict(self._config).keys())
                    new_keys = set(self._flatten_dict(new_config).keys())

                    # Find changed keys
                    for key in old_keys - new_keys:
                        self.set(key, None, source="reload")

                    for key, value in self._flatten_dict(new_config).items():
                        old_value = self.get(key)
                        if old_value != value:
                            change = ConfigChange(
                                key=key,
                                old_value=old_value,
                                new_value=value,
                                timestamp=time.time(),
                                source=source,
                                triggered_by="reload"
                            )
                            self._notify_change_handlers(change)

                    self._config.update(new_config)

                logger.info(f"Reloaded config from: {source_name}")
        else:
            self.load_all()

    def _on_config_change(self, source: ConfigSource):
        """Handle configuration change from source."""
        logger.info(f"Config change detected from: {source.name}")

        if self._strategy == ReloadStrategy.IMMEDIATE:
            self.reload(source.name)
        elif self._strategy == ReloadStrategy.GRACEFUL:
            self._schedule_graceful_reload(source.name)

    def _schedule_graceful_reload(self, source_name: str):
        """Schedule a graceful reload."""
        def delayed_reload():
            time.sleep(5)  # Wait for current operations
            self.reload(source_name)

        thread = threading.Thread(target=delayed_reload, daemon=True)
        thread.start()

    def _notify_change_handlers(self, change: ConfigChange):
        """Notify all change handlers."""
        for handler in self._change_handlers:
            try:
                handler(change)
            except Exception as e:
                logger.error(f"Config change handler failed: {e}")

    def add_change_handler(self, handler: Callable[[ConfigChange], None]):
        """Add a handler for config changes."""
        self._change_handlers.append(handler)

    def _flatten_dict(self, d: Dict, parent_key: str = '') -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_version(self) -> int:
        """Get current config version."""
        return self._version

    def get_history(self, limit: int = 10) -> List[ConfigVersion]:
        """Get config version history."""
        return self._version_history[-limit:]

    def set_strategy(self, strategy: ReloadStrategy):
        """Set the reload strategy."""
        self._strategy = strategy
        logger.info(f"Config reload strategy set to: {strategy.value}")

    def get_stats(self) -> Dict:
        """Get hot-reloader statistics."""
        with self._lock:
            return {
                'sources': list(self._sources.keys()),
                'version': self._version,
                'strategy': self._strategy.value,
                'handlers': len(self._change_handlers)
            }


class EnvironmentConfigSource(ConfigSource):
    """Configuration from environment variables."""

    def __init__(self, prefix: str = "APP_"):
        super().__init__("environment")
        self.prefix = prefix

    def load(self) -> Dict:
        """Load configuration from environment."""
        config = {}

        for key, value in os.environ.items():
            if key.startswith(self.prefix):
                config_key = key[len(self.prefix):].lower()
                config[config_key] = self._parse_value(value)

        return config

    def _parse_value(self, value: str) -> Any:
        """Parse environment variable value."""
        # Try to parse as JSON
        try:
            import json
            return json.loads(value)
        except:
            pass

        # Boolean
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'

        # Number
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except:
            pass

        return value


# Global config reloader
_config_reloader = ConfigHotReloader()


def get_config_reloader() -> ConfigHotReloader:
    return _config_reloader


def load_env_config(prefix: str = "APP_"):
    """Load configuration from environment variables."""
    source = EnvironmentConfigSource(prefix)
    _config_reloader.add_source("env", source)
    return _config_reloader.load_all()