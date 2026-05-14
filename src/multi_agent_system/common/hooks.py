"""Task hooks for pre/post execution callbacks."""

import logging
from typing import Callable, Dict, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger('hooks')


@dataclass
class TaskHook:
    """Represents a task hook callback."""
    name: str
    callback: Callable
    hook_type: str  # 'pre_execute', 'post_execute', 'on_success', 'on_failure', 'on_timeout'
    task_types: List[str] = field(default_factory=list)  # Empty means all types


class TaskHookManager:
    """Manages task execution hooks."""

    def __init__(self):
        self._hooks: Dict[str, List[TaskHook]] = {
            'pre_execute': [],
            'post_execute': [],
            'on_success': [],
            'on_failure': [],
            'on_timeout': []
        }

    def register(self, hook_type: str, callback: Callable,
                 name: str = None, task_types: List[str] = None):
        """Register a task hook."""
        hook = TaskHook(
            name=name or callback.__name__,
            callback=callback,
            hook_type=hook_type,
            task_types=task_types or []
        )
        self._hooks[hook_type].append(hook)
        logger.info(f'Registered {hook_type} hook: {hook.name}')

    def unregister(self, name: str):
        """Unregister a hook by name."""
        for hook_type in self._hooks:
            self._hooks[hook_type] = [h for h in self._hooks[hook_type] if h.name != name]
        logger.info(f'Unregistered hook: {name}')

    def execute_hooks(self, hook_type: str, task_id: str,
                      task_type: str, context: Dict) -> bool:
        """
        Execute hooks of a given type.
        Returns True if execution should continue, False to abort.
        """
        if hook_type not in self._hooks:
            return True

        for hook in self._hooks[hook_type]:
            # Check if hook applies to this task type
            if hook.task_types and task_type not in hook.task_types:
                continue

            try:
                result = hook.callback(task_id, task_type, context)
                # If hook returns False, abort the operation
                if result is False:
                    logger.info(f'Hook {hook.name} returned False, aborting {hook_type}')
                    return False
            except Exception as e:
                logger.error(f'Hook {hook.name} failed: {e}')

        return True

    def execute_pre_execute(self, task_id: str, task_type: str,
                            task_data: Dict) -> bool:
        """Execute pre-execute hooks. Return False to abort task."""
        return self.execute_hooks('pre_execute', task_id, task_type, {
            'task_data': task_data,
            'action': 'execute'
        })

    def execute_post_execute(self, task_id: str, task_type: str,
                             context: Dict):
        """Execute post-execute hooks."""
        self.execute_hooks('post_execute', task_id, task_type, context)

    def execute_on_success(self, task_id: str, task_type: str,
                           result: Dict):
        """Execute success hooks."""
        self.execute_hooks('on_success', task_id, task_type, {
            'result': result,
            'action': 'success'
        })

    def execute_on_failure(self, task_id: str, task_type: str,
                          error: str):
        """Execute failure hooks."""
        self.execute_hooks('on_failure', task_id, task_type, {
            'error': error,
            'action': 'failure'
        })

    def execute_on_timeout(self, task_id: str, task_type: str):
        """Execute timeout hooks."""
        self.execute_hooks('on_timeout', task_id, task_type, {
            'action': 'timeout'
        })

    def get_hooks_status(self) -> Dict:
        """Get status of all registered hooks."""
        return {
            hook_type: [{'name': h.name, 'task_types': h.task_types}
                        for h in hooks]
            for hook_type, hooks in self._hooks.items()
        }


# Global hook manager
_hook_manager = TaskHookManager()


def get_hook_manager() -> TaskHookManager:
    return _hook_manager


def register_pre_execute(callback: Callable, name: str = None,
                         task_types: List[str] = None):
    """Decorator-style registration for pre-execute hooks."""
    _hook_manager.register('pre_execute', callback, name, task_types)


def register_post_execute(callback: Callable, name: str = None,
                          task_types: List[str] = None):
    """Decorator-style registration for post-execute hooks."""
    _hook_manager.register('post_execute', callback, name, task_types)


def register_on_success(callback: Callable, name: str = None,
                       task_types: List[str] = None):
    """Decorator-style registration for on_success hooks."""
    _hook_manager.register('on_success', callback, name, task_types)


def register_on_failure(callback: Callable, name: str = None,
                        task_types: List[str] = None):
    """Decorator-style registration for on_failure hooks."""
    _hook_manager.register('on_failure', callback, name, task_types)