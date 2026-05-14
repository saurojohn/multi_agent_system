"""Workflow engine for orchestrating multi-step processes."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('workflow')


class WorkflowState(Enum):
    """Workflow execution states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StepState(Enum):
    """Step execution states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class WorkflowStep:
    """A step in a workflow."""
    step_id: str
    name: str
    action: str  # Action to perform
    handler: Callable = None  # Handler function
    args: Dict = field(default_factory=dict)  # Arguments for action
    conditions: List[Callable] = field(default_factory=list)  # Conditions to skip
    retry_count: int = 0
    timeout: int = 300
    on_failure: str = "stop"  # "stop", "continue", "retry"
    depends_on: List[str] = field(default_factory=list)  # Step IDs this depends on


@dataclass
class StepResult:
    """Result of a step execution."""
    step_id: str
    state: StepState
    result: Any = None
    error: Optional[str] = None
    duration_ms: float = 0
    attempts: int = 1


@dataclass
class WorkflowExecution:
    """Execution context for a workflow."""
    execution_id: str
    workflow_id: str
    state: WorkflowState
    current_step: int = 0
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    context: Dict = field(default_factory=dict)  # Shared context across steps
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    error: Optional[str] = None


@dataclass
class Workflow:
    """A workflow definition."""
    workflow_id: str
    name: str
    steps: List[WorkflowStep]
    description: str = ""
    metadata: Dict = field(default_factory=dict)


class WorkflowEngine:
    """
    Workflow execution engine.
    """

    def __init__(self):
        self._workflows: Dict[str, Workflow] = {}
        self._executions: Dict[str, WorkflowExecution] = {}
        self._lock = threading.RLock()
        self._executors: Dict[str, Callable] = {}  # action -> handler

    def register_workflow(self, workflow: Workflow):
        """Register a workflow definition."""
        with self._lock:
            self._workflows[workflow.workflow_id] = workflow
            logger.info(f"Registered workflow: {workflow.name}")

    def register_executor(self, action: str, handler: Callable):
        """Register an executor for an action."""
        self._executors[action] = handler

    def create_workflow(self, name: str, steps: List[WorkflowStep],
                       **metadata) -> Workflow:
        """Create and register a new workflow."""
        workflow_id = str(uuid.uuid4())
        workflow = Workflow(
            workflow_id=workflow_id,
            name=name,
            steps=steps,
            metadata=metadata
        )
        self.register_workflow(workflow)
        return workflow

    def execute(self, workflow_id: str, initial_context: Dict = None,
               execution_id: str = None) -> WorkflowExecution:
        """Execute a workflow."""
        with self._lock:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")

            execution_id = execution_id or str(uuid.uuid4())
            execution = WorkflowExecution(
                execution_id=execution_id,
                workflow_id=workflow_id,
                state=WorkflowState.RUNNING,
                context=initial_context or {}
            )
            self._executions[execution_id] = execution

        # Run workflow in background
        thread = threading.Thread(
            target=self._execute_workflow,
            args=(workflow_id, execution_id)
        )
        thread.start()

        return execution

    def _execute_workflow(self, workflow_id: str, execution_id: str):
        """Execute workflow steps."""
        workflow = self._workflows.get(workflow_id)
        execution = self._executions.get(execution_id)

        if not workflow or not execution:
            return

        try:
            for step_idx, step in enumerate(workflow.steps):
                execution.current_step = step_idx

                # Check if step should be skipped
                if self._should_skip_step(step, execution):
                    execution.step_results[step.step_id] = StepResult(
                        step_id=step.step_id,
                        state=StepState.SKIPPED
                    )
                    continue

                # Execute step
                result = self._execute_step(step, execution)
                execution.step_results[step.step_id] = result

                # Handle step failure
                if result.state == StepState.FAILED:
                    if step.on_failure == "stop":
                        execution.state = WorkflowState.FAILED
                        execution.error = f"Step {step.name} failed: {result.error}"
                        break
                    elif step.on_failure == "continue":
                        continue

            if execution.state == WorkflowState.RUNNING:
                execution.state = WorkflowState.COMPLETED

        except Exception as e:
            logger.error(f"Workflow execution failed: {e}")
            execution.state = WorkflowState.FAILED
            execution.error = str(e)

        execution.end_time = time.time()
        logger.info(f"Workflow {workflow.name} execution {execution_id}: {execution.state.value}")

    def _should_skip_step(self, step: WorkflowStep, execution: WorkflowExecution) -> bool:
        """Check if step should be skipped due to conditions."""
        for condition in step.conditions:
            try:
                if not condition(execution.context):
                    return True
            except Exception as e:
                logger.warning(f"Condition check failed for {step.name}: {e}")
        return False

    def _execute_step(self, step: WorkflowStep, execution: WorkflowExecution) -> StepResult:
        """Execute a single step."""
        start_time = time.time()

        # Check dependencies
        for dep_id in step.depends_on:
            dep_result = execution.step_results.get(dep_id)
            if not dep_result or dep_result.state != StepState.COMPLETED:
                return StepResult(
                    step_id=step.step_id,
                    state=StepState.FAILED,
                    error=f"Dependency {dep_id} not completed",
                    duration_ms=(time.time() - start_time) * 1000
                )

        # Retry loop
        attempts = 0
        last_error = None

        while attempts <= step.retry_count:
            try:
                # Get handler
                handler = step.handler or self._executors.get(step.action)

                if not handler:
                    return StepResult(
                        step_id=step.step_id,
                        state=StepState.FAILED,
                        error=f"No handler for action: {step.action}",
                        duration_ms=(time.time() - start_time) * 1000,
                        attempts=attempts + 1
                    )

                # Execute with timeout
                result = self._execute_with_timeout(
                    handler,
                    execution.context,
                    step.timeout
                )

                return StepResult(
                    step_id=step.step_id,
                    state=StepState.COMPLETED,
                    result=result,
                    duration_ms=(time.time() - start_time) * 1000,
                    attempts=attempts + 1
                )

            except Exception as e:
                last_error = str(e)
                attempts += 1
                logger.warning(f"Step {step.name} attempt {attempts} failed: {e}")

                if attempts <= step.retry_count:
                    time.sleep(1 * attempts)  # Exponential backoff

        return StepResult(
            step_id=step.step_id,
            state=StepState.FAILED,
            error=last_error,
            duration_ms=(time.time() - start_time) * 1000,
            attempts=attempts
        )

    def _execute_with_timeout(self, handler: Callable, context: Dict,
                              timeout: int) -> Any:
        """Execute handler with timeout."""
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(handler, context)
            return future.result(timeout=timeout)

    def get_execution(self, execution_id: str) -> Optional[WorkflowExecution]:
        """Get workflow execution."""
        with self._lock:
            return self._executions.get(execution_id)

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        with self._lock:
            if execution_id in self._executions:
                exec = self._executions[execution_id]
                if exec.state == WorkflowState.RUNNING:
                    exec.state = WorkflowState.CANCELLED
                    exec.end_time = time.time()
                    return True
        return False

    def pause_execution(self, execution_id: str) -> bool:
        """Pause a running execution."""
        with self._lock:
            if execution_id in self._executions:
                exec = self._executions[execution_id]
                if exec.state == WorkflowState.RUNNING:
                    exec.state = WorkflowState.PAUSED
                    return True
        return False

    def resume_execution(self, execution_id: str) -> bool:
        """Resume a paused execution."""
        with self._lock:
            if execution_id in self._executions:
                exec = self._executions[execution_id]
                if exec.state == WorkflowState.PAUSED:
                    exec.state = WorkflowState.RUNNING
                    # Restart execution in background
                    thread = threading.Thread(
                        target=self._execute_workflow,
                        args=(exec.workflow_id, execution_id)
                    )
                    thread.start()
                    return True
        return False

    def get_stats(self) -> Dict:
        """Get workflow engine statistics."""
        with self._lock:
            return {
                'total_workflows': len(self._workflows),
                'total_executions': len(self._executions),
                'running': sum(1 for e in self._executions.values() if e.state == WorkflowState.RUNNING),
                'completed': sum(1 for e in self._executions.values() if e.state == WorkflowState.COMPLETED),
                'failed': sum(1 for e in self._executions.values() if e.state == WorkflowState.FAILED)
            }


class WorkflowBuilder:
    """Builder for creating workflows."""

    def __init__(self, name: str):
        self._name = name
        self._steps: List[WorkflowStep] = []

    def add_step(self, name: str, action: str, **config) -> 'WorkflowBuilder':
        """Add a step to the workflow."""
        step = WorkflowStep(
            step_id=str(uuid.uuid4()),
            name=name,
            action=action,
            **{k: v for k, v in config.items() if v is not None}
        )
        self._steps.append(step)
        return self

    def add_condition(self, condition: Callable) -> 'WorkflowBuilder':
        """Add condition to the last step."""
        if self._steps:
            self._steps[-1].conditions.append(condition)
        return self

    def depends_on(self, step_id: str) -> 'WorkflowBuilder':
        """Add dependency to the last step."""
        if self._steps:
            self._steps[-1].depends_on.append(step_id)
        return self

    def with_retry(self, count: int) -> 'WorkflowBuilder':
        """Set retry count for last step."""
        if self._steps:
            self._steps[-1].retry_count = count
        return self

    def with_timeout(self, timeout: int) -> 'WorkflowBuilder':
        """Set timeout for last step."""
        if self._steps:
            self._steps[-1].timeout = timeout
        return self

    def on_failure(self, action: str) -> 'WorkflowBuilder':
        """Set failure behavior for last step."""
        if self._steps:
            self._steps[-1].on_failure = action
        return self

    def build(self, engine: WorkflowEngine = None) -> Workflow:
        """Build the workflow."""
        workflow = Workflow(
            workflow_id=str(uuid.uuid4()),
            name=self._name,
            steps=self._steps
        )

        if engine:
            engine.register_workflow(workflow)

        return workflow


# Global workflow engine
_workflow_engine = WorkflowEngine()


def get_workflow_engine() -> WorkflowEngine:
    return _workflow_engine


def create_workflow(name: str) -> WorkflowBuilder:
    """Create a new workflow builder."""
    return WorkflowBuilder(name)